//! Query Result Caching for Tavana Gateway
//!
//! Implements a distributed query cache using Redis (or Dragonfly) for high-performance
//! caching of SQL query results. Based on Redis caching best practices:
//!
//! ## TTL Strategy
//! - Configurable TTL per query pattern (default: 5 minutes)
//! - Pattern-based TTL: aggregate queries get longer TTL than point queries
//! - Event-driven invalidation support via pub/sub (future)
//!
//! ## Cache Key Design
//! - BLAKE3 hash of normalized SQL + user context for O(1) lookups
//! - Versioned keys for easy bulk invalidation
//!
//! ## Best Practices Applied
//! - Lazy invalidation for reduced cache misses
//! - Connection pooling via `redis::aio::ConnectionManager` (thread-safe, no pool needed)
//! - LZ4 compression for large result sets
//! - Cache-aside pattern (read-through, write-around)

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Cache statistics for monitoring
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: AtomicU64,
    /// Total cache misses
    pub misses: AtomicU64,
    /// Total bytes served from cache
    pub bytes_served: AtomicU64,
    /// Total bytes stored in cache
    pub bytes_stored: AtomicU64,
    /// Cache evictions
    pub evictions: AtomicU64,
}

impl CacheStats {
    pub fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn served(&self, bytes: u64) {
        self.bytes_served.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn stored(&self, bytes: u64) {
        self.bytes_stored.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Query cache configuration
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// Redis/Dragonfly connection URL (e.g., "redis://localhost:6379")
    pub redis_url: String,
    /// Default TTL for cached results
    pub default_ttl: Duration,
    /// Maximum cacheable result size (bytes)
    pub max_result_size: usize,
    /// Enable LZ4 compression for results > 1KB
    pub enable_compression: bool,
    /// Compression threshold (bytes)
    pub compression_threshold: usize,
    /// Key prefix for namespacing
    pub key_prefix: String,
    /// Cache version (increment to invalidate all entries)
    pub cache_version: u32,
    /// Enable in-memory L1 cache (local hot cache)
    pub enable_l1_cache: bool,
    /// L1 cache size (number of entries)
    pub l1_cache_size: usize,
    /// L1 cache TTL
    pub l1_ttl: Duration,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://localhost:6379".to_string(),
            default_ttl: Duration::from_secs(300), // 5 minutes
            max_result_size: 100 * 1024 * 1024,    // 100MB max
            enable_compression: true,
            compression_threshold: 1024, // 1KB
            key_prefix: "tavana:qcache".to_string(),
            cache_version: 1,
            enable_l1_cache: true,
            l1_cache_size: 1000,
            l1_ttl: Duration::from_secs(30), // 30 seconds local cache
        }
    }
}

impl QueryCacheConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(url) = std::env::var("TAVANA_CACHE_REDIS_URL") {
            config.redis_url = url;
        }

        if let Ok(ttl) = std::env::var("TAVANA_CACHE_TTL_SECS") {
            if let Ok(secs) = ttl.parse::<u64>() {
                config.default_ttl = Duration::from_secs(secs);
            }
        }

        if let Ok(size) = std::env::var("TAVANA_CACHE_MAX_SIZE_MB") {
            if let Ok(mb) = size.parse::<usize>() {
                config.max_result_size = mb * 1024 * 1024;
            }
        }

        if let Ok(v) = std::env::var("TAVANA_CACHE_COMPRESSION") {
            config.enable_compression = v == "true" || v == "1";
        }

        if let Ok(v) = std::env::var("TAVANA_CACHE_VERSION") {
            if let Ok(ver) = v.parse::<u32>() {
                config.cache_version = ver;
            }
        }

        if let Ok(v) = std::env::var("TAVANA_CACHE_L1_ENABLED") {
            config.enable_l1_cache = v == "true" || v == "1";
        }

        if let Ok(size) = std::env::var("TAVANA_CACHE_L1_SIZE") {
            if let Ok(s) = size.parse::<usize>() {
                config.l1_cache_size = s;
            }
        }

        config
    }
}

/// L1 cache entry with expiry
struct L1Entry {
    data: Bytes,
    expires_at: Instant,
    compressed: bool,
}

/// Distributed query result cache
///
/// Two-tier architecture:
/// - L1: In-memory DashMap for ultra-low latency (< 1μs)
/// - L2: Redis/Dragonfly for distributed caching across gateway replicas
pub struct QueryCache {
    /// Redis connection manager (cheaply cloneable, thread-safe)
    redis: Option<redis::aio::ConnectionManager>,
    /// Configuration
    config: QueryCacheConfig,
    /// Statistics
    stats: Arc<CacheStats>,
    /// L1 in-memory cache
    l1_cache: DashMap<String, L1Entry>,
}

impl QueryCache {
    /// Create a new query cache instance
    pub async fn new(config: QueryCacheConfig) -> Result<Self> {
        let redis = match redis::Client::open(config.redis_url.as_str()) {
            Ok(client) => {
                match client.get_connection_manager().await {
                    Ok(cm) => {
                        info!("Query cache connected to Redis: {}", config.redis_url);
                        Some(cm)
                    }
                    Err(e) => {
                        warn!("Failed to connect to Redis cache: {} - caching disabled", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("Invalid Redis URL: {} - caching disabled", e);
                None
            }
        };

        Ok(Self {
            redis,
            config,
            stats: Arc::new(CacheStats::default()),
            l1_cache: DashMap::new(),
        })
    }

    /// Create a disabled cache (no-op operations)
    pub fn disabled() -> Self {
        Self {
            redis: None,
            config: QueryCacheConfig::default(),
            stats: Arc::new(CacheStats::default()),
            l1_cache: DashMap::new(),
        }
    }

    /// Check if caching is enabled
    pub fn is_enabled(&self) -> bool {
        self.redis.is_some()
    }

    /// Generate cache key from SQL and user context
    ///
    /// Uses BLAKE3 for fast, collision-resistant hashing.
    /// Key format: `{prefix}:v{version}:{hash}`
    pub fn cache_key(&self, sql: &str, user_id: &str) -> String {
        // Normalize SQL: trim whitespace, lowercase keywords
        let normalized = sql.trim();

        // Hash the normalized SQL + user context
        let mut hasher = blake3::Hasher::new();
        hasher.update(normalized.as_bytes());
        hasher.update(user_id.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = &hash.to_hex()[..16]; // First 16 chars (64 bits)

        format!(
            "{}:v{}:{}",
            self.config.key_prefix, self.config.cache_version, hash_hex
        )
    }

    /// Check if a query should be cached based on SQL pattern
    ///
    /// Queries that modify data (INSERT, UPDATE, DELETE, etc.) are not cached.
    /// DDL statements (CREATE, DROP, ALTER) are not cached.
    /// System commands (SET, SHOW) are not cached.
    pub fn is_cacheable(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();

        // Only cache SELECT queries
        if !upper.starts_with("SELECT") && !upper.starts_with("WITH") {
            return false;
        }

        // Don't cache queries with random/time functions
        if upper.contains("RANDOM()")
            || upper.contains("NOW()")
            || upper.contains("CURRENT_TIMESTAMP")
            || upper.contains("CURRENT_DATE")
            || upper.contains("UUID()")
        {
            return false;
        }

        true
    }

    /// Determine TTL based on query pattern
    ///
    /// - Aggregate queries (GROUP BY, COUNT, SUM): longer TTL (10 min)
    /// - Point queries (WHERE id = ...): shorter TTL (2 min)
    /// - Default: configured TTL (5 min)
    pub fn get_ttl(&self, sql: &str) -> Duration {
        let upper = sql.to_uppercase();

        // Aggregate queries change less frequently
        if upper.contains("GROUP BY")
            || upper.contains("COUNT(")
            || upper.contains("SUM(")
            || upper.contains("AVG(")
            || upper.contains("MAX(")
            || upper.contains("MIN(")
        {
            return self.config.default_ttl * 2; // 10 minutes
        }

        // Point queries may be more volatile
        if upper.contains("WHERE") && (upper.contains(" = ") || upper.contains(" IN (")) {
            return self.config.default_ttl / 2; // 2.5 minutes
        }

        self.config.default_ttl
    }

    /// Get cached result for a query
    ///
    /// Checks L1 (memory) first, then L2 (Redis).
    /// Returns (data, from_cache) tuple.
    pub async fn get(&self, sql: &str, user_id: &str) -> Option<CachedResult> {
        let key = self.cache_key(sql, user_id);

        // Check L1 cache first
        if self.config.enable_l1_cache {
            if let Some(entry) = self.l1_cache.get(&key) {
                if entry.expires_at > Instant::now() {
                    self.stats.hit();
                    let data = entry.data.clone();
                    let compressed = entry.compressed;
                    drop(entry); // Release lock
                    
                    debug!(key = %key, "L1 cache hit");
                    return Some(CachedResult {
                        data,
                        compressed,
                        source: CacheSource::L1,
                    });
                } else {
                    // Expired, remove it
                    drop(entry);
                    self.l1_cache.remove(&key);
                }
            }
        }

        // Check L2 (Redis) cache
        if let Some(ref redis) = self.redis {
            let mut conn = redis.clone();
            match redis::cmd("GET")
                .arg(&key)
                .query_async::<Vec<u8>>(&mut conn)
                .await
            {
                Ok(data) if !data.is_empty() => {
                    self.stats.hit();
                    self.stats.served(data.len() as u64);
                    debug!(key = %key, size = data.len(), "L2 cache hit");

                    // Check compression flag (first byte)
                    let (compressed, payload) = if !data.is_empty() && data[0] == 1 {
                        (true, Bytes::from(data[1..].to_vec()))
                    } else if !data.is_empty() {
                        (false, Bytes::from(data[1..].to_vec()))
                    } else {
                        (false, Bytes::from(data))
                    };

                    // Promote to L1 cache
                    if self.config.enable_l1_cache {
                        self.l1_cache.insert(
                            key,
                            L1Entry {
                                data: payload.clone(),
                                expires_at: Instant::now() + self.config.l1_ttl,
                                compressed,
                            },
                        );
                    }

                    return Some(CachedResult {
                        data: payload,
                        compressed,
                        source: CacheSource::L2,
                    });
                }
                Ok(_) => {}
                Err(e) => {
                    debug!(key = %key, error = %e, "Redis GET error");
                }
            }
        }

        self.stats.miss();
        None
    }

    /// Store result in cache
    ///
    /// Compresses large results with LZ4 for storage efficiency.
    /// Stores in both L1 and L2 for write-through caching.
    pub async fn set(&self, sql: &str, user_id: &str, data: &[u8]) -> Result<()> {
        // Don't cache oversized results
        if data.len() > self.config.max_result_size {
            debug!(
                size = data.len(),
                max = self.config.max_result_size,
                "Result too large to cache"
            );
            return Ok(());
        }

        let key = self.cache_key(sql, user_id);
        let ttl = self.get_ttl(sql);

        // Compress if enabled and above threshold
        let (stored_data, compressed) =
            if self.config.enable_compression && data.len() > self.config.compression_threshold {
                match lz4_flex::compress_prepend_size(data) {
                    compressed if compressed.len() < data.len() => {
                        debug!(
                            original = data.len(),
                            compressed = compressed.len(),
                            ratio = format!("{:.1}%", (compressed.len() as f64 / data.len() as f64) * 100.0),
                            "Compressed result"
                        );
                        (compressed, true)
                    }
                    _ => (data.to_vec(), false), // Compression didn't help
                }
            } else {
                (data.to_vec(), false)
            };

        // Prepend compression flag
        let mut payload = Vec::with_capacity(1 + stored_data.len());
        payload.push(if compressed { 1 } else { 0 });
        payload.extend_from_slice(&stored_data);

        // Store in L1 cache
        if self.config.enable_l1_cache {
            self.l1_cache.insert(
                key.clone(),
                L1Entry {
                    data: Bytes::from(stored_data.clone()),
                    expires_at: Instant::now() + self.config.l1_ttl,
                    compressed,
                },
            );

            // Evict old entries if L1 is too large
            if self.l1_cache.len() > self.config.l1_cache_size {
                self.evict_l1();
            }
        }

        // Store in L2 (Redis)
        if let Some(ref redis) = self.redis {
            let mut conn = redis.clone();
            match redis::cmd("SETEX")
                .arg(&key)
                .arg(ttl.as_secs())
                .arg(&payload)
                .query_async::<()>(&mut conn)
                .await
            {
                Ok(()) => {
                    self.stats.stored(payload.len() as u64);
                    debug!(key = %key, size = payload.len(), ttl = ?ttl, "Cached result");
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to cache result");
                }
            }
        }

        Ok(())
    }

    /// Invalidate cache entry for a specific query
    pub async fn invalidate(&self, sql: &str, user_id: &str) -> Result<()> {
        let key = self.cache_key(sql, user_id);

        // Remove from L1
        self.l1_cache.remove(&key);

        // Remove from L2
        if let Some(ref redis) = self.redis {
            let mut conn = redis.clone();
            let _ = redis::cmd("DEL")
                .arg(&key)
                .query_async::<()>(&mut conn)
                .await;
        }

        Ok(())
    }

    /// Invalidate all cache entries (by incrementing version)
    pub async fn invalidate_all(&mut self) {
        self.config.cache_version += 1;
        self.l1_cache.clear();
        info!(
            version = self.config.cache_version,
            "Cache invalidated (version incremented)"
        );
    }

    /// Evict expired and oldest entries from L1 cache
    fn evict_l1(&self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        // First pass: find expired entries
        for entry in self.l1_cache.iter() {
            if entry.value().expires_at <= now {
                to_remove.push(entry.key().clone());
            }
        }

        // Remove expired
        for key in &to_remove {
            self.l1_cache.remove(key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // If still over capacity, remove oldest entries (simple FIFO for now)
        let excess = self.l1_cache.len().saturating_sub(self.config.l1_cache_size);
        if excess > 0 {
            let keys: Vec<String> = self.l1_cache.iter().take(excess).map(|e| e.key().clone()).collect();
            for key in keys {
                self.l1_cache.remove(&key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get L1 cache size
    pub fn l1_size(&self) -> usize {
        self.l1_cache.len()
    }
}

/// Cached result with metadata
pub struct CachedResult {
    /// Raw data (may be compressed)
    pub data: Bytes,
    /// Whether data is LZ4 compressed
    pub compressed: bool,
    /// Cache source (L1 or L2)
    pub source: CacheSource,
}

impl CachedResult {
    /// Decompress data if needed
    pub fn decompress(&self) -> Result<Vec<u8>> {
        if self.compressed {
            lz4_flex::decompress_size_prepended(&self.data)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
        } else {
            Ok(self.data.to_vec())
        }
    }
}

/// Cache source for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSource {
    /// Local in-memory cache (L1)
    L1,
    /// Distributed Redis cache (L2)
    L2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cacheable() {
        assert!(QueryCache::is_cacheable("SELECT * FROM users"));
        assert!(QueryCache::is_cacheable("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(!QueryCache::is_cacheable("INSERT INTO users VALUES (1)"));
        assert!(!QueryCache::is_cacheable("UPDATE users SET x = 1"));
        assert!(!QueryCache::is_cacheable("DELETE FROM users"));
        assert!(!QueryCache::is_cacheable("SELECT random()"));
        assert!(!QueryCache::is_cacheable("SELECT now()"));
    }

    #[test]
    fn test_cache_key_generation() {
        let config = QueryCacheConfig::default();
        let cache = QueryCache {
            redis: None,
            config,
            stats: Arc::new(CacheStats::default()),
            l1_cache: DashMap::new(),
        };

        let key1 = cache.cache_key("SELECT * FROM users", "user1");
        let key2 = cache.cache_key("SELECT * FROM users", "user1");
        let key3 = cache.cache_key("SELECT * FROM users", "user2");

        assert_eq!(key1, key2); // Same SQL + user = same key
        assert_ne!(key1, key3); // Different user = different key
    }

    #[test]
    fn test_ttl_calculation() {
        let config = QueryCacheConfig::default();
        let cache = QueryCache {
            redis: None,
            config: config.clone(),
            stats: Arc::new(CacheStats::default()),
            l1_cache: DashMap::new(),
        };

        // Aggregate queries get longer TTL
        let ttl_agg = cache.get_ttl("SELECT COUNT(*) FROM users GROUP BY status");
        assert_eq!(ttl_agg, config.default_ttl * 2);

        // Point queries get shorter TTL
        let ttl_point = cache.get_ttl("SELECT * FROM users WHERE id = 123");
        assert_eq!(ttl_point, config.default_ttl / 2);

        // Default TTL
        let ttl_default = cache.get_ttl("SELECT * FROM users LIMIT 100");
        assert_eq!(ttl_default, config.default_ttl);
    }
}
