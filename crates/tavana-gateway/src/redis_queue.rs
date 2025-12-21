//! Redis-based Distributed Query Queue
//!
//! Features:
//! - Priority queues (High/Normal/Low)
//! - Fair scheduling (per-user quotas)
//! - Query deduplication
//! - Dead letter queue for failed queries
//! - Persistence across gateway restarts
//! - Multi-gateway support

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ═══════════════════════════════════════════════════════════════════════════
// TYPES AND CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════

/// Priority levels for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryPriority {
    High,
    Normal,
    Low,
}

impl QueryPriority {
    pub fn queue_key(&self) -> &'static str {
        match self {
            QueryPriority::High => "tavana:queue:high",
            QueryPriority::Normal => "tavana:queue:normal",
            QueryPriority::Low => "tavana:queue:low",
        }
    }
    
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryPriority::High => "high",
            QueryPriority::Normal => "normal",
            QueryPriority::Low => "low",
        }
    }
}

/// A queued query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedQuery {
    pub id: String,
    pub sql: String,
    pub user_id: String,
    pub priority: QueryPriority,
    pub estimated_size_mb: u64,
    pub queued_at: u64,  // Unix timestamp ms
    pub attempt: u32,
    pub max_attempts: u32,
}

impl QueuedQuery {
    pub fn new(sql: String, user_id: String, priority: QueryPriority, estimated_size_mb: u64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            sql,
            user_id,
            priority,
            estimated_size_mb,
            queued_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            attempt: 0,
            max_attempts: 3,
        }
    }
    
    pub fn age_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now.saturating_sub(self.queued_at)
    }
}

/// Queue configuration
#[derive(Debug, Clone)]
pub struct RedisQueueConfig {
    /// Redis connection URL
    pub redis_url: String,
    /// Maximum queries in queue per priority
    pub max_queue_size: usize,
    /// Maximum time a query can wait (seconds)
    pub max_wait_time_secs: u64,
    /// Rate limit: queries per minute per user
    pub user_rate_limit: u32,
    /// Enable query deduplication
    pub enable_dedup: bool,
    /// Dedup TTL in seconds
    pub dedup_ttl_secs: u64,
    /// Dead letter queue enabled
    pub enable_dlq: bool,
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://redis:6379".to_string(),
            max_queue_size: 1000,
            max_wait_time_secs: 300,  // 5 minutes
            user_rate_limit: 60,      // 60 queries/min
            enable_dedup: true,
            dedup_ttl_secs: 60,       // 1 minute dedup window
            enable_dlq: true,
        }
    }
}

impl RedisQueueConfig {
    pub fn from_env() -> Self {
        Self {
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://redis:6379".to_string()),
            max_queue_size: std::env::var("QUEUE_MAX_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            max_wait_time_secs: std::env::var("QUEUE_MAX_WAIT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300),
            user_rate_limit: std::env::var("QUEUE_USER_RATE_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
            enable_dedup: std::env::var("QUEUE_ENABLE_DEDUP")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            dedup_ttl_secs: std::env::var("QUEUE_DEDUP_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
            enable_dlq: std::env::var("QUEUE_ENABLE_DLQ")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// REDIS QUEUE IMPLEMENTATION
// ═══════════════════════════════════════════════════════════════════════════

/// Redis keys used by the queue
mod keys {
    pub const QUEUE_HIGH: &str = "tavana:queue:high";
    pub const QUEUE_NORMAL: &str = "tavana:queue:normal";
    pub const QUEUE_LOW: &str = "tavana:queue:low";
    pub const QUEUE_DLQ: &str = "tavana:queue:dlq";
    pub const PROCESSING: &str = "tavana:processing";
    pub const DEDUP: &str = "tavana:dedup";
    pub const RATE_LIMIT: &str = "tavana:ratelimit";
    pub const WORKERS: &str = "tavana:workers";
    pub const STATS: &str = "tavana:stats";
    pub const NOTIFY: &str = "tavana:notify";
}

/// Errors that can occur during queue operations
#[derive(Debug, Clone)]
pub enum QueueError {
    QueueFull,
    RateLimitExceeded,
    DuplicateQuery,
    QueryExpired,
    RedisError(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::QueueFull => write!(f, "Queue is full"),
            QueueError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            QueueError::DuplicateQuery => write!(f, "Duplicate query"),
            QueueError::QueryExpired => write!(f, "Query expired in queue"),
            QueueError::RedisError(e) => write!(f, "Redis error: {}", e),
        }
    }
}

impl std::error::Error for QueueError {}

/// Redis-based distributed query queue
pub struct RedisQueue {
    conn: ConnectionManager,
    config: RedisQueueConfig,
    gateway_id: String,
}

impl RedisQueue {
    /// Create a new Redis queue connection
    pub async fn new(config: RedisQueueConfig) -> Result<Self> {
        let client = redis::Client::open(config.redis_url.as_str())
            .context("Failed to create Redis client")?;
        
        let conn = ConnectionManager::new(client)
            .await
            .context("Failed to connect to Redis")?;
        
        let gateway_id = format!("gateway-{}", Uuid::new_v4().to_string()[..8].to_string());
        
        info!("Connected to Redis at {} as {}", config.redis_url, gateway_id);
        
        Ok(Self {
            conn,
            config,
            gateway_id,
        })
    }
    
    /// Enqueue a query with priority
    pub async fn enqueue(&self, query: QueuedQuery) -> Result<String, QueueError> {
        let mut conn = self.conn.clone();
        
        // 1. Check rate limit
        if !self.check_rate_limit(&query.user_id).await? {
            crate::metrics::record_queue_rejected();
            return Err(QueueError::RateLimitExceeded);
        }
        
        // 2. Check for duplicate (if enabled)
        if self.config.enable_dedup {
            if self.is_duplicate(&query).await? {
                debug!("Duplicate query detected: {}", query.id);
                return Err(QueueError::DuplicateQuery);
            }
        }
        
        // 3. Check queue size
        let queue_key = query.priority.queue_key();
        let queue_size: usize = conn.zcard(queue_key).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        if queue_size >= self.config.max_queue_size {
            crate::metrics::record_queue_rejected();
            return Err(QueueError::QueueFull);
        }
        
        // 4. Serialize and add to sorted set (score = timestamp for FIFO within priority)
        let query_json = serde_json::to_string(&query)
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        let score = query.queued_at as f64;
        
        conn.zadd::<_, _, _, ()>(queue_key, &query_json, score).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        // 5. Mark as seen for dedup
        if self.config.enable_dedup {
            let dedup_key = self.dedup_key(&query);
            conn.set_ex::<_, _, ()>(&dedup_key, "1", self.config.dedup_ttl_secs).await
                .map_err(|e| QueueError::RedisError(e.to_string()))?;
        }
        
        // 6. Increment stats
        conn.hincr::<_, _, _, ()>(keys::STATS, "enqueued", 1).await.ok();
        
        // 7. Update metrics
        self.update_queue_metrics().await.ok();
        
        // 8. Publish notification
        conn.publish::<_, _, ()>(keys::NOTIFY, "new_query").await.ok();
        
        info!(
            "Enqueued query {} (priority={:?}, user={}, size={}MB)",
            query.id, query.priority, query.user_id, query.estimated_size_mb
        );
        
        Ok(query.id)
    }
    
    /// Dequeue the highest priority available query
    pub async fn dequeue(&self) -> Result<Option<QueuedQuery>, QueueError> {
        let mut conn = self.conn.clone();
        
        // Try queues in priority order: high -> normal -> low
        for queue_key in [keys::QUEUE_HIGH, keys::QUEUE_NORMAL, keys::QUEUE_LOW] {
            // Atomically pop the lowest score (oldest) entry
            let result: Option<Vec<(String, f64)>> = conn
                .zpopmin(queue_key, 1)
                .await
                .map_err(|e| QueueError::RedisError(e.to_string()))?;
            
            if let Some(entries) = result {
                if let Some((query_json, _score)) = entries.into_iter().next() {
                    let query: QueuedQuery = serde_json::from_str(&query_json)
                        .map_err(|e| QueueError::RedisError(e.to_string()))?;
                    
                    // Check if query has expired
                    let age_secs = query.age_ms() / 1000;
                    if age_secs > self.config.max_wait_time_secs {
                        // Move to DLQ
                        if self.config.enable_dlq {
                            self.move_to_dlq(&query, "expired").await.ok();
                        }
                        crate::metrics::record_queue_timeout();
                        continue;  // Try next query
                    }
                    
                    // Add to processing set
                    conn.hset::<_, _, _, ()>(
                        keys::PROCESSING,
                        &query.id,
                        &query_json
                    ).await.ok();
                    
                    // Record wait time
                    let wait_secs = query.age_ms() as f64 / 1000.0;
                    crate::metrics::record_queue_wait_time(wait_secs);
                    
                    // Update stats
                    conn.hincr::<_, _, _, ()>(keys::STATS, "dequeued", 1).await.ok();
                    
                    // Update metrics
                    self.update_queue_metrics().await.ok();
                    
                    debug!(
                        "Dequeued query {} (waited {}s, priority={:?})",
                        query.id, wait_secs, query.priority
                    );
                    
                    return Ok(Some(query));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Mark a query as completed (remove from processing)
    pub async fn complete(&self, query_id: &str) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        
        conn.hdel::<_, _, ()>(keys::PROCESSING, query_id).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        conn.hincr::<_, _, _, ()>(keys::STATS, "completed", 1).await.ok();
        
        debug!("Completed query {}", query_id);
        
        Ok(())
    }
    
    /// Mark a query as failed (retry or move to DLQ)
    pub async fn fail(&self, query_id: &str, error: &str) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        
        // Get query from processing
        let query_json: Option<String> = conn.hget(keys::PROCESSING, query_id).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        if let Some(json) = query_json {
            let mut query: QueuedQuery = serde_json::from_str(&json)
                .map_err(|e| QueueError::RedisError(e.to_string()))?;
            
            // Remove from processing
            conn.hdel::<_, _, ()>(keys::PROCESSING, query_id).await.ok();
            
            query.attempt += 1;
            
            if query.attempt < query.max_attempts {
                // Re-enqueue with same priority
                let new_json = serde_json::to_string(&query)
                    .map_err(|e| QueueError::RedisError(e.to_string()))?;
                
                let score = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as f64;
                
                conn.zadd::<_, _, _, ()>(query.priority.queue_key(), &new_json, score).await
                    .map_err(|e| QueueError::RedisError(e.to_string()))?;
                
                warn!("Re-queued failed query {} (attempt {}/{})", 
                    query_id, query.attempt, query.max_attempts);
            } else {
                // Move to DLQ
                if self.config.enable_dlq {
                    self.move_to_dlq(&query, error).await?;
                }
                conn.hincr::<_, _, _, ()>(keys::STATS, "failed", 1).await.ok();
            }
        }
        
        Ok(())
    }
    
    /// Get queue depths by priority
    pub async fn get_depths(&self) -> Result<(usize, usize, usize), QueueError> {
        let mut conn = self.conn.clone();
        
        let high: usize = conn.zcard(keys::QUEUE_HIGH).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        let normal: usize = conn.zcard(keys::QUEUE_NORMAL).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        let low: usize = conn.zcard(keys::QUEUE_LOW).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        Ok((high, normal, low))
    }
    
    /// Get total queue depth
    pub async fn get_total_depth(&self) -> Result<usize, QueueError> {
        let (high, normal, low) = self.get_depths().await?;
        Ok(high + normal + low)
    }
    
    /// Get number of queries currently being processed
    pub async fn get_processing_count(&self) -> Result<usize, QueueError> {
        let mut conn = self.conn.clone();
        conn.hlen(keys::PROCESSING).await
            .map_err(|e| QueueError::RedisError(e.to_string()))
    }
    
    /// Get dead letter queue size
    pub async fn get_dlq_size(&self) -> Result<usize, QueueError> {
        let mut conn = self.conn.clone();
        conn.llen(keys::QUEUE_DLQ).await
            .map_err(|e| QueueError::RedisError(e.to_string()))
    }
    
    /// Subscribe to queue notifications
    pub async fn subscribe(&self) -> Result<redis::aio::PubSub, QueueError> {
        let client = redis::Client::open(self.config.redis_url.as_str())
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        let mut pubsub = client.get_async_pubsub().await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        pubsub.subscribe(keys::NOTIFY).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        Ok(pubsub)
    }
    
    // ═══════════════════════════════════════════════════════════════════════
    // PRIVATE HELPERS
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Check user rate limit
    async fn check_rate_limit(&self, user_id: &str) -> Result<bool, QueueError> {
        let mut conn = self.conn.clone();
        let key = format!("{}:{}", keys::RATE_LIMIT, user_id);
        
        // Use sliding window rate limiting
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let window_start = now - 60;  // 1 minute window
        
        // Remove old entries
        // Remove entries older than window_start using ZREMRANGEBYSCORE
        redis::cmd("ZREMRANGEBYSCORE")
            .arg(&key)
            .arg(0)
            .arg(window_start as f64)
            .query_async::<()>(&mut conn)
            .await
            .ok();
        
        // Count current window
        let count: usize = conn.zcard(&key).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        if count >= self.config.user_rate_limit as usize {
            return Ok(false);
        }
        
        // Add current request
        conn.zadd::<_, _, _, ()>(&key, now.to_string(), now as f64).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        // Set expiry on the key
        conn.expire::<_, ()>(&key, 120).await.ok();
        
        Ok(true)
    }
    
    /// Check if query is a duplicate
    async fn is_duplicate(&self, query: &QueuedQuery) -> Result<bool, QueueError> {
        let mut conn = self.conn.clone();
        let key = self.dedup_key(query);
        
        let exists: bool = conn.exists(&key).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        Ok(exists)
    }
    
    /// Generate dedup key for a query
    fn dedup_key(&self, query: &QueuedQuery) -> String {
        // Hash SQL + user_id for dedup
        let hash = blake3::hash(format!("{}:{}", query.user_id, query.sql).as_bytes());
        format!("{}:{}", keys::DEDUP, hash.to_hex())
    }
    
    /// Move a query to the dead letter queue
    async fn move_to_dlq(&self, query: &QueuedQuery, reason: &str) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        
        let dlq_entry = serde_json::json!({
            "query": query,
            "reason": reason,
            "moved_at": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
        
        conn.lpush::<_, _, ()>(keys::QUEUE_DLQ, dlq_entry.to_string()).await
            .map_err(|e| QueueError::RedisError(e.to_string()))?;
        
        // Trim DLQ to last 1000 entries
        conn.ltrim::<_, ()>(keys::QUEUE_DLQ, 0, 999).await.ok();
        
        warn!("Moved query {} to DLQ: {}", query.id, reason);
        
        Ok(())
    }
    
    /// Update Prometheus queue metrics
    async fn update_queue_metrics(&self) -> Result<(), QueueError> {
        let (high, normal, low) = self.get_depths().await?;
        let _processing = self.get_processing_count().await?;
        
        crate::metrics::record_queue_depth(high + normal + low);
        crate::metrics::record_queue_depth_by_priority(high, normal, low);
        
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUEUE WORKER
// ═══════════════════════════════════════════════════════════════════════════

/// Worker that processes queries from the queue
pub struct QueueWorker {
    queue: Arc<RedisQueue>,
    worker_id: String,
}

impl QueueWorker {
    pub fn new(queue: Arc<RedisQueue>) -> Self {
        Self {
            queue,
            worker_id: format!("worker-{}", Uuid::new_v4().to_string()[..8].to_string()),
        }
    }
    
    /// Start processing queries from the queue
    pub async fn run<F, Fut>(&self, process_fn: F) -> Result<()>
    where
        F: Fn(QueuedQuery) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        info!("Queue worker {} starting", self.worker_id);
        
        loop {
            // Try to dequeue a query
            match self.queue.dequeue().await {
                Ok(Some(query)) => {
                    let query_id = query.id.clone();
                    
                    match process_fn(query).await {
                        Ok(_) => {
                            self.queue.complete(&query_id).await.ok();
                        }
                        Err(e) => {
                            error!("Query {} failed: {}", query_id, e);
                            self.queue.fail(&query_id, &e.to_string()).await.ok();
                        }
                    }
                }
                Ok(None) => {
                    // No queries available, wait a bit
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Failed to dequeue: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_queue_config_from_env() {
        let config = RedisQueueConfig::from_env();
        assert!(config.max_queue_size > 0);
        assert!(config.user_rate_limit > 0);
    }
    
    #[test]
    fn test_queued_query_creation() {
        let query = QueuedQuery::new(
            "SELECT 1".to_string(),
            "user1".to_string(),
            QueryPriority::Normal,
            100,
        );
        assert!(!query.id.is_empty());
        assert_eq!(query.attempt, 0);
    }
}

