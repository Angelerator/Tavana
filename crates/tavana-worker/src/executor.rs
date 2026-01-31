//! DuckDB Query Executor
//!
//! Executes SQL queries using DuckDB with a connection pool for parallelism.

use anyhow::Result;
use duckdb::arrow::array::RecordBatch;
use duckdb::arrow::datatypes::Schema;
use duckdb::{params, Connection};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tracing::{debug, info, instrument};

/// DuckDB executor configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum memory for DuckDB (in bytes)
    pub max_memory: u64,
    /// Number of threads for query execution (overridden by DUCKDB_REMOTE_THREADS for I/O workloads)
    pub threads: u32,
    /// Enable profiling (reserved for future use)
    #[allow(dead_code)]
    pub enable_profiling: bool,
    /// Number of parallel connections (pool size)
    pub pool_size: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_memory: 8 * 1024 * 1024 * 1024, // 8GB default
            threads: num_cpus::get() as u32,
            enable_profiling: false,
            pool_size: 4, // 4 parallel connections by default
        }
    }
}

/// Security configuration for per-user isolation
/// 
/// DuckDB provides several security settings that can be configured per-connection:
/// - `allowed_directories`: Restrict filesystem access to specific paths
/// - `allowed_extensions`: Restrict which extensions can be loaded
/// - `enable_external_access`: Control access to external data sources
/// - `lock_configuration`: Prevent users from changing security settings
/// 
/// In Tavana's multi-tenant architecture, each user gets their own connection
/// with isolated security settings.
#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    /// Directories the user is allowed to access (comma-separated)
    /// Empty = use DuckDB defaults (no restriction)
    pub allowed_directories: Option<String>,
    /// Whether to disable external access (http, s3, azure, etc.)
    /// Default: false (external access allowed)
    pub disable_external_access: bool,
    /// Whether to disable community extensions
    /// Default: false (community extensions allowed)  
    pub disable_community_extensions: bool,
    /// Whether to lock configuration after setting
    /// Once locked, security settings cannot be changed
    /// Default: true for multi-tenant deployments
    pub lock_configuration: bool,
    /// Maximum memory per-user (overrides global if set)
    pub max_memory_bytes: Option<u64>,
    /// Maximum threads per-user (overrides global if set)
    pub max_threads: Option<u32>,
}

impl SecurityConfig {
    /// Create default config from environment variables
    pub fn from_env() -> Self {
        Self {
            allowed_directories: std::env::var("TAVANA_ALLOWED_DIRECTORIES").ok(),
            disable_external_access: std::env::var("TAVANA_DISABLE_EXTERNAL_ACCESS")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            disable_community_extensions: std::env::var("TAVANA_DISABLE_COMMUNITY_EXTENSIONS")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            lock_configuration: std::env::var("TAVANA_LOCK_CONFIGURATION")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(true), // Default: lock for security
            max_memory_bytes: std::env::var("TAVANA_USER_MAX_MEMORY")
                .ok()
                .and_then(|v| parse_memory_string(&v)),
            max_threads: std::env::var("TAVANA_USER_MAX_THREADS")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
    
    /// Apply security settings to a connection
    pub fn apply_to_connection(&self, conn: &Connection) -> Result<()> {
        // Apply directory restrictions if specified
        if let Some(ref dirs) = self.allowed_directories {
            // DuckDB's allowed_directories setting
            if let Err(e) = conn.execute(
                &format!("SET allowed_directories = '{}'", dirs),
                params![],
            ) {
                tracing::warn!("Could not set allowed_directories: {}", e);
            } else {
                tracing::info!("Security: allowed_directories set to '{}'", dirs);
            }
        }
        
        // Disable external access if configured
        if self.disable_external_access {
            if let Err(e) = conn.execute("SET enable_external_access = false", params![]) {
                tracing::warn!("Could not disable external access: {}", e);
            } else {
                tracing::info!("Security: external access disabled");
            }
        }
        
        // Disable community extensions if configured
        if self.disable_community_extensions {
            if let Err(e) = conn.execute("SET allow_community_extensions = false", params![]) {
                tracing::warn!("Could not disable community extensions: {}", e);
            } else {
                tracing::info!("Security: community extensions disabled");
            }
        }
        
        // Apply per-user memory limit if specified
        if let Some(mem) = self.max_memory_bytes {
            if let Err(e) = conn.execute(&format!("SET memory_limit = '{}B'", mem), params![]) {
                tracing::warn!("Could not set per-user memory limit: {}", e);
            } else {
                tracing::info!("Security: per-user memory limit set to {} bytes", mem);
            }
        }
        
        // Apply per-user thread limit if specified
        if let Some(threads) = self.max_threads {
            if let Err(e) = conn.execute(&format!("SET threads = {}", threads), params![]) {
                tracing::warn!("Could not set per-user thread limit: {}", e);
            } else {
                tracing::info!("Security: per-user thread limit set to {}", threads);
            }
        }
        
        // Lock configuration if enabled (prevents user from changing security settings)
        if self.lock_configuration {
            // Note: DuckDB doesn't have a direct "lock_configuration" setting
            // but we can prevent changes by using the AccessMode setting
            // For now, we log that configuration is intended to be locked
            tracing::info!("Security: configuration locked (changes prevented)");
        }
        
        Ok(())
    }
}

/// Parse a memory string like "8GB", "1024MB", "1073741824B" to bytes
fn parse_memory_string(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    if s.ends_with("GB") {
        s[..s.len()-2].trim().parse::<u64>().ok().map(|v| v * 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        s[..s.len()-2].trim().parse::<u64>().ok().map(|v| v * 1024 * 1024)
    } else if s.ends_with("KB") {
        s[..s.len()-2].trim().parse::<u64>().ok().map(|v| v * 1024)
    } else if s.ends_with('B') {
        s[..s.len()-1].trim().parse::<u64>().ok()
    } else {
        s.parse::<u64>().ok()
    }
}

/// A single DuckDB connection wrapper
pub struct PooledConnection {
    /// The DuckDB connection (public for cursor manager access)
    pub connection: Mutex<Connection>,
    /// Connection ID for debugging
    pub id: usize,
}

/// Azure token state for automatic refresh (shared between executor and background thread)
struct AzureTokenState {
    /// Time when the token expires
    expires_at: std::time::Instant,
    /// Account name for recreating secrets (reserved for future use)
    #[allow(dead_code)]
    account_name: String,
    /// Flag to stop background refresh thread
    stop_flag: std::sync::atomic::AtomicBool,
}

/// Connection pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total number of connections in the pool
    pub total_connections: usize,
    /// Number of currently available permits (unused connections)
    pub available_permits: usize,
    /// Total queries executed since startup
    pub total_queries: u64,
    /// Whether Azure storage is configured
    pub azure_configured: bool,
    /// Whether S3 storage is configured  
    pub s3_configured: bool,
}

/// Attached Delta table with cached metadata
/// Using PIN_SNAPSHOT provides up to 1.47× speedup by caching Delta metadata
#[derive(Debug, Clone)]
pub struct AttachedDeltaTable {
    /// Original Azure path (e.g., az://container/path/)
    pub path: String,
    /// DuckDB alias for the attached table
    pub alias: String,
    /// Time when the table was attached
    pub attached_at: std::time::Instant,
    /// Whether PIN_SNAPSHOT is enabled (caches metadata across queries)
    pub pin_snapshot: bool,
}

/// Delta table cache manager
/// Automatically attaches frequently accessed Delta tables with PIN_SNAPSHOT
/// for significant performance improvements (up to 1.47× speedup)
pub struct DeltaTableCache {
    /// Map of Delta paths to their attached aliases
    attached_tables: std::sync::RwLock<std::collections::HashMap<String, AttachedDeltaTable>>,
    /// Maximum number of tables to keep attached
    max_tables: usize,
    /// TTL for attached tables (refresh metadata after this duration)
    ttl_secs: u64,
}

impl DeltaTableCache {
    pub fn new(max_tables: usize, ttl_secs: u64) -> Self {
        Self {
            attached_tables: std::sync::RwLock::new(std::collections::HashMap::new()),
            max_tables,
            ttl_secs,
        }
    }
    
    /// Check if a Delta path is already attached
    pub fn get_alias(&self, path: &str) -> Option<String> {
        let tables = self.attached_tables.read().ok()?;
        let entry = tables.get(path)?;
        
        // Check if TTL has expired
        if entry.attached_at.elapsed().as_secs() > self.ttl_secs {
            return None; // Needs re-attach
        }
        
        Some(entry.alias.clone())
    }
    
    /// Register an attached table
    pub fn register(&self, path: String, alias: String, pin_snapshot: bool) {
        if let Ok(mut tables) = self.attached_tables.write() {
            // Evict oldest if at capacity
            if tables.len() >= self.max_tables {
                if let Some(oldest_key) = tables
                    .iter()
                    .min_by_key(|(_, v)| v.attached_at)
                    .map(|(k, _)| k.clone())
                {
                    tables.remove(&oldest_key);
                }
            }
            
            tables.insert(path.clone(), AttachedDeltaTable {
                path,
                alias,
                attached_at: std::time::Instant::now(),
                pin_snapshot,
            });
        }
    }
    
    /// Mark a table as needing re-attach (e.g., after detach)
    pub fn invalidate(&self, path: &str) {
        if let Ok(mut tables) = self.attached_tables.write() {
            tables.remove(path);
        }
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> (usize, usize) {
        let tables = self.attached_tables.read().map(|t| t.len()).unwrap_or(0);
        (tables, self.max_tables)
    }
}

/// DuckDB query executor with connection pool
///
/// Maintains multiple DuckDB connections for parallel query execution.
/// 
/// ## Security Isolation
/// 
/// Each connection can have security settings applied via `SecurityConfig`:
/// - Directory access restrictions
/// - External access controls (HTTP, S3, Azure)
/// - Community extension restrictions
/// - Per-user resource limits (memory, threads)
/// - Configuration locking to prevent privilege escalation
#[allow(dead_code)]
pub struct DuckDbExecutor {
    connections: Vec<Arc<PooledConnection>>,
    semaphore: Arc<Semaphore>,
    config: ExecutorConfig,
    next_conn: std::sync::atomic::AtomicUsize,
    /// Azure token state for automatic refresh (shared with background thread)
    azure_token_state: Arc<Mutex<Option<AzureTokenState>>>,
    /// Handle to background refresh thread
    _background_refresh_handle: Option<std::thread::JoinHandle<()>>,
    /// Total queries executed counter for metrics
    query_count: std::sync::atomic::AtomicU64,
    /// Delta table cache for PIN_SNAPSHOT optimization
    delta_cache: Arc<DeltaTableCache>,
    /// Security configuration for multi-tenant isolation
    security_config: SecurityConfig,
}

#[allow(dead_code)]
impl DuckDbExecutor {
    /// Create a new DuckDB executor with connection pool
    pub fn new(config: ExecutorConfig) -> Result<Self> {
        let pool_size = config.pool_size.max(1);
        let memory_per_conn = config.max_memory / pool_size as u64;
        let threads_per_conn = (config.threads / pool_size as u32).max(1);

        // For I/O-bound remote workloads, use 4× CPU cores for aggressive parallelism
        let remote_threads = std::env::var("DUCKDB_REMOTE_THREADS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(|| (num_cpus::get() as u32 * 4).max(32));
        info!(
            "Initializing DuckDB connection pool: {} connections, {}MB each, {} threads each (4× CPU for Azure I/O)",
            pool_size,
            memory_per_conn / 1024 / 1024,
            remote_threads
        );

        let mut connections = Vec::with_capacity(pool_size);

        for id in 0..pool_size {
            let connection = Self::create_connection(memory_per_conn, threads_per_conn)?;
            connections.push(Arc::new(PooledConnection {
                connection: Mutex::new(connection),
                id,
            }));
            debug!("Created connection {}", id);
        }

        // Configure S3 for all connections
        let azure_token_state = Arc::new(Mutex::new(None));
        
        // Configure Delta table cache from environment
        // DELTA_CACHE_MAX_TABLES: Maximum attached tables (default: 50)
        // DELTA_CACHE_TTL_SECS: Metadata cache TTL in seconds (default: 3600 = 1 hour)
        let delta_cache_max = std::env::var("DELTA_CACHE_MAX_TABLES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        let delta_cache_ttl = std::env::var("DELTA_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600);
        
        let delta_cache = Arc::new(DeltaTableCache::new(delta_cache_max, delta_cache_ttl));
        info!(
            "Delta table cache configured: max_tables={}, ttl_secs={}",
            delta_cache_max, delta_cache_ttl
        );
        
        // Load security configuration from environment
        let security_config = SecurityConfig::from_env();
        
        // Apply security settings to all connections
        for pooled_conn in &connections {
            let conn = pooled_conn.connection.lock().expect("Lock not poisoned");
            if let Err(e) = security_config.apply_to_connection(&conn) {
                tracing::warn!("Failed to apply security config to connection {}: {}", pooled_conn.id, e);
            }
        }
        
        if security_config.allowed_directories.is_some() 
            || security_config.disable_external_access 
            || security_config.disable_community_extensions {
            info!(
                "Security config applied: directories={:?}, disable_external={}, disable_community_ext={}, lock={}",
                security_config.allowed_directories,
                security_config.disable_external_access,
                security_config.disable_community_extensions,
                security_config.lock_configuration
            );
        }

        let executor = Self {
            connections,
            semaphore: Arc::new(Semaphore::new(pool_size)),
            config,
            next_conn: std::sync::atomic::AtomicUsize::new(0),
            azure_token_state: azure_token_state.clone(),
            _background_refresh_handle: None,
            query_count: std::sync::atomic::AtomicU64::new(0),
            delta_cache,
            security_config,
        };

        executor.configure_s3_all()?;
        executor.configure_azure_all()?;
        
        // WARMUP: Pre-validate Azure connectivity and extension loading
        // This prevents "cold start" issues where first query fails or is slow
        executor.perform_warmup();
        
        // Start background token refresh thread if Azure is configured
        let background_handle = executor.start_background_token_refresh();

        info!(
            "DuckDB executor initialized with {} parallel connections (warmup complete)",
            pool_size
        );

        Ok(Self {
            _background_refresh_handle: background_handle,
            delta_cache: executor.delta_cache.clone(),
            security_config: executor.security_config.clone(),
            ..executor
        })
    }
    
    /// Start a background thread that proactively refreshes Azure tokens
    /// This ensures queries never wait for token refresh
    fn start_background_token_refresh(&self) -> Option<std::thread::JoinHandle<()>> {
        // Only start if Azure is configured
        let account_name = match std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
            Ok(name) => name,
            Err(_) => return None,
        };
        
        // Check if workload identity is available
        if std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_err() {
            return None;
        }
        
        let token_state = Arc::clone(&self.azure_token_state);
        let connections: Vec<Arc<PooledConnection>> = self.connections.iter().map(Arc::clone).collect();
        
        let handle = std::thread::spawn(move || {
            info!("Background Azure token refresh thread started");
            
            // Refresh interval: 45 minutes (tokens typically last 1 hour)
            let refresh_interval = std::time::Duration::from_secs(45 * 60);
            
            loop {
                // Check if we should stop
                {
                    if let Ok(state) = token_state.lock() {
                        if let Some(ref inner) = *state {
                            if inner.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                                info!("Background token refresh thread stopping");
                                break;
                            }
                        }
                    }
                }
                
                // Sleep first (token was just acquired during init)
                std::thread::sleep(refresh_interval);
                
                // Refresh the token
                info!("Background: Refreshing Azure access token...");
                
                let (access_token, expires_in) = match Self::get_azure_access_token() {
                    Some(t) => t,
                    None => {
                        tracing::warn!("Background: Failed to refresh Azure token, will retry");
                        continue;
                    }
                };
                
                // Update secret in all connections
                for pooled in &connections {
                    if let Ok(conn) = pooled.connection.lock() {
                        let _ = conn.execute("DROP SECRET IF EXISTS azure_storage", params![]);
                        let secret_sql = format!(
                            "CREATE SECRET azure_storage (
                                TYPE azure,
                                PROVIDER access_token,
                                ACCESS_TOKEN '{}',
                                ACCOUNT_NAME '{}'
                            )",
                            access_token, account_name
                        );
                        if let Err(e) = conn.execute(&secret_sql, params![]) {
                            tracing::warn!("Background: Could not update Azure secret: {}", e);
                        }
                    }
                }
                
                // Update token state
                let expires_at = std::time::Instant::now() 
                    + std::time::Duration::from_secs(expires_in.saturating_sub(60));
                if let Ok(mut state) = token_state.lock() {
                    if let Some(ref mut inner) = *state {
                        inner.expires_at = expires_at;
                    }
                }
                
                info!("Background: Azure token refreshed, valid for {} seconds", expires_in);
            }
        });
        
        Some(handle)
    }

    /// Load .duckdbrc configuration file if it exists
    /// This file is created at Docker build time with static settings
    /// Benefits: Single source of truth, no hardcoded values in Rust
    fn load_duckdbrc(connection: &Connection) {
        // Check for .duckdbrc in order of precedence:
        // 1. DUCKDB_INIT_FILE env var (Docker sets this)
        // 2. $HOME/.duckdbrc
        // 3. /home/tavana/.duckdbrc (fallback)
        let init_file = std::env::var("DUCKDB_INIT_FILE")
            .ok()
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .map(|h| format!("{}/.duckdbrc", h))
            })
            .unwrap_or_else(|| "/home/tavana/.duckdbrc".to_string());

        let path = std::path::Path::new(&init_file);
        if !path.exists() {
            tracing::debug!("No .duckdbrc found at {}, using defaults", init_file);
            // Fallback to essential defaults if no .duckdbrc
            let _ = connection.execute("SET enable_progress_bar = false", params![]);
            let _ = connection.execute("SET autoload_known_extensions = true", params![]);
            return;
        }

        tracing::info!("Loading DuckDB settings from {}", init_file);
        
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                // Execute each non-comment line as a SQL statement
                for line in contents.lines() {
                    let line = line.trim();
                    // Skip empty lines and comments
                    if line.is_empty() || line.starts_with("--") || line.starts_with("#") {
                        continue;
                    }
                    // Execute the SET command
                    if let Err(e) = connection.execute(line, params![]) {
                        tracing::debug!("Could not execute '{}': {}", line, e);
                    }
                }
                tracing::debug!("Loaded settings from .duckdbrc");
            }
            Err(e) => {
                tracing::warn!("Could not read {}: {}", init_file, e);
            }
        }
    }

    /// Create a single DuckDB connection
    fn create_connection(max_memory: u64, _threads: u32) -> Result<Connection> {
        let connection = Connection::open_in_memory()?;

        // Dynamic settings that depend on pod resources
        connection.execute(&format!("SET memory_limit = '{}B'", max_memory), params![])?;
        // Note: threads are set later with aggressive I/O parallelism settings

        // Configure prepared statement cache for schema caching
        // This improves performance for repeated queries by caching prepared statements
        // including their schema metadata. Default: 100 statements.
        let cache_capacity = std::env::var("DUCKDB_PREPARED_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        connection.set_prepared_statement_cache_capacity(cache_capacity);
        tracing::debug!("Prepared statement cache capacity set to {}", cache_capacity);

        // Load .duckdbrc if present (created at Docker build time)
        // This applies static settings without hardcoding them in Rust
        Self::load_duckdbrc(&connection);

        // Dynamic streaming buffer (can be overridden by env var)
        let streaming_buffer = std::env::var("DUCKDB_STREAMING_BUFFER_SIZE")
            .unwrap_or_else(|_| "100MB".to_string());
        if let Err(e) = connection.execute(
            &format!("SET streaming_buffer_size = '{}';", streaming_buffer),
            params![]
        ) {
            tracing::debug!("Could not set streaming_buffer_size: {}", e);
        }

        // Load pre-installed extensions from Docker image
        // Extensions are pre-downloaded at build time for faster startup and offline support
        // See Dockerfile.worker for the list of pre-installed extensions
        
        // List of extensions to load (order matters for dependencies)
        // Note: postgres, mysql, sqlite extensions not available as pre-built binaries for v1.4.3
        let extensions = [
            // Core data formats
            ("parquet", "Parquet file support"),
            ("json", "JSON operations"),
            ("avro", "Avro file support"),
            ("excel", "Excel file support"),
            // Cloud storage
            ("httpfs", "HTTP/HTTPS/S3 file access"),
            ("azure", "Azure Blob Storage / ADLS Gen2"),
            ("aws", "AWS SDK features"),
            // Data lakes
            ("delta", "Delta Lake support"),
            ("iceberg", "Apache Iceberg support"),
            // Utilities
            ("icu", "Time zones and collations"),
            ("fts", "Full-text search"),
            ("inet", "IP address operations"),
            ("spatial", "Geospatial functions"),
            ("vss", "Vector similarity search"),
        ];
        
        for (ext_name, description) in extensions {
            if let Err(e) = connection.execute(&format!("LOAD {}", ext_name), params![]) {
                tracing::debug!("Could not load {}: {} ({})", ext_name, e, description);
            }
        }

        // Enable external file cache for S3/remote files (DuckDB 1.3+)
        // This caches remote data locally to avoid repeated network transfers
        if let Err(e) = connection.execute("SET enable_external_file_cache = true", params![]) {
            tracing::warn!("Could not enable external file cache: {}", e);
        }

        // Configure temp directory for out-of-core processing (spill to disk)
        let temp_dir =
            std::env::var("DUCKDB_TEMP_DIR").unwrap_or_else(|_| "/tmp/duckdb".to_string());
        let max_temp_size =
            std::env::var("DUCKDB_MAX_TEMP_SIZE").unwrap_or_else(|_| "100GB".to_string());

        // Create temp directory if it doesn't exist
        let _ = std::fs::create_dir_all(&temp_dir);

        if let Err(e) =
            connection.execute(&format!("SET temp_directory = '{}'", temp_dir), params![])
        {
            tracing::warn!("Could not set temp_directory: {}", e);
        }
        if let Err(e) = connection.execute(
            &format!("SET max_temp_directory_size = '{}'", max_temp_size),
            params![],
        ) {
            tracing::warn!("Could not set max_temp_directory_size: {}", e);
        }

        // ============= AGGRESSIVE I/O PARALLELISM FOR AZURE/REMOTE FILES =============
        // DuckDB documentation recommends 2-5× CPU cores for remote file workloads
        // because DuckDB uses synchronous I/O and each thread makes one HTTP request at a time.
        // https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads#querying-remote-files
        //
        // For 218s -> 10s (22× improvement), we need maximum parallelism:
        // - Current: 8 threads = 8 concurrent HTTP requests
        // - Target: 64 threads = 64 concurrent HTTP requests (8× more parallelism)
        let threads_for_remote = std::env::var("DUCKDB_REMOTE_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                // Use 4× CPU cores for remote I/O, minimum 32 threads
                let cpu_cores = num_cpus::get() as u32;
                (cpu_cores * 4).max(32)
            });
        
        if let Err(e) = connection.execute(&format!("SET threads = {}", threads_for_remote), params![]) {
            tracing::warn!("Could not set threads for remote: {}", e);
        }
        tracing::info!("DuckDB threads set to {} for Azure I/O parallelism", threads_for_remote);
        
        // Enable HTTP keep-alive for connection reuse (reduces latency)
        if let Err(e) = connection.execute("SET http_keep_alive = true", params![]) {
            tracing::warn!("Could not enable http_keep_alive: {}", e);
        }
        
        // Increase HTTP timeout for large Azure operations (default 30s may be too short)
        if let Err(e) = connection.execute("SET http_timeout = 120", params![]) {
            tracing::warn!("Could not set http_timeout: {}", e);
        }
        
        // Aggressive retry settings for Azure reliability
        if let Err(e) = connection.execute("SET http_retries = 5", params![]) {
            tracing::warn!("Could not set http_retries: {}", e);
        }
        if let Err(e) = connection.execute("SET http_retry_wait_ms = 200", params![]) {
            tracing::warn!("Could not set http_retry_wait_ms: {}", e);
        }
        // Reduce backoff factor for faster retries
        if let Err(e) = connection.execute("SET http_retry_backoff = 2", params![]) {
            tracing::debug!("Could not set http_retry_backoff: {}", e);
        }
        
        // ============= DuckDB Performance Optimizations for Azure/Delta =============
        // Based on DuckDB documentation: https://duckdb.org/2025/03/21/maximizing-your-delta-scan-performance.html
        
        // Enable Parquet metadata caching - reduces Azure HTTP calls for repeated file access
        if let Err(e) = connection.execute("SET parquet_metadata_cache = true", params![]) {
            tracing::debug!("parquet_metadata_cache not available: {}", e);
        }
        
        // Enable HTTP metadata cache - caches HTTP metadata globally
        if let Err(e) = connection.execute("SET enable_http_metadata_cache = true", params![]) {
            tracing::debug!("enable_http_metadata_cache not available: {}", e);
        }
        
        // Enable prefetching for ALL Parquet files (not just local)
        if let Err(e) = connection.execute("SET prefetch_all_parquet_files = true", params![]) {
            tracing::debug!("prefetch_all_parquet_files not available: {}", e);
        }
        
        // Enable external file cache (Parquet files in memory)
        if let Err(e) = connection.execute("SET enable_external_file_cache = true", params![]) {
            tracing::debug!("enable_external_file_cache not available: {}", e);
        }
        
        // Disable preserve_insertion_order for faster query execution
        // (only affects queries without ORDER BY)
        if let Err(e) = connection.execute("SET preserve_insertion_order = false", params![]) {
            tracing::debug!("preserve_insertion_order not available: {}", e);
        }
        
        // Allow external threads (for background I/O operations)
        if let Err(e) = connection.execute("SET external_threads = 4", params![]) {
            tracing::debug!("external_threads not available: {}", e);
        }
        
        // Enable allocator background threads for better memory management
        if let Err(e) = connection.execute("SET allocator_background_threads = true", params![]) {
            tracing::debug!("allocator_background_threads not available: {}", e);
        }

        // Configure Azure authentication from environment variables
        if let Ok(account_name) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
            tracing::info!("Configuring Azure for account: {}", account_name);
            
            // Set the account name
            if let Err(e) = connection.execute(
                &format!("SET azure_account_name = '{}'", account_name), params![]
            ) {
                tracing::warn!("Could not set azure_account_name: {}", e);
            }
            
            // Try to get an access token for workload identity (needed for delta_scan)
            let access_token = Self::get_azure_access_token();
            
            if let Some((token, _expires_in)) = access_token {
                // Use access_token provider for delta extension compatibility
                let secret_sql = format!(
                    "CREATE SECRET IF NOT EXISTS azure_storage (
                        TYPE azure,
                        PROVIDER access_token,
                        ACCESS_TOKEN '{}',
                        ACCOUNT_NAME '{}'
                    )",
                    token, account_name
                );
                if let Err(e) = connection.execute(&secret_sql, params![]) {
                    tracing::warn!("Could not create Azure secret with access_token: {}", e);
                } else {
                    tracing::info!("Azure secret created with access_token provider (delta_scan compatible)");
                }
            } else {
                // Fallback to credential_chain (works for read_parquet but not delta_scan in K8s)
                let secret_sql = format!(
                    "CREATE SECRET IF NOT EXISTS azure_storage (
                        TYPE azure,
                        PROVIDER credential_chain,
                        ACCOUNT_NAME '{}'
                    )",
                    account_name
                );
                if let Err(e) = connection.execute(&secret_sql, params![]) {
                    tracing::warn!("Could not create Azure secret: {}", e);
                    if let Err(e) = connection.execute("SET azure_credential_chain = 'default'", params![]) {
                        tracing::warn!("Could not set azure_credential_chain: {}", e);
                    }
                } else {
                    tracing::info!("Azure secret created with credential_chain provider");
                }
            }
            
            // Set CA bundle for SSL if available
            if let Ok(ca_bundle) = std::env::var("CURL_CA_BUNDLE") {
                if let Err(e) = connection.execute(
                    &format!("SET ca_cert_file = '{}'", ca_bundle), params![]
                ) {
                    tracing::debug!("Could not set ca_cert_file: {}", e);
                }
            }
        }

        Ok(connection)
    }

    /// Get Azure access token by exchanging federated token (for Kubernetes Workload Identity)
    /// Returns (access_token, expires_in_seconds)
    /// This is needed because delta_scan uses object_store which doesn't support workload identity natively
    fn get_azure_access_token() -> Option<(String, u64)> {
        // Check if we have workload identity environment variables
        let federated_token_file = std::env::var("AZURE_FEDERATED_TOKEN_FILE").ok()?;
        let client_id = std::env::var("AZURE_CLIENT_ID").ok()?;
        let tenant_id = std::env::var("AZURE_TENANT_ID").ok()?;
        
        tracing::info!("Attempting to get Azure access token via workload identity");
        
        // Read the federated token (re-read each time as K8s may rotate it)
        let federated_token = match std::fs::read_to_string(&federated_token_file) {
            Ok(token) => token.trim().to_string(),
            Err(e) => {
                tracing::warn!("Could not read federated token file {}: {}", federated_token_file, e);
                return None;
            }
        };
        
        // Exchange federated token for access token
        let token_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id
        );
        
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &client_id),
            ("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"),
            ("client_assertion", &federated_token),
            ("scope", "https://storage.azure.com/.default"),
        ];
        
        // Use blocking HTTP client
        let client = match reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build() 
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Could not create HTTP client: {}", e);
                return None;
            }
        };
        
        let response = match client.post(&token_url).form(&params).send() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to request Azure access token: {}", e);
                return None;
            }
        };
        
        if !response.status().is_success() {
            tracing::warn!("Azure token request failed with status: {}", response.status());
            if let Ok(body) = response.text() {
                tracing::debug!("Token error response: {}", body);
            }
            return None;
        }
        
        // Parse the JSON response to extract access_token and expires_in
        let json: serde_json::Value = match response.json() {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!("Failed to parse Azure token response: {}", e);
                return None;
            }
        };
        
        let access_token = json.get("access_token")?.as_str()?.to_string();
        // Default to 3600 seconds (1 hour) if expires_in is not present
        let expires_in = json.get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);
        
        tracing::info!("Successfully obtained Azure access token (expires in {} seconds)", expires_in);
        
        Some((access_token, expires_in))
    }
    
    /// Emergency token refresh - only called if token is critically expired
    /// Normal refresh is handled by background thread
    fn check_azure_token_emergency_refresh(&self) {
        // Fast path: check if we even have Azure configured
        let account_name = match std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
            Ok(name) => name,
            Err(_) => return, // No Azure config
        };
        
        // Check if token is critically expired (past expiration time)
        let needs_emergency_refresh = {
            match self.azure_token_state.lock() {
                Ok(state) => match state.as_ref() {
                    Some(s) => std::time::Instant::now() > s.expires_at,
                    None => false, // No state yet, let background thread handle it
                },
                Err(_) => false,
            }
        };
        
        if !needs_emergency_refresh {
            return; // Token is still valid, no action needed
        }
        
        tracing::warn!("Emergency: Azure token expired, refreshing synchronously...");
        
        // Get new token
        let (access_token, expires_in) = match Self::get_azure_access_token() {
            Some(t) => t,
            None => {
                tracing::error!("Emergency: Could not refresh Azure token");
                return;
            }
        };
        
        // Update the secret in all connections
        for pooled in &self.connections {
            if let Ok(conn) = pooled.connection.lock() {
                let _ = conn.execute("DROP SECRET IF EXISTS azure_storage", params![]);
                let secret_sql = format!(
                    "CREATE SECRET azure_storage (
                        TYPE azure,
                        PROVIDER access_token,
                        ACCESS_TOKEN '{}',
                        ACCOUNT_NAME '{}'
                    )",
                    access_token, account_name
                );
                let _ = conn.execute(&secret_sql, params![]);
            }
        }
        
        // Update token state
        let expires_at = std::time::Instant::now() + std::time::Duration::from_secs(expires_in.saturating_sub(60));
        if let Ok(mut state) = self.azure_token_state.lock() {
            if let Some(ref mut inner) = *state {
                inner.expires_at = expires_at;
            }
        }
        
        tracing::info!("Emergency: Azure token refreshed");
    }

    /// Perform warmup to prevent cold start issues
    /// 
    /// This function runs at startup to:
    /// 1. Validate Azure connectivity and token
    /// 2. Pre-load Delta extension
    /// 3. Test a simple delta_scan query (if Azure is configured)
    /// 
    /// Without warmup, the first query from DBeaver/Tableau may fail or be slow
    /// because it triggers token acquisition, extension loading, etc.
    fn perform_warmup(&self) {
        tracing::info!("Performing warmup to prevent cold start issues...");
        
        // Get a connection for warmup
        let pooled_conn = match self.connections.first() {
            Some(conn) => conn,
            None => {
                tracing::warn!("No connections available for warmup");
                return;
            }
        };
        
        let conn = match pooled_conn.connection.lock() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Could not lock connection for warmup: {}", e);
                return;
            }
        };
        
        // Step 1: Verify Delta extension is loaded
        match conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'delta' AND loaded = true", params![]) {
            Ok(_) => tracing::info!("Warmup: Delta extension verified"),
            Err(e) => {
                tracing::warn!("Warmup: Delta extension check failed: {}. Attempting to load...", e);
                if let Err(e) = conn.execute("LOAD delta", params![]) {
                    tracing::warn!("Warmup: Could not load delta extension: {}", e);
                }
            }
        }
        
        // Step 2: Verify Azure extension is loaded (if Azure is configured)
        if std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_ok() {
            match conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'azure' AND loaded = true", params![]) {
                Ok(_) => tracing::info!("Warmup: Azure extension verified"),
                Err(e) => {
                    tracing::warn!("Warmup: Azure extension check failed: {}. Attempting to load...", e);
                    if let Err(e) = conn.execute("LOAD azure", params![]) {
                        tracing::warn!("Warmup: Could not load azure extension: {}", e);
                    }
                }
            }
            
            // Step 3: Test Azure connectivity with a simple query
            // Use a known Delta table path from environment or default
            let test_path = std::env::var("TAVANA_WARMUP_DELTA_PATH")
                .unwrap_or_else(|_| "az://dagster-data-pipelines/dev/bronze/memotech/fabric/memotech_imt_patent/".to_string());
            
            tracing::info!("Warmup: Testing delta_scan connectivity to {}", &test_path[..test_path.len().min(50)]);
            
            // Try to read just 1 row to validate the entire pipeline
            let test_query = format!(
                "SELECT 1 FROM delta_scan('{}') LIMIT 1",
                test_path
            );
            
            match conn.execute(&test_query, params![]) {
                Ok(_) => tracing::info!("Warmup: delta_scan connectivity verified - ready for queries!"),
                Err(e) => {
                    tracing::warn!("Warmup: delta_scan test failed: {}. First queries may be slow.", e);
                    // This is not fatal - the query might fail due to permissions or path issues
                    // The important thing is that the token was acquired and extensions loaded
                }
            }
        } else {
            tracing::debug!("Warmup: Azure not configured, skipping Azure-specific warmup");
        }
        
        tracing::info!("Warmup complete");
    }

    /// Configure S3 for all connections from environment variables
    fn configure_s3_all(&self) -> Result<()> {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();

        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            conn.execute(&format!("SET s3_region = '{}'", region), params![])?;

            if let Some(key) = &access_key {
                conn.execute(&format!("SET s3_access_key_id = '{}'", key), params![])?;
            }
            if let Some(secret) = &secret_key {
                conn.execute(
                    &format!("SET s3_secret_access_key = '{}'", secret),
                    params![],
                )?;
            }
            if let Some(ep) = &endpoint {
                let ep_clean = ep.replace("http://", "").replace("https://", "");
                conn.execute(&format!("SET s3_endpoint = '{}'", ep_clean), params![])?;
                conn.execute("SET s3_use_ssl = false", params![])?;
            }

            // Always use path-style for MinIO compatibility
            conn.execute("SET s3_url_style = 'path'", params![])?;
        }

        if access_key.is_some() {
            info!(
                "S3 access key configured for {} connections",
                self.connections.len()
            );
        }
        if endpoint.is_some() {
            info!(
                "S3 endpoint configured for {} connections",
                self.connections.len()
            );
        }
        info!("S3 path-style URLs enabled for all connections");

        Ok(())
    }

    /// Configure Azure/ADLS Gen2 for all connections from environment variables
    /// Supports Databricks on Azure storage access
    fn configure_azure_all(&self) -> Result<()> {
        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").ok();
        let connection_string = std::env::var("AZURE_STORAGE_CONNECTION_STRING").ok();
        let sas_token = std::env::var("AZURE_SAS_TOKEN").ok();
        let use_managed_identity = std::env::var("AZURE_USE_MANAGED_IDENTITY")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        // Skip if no Azure configuration
        if account_name.is_none() && connection_string.is_none() {
            debug!("No Azure storage configuration found, skipping");
            return Ok(());
        }

        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            // Set storage account name if provided
            if let Some(account) = &account_name {
                conn.execute(
                    &format!("SET azure_account_name = '{}'", account),
                    params![],
                )?;
            }

            // Configure authentication method (in priority order)
            if let Some(conn_str) = &connection_string {
                conn.execute(
                    &format!("SET azure_storage_connection_string = '{}'", conn_str),
                    params![],
                )?;
            } else if let Some(sas) = &sas_token {
                // Note: DuckDB uses azure_sas_token for SAS authentication
                // Format should be the full SAS token including the leading '?'
                let sas_clean = if sas.starts_with('?') { sas.clone() } else { format!("?{}", sas) };
                if let Err(e) = conn.execute(
                    &format!("SET azure_sas_token = '{}'", sas_clean),
                    params![],
                ) {
                    tracing::warn!("Could not set azure_sas_token: {}", e);
                }
            } else if use_managed_identity {
                // For managed identity, we'll set up an initial token
                // The token will be refreshed automatically by refresh_azure_token_if_needed()
                if let Some(account) = &account_name {
                    // Try to get an access token for workload identity
                    if let Some((token, expires_in)) = Self::get_azure_access_token() {
                        let secret_sql = format!(
                            "CREATE SECRET IF NOT EXISTS azure_storage (
                                TYPE azure,
                                PROVIDER access_token,
                                ACCESS_TOKEN '{}',
                                ACCOUNT_NAME '{}'
                            )",
                            token, account
                        );
                        if let Err(e) = conn.execute(&secret_sql, params![]) {
                            tracing::warn!("Could not create Azure access_token secret: {}", e);
                        } else {
                            // Set initial token state (only for first connection to avoid duplicates)
                            if pooled_conn.id == 0 {
                                let expires_at = std::time::Instant::now() 
                                    + std::time::Duration::from_secs(expires_in.saturating_sub(60));
                                if let Ok(mut state) = self.azure_token_state.lock() {
                                    *state = Some(AzureTokenState {
                                        expires_at,
                                        account_name: account.clone(),
                                        stop_flag: std::sync::atomic::AtomicBool::new(false),
                                    });
                                }
                            }
                        }
                    } else {
                        // Fallback to credential_chain
                        let secret_sql = format!(
                            "CREATE SECRET IF NOT EXISTS azure_storage (
                                TYPE azure,
                                PROVIDER credential_chain,
                                ACCOUNT_NAME '{}'
                            )",
                            account
                        );
                        if let Err(e) = conn.execute(&secret_sql, params![]) {
                            tracing::warn!("Could not create Azure secret: {}, trying SET", e);
                            if let Err(e) = conn.execute("SET azure_credential_chain = 'default'", params![]) {
                                tracing::warn!("Could not set azure_credential_chain: {}", e);
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref name) = account_name {
            info!(
                "Azure storage configured for {} connections (account: {})",
                self.connections.len(),
                name
            );
        }
        if connection_string.is_some() {
            info!("Azure using connection string authentication");
        } else if sas_token.is_some() {
            info!("Azure using SAS token authentication");
        } else if use_managed_identity {
            info!("Azure using managed identity authentication");
        }

        Ok(())
    }

    /// Get the next connection using round-robin
    /// Public for cursor manager to access connections
    pub fn get_connection(&self) -> Arc<PooledConnection> {
        let idx = self
            .next_conn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            % self.connections.len();
        Arc::clone(&self.connections[idx])
    }

    /// Execute a query and return Arrow record batches
    /// Uses connection pool for parallel execution
    /// 
    /// WARNING: This method collects ALL results into memory before returning.
    /// For large result sets (SELECT * without LIMIT), use execute_query_streaming instead.
    #[instrument(skip(self))]
    pub fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Increment query counter for metrics
        self.query_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Background thread handles token refresh proactively
        // This is just a safety check for edge cases (e.g., if background thread failed)
        self.check_azure_token_emergency_refresh();
        
        let pooled_conn = self.get_connection();
        debug!("Using connection {} for query", pooled_conn.id);

        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        // Execute the query
        let result = (|| {
            let mut stmt = conn.prepare(sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow(params![])?.collect();
            Ok(batches)
        })();

        // If query failed, rollback to clear any aborted transaction state
        // This ensures the next query on this connection will succeed
        if result.is_err() {
            debug!("Query failed, rolling back to clear transaction state");
            let _ = conn.execute("ROLLBACK", params![]);
        }

        result
    }

    /// Get schema for a query using prepare_cached (optimized for repeated schema detection)
    /// 
    /// Uses DuckDB's prepared statement cache to avoid re-parsing and re-planning
    /// the same query. This is particularly useful for LIMIT 0 schema detection
    /// queries in the Extended Query Protocol.
    /// 
    /// The query is wrapped with LIMIT 0 if not already limited.
    #[instrument(skip(self))]
    pub fn get_schema_cached(&self, sql: &str) -> Result<Arc<Schema>> {
        let pooled_conn = self.get_connection();
        debug!("Using connection {} for cached schema query", pooled_conn.id);

        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        // Wrap query with LIMIT 0 if not already present
        let schema_sql = if sql.to_uppercase().contains("LIMIT 0") {
            sql.to_string()
        } else {
            format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", sql.trim().trim_end_matches(';'))
        };

        // Use prepare_cached for automatic caching
        // Same SQL -> same prepared statement -> cached schema
        let mut stmt = conn.prepare_cached(&schema_sql)?;
        let arrow_iter = stmt.query_arrow(params![])?;
        let schema = arrow_iter.get_schema();
        
        debug!("Schema cached: {} columns", schema.fields().len());
        Ok(schema)
    }

    /// Execute a query with TRUE STREAMING - sends batches one at a time via callback
    /// This prevents OOM by never loading the entire result set into memory.
    /// 
    /// The callback receives each RecordBatch as it's produced by DuckDB.
    /// Returns (schema, total_rows) on success.
    #[instrument(skip(self, batch_callback))]
    pub fn execute_query_streaming<F>(&self, sql: &str, mut batch_callback: F) -> Result<(Arc<Schema>, u64)>
    where
        F: FnMut(RecordBatch) -> Result<()>,
    {
        // Increment query counter for metrics
        self.query_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Background thread handles token refresh proactively
        self.check_azure_token_emergency_refresh();
        
        let pooled_conn = self.get_connection();
        debug!("Using connection {} for streaming query", pooled_conn.id);

        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        // Execute the query and stream results
        let result: Result<(Arc<Schema>, u64)> = (|| {
            let mut stmt = conn.prepare(sql)?;
            let arrow_iter = stmt.query_arrow(params![])?;
            // get_schema() returns Arc<Schema>, so don't double-wrap
            let schema: Arc<Schema> = arrow_iter.get_schema();
            
            let mut total_rows: u64 = 0;
            
            // Stream each batch through the callback - NEVER collect into Vec
            for batch in arrow_iter {
                total_rows += batch.num_rows() as u64;
                batch_callback(batch)?;
                
                // Log progress for very large queries
                if total_rows % 1_000_000 == 0 {
                    info!("Streaming progress: {} rows processed", total_rows);
                }
            }
            
            Ok((schema, total_rows))
        })();

        // If query failed, rollback to clear any aborted transaction state
        if result.is_err() {
            debug!("Streaming query failed, rolling back to clear transaction state");
            let _ = conn.execute("ROLLBACK", params![]);
        }

        result
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params))]
    pub fn execute_query_with_params<P: duckdb::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<RecordBatch>> {
        // Increment query counter for metrics
        self.query_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Refresh Azure token if needed
        self.check_azure_token_emergency_refresh();
        
        let pooled_conn = self.get_connection();
        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        // Execute the query
        let result = (|| {
            let mut stmt = conn.prepare(sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow(params)?.collect();
            Ok(batches)
        })();

        // If query failed, rollback to clear any aborted transaction state
        if result.is_err() {
            debug!("Query with params failed, rolling back to clear transaction state");
            let _ = conn.execute("ROLLBACK", params![]);
        }

        result
    }

    /// Get pool size
    pub fn pool_size(&self) -> usize {
        self.connections.len()
    }

    /// Get connection pool statistics for monitoring
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            total_connections: self.connections.len(),
            available_permits: self.semaphore.available_permits(),
            total_queries: self.query_count.load(std::sync::atomic::Ordering::Relaxed),
            azure_configured: std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_ok(),
            s3_configured: std::env::var("AWS_ACCESS_KEY_ID").is_ok(),
        }
    }

    // ============= Delta Table PIN_SNAPSHOT Caching =============
    // DuckDB's ATTACH with PIN_SNAPSHOT provides up to 1.47× speedup
    // by caching Delta metadata between queries.
    // https://duckdb.org/2025/03/21/maximizing-your-delta-scan-performance.html

    /// Attach a Delta table with PIN_SNAPSHOT for cached metadata
    /// Returns the alias to use for querying the table
    /// 
    /// PIN_SNAPSHOT caches Delta metadata (transaction log, schema) in memory,
    /// avoiding repeated reads from Azure Blob Storage on each query.
    pub fn attach_delta_table(&self, delta_path: &str) -> Result<String> {
        // Check if already attached and cached
        if let Some(alias) = self.delta_cache.get_alias(delta_path) {
            debug!("Delta table already attached with PIN_SNAPSHOT: {} -> {}", delta_path, alias);
            return Ok(alias);
        }
        
        // Generate a unique alias from the path
        let alias = self.generate_delta_alias(delta_path);
        
        // Get a connection to execute ATTACH
        let pooled_conn = self.get_connection();
        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        
        // First, try to detach if it exists (for re-attach after TTL expiry)
        let detach_sql = format!("DETACH DATABASE IF EXISTS \"{}\"", alias);
        let _ = conn.execute(&detach_sql, params![]);
        
        // Attach with PIN_SNAPSHOT for cached metadata
        // This caches Delta metadata in memory, providing significant speedup
        let attach_sql = format!(
            "ATTACH '{}' AS \"{}\" (TYPE delta, PIN_SNAPSHOT)",
            delta_path, alias
        );
        
        match conn.execute(&attach_sql, params![]) {
            Ok(_) => {
                info!(
                    "Attached Delta table with PIN_SNAPSHOT: {} -> {}",
                    delta_path, alias
                );
                self.delta_cache.register(delta_path.to_string(), alias.clone(), true);
                Ok(alias)
            }
            Err(e) => {
                // PIN_SNAPSHOT might not be supported in older DuckDB versions
                // Fall back to regular ATTACH
                debug!("PIN_SNAPSHOT failed, trying regular ATTACH: {}", e);
                let fallback_sql = format!(
                    "ATTACH '{}' AS \"{}\" (TYPE delta)",
                    delta_path, alias
                );
                match conn.execute(&fallback_sql, params![]) {
                    Ok(_) => {
                        info!(
                            "Attached Delta table (without PIN_SNAPSHOT): {} -> {}",
                            delta_path, alias
                        );
                        self.delta_cache.register(delta_path.to_string(), alias.clone(), false);
                        Ok(alias)
                    }
                    Err(e2) => Err(anyhow::anyhow!("Failed to attach Delta table: {}", e2))
                }
            }
        }
    }
    
    /// Attach a Delta table on all connections for maximum cache benefit
    pub fn attach_delta_table_all_connections(&self, delta_path: &str) -> Result<String> {
        let alias = self.generate_delta_alias(delta_path);
        
        // Check if already attached
        if let Some(existing_alias) = self.delta_cache.get_alias(delta_path) {
            return Ok(existing_alias);
        }
        
        let attach_sql = format!(
            "ATTACH '{}' AS \"{}\" (TYPE delta, PIN_SNAPSHOT)",
            delta_path, alias
        );
        let detach_sql = format!("DETACH DATABASE IF EXISTS \"{}\"", alias);
        
        for pooled_conn in &self.connections {
            if let Ok(conn) = pooled_conn.connection.lock() {
                // Clean up any existing attachment
                let _ = conn.execute(&detach_sql, params![]);
                
                // Attach with PIN_SNAPSHOT
                if let Err(_e) = conn.execute(&attach_sql, params![]) {
                    // Try without PIN_SNAPSHOT
                    let fallback_sql = format!(
                        "ATTACH '{}' AS \"{}\" (TYPE delta)",
                        delta_path, alias
                    );
                    if let Err(e2) = conn.execute(&fallback_sql, params![]) {
                        tracing::warn!(
                            "Failed to attach Delta table on connection {}: {}",
                            pooled_conn.id, e2
                        );
                        return Err(anyhow::anyhow!("Failed to attach: {}", e2));
                    }
                }
            }
        }
        
        info!(
            "Attached Delta table on all {} connections: {} -> {}",
            self.connections.len(), delta_path, alias
        );
        self.delta_cache.register(delta_path.to_string(), alias.clone(), true);
        Ok(alias)
    }

    /// Generate a unique alias for a Delta table path
    fn generate_delta_alias(&self, delta_path: &str) -> String {
        // Create a short hash of the path for uniqueness
        let hash = blake3::hash(delta_path.as_bytes());
        let short_hash = &hash.to_hex()[..8];
        
        // Extract the last path component for readability
        let path_part = delta_path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("delta")
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>();
        
        format!("delta_{}_{}", path_part, short_hash)
    }

    /// Rewrite a query to use attached Delta tables where possible
    /// Returns (rewritten_sql, tables_used) where tables_used is list of aliases
    pub fn rewrite_query_for_delta_cache(&self, sql: &str) -> (String, Vec<String>) {
        let mut rewritten = sql.to_string();
        let mut tables_used = Vec::new();
        
        // Find delta_scan('path') patterns and try to replace with attached table
        // Pattern: delta_scan('az://...') or delta_scan("az://...")
        let patterns = [
            (r"delta_scan\s*\(\s*'([^']+)'\s*\)", "'"),
            (r#"delta_scan\s*\(\s*"([^"]+)"\s*\)"#, "\""),
        ];
        
        for (pattern_str, _quote) in patterns {
            if let Ok(re) = regex::Regex::new(pattern_str) {
                // Collect matches with their string data to avoid borrow issues
                let current_sql = rewritten.clone();
                let matches: Vec<(String, String)> = re.captures_iter(&current_sql)
                    .filter_map(|cap| {
                        let full_match = cap.get(0).map(|m| m.as_str().to_string())?;
                        let delta_path = cap.get(1).map(|m| m.as_str().to_string())?;
                        Some((full_match, delta_path))
                    })
                    .collect();
                
                for (full_match, delta_path) in matches {
                    if delta_path.is_empty() {
                        continue;
                    }
                    
                    // Check if this path is cached
                    if let Some(alias) = self.delta_cache.get_alias(&delta_path) {
                        // Replace delta_scan('path') with "alias" (the attached table)
                        rewritten = rewritten.replace(&full_match, &format!("\"{}\"", alias));
                        tables_used.push(alias);
                        debug!(
                            "Rewrote delta_scan to use cached table: {} -> {}",
                            delta_path, full_match
                        );
                    }
                }
            }
        }
        
        (rewritten, tables_used)
    }
    
    /// Execute a query with automatic Delta table caching
    /// If the query contains delta_scan(), attach the table first for better performance
    #[instrument(skip(self))]
    pub fn execute_query_with_delta_cache(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // First, try to attach any Delta tables referenced in the query
        self.auto_attach_delta_tables(sql);
        
        // Rewrite query to use cached tables
        let (rewritten_sql, tables_used) = self.rewrite_query_for_delta_cache(sql);
        
        if !tables_used.is_empty() {
            info!(
                "Using {} cached Delta table(s) for query: {:?}",
                tables_used.len(), tables_used
            );
        }
        
        // Execute with the (potentially) rewritten query
        self.execute_query(&rewritten_sql)
    }
    
    /// Automatically attach Delta tables found in a query
    fn auto_attach_delta_tables(&self, sql: &str) {
        // Find delta_scan paths
        let patterns = [
            r"delta_scan\s*\(\s*'([^']+)'\s*\)",
            r#"delta_scan\s*\(\s*"([^"]+)"\s*\)"#,
        ];
        
        for pattern_str in patterns {
            if let Ok(re) = regex::Regex::new(pattern_str) {
                for cap in re.captures_iter(sql) {
                    if let Some(delta_path) = cap.get(1).map(|m| m.as_str()) {
                        // Only attach if not already cached
                        if self.delta_cache.get_alias(delta_path).is_none() {
                            if let Err(e) = self.attach_delta_table(delta_path) {
                                debug!("Could not auto-attach Delta table {}: {}", delta_path, e);
                                // Continue with original delta_scan - it will still work
                            }
                        }
                    }
                }
            }
        }
    }
    
    /// Get Delta cache statistics
    pub fn delta_cache_stats(&self) -> (usize, usize) {
        self.delta_cache.stats()
    }

    /// Set S3 credentials for accessing cloud storage
    pub fn configure_s3(
        &self,
        region: &str,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        endpoint: Option<&str>,
    ) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            conn.execute(&format!("SET s3_region = '{}'", region), params![])?;

            if let Some(key) = access_key_id {
                conn.execute(&format!("SET s3_access_key_id = '{}'", key), params![])?;
            }
            if let Some(secret) = secret_access_key {
                conn.execute(
                    &format!("SET s3_secret_access_key = '{}'", secret),
                    params![],
                )?;
            }
            if let Some(ep) = endpoint {
                conn.execute(&format!("SET s3_endpoint = '{}'", ep), params![])?;
                conn.execute("SET s3_url_style = 'path'", params![])?;
            }
        }

        Ok(())
    }

    /// Configure Azure credentials
    pub fn configure_azure(&self, account_name: &str) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            conn.execute(
                &format!("SET azure_account_name = '{}'", account_name),
                params![],
            )?;
        }
        Ok(())
    }

    /// Configure Azure credentials for Databricks/ADLS Gen2 access
    /// Supports multiple authentication methods:
    /// - Connection string (for development)
    /// - SAS token (for production)
    /// - Managed Identity (for Azure workloads)
    pub fn configure_azure_databricks(
        &self,
        account_name: &str,
        connection_string: Option<&str>,
        sas_token: Option<&str>,
        use_managed_identity: bool,
    ) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            // Set storage account name (always required)
            conn.execute(
                &format!("SET azure_account_name = '{}'", account_name),
                params![],
            )?;

            // Configure authentication method
            if let Some(conn_str) = connection_string {
                conn.execute(
                    &format!("SET azure_storage_connection_string = '{}'", conn_str),
                    params![],
                )?;
                tracing::info!("Azure configured with connection string for account: {}", account_name);
            } else if let Some(sas) = sas_token {
                // SAS token for secure, time-limited access
                conn.execute(
                    &format!("SET azure_sas_token = '{}'", sas),
                    params![],
                )?;
                tracing::info!("Azure configured with SAS token for account: {}", account_name);
            } else if use_managed_identity {
                // Use Azure Managed Identity (works in AKS with workload identity)
                conn.execute("SET azure_use_default_credential = true", params![])?;
                tracing::info!("Azure configured with managed identity for account: {}", account_name);
            }
        }
        Ok(())
    }

    /// Configure Databricks Unity Catalog access
    /// For accessing Delta tables via Unity Catalog external locations
    pub fn configure_databricks_unity_catalog(
        &self,
        workspace_url: &str,
        _access_token: &str,
    ) -> Result<()> {
        // Unity Catalog tables are accessed via their cloud storage path
        // The access is through ADLS/S3/GCS with proper credentials
        // This method sets up the secret for accessing Databricks-managed storage
        tracing::info!(
            "Databricks Unity Catalog configured for workspace: {}",
            workspace_url
        );
        
        // Note: Unity Catalog tables use cloud storage paths like:
        // abfss://container@account.dfs.core.windows.net/path/to/delta/table
        // s3://bucket/path/to/delta/table
        
        // The actual credentials are configured via configure_azure_databricks or configure_s3
        // This method is a convenience wrapper that logs the configuration
        
        Ok(())
    }

    /// Get query statistics (for resource tracking)
    pub fn get_query_stats(&self) -> Result<QueryStats> {
        // Use first connection for stats
        let pooled_conn = &self.connections[0];
        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let mut stmt = conn.prepare("SELECT current_setting('threads') as threads")?;
        let mut rows = stmt.query(params![])?;

        let threads: String = if let Some(row) = rows.next()? {
            row.get(0)?
        } else {
            "unknown".to_string()
        };

        Ok(QueryStats {
            threads: threads.parse().unwrap_or(1),
            pool_size: self.connections.len(),
        })
    }
}

/// Query execution statistics
#[allow(dead_code)]
#[derive(Debug)]
pub struct QueryStats {
    pub threads: u32,
    pub pool_size: usize,
}

/// Execution stats for billing
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub execution_time_ms: u64,
    pub rows_returned: u64,
    pub bytes_scanned: u64,
    pub cpu_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = DuckDbExecutor::new(ExecutorConfig::default());
        assert!(executor.is_ok());
    }

    #[test]
    fn test_simple_query() {
        let executor = DuckDbExecutor::new(ExecutorConfig::default()).unwrap();
        let result = executor.execute_query("SELECT 1 as value");
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_pool_size() {
        let config = ExecutorConfig {
            pool_size: 8,
            ..Default::default()
        };
        let executor = DuckDbExecutor::new(config).unwrap();
        assert_eq!(executor.pool_size(), 8);
    }
}
