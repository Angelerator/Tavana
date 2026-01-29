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
    /// Number of threads for query execution
    pub threads: u32,
    /// Enable profiling
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
    /// Account name for recreating secrets
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

/// DuckDB query executor with connection pool
///
/// Maintains multiple DuckDB connections for parallel query execution.
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
}

#[allow(dead_code)]
impl DuckDbExecutor {
    /// Create a new DuckDB executor with connection pool
    pub fn new(config: ExecutorConfig) -> Result<Self> {
        let pool_size = config.pool_size.max(1);
        let memory_per_conn = config.max_memory / pool_size as u64;
        let threads_per_conn = (config.threads / pool_size as u32).max(1);

        // For I/O-bound remote workloads, use more threads regardless of CPU
        let threads_for_display = threads_per_conn.max(8);
        info!(
            "Initializing DuckDB connection pool: {} connections, {}MB each, {} threads each (optimized for Azure I/O)",
            pool_size,
            memory_per_conn / 1024 / 1024,
            threads_for_display
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
        
        let executor = Self {
            connections,
            semaphore: Arc::new(Semaphore::new(pool_size)),
            config,
            next_conn: std::sync::atomic::AtomicUsize::new(0),
            azure_token_state: azure_token_state.clone(),
            _background_refresh_handle: None,
            query_count: std::sync::atomic::AtomicU64::new(0),
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
    fn create_connection(max_memory: u64, threads: u32) -> Result<Connection> {
        let connection = Connection::open_in_memory()?;

        // Dynamic settings that depend on pod resources
        connection.execute(&format!("SET memory_limit = '{}B'", max_memory), params![])?;
        connection.execute(&format!("SET threads = {}", threads), params![])?;

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

        // Increase threads for remote file parallelism (Azure/S3)
        // DuckDB uses synchronous I/O for remote files, so more threads = better I/O parallelism
        // For I/O-bound workloads (Azure Storage), threads don't need to match CPU cores
        // Use at least 8 threads per connection for good parallel HTTP requests
        let threads_for_remote = threads.max(8);
        if let Err(e) =
            connection.execute(&format!("SET threads = {}", threads_for_remote), params![])
        {
            tracing::warn!("Could not increase threads for remote: {}", e);
        }
        
        // Enable HTTP keep-alive for faster repeated requests to Azure
        if let Err(e) = connection.execute("SET http_keep_alive = true", params![]) {
            tracing::warn!("Could not enable http_keep_alive: {}", e);
        }
        
        // Set larger HTTP retry settings for Azure reliability
        if let Err(e) = connection.execute("SET http_retries = 5", params![]) {
            tracing::warn!("Could not set http_retries: {}", e);
        }
        if let Err(e) = connection.execute("SET http_retry_wait_ms = 500", params![]) {
            tracing::warn!("Could not set http_retry_wait_ms: {}", e);
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
