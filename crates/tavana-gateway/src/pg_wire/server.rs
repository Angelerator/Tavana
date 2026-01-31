//! PostgreSQL wire protocol implementation with streaming support
//!
//! Enables Tableau, PowerBI, DBeaver and other BI tools to connect natively.
//! Uses true row-by-row streaming to handle unlimited result sizes without OOM.
//!
//! KEY FEATURE: True FIFO queuing with K8s capacity awareness
//! - Queries are ALWAYS accepted (never rejected)
//! - Enqueued queries wait until K8s capacity is available
//! - Queue depth signals HPA to scale up
//!
//! TLS SUPPORT:
//! - Accepts both SSL and non-SSL connections
//! - Self-signed certificates for development
//! - Custom certificates for production
//!
//! ARCHITECTURE (v1.0.64):
//! - Unified generic handler for TLS and non-TLS (reduces code duplication)
//! - True streaming portal state using channels (not buffered arrays)
//! - Query timeout enforcement via tokio::time::timeout
//! - BufWriter for 2-5x write throughput improvement
//! - Config-driven tuning (no magic hardcoded values)

use crate::auth::{AuthService, AuthGateway, AuthContext, AuthenticatedPrincipal};
use crate::errors::classify_error;
use crate::metrics;
use crate::pg_compat;
use crate::pg_wire::backpressure::{
    BackpressureConfig, BackpressureWriter,
    build_row_description, build_command_complete, build_data_row,
    is_disconnect_error,
};
use crate::query_queue::{QueryOutcome, QueryQueue};
use crate::query_router::{QueryRouter, QueryTarget};
use crate::redis_queue::{RedisQueue, RedisQueueConfig};
use crate::smart_scaler::SmartScaler;
use crate::tls_config::TlsConfig;
use crate::worker_client::{StreamingBatch, StreamingResult, WorkerClient, WorkerClientPool};
use crate::worker_pool::WorkerPoolManager;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tavana_common::proto;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

// ===== Server-Side Cursor Support =====
// Cursor management extracted to `cursors` module for cleaner organization
// See: crates/tavana-gateway/src/cursors.rs
use crate::cursors::{self, ConnectionCursors, CursorResult};

// ===== Configuration (Environment-driven, no hardcoded values) =====

/// PostgreSQL wire protocol configuration
/// All values can be overridden via environment variables
#[derive(Debug, Clone)]
pub struct PgWireConfig {
    /// Maximum rows to buffer before flushing to client (backpressure control)
    /// DEPRECATED: Use flush_threshold_rows for bytes-aware backpressure
    pub streaming_batch_size: usize,
    /// Query timeout in seconds (prevents runaway queries)
    pub query_timeout_secs: u64,
    /// Socket write buffer size in bytes for BufWriter
    /// Smaller = more backpressure, higher latency
    /// Larger = less backpressure, risk of overwhelming client
    pub write_buffer_size: usize,
    /// Maximum message size for gRPC (1GB default)
    #[allow(dead_code)]
    pub max_grpc_message_size: usize,
    /// Worker connection timeout in seconds
    #[allow(dead_code)]
    pub worker_connect_timeout_secs: u64,
    /// Worker query timeout in seconds
    #[allow(dead_code)]
    pub worker_query_timeout_secs: u64,
    /// TCP keepalive time in seconds (detects dead connections)
    pub tcp_keepalive_secs: u64,
    /// Interval to check client connection health during streaming (in rows)
    pub connection_check_interval_rows: usize,
    /// Maximum rows to return in a single query result (like ClickHouse max_result_rows)
    /// This prevents client OOM by limiting result size. Use OFFSET for pagination.
    /// Set to 0 for unlimited (not recommended for production).
    /// Default: 100000 rows (protects JDBC clients from OOM)
    pub max_result_rows: usize,

    // === Backpressure Settings ===
    
    /// Bytes to buffer before forcing a flush (default: 64KB)
    /// This is the primary backpressure threshold, matching TCP window sizes
    pub flush_threshold_bytes: usize,
    /// Maximum rows before forcing a flush regardless of bytes (default: 100)
    /// Acts as a safety net for rows with very small data
    pub flush_threshold_rows: usize,
    /// Timeout for flush operations in seconds (default: 5s)
    /// If flush takes longer, client is likely overwhelmed
    pub flush_timeout_secs: u64,
    /// Maximum rows to buffer in portal state (default: 50000)
    /// Prevents gateway OOM for large result sets with JDBC scrolling
    /// If result exceeds this, client must use DECLARE CURSOR / FETCH
    pub max_portal_buffer_rows: usize,
}

impl Default for PgWireConfig {
    fn default() -> Self {
        Self {
            streaming_batch_size: std::env::var("TAVANA_STREAMING_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            query_timeout_secs: std::env::var("TAVANA_QUERY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600), // 10 minutes - aligned with worker
            write_buffer_size: std::env::var("TAVANA_WRITE_BUFFER_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(32 * 1024), // 32KB - smaller for better backpressure
            max_grpc_message_size: std::env::var("TAVANA_MAX_GRPC_MESSAGE_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024 * 1024 * 1024), // 1GB
            worker_connect_timeout_secs: std::env::var("TAVANA_WORKER_CONNECT_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            worker_query_timeout_secs: std::env::var("TAVANA_WORKER_QUERY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800), // 30 minutes for large queries
            tcp_keepalive_secs: std::env::var("TAVANA_TCP_KEEPALIVE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10), // 10 seconds - fast dead connection detection
            connection_check_interval_rows: std::env::var("TAVANA_CONNECTION_CHECK_INTERVAL_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500), // Check more frequently for better disconnect detection
            max_result_rows: std::env::var("TAVANA_MAX_RESULT_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100_000), // 100k rows - protects JDBC clients from OOM

            // Backpressure settings
            flush_threshold_bytes: std::env::var("TAVANA_FLUSH_THRESHOLD_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64 * 1024), // 64KB - matches typical TCP window
            flush_threshold_rows: std::env::var("TAVANA_FLUSH_THRESHOLD_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100), // Safety net for small rows
            flush_timeout_secs: std::env::var("TAVANA_FLUSH_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300), // 5 minutes - StarRocks-style patient waiting
            max_portal_buffer_rows: std::env::var("TAVANA_MAX_PORTAL_BUFFER_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50_000), // 50k rows max in portal buffer
        }
    }
}

/// Configure TCP keepalive on a socket for faster dead connection detection
/// This is critical for Windows clients that may disconnect without proper FIN
fn configure_tcp_keepalive(stream: &tokio::net::TcpStream, keepalive_secs: u64) {
    use socket2::SockRef;
    
    // Set TCP_NODELAY for low latency
    if let Err(e) = stream.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY: {}", e);
    }
    
    // Configure TCP keepalive using socket2
    let socket = SockRef::from(stream);
    
    // Enable keepalive
    if let Err(e) = socket.set_keepalive(true) {
        warn!("Failed to enable TCP keepalive: {}", e);
        return;
    }
    
    // Configure keepalive timing
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(keepalive_secs))
        .with_interval(Duration::from_secs(keepalive_secs / 2 + 1));
    
    // Add retries on platforms that support it
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(3);
    
    if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
        warn!("Failed to configure TCP keepalive timing: {}", e);
    } else {
        debug!("TCP keepalive configured: {}s", keepalive_secs);
    }
}

/// Check if client connection is still alive by attempting a zero-byte peek
/// Returns true if connection is alive, false if disconnected
async fn is_client_connected(stream: &tokio::net::TcpStream) -> bool {
    use std::io::ErrorKind;
    
    // Try to peek at the socket to check connection state
    // If the client has disconnected, this will return an error
    match stream.peek(&mut [0u8; 0]).await {
        Ok(_) => true,
        Err(e) => {
            match e.kind() {
                // Connection reset or broken pipe = definitely disconnected
                ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::NotConnected => {
                    debug!("Client disconnected: {:?}", e.kind());
                    false
                }
                // WouldBlock is expected for non-blocking sockets with no data
                ErrorKind::WouldBlock => true,
                // Other errors might be transient
                _ => {
                    debug!("Connection check returned: {:?}", e.kind());
                    true
                }
            }
        }
    }
}

// Legacy constant for backwards compatibility (when config is not passed)
const STREAMING_BATCH_SIZE: usize = 100;

/// Substitute $1, $2, etc. parameters in SQL with actual values
/// This converts PostgreSQL-style parameterized queries to literal SQL
fn substitute_parameters(sql: &str, params: &[Option<String>]) -> String {
    let mut result = sql.to_string();
    
    // Replace parameters in reverse order to avoid $1 matching $10, $11, etc.
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match param {
            Some(value) => {
                // Escape single quotes in the value
                let escaped = value.replace('\'', "''");
                // Check if it's a number (don't quote numbers)
                if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                    escaped
                } else {
                    format!("'{}'", escaped)
                }
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }
    
    result
}

/// Apply automatic LIMIT to SELECT queries that don't have one
/// This prevents client OOM by limiting result size (like ClickHouse max_result_rows)
/// 
/// Returns (modified_sql, was_limited) where was_limited is true if LIMIT was added
/// 
/// Query hints to bypass the limit:
/// - `-- TAVANA:UNLIMITED` (SQL comment)
/// - `/*TAVANA:UNLIMITED*/` (block comment)
/// 
/// Example:
/// ```sql
/// -- TAVANA:UNLIMITED
/// SELECT * FROM delta_scan('az://...');
/// ```
fn apply_result_limit(sql: &str, max_rows: usize) -> (String, bool) {
    if max_rows == 0 {
        return (sql.to_string(), false);
    }
    
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    
    // Check for TAVANA:UNLIMITED hint - allows bypassing the limit
    if sql_upper.contains("TAVANA:UNLIMITED") {
        return (sql.to_string(), false);
    }
    
    // Only apply to SELECT queries
    if !sql_trimmed.starts_with("SELECT") {
        return (sql.to_string(), false);
    }
    
    // Skip if query is a cursor operation (DECLARE, FETCH, etc.)
    if sql_trimmed.contains("CURSOR") {
        return (sql.to_string(), false);
    }
    
    // Skip if query already has LIMIT
    // Check for LIMIT followed by a number (to avoid matching "LIMIT" in column names)
    if sql_upper.contains(" LIMIT ") {
        return (sql.to_string(), false);
    }
    
    // Skip if query has OFFSET without LIMIT (invalid, let it fail naturally)
    // Skip common subquery patterns (we want to limit only the outer query)
    
    // Add LIMIT to the end of the query
    // Handle trailing semicolon
    let sql_trimmed_orig = sql.trim();
    let (base_sql, has_semicolon) = if sql_trimmed_orig.ends_with(';') {
        (&sql_trimmed_orig[..sql_trimmed_orig.len()-1], true)
    } else {
        (sql_trimmed_orig, false)
    };
    
    let limited_sql = if has_semicolon {
        format!("{} LIMIT {};", base_sql, max_rows)
    } else {
        format!("{} LIMIT {}", base_sql, max_rows)
    };
    
    (limited_sql, true)
}

/// Send a PostgreSQL NOTICE message to inform client about automatic limiting
async fn send_notice_message<S: AsyncWrite + Unpin>(
    socket: &mut S,
    message: &str,
) -> std::io::Result<()> {
    // PostgreSQL NoticeResponse format:
    // 'N' (1 byte) + length (4 bytes) + fields
    // Fields: severity 'S' + "NOTICE\0" + message 'M' + message\0 + code 'C' + "01000\0" + \0
    
    let severity = b"NOTICE";
    let code = b"01000"; // Warning code
    
    // Calculate message length
    let msg_len = 4  // length field
        + 1 + severity.len() + 1  // 'S' + severity + null
        + 1 + code.len() + 1      // 'C' + code + null  
        + 1 + message.len() + 1   // 'M' + message + null
        + 1;                       // final null terminator
    
    let mut buf = Vec::with_capacity(1 + msg_len);
    buf.push(b'N'); // NoticeResponse
    buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
    
    // Severity field
    buf.push(b'S');
    buf.extend_from_slice(severity);
    buf.push(0);
    
    // SQLSTATE code field
    buf.push(b'C');
    buf.extend_from_slice(code);
    buf.push(0);
    
    // Message field
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    
    // Terminator
    buf.push(0);
    
    socket.write_all(&buf).await
}

/// Result of streaming query execution (for metrics tracking)
#[derive(Debug, Default)]
#[allow(dead_code)]
struct StreamingMetrics {
    /// Total rows streamed to client
    rows: usize,
    /// Total bytes streamed to client
    bytes: usize,
}

/// PostgreSQL wire protocol server with SmartScaler (Formula 3) and Query Queue
///
/// Architecture:
/// 1. Connection arrives → authenticate via AuthGateway
/// 2. Query arrives → QueryQueue.enqueue() (blocks until capacity available)
/// 3. Queue depth signals HPA to scale up if needed
/// 4. When dispatched → SmartScaler selects worker + VPA pre-size
/// 5. Execute query with streaming (true streaming, OOM-proof)
/// 6. Complete → release capacity for next query
pub struct PgWireServer {
    addr: SocketAddr,
    auth_service: Arc<AuthService>,
    #[allow(dead_code)]
    auth_gateway: Option<Arc<AuthGateway>>,
    worker_client: Arc<WorkerClient>,
    /// Pool of worker clients for cursor affinity routing
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
    redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    tls_config: Option<Arc<TlsConfig>>,
    /// Configuration (environment-driven, no hardcoded values)
    config: Arc<PgWireConfig>,
}

#[allow(dead_code)]
impl PgWireServer {
    pub fn new(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid port number for SocketAddr");
        
        // Extract auth gateway from auth service if available
        let auth_gateway = auth_service.gateway().cloned();
        
        // Create worker client pool for cursor affinity routing
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        
        // Load config from environment (no hardcoded values)
        let config = Arc::new(PgWireConfig::default());
        info!(
            "PgWireServer config: batch_size={}, timeout={}s, buffer={}KB",
            config.streaming_batch_size,
            config.query_timeout_secs,
            config.write_buffer_size / 1024
        );
        
        Self {
            addr,
            auth_service,
            auth_gateway,
            worker_client,
            worker_client_pool,
            query_router,
            pool_manager: None,
            redis_queue: None,
            smart_scaler: None,
            query_queue: QueryQueue::new(),
            tls_config: None,
            config,
        }
    }

    /// Set custom config
    pub fn with_config(mut self, config: PgWireConfig) -> Self {
        self.config = Arc::new(config);
        self
    }
    
    /// Set custom query timeout (convenience method)
    pub fn with_query_timeout(mut self, timeout_secs: u64) -> Self {
        let mut config = (*self.config).clone();
        config.query_timeout_secs = timeout_secs;
        self.config = Arc::new(config);
        self
    }

    /// Set TLS configuration for SSL connections
    pub fn with_tls(mut self, tls_config: Option<TlsConfig>) -> Self {
        self.tls_config = tls_config.map(Arc::new);
        self
    }

    /// Create with pool manager and optional Redis queue
    pub async fn with_pool_and_queue(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid port number for SocketAddr");

        // Initialize Redis queue if available
        let redis_queue = match RedisQueue::new(RedisQueueConfig::from_env()).await {
            Ok(queue) => {
                info!("Redis queue initialized - burst traffic handling enabled");
                Some(Arc::new(queue))
            }
            Err(e) => {
                warn!("Failed to initialize Redis queue: {} - queue disabled", e);
                None
            }
        };

        // Extract auth gateway from auth service if available
        let auth_gateway = auth_service.gateway().cloned();
        
        // Create worker client pool for cursor affinity routing
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        
        // Load config from environment
        let config = Arc::new(PgWireConfig::default());

        Self {
            addr,
            auth_service,
            auth_gateway,
            worker_client,
            worker_client_pool,
            query_router,
            pool_manager,
            redis_queue,
            smart_scaler: None,
            query_queue: QueryQueue::new(),
            tls_config: None,
            config,
        }
    }

    /// Create with SmartScaler (Formula 3 Combined Smart Scaling) and QueryQueue
    pub async fn with_smart_scaler(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
        smart_scaler: Option<Arc<SmartScaler>>,
    ) -> Self {
        // Create internal query queue
        let query_queue = QueryQueue::new();
        Self::with_smart_scaler_and_queue(
            port,
            auth_service,
            worker_client,
            query_router,
            pool_manager,
            smart_scaler,
            query_queue,
        )
        .await
    }

    /// Create with SmartScaler and SHARED QueryQueue
    /// Use this to share the queue with SmartScaler for HPA integration
    pub async fn with_smart_scaler_and_queue(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
        smart_scaler: Option<Arc<SmartScaler>>,
        query_queue: Arc<QueryQueue>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid port number for SocketAddr");

        // Redis queue disabled - QueryQueue handles queuing
        let redis_queue: Option<Arc<RedisQueue>> = None;
        
        // Create worker client pool for cursor affinity routing
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        
        // Load config from environment
        let config = Arc::new(PgWireConfig::default());

        info!(
            "PgWireServer initialized: SmartScaler={}, batch_size={}, timeout={}s",
            if smart_scaler.is_some() { "enabled" } else { "disabled" },
            config.streaming_batch_size,
            config.query_timeout_secs
        );

        // Extract auth gateway from auth service if available
        let auth_gateway = auth_service.gateway().cloned();

        Self {
            addr,
            auth_service,
            auth_gateway,
            worker_client,
            worker_client_pool,
            query_router,
            pool_manager,
            redis_queue,
            smart_scaler,
            query_queue,
            tls_config: None,
            config,
        }
    }

    /// Set TLS configuration (can be called after any constructor)
    pub fn set_tls(&mut self, tls_config: Option<TlsConfig>) {
        self.tls_config = tls_config.map(Arc::new);
    }

    /// Get auth gateway reference
    pub fn auth_gateway(&self) -> Option<&Arc<AuthGateway>> {
        self.auth_gateway.as_ref()
    }

    /// Start the PostgreSQL wire protocol server
    pub async fn start(&self) -> anyhow::Result<()> {
        let tls_status = if self.tls_config.is_some() {
            "TLS enabled (SSL + non-SSL)"
        } else {
            "TLS disabled (non-SSL only)"
        };
        info!(
            "Starting PostgreSQL wire protocol server on {} ({}, SmartScaler + QueryQueue mode)",
            self.addr, tls_status
        );

        // Start queue worker if Redis queue is available
        if let Some(ref queue) = self.redis_queue {
            let queue_clone = queue.clone();
            let worker_client = self.worker_client.clone();
            let query_router = self.query_router.clone();
            let pool_manager = self.pool_manager.clone();
            let smart_scaler = self.smart_scaler.clone();

            tokio::spawn(async move {
                run_queue_worker(
                    queue_clone,
                    worker_client,
                    query_router,
                    pool_manager,
                    smart_scaler,
                )
                .await;
            });
            info!("Queue worker started - processing queued queries with SmartScaler");
        }

        // Start queue stats logging loop (every 5 seconds)
        let queue_for_stats = self.query_queue.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                // Update metrics from queue stats
                let stats = queue_for_stats.stats().await;
                metrics::set_queue_depth(stats.queue_depth);
                metrics::set_current_usage_mb(stats.current_usage_mb);
                metrics::set_cluster_capacity_mb(stats.total_capacity_mb);
                metrics::set_available_capacity_mb(stats.available_mb);
                metrics::set_hpa_scale_up_signal(stats.hpa_scale_up_signal);

                // Log queue status periodically
                if stats.queue_depth > 0 || stats.active_queries > 0 {
                    tracing::debug!(
                        queue_depth = stats.queue_depth,
                        active = stats.active_queries,
                        usage_mb = stats.current_usage_mb,
                        capacity_mb = stats.total_capacity_mb,
                        avg_wait_ms = stats.avg_queue_wait_ms,
                        "QueryQueue status"
                    );
                }
            }
        });
        info!("QueryQueue stats logging started");

        let listener = TcpListener::bind(&self.addr).await?;

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            info!("New PostgreSQL connection from {}", peer_addr);

            // Configure TCP keepalive to detect dead connections quickly
            // This is critical for Windows clients (DBeaver, Tableau, Power BI)
            // that may disconnect without proper TCP FIN
            configure_tcp_keepalive(&socket, self.config.tcp_keepalive_secs);

            let auth_service = self.auth_service.clone();
            let worker_client = self.worker_client.clone();
            let worker_client_pool = self.worker_client_pool.clone();
            let query_router = self.query_router.clone();
            let pool_manager = self.pool_manager.clone();
            let redis_queue = self.redis_queue.clone();
            let smart_scaler = self.smart_scaler.clone();
            let query_queue = self.query_queue.clone();
            let tls_config = self.tls_config.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection_with_tls(
                    socket,
                    auth_service,
                    worker_client,
                    worker_client_pool,
                    query_router,
                    pool_manager,
                    redis_queue,
                    smart_scaler,
                    query_queue,
                    tls_config,
                    config,
                )
                .await
                {
                    // Only log as error if it's not a normal disconnect
                    let err_str = e.to_string();
                    if err_str.contains("early eof") || err_str.contains("connection reset") {
                        debug!("Client disconnected: {}", err_str);
                    } else {
                    error!("Error handling PostgreSQL connection: {}", e);
                    }
                }
            });
        }
    }
}

/// Background worker that processes queries from the Redis queue
/// Spawns concurrent query processors based on available workers
async fn run_queue_worker(
    queue: Arc<RedisQueue>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
    smart_scaler: Option<Arc<SmartScaler>>,
) {
    info!("Queue worker starting with SmartScaler support...");

    // Track active query processing tasks
    let _active_tasks = Arc::new(tokio::sync::Semaphore::new(0));

    loop {
        // Check availability using SmartScaler if available, otherwise pool_manager
        let idle_count: i32 = if let Some(ref scaler) = smart_scaler {
            let capacity = scaler.calculate_capacity().await;
            (capacity.worker_count as u64).saturating_sub(capacity.running_queries) as i32
        } else if let Some(ref pm) = pool_manager {
            let availability = pm.check_availability().await;
            availability.idle as i32
        } else {
            3 // Default to 3 concurrent processors
        };

        if idle_count == 0 {
            // No workers available, wait a bit
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }

        // Try to dequeue
        match queue.dequeue().await {
            Ok(Some(queued_query)) => {
                // Clone what we need for the spawned task
                let query_id = queued_query.id.clone();
                let sql = queued_query.sql.clone();
                let user_id = queued_query.user_id.clone();
                let worker_client = worker_client.clone();
                let query_router = query_router.clone();
                let queue = queue.clone();

                // Spawn a task to process this query concurrently
                tokio::spawn(async move {
                    let start = Instant::now();

                    debug!(
                        query_id = %query_id,
                        user = %user_id,
                        "Processing queued query"
                    );

                    // Execute the query
                    match execute_queued_query(&worker_client, &query_router, &sql, &user_id).await
                    {
                        Ok((row_count, worker_name)) => {
                            // Release the worker
                            if let Some(ref name) = worker_name {
                                query_router.release_worker(name, None).await;
                            }

                            info!(
                                query_id = %query_id,
                                rows = row_count,
                                worker = ?worker_name,
                                time_ms = start.elapsed().as_millis(),
                                "Queued query completed"
                            );
                            if let Err(e) = queue.complete(&query_id).await {
                                error!("Failed to mark query as complete: {}", e);
                            }
                        }
                        Err(e) => {
                            error!(
                                query_id = %query_id,
                                error = %e,
                                "Queued query failed"
                            );
                            if let Err(e) = queue.fail(&query_id, &e.to_string()).await {
                                error!("Failed to mark query as failed: {}", e);
                            }
                        }
                    }
                });

                // Small delay to allow task to start and claim a worker
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok(None) => {
                // No queries in queue, wait a bit
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => {
                error!("Failed to dequeue: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Execute a query from the queue (fire-and-forget style, no streaming to client)
/// Returns (row_count, worker_name) so the caller can release the worker
async fn execute_queued_query(
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<(usize, Option<String>)> {
    let estimate = query_router.route(sql).await;

    match estimate.target {
        QueryTarget::PreSizedWorker {
            address,
            worker_name,
        } => {
            // Execute on pre-sized worker
            let result = execute_query_on_worker(&address, sql, user_id).await?;
            Ok((result, Some(worker_name)))
        }
        QueryTarget::TenantPool {
            service_addr,
            tenant_id,
        } => {
            // Execute on tenant's dedicated pool (same path as pre-sized)
            info!("Routing to tenant pool: {}", tenant_id);
            let result = execute_query_on_worker(&service_addr, sql, user_id).await?;
            Ok((result, None))
        }
        QueryTarget::WorkerPool => {
            // Execute on default worker
            let result = worker_client.execute_query(sql, user_id).await?;
            Ok((result.rows.len(), None))
        }
    }
}

/// Helper to execute query directly on a worker address
async fn execute_query_on_worker(
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800))
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id: query_id.clone(),
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: 1800,
            max_rows: 0,
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut total_rows = 0;
    while let Some(batch) = stream.message().await? {
        if let Some(proto::query_result_batch::Result::RecordBatch(data)) = batch.result {
            total_rows += data.row_count as usize;
        }
    }

    Ok(total_rows)
}

/// Handle connection with optional TLS upgrade
/// This function handles SSL negotiation and upgrades the connection if TLS is configured
async fn handle_connection_with_tls(
    mut socket: tokio::net::TcpStream,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
    redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    tls_config: Option<Arc<TlsConfig>>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    // Get client IP before TLS upgrade (socket ref is consumed by TLS)
    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());
    
    let mut buf = [0u8; 4];
    
    // Read initial message to check for SSL request
    socket.read_exact(&mut buf).await?;
    let len = u32::from_be_bytes(buf) as usize;
    
    if len < 8 || len > 10000 {
        return Err(anyhow::anyhow!("Invalid message length: {}", len));
    }
    
    let mut startup_msg = vec![0u8; len - 4];
    socket.read_exact(&mut startup_msg).await?;
    
    // Check for SSL/GSSAPI negotiation
    if len == 8 && startup_msg.len() >= 4 {
        let code = u32::from_be_bytes([
            startup_msg[0],
            startup_msg[1],
            startup_msg[2],
            startup_msg[3],
        ]);
        
        match code {
            80877103 => {
                // SSLRequest
                if let Some(ref tls) = tls_config {
                    debug!("SSL negotiation requested, accepting");
                    socket.write_all(&[b'S']).await?;
                    socket.flush().await?;
                    
                    // Upgrade to TLS
                    let acceptor = tls.acceptor();
                    let tls_stream = acceptor.accept(socket).await
                        .map_err(|e| anyhow::anyhow!("TLS handshake failed: {}", e))?;
                    
                    info!("TLS connection established");
                    
                    // Continue with TLS stream (pass client_ip captured before TLS)
                    return handle_connection_generic(
                        tls_stream,
                        auth_service,
                        worker_client,
                        worker_client_pool,
                        query_router,
                        pool_manager,
                        redis_queue,
                        smart_scaler,
                        query_queue,
                        config,
                        client_ip,
                    ).await;
                } else {
                    debug!("SSL negotiation requested, declining (no TLS config)");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    
                    // Client will resend startup message without SSL
                    return handle_connection(
                        socket,
                        auth_service,
                        worker_client,
                        worker_client_pool,
                        query_router,
                        pool_manager,
                        redis_queue,
                        smart_scaler,
                        query_queue,
                        config,
                    ).await;
                }
            }
            80877104 => {
                // GSSENCRequest
                debug!("GSSAPI negotiation requested, declining");
                socket.write_all(&[b'N']).await?;
                socket.flush().await?;
                
                // Client will resend startup message
                return handle_connection(
                    socket,
                    auth_service,
                    worker_client,
                    worker_client_pool,
                    query_router,
                    pool_manager,
                    redis_queue,
                    smart_scaler,
                    query_queue,
                    config,
                ).await;
            }
            80877102 => {
                // CancelRequest - client wants to cancel a running query
                // The message format is: length (4) + code (4) + pid (4) + secret (4)
                // We've already read the code, now we need pid and secret
                let backend_pid = if startup_msg.len() >= 8 {
                    u32::from_be_bytes([startup_msg[4], startup_msg[5], startup_msg[6], startup_msg[7]])
                } else {
                    0
                };
                let cancel_secret = if startup_msg.len() >= 12 {
                    u32::from_be_bytes([startup_msg[8], startup_msg[9], startup_msg[10], startup_msg[11]])
                } else {
                    0
                };
                
                warn!(
                    backend_pid = backend_pid,
                    "CancelRequest received. Query cancellation not yet fully implemented. \
                     The query will continue running. Use LIMIT clause for large queries."
                );
                
                // CancelRequest connections are closed immediately without response
                // (per PostgreSQL protocol specification)
                return Ok(());
            }
            _ => {
                // Normal startup message, process directly
            }
        }
    }
    
    // Process as regular startup message (no SSL requested)
    handle_connection_with_startup(
        socket,
        startup_msg,
        auth_service,
        worker_client,
        worker_client_pool,
        query_router,
        pool_manager,
        redis_queue,
        smart_scaler,
        query_queue,
        config,
    ).await
}

/// Handle connection after SSL negotiation declined - expects client to retry without SSL
async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    let mut startup_msg;

    // Get client IP for auth context
    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());
    
    // Get auth gateway from service
    let auth_gateway = auth_service.gateway().cloned();

    // Loop to handle multiple negotiation requests (SSL, GSSAPI)
    loop {
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message, length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        startup_msg = vec![0u8; len - 4];
        socket.read_exact(&mut startup_msg).await?;

        if len == 8 && startup_msg.len() >= 4 {
            let code = u32::from_be_bytes([
                startup_msg[0],
                startup_msg[1],
                startup_msg[2],
                startup_msg[3],
            ]);
            match code {
                80877103 => {
                    debug!("SSL negotiation requested, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877104 => {
                    debug!("GSSAPI negotiation requested, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877102 => {
                    // CancelRequest - log and close
                    warn!("CancelRequest received (query cancellation not fully implemented)");
                    return Ok(());
                }
                _ => {}
                }
            }
            break;
        }

    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    // Perform password authentication with auth gateway
    let _principal = perform_md5_auth(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    // Send common parameter status messages
    send_parameter_status(&mut socket, "server_version", "15.0.0 (Tavana DuckDB)").await?;
    send_parameter_status(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status(&mut socket, "standard_conforming_strings", "on").await?;

    // Send BackendKeyData (K)
    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345); // Simple PRNG
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    // Send ReadyForQuery (Z)
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    // Use shared query loop (reduces code duplication)
    run_query_loop(
        &mut socket,
        &worker_client,
        &worker_client_pool,
        &query_router,
        &user_id,
        smart_scaler.as_ref().map(|s| s.as_ref()),
        &query_queue,
        &config,
    ).await
}

/// Handle connection with an already-parsed startup message (non-SSL path)
async fn handle_connection_with_startup(
    mut socket: tokio::net::TcpStream,
    startup_msg: Vec<u8>,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    // Get client IP for auth context
    let client_ip = socket.peer_addr().ok().map(|addr| addr.ip().to_string());
    
    // Get auth gateway from service
    let auth_gateway = auth_service.gateway().cloned();

    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    // Perform password authentication with auth gateway
    let _principal = perform_md5_auth(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    // Send common parameter status messages
    send_parameter_status(&mut socket, "server_version", "15.0.0 (Tavana DuckDB)").await?;
    send_parameter_status(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status(&mut socket, "standard_conforming_strings", "on").await?;

    // Send BackendKeyData (K)
    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    // Send ReadyForQuery (Z)
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    // Run the query loop
    run_query_loop(&mut socket, &worker_client, &worker_client_pool, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

/// Generic connection handler for both TLS and non-TLS streams
async fn handle_connection_generic<S>(
    mut socket: S,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
    client_ip: Option<String>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Get auth gateway from service
    let auth_gateway = auth_service.gateway().cloned();

    let mut buf = [0u8; 4];
    let mut startup_msg;

    // Loop to handle multiple negotiation requests after TLS upgrade
    loop {
        socket.read_exact(&mut buf).await?;
        let len = u32::from_be_bytes(buf) as usize;
        debug!("Received message (TLS), length: {}", len);

        if len < 8 || len > 10000 {
            return Err(anyhow::anyhow!("Invalid message length: {}", len));
        }

        startup_msg = vec![0u8; len - 4];
        socket.read_exact(&mut startup_msg).await?;

        if len == 8 && startup_msg.len() >= 4 {
            let code = u32::from_be_bytes([
            startup_msg[0],
            startup_msg[1],
            startup_msg[2],
            startup_msg[3],
        ]);
            match code {
                80877103 | 80877104 => {
                    // SSLRequest or GSSENCRequest after TLS - decline (already encrypted)
                    debug!("Nested SSL/GSSAPI request, declining");
                    socket.write_all(&[b'N']).await?;
                    socket.flush().await?;
                    continue;
                }
                80877102 => {
                    // CancelRequest - log and close
                    warn!("CancelRequest received via TLS (query cancellation not fully implemented)");
                    return Ok(());
                }
                _ => {}
            }
        }
        break;
    }

    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {} (TLS)", user_id);

    // Perform password authentication with auth gateway
    let _principal = perform_md5_auth_generic(&mut socket, &user_id, auth_gateway.as_ref(), client_ip).await?;

    // Send common parameter status messages
    send_parameter_status_generic(&mut socket, "server_version", "15.0.0 (Tavana DuckDB)").await?;
    send_parameter_status_generic(&mut socket, "client_encoding", "UTF8").await?;
    send_parameter_status_generic(&mut socket, "server_encoding", "UTF8").await?;
    send_parameter_status_generic(&mut socket, "DateStyle", "ISO, MDY").await?;
    send_parameter_status_generic(&mut socket, "TimeZone", "UTC").await?;
    send_parameter_status_generic(&mut socket, "integer_datetimes", "on").await?;
    send_parameter_status_generic(&mut socket, "standard_conforming_strings", "on").await?;

    // Send BackendKeyData (K)
    let pid = std::process::id();
    let secret = pid.wrapping_mul(1103515245).wrapping_add(12345);
    let mut key_data = vec![b'K', 0, 0, 0, 12];
    key_data.extend_from_slice(&(pid as u32).to_be_bytes());
    key_data.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&key_data).await?;

    // Send ReadyForQuery (Z)
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    socket.write_all(&ready).await?;
    socket.flush().await?;

    // Run the query loop for TLS stream
    run_query_loop_generic(&mut socket, &worker_client, &worker_client_pool, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

/// Run the main query loop (for non-TLS)
/// 
/// Uses config for timeout enforcement and streaming batch size.
async fn run_query_loop(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    worker_client_pool: &WorkerClientPool,
    query_router: &QueryRouter,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
    config: &PgWireConfig,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    // Transaction status for ReadyForQuery: 'I' = Idle, 'T' = In Transaction, 'E' = Error
    // This is CRITICAL for JDBC cursor-based streaming - JDBC only uses server-side cursors
    // when it thinks we're in a transaction (status 'T')
    let mut transaction_status: u8 = TRANSACTION_STATUS_IDLE;
    // ReadyForQuery message - updated when transaction status changes
    let mut ready = [b'Z', 0, 0, 0, 5, transaction_status];
    let mut prepared_query: Option<String> = None;
    // Track whether Describe sent RowDescription (true) or NoData (false)
    let mut describe_sent_row_description = false;
    // Cache the column count from Describe to ensure Execute sends matching data
    let mut __describe_column_count: usize = 0;
    // Server-side cursor storage for this connection
    let mut cursors = ConnectionCursors::new();
    
    // Helper macro to update ready message when transaction status changes
    macro_rules! update_transaction_status {
        ($new_status:expr) => {{
            transaction_status = $new_status;
            ready[5] = transaction_status;
            debug!("Transaction status changed to: {}", 
                match transaction_status {
                    TRANSACTION_STATUS_IDLE => "Idle",
                    TRANSACTION_STATUS_IN_TRANSACTION => "In Transaction",
                    TRANSACTION_STATUS_ERROR => "Error",
                    _ => "Unknown"
                }
            );
        }};
    }

    loop {
        let mut msg_type = [0u8; 1];
        if socket.read_exact(&mut msg_type).await.is_err() {
            debug!("Client disconnected");
            break;
        }

        match msg_type[0] {
            b'X' => {
                debug!("Client sent Terminate message");
                break;
            }
            b'Q' => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut query_bytes = vec![0u8; len];
                socket.read_exact(&mut query_bytes).await?;

                let query = String::from_utf8_lossy(&query_bytes)
                    .trim_end_matches('\0')
                    .to_string();

                debug!("Received query: {}", &query[..query.len().min(100)]);

                // Check for cursor commands first (require connection-level state)
                let query_upper = query.to_uppercase();
                let query_trimmed = query_upper.trim();
                
                // CRITICAL: Check for transaction state changes (BEGIN/COMMIT/ROLLBACK)
                // This MUST be done BEFORE processing the query to ensure JDBC gets correct
                // transaction status in ReadyForQuery - required for server-side cursors!
                let (new_tx_status, is_tx_cmd) = get_transaction_state_change(&query, transaction_status);
                if is_tx_cmd {
                    info!(
                        query = %query_trimmed,
                        old_status = match transaction_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        new_status = match new_tx_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        "Transaction command detected (non-TLS) - updating status"
                    );
                    update_transaction_status!(new_tx_status);
                }
                
                // Handle DECLARE CURSOR
                if query_trimmed.starts_with("DECLARE ") && query_trimmed.contains(" CURSOR ") {
                    debug!(
                        query_trimmed = %query_trimmed,
                        "Detected DECLARE CURSOR command (non-TLS)"
                    );
                    // Use default worker client for DECLARE (stores worker_addr for later FETCH affinity)
                    let default_client = worker_client_pool.default_client();
                    if let Some(result) = cursors::handle_declare_cursor(&query, &mut cursors, &default_client, user_id).await {
                        info!(cursor_count = cursors.len(), "DECLARE CURSOR handled successfully (non-TLS)");
                        send_query_result_immediate(socket, result.into()).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
            } else {
                        warn!(query = %query, "DECLARE CURSOR parsing failed (non-TLS)");
                    }
                }
                
                // Handle FETCH - uses cursor affinity routing via WorkerClientPool
                if query_trimmed.starts_with("FETCH ") {
                    match cursors::handle_fetch_cursor(&query, &mut cursors, worker_client_pool, user_id).await {
                        Some(result) => {
                            send_query_result_immediate(socket, result.into()).await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                        None => {
                            // Cursor not found - send error
                            send_error(socket, "cursor does not exist").await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                    }
                }
                
                // Handle CLOSE cursor - uses cursor affinity routing via WorkerClientPool
                if query_trimmed.starts_with("CLOSE ") {
                    if let Some(result) = cursors::handle_close_cursor(&query, &mut cursors, worker_client_pool).await {
                        info!(command_tag = ?result.command_tag, "CLOSE CURSOR handled (TLS simple query)");
                        send_query_result_immediate(socket, QueryExecutionResult {
                            columns: vec![],
                            rows: vec![],
                            row_count: 0,
                            command_tag: result.command_tag,
                        }).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
                    }
                }

                // Handle ROLLBACK - must actually execute in DuckDB to clear aborted transaction state
                if query_trimmed == "ROLLBACK" {
                    debug!("Executing ROLLBACK in DuckDB to clear transaction state");
                    // Execute ROLLBACK in DuckDB to clear any aborted transaction state
                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                    // Return success regardless of result (ROLLBACK always succeeds, even if no transaction)
                    send_query_result_immediate(socket, QueryExecutionResult {
                        columns: vec![],
                        rows: vec![],
                        row_count: 0,
                        command_tag: Some("ROLLBACK".to_string()),
                    }).await?;
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // Handle empty queries (sent by Metabase and other clients during connection test)
                if query_trimmed.is_empty() {
                    debug!("Empty query received (non-TLS), returning EmptyQueryResponse");
                    // PostgreSQL protocol: send EmptyQueryResponse ('I') for empty queries
                    socket.write_all(&[b'I', 0, 0, 0, 4]).await?; // EmptyQueryResponse
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // CRITICAL: Apply automatic LIMIT to prevent client OOM
                // Like ClickHouse's max_result_rows with result_overflow_mode=break
                let (limited_query, was_limited) = apply_result_limit(&query, config.max_result_rows);
                if was_limited {
                    info!(
                        max_rows = config.max_result_rows,
                        "Query automatically limited to {} rows (non-TLS).",
                        config.max_result_rows
                    );
                    // Send NOTICE to inform client about the limit
                    let notice_msg = format!(
                        "Results limited to {} rows. Use LIMIT/OFFSET for pagination.",
                        config.max_result_rows
                    );
                    if let Err(e) = send_notice_message(socket, &notice_msg).await {
                        debug!("Failed to send NOTICE: {}", e);
                    }
                }
                let query_to_execute = if was_limited { &limited_query } else { &query };

                let query_id = uuid::Uuid::new_v4().to_string();
                let estimate = query_router.route(query_to_execute).await;
                let estimated_data_mb = estimate.data_size_mb;

                let enqueue_result = query_queue
                    .enqueue(query_id.clone(), estimated_data_mb)
                    .await;

                match enqueue_result {
                    Ok(query_token) => {
                        let start = std::time::Instant::now();
                        let timeout_duration = Duration::from_secs(config.query_timeout_secs);
                        
                        // Execute query WITH TIMEOUT ENFORCEMENT
                        let result = tokio::time::timeout(
                            timeout_duration,
                            execute_query_streaming_with_scaler(
                                socket,
                                worker_client,
                                query_router,
                                query_to_execute,
                                user_id,
                                smart_scaler,
                            )
                        )
                        .await;

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let outcome = match result {
                            Ok(Ok(row_count)) => {
                                info!(
                                    query_id = %query_id,
                                    rows = row_count,
                                    wait_ms = query_token.queue_wait_ms(),
                                    exec_ms = duration_ms,
                                    "Query completed (streaming)"
                                );
                                QueryOutcome::Success {
                                    rows: row_count as u64,
                                    bytes: 0,
                                    duration_ms,
                                }
                            }
                            Ok(Err(e)) => {
                                error!("Query {} error: {}", query_id, e);
                                
                                // If error is a transaction error, automatically rollback to clear state
                                let error_msg = e.to_string();
                                if error_msg.contains("transaction") && error_msg.contains("aborted") {
                                    warn!("Transaction error detected, automatically rolling back");
                                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                                }
                                
                                send_error(socket, &error_msg).await?;
                                QueryOutcome::Failure {
                                    error: error_msg,
                                    duration_ms,
                                }
                            }
                            Err(_elapsed) => {
                                // Timeout expired
                                let error_msg = format!(
                                    "Query timeout: exceeded {}s limit. Use LIMIT clause.",
                                    config.query_timeout_secs
                                );
                                error!("Query {} timed out after {}s", query_id, config.query_timeout_secs);
                                send_error(socket, &error_msg).await?;
                                QueryOutcome::Failure {
                                    error: error_msg,
                                    duration_ms,
                                }
                            }
                        };
                        query_queue.complete(query_token, outcome).await;
                    }
                    Err(queue_error) => {
                        let error_msg = format!("Query queue error: {}", queue_error);
                        warn!(query_id = %query_id, "Query failed to queue: {}", error_msg);
                        send_error(socket, &error_msg).await?;
                    }
                }

    socket.write_all(&ready).await?;
    socket.flush().await?;
            }
            b'P' => {
                let sql = handle_parse_extended(socket, &mut buf).await?;
                if let Some(query) = sql {
                    prepared_query = Some(query);
                }
            }
            b'B' => handle_bind(socket, &mut buf).await?,
            b'D' => {
                let (sent_row_desc, col_count) = handle_describe(socket, &mut buf, prepared_query.as_deref(), worker_client, user_id).await?;
                describe_sent_row_description = sent_row_desc;
                __describe_column_count = col_count;
            }
            b'E' => {
                if let Some(ref query) = prepared_query {
                    // If Describe sent NoData, we must not send DataRows
                    if !describe_sent_row_description {
                        // Read Execute message data
                        socket.read_exact(&mut buf).await?;
                        let len = u32::from_be_bytes(buf) as usize - 4;
                        let mut data = vec![0u8; len];
                        socket.read_exact(&mut data).await?;
                        // Get the correct command tag for this SQL (BEGIN, SET, COMMIT, etc.)
                        let cmd_tag = if let Some(result) = handle_pg_specific_command(query) {
                            result.command_tag.unwrap_or_else(|| "SELECT 0".to_string())
                        } else {
                            "SELECT 0".to_string()
                        };
                        debug!("Extended Protocol (non-TLS) - Execute: Describe sent NoData, sending CommandComplete: {}", cmd_tag);
                        let cmd = cmd_tag.as_bytes();
                        let cmd_len = (4 + cmd.len() + 1) as u32;
                        let mut msg = vec![b'C'];
                        msg.extend_from_slice(&cmd_len.to_be_bytes());
                        msg.extend_from_slice(cmd);
                        msg.push(0);
                        socket.write_all(&msg).await?;
                        socket.flush().await?;
                    } else {
                        handle_execute_extended(
                            socket,
                            &mut buf,
                            worker_client,
                            query_router,
                            query,
                            user_id,
                            smart_scaler,
                            query_queue,
                        )
                        .await?;
                    }
                } else {
                    handle_execute_empty(socket, &mut buf).await?;
                }
                prepared_query = None;
                describe_sent_row_description = false;
            }
            b'C' => {
                // Close message - close a prepared statement or portal
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                // Clear prepared query state
                prepared_query = None;
                describe_sent_row_description = false;
                __describe_column_count = 0;
                
                // Send CloseComplete
                let close_complete = [b'3', 0, 0, 0, 4];
                socket.write_all(&close_complete).await?;
                socket.flush().await?;
            }
            b'S' => {
                // Sync message has length field (4 bytes with value 4)
                socket.read_exact(&mut buf).await?;
                info!("Extended Protocol - Sync: sending ReadyForQuery");
                socket.write_all(&ready).await?;
                socket.flush().await?;
            }
            _ => {
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut skip = vec![0u8; len];
                socket.read_exact(&mut skip).await?;
            }
        }
    }

    Ok(())
}

/// Portal state for Extended Query Protocol cursor support
/// This enables JDBC setFetchSize() streaming by maintaining row state between Execute calls
/// 
/// Supports two modes:
/// 1. **TRUE STREAMING**: Holds a gRPC stream handle, reads rows on-demand
/// 2. **BUFFERED**: Falls back to buffering for small results or compatibility
/// 
/// TRUE STREAMING provides:
/// - Low latency (first row sent immediately after query starts)
/// - Low memory (only one batch in memory at a time)
/// - Backpressure propagation (slow client -> slow worker via gRPC flow control)
enum PortalState {
    /// TRUE STREAMING: Holds gRPC stream handle, reads on-demand
    /// This is the preferred mode for large result sets
    Streaming(StreamingPortal),
    /// BUFFERED: Fallback for small results or when streaming setup fails
    Buffered(BufferedPortal),
}

/// Streaming portal - holds a live gRPC stream for true streaming
struct StreamingPortal {
    /// The gRPC streaming result from worker (owns the receiver)
    stream: StreamingResult,
    /// Column metadata from first batch
    columns: Vec<(String, String)>,
    /// Total rows sent so far (for CommandComplete)
    rows_sent: usize,
    /// Column count for DataRow consistency
    column_count: usize,
    /// Buffer for partial batch consumption (when max_rows < batch size)
    /// Rows that were read from stream but not yet sent to client
    pending_rows: Vec<Vec<String>>,
}

/// Buffered portal - stores all rows in memory (fallback mode)
struct BufferedPortal {
    /// All rows from query execution
    rows: Vec<Vec<String>>,
    /// Current row offset (for resumption after PortalSuspended)
    offset: usize,
    /// Column count for DataRow consistency
    column_count: usize,
}

impl PortalState {
    /// Create a new streaming portal
    fn new_streaming(stream: StreamingResult, columns: Vec<(String, String)>) -> Self {
        let column_count = columns.len();
        PortalState::Streaming(StreamingPortal {
            stream,
            columns,
            rows_sent: 0,
            column_count,
            pending_rows: Vec::new(),
        })
    }
    
    /// Create a new buffered portal (fallback)
    fn new_buffered(rows: Vec<Vec<String>>, column_count: usize) -> Self {
        PortalState::Buffered(BufferedPortal {
            rows,
            offset: 0,
            column_count,
        })
    }
}

/// Run the main query loop for generic streams (TLS)
/// Supports both Simple Query and Extended Query protocols with proper cursor streaming.
/// 
/// Uses config for timeout enforcement and streaming batch size.
async fn run_query_loop_generic<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    worker_client_pool: &WorkerClientPool,
    query_router: &QueryRouter,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
    config: &PgWireConfig,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = [0u8; 4];
    // Transaction status for ReadyForQuery: 'I' = Idle, 'T' = In Transaction, 'E' = Error
    // CRITICAL: JDBC only uses server-side cursors when status is 'T' (in transaction)
    let mut transaction_status: u8 = TRANSACTION_STATUS_IDLE;
    // ReadyForQuery message - updated when transaction status changes
    let mut ready = [b'Z', 0, 0, 0, 5, transaction_status];
    let mut prepared_query: Option<String> = None;
    // Track whether Describe sent RowDescription (true) or NoData (false)
    // This ensures Execute phase is consistent with Describe phase
    let mut describe_sent_row_description = false;
    // Cache the column count from Describe to ensure Execute sends matching data
    let mut __describe_column_count: usize = 0;
    // Server-side cursor storage for this connection
    // Enables DECLARE CURSOR / FETCH / CLOSE for large result sets
    let mut cursors = ConnectionCursors::new();
    // Portal state for Extended Query Protocol cursor streaming
    // Stores buffered rows and offset for resumption after PortalSuspended
    let mut portal_state: Option<PortalState> = None;
    // Bound parameter values from Bind message (for parameterized queries)
    let mut bound_parameters: Vec<Option<String>> = Vec::new();
    
    // Schema cache for parameterized queries - avoids re-executing LIMIT 0 queries
    // Key: SQL template (with $1, $2 placeholders), Value: column schemas
    // This dramatically reduces latency for repeated pg_catalog queries from JDBC drivers
    let mut schema_cache: std::collections::HashMap<String, Vec<(String, String)>> = std::collections::HashMap::new();
    
    // Cached columns from Describe phase - reused in Execute to avoid double-query
    // This is cleared after each Execute completes
    let mut cached_describe_columns: Option<Vec<(String, String)>> = None;
    
    // Skip-till-sync flag for error recovery (PostgreSQL Extended Protocol pattern)
    // When an error occurs during extended query protocol (Parse/Bind/Describe/Execute),
    // we set this flag and skip all messages until we receive a Sync ('S') message.
    // This prevents protocol desynchronization and allows clients to recover gracefully.
    let mut ignore_till_sync = false;
    
    // Helper macro to update ready message when transaction status changes
    macro_rules! update_transaction_status {
        ($new_status:expr) => {{
            transaction_status = $new_status;
            ready[5] = transaction_status;
            debug!("Transaction status changed to: {}", 
                match transaction_status {
                    TRANSACTION_STATUS_IDLE => "Idle",
                    TRANSACTION_STATUS_IN_TRANSACTION => "In Transaction",
                    TRANSACTION_STATUS_ERROR => "Error",
                    _ => "Unknown"
                }
            );
        }};
    }

    loop {
        let mut msg_type = [0u8; 1];
        if socket.read_exact(&mut msg_type).await.is_err() {
            debug!("Client disconnected (TLS)");
            break;
        }

        // Skip-till-sync: Skip all messages except Sync and Terminate when in error state
        // This is the PostgreSQL protocol pattern for error recovery in extended query protocol.
        // After an error, clients send Sync to resynchronize, and we skip everything until then.
        if ignore_till_sync && msg_type[0] != b'S' && msg_type[0] != b'X' {
            // Read and discard the message
            socket.read_exact(&mut buf).await?;
            let len = u32::from_be_bytes(buf) as usize - 4;
            if len > 0 {
                let mut skip_buf = vec![0u8; len];
                socket.read_exact(&mut skip_buf).await?;
            }
            debug!("Skip-till-sync: Skipped message type '{}'", msg_type[0] as char);
            continue;
        }

        match msg_type[0] {
            b'X' => {
                debug!("Client sent Terminate message (TLS)");
                break;
            }
            b'Q' => {
                info!("Simple Query message received (TLS)"); // Debug: confirm message type is received
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut query_bytes = vec![0u8; len];
                socket.read_exact(&mut query_bytes).await?;

                let query = String::from_utf8_lossy(&query_bytes)
                    .trim_end_matches('\0')
                    .to_string();

                info!("Received query (TLS): '{}' (len={}, bytes={:?})", 
                    &query[..query.len().min(100)],
                    query.len(),
                    &query.as_bytes()[..query.len().min(20)]
                ); // DEBUG: Log query with length and bytes

                // Check for cursor commands first (require connection-level state)
                let query_upper = query.to_uppercase();
                let query_trimmed = query_upper.trim();
                
                // CRITICAL: Check for transaction state changes (BEGIN/COMMIT/ROLLBACK)
                // This MUST be done BEFORE processing the query to ensure JDBC gets correct
                // transaction status in ReadyForQuery - required for server-side cursors!
                let (new_tx_status, is_tx_cmd) = get_transaction_state_change(&query, transaction_status);
                if is_tx_cmd {
                    info!(
                        query = %query_trimmed,
                        old_status = match transaction_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        new_status = match new_tx_status { b'I' => "Idle", b'T' => "InTx", b'E' => "Err", _ => "?" },
                        "Transaction command detected - updating status"
                    );
                    update_transaction_status!(new_tx_status);
                }
                
                // Log query at INFO level for debugging cursor issues
                if query_trimmed.contains("CURSOR") || query_trimmed.starts_with("DECLARE") || query_trimmed.starts_with("FETCH") {
                    info!(
                        query_preview = %&query_trimmed[..query_trimmed.len().min(80)],
                        "Processing potential cursor command - matched cursor detection"
                    );
                }
                
                // Handle DECLARE CURSOR - use default worker client (stores worker_addr for affinity)
                if query_trimmed.starts_with("DECLARE ") && query_trimmed.contains(" CURSOR ") {
                    info!(
                        query_trimmed = %query_trimmed,
                        "Detected DECLARE CURSOR command, attempting to handle"
                    );
                    let default_client = worker_client_pool.default_client();
                    if let Some(result) = cursors::handle_declare_cursor(&query, &mut cursors, &default_client, user_id).await {
                        info!(
                            cursor_count = cursors.len(),
                            "DECLARE CURSOR handled successfully"
                        );
                        send_simple_result_generic(socket, &[], &[], result.command_tag.as_deref()).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
                    } else {
                        warn!(
                            query = %query,
                            "DECLARE CURSOR parsing failed, forwarding to worker"
                        );
                    }
                }
                
                // Handle FETCH - uses cursor affinity routing via WorkerClientPool
                if query_trimmed.starts_with("FETCH ") {
                    match cursors::handle_fetch_cursor(&query, &mut cursors, worker_client_pool, user_id).await {
                        Some(result) => {
                            let cols: Vec<(&str, i32)> = result.columns.iter()
                                .map(|(n, _)| (n.as_str(), 25i32))
                                .collect();
                            send_simple_result_generic(socket, &cols, &result.rows, result.command_tag.as_deref()).await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                        None => {
                            // Cursor not found - send error
                            send_error_generic(socket, "cursor does not exist").await?;
                            socket.write_all(&ready).await?;
                            socket.flush().await?;
                            continue;
                        }
                    }
                }
                
                // Handle CLOSE cursor - uses cursor affinity routing via WorkerClientPool
                if query_trimmed.starts_with("CLOSE ") {
                    if let Some(result) = cursors::handle_close_cursor(&query, &mut cursors, worker_client_pool).await {
                        info!(command_tag = ?result.command_tag, "CLOSE CURSOR handled (TLS extended query)");
                        send_simple_result_generic(socket, &[], &[], result.command_tag.as_deref()).await?;
                socket.write_all(&ready).await?;
                socket.flush().await?;
                        continue;
                    }
                }

                // Handle ROLLBACK - must actually execute in DuckDB to clear aborted transaction state
                if query_trimmed == "ROLLBACK" {
                    debug!("Executing ROLLBACK in DuckDB to clear transaction state (TLS)");
                    // Execute ROLLBACK in DuckDB to clear any aborted transaction state
                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                    // Return success regardless of result (ROLLBACK always succeeds, even if no transaction)
                    send_simple_result_generic(socket, &[], &[], Some("ROLLBACK")).await?;
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // Handle empty queries (sent by Metabase and other clients during connection test)
                if query_trimmed.is_empty() {
                    info!("Empty query received (TLS), returning EmptyQueryResponse + ReadyForQuery");
                    // PostgreSQL protocol: send EmptyQueryResponse ('I') for empty queries
                    socket.write_all(&[b'I', 0, 0, 0, 4]).await?; // EmptyQueryResponse
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // CRITICAL: Apply automatic LIMIT to prevent client OOM
                // Like ClickHouse's max_result_rows with result_overflow_mode=break
                // This protects JDBC clients (DBeaver, Tableau) that buffer all rows in memory
                let (limited_query, was_limited) = apply_result_limit(&query, config.max_result_rows);
                if was_limited {
                    info!(
                        max_rows = config.max_result_rows,
                        "Query automatically limited to {} rows. Use LIMIT/OFFSET for pagination.",
                        config.max_result_rows
                    );
                    // Send NOTICE to inform client about the limit
                    let notice_msg = format!(
                        "Results limited to {} rows. Use LIMIT/OFFSET for pagination, or set TAVANA_MAX_RESULT_ROWS=0 to disable.",
                        config.max_result_rows
                    );
                    if let Err(e) = send_notice_message(socket, &notice_msg).await {
                        debug!("Failed to send NOTICE: {}", e);
                    }
                }
                let query_to_execute = if was_limited { &limited_query } else { &query };

                // For TLS connections, execute queries directly using a simpler path
                let query_id = uuid::Uuid::new_v4().to_string();
                let estimate = query_router.route(query_to_execute).await;
                let estimated_data_mb = estimate.data_size_mb;

                let enqueue_result = query_queue
                    .enqueue(query_id.clone(), estimated_data_mb)
                    .await;

                match enqueue_result {
                    Ok(query_token) => {
                        let start = std::time::Instant::now();
                        let timeout_duration = Duration::from_secs(config.query_timeout_secs);
                        
                        // Execute query WITH TIMEOUT ENFORCEMENT
                        // This prevents runaway queries from blocking resources indefinitely
                        let result = tokio::time::timeout(
                            timeout_duration,
                            execute_query_tls(
                                socket,
                                worker_client,
                                query_router,
                                query_to_execute,
                                user_id,
                                smart_scaler,
                            )
                        )
                        .await;

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let outcome = match result {
                            Ok(Ok(row_count)) => {
                                info!(
                                    query_id = %query_id,
                                    rows = row_count,
                                    wait_ms = query_token.queue_wait_ms(),
                                    exec_ms = duration_ms,
                                    "Query completed (TLS streaming)"
                                );
                                
                                // Warn about large result sets that may cause client-side issues
                                if row_count > 100_000 {
                                    warn!(
                                        query_id = %query_id,
                                        rows = row_count,
                                        "Large result set streamed. If client crashes, use LIMIT/OFFSET or DECLARE CURSOR"
                                    );
                                }
                                QueryOutcome::Success {
                                    rows: row_count as u64,
                                    bytes: 0,
                                    duration_ms,
                                }
                            }
                            Ok(Err(e)) => {
                                error!("Query {} error (TLS): {}", query_id, e);
                                
                                // If error is a transaction error, automatically rollback to clear state
                                let error_msg = e.to_string();
                                if error_msg.contains("transaction") && error_msg.contains("aborted") {
                                    warn!("Transaction error detected (TLS), automatically rolling back");
                                    let _ = worker_client.execute_query("ROLLBACK", user_id).await;
                                }
                                
                                send_error_generic(socket, &error_msg).await?;
                                QueryOutcome::Failure {
                                    error: error_msg,
                                    duration_ms,
                                }
                            }
                            Err(_elapsed) => {
                                // Timeout expired - query took too long
                                let error_msg = format!(
                                    "Query timeout: exceeded {}s limit. Use LIMIT clause or increase timeout.",
                                    config.query_timeout_secs
                                );
                                error!("Query {} timed out after {}s", query_id, config.query_timeout_secs);
                                send_error_generic(socket, &error_msg).await?;
                                QueryOutcome::Failure {
                                    error: error_msg,
                                    duration_ms,
                                }
                            }
                        };
                        query_queue.complete(query_token, outcome).await;
                    }
                    Err(queue_error) => {
                        let error_msg = format!("Query queue error: {}", queue_error);
                        warn!(query_id = %query_id, "Query failed to queue (TLS): {}", error_msg);
                        send_error_generic(socket, &error_msg).await?;
                    }
                }

                socket.write_all(&ready).await?;
                socket.flush().await?;
            }
            b'P' => {
                // Parse - extract and store the prepared statement
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                // Extract query from Parse message
                let stmt_end = data.iter().position(|&b| b == 0).unwrap_or(0);
                let query_start = stmt_end + 1;
                let query_end = data[query_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(data.len() - query_start)
                    + query_start;
                let query = String::from_utf8_lossy(&data[query_start..query_end]).to_string();
                
                // Extract inner query from COPY commands at parse time
                // This enables DuckDB postgres_query() with text protocol
                let actual_query = if let Some(inner) = extract_copy_inner_query(&query) {
                    debug!("Extended Protocol - Converted COPY to SELECT");
                    inner
                } else {
                    query
                };
                
                if !actual_query.is_empty() {
                    prepared_query = Some(actual_query);
                }
                
                socket.write_all(&[b'1', 0, 0, 0, 4]).await?; // ParseComplete
                socket.flush().await?;
            }
            b'B' => {
                // Bind - parse and check result format codes
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                // Parse Bind message to detect binary format requests
                let mut offset = 0;
                // Skip portal name
                while offset < data.len() && data[offset] != 0 { offset += 1; }
                offset += 1;
                // Skip statement name  
                while offset < data.len() && data[offset] != 0 { offset += 1; }
                offset += 1;
                
                // Parse parameter format codes (text=0, binary=1)
                let mut param_formats: Vec<i16> = Vec::new();
                if offset + 2 <= data.len() {
                    let param_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;
                    for _ in 0..param_format_count {
                        if offset + 2 <= data.len() {
                            param_formats.push(i16::from_be_bytes([data[offset], data[offset+1]]));
                            offset += 2;
                        }
                    }
                }
                
                // Extract parameter values and store them
                bound_parameters.clear();
                if offset + 2 <= data.len() {
                    let param_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;
                    for i in 0..param_count {
                        if offset + 4 <= data.len() {
                            let param_len = i32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
                            offset += 4;
                            if param_len < 0 {
                                // NULL parameter
                                bound_parameters.push(None);
                            } else if param_len == 0 {
                                // Empty string
                                bound_parameters.push(Some(String::new()));
                            } else if offset + param_len as usize <= data.len() {
                                // Check format - binary (1) or text (0)
                                let is_binary = param_formats.get(i).copied().unwrap_or(
                                    param_formats.first().copied().unwrap_or(0)
                                ) == 1;
                                
                                if is_binary {
                                    // Binary format - convert based on type (for now, assume int4/int8)
                                    let bytes = &data[offset..offset + param_len as usize];
                                    let value = match param_len {
                                        4 => i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string(),
                                        8 => i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]).to_string(),
                                        _ => {
                                            // Unknown binary format, try as text
                                            String::from_utf8_lossy(bytes).to_string()
                                        }
                                    };
                                    bound_parameters.push(Some(value));
                                } else {
                                    // Text format
                                    let value = String::from_utf8_lossy(&data[offset..offset + param_len as usize]).to_string();
                                    bound_parameters.push(Some(value));
                                }
                                offset += param_len as usize;
                            }
                        }
                    }
                    if !bound_parameters.is_empty() {
                        debug!("Bind: extracted {} parameters: {:?}", bound_parameters.len(), bound_parameters);
                    }
                }
                
                // Parse result format codes - check for binary requests
                let mut client_wants_binary = false;
                if offset + 2 <= data.len() {
                    let result_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
                    offset += 2;
                    
                    // Check if client wants binary for any column
                    if result_format_count == 1 && offset + 2 <= data.len() {
                        // Single format applies to all columns
                        let fmt = i16::from_be_bytes([data[offset], data[offset + 1]]);
                        if fmt == 1 {
                            client_wants_binary = true;
                        }
                    } else if result_format_count > 1 && offset + result_format_count * 2 <= data.len() {
                        // Per-column formats
                        for i in 0..result_format_count {
                            let fmt = i16::from_be_bytes([data[offset + i*2], data[offset + i*2 + 1]]);
                            if fmt == 1 {
                                client_wants_binary = true;
                                break;
                            }
                        }
                    }
                }
                
                if client_wants_binary {
                    // Client requested binary format but we only support text
                    // Accept the bind anyway - we'll send text format in response
                    // Most clients (including Tableau, JDBC drivers) handle this gracefully
                    debug!("Bind (TLS): client requested BINARY format, will respond with TEXT (DuckDB limitation)");
                }
                
                // Always send BindComplete - we'll respond with text format regardless
                debug!("Bind (TLS): sending BindComplete");
                socket.write_all(&[b'2', 0, 0, 0, 4]).await?; // BindComplete
                socket.flush().await?;
            }
            b'D' => {
                // Describe - return column info
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                debug!("Extended Protocol - Describe");
                
                // Reset state for this Describe
                describe_sent_row_description = false;
                __describe_column_count = 0;
                
                // Send ParameterDescription (no parameters)
                socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
                
                // Get column descriptions if we have a prepared query
                // Note: COPY commands were already rewritten to SELECT at Parse time
                if let Some(ref sql) = prepared_query {
                    // Check for PG-specific commands first
                    if let Some(result) = handle_pg_specific_command(sql) {
                        if result.columns.is_empty() {
                            debug!("Extended Protocol - Describe: PG command returns no data");
                            socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                        } else {
                            info!("Extended Protocol - Describe: PG command returns {} columns", result.columns.len());
                            __describe_column_count = result.columns.len();
                            send_row_description_generic(socket, &result.columns).await?;
                            describe_sent_row_description = true;
                        }
                    } else {
                        // Check if query has parameters ($1, $2, etc.)
                        // We can't execute parameterized queries during Describe because we don't have
                        // the parameter values yet (they come in Bind message)
                        let has_parameters = sql.contains("$1") || sql.contains("$2") || sql.contains("$3");
                        let sql_upper = sql.to_uppercase();
                        
                        if has_parameters {
                            // Check schema cache first - avoids expensive LIMIT 0 query for repeated queries
                            // This is critical for JDBC drivers that send the same pg_catalog queries repeatedly
                            if let Some(cached_columns) = schema_cache.get(sql) {
                                debug!("Extended Protocol - Describe: using cached schema for parameterized query ({} columns)", cached_columns.len());
                                __describe_column_count = cached_columns.len();
                                cached_describe_columns = Some(cached_columns.clone());
                                send_row_description_generic(socket, cached_columns).await?;
                                describe_sent_row_description = true;
                            } else {
                                // For parameterized queries, try to get schema by replacing params with NULL
                                // This works for most queries including pg_catalog
                                // DuckDB has full native pg_catalog support, so we get real schemas
                                let schema_sql = sql
                                    .replace("$1", "NULL")
                                    .replace("$2", "NULL")
                                    .replace("$3", "NULL")
                                    .replace("$4", "NULL")
                                    .replace("$5", "NULL")
                                    .replace("$6", "NULL")
                                    .replace("$7", "NULL")
                                    .replace("$8", "NULL")
                                    .replace("$9", "NULL");
                                
                                // CRITICAL: Apply pg_compat rewriting BEFORE executing schema query
                                // This fixes DBeaver crashes from ::regclass, ::regtype etc. types
                                let schema_sql_rewritten = pg_compat::rewrite_pg_to_duckdb(&schema_sql);
                                
                                // Wrap in LIMIT 0 to avoid fetching data
                                let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", 
                                    schema_sql_rewritten.trim_end_matches(';'));
                                
                                match worker_client.execute_query(&schema_query, user_id).await {
                                    Ok(result) => {
                                        let columns: Vec<(String, String)> = result.columns.iter()
                                            .map(|c| (c.name.clone(), c.type_name.clone()))
                                            .collect();
                                        if columns.is_empty() {
                                            debug!("Extended Protocol - Describe: parameterized query returns no columns");
                                            socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                                        } else {
                                            info!("Extended Protocol - Describe: parameterized query returns {} columns", columns.len());
                                            __describe_column_count = columns.len();
                                            // Cache the schema for future use
                                            schema_cache.insert(sql.clone(), columns.clone());
                                            cached_describe_columns = Some(columns.clone());
                                            send_row_description_generic(socket, &columns).await?;
                                            describe_sent_row_description = true;
                                        }
                                    }
                                    Err(e) => {
                                        // Schema detection failed, send NoData (valid protocol response)
                                        debug!("Extended Protocol - Describe: schema detection failed: {}, sending NoData", e);
                                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                                    }
                                }
                            }
                        } else {
                            // Non-parameterized query - use LIMIT 0 to get schema without fetching data
                            // This FIXES the doubled latency issue: previously we executed the full query
                            // in Describe, then again in Execute. Now Describe only gets schema.
                            
                            // Apply pg_compat rewriting for PostgreSQL compatibility
                            let sql_rewritten = pg_compat::rewrite_pg_to_duckdb(sql);
                            
                            // Use LIMIT 0 to get schema without fetching any rows
                            let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", 
                                sql_rewritten.trim_end_matches(';'));
                            
                            match worker_client.execute_query(&schema_query, user_id).await {
                                Ok(result) => {
                                    let columns: Vec<(String, String)> = result.columns.iter()
                                        .map(|c| (c.name.clone(), c.type_name.clone()))
                                        .collect();
                                    if columns.is_empty() {
                                        debug!("Extended Protocol - Describe: query returns no columns");
                                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                                    } else {
                                        info!("Extended Protocol - Describe: query returns {} columns: {:?}", 
                                            columns.len(), 
                                            columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                                        );
                                        __describe_column_count = columns.len();
                                        // Cache columns for potential reuse
                                        cached_describe_columns = Some(columns.clone());
                                        send_row_description_generic(socket, &columns).await?;
                                        describe_sent_row_description = true;
                                    }
                                }
                                Err(e) => {
                                    warn!("Extended Protocol - Describe: schema query failed: {}, sending NoData", e);
                                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                                }
                            }
                        }
                    }
                } else {
                    debug!("Extended Protocol - Describe: no prepared query");
                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                }
                socket.flush().await?;
            }
            b'E' => {
                // Execute - for Extended Query Protocol
                // Message format: portal_name (C string) + max_rows (int32)
                // max_rows = 0 means unlimited, otherwise limit to that many rows
                // This is how JDBC clients control streaming via setFetchSize()
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                // Parse Execute message: portal name (null-terminated) + max_rows (4 bytes)
                let portal_end = data.iter().position(|&b| b == 0).unwrap_or(0);
                let max_rows = if portal_end + 5 <= data.len() {
                    let max_rows_bytes = &data[portal_end + 1..portal_end + 5];
                    i32::from_be_bytes([max_rows_bytes[0], max_rows_bytes[1], max_rows_bytes[2], max_rows_bytes[3]])
                } else {
                    0 // Unlimited
                };
                
                // Check if we're resuming from a suspended portal
                // Use take() to avoid borrow issues with mutable state
                if let Some(mut state) = portal_state.take() {
                    let (should_continue, keep_state) = match &mut state {
                        PortalState::Streaming(portal) => {
                            // TRUE STREAMING: Read from gRPC stream on-demand
                            let max_rows_to_send = if max_rows > 0 { max_rows as usize } else { usize::MAX };
                            let mut rows_sent_this_batch = 0usize;
                            let mut bytes_since_flush = 0usize;
                            let mut client_disconnected = false;
                            let mut stream_exhausted = false;
                            let mut stream_error: Option<String> = None;
                            
                            debug!("Extended Protocol - Execute: TRUE STREAMING resume, max_rows={}", max_rows);
                            
                            // First, send any pending rows from previous partial batch
                            while !portal.pending_rows.is_empty() && rows_sent_this_batch < max_rows_to_send {
                                let row = portal.pending_rows.remove(0);
                                let data_row = build_data_row(&row);
                                bytes_since_flush += data_row.len();
                                if let Err(e) = socket.write_all(&data_row).await {
                                    if is_disconnect_error(&e) { client_disconnected = true; break; }
                                    return Err(e.into());
                                }
                                rows_sent_this_batch += 1;
                                portal.rows_sent += 1;
                                
                                if bytes_since_flush >= config.flush_threshold_bytes {
                                    if let Err(e) = socket.flush().await {
                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                        return Err(e.into());
                                    }
                                    bytes_since_flush = 0;
                                }
                            }
                            
                            // Then read from stream until we reach max_rows or stream exhausted
                            while !client_disconnected && rows_sent_this_batch < max_rows_to_send {
                                match portal.stream.next().await {
                                    Some(Ok(StreamingBatch::Rows(batch_rows))) => {
                                        for row in batch_rows {
                                            if rows_sent_this_batch >= max_rows_to_send {
                                                // Save remaining rows for next Execute
                                                portal.pending_rows.push(row);
                                            } else {
                                                let data_row = build_data_row(&row);
                                                bytes_since_flush += data_row.len();
                                                if let Err(e) = socket.write_all(&data_row).await {
                                                    if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                    return Err(e.into());
                                                }
                                                rows_sent_this_batch += 1;
                                                portal.rows_sent += 1;
                                                
                                                if bytes_since_flush >= config.flush_threshold_bytes {
                                                    if let Err(e) = socket.flush().await {
                                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                                        return Err(e.into());
                                                    }
                                                    bytes_since_flush = 0;
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(StreamingBatch::Metadata { .. })) => continue,
                                    Some(Ok(StreamingBatch::Error(msg))) => { stream_error = Some(msg); break; }
                                    Some(Err(e)) => { stream_error = Some(e.to_string()); break; }
                                    None => { stream_exhausted = true; break; }
                                }
                            }
                            
                            if !client_disconnected && stream_error.is_none() {
                                let _ = socket.flush().await;
                            }
                            
                            if client_disconnected {
                                info!("Client disconnected during streaming resume");
                                (false, false) // break loop, don't keep state
                            } else if let Some(err) = stream_error {
                                send_error_generic(socket, &err).await?;
                                (true, false) // continue, don't keep state
                            } else if stream_exhausted && portal.pending_rows.is_empty() {
                                // All rows sent
                                let cmd_tag = format!("SELECT {}", portal.rows_sent);
                                send_command_complete_generic(socket, &cmd_tag).await?;
                                prepared_query = None;
                                describe_sent_row_description = false;
                                __describe_column_count = 0;
                                bound_parameters.clear();
                                cached_describe_columns = None;
                                (true, false) // continue, don't keep state
                            } else {
                                // More rows available - send PortalSuspended
                                debug!("Extended Protocol - Execute: TRUE STREAMING sent {} rows, more available", rows_sent_this_batch);
                                socket.write_all(&[b's', 0, 0, 0, 4]).await?;
                                socket.flush().await?;
                                (true, true) // continue, keep state
                            }
                        }
                        PortalState::Buffered(portal) => {
                            // BUFFERED: Resume from stored rows (fallback mode)
                            let remaining = portal.rows.len() - portal.offset;
                            let rows_to_send = if max_rows > 0 && (max_rows as usize) < remaining {
                                max_rows as usize
                            } else {
                                remaining
                            };
                            
                            let end = portal.offset + rows_to_send;
                            debug!("Extended Protocol - Execute: BUFFERED resume from offset {}, sending {} rows", portal.offset, rows_to_send);
                            
                            let mut bytes_since_flush = 0usize;
                            let mut rows_since_flush = 0usize;
                            let mut client_disconnected = false;
                            
                            for row in &portal.rows[portal.offset..end] {
                                let data_row = build_data_row(row);
                                bytes_since_flush += data_row.len();
                                rows_since_flush += 1;
                                socket.write_all(&data_row).await?;
                                
                                if bytes_since_flush >= config.flush_threshold_bytes 
                                    || rows_since_flush >= config.flush_threshold_rows {
                                    if let Err(e) = socket.flush().await {
                                        if is_disconnect_error(&e) { client_disconnected = true; break; }
                                        return Err(e.into());
                                    }
                                    bytes_since_flush = 0;
                                    rows_since_flush = 0;
                                }
                            }
                            
                            if !client_disconnected {
                                socket.flush().await?;
                            }
                            
                            if client_disconnected {
                                info!("Client disconnected during buffered resume");
                                (false, false) // break loop, don't keep state
                            } else {
                                portal.offset = end;
                                
                                if portal.offset >= portal.rows.len() {
                                    // All rows sent
                                    let cmd_tag = format!("SELECT {}", portal.rows.len());
                                    send_command_complete_generic(socket, &cmd_tag).await?;
                                    prepared_query = None;
                                    describe_sent_row_description = false;
                                    __describe_column_count = 0;
                                    bound_parameters.clear();
                                    cached_describe_columns = None;
                                    (true, false) // continue, don't keep state
                                } else {
                                    // More rows available - send PortalSuspended
                                    socket.write_all(&[b's', 0, 0, 0, 4]).await?;
                                    socket.flush().await?;
                                    (true, true) // continue, keep state
                                }
                            }
                        }
                    };
                    
                    // Restore portal_state if we should keep it
                    if keep_state {
                        portal_state = Some(state);
                        continue; // Don't process further, wait for next Execute
                    }
                    
                    if !should_continue {
                        break; // Client disconnected
                    }
                } else if let Some(ref sql) = prepared_query {
                    // First Execute - need to run query
                    // Substitute parameters if any were bound
                    let final_sql = if !bound_parameters.is_empty() && sql.contains('$') {
                        let substituted = substitute_parameters(sql, &bound_parameters);
                        debug!("Extended Protocol - Execute: substituted parameters, SQL: {}", 
                            &substituted[..substituted.len().min(200)]);
                        substituted
                    } else {
                        sql.clone()
                    };
                    
                    // CRITICAL: If Describe sent NoData, we MUST NOT send DataRows
                    if !describe_sent_row_description {
                        let cmd_tag = if let Some(result) = handle_pg_specific_command(&final_sql) {
                            result.command_tag.unwrap_or_else(|| "SELECT 0".to_string())
                        } else {
                            "SELECT 0".to_string()
                        };
                        debug!("Extended Protocol - Execute: Describe sent NoData, sending CommandComplete: {}", cmd_tag);
                        send_command_complete_generic(socket, &cmd_tag).await?;
                        socket.flush().await?;
                    } else {
                        // FIRST: Check if this is an intercepted command (like Tableau temp table SELECT)
                        // If so, return the fake rows instead of executing against worker
                        if let Some(result) = handle_pg_specific_command(&final_sql) {
                            if !result.rows.is_empty() {
                                info!("Extended Protocol - Execute: Intercepted command returns {} rows", result.rows.len());
                                for row in &result.rows {
                                    send_data_row_generic(socket, row, __describe_column_count).await?;
                                }
                                let cmd_tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
                                send_command_complete_generic(socket, &cmd_tag).await?;
                                socket.flush().await?;
                                info!("Extended Protocol - Execute: sent intercepted CommandComplete ({}), flushed", cmd_tag);
                                prepared_query = None;
                                describe_sent_row_description = false;
                                __describe_column_count = 0;
                                bound_parameters.clear();
                                cached_describe_columns = None;
                                continue;
                            }
                        }
                        
                        // Execute query and get all rows
                        // NOTE: This still buffers all rows for JDBC cursor support.
                        // True streaming for JDBC requires DECLARE CURSOR / FETCH instead.
                        info!("Extended Protocol - Execute: Describe sent RowDescription ({} cols), max_rows={}, executing query", __describe_column_count, max_rows);
                        
                        // Apply query timeout enforcement
                        let timeout_duration = Duration::from_secs(config.query_timeout_secs);
                        let query_result = tokio::time::timeout(
                            timeout_duration,
                            execute_query_get_rows(worker_client, query_router, &final_sql, user_id, smart_scaler)
                        ).await;
                        
                        match query_result {
                            Ok(Ok(rows)) => {
                                let total_rows = rows.len();
                                // CRITICAL: Validate that returned row column count matches Describe
                                let actual_col_count = if total_rows > 0 { rows[0].len() } else { 0 };
                                if actual_col_count > 0 && actual_col_count != __describe_column_count {
                                    warn!(
                                        "Extended Protocol - Execute: COLUMN COUNT MISMATCH! Describe announced {} cols, Execute returned {} cols. This may cause client errors.",
                                        __describe_column_count, actual_col_count
                                    );
                                }
                                info!("Extended Protocol - Execute: query returned {} rows, {} cols (expected {} cols)", 
                                    total_rows, actual_col_count, __describe_column_count);
                                // Log sample of first row for debugging (truncated values)
                                if total_rows > 0 && actual_col_count > 0 {
                                    let first_row_preview: Vec<String> = rows[0].iter()
                                        .take(5)
                                        .map(|v| if v.len() > 30 { format!("{}...", &v[..30]) } else { v.clone() })
                                        .collect();
                                    debug!("Extended Protocol - First row preview (first 5 cols): {:?}", first_row_preview);
                                    
                                    // Check for potential issues in data
                                    for (i, v) in rows[0].iter().enumerate() {
                                        if v.contains('\0') {
                                            warn!("Column {} contains NULL byte! Value preview: {:?}", i, &v[..v.len().min(50)]);
                                        }
                                        if v.len() > 10_000_000 {
                                            warn!("Column {} has unusually large value: {} bytes", i, v.len());
                                        }
                                    }
                                }
                                let rows_to_send = if max_rows > 0 && (max_rows as usize) < total_rows {
                                    max_rows as usize
                                } else {
                                    total_rows
                                };
                                
                                // Check if remaining rows exceed portal buffer limit
                                let remaining_rows = total_rows - rows_to_send;
                                if remaining_rows > config.max_portal_buffer_rows {
                                    // Too many rows to buffer - reject to prevent OOM
                                    // Client should use DECLARE CURSOR / FETCH or LIMIT clause
                                    let error_msg = format!(
                                        "Result set too large for portal buffer: {} rows exceed {} limit. \
                                        Use LIMIT clause, DECLARE CURSOR / FETCH, or increase max_rows in Execute.",
                                        remaining_rows, config.max_portal_buffer_rows
                                    );
                                    warn!("Extended Protocol - Execute: {}", error_msg);
                                    send_error_generic(socket, &error_msg).await?;
                                    ignore_till_sync = true;
                                    update_transaction_status!(TRANSACTION_STATUS_ERROR);
                                    continue;
                                }
                                
                                // Send rows with backpressure awareness (StarRocks-style)
                                // Flush every N rows to prevent overwhelming slow clients
                                let mut bytes_since_flush = 0usize;
                                let mut rows_since_flush = 0usize;
                                let mut client_disconnected = false;
                                
                                for row in &rows[..rows_to_send] {
                                    // Build and send DataRow
                                    let data_row = build_data_row(row);
                                    bytes_since_flush += data_row.len();
                                    rows_since_flush += 1;
                                    socket.write_all(&data_row).await?;
                                    
                                    // Backpressure check: flush based on bytes or row count
                                    if bytes_since_flush >= config.flush_threshold_bytes 
                                        || rows_since_flush >= config.flush_threshold_rows {
                                        // Flush and check for slow client
                                        let flush_start = std::time::Instant::now();
                                        if let Err(e) = socket.flush().await {
                                            if is_disconnect_error(&e) {
                                                warn!("Client disconnected during streaming after {} rows", rows_since_flush);
                                                client_disconnected = true;
                                                break;
                                            }
                                            return Err(e.into());
                                        }
                                        let flush_time = flush_start.elapsed();
                                        if flush_time.as_secs() >= 5 {
                                            debug!("Slow client detected: flush took {}ms", flush_time.as_millis());
                                        }
                                        bytes_since_flush = 0;
                                        rows_since_flush = 0;
                                    }
                                }
                                
                                // Final flush
                                if !client_disconnected {
                                    socket.flush().await?;
                                }
                                
                                if client_disconnected {
                                    // Client gone, clean up
                                    info!("Client disconnected during Execute, cleaning up");
                                    break;
                                }
                                
                                if rows_to_send < total_rows {
                                    // More rows - store state and send PortalSuspended
                                    // Only buffer what we need (rows after offset)
                                    let rows_to_buffer: Vec<Vec<String>> = rows.into_iter()
                                        .skip(rows_to_send)
                                        .collect();
                                    portal_state = Some(PortalState::new_buffered(
                                        rows_to_buffer,
                                        __describe_column_count,
                                    ));
                                    debug!("Extended Protocol - Execute: {} rows sent, {} buffered for portal, sending PortalSuspended", 
                                        rows_to_send, total_rows - rows_to_send);
                                    socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                                    socket.flush().await?;
                                    continue; // Don't clear state
                                } else {
                                    // All rows sent
                                    let cmd_tag = format!("SELECT {}", total_rows);
                                    send_command_complete_generic(socket, &cmd_tag).await?;
                                    socket.flush().await?;
                                    info!("Extended Protocol - Execute: sent CommandComplete ({}), flushed", cmd_tag);
                                }
                            }
                            Ok(Err(e)) => {
                                error!("Extended Protocol - Execute: query failed: {}", e);
                                send_error_generic(socket, &e.to_string()).await?;
                                // Set skip-till-sync for error recovery (PostgreSQL protocol)
                                // Don't send ReadyForQuery yet - wait for client to send Sync
                                ignore_till_sync = true;
                                update_transaction_status!(TRANSACTION_STATUS_ERROR);
                            }
                            Err(_elapsed) => {
                                // Timeout expired
                                let error_msg = format!(
                                    "Query timeout: exceeded {}s limit. Use LIMIT clause or DECLARE CURSOR.",
                                    config.query_timeout_secs
                                );
                                error!("Extended Protocol - Execute: query timed out after {}s", config.query_timeout_secs);
                                send_error_generic(socket, &error_msg).await?;
                                // Set skip-till-sync for error recovery
                                ignore_till_sync = true;
                                update_transaction_status!(TRANSACTION_STATUS_ERROR);
                            }
                        }
                    }
                    
                    // Clear state only if no more rows
                    prepared_query = None;
                    describe_sent_row_description = false;
                    __describe_column_count = 0;
                    bound_parameters.clear();
                    cached_describe_columns = None;
                } else {
                    // No prepared statement - send empty CommandComplete
                    debug!("Extended Protocol - Execute: no prepared query");
                    send_command_complete_generic(socket, "SELECT 0").await?;
                }
            }
            b'C' => {
                // Close message - close a prepared statement or portal
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
                
                let close_type = if data.is_empty() { b'S' } else { data[0] };
                debug!("Extended Protocol - Close type: {}", close_type as char);
                
                // Clear ALL state when closing
                prepared_query = None;
                describe_sent_row_description = false;
                __describe_column_count = 0;
                portal_state = None; // Clear buffered rows
                bound_parameters.clear();
                cached_describe_columns = None;
                
                // Send CloseComplete
                socket.write_all(&[b'3', 0, 0, 0, 4]).await?;
                socket.flush().await?;
            }
            b'S' => {
                // Sync message has length field (4 bytes with value 4)
                socket.read_exact(&mut buf).await?;
                
                // Reset skip-till-sync state (error recovery complete)
                if ignore_till_sync {
                    info!("Extended Protocol - Sync: Error recovery complete, resuming normal operation");
                    ignore_till_sync = false;
                    // Keep transaction_status as 'E' so client knows there was an error
                    // Client will typically send ROLLBACK after seeing error status
                }
                
                info!("Extended Protocol - Sync: sending ReadyForQuery (status={})", 
                    match transaction_status {
                        TRANSACTION_STATUS_IDLE => "Idle",
                        TRANSACTION_STATUS_IN_TRANSACTION => "InTransaction",
                        TRANSACTION_STATUS_ERROR => "Error",
                        _ => "Unknown"
                    }
                );
                socket.write_all(&ready).await?;
                socket.flush().await?;
                
                // After Sync with error status, reset to Idle for next command batch
                if transaction_status == TRANSACTION_STATUS_ERROR {
                    update_transaction_status!(TRANSACTION_STATUS_IDLE);
                }
            }
            _ => {
                debug!("Unknown protocol message type: 0x{:02x}", msg_type[0]);
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut skip = vec![0u8; len];
                socket.read_exact(&mut skip).await?;
            }
        }
    }

    Ok(())
}

/// Send parameter status for generic streams
async fn send_parameter_status_generic<S>(socket: &mut S, name: &str, value: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let total_len = 4 + name.len() + 1 + value.len() + 1;
    let mut msg = vec![b'S'];
    msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    msg.extend_from_slice(name.as_bytes());
    msg.push(0);
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send error message for generic streams (legacy - simple message only)
async fn send_error_generic<S>(socket: &mut S, message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Use the new classified error system for better error messages
    send_classified_error_generic(socket, message).await
}

/// Send a classified error with proper SQLSTATE code, message, hint, and detail
/// This provides much better error messages to clients like Tableau, DBeaver, etc.
async fn send_classified_error_generic<S>(socket: &mut S, raw_message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let classified = classify_error(raw_message);
    
    // Log with full context for debugging
    debug!(
        category = %classified.category,
        sqlstate = %classified.sqlstate,
        message = %classified.message,
        raw = %classified.raw_error,
        "Sending classified error to client"
    );
    
    // Build PostgreSQL Error Response with all fields
    // Fields: S (severity), V (severity non-localized), C (code), M (message), D (detail), H (hint)
    let mut fields = Vec::new();
    
    // Severity (localized)
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // Severity (non-localized for programmatic use)
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // SQLSTATE code
    fields.push(b'C');
    fields.extend_from_slice(classified.sqlstate.as_bytes());
    fields.push(0);
    
    // Primary message
    fields.push(b'M');
    fields.extend_from_slice(classified.message.as_bytes());
    fields.push(0);
    
    // Detail (if present)
    if let Some(ref detail) = classified.detail {
        if !detail.is_empty() {
            fields.push(b'D');
            fields.extend_from_slice(detail.as_bytes());
            fields.push(0);
        }
    }
    
    // Hint (if present)
    if let Some(ref hint) = classified.hint {
        fields.push(b'H');
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);
    }
    
    // Terminator
    fields.push(0);
    
    // Build the message
    let total_len = 4 + fields.len();
    let mut error_msg = vec![b'E'];
    error_msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    error_msg.extend_from_slice(&fields);
    
    socket.write_all(&error_msg).await?;
    socket.flush().await?;
    
    // Note: Do NOT send CommandComplete after ErrorResponse!
    // The PostgreSQL protocol specifies that ErrorResponse terminates the command.
    // The client should wait for ReadyForQuery after the error.

    Ok(())
}

/// Execute query for TLS streams - uses WorkerClient for query execution
/// Execute query with TRUE STREAMING - OOM-proof implementation
/// 
/// Unlike the previous buffered implementation, this function:
/// 1. NEVER loads all rows into memory
/// 2. Streams each batch to the client as it arrives from the worker
/// 3. Uses bounded channels for backpressure
/// 
/// This is how Databricks/Snowflake/BigQuery handle large result sets.
async fn execute_query_tls<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    _query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        let columns: Vec<(&str, i32)> = result.columns.iter().map(|(n, _t)| (n.as_str(), 25i32)).collect();
        send_simple_result_generic(socket, &columns, &result.rows, result.command_tag.as_deref()).await?;
        return Ok(0);
    }
    
    // Check if this is a COPY command
    let is_copy_command = sql.trim().to_uppercase().starts_with("COPY");
    
    // Handle COPY commands by extracting inner query
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        info!("Rewrote COPY command to inner query: {}", &inner[..inner.len().min(80)]);
        inner
    } else {
        sql.to_string()
    };
    
    // Rewrite PostgreSQL-specific syntax to DuckDB equivalents
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    metrics::query_started();
    let start_time = std::time::Instant::now();

    // TRUE STREAMING: Use streaming API instead of buffered
    // This is the key change for OOM-proof execution
    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            let mut columns_sent = false;
            let mut total_rows: usize = 0;
            let mut batch_rows: usize = 0;
            let mut column_names: Vec<String> = vec![];
            let mut _column_types: Vec<String> = vec![];

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata { columns, column_types: types } => {
                        column_names = columns;
                        _column_types = types;
                        
                        if !columns_sent {
                            if is_copy_command {
                                // Send CopyOutResponse for COPY commands
                                send_copy_out_response_header_generic(socket, column_names.len()).await?;
                            } else {
                                // Send RowDescription for SELECT
                                let col_pairs: Vec<(String, String)> = column_names.iter()
                                    .zip(_column_types.iter())
                                    .map(|(n, t)| (n.clone(), t.clone()))
                                    .collect();
                                send_row_description_generic(socket, &col_pairs).await?;
                            }
                            columns_sent = true;
                        }
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            if is_copy_command {
                                send_copy_data_row_generic(socket, &row).await?;
                            } else {
                                send_data_row_generic(socket, &row, column_names.len()).await?;
                            }
                            total_rows += 1;
                            batch_rows += 1;

                            // Flush periodically to provide backpressure and prevent client timeout
                            if batch_rows >= STREAMING_BATCH_SIZE {
                                socket.flush().await?;
                                batch_rows = 0;
                            }
                        }
                    }
                    StreamingBatch::Error(msg) => {
                        return Err(anyhow::anyhow!("{}", msg));
                    }
                }
            }

            // Send completion messages
            if is_copy_command {
                send_copy_done_generic(socket).await?;
            }
            
            // If no columns were sent (empty result), send empty row description
            if !columns_sent && !is_copy_command {
                send_row_description_generic(socket, &[]).await?;
            }
            
            let tag = format!("SELECT {}", total_rows);
            send_command_complete_generic(socket, &tag).await?;
            socket.flush().await?;

            metrics::query_ended();
            debug!(
                rows = total_rows,
                elapsed_ms = start_time.elapsed().as_millis(),
                "Query completed (TRUE STREAMING - OOM-proof)"
            );
            
            Ok(total_rows)
        }
        Err(e) => {
            error!("Streaming query failed: {}", e);
            Err(e)
        }
    }
}

/// Execute query for Extended Query Protocol (Parse/Bind/Describe/Execute)
/// Unlike execute_query_tls, this does NOT send RowDescription because
/// it was already sent during the Describe phase.
/// `expected_column_count` ensures DataRow column count matches RowDescription.
/// `max_rows` controls streaming - if > 0, limits rows sent and sends PortalSuspended
/// instead of CommandComplete when limit is reached (enabling JDBC cursor streaming).
#[allow(dead_code)]
async fn execute_query_extended_protocol<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
    expected_column_count: usize,
    max_rows: i32,
) -> anyhow::Result<(usize, bool)>  // Returns (rows_sent, more_rows_available)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::query_router::QueryTarget;
    
    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        // For extended protocol, don't send RowDescription again (it was sent during Describe or NoData was sent)
        send_data_rows_only_generic(socket, &result.rows, result.rows.len(), result.command_tag.as_deref(), expected_column_count).await?;
        return Ok((result.rows.len(), false)); // PG commands are always complete
    }
    
    // Handle COPY commands by extracting inner query
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        debug!("Extended Protocol Execute - Converted COPY to SELECT");
        inner
    } else {
        sql.to_string()
    };
    
    // Rewrite PostgreSQL-specific syntax to DuckDB equivalents
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    metrics::query_started();

    let estimate = query_router.route(sql).await;
    debug!(
        data_mb = estimate.data_size_mb,
        target = ?estimate.target,
        "Extended Protocol - Query routed"
    );

    // Execute query on the routed target worker
    let (_columns, rows) = match &estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            debug!("Extended Protocol - Executing on pre-sized worker: {}", worker_name);
            execute_query_on_worker_buffered(address, sql, user_id).await?
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            debug!("Extended Protocol - Executing on tenant pool: {}", tenant_id);
            execute_query_on_worker_buffered(service_addr, sql, user_id).await?
        }
        QueryTarget::WorkerPool => {
            // Fallback to default worker client
            let result = worker_client.execute_query(sql, user_id).await?;
            let cols: Vec<(String, String)> = result.columns.iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect();
            (cols, result.rows)
        }
    };
    
    let total_rows = rows.len();
    
    // NOTE: max_rows cursor streaming is DISABLED for now.
    // Proper JDBC cursor streaming requires maintaining row offset state between Execute calls,
    // which we don't have. Until we implement proper stateful cursors, always send all rows.
    // This prevents the bug where we'd send PortalSuspended but then clear prepared_query anyway.
    let (rows_to_send, more_available) = (&rows[..], false);
    
    if max_rows > 0 {
        debug!(
            max_rows = max_rows,
            total_rows = total_rows,
            "Extended Protocol - max_rows requested but cursor streaming not yet implemented, sending all rows"
        );
    }
    
    let row_count = rows_to_send.len();
    
    // For Extended Protocol, only send DataRows
    // RowDescription was already sent during Describe phase
    // Use expected_column_count to ensure DataRow column count matches
    send_data_rows_only_generic(socket, rows_to_send, row_count, None, expected_column_count).await?;
    
    debug!("Extended Protocol Execute completed: {} rows (all sent)", row_count);
    
    Ok((row_count, more_available))
}

/// Execute query and return rows only (for cursor streaming with portal state)
/// Used by Extended Query Protocol with proper cursor resumption support
async fn execute_query_get_rows(
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    _smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<Vec<Vec<String>>> {
    use crate::query_router::QueryTarget;
    
    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        return Ok(result.rows);
    }
    
    // Handle COPY commands by extracting inner query
    let actual_sql = if let Some(inner) = extract_copy_inner_query(sql) {
        debug!("execute_query_get_rows - Converted COPY to SELECT");
        inner
    } else {
        sql.to_string()
    };
    
    // Rewrite PostgreSQL-specific syntax to DuckDB equivalents
    let actual_sql = pg_compat::rewrite_pg_to_duckdb(&actual_sql);
    let sql = &actual_sql;

    let estimate = query_router.route(sql).await;
    
    // Execute query on the routed target worker
    let (_columns, rows) = match &estimate.target {
        QueryTarget::PreSizedWorker { address, worker_name } => {
            debug!("execute_query_get_rows - Executing on pre-sized worker: {}", worker_name);
            execute_query_on_worker_buffered(address, sql, user_id).await?
        }
        QueryTarget::TenantPool { service_addr, tenant_id } => {
            debug!("execute_query_get_rows - Executing on tenant pool: {}", tenant_id);
            execute_query_on_worker_buffered(service_addr, sql, user_id).await?
        }
        QueryTarget::WorkerPool => {
            let result = worker_client.execute_query(sql, user_id).await?;
            let cols: Vec<(String, String)> = result.columns.iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect();
            (cols, result.rows)
        }
    };
    
    Ok(rows)
}

/// Execute query on a specific worker address and return buffered results
async fn execute_query_on_worker_buffered(
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<(Vec<(String, String)>, Vec<Vec<String>>)> {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800))
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id: query_id.clone(),
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: 1800,
            max_rows: 0,
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut columns: Vec<(String, String)> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                // Extract columns from metadata
                columns = meta.columns.iter()
                    .zip(meta.column_types.iter())
                    .map(|(name, type_name)| (name.clone(), type_name.clone()))
                    .collect();
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch_data)) => {
                // Decode JSON data
                if !batch_data.data.is_empty() {
                    if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch_data.data) {
                        rows.extend(batch_rows);
                    }
                }
            }
            Some(proto::query_result_batch::Result::Error(err)) => {
                return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
            }
            _ => {}
        }
    }

    Ok((columns, rows))
}

/// Send simple result for generic streams
/// command_tag is used for commands like SET, BEGIN, COMMIT, RESET that return no data
async fn send_simple_result_generic<S>(socket: &mut S, columns: &[(&str, i32)], rows: &[Vec<String>], command_tag: Option<&str>) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Only send RowDescription and DataRows if we have columns
    // Commands like SET, BEGIN, RESET return no columns - only CommandComplete
    if !columns.is_empty() {
        // Build RowDescription
        let mut row_desc = vec![b'T'];
        let num_fields = columns.len() as i16;
        let mut fields_data = Vec::new();
        fields_data.extend_from_slice(&num_fields.to_be_bytes());

        for (col_name, _type_oid) in columns {
            fields_data.extend_from_slice(col_name.as_bytes());
            fields_data.push(0);
            fields_data.extend_from_slice(&0i32.to_be_bytes()); // table OID
            fields_data.extend_from_slice(&0i16.to_be_bytes()); // column attr
            fields_data.extend_from_slice(&25i32.to_be_bytes()); // type OID (text)
            fields_data.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            fields_data.extend_from_slice(&(-1i32).to_be_bytes()); // type mod
            fields_data.extend_from_slice(&0i16.to_be_bytes()); // format (text)
        }

        let row_desc_len = (4 + fields_data.len()) as u32;
        row_desc.extend_from_slice(&row_desc_len.to_be_bytes());
        row_desc.extend_from_slice(&fields_data);
        socket.write_all(&row_desc).await?;

        // Send DataRows
        for row in rows {
            let mut data_row = vec![b'D'];
            let mut row_data = Vec::new();
            row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

            for val in row {
                let bytes = val.as_bytes();
                row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                row_data.extend_from_slice(bytes);
            }

            let row_len = (4 + row_data.len()) as u32;
            data_row.extend_from_slice(&row_len.to_be_bytes());
            data_row.extend_from_slice(&row_data);
            socket.write_all(&data_row).await?;
        }
    }

    // Send CommandComplete with appropriate tag
    // Use provided command_tag for commands, or SELECT N for queries
    let cmd = match command_tag {
        Some(tag) => tag.to_string(),
        None => format!("SELECT {}", rows.len()),
    };
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    socket.flush().await?;

    Ok(())
}

/// Send only DataRows (no RowDescription) for Extended Query Protocol Execute phase
/// In Extended Protocol, RowDescription is sent during Describe, not Execute
/// `expected_column_count` ensures DataRow column count matches RowDescription declaration
#[allow(dead_code)]
async fn send_data_rows_only_generic<S>(socket: &mut S, rows: &[Vec<String>], row_count: usize, command_tag: Option<&str>, expected_column_count: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Send DataRows only - RowDescription was already sent during Describe
    for row in rows {
        let mut data_row = vec![b'D'];
        let mut row_data = Vec::new();
        
        // CRITICAL: Use expected_column_count to match RowDescription
        // This prevents "Index N out of bounds" errors in JDBC drivers
        let actual_cols = row.len();
        let cols_to_send = if expected_column_count > 0 { expected_column_count } else { actual_cols };
        
        row_data.extend_from_slice(&(cols_to_send as i16).to_be_bytes());

        for i in 0..cols_to_send {
            if i < actual_cols {
                let bytes = row[i].as_bytes();
                row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                row_data.extend_from_slice(bytes);
            } else {
                // Column expected but not in row - send NULL (-1 length)
                row_data.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }

        let row_len = (4 + row_data.len()) as u32;
        data_row.extend_from_slice(&row_len.to_be_bytes());
        data_row.extend_from_slice(&row_data);
        socket.write_all(&data_row).await?;
    }

    // Send CommandComplete with appropriate tag
    let cmd = match command_tag {
        Some(tag) => tag.to_string(),
        None => format!("SELECT {}", row_count),
    };
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    socket.flush().await?;

    Ok(())
}

/// Send COPY OUT protocol response for COPY TO STDOUT commands
/// Used by DuckDB postgres_query() with pg_use_text_protocol=true
/// Sends TEXT format (tab-separated values) which is simpler and compatible
#[allow(dead_code)]
async fn send_copy_out_response<S>(
    socket: &mut S,
    columns: &[(String, String)],
    rows: &[Vec<String>],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // CopyOutResponse (H) - indicates server is starting COPY OUT
    // Use TEXT format (0) since postgres_scanner can handle it and we don't need to
    // convert our string values to proper binary format for each type
    let num_cols = columns.len() as i16;
    let msg_len = 4 + 1 + 2 + (2 * columns.len() as i32);
    let mut copy_out_resp = vec![b'H'];
    copy_out_resp.extend_from_slice(&(msg_len as u32).to_be_bytes());
    copy_out_resp.push(0); // Overall format: 0 = TEXT
    copy_out_resp.extend_from_slice(&num_cols.to_be_bytes());
    for _ in 0..columns.len() {
        copy_out_resp.extend_from_slice(&0i16.to_be_bytes()); // Format per column: 0 = TEXT
    }
    socket.write_all(&copy_out_resp).await?;
    
    // Send each row as CopyData with TEXT format (tab-separated, newline terminated)
    for row in rows {
        let row_text = row.join("\t") + "\n";
        let row_bytes = row_text.as_bytes();
        
        let mut copy_data = vec![b'd'];
        copy_data.extend_from_slice(&((4 + row_bytes.len()) as u32).to_be_bytes());
        copy_data.extend_from_slice(row_bytes);
        socket.write_all(&copy_data).await?;
    }
    
    // CopyDone (c)
    socket.write_all(&[b'c', 0, 0, 0, 4]).await?;
    
    // CommandComplete (C) with COPY row count
    let cmd = format!("COPY {}", rows.len());
    let cmd_bytes = cmd.as_bytes();
    let mut complete = vec![b'C'];
    complete.extend_from_slice(&((4 + cmd_bytes.len() + 1) as u32).to_be_bytes());
    complete.extend_from_slice(cmd_bytes);
    complete.push(0);
    socket.write_all(&complete).await?;
    
    socket.flush().await?;
    
    debug!("Sent COPY OUT text response: {} rows", rows.len());
    Ok(())
}

/// Execute query with TRUE STREAMING - rows are sent to client as they arrive
/// This prevents OOM by never buffering the full result set
#[allow(dead_code)]
async fn execute_query_streaming(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_with_scaler(socket, worker_client, query_router, sql, user_id, None)
        .await
}

/// Execute query with SmartScaler (Formula 3) lifecycle management
/// 
/// `extended_protocol`: If true, we're in Extended Query Protocol and RowDescription
/// was already sent during Describe phase. We should NOT send it again in Execute.
#[allow(dead_code)]
async fn execute_query_streaming_with_scaler(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize> {
    execute_query_streaming_impl(socket, worker_client, query_router, sql, user_id, smart_scaler, false).await
}

/// Execute query for Extended Query Protocol (Describe already sent RowDescription)
#[allow(dead_code)]
async fn execute_query_streaming_extended(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
) -> anyhow::Result<usize> {
    execute_query_streaming_impl(socket, worker_client, query_router, sql, user_id, smart_scaler, true).await
}

/// Internal implementation with skip_row_description flag
/// 
/// Uses TRUE STREAMING with small batch sizes (100 rows) for backpressure control.
/// This prevents client OOM by ensuring data flows in manageable chunks.
#[allow(dead_code)]
async fn execute_query_streaming_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    // Handle ROLLBACK specially - must actually execute in DuckDB to clear aborted transaction state
    let sql_trimmed = sql.trim().to_uppercase();
    if sql_trimmed == "ROLLBACK" {
        debug!("Executing ROLLBACK in DuckDB to clear transaction state (streaming)");
        // Execute ROLLBACK in DuckDB to clear any aborted transaction state
        let _ = worker_client.execute_query("ROLLBACK", user_id).await;
        // Return success regardless of result (ROLLBACK always succeeds, even if no transaction)
        let result = QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("ROLLBACK".to_string()),
        };
        if skip_row_description {
            send_query_result_data_only(socket, result).await?;
        } else {
            send_query_result_immediate(socket, result).await?;
        }
        return Ok(0);
    }

    // Handle PostgreSQL-specific commands locally
    if let Some(result) = handle_pg_specific_command(sql) {
        if skip_row_description {
            // Extended protocol: only send DataRows and CommandComplete
            send_query_result_data_only(socket, result).await?;
        } else {
            send_query_result_immediate(socket, result).await?;
        }
        return Ok(0);
    }

    // Rewrite PostgreSQL-specific syntax to DuckDB equivalents
    let rewritten_sql = pg_compat::rewrite_pg_to_duckdb(sql);
    let sql = rewritten_sql.as_str();

    // Track active queries for HPA scaling
    metrics::query_started();

    // Route query to get estimate
    let estimate = query_router.route(sql).await;

    info!(
        data_mb = estimate.data_size_mb,
        target = ?estimate.target,
        skip_row_desc = skip_row_description,
        "Query routed for streaming execution"
    );

    // If SmartScaler is available, use it for worker selection and pre-sizing
    if let Some(scaler) = smart_scaler {
        return execute_with_smart_scaler_impl(
            socket,
            worker_client,
            query_router,
            scaler,
            sql,
            user_id,
            &estimate,
            skip_row_description,
        )
        .await;
    }

    // Fallback to legacy routing
    let (result, worker_name) = match estimate.target {
        QueryTarget::PreSizedWorker {
            address,
            worker_name,
        } => {
            info!("Using pre-sized worker {} at {}", worker_name, address);
            metrics::update_worker_active_queries(&worker_name, 1);
            let result = execute_query_streaming_to_worker_impl(socket, &address, sql, user_id, skip_row_description).await;
            metrics::update_worker_active_queries(&worker_name, 0);
            (result, Some(worker_name))
        }
        QueryTarget::TenantPool {
            service_addr,
            tenant_id,
        } => {
            info!("Using tenant pool {} at {}", tenant_id, service_addr);
            let result =
                execute_query_streaming_to_worker_impl(socket, &service_addr, sql, user_id, skip_row_description).await;
            (result, None)
        }
        QueryTarget::WorkerPool => {
            let result = execute_query_streaming_default_impl(socket, worker_client, sql, user_id, skip_row_description).await;
            (result, None)
        }
    };

    // Release the worker after query completion (success or failure)
    if let Some(ref name) = worker_name {
        query_router.release_worker(name, None).await;
        debug!("Released worker {} after streaming query", name);
    }

    metrics::query_ended();

    result
}

/// Execute query with SmartScaler (Formula 3) - full lifecycle
#[allow(dead_code)]
async fn execute_with_smart_scaler(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    scaler: &SmartScaler,
    sql: &str,
    user_id: &str,
    estimate: &crate::query_router::QueryEstimate,
) -> anyhow::Result<usize> {
    execute_with_smart_scaler_impl(socket, worker_client, query_router, scaler, sql, user_id, estimate, false).await
}

/// Execute query with SmartScaler - internal implementation with skip_row_description flag
#[allow(dead_code)]
async fn execute_with_smart_scaler_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    scaler: &SmartScaler,
    sql: &str,
    user_id: &str,
    estimate: &crate::query_router::QueryEstimate,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    let query_id = uuid::Uuid::new_v4().to_string();

    // Step 1: Select worker and determine if pre-sizing needed
    let selection = match scaler.select_worker(&query_id, estimate.data_size_mb).await {
        Some(s) => s,
        None => {
            // No workers available - evaluate HPA scaling
            let pending = scaler.get_pending_demand_mb().await + estimate.data_size_mb;
            let decision = scaler.evaluate_hpa_scaling(pending).await;
            if let Err(e) = scaler.apply_hpa_scaling(&decision).await {
                warn!("Failed to apply HPA scaling: {}", e);
            }

            // Queue the query and retry
            scaler.enqueue_query(&query_id, estimate.data_size_mb).await;

            // Wait for a worker to become available (with timeout)
            for _ in 0..30 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Some(_s) = scaler.select_worker(&query_id, estimate.data_size_mb).await {
                    scaler.dequeue_query(&query_id).await;
                    break;
                }
            }

            // If still no worker, fall back to worker pool
            warn!("No SmartScaler worker available after waiting, falling back to pool");
            metrics::query_ended();
            return execute_query_streaming_default_impl(socket, worker_client, sql, user_id, skip_row_description).await;
        }
    };

    info!(
        "SmartScaler selected worker {} (needs_resize={}, limit={}MB->{}MB)",
        selection.worker_name,
        selection.needs_resize,
        selection.current_limit_mb,
        selection.new_limit_mb
    );

    // Step 2: Pre-size worker if needed (VPA pre-assignment)
    if selection.needs_resize {
        if let Err(e) = scaler
            .presize_worker(&selection.worker_name, selection.new_limit_mb)
            .await
        {
            warn!("Failed to pre-size worker: {}", e);
            // Continue anyway - worker might still have enough capacity
        }
    }

    // Step 3: Register query start
    scaler
        .query_started(
            &selection.worker_name,
            &query_id,
            estimate.data_size_mb,
            selection.new_limit_mb,
        )
        .await;

    metrics::update_worker_active_queries(&selection.worker_name, 1);

    // Step 4: Start elastic monitoring in background
    let elastic_handle = scaler.start_elastic_monitoring(selection.worker_name.clone());

    // Step 5: Execute query
    let result =
        execute_query_streaming_to_worker_impl(socket, &selection.worker_address, sql, user_id, skip_row_description).await;

    // Step 6: Clean up - abort elastic monitoring
    elastic_handle.abort();

    // Step 7: Register query completion
    scaler
        .query_completed(&selection.worker_name, &query_id)
        .await;
    query_router
        .release_worker(&selection.worker_name, None)
        .await;

    metrics::update_worker_active_queries(&selection.worker_name, 0);
    metrics::query_ended();

    info!(
        "SmartScaler query {} completed on worker {}",
        query_id, selection.worker_name
    );

    result
}

/// Stream query results directly to PostgreSQL client
#[allow(dead_code)]
async fn execute_query_streaming_to_worker(
    socket: &mut tokio::net::TcpStream,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_to_worker_impl(socket, worker_addr, sql, user_id, false).await
}

/// Stream query results - internal implementation with skip_row_description flag
/// 
/// Uses BackpressureWriter for proper flow control:
/// - Bytes-based flushing (not just row count)
/// - Short flush timeouts to detect slow clients
/// - TCP-level backpressure via bounded BufWriter
#[allow(dead_code)]
async fn execute_query_streaming_to_worker_impl(
    socket: &mut tokio::net::TcpStream,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    info!("Streaming query to worker: {}", worker_addr);

    // Load backpressure config
    let config = PgWireConfig::default();
    let bp_config = BackpressureConfig {
        flush_threshold_bytes: config.flush_threshold_bytes,
        flush_threshold_rows: config.flush_threshold_rows,
        flush_timeout_secs: config.flush_timeout_secs,
        connection_check_interval_rows: config.connection_check_interval_rows,
        write_buffer_size: config.write_buffer_size,
    };

    // Connect to worker with large buffer settings for big result sets
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB per gRPC message

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800)) // 30 min for very large queries
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id: query_id.clone(),
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: config.query_timeout_secs as u32,
            max_rows: 0, // Unlimited - we stream
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    // Create backpressure-aware writer
    // This wraps the socket in BufWriter with small buffer for TCP backpressure
    // Use reborrow so we can still use socket for connection checks
    let mut writer = BackpressureWriter::new(&mut *socket, bp_config);
    let mut columns_sent = false;

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                // Send RowDescription once (unless in Extended Protocol where Describe already sent it)
                if !columns_sent && !skip_row_description {
                    let row_desc = build_row_description(&meta.columns, &meta.column_types);
                    writer.write_bytes(&row_desc).await?;
                }
                columns_sent = true;
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                if !batch.data.is_empty() {
                    if let Ok(rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                        for row in rows {
                            // Build and write DataRow with backpressure tracking
                            let data_row = build_data_row(&row);
                            
                            match writer.write_bytes(&data_row).await {
                                Ok(_) => {}
                                Err(e) if is_disconnect_error(&e) => {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows, {} bytes", 
                                        stats.rows_sent, stats.bytes_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                Err(e) => return Err(e.into()),
                            }
                            writer.row_sent();

                            // Check if we should flush (based on bytes AND rows)
                            if writer.should_flush() {
                                match writer.flush_with_backpressure().await {
                                    Ok(true) => {} // Normal flush
                                    Ok(false) => {
                                        // Slow client detected - continue but log
                                        let stats = writer.stats();
                                        debug!("Slow client detected after {} rows", stats.rows_sent);
                                    }
                                    Err(e) if is_disconnect_error(&e) => {
                                        let stats = writer.stats();
                                        warn!("Client disconnected during flush after {} rows", stats.rows_sent);
                                        return Err(anyhow::anyhow!("Client disconnected"));
                                    }
                                    Err(e) => {
                                        // Timeout or other error - client too slow
                                        warn!("Flush failed: {} - client may be overwhelmed", e);
                                        return Err(anyhow::anyhow!("Client too slow: {}", e));
                                    }
                                }
                            }

                            // Periodically check if client is still connected
                            if writer.should_check_connection() {
                                if !writer.is_connected().await {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                
                                let stats = writer.stats();
                                debug!("Streaming progress: {} rows, {} bytes, {} flushes", 
                                    stats.rows_sent, stats.bytes_sent, stats.flush_count);
                            }

                            // Log progress for very large queries
                            let stats = writer.stats();
                            if stats.rows_sent % 1_000_000 == 0 && stats.rows_sent > 0 {
                                info!("Streaming progress: {} rows, {} MB sent", 
                                    stats.rows_sent, stats.bytes_sent / (1024 * 1024));
                            }
                        }
                    }
                }
            }
            Some(proto::query_result_batch::Result::Error(err)) => {
                return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
            }
            _ => {}
        }
    }

    // If no columns were sent (empty result) and not in extended protocol, send empty row description
    if !columns_sent && !skip_row_description {
        let row_desc = build_row_description(&[], &[]);
        writer.write_bytes(&row_desc).await?;
    }

    // Send CommandComplete
    let stats = writer.stats();
    let tag = format!("SELECT {}", stats.rows_sent);
    let cmd_complete = build_command_complete(&tag);
    writer.write_bytes(&cmd_complete).await?;

    // Final flush to ensure all data is sent
    writer.force_flush().await?;

    let final_stats = writer.stats();
    info!(
        "Streaming complete: {} rows, {} bytes, {} flushes ({} slow)",
        final_stats.rows_sent, final_stats.bytes_sent, 
        final_stats.flush_count, final_stats.slow_flush_count
    );

    Ok(final_stats.rows_sent)
}

/// Stream query results using the default worker client
#[allow(dead_code)]
async fn execute_query_streaming_default(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<usize> {
    execute_query_streaming_default_impl(socket, worker_client, sql, user_id, false).await
}

/// Stream query results using the default worker client - internal implementation
/// 
/// Uses BackpressureWriter for proper flow control to prevent client overwhelm.
#[allow(dead_code)]
async fn execute_query_streaming_default_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    // Load backpressure config
    let config = PgWireConfig::default();
    let bp_config = BackpressureConfig {
        flush_threshold_bytes: config.flush_threshold_bytes,
        flush_threshold_rows: config.flush_threshold_rows,
        flush_timeout_secs: config.flush_timeout_secs,
        connection_check_interval_rows: config.connection_check_interval_rows,
        write_buffer_size: config.write_buffer_size,
    };

    // For the default path, we still need to use the existing client
    // but we'll stream the results to the client as they come
    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            // Create backpressure-aware writer with reborrow
            let mut writer = BackpressureWriter::new(&mut *socket, bp_config);
            let mut columns_sent = false;

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata {
                        columns,
                        column_types,
                    } => {
                        // Send RowDescription once (unless in Extended Protocol where Describe already sent it)
                        if !columns_sent && !skip_row_description {
                            let row_desc = build_row_description(&columns, &column_types);
                            writer.write_bytes(&row_desc).await?;
                        }
                        columns_sent = true;
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            // Build and write DataRow with backpressure tracking
                            let data_row = build_data_row(&row);
                            
                            match writer.write_bytes(&data_row).await {
                                Ok(_) => {}
                                Err(e) if is_disconnect_error(&e) => {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                                Err(e) => return Err(e.into()),
                            }
                            writer.row_sent();

                            // Check if we should flush (based on bytes AND rows)
                            if writer.should_flush() {
                                match writer.flush_with_backpressure().await {
                                    Ok(true) => {} // Normal flush
                                    Ok(false) => {
                                        // Slow client - continue but log
                                        let stats = writer.stats();
                                        debug!("Slow client detected after {} rows", stats.rows_sent);
                                    }
                                    Err(e) if is_disconnect_error(&e) => {
                                        let stats = writer.stats();
                                        warn!("Client disconnected during flush after {} rows", stats.rows_sent);
                                        return Err(anyhow::anyhow!("Client disconnected"));
                                    }
                                    Err(e) => {
                                        warn!("Flush failed: {} - client may be overwhelmed", e);
                                        return Err(anyhow::anyhow!("Client too slow: {}", e));
                                    }
                                }
                            }

                            // Periodically check if client is still connected
                            if writer.should_check_connection() {
                                if !writer.is_connected().await {
                                    let stats = writer.stats();
                                    warn!("Client disconnected during streaming after {} rows", stats.rows_sent);
                                    return Err(anyhow::anyhow!("Client disconnected"));
                                }
                            }
                        }
                    }
                    StreamingBatch::Error(msg) => {
                        return Err(anyhow::anyhow!("{}", msg));
                    }
                }
            }

            // If no columns were sent and not in extended protocol, send empty row description
            if !columns_sent && !skip_row_description {
                let row_desc = build_row_description(&[], &[]);
                writer.write_bytes(&row_desc).await?;
            }

            let stats = writer.stats();
            let tag = format!("SELECT {}", stats.rows_sent);
            let cmd_complete = build_command_complete(&tag);
            writer.write_bytes(&cmd_complete).await?;

            // Final flush
            writer.force_flush().await?;

            let final_stats = writer.stats();
            debug!(
                "Streaming complete: {} rows, {} bytes, {} flushes",
                final_stats.rows_sent, final_stats.bytes_sent, final_stats.flush_count
            );

            Ok(final_stats.rows_sent)
        }
        Err(e) => {
            // Fallback to non-streaming for compatibility
            warn!("Streaming not available, falling back to buffered: {}", e);
            let result = execute_query_buffered(worker_client, sql, user_id).await?;
            if skip_row_description {
                send_query_result_data_only(socket, result).await
            } else {
                send_query_result_immediate(socket, result).await
            }
        }
    }
}

/// Buffered execution fallback
async fn execute_query_buffered(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> anyhow::Result<QueryExecutionResult> {
    match worker_client.execute_query(sql, user_id).await {
        Ok(result) => Ok(QueryExecutionResult {
            columns: result
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.type_name.clone()))
                .collect(),
            rows: result.rows,
            row_count: result.total_rows as usize,
            command_tag: None,
        }),
        Err(e) => {
            warn!("Worker pool error: {}", e);
            execute_local_fallback(sql).await
        }
    }
}

/// Extract inner SELECT from COPY command if present
/// COPY (SELECT ...) TO STDOUT (FORMAT "binary") -> SELECT ...
fn extract_copy_inner_query(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    
    // Check if it's a COPY (...) TO STDOUT command
    if !sql_trimmed.starts_with("COPY (") && !sql_trimmed.starts_with("COPY(") {
        return None;
    }
    
    // Find the matching closing parenthesis for the subquery
    let start_idx = sql.find('(')? + 1;
    let mut depth = 1;
    let mut end_idx = start_idx;
    
    for (i, c) in sql[start_idx..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end_idx = start_idx + i;
                    break;
                }
            }
            _ => {}
        }
    }
    
    if depth != 0 {
        return None;
    }
    
    let inner_query = sql[start_idx..end_idx].trim().to_string();
    if inner_query.to_uppercase().starts_with("SELECT") {
        Some(inner_query)
    } else {
        None
    }
}

/// Transaction status constants for PostgreSQL wire protocol
/// These are sent in the ReadyForQuery message to tell the client the transaction state
/// CRITICAL: JDBC uses this to decide whether to use server-side cursors for streaming!
pub const TRANSACTION_STATUS_IDLE: u8 = b'I';
pub const TRANSACTION_STATUS_IN_TRANSACTION: u8 = b'T';
pub const TRANSACTION_STATUS_ERROR: u8 = b'E';

/// Check if a SQL command changes transaction state
/// Returns (new_transaction_status, is_transaction_command)
/// This is essential for JDBC cursor-based streaming to work!
fn get_transaction_state_change(sql: &str, current_status: u8) -> (u8, bool) {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    
    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") 
        || sql_trimmed.starts_with("START TRANSACTION") {
        // BEGIN starts a transaction - status becomes 'T'
        (TRANSACTION_STATUS_IN_TRANSACTION, true)
    } else if sql_trimmed == "COMMIT" || sql_trimmed == "END" {
        // COMMIT ends a transaction - status becomes 'I'
        (TRANSACTION_STATUS_IDLE, true)
    } else if sql_trimmed == "ROLLBACK" || sql_trimmed == "ABORT" {
        // ROLLBACK ends a transaction - status becomes 'I'
        (TRANSACTION_STATUS_IDLE, true)
    } else {
        // No transaction state change
        (current_status, false)
    }
}

fn handle_pg_specific_command(sql: &str) -> Option<QueryExecutionResult> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    if sql_trimmed.starts_with("SET ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("SET".to_string()),
        });
    }

    if sql_trimmed.starts_with("SHOW ") {
        if sql_trimmed.contains("TRANSACTION ISOLATION") {
            return Some(QueryExecutionResult {
                columns: vec![("transaction_isolation".to_string(), "text".to_string())],
                rows: vec![vec!["read committed".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }
        return Some(QueryExecutionResult {
            columns: vec![("setting".to_string(), "text".to_string())],
            rows: vec![vec!["".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    if sql_trimmed.starts_with("RESET ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("RESET".to_string()),
        });
    }

    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") 
        || sql_trimmed.starts_with("START TRANSACTION") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("BEGIN".to_string()),
        });
    }

    if sql_trimmed == "COMMIT" || sql_trimmed == "END" {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("COMMIT".to_string()),
        });
    }

    if sql_trimmed == "ROLLBACK" || sql_trimmed == "ABORT" {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("ROLLBACK".to_string()),
        });
    }

    if sql_trimmed.starts_with("DISCARD ")
        || sql_trimmed.starts_with("DEALLOCATE ")
    {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    // NOTE: DECLARE CURSOR, FETCH, and CLOSE are handled in run_query_loop_generic
    // because they require access to per-connection cursor state

    // =========================================================================
    // TABLEAU TEMP TABLE INTERCEPTION
    // =========================================================================
    // Tableau creates temporary tables like #Tableau_N_GUID_N_Connect_Check for
    // connection validation. Since Tavana uses stateless workers, temp tables
    // created on one worker don't exist on another. We intercept these and
    // return fake success responses to make Tableau's connection check pass.
    //
    // Pattern: #Tableau_<N>_<GUID>_<N>_Connect_Check
    // =========================================================================
    
    // =========================================================================
    // TABLEAU TEMP TABLE INTERCEPTION - Comprehensive pattern matching
    // =========================================================================
    // Tableau creates temporary tables like #Tableau_N_GUID_N_Connect_Check
    // and also uses SELECT INTO for connection validation.
    // We intercept ALL queries containing #Tableau_ patterns.
    // =========================================================================
    
    // Check if this is ANY Tableau temp table operation
    let is_tableau_temp = sql_upper.contains("#TABLEAU_");
    
    if is_tableau_temp {
        tracing::info!("Intercepting Tableau temp table query: {}", 
            if sql.len() > 100 { &sql[..100] } else { sql });
        
        // CREATE TABLE (with or without TEMP/TEMPORARY keyword)
        if sql_upper.contains("CREATE") && sql_upper.contains("TABLE") {
            tracing::info!("Intercepted Tableau CREATE TABLE - returning success");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
                command_tag: Some("CREATE TABLE".to_string()),
            });
        }
        
        // SELECT INTO (Tableau uses this for temp table creation)
        if sql_upper.contains("SELECT") && sql_upper.contains("INTO") {
            tracing::info!("Intercepted Tableau SELECT INTO - returning success");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
                command_tag: Some("SELECT 0".to_string()),
            });
        }
        
        // DROP TABLE
        if sql_upper.contains("DROP") {
            tracing::info!("Intercepted Tableau DROP TABLE - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("DROP TABLE".to_string()),
            });
        }
        
        // INSERT INTO
        if sql_upper.contains("INSERT") {
            tracing::info!("Intercepted Tableau INSERT - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 1,
                command_tag: Some("INSERT 0 1".to_string()),
            });
        }
        
        // SELECT FROM #Tableau_ table - return one success row
        if sql_upper.contains("SELECT") && sql_upper.contains("FROM") {
            tracing::info!("Intercepted Tableau SELECT FROM temp table - returning success row");
            return Some(QueryExecutionResult {
                columns: vec![("x".to_string(), "integer".to_string())],
                rows: vec![vec!["1".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }
        
        // Any other query referencing #Tableau_ - return empty success
        tracing::info!("Intercepted unknown Tableau temp table query - returning empty success");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    // NOTE: Both information_schema AND pg_catalog queries pass through to DuckDB.
    // DuckDB has full native support for:
    // - information_schema views (tables, columns, schemata, etc.)
    // - pg_catalog tables (pg_class, pg_type, pg_namespace, pg_attribute, pg_proc, pg_settings, etc.)
    // 
    // This allows BI tools like DBeaver, Tableau, and PowerBI to:
    // - Discover tables/views created via Initial SQL
    // - Query metadata with proper PostgreSQL compatibility
    // - Use parameterized metadata queries (JDBC drivers)
    //
    // IMPORTANT: Do NOT intercept pg_catalog queries - DuckDB returns real metadata!

    None
}

async fn execute_local_fallback(sql: &str) -> anyhow::Result<QueryExecutionResult> {
    let sql_upper = sql.to_uppercase();

    if sql_upper.starts_with("SELECT 1") {
        return Ok(QueryExecutionResult {
            columns: vec![("result".to_string(), "int4".to_string())],
            rows: vec![vec!["1".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    if sql_upper.starts_with("SELECT VERSION()") {
        return Ok(QueryExecutionResult {
            columns: vec![("version".to_string(), "text".to_string())],
            rows: vec![vec!["Tavana DuckDB 1.0".to_string()]],
            row_count: 1,
            command_tag: None,
        });
    }

    Ok(QueryExecutionResult {
            columns: vec![("result".to_string(), "text".to_string())],
        rows: vec![],
        row_count: 0,
        command_tag: None,
    })
}

// ===== PostgreSQL Wire Protocol Message Helpers =====

struct QueryExecutionResult {
    columns: Vec<(String, String)>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    command_tag: Option<String>,
}

impl From<CursorResult> for QueryExecutionResult {
    fn from(r: CursorResult) -> Self {
        Self {
            columns: r.columns,
            rows: r.rows,
            row_count: r.row_count,
            command_tag: r.command_tag,
        }
    }
}

/// Send RowDescription message
async fn send_row_description(
    socket: &mut tokio::net::TcpStream,
    columns: &[String],
    column_types: &[String],
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut row_desc = Vec::new();
    row_desc.push(b'T');

    let field_count = columns.len() as i16;
    let mut fields_data = Vec::new();
    fields_data.extend_from_slice(&field_count.to_be_bytes());

    for (i, name) in columns.iter().enumerate() {
        let type_name = column_types.get(i).map(|s| s.as_str()).unwrap_or("text");

        fields_data.extend_from_slice(name.as_bytes());
        fields_data.push(0);
        fields_data.extend_from_slice(&0u32.to_be_bytes()); // table OID
        fields_data.extend_from_slice(&0i16.to_be_bytes()); // column attr
        fields_data.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        fields_data.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        fields_data.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        fields_data.extend_from_slice(&0i16.to_be_bytes()); // format code (text)
    }

    let len = (4 + fields_data.len()) as u32;
    row_desc.extend_from_slice(&len.to_be_bytes());
    row_desc.extend_from_slice(&fields_data);
    socket.write_all(&row_desc).await?;

    Ok(())
}

/// Send a single DataRow message
async fn send_data_row(socket: &mut tokio::net::TcpStream, row: &[String]) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
        data_row.push(b'D');

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
        row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

        for value in row {
        if value.is_empty() || value == "null" || value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else {
                row_data.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row_data.extend_from_slice(value.as_bytes());
            }
        }

        let len = (4 + row_data.len()) as u32;
        data_row.extend_from_slice(&len.to_be_bytes());
        data_row.extend_from_slice(&row_data);
        socket.write_all(&data_row).await?;

    Ok(())
}

/// Send query result immediately (for small results)
async fn send_query_result_immediate(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<usize> {
    use tokio::io::AsyncWriteExt;

    if result.columns.is_empty() {
        let tag = result
            .command_tag
            .unwrap_or_else(|| format!("OK {}", result.row_count));
        send_command_complete(socket, &tag).await?;
        return Ok(0);
    }

    // Send row description
    let columns: Vec<String> = result.columns.iter().map(|(n, _)| n.clone()).collect();
    let types: Vec<String> = result.columns.iter().map(|(_, t)| t.clone()).collect();
    send_row_description(socket, &columns, &types).await?;

    // Send data rows
    let mut count = 0;
    for row in &result.rows {
        send_data_row(socket, row).await?;
        count += 1;

        // Flush periodically
        if count % STREAMING_BATCH_SIZE == 0 {
            socket.flush().await?;
        }
    }

    // Use custom command_tag if provided (e.g., "FETCH 100"), otherwise "SELECT n"
    let tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
    send_command_complete(socket, &tag).await?;

    Ok(result.rows.len())
}

/// Send query result without RowDescription (for Extended Query Protocol where Describe already sent it)
async fn send_query_result_data_only(
    socket: &mut tokio::net::TcpStream,
    result: QueryExecutionResult,
) -> anyhow::Result<usize> {
    use tokio::io::AsyncWriteExt;

    if result.columns.is_empty() {
        let tag = result
            .command_tag
            .unwrap_or_else(|| format!("OK {}", result.row_count));
        send_command_complete(socket, &tag).await?;
        return Ok(0);
    }

    // DO NOT send row description - Describe already sent it in Extended Protocol
    
    // Send data rows only
    let mut count = 0;
    for row in &result.rows {
        send_data_row(socket, row).await?;
        count += 1;

        // Flush periodically
        if count % STREAMING_BATCH_SIZE == 0 {
            socket.flush().await?;
        }
    }

    let tag = format!("SELECT {}", result.rows.len());
    send_command_complete(socket, &tag).await?;

    Ok(result.rows.len())
}

async fn send_command_complete(
    socket: &mut tokio::net::TcpStream,
    tag: &str,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut msg = Vec::new();
    msg.push(b'C');
    let len = 4 + tag.len() + 1;
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(tag.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

async fn send_error(socket: &mut tokio::net::TcpStream, message: &str) -> anyhow::Result<()> {
    // Use the new classified error system
    send_classified_error_tcp(socket, message).await
}

/// Send a classified error to a TcpStream with proper SQLSTATE, hint, and detail
async fn send_classified_error_tcp(socket: &mut tokio::net::TcpStream, raw_message: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    
    let classified = classify_error(raw_message);
    
    // Log with full context
    debug!(
        category = %classified.category,
        sqlstate = %classified.sqlstate,
        message = %classified.message,
        "Sending classified error to client (TCP)"
    );
    
    let mut fields = Vec::new();
    
    // Severity
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // Severity (non-localized)
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);
    
    // SQLSTATE code
    fields.push(b'C');
    fields.extend_from_slice(classified.sqlstate.as_bytes());
    fields.push(0);
    
    // Message
    fields.push(b'M');
    fields.extend_from_slice(classified.message.as_bytes());
    fields.push(0);
    
    // Detail
    if let Some(ref detail) = classified.detail {
        if !detail.is_empty() {
            fields.push(b'D');
            fields.extend_from_slice(detail.as_bytes());
            fields.push(0);
        }
    }
    
    // Hint
    if let Some(ref hint) = classified.hint {
        fields.push(b'H');
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);
    }
    
    // Terminator
    fields.push(0);
    
    let mut msg = Vec::new();
    msg.push(b'E');
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

/// Send error response with specific SQLSTATE code (legacy - for specific cases)
#[allow(dead_code)]
async fn send_error_response(socket: &mut tokio::net::TcpStream, code: &str, message: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let code_bytes = format!("{}\0", code);

    let mut msg = Vec::new();
    msg.push(b'E');
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C');
    fields.extend_from_slice(code_bytes.as_bytes());
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    socket.flush().await?;
    Ok(())
}

#[allow(dead_code)]
async fn send_notice(socket: &mut tokio::net::TcpStream, message: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut msg = Vec::new();
    msg.push(b'N'); // NoticeResponse
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"WARNING\0");
    fields.push(b'C');
    fields.extend_from_slice(b"01000\0"); // Warning code
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);
    let len = 4 + fields.len();
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(&fields);
    socket.write_all(&msg).await?;
    Ok(())
}

async fn send_parameter_status(
    socket: &mut tokio::net::TcpStream,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut msg = Vec::new();
    msg.push(b'S');
    let len = 4 + key.len() + 1 + value.len() + 1;
    msg.extend_from_slice(&(len as u32).to_be_bytes());
    msg.extend_from_slice(key.as_bytes());
    msg.push(0);
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    socket.write_all(&msg).await?;
    Ok(())
}

fn extract_startup_param(msg: &[u8], key: &str) -> Option<String> {
    if msg.len() < 8 {
        return None;
    }
    let params = &msg[4..];
    let mut iter = params.split(|&b| b == 0);
    while let Some(k) = iter.next() {
        if k.is_empty() {
            break;
        }
        let v = iter.next()?;
        if k == key.as_bytes() {
            return String::from_utf8(v.to_vec()).ok();
        }
    }
    None
}

fn pg_type_oid(type_name: &str) -> u32 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" | "int32" => 23,
        "int8" | "bigint" | "int64" => 20,
        "int2" | "smallint" | "int16" => 21,
        "float4" | "real" | "float" => 700,
        "float8" | "double" | "float64" => 701,
        "bool" | "boolean" => 16,
        "timestamp" | "timestamptz" => 1184,
        "date" => 1082,
        _ => 25, // TEXT
    }
}

fn pg_type_len(type_name: &str) -> i16 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" | "int32" => 4,
        "int8" | "bigint" | "int64" => 8,
        "int2" | "smallint" | "int16" => 2,
        "float4" | "real" | "float" => 4,
        "float8" | "double" | "float64" => 8,
        "bool" | "boolean" => 1,
        _ => -1, // Variable length
    }
}

// ===== Password Authentication =====
// Supports both MD5 (legacy) and Cleartext (for JWT/token auth)
// Required for Tableau Desktop, DBeaver, and other PostgreSQL clients

/// Perform cleartext password authentication for TcpStream
/// Uses cleartext to allow JWTs, API keys, or other tokens in the password field
async fn perform_md5_auth(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    perform_cleartext_auth_internal(socket, user, Some((auth_gateway, client_ip))).await
}

/// Perform cleartext password authentication with optional gateway validation
#[allow(dead_code)]
async fn perform_cleartext_auth_with_gateway(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    perform_cleartext_auth_internal(socket, user, Some((auth_gateway, client_ip))).await
}

/// Internal implementation of cleartext password authentication
async fn perform_cleartext_auth_internal(
    socket: &mut tokio::net::TcpStream,
    user: &str,
    auth_info: Option<(Option<&Arc<AuthGateway>>, Option<String>)>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>> {
    // Send AuthenticationCleartextPassword (R with auth type 3)
    // This allows clients to send JWTs/tokens directly without MD5 hashing
    let auth_req = [b'R', 0, 0, 0, 8, 0, 0, 0, 3]; // auth type 3 = Cleartext
    socket.write_all(&auth_req).await?;
    socket.flush().await?;
    
    debug!("Sent cleartext auth request to client for user: {}", user);
    
    // Read password response
    let mut msg_type = [0u8; 1];
    socket.read_exact(&mut msg_type).await?;
    
    if msg_type[0] != b'p' {
        return Err(anyhow::anyhow!("Expected password message, got: {:?}", msg_type[0]));
    }
    
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut password_data = vec![0u8; len];
    socket.read_exact(&mut password_data).await?;
    
    // Extract password (null-terminated string)
    let password = String::from_utf8_lossy(&password_data)
        .trim_end_matches('\0')
        .to_string();
    
    // Validate with auth gateway if available
    if let Some((gateway_opt, client_ip)) = auth_info {
        if let Some(gateway) = gateway_opt {
            // Check if gateway is in passthrough mode
            if !gateway.is_passthrough() {
                let context = AuthContext {
                    client_ip,
                    application_name: None,
                    client_id: None,
                };
                
                match gateway.authenticate(user, &password, context).await {
                    crate::auth::identity::AuthResult::Success(principal) => {
                        // Send AuthenticationOk
                        socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
                        socket.flush().await?;
                        info!(
                            user_id = %principal.id,
                            principal_type = %principal.principal_type,
                            "Authentication successful for user: {}",
                            user
                        );
                        return Ok(Some(principal));
                    }
                    crate::auth::identity::AuthResult::InvalidCredentials(reason) => {
                        warn!("Authentication failed for user {}: {}", user, reason);
                        send_error_response(socket, "28P01", "password authentication failed").await?;
                        return Err(anyhow::anyhow!("Authentication failed: {}", reason));
                    }
                    crate::auth::identity::AuthResult::Expired => {
                        warn!("Authentication expired for user {}", user);
                        send_error_response(socket, "28P01", "authentication token expired").await?;
                        return Err(anyhow::anyhow!("Token expired"));
                    }
                    crate::auth::identity::AuthResult::ProviderError(msg) => {
                        error!("Auth provider error for user {}: {}", user, msg);
                        send_error_response(socket, "XX000", "authentication service error").await?;
                        return Err(anyhow::anyhow!("Provider error: {}", msg));
                    }
                    crate::auth::identity::AuthResult::NotAuthenticated => {
                        // Fall through to passthrough mode
                    }
                }
            }
        }
    }
    
    // Passthrough mode - accept any password
    debug!("Received password response, accepting (passthrough mode)");
    
    // Send AuthenticationOk
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    socket.flush().await?;
    
    info!("Authentication completed for user: {} (passthrough)", user);
    Ok(None)
}

/// Perform cleartext password authentication for generic async streams (TLS connections)
async fn perform_md5_auth_generic<S>(
    socket: &mut S,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    perform_cleartext_auth_generic_with_gateway(socket, user, auth_gateway, client_ip).await
}

/// Perform cleartext password authentication for TLS with optional gateway
#[allow(dead_code)]
async fn perform_cleartext_auth_generic_with_gateway<S>(
    socket: &mut S,
    user: &str,
    auth_gateway: Option<&Arc<AuthGateway>>,
    client_ip: Option<String>,
) -> anyhow::Result<Option<AuthenticatedPrincipal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Send AuthenticationCleartextPassword (R with auth type 3)
    let auth_req = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    socket.write_all(&auth_req).await?;
    socket.flush().await?;
    
    debug!("Sent cleartext auth request to client for user: {} (TLS)", user);
    
    // Read password response
    let mut msg_type = [0u8; 1];
    socket.read_exact(&mut msg_type).await?;
    
    if msg_type[0] != b'p' {
        return Err(anyhow::anyhow!("Expected password message, got: {:?}", msg_type[0]));
    }
    
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut password_data = vec![0u8; len];
    socket.read_exact(&mut password_data).await?;
    
    // Extract password
    let password = String::from_utf8_lossy(&password_data)
        .trim_end_matches('\0')
        .to_string();
    
    // Validate with auth gateway if available and not passthrough
    if let Some(gateway) = auth_gateway {
        if !gateway.is_passthrough() {
            let context = AuthContext {
                client_ip,
                application_name: None,
                client_id: None,
            };
            
            match gateway.authenticate(user, &password, context).await {
                crate::auth::identity::AuthResult::Success(principal) => {
                    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
                    socket.flush().await?;
                    info!(
                        user_id = %principal.id,
                        "Authentication successful for user: {} (TLS)",
                        user
                    );
                    return Ok(Some(principal));
                }
                crate::auth::identity::AuthResult::InvalidCredentials(reason) => {
                    warn!("Authentication failed for user {} (TLS): {}", user, reason);
                    // Note: send_error_response is for TcpStream, need generic version
                    return Err(anyhow::anyhow!("Authentication failed: {}", reason));
                }
                _ => {
                    // Fall through to passthrough
                }
            }
        }
    }
    
    // Passthrough mode
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    socket.flush().await?;
    
    info!("Authentication completed for user: {} (TLS passthrough)", user);
    Ok(None)
}

// ===== Extended Query Protocol Handlers =====

/// Handle Parse message - extract and store the SQL query
async fn handle_parse_extended(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<Option<String>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    // Parse message format:
    // - Statement name (null-terminated string)
    // - Query string (null-terminated string)
    // - Number of parameter types (Int16)
    // - Parameter types (Int32 each)

    // Find the first null byte (end of statement name)
    let stmt_end = data.iter().position(|&b| b == 0).unwrap_or(0);
    // Find the second null byte (end of query)
    let query_start = stmt_end + 1;
    let query_end = data[query_start..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(data.len() - query_start)
        + query_start;

    let query = String::from_utf8_lossy(&data[query_start..query_end]).to_string();
    debug!(
        "Extended query protocol - Parse: {}",
        &query[..query.len().min(100)]
    );

    socket.write_all(&[b'1', 0, 0, 0, 4]).await?; // ParseComplete
    socket.flush().await?;

    if query.is_empty() {
        Ok(None)
    } else {
        Ok(Some(query))
    }
}

async fn handle_bind(socket: &mut tokio::net::TcpStream, buf: &mut [u8; 4]) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    // Parse Bind message to log format codes
    // Format: portal_name\0 + stmt_name\0 + param_format_count(i16) + param_formats + 
    //         param_values_count(i16) + params + result_format_count(i16) + result_formats
    let mut offset = 0;
    // Skip portal name
    while offset < data.len() && data[offset] != 0 { offset += 1; }
    offset += 1; // skip null
    // Skip statement name  
    while offset < data.len() && data[offset] != 0 { offset += 1; }
    offset += 1; // skip null
    
    // Skip parameter format codes
    if offset + 2 <= data.len() {
        let param_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2 + param_format_count * 2;
    }
    
    // Skip parameter values
    if offset + 2 <= data.len() {
        let param_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2;
        for _ in 0..param_count {
            if offset + 4 <= data.len() {
                let param_len = i32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
                offset += 4;
                if param_len > 0 {
                    offset += param_len as usize;
                }
            }
        }
    }
    
    // Parse result format codes
    if offset + 2 <= data.len() {
        let result_format_count = i16::from_be_bytes([data[offset], data[offset+1]]) as usize;
        offset += 2;
        if result_format_count > 0 && offset + result_format_count * 2 <= data.len() {
            let mut has_binary = false;
            for i in 0..result_format_count {
                let fmt = i16::from_be_bytes([data[offset + i*2], data[offset + i*2 + 1]]);
                if fmt == 1 {
                    has_binary = true;
                }
            }
            if has_binary {
                // CRITICAL: Client requested binary format but Tavana only supports text
                warn!("Bind: client requested BINARY format for {} result columns! Tavana only supports TEXT format, this may cause client parsing errors.", result_format_count);
            } else {
                debug!("Bind: client requested TEXT format for {} result columns", result_format_count);
            }
        }
    }
    
    socket.write_all(&[b'2', 0, 0, 0, 4]).await?; // BindComplete
    socket.flush().await?;
    Ok(())
}

/// Handle Describe message for extended query protocol
/// Returns (sent_row_description, column_count) tuple:
/// - sent_row_description: true if RowDescription was sent, false if NoData was sent
/// - column_count: number of columns declared in RowDescription (0 if NoData)
async fn handle_describe(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
    prepared_query: Option<&str>,
    worker_client: &WorkerClient,
    user_id: &str,
) -> anyhow::Result<(bool, usize)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    // Describe message: first byte is 'S' (statement) or 'P' (portal)
    // followed by the name (null-terminated)
    let describe_type = if data.is_empty() { b'S' } else { data[0] };
    
    debug!("Extended query protocol - Describe type: {}", describe_type as char);
    
    // If we have a prepared query, execute it to get column info
    if let Some(sql) = prepared_query {
        // Check for PostgreSQL-specific commands first
        if let Some(result) = handle_pg_specific_command(sql) {
            // Send ParameterDescription (no parameters)
            socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?; // 0 parameters
            
            // Send RowDescription for the columns
            if result.columns.is_empty() {
                socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                return Ok((false, 0));
            } else {
                let col_count = result.columns.len();
                send_row_description_for_describe(socket, &result.columns).await?;
                return Ok((true, col_count));
            }
        } else {
            // Check if query has parameters ($1, $2, etc.)
            let has_parameters = sql.contains("$1") || sql.contains("$2") || sql.contains("$3");
            
            // For parameterized queries, replace params with NULL
            let schema_sql = if has_parameters {
                sql.replace("$1", "NULL")
                    .replace("$2", "NULL")
                    .replace("$3", "NULL")
                    .replace("$4", "NULL")
                    .replace("$5", "NULL")
                    .replace("$6", "NULL")
                    .replace("$7", "NULL")
                    .replace("$8", "NULL")
                    .replace("$9", "NULL")
            } else {
                sql.to_string()
            };
            
            // CRITICAL: Apply pg_compat rewriting BEFORE executing schema query
            // This fixes DBeaver crashes from ::regclass, ::regtype etc. types
            let schema_sql_rewritten = pg_compat::rewrite_pg_to_duckdb(&schema_sql);
            
            // Use LIMIT 0 to get schema without fetching any rows (fixes doubled latency)
            let schema_query = format!("SELECT * FROM ({}) AS _schema_query LIMIT 0", 
                schema_sql_rewritten.trim_end_matches(';'));
            
            match worker_client.execute_query(&schema_query, user_id).await {
                Ok(result) => {
                    // Send ParameterDescription (no parameters)
                    socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?; // 0 parameters
                    
                    // Build columns list
                    let columns: Vec<(String, String)> = result.columns.iter()
                        .map(|c| (c.name.clone(), c.type_name.clone()))
                        .collect();
                    
                    if columns.is_empty() {
                        socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
                        return Ok((false, 0));
                    } else {
                        let col_count = columns.len();
                        info!("Extended Protocol (non-TLS) - Describe: {} columns: {:?}", 
                            col_count, 
                            columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                        );
                        send_row_description_for_describe(socket, &columns).await?;
                        return Ok((true, col_count));
                    }
                }
                Err(e) => {
                    warn!("Extended Protocol (non-TLS) - Describe failed: {}, sending NoData", e);
                    // Send ParameterDescription (no parameters) and NoData as fallback
                    socket.write_all(&[b't', 0, 0, 0, 6, 0, 0]).await?;
                    socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
                    return Ok((false, 0));
                }
            }
        }
    } else {
        // No prepared statement - send NoData
        socket.write_all(&[b'n', 0, 0, 0, 4]).await?;
    socket.flush().await?;
        return Ok((false, 0));
    }
}

/// Send RowDescription message for Describe response (columns with types)
async fn send_row_description_for_describe(
    socket: &mut tokio::net::TcpStream,
    columns: &[(String, String)],
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes()); // Field count
    
    for (name, type_name) in columns {
        // Field name (null-terminated)
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        
        // Table OID (0 = no table)
        msg.extend_from_slice(&0u32.to_be_bytes());
        // Column attribute number (0 = no column)
        msg.extend_from_slice(&0i16.to_be_bytes());
        // Type OID
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        // Type size
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        // Type modifier (-1 = no modifier)
        msg.extend_from_slice(&(-1i32).to_be_bytes());
        // Format code (0 = text)
        msg.extend_from_slice(&0i16.to_be_bytes());
    }
    
    // Fill in the length
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send RowDescription for generic streams (TLS)
async fn send_row_description_generic<S>(
    socket: &mut S,
    columns: &[(String, String)],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes()); // Field count
    
    for (name, type_name) in columns {
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&0u32.to_be_bytes()); // table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // column attr
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        msg.extend_from_slice(&0i16.to_be_bytes()); // format code
    }
    
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CommandComplete for generic streams (TLS)
async fn send_command_complete_generic<S>(socket: &mut S, tag: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut msg = Vec::new();
    msg.push(b'C'); // CommandComplete
    let tag_bytes = tag.as_bytes();
    let len = (4 + tag_bytes.len() + 1) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(tag_bytes);
    msg.push(0); // Null terminator
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyOutResponse header for generic streams (TLS)
/// Used for COPY TO STDOUT commands
async fn send_copy_out_response_header_generic<S>(socket: &mut S, column_count: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // CopyOutResponse: 'H' + length + format (0=text) + column count + format per column
    let mut msg = Vec::new();
    msg.push(b'H'); // CopyOutResponse
    let body_len = 4 + 1 + 2 + (column_count * 2); // length + format + col count + format per col
    msg.extend_from_slice(&(body_len as u32).to_be_bytes());
    msg.push(0); // Overall format: 0 = text
    msg.extend_from_slice(&(column_count as i16).to_be_bytes());
    for _ in 0..column_count {
        msg.extend_from_slice(&0i16.to_be_bytes()); // Per-column format: 0 = text
    }
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyData row for generic streams (TLS)
async fn send_copy_data_row_generic<S>(socket: &mut S, row: &[String]) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Build tab-separated row with newline
    let row_text = row.join("\t") + "\n";
    let row_bytes = row_text.as_bytes();
    
    // CopyData: 'd' + length + data
    let mut msg = Vec::new();
    msg.push(b'd'); // CopyData
    let len = (4 + row_bytes.len()) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(row_bytes);
    socket.write_all(&msg).await?;
    Ok(())
}

/// Send CopyDone for generic streams (TLS)
async fn send_copy_done_generic<S>(socket: &mut S) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // CopyDone: 'c' + length (4)
    socket.write_all(&[b'c', 0, 0, 0, 4]).await?;
    Ok(())
}

/// Send a single DataRow for generic streams (TLS)
async fn send_data_row_generic<S>(socket: &mut S, row: &[String], expected_cols: usize) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
    data_row.push(b'D');

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
    let cols_to_send = if expected_cols > 0 { expected_cols } else { row.len() };
    row_data.extend_from_slice(&(cols_to_send as i16).to_be_bytes());

    for i in 0..cols_to_send {
        if i < row.len() {
            let value = &row[i];
            // Treat explicit "NULL" strings as NULL, but preserve empty strings
            if value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else if value.is_empty() {
                // Empty string: length 0, no bytes
                row_data.extend_from_slice(&0i32.to_be_bytes());
            } else {
                // CRITICAL: PostgreSQL text format should not contain NULL bytes
                // Remove any embedded NULL bytes to prevent parsing issues
                let sanitized: String = value.chars().filter(|&c| c != '\0').collect();
                row_data.extend_from_slice(&(sanitized.len() as i32).to_be_bytes());
                row_data.extend_from_slice(sanitized.as_bytes());
            }
        } else {
            row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL for missing columns
        }
    }

    let len = (4 + row_data.len()) as u32;
    data_row.extend_from_slice(&len.to_be_bytes());
    data_row.extend_from_slice(&row_data);

    socket.write_all(&data_row).await?;
    Ok(())
}

/// Handle Execute message for extended query protocol - with QueryQueue
async fn handle_execute_extended(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    sql: &str,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    debug!(
        "Extended query protocol - Execute: {}",
        &sql[..sql.len().min(100)]
    );

    // FIRST: Check if this is an intercepted command (like Tableau temp table SELECT)
    // If so, return the fake rows instead of executing against worker
    if let Some(result) = handle_pg_specific_command(sql) {
        if !result.rows.is_empty() {
            info!("Extended query protocol - Execute: Intercepted command returns {} rows", result.rows.len());
            // Send DataRow for each row
            for row in &result.rows {
                let mut row_msg = vec![b'D'];
                let field_count = row.len() as i16;
                let mut row_data = Vec::new();
                row_data.extend_from_slice(&field_count.to_be_bytes());
                for value in row {
                    let bytes = value.as_bytes();
                    row_data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    row_data.extend_from_slice(bytes);
                }
                let row_len = (4 + row_data.len()) as u32;
                row_msg.extend_from_slice(&row_len.to_be_bytes());
                row_msg.extend_from_slice(&row_data);
                socket.write_all(&row_msg).await?;
            }
            // Send CommandComplete
            let cmd_tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
            let cmd_bytes = cmd_tag.as_bytes();
            let cmd_len = (4 + cmd_bytes.len() + 1) as u32;
            let mut cmd_msg = vec![b'C'];
            cmd_msg.extend_from_slice(&cmd_len.to_be_bytes());
            cmd_msg.extend_from_slice(cmd_bytes);
            cmd_msg.push(0);
            socket.write_all(&cmd_msg).await?;
    socket.flush().await?;
            return Ok(());
        }
    }

    let query_id = uuid::Uuid::new_v4().to_string();

    // Route query to estimate size
    let estimate = query_router.route(sql).await;
    let estimated_data_mb = estimate.data_size_mb;

    // Enqueue and wait for capacity
    let enqueue_result = query_queue
        .enqueue(query_id.clone(), estimated_data_mb)
        .await;

    match enqueue_result {
        Ok(query_token) => {
            let start = std::time::Instant::now();

            // Use extended protocol version that skips RowDescription
            // (Describe already sent it)
            let result = execute_query_streaming_extended(
                socket,
                worker_client,
                query_router,
                sql,
                user_id,
                smart_scaler,
            )
            .await;

            let duration_ms = start.elapsed().as_millis() as u64;

            let outcome = match &result {
                Ok(row_count) => {
                    info!(
                        rows = row_count,
                        wait_ms = query_token.queue_wait_ms(),
                        exec_ms = duration_ms,
                        "Extended query completed (streaming, no RowDesc)"
                    );
                    QueryOutcome::Success {
                        rows: *row_count as u64,
                        bytes: 0,
                        duration_ms,
                    }
                }
                Err(e) => {
                    error!("Extended query error: {}", e);
                    send_error(socket, &e.to_string()).await?;
                    QueryOutcome::Failure {
                        error: e.to_string(),
                        duration_ms,
                    }
                }
            };

            query_queue.complete(query_token, outcome).await;
        }
        Err(queue_error) => {
            let error_msg = format!("Query queue error: {}", queue_error);
            warn!(query_id = %query_id, "Extended query failed to queue: {}", error_msg);
            send_error(socket, &error_msg).await?;
        }
    }

    Ok(())
}

/// Handle Execute message when there's no prepared statement
async fn handle_execute_empty(
    socket: &mut tokio::net::TcpStream,
    buf: &mut [u8; 4],
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    socket.read_exact(buf).await?;
    let len = u32::from_be_bytes(*buf) as usize - 4;
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    send_command_complete(socket, "SELECT 0").await?;
    Ok(())
}


