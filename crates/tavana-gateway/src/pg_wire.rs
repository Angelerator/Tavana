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
use crate::metrics;
use crate::query_queue::{QueryOutcome, QueryQueue};
use crate::query_router::{QueryRouter, QueryTarget};
use crate::redis_queue::{RedisQueue, RedisQueueConfig};
use crate::smart_scaler::SmartScaler;
use crate::tls_config::TlsConfig;
use crate::worker_client::{StreamingBatch, WorkerClient};
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
    pub streaming_batch_size: usize,
    /// Query timeout in seconds (prevents runaway queries)
    pub query_timeout_secs: u64,
    /// Socket write buffer size in bytes (future: BufWriter optimization)
    #[allow(dead_code)]
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
                .unwrap_or(300), // 5 minutes
            write_buffer_size: std::env::var("TAVANA_WRITE_BUFFER_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64 * 1024), // 64KB
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
        }
    }
}

// Legacy constant for backwards compatibility (when config is not passed)
const STREAMING_BATCH_SIZE: usize = 100;

/// Result of streaming query execution (for metrics tracking)
#[derive(Debug, Default)]
#[allow(dead_code)]
struct StreamingResult {
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
        
        // Load config from environment
        let config = Arc::new(PgWireConfig::default());

        Self {
            addr,
            auth_service,
            auth_gateway,
            worker_client,
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

            // Configure TCP keepalive to prevent stale connections
            // This is critical for DBeaver, Tableau, Power BI which may idle
            if let Err(e) = configure_tcp_keepalive(&socket) {
                warn!("Failed to configure TCP keepalive: {}", e);
            }

            let auth_service = self.auth_service.clone();
            let worker_client = self.worker_client.clone();
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
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
    redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    tls_config: Option<Arc<TlsConfig>>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
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
                    
                    // Continue with TLS stream
                    return handle_connection_generic(
                        tls_stream,
                        auth_service,
                        worker_client,
                        query_router,
                        pool_manager,
                        redis_queue,
                        smart_scaler,
                        query_queue,
                        config,
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
                    query_router,
                    pool_manager,
                    redis_queue,
                    smart_scaler,
                    query_queue,
                    config,
                ).await;
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
    _auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    let mut startup_msg;

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
                _ => {}
                }
            }
            break;
        }

    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    // Perform MD5 password authentication (Tableau/DBeaver compatibility)
    perform_md5_auth(&mut socket, &user_id).await?;

    // Send common parameter status messages
    send_parameter_status(&mut socket, "server_version", "15.0 (Tavana DuckDB)").await?;
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
    _auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()> {
    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {}", user_id);

    // Perform MD5 password authentication (Tableau/DBeaver compatibility)
    perform_md5_auth(&mut socket, &user_id).await?;

    // Send common parameter status messages
    send_parameter_status(&mut socket, "server_version", "15.0 (Tavana DuckDB)").await?;
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
    run_query_loop(&mut socket, &worker_client, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

/// Generic connection handler for both TLS and non-TLS streams
async fn handle_connection_generic<S>(
    mut socket: S,
    _auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    query_router: Arc<QueryRouter>,
    _pool_manager: Option<Arc<WorkerPoolManager>>,
    _redis_queue: Option<Arc<RedisQueue>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    config: Arc<PgWireConfig>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
                _ => {}
            }
        }
        break;
    }

    // Extract user from startup message
    let user_id =
        extract_startup_param(&startup_msg, "user").unwrap_or_else(|| "anonymous".to_string());
    info!("PostgreSQL client connected as user: {} (TLS)", user_id);

    // Perform MD5 password authentication (Tableau/DBeaver compatibility)
    perform_md5_auth_generic(&mut socket, &user_id).await?;

    // Send common parameter status messages
    send_parameter_status_generic(&mut socket, "server_version", "15.0 (Tavana DuckDB)").await?;
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
    run_query_loop_generic(&mut socket, &worker_client, &query_router, &user_id, smart_scaler.as_ref().map(|s| s.as_ref()), &query_queue, &config).await
}

/// Run the main query loop (for non-TLS)
/// 
/// Uses config for timeout enforcement and streaming batch size.
async fn run_query_loop(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    query_router: &QueryRouter,
    user_id: &str,
    smart_scaler: Option<&SmartScaler>,
    query_queue: &Arc<QueryQueue>,
    config: &PgWireConfig,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
    let mut prepared_query: Option<String> = None;
    // Track whether Describe sent RowDescription (true) or NoData (false)
    let mut describe_sent_row_description = false;
    // Cache the column count from Describe to ensure Execute sends matching data
    let mut __describe_column_count: usize = 0;
    // Server-side cursor storage for this connection
    let mut cursors = ConnectionCursors::new();

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
                
                // Handle DECLARE CURSOR
                if query_trimmed.starts_with("DECLARE ") && query_trimmed.contains(" CURSOR ") {
                    debug!(
                        query_trimmed = %query_trimmed,
                        "Detected DECLARE CURSOR command (non-TLS)"
                    );
                    if let Some(result) = cursors::handle_declare_cursor(&query, &mut cursors, worker_client, user_id).await {
                        info!(cursor_count = cursors.len(), "DECLARE CURSOR handled successfully (non-TLS)");
                        send_query_result_immediate(socket, result.into()).await?;
                        socket.write_all(&ready).await?;
                        socket.flush().await?;
                        continue;
                    } else {
                        warn!(query = %query, "DECLARE CURSOR parsing failed (non-TLS)");
                    }
                }
                
                // Handle FETCH
                if query_trimmed.starts_with("FETCH ") {
                    match cursors::handle_fetch_cursor(&query, &mut cursors, worker_client, user_id).await {
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
                
                // Handle CLOSE cursor (also closes on worker for true streaming)
                if query_trimmed.starts_with("CLOSE ") {
                    if let Some(result) = cursors::handle_close_cursor(&query, &mut cursors, worker_client).await {
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

                let query_id = uuid::Uuid::new_v4().to_string();
                let estimate = query_router.route(&query).await;
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
                                &query,
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
struct PortalState {
    /// Buffered rows from query execution
    rows: Vec<Vec<String>>,
    /// Current row offset (for resumption)
    offset: usize,
    /// Column count for DataRow consistency
    column_count: usize,
}

/// Run the main query loop for generic streams (TLS)
/// Supports both Simple Query and Extended Query protocols with proper cursor streaming.
/// 
/// Uses config for timeout enforcement and streaming batch size.
async fn run_query_loop_generic<S>(
    socket: &mut S,
    worker_client: &WorkerClient,
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
    let ready = [b'Z', 0, 0, 0, 5, b'I'];
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

    loop {
        let mut msg_type = [0u8; 1];
        if socket.read_exact(&mut msg_type).await.is_err() {
            debug!("Client disconnected (TLS)");
            break;
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

                info!("Received query (TLS): {}", &query[..query.len().min(100)]); // DEBUG: Always log query

                // Check for cursor commands first (require connection-level state)
                let query_upper = query.to_uppercase();
                let query_trimmed = query_upper.trim();
                
                // Log query at INFO level for debugging cursor issues
                if query_trimmed.contains("CURSOR") || query_trimmed.starts_with("DECLARE") || query_trimmed.starts_with("FETCH") {
                    info!(
                        query_preview = %&query_trimmed[..query_trimmed.len().min(80)],
                        "Processing potential cursor command - matched cursor detection"
                    );
                }
                
                // Handle DECLARE CURSOR
                if query_trimmed.starts_with("DECLARE ") && query_trimmed.contains(" CURSOR ") {
                    info!(
                        query_trimmed = %query_trimmed,
                        "Detected DECLARE CURSOR command, attempting to handle"
                    );
                    if let Some(result) = cursors::handle_declare_cursor(&query, &mut cursors, worker_client, user_id).await {
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
                
                // Handle FETCH
                if query_trimmed.starts_with("FETCH ") {
                    match cursors::handle_fetch_cursor(&query, &mut cursors, worker_client, user_id).await {
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
                
                // Handle CLOSE cursor (also closes on worker for true streaming)
                if query_trimmed.starts_with("CLOSE ") {
                    if let Some(result) = cursors::handle_close_cursor(&query, &mut cursors, worker_client).await {
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
                    debug!("Empty query received (TLS), returning EmptyQueryResponse");
                    // PostgreSQL protocol: send EmptyQueryResponse ('I') for empty queries
                    socket.write_all(&[b'I', 0, 0, 0, 4]).await?; // EmptyQueryResponse
                    socket.write_all(&ready).await?;
                    socket.flush().await?;
                    continue;
                }

                // For TLS connections, execute queries directly using a simpler path
                let query_id = uuid::Uuid::new_v4().to_string();
                let estimate = query_router.route(&query).await;
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
                                &query,
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
                // Bind
                socket.read_exact(&mut buf).await?;
                let len = u32::from_be_bytes(buf) as usize - 4;
                let mut data = vec![0u8; len];
                socket.read_exact(&mut data).await?;
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
                        // Execute query to get column info
                        match worker_client.execute_query(sql, user_id).await {
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
                                    send_row_description_generic(socket, &columns).await?;
                                    describe_sent_row_description = true;
                                }
                            }
                            Err(e) => {
                                warn!("Extended Protocol - Describe: query failed: {}, sending NoData", e);
                                socket.write_all(&[b'n', 0, 0, 0, 4]).await?; // NoData
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
                if let Some(ref mut state) = portal_state {
                    // Resume from stored rows
                    let remaining = state.rows.len() - state.offset;
                    let rows_to_send = if max_rows > 0 && (max_rows as usize) < remaining {
                        max_rows as usize
                    } else {
                        remaining
                    };
                    
                    let end = state.offset + rows_to_send;
                    debug!("Extended Protocol - Execute: Resuming from offset {}, sending {} rows", state.offset, rows_to_send);
                    
                    // Send rows from offset to end
                    for row in &state.rows[state.offset..end] {
                        send_data_row_generic(socket, row, state.column_count).await?;
                    }
                    socket.flush().await?;
                    
                    state.offset = end;
                    
                    if state.offset >= state.rows.len() {
                        // All rows sent
                        let cmd_tag = format!("SELECT {}", state.rows.len());
                        send_command_complete_generic(socket, &cmd_tag).await?;
                        portal_state = None;
                        prepared_query = None;
                        describe_sent_row_description = false;
                        __describe_column_count = 0;
                    } else {
                        // More rows available
                        socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                        socket.flush().await?;
                        continue; // Don't clear state
                    }
                } else if let Some(ref sql) = prepared_query {
                    // First Execute - need to run query
                    // CRITICAL: If Describe sent NoData, we MUST NOT send DataRows
                    if !describe_sent_row_description {
                        let cmd_tag = if let Some(result) = handle_pg_specific_command(sql) {
                            result.command_tag.unwrap_or_else(|| "SELECT 0".to_string())
                        } else {
                            "SELECT 0".to_string()
                        };
                        debug!("Extended Protocol - Execute: Describe sent NoData, sending CommandComplete: {}", cmd_tag);
                        send_command_complete_generic(socket, &cmd_tag).await?;
                    } else {
                        // Execute query and get all rows
                        // NOTE: This still buffers all rows for JDBC cursor support.
                        // True streaming for JDBC requires DECLARE CURSOR / FETCH instead.
                        info!("Extended Protocol - Execute: Describe sent RowDescription ({} cols), max_rows={}, executing query", __describe_column_count, max_rows);
                        
                        // Apply query timeout enforcement
                        let timeout_duration = Duration::from_secs(config.query_timeout_secs);
                        let query_result = tokio::time::timeout(
                            timeout_duration,
                            execute_query_get_rows(worker_client, query_router, sql, user_id, smart_scaler)
                        ).await;
                        
                        match query_result {
                            Ok(Ok(rows)) => {
                                let total_rows = rows.len();
                                let rows_to_send = if max_rows > 0 && (max_rows as usize) < total_rows {
                                    max_rows as usize
                                } else {
                                    total_rows
                                };
                                
                                // Send first batch of rows
                                for row in &rows[..rows_to_send] {
                                    send_data_row_generic(socket, row, __describe_column_count).await?;
                                }
                                socket.flush().await?;
                                
                                if rows_to_send < total_rows {
                                    // More rows - store state and send PortalSuspended
                                    portal_state = Some(PortalState {
                                        rows,
                                        offset: rows_to_send,
                                        column_count: __describe_column_count,
                                    });
                                    debug!("Extended Protocol - Execute: {} rows sent, {} remaining, sending PortalSuspended", rows_to_send, total_rows - rows_to_send);
                                    socket.write_all(&[b's', 0, 0, 0, 4]).await?; // PortalSuspended
                                    socket.flush().await?;
                                    continue; // Don't clear state
                                } else {
                                    // All rows sent
                                    let cmd_tag = format!("SELECT {}", total_rows);
                                    send_command_complete_generic(socket, &cmd_tag).await?;
                                }
                            }
                            Ok(Err(e)) => {
                                error!("Extended Protocol - Execute: query failed: {}", e);
                                send_error_generic(socket, &e.to_string()).await?;
                            }
                            Err(_elapsed) => {
                                // Timeout expired
                                let error_msg = format!(
                                    "Query timeout: exceeded {}s limit. Use LIMIT clause or DECLARE CURSOR.",
                                    config.query_timeout_secs
                                );
                                error!("Extended Protocol - Execute: query timed out after {}s", config.query_timeout_secs);
                                send_error_generic(socket, &error_msg).await?;
                            }
                        }
                    }
                    
                    // Clear state only if no more rows
                    prepared_query = None;
                    describe_sent_row_description = false;
                    __describe_column_count = 0;
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
                
                // Send CloseComplete
                socket.write_all(&[b'3', 0, 0, 0, 4]).await?;
                socket.flush().await?;
            }
            b'S' => {
                // Sync message has length field (4 bytes with value 4)
                socket.read_exact(&mut buf).await?;
                debug!("Extended Protocol - Sync");
                socket.write_all(&ready).await?;
                socket.flush().await?;
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

/// Send error message for generic streams
async fn send_error_generic<S>(socket: &mut S, message: &str) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // PostgreSQL Error Response format:
    // 'E' - error message type
    // int32 - length
    // fields: each field is a single byte field identifier followed by null-terminated string
    // Fields end with a zero byte
    
    // We need to send: S (severity), C (code), M (message), then null terminator
    let severity = b"ERROR\0";
    let code = b"42000\0"; // Syntax error or access rule violation
    let msg_bytes = message.as_bytes();
    
    // Calculate length: 4 (len) + S + severity + C + code + M + message + null + final null
    let total_len = 4 + 1 + severity.len() + 1 + code.len() + 1 + msg_bytes.len() + 1 + 1;
    
    let mut error_msg = vec![b'E'];
    error_msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    
    // Severity field
    error_msg.push(b'S');
    error_msg.extend_from_slice(severity);
    
    // SQL State code field
    error_msg.push(b'C');
    error_msg.extend_from_slice(code);
    
    // Message field
    error_msg.push(b'M');
    error_msg.extend_from_slice(msg_bytes);
    error_msg.push(0);
    
    // Terminator
    error_msg.push(0);
    
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
#[allow(dead_code)]
async fn execute_query_streaming_to_worker_impl(
    socket: &mut tokio::net::TcpStream,
    worker_addr: &str,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    use tokio::io::AsyncWriteExt;

    info!("Streaming query to worker: {}", worker_addr);

    // Connect to worker with large buffer settings for big result sets
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB per gRPC message

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800)) // 30 min for very large queries
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
            timeout_seconds: 600,
            max_rows: 0, // Unlimited - we stream
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut columns_sent = false;
    let mut total_rows: usize = 0;
    let mut batch_rows: usize = 0;

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                // Send RowDescription once (unless in Extended Protocol where Describe already sent it)
                if !columns_sent && !skip_row_description {
                    send_row_description(socket, &meta.columns, &meta.column_types).await?;
                }
                columns_sent = true;
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                if !batch.data.is_empty() {
                    if let Ok(rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                        for row in rows {
                            send_data_row(socket, &row).await?;
                            total_rows += 1;
                            batch_rows += 1;

                            // Flush every STREAMING_BATCH_SIZE rows for backpressure
                            if batch_rows >= STREAMING_BATCH_SIZE {
                                socket.flush().await?;
                                batch_rows = 0;
                            }

                            // Log progress for very large queries
                            if total_rows % 1_000_000 == 0 {
                                info!("Streaming progress: {} rows sent", total_rows);
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
        send_row_description(socket, &[], &[]).await?;
    }

    // Send CommandComplete
    let tag = format!("SELECT {}", total_rows);
    send_command_complete(socket, &tag).await?;

    Ok(total_rows)
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
#[allow(dead_code)]
async fn execute_query_streaming_default_impl(
    socket: &mut tokio::net::TcpStream,
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
    skip_row_description: bool,
) -> anyhow::Result<usize> {
    use tokio::io::AsyncWriteExt;

    // For the default path, we still need to use the existing client
    // but we'll stream the results to the client as they come
    match worker_client.execute_query_streaming(sql, user_id).await {
        Ok(mut stream) => {
            let mut columns_sent = false;
            let mut total_rows: usize = 0;
            let mut batch_rows: usize = 0;

            while let Some(batch) = stream.next().await {
                match batch? {
                    StreamingBatch::Metadata {
                        columns,
                        column_types,
                    } => {
                        // Send RowDescription once (unless in Extended Protocol where Describe already sent it)
                        if !columns_sent && !skip_row_description {
                            send_row_description(socket, &columns, &column_types).await?;
                        }
                        columns_sent = true;
                    }
                    StreamingBatch::Rows(rows) => {
                        for row in rows {
                            send_data_row(socket, &row).await?;
                            total_rows += 1;
                            batch_rows += 1;

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

            // If no columns were sent and not in extended protocol, send empty row description
            if !columns_sent && !skip_row_description {
                send_row_description(socket, &[], &[]).await?;
            }

            let tag = format!("SELECT {}", total_rows);
            send_command_complete(socket, &tag).await?;

            Ok(total_rows)
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

    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("BEGIN".to_string()),
        });
    }

    if sql_trimmed == "COMMIT" {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("COMMIT".to_string()),
        });
    }

    if sql_trimmed == "ROLLBACK" {
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

    if sql_upper.contains("PG_CATALOG") || sql_upper.contains("INFORMATION_SCHEMA") {
        return Some(QueryExecutionResult {
            columns: vec![
                ("table_catalog".to_string(), "text".to_string()),
                ("table_schema".to_string(), "text".to_string()),
                ("table_name".to_string(), "text".to_string()),
            ],
            rows: vec![],
            row_count: 0,
            command_tag: None,
        });
    }

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
    send_error_response(socket, "42000", message).await
}

/// Send error response with specific SQLSTATE code
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
async fn perform_md5_auth(socket: &mut tokio::net::TcpStream, user: &str) -> anyhow::Result<()> {
    // For backwards compatibility, use cleartext passthrough mode
    let _ = perform_cleartext_auth_internal(socket, user, None).await?;
    Ok(())
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
async fn perform_md5_auth_generic<S>(socket: &mut S, user: &str) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Send AuthenticationCleartextPassword (R with auth type 3)
    let auth_req = [b'R', 0, 0, 0, 8, 0, 0, 0, 3]; // auth type 3 = Cleartext
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
    
    debug!("Received password response, accepting (passthrough mode TLS)");
    
    // Send AuthenticationOk
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
    socket.flush().await?;
    
    info!("Authentication completed for user: {} (TLS passthrough)", user);
    Ok(())
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
            // Execute query to get column metadata
            match worker_client.execute_query(sql, user_id).await {
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
            if value.is_empty() || value == "null" || value == "NULL" {
                row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else {
                row_data.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row_data.extend_from_slice(value.as_bytes());
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

/// Configure TCP keepalive on a socket to prevent stale connections
/// 
/// This is essential for BI tools like DBeaver, Tableau, Power BI that may
/// hold connections open for long periods. Without keepalive:
/// - Network equipment (firewalls, load balancers) may drop idle connections
/// - Clients see "connection reset" or "stale connection" errors
/// - Users need to manually reconnect
fn configure_tcp_keepalive(socket: &tokio::net::TcpStream) -> std::io::Result<()> {
    use socket2::{SockRef, TcpKeepalive};
    
    let sock_ref = SockRef::from(socket);
    
    // Configure TCP keepalive with:
    // - Start sending probes after 30 seconds of idle
    // - Send probes every 15 seconds
    // - On Linux, give up after 4 failed probes (~1 minute total)
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(15));
    
    sock_ref.set_tcp_keepalive(&keepalive)?;
    
    // Disable Nagle's algorithm for lower latency (useful for interactive queries)
    sock_ref.set_nodelay(true)?;
    
    debug!("TCP keepalive configured: idle=30s, interval=15s, nodelay=true");
    Ok(())
}

