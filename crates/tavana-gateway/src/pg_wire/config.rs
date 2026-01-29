//! PostgreSQL wire protocol configuration
//!
//! All values can be overridden via environment variables for flexible deployment.

use tracing::info;

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
    /// TCP keepalive time in seconds (detects dead connections)
    pub tcp_keepalive_secs: u64,
    /// Interval to check client connection health during streaming (in rows)
    pub connection_check_interval_rows: usize,
    /// Maximum rows to return in a single query result (like ClickHouse max_result_rows)
    /// This prevents client OOM by limiting result size. Use OFFSET for pagination.
    /// Set to 0 for unlimited (not recommended for production).
    /// Default: 100000 rows (protects JDBC clients from OOM)
    pub max_result_rows: usize,
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
                .unwrap_or(600),
            tcp_keepalive_secs: std::env::var("TAVANA_TCP_KEEPALIVE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            connection_check_interval_rows: std::env::var("TAVANA_CONNECTION_CHECK_INTERVAL_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            max_result_rows: std::env::var("TAVANA_MAX_RESULT_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100_000),
        }
    }
}

impl PgWireConfig {
    /// Log configuration on startup
    pub fn log_config(&self) {
        info!(
            "PgWireServer config: batch_size={}, timeout={}s, buffer={}KB, max_result_rows={}",
            self.streaming_batch_size,
            self.query_timeout_secs,
            self.write_buffer_size / 1024,
            self.max_result_rows
        );
    }
}
