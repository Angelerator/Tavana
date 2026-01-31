//! PostgreSQL wire protocol configuration
//!
//! All values can be overridden via environment variables for flexible deployment.
//!
//! ## Backpressure Configuration
//!
//! Key settings for preventing client overwhelm:
//! - `flush_threshold_bytes`: Bytes before forcing flush (default: 64KB)
//! - `flush_threshold_rows`: Rows before forcing flush (default: 100)
//! - `flush_timeout_secs`: Timeout for flush operations (default: 5s)
//!
//! These work together to ensure the server can't send data faster than
//! the client can consume it, similar to MySQL's blocking write semantics.

use super::backpressure::BackpressureConfig;
use tracing::info;

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
                .unwrap_or(600),
            tcp_keepalive_secs: std::env::var("TAVANA_TCP_KEEPALIVE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            connection_check_interval_rows: std::env::var("TAVANA_CONNECTION_CHECK_INTERVAL_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500), // Check more frequently

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
        }
    }
}

impl PgWireConfig {
    /// Log configuration on startup
    pub fn log_config(&self) {
        info!(
            "PgWireServer config: batch_size={}, timeout={}s, buffer={}KB, flush_bytes={}KB, flush_rows={}, flush_timeout={}s (no result limit, warnings at 16GB+)",
            self.streaming_batch_size,
            self.query_timeout_secs,
            self.write_buffer_size / 1024,
            self.flush_threshold_bytes / 1024,
            self.flush_threshold_rows,
            self.flush_timeout_secs,
        );
    }

    /// Convert to BackpressureConfig for use with BackpressureWriter
    pub fn backpressure_config(&self) -> BackpressureConfig {
        BackpressureConfig {
            flush_threshold_bytes: self.flush_threshold_bytes,
            flush_threshold_rows: self.flush_threshold_rows,
            flush_timeout_secs: self.flush_timeout_secs,
            connection_check_interval_rows: self.connection_check_interval_rows,
            write_buffer_size: self.write_buffer_size,
        }
    }
}
