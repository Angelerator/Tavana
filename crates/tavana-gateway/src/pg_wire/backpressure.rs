//! Backpressure-aware streaming for PostgreSQL wire protocol
//!
//! This module provides mechanisms to prevent overwhelming slow clients:
//!
//! 1. **Bytes-based batching**: Flush based on bytes written, not just row count
//! 2. **Adaptive flush timeouts**: Detect slow clients early
//! 3. **TCP send buffer monitoring**: True backpressure via socket pressure
//! 4. **Bounded write buffers**: Prevent unbounded memory growth
//!
//! ## How it works
//!
//! Unlike naive streaming that writes to kernel buffers as fast as possible,
//! this implementation:
//!
//! - Tracks bytes written since last flush
//! - Flushes after hitting byte threshold (not just row count)
//! - Uses short timeouts on flush to detect slow clients
//! - Polls socket writability to ensure true backpressure
//!
//! This mirrors what StarRocks/MySQL does with `writeBlocking()` - the server
//! can't produce data faster than the client can consume it.

use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, warn};

/// Configuration for backpressure-aware streaming
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Bytes to buffer before forcing a flush (default: 64KB)
    /// Smaller = more backpressure, higher latency
    /// Larger = less backpressure, lower latency, risk of overwhelming client
    pub flush_threshold_bytes: usize,

    /// Maximum rows before forcing a flush regardless of bytes
    /// Acts as a safety net for rows with very small data
    pub flush_threshold_rows: usize,

    /// Timeout for flush operations (seconds)
    /// If flush takes longer than this, client is likely slow/overwhelmed
    pub flush_timeout_secs: u64,

    /// How often to check client connection health (in rows)
    pub connection_check_interval_rows: usize,

    /// Size of the BufWriter internal buffer
    /// This is the maximum data that can accumulate before write blocks
    pub write_buffer_size: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            // 64KB - matches typical TCP window size
            // This ensures we're working with the TCP flow control
            flush_threshold_bytes: std::env::var("TAVANA_FLUSH_THRESHOLD_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64 * 1024),

            // 100 rows max between flushes (safety net)
            flush_threshold_rows: std::env::var("TAVANA_FLUSH_THRESHOLD_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),

            // 300 seconds (5 minutes) - StarRocks-style patient waiting for slow clients
            // Key insight: Don't disconnect slow clients, wait for TCP backpressure to work
            // This matches StarRocks' Channels.writeBlocking() which blocks indefinitely
            flush_timeout_secs: std::env::var("TAVANA_FLUSH_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),

            // Check connection every 500 rows
            connection_check_interval_rows: std::env::var("TAVANA_CONNECTION_CHECK_INTERVAL_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500),

            // 32KB write buffer - small enough to create backpressure
            write_buffer_size: std::env::var("TAVANA_WRITE_BUFFER_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(32 * 1024),
        }
    }
}

/// Statistics from a streaming session
#[derive(Debug, Default, Clone)]
pub struct StreamingStats {
    pub rows_sent: usize,
    pub bytes_sent: usize,
    pub flush_count: usize,
    pub slow_flush_count: usize,
}

/// Backpressure-aware writer wrapper
///
/// This wraps a socket with proper flow control:
/// - Tracks bytes written since last flush
/// - Forces flush based on byte threshold
/// - Detects slow clients via flush timeouts
/// - Provides true TCP-level backpressure
pub struct BackpressureWriter<W: AsyncWrite + Unpin> {
    writer: BufWriter<W>,
    config: BackpressureConfig,
    bytes_since_flush: usize,
    rows_since_flush: usize,
    stats: StreamingStats,
}

impl<W: AsyncWrite + Unpin> BackpressureWriter<W> {
    /// Create a new backpressure-aware writer
    pub fn new(writer: W, config: BackpressureConfig) -> Self {
        let buf_size = config.write_buffer_size;
        Self {
            writer: BufWriter::with_capacity(buf_size, writer),
            config,
            bytes_since_flush: 0,
            rows_since_flush: 0,
            stats: StreamingStats::default(),
        }
    }

    /// Write data and track bytes for backpressure
    pub async fn write_bytes(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(data).await?;
        self.bytes_since_flush += data.len();
        self.stats.bytes_sent += data.len();
        Ok(())
    }

    /// Mark a row as sent and check if flush is needed
    pub fn row_sent(&mut self) {
        self.rows_since_flush += 1;
        self.stats.rows_sent += 1;
    }

    /// Check if we should flush based on thresholds
    pub fn should_flush(&self) -> bool {
        self.bytes_since_flush >= self.config.flush_threshold_bytes
            || self.rows_since_flush >= self.config.flush_threshold_rows
    }

    /// Check if we should verify client connection
    pub fn should_check_connection(&self) -> bool {
        self.stats.rows_sent % self.config.connection_check_interval_rows == 0
            && self.stats.rows_sent > 0
    }

    /// Perform a backpressure-aware flush - StarRocks-style blocking
    ///
    /// This implements the key insight from StarRocks: let TCP handle backpressure naturally.
    /// We simply call flush() and let it block as long as needed - no timeout!
    /// The async runtime will yield to other tasks while waiting.
    ///
    /// Returns:
    /// - Ok(true) if flush succeeded normally
    /// - Ok(false) if flush succeeded but was slow (client may be overwhelmed)
    /// - Err ONLY if client actually disconnected (BrokenPipe, ConnectionReset, etc.)
    pub async fn flush_with_backpressure(&mut self) -> Result<bool, std::io::Error> {
        if self.bytes_since_flush == 0 {
            return Ok(true);
        }

        let flush_start = std::time::Instant::now();

        // StarRocks-style: just block on flush, no timeout
        // TCP flow control will naturally throttle if client is slow
        match self.writer.flush().await {
            Ok(()) => {
                let elapsed = flush_start.elapsed();
                self.stats.flush_count += 1;
                self.bytes_since_flush = 0;
                self.rows_since_flush = 0;

                // If flush took more than 5 seconds, client is getting slow
                if elapsed.as_secs() >= 5 {
                    self.stats.slow_flush_count += 1;
                    debug!(
                        "Slow flush completed: {}ms for {} bytes",
                        elapsed.as_millis(),
                        self.stats.bytes_sent
                    );
                    return Ok(false);
                }

                Ok(true)
            }
            Err(e) => {
                // Check for connection errors
                if is_disconnect_error(&e) {
                    warn!(
                        "Client disconnected during flush after {} rows, {} bytes",
                        self.stats.rows_sent, self.stats.bytes_sent
                    );
                }
                Err(e)
            }
        }
    }

    /// Force flush regardless of thresholds
    pub async fn force_flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().await?;
        self.bytes_since_flush = 0;
        self.rows_since_flush = 0;
        self.stats.flush_count += 1;
        Ok(())
    }

    /// Get current streaming statistics
    pub fn stats(&self) -> &StreamingStats {
        &self.stats
    }

    /// Get mutable reference to inner writer for direct writes
    /// Use sparingly - prefer write_bytes for proper tracking
    pub fn inner_mut(&mut self) -> &mut BufWriter<W> {
        &mut self.writer
    }

    /// Get reference to the underlying writer (bypassing BufWriter)
    /// Useful for connection checks on TcpStream
    pub fn get_ref(&self) -> &W {
        self.writer.get_ref()
    }

    /// Get mutable reference to the underlying writer (bypassing BufWriter)
    pub fn get_mut(&mut self) -> &mut W {
        self.writer.get_mut()
    }

    /// Consume and return the inner writer
    pub fn into_inner(self) -> BufWriter<W> {
        self.writer
    }
}

/// Specialized implementation for TcpStream to enable connection checks
impl BackpressureWriter<&mut tokio::net::TcpStream> {
    /// Check if the client connection is still alive
    /// Returns true if connected, false if disconnected
    pub async fn is_connected(&self) -> bool {
        is_client_connected(self.writer.get_ref()).await
    }
}

/// Check if a TcpStream client is still connected
pub async fn is_client_connected(stream: &TcpStream) -> bool {
    // Try to peek at the socket to check connection state
    match stream.peek(&mut [0u8; 0]).await {
        Ok(_) => true,
        Err(e) => {
            match e.kind() {
                // Definitely disconnected
                ErrorKind::ConnectionReset
                | ErrorKind::BrokenPipe
                | ErrorKind::NotConnected
                | ErrorKind::ConnectionAborted => {
                    debug!("Client disconnected: {:?}", e.kind());
                    false
                }
                // WouldBlock is expected for non-blocking sockets
                ErrorKind::WouldBlock => true,
                // Other errors might be transient
                _ => true,
            }
        }
    }
}

/// Check if an error indicates client disconnection
pub fn is_disconnect_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::NotConnected
            | ErrorKind::ConnectionAborted
    ) || e.to_string().contains("Broken pipe")
        || e.to_string().contains("Connection reset")
        || e.to_string().contains("connection reset")
}

/// Result of sending a data row with backpressure
pub enum SendRowResult {
    /// Row sent successfully
    Ok,
    /// Row sent but client is slow (flush took a long time)
    SlowClient,
    /// Client disconnected
    Disconnected,
}

/// Send a PostgreSQL DataRow with backpressure awareness
///
/// This is the main function for streaming rows to clients.
/// It automatically handles:
/// - Building the DataRow message
/// - Tracking bytes for backpressure
/// - Flushing when thresholds are reached
/// - Detecting slow/disconnected clients
pub async fn send_data_row_with_backpressure<W: AsyncWrite + Unpin>(
    writer: &mut BackpressureWriter<W>,
    row: &[String],
) -> Result<SendRowResult, std::io::Error> {
    // Build DataRow message
    let data_row = build_data_row(row);

    // Write with tracking
    writer.write_bytes(&data_row).await?;
    writer.row_sent();

    // Check if we should flush
    if writer.should_flush() {
        match writer.flush_with_backpressure().await {
            Ok(true) => Ok(SendRowResult::Ok),
            Ok(false) => Ok(SendRowResult::SlowClient),
            Err(e) if is_disconnect_error(&e) => Ok(SendRowResult::Disconnected),
            Err(e) => Err(e),
        }
    } else {
        Ok(SendRowResult::Ok)
    }
}

/// Build a PostgreSQL DataRow message
pub fn build_data_row(row: &[String]) -> Vec<u8> {
    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
    data_row.push(b'D'); // DataRow type

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
    row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

    for value in row {
        if value == "NULL" {
            row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        } else if value.is_empty() {
            row_data.extend_from_slice(&0i32.to_be_bytes()); // Empty string
        } else {
            // Sanitize: remove NULL bytes that could break protocol parsing
            let sanitized: String = value.chars().filter(|&c| c != '\0').collect();
            row_data.extend_from_slice(&(sanitized.len() as i32).to_be_bytes());
            row_data.extend_from_slice(sanitized.as_bytes());
        }
    }

    // Fill in length
    let len = (4 + row_data.len()) as u32;
    data_row.extend_from_slice(&len.to_be_bytes());
    data_row.extend_from_slice(&row_data);

    data_row
}

/// Build a PostgreSQL RowDescription message
pub fn build_row_description(columns: &[String], column_types: &[String]) -> Vec<u8> {
    use super::protocol::types::{pg_type_len, pg_type_oid};

    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (i, name) in columns.iter().enumerate() {
        let type_name = column_types.get(i).map(|s| s.as_str()).unwrap_or("text");

        msg.extend_from_slice(name.as_bytes());
        msg.push(0); // Null terminator
        msg.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // Column attr
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // Type modifier
        msg.extend_from_slice(&0i16.to_be_bytes()); // Format code (text)
    }

    // Fill in length
    let len = (msg.len() - 1) as u32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());

    msg
}

/// Build a PostgreSQL CommandComplete message
pub fn build_command_complete(tag: &str) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'C'); // CommandComplete
    let tag_bytes = tag.as_bytes();
    let len = (4 + tag_bytes.len() + 1) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(tag_bytes);
    msg.push(0); // Null terminator
    msg
}

// Implement AsyncWrite for BackpressureWriter so it can be used directly
impl<W: AsyncWrite + Unpin> AsyncWrite for BackpressureWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_data_row() {
        let row = vec!["hello".to_string(), "world".to_string()];
        let data_row = build_data_row(&row);

        // Check message type
        assert_eq!(data_row[0], b'D');

        // Check it has proper length field
        let len = u32::from_be_bytes([data_row[1], data_row[2], data_row[3], data_row[4]]);
        assert_eq!(len as usize + 1, data_row.len());
    }

    #[test]
    fn test_build_data_row_with_null() {
        let row = vec!["hello".to_string(), "NULL".to_string()];
        let data_row = build_data_row(&row);

        // Should contain -1 (NULL indicator) for second column
        assert!(data_row.len() > 10);
    }

    #[test]
    fn test_config_defaults() {
        let config = BackpressureConfig::default();
        assert_eq!(config.flush_threshold_bytes, 64 * 1024);
        assert_eq!(config.flush_threshold_rows, 100);
        assert_eq!(config.flush_timeout_secs, 5);
    }
}
