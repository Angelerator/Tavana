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
    /// Last threshold (in GB) at which we warned about large transfer
    /// First warning at 16GB, then every 2GB after (18, 20, 22, ...)
    last_warning_gb: usize,
}

impl StreamingStats {
    /// Check if we should warn about large data transfer
    /// Returns Some(current_gb) if warning should be sent, None otherwise
    /// First warning at 16GB, then every 2GB (18GB, 20GB, ...)
    pub fn should_warn_large_transfer(&mut self) -> Option<usize> {
        const FIRST_WARNING_GB: usize = 16;
        const WARNING_INTERVAL_GB: usize = 2;
        
        let current_gb = self.bytes_sent / (1024 * 1024 * 1024);
        
        if current_gb >= FIRST_WARNING_GB {
            // Calculate which threshold we should be at
            let expected_threshold = if current_gb == FIRST_WARNING_GB {
                FIRST_WARNING_GB
            } else {
                // After 16GB, warn at 18, 20, 22, etc.
                FIRST_WARNING_GB + ((current_gb - FIRST_WARNING_GB) / WARNING_INTERVAL_GB) * WARNING_INTERVAL_GB
            };
            
            if expected_threshold > self.last_warning_gb {
                self.last_warning_gb = expected_threshold;
                return Some(current_gb);
            }
        }
        
        None
    }
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

    /// Get mutable streaming statistics (for warning threshold updates)
    pub fn stats_mut(&mut self) -> &mut StreamingStats {
        &mut self.stats
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
/// 
/// IMPORTANT: This uses a very short timeout on peek() because:
/// - PostgreSQL clients don't send data while waiting for query results
/// - A blocking peek() would deadlock: server waits for client, client waits for server
/// - If peek doesn't return immediately, we assume connection is alive
pub async fn is_client_connected(stream: &TcpStream) -> bool {
    use std::time::Duration;
    
    // Use a 1ms timeout - if peek blocks, connection is alive (client just has no data to send)
    // Only actual disconnection errors (which return immediately) indicate a dead connection
    match tokio::time::timeout(Duration::from_millis(1), stream.peek(&mut [0u8; 1])).await {
        Ok(Ok(_)) => true, // Data available or empty peek succeeded
        Ok(Err(e)) => {
            match e.kind() {
                // Definitely disconnected
                ErrorKind::ConnectionReset
                | ErrorKind::BrokenPipe
                | ErrorKind::NotConnected
                | ErrorKind::ConnectionAborted => {
                    debug!("Client disconnected: {:?}", e.kind());
                    false
                }
                // WouldBlock is expected for non-blocking sockets - connection alive
                ErrorKind::WouldBlock => true,
                // Other errors might be transient - assume connected
                _ => true,
            }
        }
        // Timeout - peek blocked waiting for data, connection is alive
        Err(_) => true,
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

/// Build a PostgreSQL DataRow message (text format only - legacy)
pub fn build_data_row(row: &[String]) -> Vec<u8> {
    build_data_row_with_formats(row, &[], &[])
}

/// Build a PostgreSQL DataRow message with binary format support
/// 
/// # Arguments
/// * `row` - Row values as strings
/// * `column_types` - PostgreSQL type names for each column (e.g., "int4", "text")
/// * `format_codes` - Format codes for each column (0=text, 1=binary). 
///                    Empty or single-element arrays are handled per PostgreSQL spec.
/// 
/// If format_codes is empty, all columns use text format.
/// If format_codes has one element, that format applies to all columns.
/// Otherwise, format_codes[i] applies to column i.
pub fn build_data_row_with_formats(
    row: &[String],
    column_types: &[String],
    format_codes: &[i16],
) -> Vec<u8> {
    let mut data_row = Vec::with_capacity(5 + row.len() * 20);
    data_row.push(b'D'); // DataRow type

    let mut row_data = Vec::with_capacity(2 + row.len() * 20);
    row_data.extend_from_slice(&(row.len() as i16).to_be_bytes());

    for (i, value) in row.iter().enumerate() {
        // Determine format for this column
        let format = get_format_for_column(i, format_codes);
        let type_name = column_types.get(i).map(|s| s.as_str()).unwrap_or("text");
        
        if value == "NULL" {
            row_data.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        } else if value.is_empty() {
            row_data.extend_from_slice(&0i32.to_be_bytes()); // Empty string
        } else if format == 1 {
            // Binary format requested
            encode_binary_value(&mut row_data, value, type_name);
        } else {
            // Text format
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

/// Get the format code for a specific column index
/// 
/// Per PostgreSQL spec:
/// - If format_codes is empty, use text (0) for all
/// - If format_codes has one element, use that for all columns
/// - Otherwise use format_codes[i] for column i
fn get_format_for_column(column_index: usize, format_codes: &[i16]) -> i16 {
    match format_codes.len() {
        0 => 0, // Default to text
        1 => format_codes[0], // Single format for all columns
        _ => format_codes.get(column_index).copied().unwrap_or(0),
    }
}

/// Encode a value in PostgreSQL binary format
/// 
/// Supports all common DuckDB types that have PostgreSQL binary equivalents.
/// Falls back to text format for unsupported or complex types.
/// 
/// Reference: https://duckdb.org/docs/stable/sql/data_types/overview
fn encode_binary_value(buf: &mut Vec<u8>, value: &str, type_name: &str) {
    let type_lower = type_name.to_lowercase();
    
    match type_lower.as_str() {
        // === SIGNED INTEGERS ===
        // 1-byte signed integer (TINYINT) - promoted to INT2 for PostgreSQL compatibility
        // Arrow: Int8, DuckDB: TINYINT/INT1
        "int1" | "tinyint" | "int8" => {
            // Note: Arrow "int8" means 8-bit integer, not PostgreSQL int8 (64-bit)
            // We disambiguate by checking if it parses as i8
            if let Ok(v) = value.parse::<i8>() {
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(v as i16).to_be_bytes());
            } else if let Ok(v) = value.parse::<i64>() {
                // Fallback: might be Arrow Int64 misnamed
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // 2-byte signed integer (SMALLINT)
        // Arrow: Int16, DuckDB: SMALLINT/INT2
        "int2" | "smallint" | "int16" | "short" => {
            if let Ok(v) = value.parse::<i16>() {
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // 4-byte signed integer (INTEGER)
        // Arrow: Int32, DuckDB: INTEGER/INT4/INT
        "int4" | "integer" | "int" | "int32" | "signed" => {
            if let Ok(v) = value.parse::<i32>() {
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // 8-byte signed integer (BIGINT)
        // Arrow: Int64, DuckDB: BIGINT/INT64
        "bigint" | "int64" | "long" => {
            if let Ok(v) = value.parse::<i64>() {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // 16-byte signed integer (HUGEINT) - sent as text, no PG binary equivalent
        "int128" | "hugeint" => {
            encode_text_fallback(buf, value);
        }
        
        // === UNSIGNED INTEGERS ===
        // Arrow: UInt8, DuckDB: UTINYINT
        "uint1" | "utinyint" | "uint8" => {
            if let Ok(v) = value.parse::<u8>() {
                buf.extend_from_slice(&2i32.to_be_bytes());
                buf.extend_from_slice(&(v as i16).to_be_bytes());
            } else if let Ok(v) = value.parse::<u64>() {
                // Fallback for larger values
                encode_text_fallback(buf, &v.to_string());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: UInt16, DuckDB: USMALLINT
        "uint2" | "usmallint" | "uint16" => {
            if let Ok(v) = value.parse::<u16>() {
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&(v as i32).to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: UInt32, DuckDB: UINTEGER
        "uint4" | "uinteger" | "uint32" => {
            if let Ok(v) = value.parse::<u32>() {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&(v as i64).to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: UInt64, DuckDB: UBIGINT - send as text, may overflow int8
        "ubigint" | "uint64" => {
            encode_text_fallback(buf, value);
        }
        // 16-byte unsigned integer (UHUGEINT) - send as text
        "uint128" | "uhugeint" => {
            encode_text_fallback(buf, value);
        }
        
        // === FLOATING POINT ===
        // Arrow: Float32, DuckDB: REAL/FLOAT
        "float4" | "real" | "float" | "float32" => {
            if let Ok(v) = value.parse::<f32>() {
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: Float64, DuckDB: DOUBLE
        "float8" | "double" | "float64" | "double precision" => {
            if let Ok(v) = value.parse::<f64>() {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        
        // === DECIMAL/NUMERIC ===
        // Arrow: Decimal128, DuckDB: DECIMAL/NUMERIC
        "decimal" | "numeric" | "decimal128" => {
            encode_text_fallback(buf, value);
        }
        
        // === BOOLEAN ===
        // Arrow: Boolean, DuckDB: BOOLEAN
        "bool" | "boolean" | "logical" => {
            let v: u8 = match value.to_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => 1,
                _ => 0,
            };
            buf.extend_from_slice(&1i32.to_be_bytes());
            buf.push(v);
        }
        
        // === DATE/TIME TYPES ===
        // Arrow: Date32, DuckDB: DATE
        "date" | "date32" => {
            if let Some(days) = parse_date_to_pg_days(value) {
                buf.extend_from_slice(&4i32.to_be_bytes());
                buf.extend_from_slice(&days.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: Timestamp, DuckDB: TIMESTAMP
        "timestamp" | "datetime" => {
            if let Some(micros) = parse_timestamp_to_pg_micros(value) {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&micros.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // TIMESTAMPTZ - same as timestamp for wire format
        "timestamptz" | "timestamp with time zone" => {
            if let Some(micros) = parse_timestamp_to_pg_micros(value) {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&micros.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: Time64, DuckDB: TIME
        "time" | "time without time zone" | "time64" => {
            if let Some(micros) = parse_time_to_micros(value) {
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&micros.to_be_bytes());
            } else {
                encode_text_fallback(buf, value);
            }
        }
        // Arrow: Duration/Interval
        "interval" | "duration" => {
            encode_text_fallback(buf, value);
        }
        
        // === UUID ===
        "uuid" => {
            if let Some(bytes) = parse_uuid(value) {
                buf.extend_from_slice(&16i32.to_be_bytes());
                buf.extend_from_slice(&bytes);
            } else {
                encode_text_fallback(buf, value);
            }
        }
        
        // === BINARY DATA ===
        // Arrow: Binary/LargeBinary, DuckDB: BLOB/BYTEA
        "bytea" | "blob" | "binary" | "varbinary" | "largebinary" => {
            if let Some(bytes) = parse_bytea(value) {
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(&bytes);
            } else {
                encode_text_fallback(buf, value);
            }
        }
        
        // === TEXT TYPES (always text format) ===
        // Arrow: Utf8/LargeUtf8, DuckDB: VARCHAR/TEXT
        "varchar" | "char" | "bpchar" | "text" | "string" | "utf8" | "largeutf8" |
        "json" | "bit" | "bitstring" => {
            encode_text_fallback(buf, value);
        }
        
        // === NESTED TYPES (always text format) ===
        // Arrow: List/LargeList/Map/Struct/Union
        "list" | "array" | "largelist" | "fixedsizelist" |
        "map" | "struct" | "union" => {
            encode_text_fallback(buf, value);
        }
        
        // Unsupported/unknown types: fall back to text format
        _ => {
            encode_text_fallback(buf, value);
        }
    }
}

/// Parse a date string to PostgreSQL binary format (days since 2000-01-01)
fn parse_date_to_pg_days(value: &str) -> Option<i32> {
    // PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01
    // Difference is 10957 days
    const PG_EPOCH_JDATE: i32 = 2451545; // Julian date for 2000-01-01
    const UNIX_EPOCH_JDATE: i32 = 2440588; // Julian date for 1970-01-01
    const DAYS_DIFF: i32 = PG_EPOCH_JDATE - UNIX_EPOCH_JDATE; // 10957
    
    // Try to parse YYYY-MM-DD format
    let parts: Vec<&str> = value.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    
    // Convert to days since Unix epoch using a simplified algorithm
    // This handles dates from year 1 to 9999
    let a = (14 - month) / 12;
    let y = year as u32 + 4800 - a;
    let m = month + 12 * a - 3;
    
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    let days_since_pg_epoch = jdn as i32 - PG_EPOCH_JDATE;
    
    Some(days_since_pg_epoch)
}

/// Parse a timestamp string to PostgreSQL binary format (microseconds since 2000-01-01 00:00:00)
fn parse_timestamp_to_pg_micros(value: &str) -> Option<i64> {
    // Split into date and time parts
    let parts: Vec<&str> = value.split(|c| c == ' ' || c == 'T').collect();
    if parts.is_empty() {
        return None;
    }
    
    // Parse date part
    let days = parse_date_to_pg_days(parts[0])?;
    
    // Parse time part if present
    let time_micros = if parts.len() > 1 {
        parse_time_to_micros(parts[1])?
    } else {
        0
    };
    
    // Convert days to microseconds and add time
    let day_micros = days as i64 * 24 * 60 * 60 * 1_000_000;
    Some(day_micros + time_micros)
}

/// Parse a time string to microseconds since midnight
fn parse_time_to_micros(value: &str) -> Option<i64> {
    // Handle timezone suffix if present (e.g., "12:30:45+00")
    let time_str = value.split(|c| c == '+' || c == '-').next()?;
    
    // Split HH:MM:SS or HH:MM:SS.fraction
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() < 2 {
        return None;
    }
    
    let hours: i64 = parts[0].parse().ok()?;
    let minutes: i64 = parts[1].parse().ok()?;
    
    let (seconds, micros) = if parts.len() > 2 {
        // Handle fractional seconds
        let sec_parts: Vec<&str> = parts[2].split('.').collect();
        let secs: i64 = sec_parts[0].parse().ok()?;
        let frac_micros: i64 = if sec_parts.len() > 1 {
            // Pad or truncate to 6 digits
            let frac = sec_parts[1];
            let padded = format!("{:0<6}", frac);
            padded[..6].parse().unwrap_or(0)
        } else {
            0
        };
        (secs, frac_micros)
    } else {
        (0, 0)
    };
    
    Some(hours * 3_600_000_000 + minutes * 60_000_000 + seconds * 1_000_000 + micros)
}

/// Parse a UUID string to 16 bytes
fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    // Remove hyphens and parse as hex
    let hex: String = value.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 32 {
        return None;
    }
    
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i*2..i*2+2], 16).ok()?;
    }
    Some(bytes)
}

/// Parse bytea/blob hex string to bytes
fn parse_bytea(value: &str) -> Option<Vec<u8>> {
    // DuckDB returns blobs as \x followed by hex
    if value.starts_with("\\x") || value.starts_with("\\X") {
        let hex = &value[2..];
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for i in (0..hex.len()).step_by(2) {
            if i + 2 <= hex.len() {
                bytes.push(u8::from_str_radix(&hex[i..i+2], 16).ok()?);
            }
        }
        Some(bytes)
    } else {
        // Plain bytes (shouldn't happen normally)
        Some(value.as_bytes().to_vec())
    }
}

/// Encode a value as text (fallback for unsupported binary types)
fn encode_text_fallback(buf: &mut Vec<u8>, value: &str) {
    let sanitized: String = value.chars().filter(|&c| c != '\0').collect();
    buf.extend_from_slice(&(sanitized.len() as i32).to_be_bytes());
    buf.extend_from_slice(sanitized.as_bytes());
}

/// Build a PostgreSQL RowDescription message (text format for all columns)
pub fn build_row_description(columns: &[String], column_types: &[String]) -> Vec<u8> {
    build_row_description_with_formats(columns, column_types, &[])
}

/// Build a PostgreSQL RowDescription message with format codes
/// 
/// # Arguments
/// * `columns` - Column names
/// * `column_types` - PostgreSQL type names for each column
/// * `format_codes` - Format codes (0=text, 1=binary). Per PostgreSQL spec:
///   - Empty: all columns use text (0)
///   - Single element: applies to all columns
///   - Multiple: format_codes[i] applies to column i
pub fn build_row_description_with_formats(
    columns: &[String],
    column_types: &[String],
    format_codes: &[i16],
) -> Vec<u8> {
    use super::protocol::types::{pg_type_len, pg_type_oid};

    let mut msg = Vec::new();
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
    msg.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (i, name) in columns.iter().enumerate() {
        let type_name = column_types.get(i).map(|s| s.as_str()).unwrap_or("text");
        let format = get_format_for_column(i, format_codes);

        msg.extend_from_slice(name.as_bytes());
        msg.push(0); // Null terminator
        msg.extend_from_slice(&0u32.to_be_bytes()); // Table OID
        msg.extend_from_slice(&0i16.to_be_bytes()); // Column attr
        msg.extend_from_slice(&pg_type_oid(type_name).to_be_bytes());
        msg.extend_from_slice(&pg_type_len(type_name).to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // Type modifier
        msg.extend_from_slice(&format.to_be_bytes()); // Format code
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
        assert_eq!(config.flush_timeout_secs, 300); // StarRocks-style 5 minute timeout
    }

    #[test]
    fn test_build_data_row_with_formats_text() {
        let row = vec!["42".to_string(), "hello".to_string()];
        let types = vec!["int4".to_string(), "text".to_string()];
        let formats = vec![0i16, 0i16]; // All text
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Check message type
        assert_eq!(data_row[0], b'D');
        
        // Text format: "42" is 2 bytes, "hello" is 5 bytes
        // Length: 4 (length) + 2 (column count) + 4 (len) + 2 (42) + 4 (len) + 5 (hello) = 21
        let len = u32::from_be_bytes([data_row[1], data_row[2], data_row[3], data_row[4]]);
        assert_eq!(len as usize + 1, data_row.len());
    }

    #[test]
    fn test_build_data_row_with_formats_binary_int4() {
        let row = vec!["42".to_string()];
        let types = vec!["int4".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Check message type
        assert_eq!(data_row[0], b'D');
        
        // Binary int4: 4 bytes value
        // Message: D + len(4) + col_count(2) + val_len(4) + value(4) = 15 bytes total
        assert_eq!(data_row.len(), 15);
        
        // Check the binary value (last 4 bytes should be 42 in big-endian)
        let value = i32::from_be_bytes([
            data_row[11], data_row[12], data_row[13], data_row[14]
        ]);
        assert_eq!(value, 42);
    }

    #[test]
    fn test_build_data_row_with_formats_binary_int8() {
        let row = vec!["9223372036854775807".to_string()]; // i64::MAX
        let types = vec!["int8".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Binary int8: 8 bytes value
        // Message: D + len(4) + col_count(2) + val_len(4) + value(8) = 19 bytes total
        assert_eq!(data_row.len(), 19);
        
        // Check the binary value
        let value = i64::from_be_bytes([
            data_row[11], data_row[12], data_row[13], data_row[14],
            data_row[15], data_row[16], data_row[17], data_row[18]
        ]);
        assert_eq!(value, i64::MAX);
    }

    #[test]
    fn test_build_data_row_with_formats_binary_float8() {
        let row = vec!["3.14159".to_string()];
        let types = vec!["float8".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Binary float8: 8 bytes value
        assert_eq!(data_row.len(), 19);
        
        // Check the binary value
        let value = f64::from_be_bytes([
            data_row[11], data_row[12], data_row[13], data_row[14],
            data_row[15], data_row[16], data_row[17], data_row[18]
        ]);
        assert!((value - 3.14159).abs() < 0.00001);
    }

    #[test]
    fn test_build_data_row_mixed_formats() {
        let row = vec!["42".to_string(), "hello".to_string(), "100".to_string()];
        let types = vec!["int4".to_string(), "text".to_string(), "int8".to_string()];
        let formats = vec![1i16, 0i16, 1i16]; // Binary, Text, Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Check message type
        assert_eq!(data_row[0], b'D');
        
        // Verify message is valid (has correct length prefix)
        let len = u32::from_be_bytes([data_row[1], data_row[2], data_row[3], data_row[4]]);
        assert_eq!(len as usize + 1, data_row.len());
    }

    #[test]
    fn test_get_format_for_column() {
        // Empty format codes -> all text
        assert_eq!(get_format_for_column(0, &[]), 0);
        assert_eq!(get_format_for_column(5, &[]), 0);
        
        // Single format code -> applies to all
        assert_eq!(get_format_for_column(0, &[1]), 1);
        assert_eq!(get_format_for_column(5, &[1]), 1);
        
        // Per-column format codes
        assert_eq!(get_format_for_column(0, &[0, 1, 0]), 0);
        assert_eq!(get_format_for_column(1, &[0, 1, 0]), 1);
        assert_eq!(get_format_for_column(2, &[0, 1, 0]), 0);
        assert_eq!(get_format_for_column(3, &[0, 1, 0]), 0); // Out of bounds -> default
    }

    #[test]
    fn test_parse_date_to_pg_days() {
        // 2000-01-01 is PostgreSQL epoch (day 0)
        assert_eq!(parse_date_to_pg_days("2000-01-01"), Some(0));
        
        // 2000-01-02 is day 1
        assert_eq!(parse_date_to_pg_days("2000-01-02"), Some(1));
        
        // 1999-12-31 is day -1
        assert_eq!(parse_date_to_pg_days("1999-12-31"), Some(-1));
        
        // A known date: 2024-01-15 is 8780 days after 2000-01-01
        let days = parse_date_to_pg_days("2024-01-15").unwrap();
        assert!(days > 8000 && days < 9000); // Sanity check
    }

    #[test]
    fn test_parse_time_to_micros() {
        // Midnight
        assert_eq!(parse_time_to_micros("00:00:00"), Some(0));
        
        // 1 hour
        assert_eq!(parse_time_to_micros("01:00:00"), Some(3_600_000_000));
        
        // 12:30:45
        let expected = 12 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000;
        assert_eq!(parse_time_to_micros("12:30:45"), Some(expected));
        
        // With fractional seconds
        let expected_frac = 12 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000 + 123456;
        assert_eq!(parse_time_to_micros("12:30:45.123456"), Some(expected_frac));
    }

    #[test]
    fn test_parse_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let bytes = parse_uuid(uuid).unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[1], 0x0e);
        assert_eq!(bytes[15], 0x00);
    }

    #[test]
    fn test_parse_bytea() {
        // Hex format with \x prefix
        let bytes = parse_bytea("\\x48656c6c6f").unwrap();
        assert_eq!(bytes, b"Hello");
        
        // Empty
        let empty = parse_bytea("\\x").unwrap();
        assert_eq!(empty, Vec::<u8>::new());
    }

    #[test]
    fn test_build_data_row_with_formats_date() {
        let row = vec!["2000-01-01".to_string()];
        let types = vec!["date".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Binary date: 4 bytes
        // Message: D + len(4) + col_count(2) + val_len(4) + value(4) = 15 bytes
        assert_eq!(data_row.len(), 15);
        
        // Check the value (2000-01-01 = day 0)
        let value = i32::from_be_bytes([
            data_row[11], data_row[12], data_row[13], data_row[14]
        ]);
        assert_eq!(value, 0);
    }

    #[test]
    fn test_build_data_row_with_formats_timestamp() {
        let row = vec!["2000-01-01 00:00:00".to_string()];
        let types = vec!["timestamp".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Binary timestamp: 8 bytes
        // Message: D + len(4) + col_count(2) + val_len(4) + value(8) = 19 bytes
        assert_eq!(data_row.len(), 19);
        
        // Check the value (2000-01-01 00:00:00 = 0 microseconds)
        let value = i64::from_be_bytes([
            data_row[11], data_row[12], data_row[13], data_row[14],
            data_row[15], data_row[16], data_row[17], data_row[18]
        ]);
        assert_eq!(value, 0);
    }

    #[test]
    fn test_build_data_row_with_formats_uuid() {
        let row = vec!["550e8400-e29b-41d4-a716-446655440000".to_string()];
        let types = vec!["uuid".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Binary UUID: 16 bytes
        // Message: D + len(4) + col_count(2) + val_len(4) + value(16) = 27 bytes
        assert_eq!(data_row.len(), 27);
        
        // Check first byte of UUID
        assert_eq!(data_row[11], 0x55);
    }

    #[test]
    fn test_build_data_row_with_formats_bool() {
        let row = vec!["true".to_string(), "false".to_string()];
        let types = vec!["boolean".to_string(), "boolean".to_string()];
        let formats = vec![1i16, 1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // Check message type
        assert_eq!(data_row[0], b'D');
        
        // Verify both booleans are encoded
        let len = u32::from_be_bytes([data_row[1], data_row[2], data_row[3], data_row[4]]);
        assert_eq!(len as usize + 1, data_row.len());
    }

    #[test]
    fn test_build_data_row_with_formats_tinyint() {
        let row = vec!["127".to_string()];
        let types = vec!["tinyint".to_string()];
        let formats = vec![1i16]; // Binary
        
        let data_row = build_data_row_with_formats(&row, &types, &formats);
        
        // TINYINT is promoted to INT2 (2 bytes) for PostgreSQL compatibility
        // Message: D + len(4) + col_count(2) + val_len(4) + value(2) = 13 bytes
        assert_eq!(data_row.len(), 13);
        
        // Check the value (promoted to i16)
        let value = i16::from_be_bytes([data_row[11], data_row[12]]);
        assert_eq!(value, 127);
    }
}
