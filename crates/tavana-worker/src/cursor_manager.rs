//! Server-side cursor manager for true streaming queries
//!
//! This module manages active cursors that hold DuckDB query results.
//! Uses LIMIT/OFFSET pagination to avoid loading entire result sets into memory.
//! This is critical for large Delta table scans that can return millions of rows.

use anyhow::Result;
use dashmap::DashMap;
use duckdb::arrow::array::RecordBatch;
use duckdb::arrow::datatypes::Schema;
use duckdb::{params, Connection};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Maximum rows to fetch in a single batch to prevent memory exhaustion
/// This is configurable via CURSOR_BATCH_SIZE environment variable
fn max_cursor_batch_size() -> usize {
    std::env::var("CURSOR_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000) // Default: 10K rows per batch
}

/// Metadata for a column in a cursor result
#[derive(Debug, Clone)]
pub struct CursorColumnMeta {
    pub name: String,
    pub type_name: String,
}

/// Active cursor with streaming-friendly design
/// 
/// Instead of loading all results into memory (which caused OOM for large tables),
/// this cursor uses LIMIT/OFFSET pagination to fetch data on-demand.
/// Only a configurable number of rows are kept in memory at a time.
pub struct ActiveCursor {
    /// Unique cursor ID
    pub id: String,
    /// Column metadata from the query schema
    pub columns: Vec<CursorColumnMeta>,
    /// Currently buffered record batches (small, fetched on-demand)
    batches: Mutex<Vec<RecordBatch>>,
    /// Current position in the buffered batches
    batch_index: AtomicUsize,
    /// Current row within the current batch
    row_index: AtomicUsize,
    /// Whether we've exhausted all results from the query
    exhausted: AtomicBool,
    /// Total rows fetched and returned to client so far
    pub rows_fetched: AtomicUsize,
    /// Total rows in the current buffer (for OFFSET calculation)
    rows_in_buffer: AtomicUsize,
    /// Current OFFSET for LIMIT/OFFSET pagination
    current_offset: AtomicUsize,
    /// Creation time for timeout tracking
    pub created_at: Instant,
    /// Last access time for idle cleanup
    last_accessed: Mutex<Instant>,
    /// The original SQL query (wrapped with LIMIT/OFFSET for pagination)
    pub sql: String,
    /// Schema for creating new batches
    schema: Arc<Schema>,
}

impl ActiveCursor {
    /// Create a new cursor with initial batch of results
    /// Only fetches a limited number of rows initially to avoid OOM
    pub fn new(
        id: String,
        sql: String,
        schema: Arc<Schema>,
        initial_batches: Vec<RecordBatch>,
        initial_exhausted: bool,
    ) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| CursorColumnMeta {
                name: f.name().clone(),
                type_name: f.data_type().to_string(),
            })
            .collect();

        let rows_in_buffer: usize = initial_batches.iter().map(|b| b.num_rows()).sum();

        Self {
            id,
            columns,
            batches: Mutex::new(initial_batches),
            batch_index: AtomicUsize::new(0),
            row_index: AtomicUsize::new(0),
            exhausted: AtomicBool::new(initial_exhausted),
            rows_fetched: AtomicUsize::new(0),
            rows_in_buffer: AtomicUsize::new(rows_in_buffer),
            current_offset: AtomicUsize::new(rows_in_buffer), // Next fetch starts here
            created_at: Instant::now(),
            last_accessed: Mutex::new(Instant::now()),
            sql,
            schema,
        }
    }

    /// Update last accessed time
    pub fn touch(&self) {
        *self.last_accessed.lock() = Instant::now();
    }

    /// Get last accessed time
    pub fn last_accessed(&self) -> Instant {
        *self.last_accessed.lock()
    }

    /// Check if cursor has been idle longer than the given duration
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_accessed().elapsed() > timeout
    }
    
    /// Get the schema for this cursor
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
    
    /// Get current offset for next batch fetch
    pub fn current_offset(&self) -> usize {
        self.current_offset.load(Ordering::Relaxed)
    }
    
    /// Check if cursor is exhausted
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Relaxed)
    }
    
    /// Mark cursor as exhausted
    pub fn set_exhausted(&self) {
        self.exhausted.store(true, Ordering::Relaxed);
    }
    
    /// Replace buffer with new batches (for streaming pagination)
    pub fn replace_buffer(&self, new_batches: Vec<RecordBatch>, exhausted: bool) {
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();
        
        // Update offset for next fetch
        let current = self.current_offset.load(Ordering::Relaxed);
        self.current_offset.store(current + new_rows, Ordering::Relaxed);
        
        // Replace buffer
        let mut batches = self.batches.lock();
        *batches = new_batches;
        
        // Reset position in new buffer
        self.batch_index.store(0, Ordering::Relaxed);
        self.row_index.store(0, Ordering::Relaxed);
        self.rows_in_buffer.store(new_rows, Ordering::Relaxed);
        
        if exhausted {
            self.exhausted.store(true, Ordering::Relaxed);
        }
    }

    /// Fetch next N rows from the current buffer
    /// Returns (rows, need_more_data, is_exhausted)
    /// - need_more_data: true if buffer is empty but cursor is not exhausted
    pub fn fetch_from_buffer(&self, count: usize) -> (Vec<Vec<String>>, bool, bool) {
        self.touch();

        if self.exhausted.load(Ordering::Relaxed) {
            let batches = self.batches.lock();
            let batch_idx = self.batch_index.load(Ordering::Relaxed);
            if batch_idx >= batches.len() {
                return (vec![], false, true);
            }
        }

        let batches = self.batches.lock();
        let mut rows = Vec::with_capacity(count);
        let mut batch_idx = self.batch_index.load(Ordering::Relaxed);
        let mut row_idx = self.row_index.load(Ordering::Relaxed);

        while rows.len() < count && batch_idx < batches.len() {
            let batch = &batches[batch_idx];
            
            while row_idx < batch.num_rows() && rows.len() < count {
                let row = Self::extract_row(batch, row_idx);
                rows.push(row);
                row_idx += 1;
            }

            if row_idx >= batch.num_rows() {
                batch_idx += 1;
                row_idx = 0;
            }
        }

        // Update positions
        self.batch_index.store(batch_idx, Ordering::Relaxed);
        self.row_index.store(row_idx, Ordering::Relaxed);
        self.rows_fetched.fetch_add(rows.len(), Ordering::Relaxed);

        let buffer_exhausted = batch_idx >= batches.len();
        let cursor_exhausted = self.exhausted.load(Ordering::Relaxed);
        
        // need_more_data if we didn't get enough rows and cursor isn't exhausted
        let need_more = rows.len() < count && buffer_exhausted && !cursor_exhausted;
        
        (rows, need_more, buffer_exhausted && cursor_exhausted)
    }
    
    /// Legacy fetch method for backwards compatibility
    /// Returns (rows, is_exhausted)
    pub fn fetch(&self, count: usize) -> (Vec<Vec<String>>, bool) {
        let (rows, _need_more, exhausted) = self.fetch_from_buffer(count);
        (rows, exhausted)
    }

    /// Extract a single row from a RecordBatch as strings
    fn extract_row(batch: &RecordBatch, row_idx: usize) -> Vec<String> {
        use duckdb::arrow::array::*;

        batch
            .columns()
            .iter()
            .map(|col| {
                if col.is_null(row_idx) {
                    return "NULL".to_string();
                }

                // Handle different Arrow types
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(row_idx).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Date32Array>() {
                    // Date32 is days since epoch
                    let days = arr.value(row_idx);
                    format!("{}", days)
                } else if let Some(arr) = col.as_any().downcast_ref::<Date64Array>() {
                    // Date64 is milliseconds since epoch
                    let ms = arr.value(row_idx);
                    format!("{}", ms)
                } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    let us = arr.value(row_idx);
                    format!("{}", us)
                } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                    let ms = arr.value(row_idx);
                    format!("{}", ms)
                } else if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
                    format!("\\x{}", hex::encode(arr.value(row_idx)))
                } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                    format!("\\x{}", hex::encode(arr.value(row_idx)))
                } else {
                    // Fallback: use debug format
                    format!("{:?}", col)
                }
            })
            .collect()
    }
}

/// Configuration for the cursor manager
#[derive(Debug, Clone)]
pub struct CursorManagerConfig {
    /// Maximum number of active cursors per worker
    pub max_cursors: usize,
    /// Idle timeout in seconds before cursor is closed
    pub idle_timeout_secs: u64,
    /// Cleanup interval in seconds
    pub cleanup_interval_secs: u64,
    /// Maximum rows per batch (for streaming pagination)
    pub batch_size: usize,
}

impl Default for CursorManagerConfig {
    fn default() -> Self {
        Self {
            max_cursors: std::env::var("MAX_CURSORS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            idle_timeout_secs: std::env::var("CURSOR_IDLE_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300), // 5 minutes
            cleanup_interval_secs: std::env::var("CURSOR_CLEANUP_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60), // 1 minute
            batch_size: max_cursor_batch_size(),
        }
    }
}

/// Manages all active cursors for this worker
pub struct CursorManager {
    /// Active cursors keyed by cursor_id
    cursors: DashMap<String, Arc<ActiveCursor>>,
    /// Configuration
    config: CursorManagerConfig,
}

impl CursorManager {
    /// Create a new cursor manager
    pub fn new(config: CursorManagerConfig) -> Self {
        Self {
            cursors: DashMap::new(),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CursorManagerConfig::default())
    }

    /// Declare a new cursor by executing the query with LIMIT for initial batch
    /// 
    /// IMPORTANT: This now uses LIMIT to fetch only the first batch of results,
    /// preventing OOM for large Delta table scans. Subsequent fetches use LIMIT/OFFSET.
    pub fn declare_cursor(
        &self,
        cursor_id: String,
        sql: &str,
        connection: &Connection,
    ) -> Result<Arc<ActiveCursor>> {
        // Check cursor limit
        if self.cursors.len() >= self.config.max_cursors {
            anyhow::bail!(
                "Maximum cursor limit ({}) reached. Close existing cursors first.",
                self.config.max_cursors
            );
        }

        // Check if cursor already exists
        if self.cursors.contains_key(&cursor_id) {
            anyhow::bail!("Cursor '{}' already exists", cursor_id);
        }

        let batch_size = max_cursor_batch_size();
        info!(
            cursor_id = %cursor_id, 
            sql = %&sql[..sql.len().min(100)], 
            batch_size = batch_size,
            "Declaring cursor with streaming pagination"
        );

        // Wrap query with LIMIT for initial batch to prevent OOM
        // We fetch batch_size + 1 to detect if there are more rows
        let initial_sql = format!(
            "SELECT * FROM ({}) AS __cursor_subquery LIMIT {}",
            sql.trim().trim_end_matches(';'),
            batch_size + 1
        );

        // Execute initial query with LIMIT, rolling back on error
        let execute_result = (|| -> Result<(Arc<Schema>, Vec<RecordBatch>, bool)> {
            let mut stmt = connection.prepare(&initial_sql)?;
            let arrow_result = stmt.query_arrow(params![])?;
            
            // Get schema before consuming iterator
            let schema = arrow_result.get_schema();

            // Collect initial batches (limited by LIMIT clause)
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut total_rows = 0;
            
            for batch in arrow_result {
                total_rows += batch.num_rows();
                batches.push(batch);
                
                // Stop if we've collected enough for initial batch
                if total_rows >= batch_size + 1 {
                    break;
                }
            }
            
            // Check if we got more than batch_size (indicates more data available)
            let has_more = total_rows > batch_size;
            
            // If we got batch_size + 1, trim to batch_size
            if has_more && !batches.is_empty() {
                // Trim the last batch if needed
                let excess = total_rows - batch_size;
                if let Some(last_batch) = batches.last() {
                    if last_batch.num_rows() <= excess {
                        // Remove entire last batch
                        batches.pop();
                    }
                    // Note: We don't perfectly trim here, accepting slightly more rows
                    // This is acceptable as it's still bounded
                }
            }
            
            Ok((schema, batches, !has_more))
        })();

        // Rollback on error to clear transaction state for next query on this connection
        let (schema, batches, initially_exhausted) = match execute_result {
            Ok(result) => result,
            Err(e) => {
                debug!("Cursor query failed, rolling back to clear transaction state");
                if let Err(rollback_err) = connection.execute("ROLLBACK", params![]) {
                    warn!("Rollback failed (non-critical): {}", rollback_err);
                }
                return Err(e);
            }
        };

        let buffered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            cursor_id = %cursor_id,
            batches = batches.len(),
            buffered_rows = buffered_rows,
            exhausted = initially_exhausted,
            "Cursor declared with initial batch (streaming-friendly)"
        );

        let cursor = Arc::new(ActiveCursor::new(
            cursor_id.clone(),
            sql.to_string(), // Store original SQL for pagination
            schema,
            batches,
            initially_exhausted,
        ));

        self.cursors.insert(cursor_id, cursor.clone());
        Ok(cursor)
    }
    
    /// Fetch more data for a cursor using LIMIT/OFFSET pagination
    /// This is called when the cursor buffer is exhausted but more data exists
    pub fn fetch_more_for_cursor(
        &self,
        cursor_id: &str,
        connection: &Connection,
    ) -> Result<bool> {
        let cursor = self
            .cursors
            .get(cursor_id)
            .ok_or_else(|| anyhow::anyhow!("Cursor '{}' not found", cursor_id))?;
        
        if cursor.is_exhausted() {
            return Ok(false);
        }
        
        let batch_size = max_cursor_batch_size();
        let offset = cursor.current_offset();
        
        // Build paginated query
        let paginated_sql = format!(
            "SELECT * FROM ({}) AS __cursor_subquery LIMIT {} OFFSET {}",
            cursor.sql.trim().trim_end_matches(';'),
            batch_size + 1,
            offset
        );
        
        debug!(
            cursor_id = %cursor_id,
            offset = offset,
            batch_size = batch_size,
            "Fetching more cursor data with LIMIT/OFFSET"
        );
        
        // Execute paginated query
        let execute_result = (|| -> Result<(Vec<RecordBatch>, bool)> {
            let mut stmt = connection.prepare(&paginated_sql)?;
            let arrow_result = stmt.query_arrow(params![])?;
            
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut total_rows = 0;
            
            for batch in arrow_result {
                total_rows += batch.num_rows();
                batches.push(batch);
                
                if total_rows >= batch_size + 1 {
                    break;
                }
            }
            
            let has_more = total_rows > batch_size;
            Ok((batches, !has_more))
        })();
        
        match execute_result {
            Ok((batches, exhausted)) => {
                let new_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                debug!(
                    cursor_id = %cursor_id,
                    new_rows = new_rows,
                    exhausted = exhausted,
                    "Fetched more cursor data"
                );
                
                cursor.replace_buffer(batches, exhausted);
                Ok(true)
            }
            Err(e) => {
                warn!(cursor_id = %cursor_id, error = %e, "Failed to fetch more cursor data");
                if let Err(rollback_err) = connection.execute("ROLLBACK", params![]) {
                    warn!("Rollback failed (non-critical): {}", rollback_err);
                }
                Err(e)
            }
        }
    }

    /// Get a cursor by ID
    pub fn get_cursor(&self, cursor_id: &str) -> Option<Arc<ActiveCursor>> {
        self.cursors.get(cursor_id).map(|c| c.value().clone())
    }

    /// Fetch rows from a cursor
    pub fn fetch_cursor(
        &self,
        cursor_id: &str,
        count: usize,
    ) -> Result<(Vec<Vec<String>>, bool)> {
        let cursor = self
            .cursors
            .get(cursor_id)
            .ok_or_else(|| anyhow::anyhow!("Cursor '{}' not found", cursor_id))?;

        let (rows, exhausted) = cursor.fetch(count);
        
        debug!(
            cursor_id = %cursor_id,
            fetched = rows.len(),
            exhausted = exhausted,
            total_fetched = cursor.rows_fetched.load(Ordering::Relaxed),
            "Cursor fetch completed"
        );

        Ok((rows, exhausted))
    }

    /// Close and remove a cursor
    pub fn close_cursor(&self, cursor_id: &str) -> bool {
        if let Some((_, cursor)) = self.cursors.remove(cursor_id) {
            info!(
                cursor_id = %cursor_id,
                total_rows = cursor.rows_fetched.load(Ordering::Relaxed),
                lifetime_secs = cursor.created_at.elapsed().as_secs(),
                "Cursor closed"
            );
            true
        } else {
            false
        }
    }

    /// Close all cursors
    pub fn close_all_cursors(&self) -> usize {
        let count = self.cursors.len();
        self.cursors.clear();
        info!(count = count, "All cursors closed");
        count
    }

    /// Get number of active cursors
    pub fn cursor_count(&self) -> usize {
        self.cursors.len()
    }

    /// Cleanup idle cursors (called periodically)
    pub fn cleanup_idle_cursors(&self) -> usize {
        let timeout = Duration::from_secs(self.config.idle_timeout_secs);
        let mut cleaned = 0;

        // Collect IDs of idle cursors
        let idle_ids: Vec<String> = self
            .cursors
            .iter()
            .filter(|entry| entry.value().is_idle(timeout))
            .map(|entry| entry.key().clone())
            .collect();

        // Remove them
        for id in idle_ids {
            if self.close_cursor(&id) {
                warn!(cursor_id = %id, "Cleaned up idle cursor");
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            info!(cleaned = cleaned, "Idle cursor cleanup completed");
        }

        cleaned
    }

    /// Start background cleanup task
    pub fn start_cleanup_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        let interval = Duration::from_secs(manager.config.cleanup_interval_secs);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                manager.cleanup_idle_cursors();
            }
        })
    }

    /// Get cursor statistics
    pub fn stats(&self) -> CursorManagerStats {
        let cursors: Vec<_> = self.cursors.iter().collect();
        
        CursorManagerStats {
            active_cursors: cursors.len(),
            total_rows_fetched: cursors
                .iter()
                .map(|c| c.value().rows_fetched.load(Ordering::Relaxed))
                .sum(),
            oldest_cursor_secs: cursors
                .iter()
                .map(|c| c.value().created_at.elapsed().as_secs())
                .max()
                .unwrap_or(0),
        }
    }
}

/// Statistics about the cursor manager
#[derive(Debug, Clone)]
pub struct CursorManagerStats {
    pub active_cursors: usize,
    pub total_rows_fetched: usize,
    pub oldest_cursor_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_manager_basic() {
        let manager = CursorManager::with_defaults();
        assert_eq!(manager.cursor_count(), 0);
    }
}

