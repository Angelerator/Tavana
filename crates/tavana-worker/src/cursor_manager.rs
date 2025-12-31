//! Server-side cursor manager for true streaming queries
//!
//! This module manages active cursors that hold DuckDB query results.
//! Instead of re-executing queries with LIMIT/OFFSET for each FETCH,
//! cursors maintain the query state and stream results incrementally.

use anyhow::Result;
use dashmap::DashMap;
use duckdb::arrow::array::RecordBatch;
use duckdb::arrow::datatypes::Schema;
use duckdb::{params, Connection};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Metadata for a column in a cursor result
#[derive(Debug, Clone)]
pub struct CursorColumnMeta {
    pub name: String,
    pub type_name: String,
}

/// Active cursor holding buffered results from a DuckDB query
/// 
/// Note: DuckDB's Arrow iterator requires holding the Statement alive,
/// which creates lifetime issues. Instead, we buffer results in batches.
pub struct ActiveCursor {
    /// Unique cursor ID
    pub id: String,
    /// Column metadata from the query schema
    pub columns: Vec<CursorColumnMeta>,
    /// Buffered record batches (fetched lazily)
    batches: Mutex<Vec<RecordBatch>>,
    /// Current position in the batches
    batch_index: AtomicUsize,
    /// Current row within the current batch
    row_index: AtomicUsize,
    /// Whether we've exhausted all results
    exhausted: Mutex<bool>,
    /// Total rows fetched so far
    pub rows_fetched: AtomicUsize,
    /// Creation time for timeout tracking
    pub created_at: Instant,
    /// Last access time for idle cleanup
    last_accessed: Mutex<Instant>,
    /// The original SQL query (for potential re-execution if needed)
    pub sql: String,
}

impl ActiveCursor {
    /// Create a new cursor from query results
    pub fn new(
        id: String,
        sql: String,
        schema: &Schema,
        batches: Vec<RecordBatch>,
    ) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| CursorColumnMeta {
                name: f.name().clone(),
                type_name: f.data_type().to_string(),
            })
            .collect();

        Self {
            id,
            columns,
            batches: Mutex::new(batches),
            batch_index: AtomicUsize::new(0),
            row_index: AtomicUsize::new(0),
            exhausted: Mutex::new(false),
            rows_fetched: AtomicUsize::new(0),
            created_at: Instant::now(),
            last_accessed: Mutex::new(Instant::now()),
            sql,
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

    /// Fetch next N rows from the cursor
    /// Returns (rows, is_exhausted)
    pub fn fetch(&self, count: usize) -> (Vec<Vec<String>>, bool) {
        self.touch();

        if *self.exhausted.lock() {
            return (vec![], true);
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

        let is_exhausted = batch_idx >= batches.len();
        if is_exhausted {
            *self.exhausted.lock() = true;
        }

        (rows, is_exhausted)
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
}

impl Default for CursorManagerConfig {
    fn default() -> Self {
        Self {
            max_cursors: 100,
            idle_timeout_secs: 300, // 5 minutes
            cleanup_interval_secs: 60, // 1 minute
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

    /// Declare a new cursor by executing the query
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

        info!(cursor_id = %cursor_id, sql = %&sql[..sql.len().min(100)], "Declaring cursor");

        // Execute query and collect batches
        let mut stmt = connection.prepare(sql)?;
        let arrow_result = stmt.query_arrow(params![])?;
        
        // Get schema before consuming iterator
        let schema = arrow_result.get_schema();

        // Collect all batches (DuckDB streams internally with streaming_buffer_size)
        let batches: Vec<RecordBatch> = arrow_result.collect();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            cursor_id = %cursor_id,
            batches = batches.len(),
            total_rows = total_rows,
            "Cursor declared with buffered results"
        );

        let cursor = Arc::new(ActiveCursor::new(
            cursor_id.clone(),
            sql.to_string(),
            &schema,
            batches,
        ));

        self.cursors.insert(cursor_id, cursor.clone());
        Ok(cursor)
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

