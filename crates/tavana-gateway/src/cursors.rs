//! Server-Side Cursor Support for PostgreSQL Wire Protocol
//!
//! Enables Tableau and other BI tools to fetch large result sets in batches.
//! Uses LIMIT/OFFSET pagination with optional worker-side cursor affinity for true streaming.
//!
//! # Cursor Lifecycle
//! 1. `DECLARE cursor_name CURSOR FOR SELECT ...` - Creates cursor state
//! 2. `FETCH count FROM cursor_name` - Returns next batch of rows
//! 3. `CLOSE cursor_name` or `CLOSE ALL` - Releases cursor resources
//!
//! # Future Enhancement: True Streaming
//! For true streaming without re-scanning on each FETCH:
//! 1. Worker declares cursor via DeclareCursor gRPC
//! 2. Gateway stores worker_id for affinity routing
//! 3. FETCH routes to same worker, which iterates without re-execution

use crate::worker_client::WorkerClient;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Maximum idle time before a cursor is considered stale and cleaned up
const DEFAULT_CURSOR_TTL_SECS: u64 = 300; // 5 minutes

/// Cursor state for server-side cursor support (DECLARE/FETCH/CLOSE)
#[derive(Debug, Clone)]
pub struct CursorState {
    /// The SQL query this cursor executes
    pub query: String,
    /// Column metadata (cached from first execution)
    pub columns: Vec<(String, String)>,
    /// Current row offset (for LIMIT/OFFSET fallback pagination)
    pub current_offset: usize,
    /// Whether all rows have been fetched
    pub exhausted: bool,
    /// Worker ID holding this cursor (for affinity routing in true streaming mode)
    pub worker_id: Option<String>,
    /// Worker address for affinity routing
    pub worker_addr: Option<String>,
    /// Whether this cursor uses true streaming (worker-side) or fallback (LIMIT/OFFSET)
    pub uses_true_streaming: bool,
    /// Timestamp when cursor was last accessed (for TTL cleanup)
    pub last_accessed: Instant,
}

impl CursorState {
    /// Create a new cursor state
    pub fn new(query: String, columns: Vec<(String, String)>) -> Self {
        Self {
            query,
            columns,
            current_offset: 0,
            exhausted: false,
            worker_id: None,
            worker_addr: None,
            uses_true_streaming: false,
            last_accessed: Instant::now(),
        }
    }

    /// Mark cursor as accessed (resets TTL timer)
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }

    /// Check if cursor is stale (idle beyond TTL)
    pub fn is_stale(&self, ttl: Duration) -> bool {
        self.last_accessed.elapsed() > ttl
    }
}

/// Result of cursor command execution (compatible with QueryExecutionResult)
#[derive(Debug)]
pub struct CursorResult {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub command_tag: Option<String>,
}

/// Manages cursors for a single connection
pub struct ConnectionCursors {
    cursors: HashMap<String, CursorState>,
    ttl: Duration,
}

impl ConnectionCursors {
    /// Create a new cursor manager with default TTL
    pub fn new() -> Self {
        Self {
            cursors: HashMap::new(),
            ttl: Duration::from_secs(DEFAULT_CURSOR_TTL_SECS),
        }
    }

    /// Create with custom TTL
    pub fn with_ttl(ttl_secs: u64) -> Self {
        Self {
            cursors: HashMap::new(),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Get number of active cursors
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    /// Check if no cursors exist
    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    /// Clean up stale cursors (call periodically)
    pub fn cleanup_stale(&mut self) -> usize {
        let before = self.cursors.len();
        self.cursors.retain(|name, cursor| {
            if cursor.is_stale(self.ttl) {
                info!(cursor = %name, "Removing stale cursor (idle for {:?})", cursor.last_accessed.elapsed());
                false
            } else {
                true
            }
        });
        before - self.cursors.len()
    }

    /// Get a cursor by name (and touch it to reset TTL)
    pub fn get_mut(&mut self, name: &str) -> Option<&mut CursorState> {
        if let Some(cursor) = self.cursors.get_mut(name) {
            cursor.touch();
            Some(cursor)
        } else {
            None
        }
    }

    /// Insert a new cursor
    pub fn insert(&mut self, name: String, cursor: CursorState) {
        self.cursors.insert(name, cursor);
    }

    /// Remove a cursor
    pub fn remove(&mut self, name: &str) -> Option<CursorState> {
        self.cursors.remove(name)
    }

    /// Close all cursors
    pub fn close_all(&mut self) {
        let count = self.cursors.len();
        self.cursors.clear();
        if count > 0 {
            info!("Closed all {} cursors", count);
        }
    }

    /// Check if cursor exists
    pub fn contains(&self, name: &str) -> bool {
        self.cursors.contains_key(name)
    }
}

impl Default for ConnectionCursors {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse DECLARE cursor_name CURSOR FOR select_query
/// Returns (cursor_name, query) if valid
pub fn parse_declare_cursor(sql: &str) -> Option<(String, String)> {
    let sql_upper = sql.to_uppercase();
    debug!(sql_upper = %sql_upper, "Parsing DECLARE CURSOR");

    // Find cursor name (between DECLARE and CURSOR)
    let declare_pos = sql_upper.find("DECLARE")?;
    let cursor_pos = sql_upper.find(" CURSOR ")?;

    debug!(declare_pos = ?declare_pos, cursor_pos = ?cursor_pos, "Found positions");

    let cursor_name = sql[declare_pos + 7..cursor_pos].trim().to_string();

    // Find the SELECT query (after "FOR")
    let for_pos = sql_upper.find(" FOR ")?;
    debug!(for_pos = ?for_pos, cursor_name = %cursor_name, "Parsing FOR position");

    // Strip trailing semicolon from the inner query
    let query = sql[for_pos + 5..].trim().trim_end_matches(';').trim().to_string();

    if cursor_name.is_empty() || query.is_empty() {
        debug!(cursor_name = %cursor_name, query = %query, "Empty cursor name or query");
        return None;
    }

    Some((cursor_name, query))
}

/// Parse FETCH count FROM cursor_name
/// Returns (fetch_count, cursor_name) if valid
pub fn parse_fetch_cursor(sql: &str) -> Option<(usize, String)> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    // Find "FROM" position
    let from_pos = sql_upper.find(" FROM ")?;
    let cursor_name = sql[from_pos + 6..].trim().trim_end_matches(';').to_string();

    // Parse fetch count (between FETCH and FROM)
    let fetch_part = sql_trimmed[5..].trim(); // After "FETCH"
    let from_idx = fetch_part.find(" FROM ").unwrap_or(fetch_part.len());
    let count_part = fetch_part[..from_idx].trim();

    let fetch_count: usize = if count_part.eq_ignore_ascii_case("ALL") {
        usize::MAX // Fetch all remaining
    } else if count_part.starts_with("FORWARD ") {
        count_part[8..].trim().parse().unwrap_or_else(|_| {
            warn!("Failed to parse FETCH FORWARD count '{}', defaulting to 1", count_part);
            1
        })
    } else {
        count_part.parse().unwrap_or_else(|_| {
            warn!("Failed to parse FETCH count '{}', defaulting to 1", count_part);
            1
        })
    };

    Some((fetch_count, cursor_name))
}

/// Extract cursor name from CLOSE command
/// Returns Some("__ALL__") for CLOSE ALL
pub fn parse_close_cursor(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    if sql_trimmed.starts_with("CLOSE ") {
        let name = sql[6..].trim().trim_end_matches(';').to_string();
        if !name.is_empty() && name.to_uppercase() != "ALL" {
            return Some(name);
        } else if name.to_uppercase() == "ALL" {
            return Some("__ALL__".to_string());
        }
    }
    None
}

/// Handle DECLARE cursor_name CURSOR FOR select_query
/// Parses the cursor declaration and stores it in the connection's cursor map
pub async fn handle_declare_cursor(
    sql: &str,
    cursors: &mut ConnectionCursors,
    worker_client: &WorkerClient,
    user_id: &str,
) -> Option<CursorResult> {
    let (cursor_name, query) = parse_declare_cursor(sql)?;

    info!(
        cursor_name = %cursor_name,
        query_preview = %&query[..query.len().min(80)],
        "DECLARE CURSOR - using LIMIT/OFFSET pagination"
    );

    // Execute query once to get column metadata (with LIMIT 0 for efficiency)
    let query_clean = query.trim().trim_end_matches(';');
    let metadata_query = format!("{} LIMIT 0", query_clean);
    let columns = match worker_client.execute_query(&metadata_query, user_id).await {
        Ok(result) => result
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.type_name.clone()))
            .collect(),
        Err(e) => {
            warn!("Failed to get cursor column metadata: {}", e);
            Vec::new()
        }
    };

    // Store cursor state
    cursors.insert(cursor_name.clone(), CursorState::new(query, columns));

    Some(CursorResult {
        columns: vec![],
        rows: vec![],
        row_count: 0,
        command_tag: Some("DECLARE CURSOR".to_string()),
    })
}

/// Handle FETCH count FROM cursor_name
/// Fetches the next batch of rows using LIMIT/OFFSET
pub async fn handle_fetch_cursor(
    sql: &str,
    cursors: &mut ConnectionCursors,
    worker_client: &WorkerClient,
    user_id: &str,
) -> Option<CursorResult> {
    let (fetch_count, cursor_name) = parse_fetch_cursor(sql)?;

    // Check if cursor exists
    if !cursors.contains(&cursor_name) {
        warn!("FETCH from non-existent cursor: {}", cursor_name);
        return Some(CursorResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some(format!("ERROR: cursor \"{}\" does not exist", cursor_name)),
        });
    }

    let cursor = cursors.get_mut(&cursor_name)?;

    if cursor.exhausted {
        // Cursor is exhausted, return empty result
        return Some(CursorResult {
            columns: cursor.columns.clone(),
            rows: vec![],
            row_count: 0,
            command_tag: Some("FETCH 0".to_string()),
        });
    }

    // Build paginated query
    let paginated_query = if fetch_count == usize::MAX {
        format!("{} OFFSET {}", cursor.query, cursor.current_offset)
    } else {
        format!(
            "{} LIMIT {} OFFSET {}",
            cursor.query, fetch_count, cursor.current_offset
        )
    };

    debug!(
        "FETCH {} FROM {}: executing {}",
        fetch_count,
        cursor_name,
        &paginated_query[..paginated_query.len().min(100)]
    );

    // Execute paginated query
    match worker_client.execute_query(&paginated_query, user_id).await {
        Ok(result) => {
            let row_count = result.rows.len();

            // Update cursor position
            cursor.current_offset += row_count;

            // Check if cursor is exhausted
            if row_count == 0 || (fetch_count != usize::MAX && row_count < fetch_count) {
                cursor.exhausted = true;
            }

            info!(
                "FETCH {} FROM {}: returned {} rows (offset now {})",
                fetch_count, cursor_name, row_count, cursor.current_offset
            );

            // Use cached column metadata if available
            let columns = if cursor.columns.is_empty() {
                result
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_name.clone()))
                    .collect()
            } else {
                cursor.columns.clone()
            };

            Some(CursorResult {
                columns,
                rows: result.rows,
                row_count,
                command_tag: Some(format!("FETCH {}", row_count)),
            })
        }
        Err(e) => {
            warn!("FETCH error: {}", e);
            Some(CursorResult {
                columns: cursor.columns.clone(),
                rows: vec![],
                row_count: 0,
                command_tag: Some("FETCH 0".to_string()),
            })
        }
    }
}

/// Handle CLOSE cursor_name or CLOSE ALL
pub fn handle_close_cursor(sql: &str, cursors: &mut ConnectionCursors) -> Option<CursorResult> {
    let cursor_name = parse_close_cursor(sql)?;

    if cursor_name == "__ALL__" {
        let count = cursors.len();
        cursors.close_all();
        info!("CLOSE ALL - closed {} cursors", count);
        return Some(CursorResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("CLOSE CURSOR".to_string()),
        });
    }

    if cursors.remove(&cursor_name).is_some() {
        info!("CLOSE CURSOR '{}'", cursor_name);
        Some(CursorResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("CLOSE CURSOR".to_string()),
        })
    } else {
        warn!("CLOSE on non-existent cursor: {}", cursor_name);
        Some(CursorResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some(format!("ERROR: cursor \"{}\" does not exist", cursor_name)),
        })
    }
}

/// Check if SQL is a cursor command (DECLARE, FETCH, or CLOSE)
pub fn is_cursor_command(sql: &str) -> bool {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    sql_trimmed.starts_with("DECLARE ") && sql_upper.contains(" CURSOR ")
        || sql_trimmed.starts_with("FETCH ")
        || sql_trimmed.starts_with("CLOSE ")
}

/// Determine the type of cursor command
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CursorCommand {
    Declare,
    Fetch,
    Close,
}

/// Parse a cursor command type from SQL
pub fn parse_cursor_command(sql: &str) -> Option<CursorCommand> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    if sql_trimmed.starts_with("DECLARE ") && sql_upper.contains(" CURSOR ") {
        Some(CursorCommand::Declare)
    } else if sql_trimmed.starts_with("FETCH ") {
        Some(CursorCommand::Fetch)
    } else if sql_trimmed.starts_with("CLOSE ") {
        Some(CursorCommand::Close)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_declare_cursor() {
        let sql = "DECLARE my_cursor CURSOR FOR SELECT * FROM users";
        let result = parse_declare_cursor(sql);
        assert!(result.is_some());
        let (name, query) = result.unwrap();
        assert_eq!(name, "my_cursor");
        assert_eq!(query, "SELECT * FROM users");
    }

    #[test]
    fn test_parse_fetch_cursor() {
        let sql = "FETCH 100 FROM my_cursor";
        let result = parse_fetch_cursor(sql);
        assert!(result.is_some());
        let (count, name) = result.unwrap();
        assert_eq!(count, 100);
        assert_eq!(name, "my_cursor");
    }

    #[test]
    fn test_parse_fetch_all() {
        let sql = "FETCH ALL FROM my_cursor";
        let result = parse_fetch_cursor(sql);
        assert!(result.is_some());
        let (count, name) = result.unwrap();
        assert_eq!(count, usize::MAX);
        assert_eq!(name, "my_cursor");
    }

    #[test]
    fn test_parse_close_cursor() {
        let sql = "CLOSE my_cursor";
        let result = parse_close_cursor(sql);
        assert_eq!(result, Some("my_cursor".to_string()));
    }

    #[test]
    fn test_parse_close_all() {
        let sql = "CLOSE ALL";
        let result = parse_close_cursor(sql);
        assert_eq!(result, Some("__ALL__".to_string()));
    }

    #[test]
    fn test_cursor_stale_detection() {
        let cursor = CursorState::new("SELECT 1".to_string(), vec![]);
        // Just created, not stale
        assert!(!cursor.is_stale(Duration::from_secs(1)));
    }

    #[test]
    fn test_is_cursor_command() {
        assert!(is_cursor_command("DECLARE c CURSOR FOR SELECT 1"));
        assert!(is_cursor_command("FETCH 10 FROM c"));
        assert!(is_cursor_command("CLOSE c"));
        assert!(!is_cursor_command("SELECT * FROM users"));
    }
}

