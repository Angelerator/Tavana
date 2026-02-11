//! Utility functions for PostgreSQL wire protocol
//!
//! SQL manipulation, PostgreSQL command interception, and query result types.

use crate::cursors::CursorResult;

// Legacy constant for backwards compatibility (when config is not passed)
pub(crate) const STREAMING_BATCH_SIZE: usize = 100;

/// Transaction status constants for PostgreSQL wire protocol
/// These are sent in the ReadyForQuery message to tell the client the transaction state
/// CRITICAL: JDBC uses this to decide whether to use server-side cursors for streaming!
pub const TRANSACTION_STATUS_IDLE: u8 = b'I';
pub const TRANSACTION_STATUS_IN_TRANSACTION: u8 = b'T';
pub const TRANSACTION_STATUS_ERROR: u8 = b'E';

/// Encapsulates query execution results
pub(crate) struct QueryExecutionResult {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub command_tag: Option<String>,
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

/// Substitute $1, $2, etc. parameters in SQL with actual values
/// This converts PostgreSQL-style parameterized queries to literal SQL
pub(crate) fn substitute_parameters(sql: &str, params: &[Option<String>]) -> String {
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

/// Build a PostgreSQL NOTICE message as bytes (for inline sending during streaming)
pub(crate) fn build_notice_message(message: &str) -> Vec<u8> {
    let severity = b"WARNING";
    let code = b"01000";
    
    let msg_len = 4
        + 1 + severity.len() + 1
        + 1 + code.len() + 1      
        + 1 + message.len() + 1
        + 1;
    
    let mut buf = Vec::with_capacity(1 + msg_len);
    buf.push(b'N');
    buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
    
    buf.push(b'S');
    buf.extend_from_slice(severity);
    buf.push(0);
    
    buf.push(b'C');
    buf.extend_from_slice(code);
    buf.push(0);
    
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    
    buf.push(0);
    
    buf
}

/// Extract inner SELECT from COPY command if present
/// COPY (SELECT ...) TO STDOUT (FORMAT "binary") -> SELECT ...
pub(crate) fn extract_copy_inner_query(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    
    if !sql_trimmed.starts_with("COPY (") && !sql_trimmed.starts_with("COPY(") {
        return None;
    }
    
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

/// Check if a SQL command changes transaction state
/// Returns (new_transaction_status, is_transaction_command)
pub(crate) fn get_transaction_state_change(sql: &str, current_status: u8) -> (u8, bool) {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();
    
    if sql_trimmed == "BEGIN" || sql_trimmed.starts_with("BEGIN ") 
        || sql_trimmed.starts_with("START TRANSACTION") {
        (TRANSACTION_STATUS_IN_TRANSACTION, true)
    } else if sql_trimmed == "COMMIT" || sql_trimmed == "END" {
        (TRANSACTION_STATUS_IDLE, true)
    } else if sql_trimmed == "ROLLBACK" || sql_trimmed == "ABORT" {
        (TRANSACTION_STATUS_IDLE, true)
    } else {
        (current_status, false)
    }
}

/// Extract parameter from startup message
pub(crate) fn extract_startup_param(msg: &[u8], key: &str) -> Option<String> {
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

/// Handle PostgreSQL-specific commands that should be intercepted locally
pub(crate) fn handle_pg_specific_command(sql: &str) -> Option<QueryExecutionResult> {
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

    // Tableau temp table interception
    let is_tableau_temp = sql_upper.contains("#TABLEAU_");
    
    if is_tableau_temp {
        tracing::info!("Intercepting Tableau temp table query: {}", 
            if sql.len() > 100 { &sql[..100] } else { sql });
        
        if sql_upper.contains("CREATE") && sql_upper.contains("TABLE") {
            tracing::info!("Intercepted Tableau CREATE TABLE - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("CREATE TABLE".to_string()),
            });
        }
        
        if sql_upper.contains("SELECT") && sql_upper.contains("INTO") {
            tracing::info!("Intercepted Tableau SELECT INTO - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("SELECT 0".to_string()),
            });
        }
        
        if sql_upper.contains("DROP") {
            tracing::info!("Intercepted Tableau DROP TABLE - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("DROP TABLE".to_string()),
            });
        }
        
        if sql_upper.contains("INSERT") {
            tracing::info!("Intercepted Tableau INSERT - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 1,
                command_tag: Some("INSERT 0 1".to_string()),
            });
        }
        
        if sql_upper.contains("SELECT") && sql_upper.contains("FROM") {
            tracing::info!("Intercepted Tableau SELECT FROM temp table - returning success row");
            return Some(QueryExecutionResult {
                columns: vec![("x".to_string(), "integer".to_string())],
                rows: vec![vec!["1".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }
        
        tracing::info!("Intercepted unknown Tableau temp table query - returning empty success");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    None
}

/// Execute local fallback for simple queries when worker is unavailable
pub(crate) async fn execute_local_fallback(sql: &str) -> anyhow::Result<QueryExecutionResult> {
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
