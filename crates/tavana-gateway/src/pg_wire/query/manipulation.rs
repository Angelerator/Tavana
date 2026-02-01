//! SQL query manipulation utilities
//!
//! Functions for modifying and transforming SQL queries.

use super::super::protocol::constants::{TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_IN_TRANSACTION};

/// Substitute $1, $2, etc. parameters in SQL with actual values
/// This converts PostgreSQL-style parameterized queries to literal SQL
pub fn substitute_parameters(sql: &str, params: &[Option<String>]) -> String {
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

/// Extract inner SELECT from COPY command if present
/// COPY (SELECT ...) TO STDOUT (FORMAT "binary") -> SELECT ...
pub fn extract_copy_inner_query(sql: &str) -> Option<String> {
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

/// Check if a SQL command changes transaction state
/// Returns (new_transaction_status, is_transaction_command)
/// This is essential for JDBC cursor-based streaming to work!
pub fn get_transaction_state_change(sql: &str, current_status: u8) -> (u8, bool) {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql_upper.trim();

    if sql_trimmed == "BEGIN"
        || sql_trimmed.starts_with("BEGIN ")
        || sql_trimmed.starts_with("START TRANSACTION")
    {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_parameters() {
        let result = substitute_parameters(
            "SELECT * FROM users WHERE id = $1 AND name = $2",
            &[Some("42".to_string()), Some("John".to_string())],
        );
        assert_eq!(result, "SELECT * FROM users WHERE id = 42 AND name = 'John'");
    }

    #[test]
    fn test_substitute_parameters_null() {
        let result = substitute_parameters(
            "SELECT * FROM users WHERE id = $1",
            &[None],
        );
        assert_eq!(result, "SELECT * FROM users WHERE id = NULL");
    }

    #[test]
    fn test_extract_copy_inner_query() {
        let result = extract_copy_inner_query(
            "COPY (SELECT * FROM users) TO STDOUT WITH (FORMAT TEXT)"
        );
        assert_eq!(result, Some("SELECT * FROM users".to_string()));
    }
}
