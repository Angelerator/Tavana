//! PostgreSQL command interception
//!
//! Intercepts PostgreSQL-specific commands (SET, SHOW, BEGIN, etc.) and returns
//! fake responses without sending them to the worker. This enables compatibility
//! with PostgreSQL clients that expect these commands to work.

use crate::cursors::CursorResult;
use tracing::info;

/// Result of a query execution (local or remote)
#[derive(Debug, Clone)]
pub struct QueryExecutionResult {
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

impl Default for QueryExecutionResult {
    fn default() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: None,
        }
    }
}

/// Handle PostgreSQL-specific commands that don't need to go to the worker
/// Returns Some(result) if the command was intercepted, None otherwise
pub fn handle_pg_specific_command(sql: &str) -> Option<QueryExecutionResult> {
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

    if sql_trimmed == "BEGIN"
        || sql_trimmed.starts_with("BEGIN ")
        || sql_trimmed.starts_with("START TRANSACTION")
    {
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

    if sql_trimmed.starts_with("DISCARD ") || sql_trimmed.starts_with("DEALLOCATE ") {
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    // NOTE: DECLARE CURSOR, FETCH, and CLOSE are handled in run_query_loop_generic
    // because they require access to per-connection cursor state

    // =========================================================================
    // TABLEAU TEMP TABLE INTERCEPTION
    // =========================================================================
    // Tableau creates temporary tables like #Tableau_N_GUID_N_Connect_Check for
    // connection validation. Since Tavana uses stateless workers, temp tables
    // created on one worker don't exist on another. We intercept these and
    // return fake success responses to make Tableau's connection check pass.
    // =========================================================================

    // Check if this is ANY Tableau temp table operation
    let is_tableau_temp = sql_upper.contains("#TABLEAU_");

    if is_tableau_temp {
        info!(
            "Intercepting Tableau temp table query: {}",
            if sql.len() > 100 { &sql[..100] } else { sql }
        );

        // CREATE TABLE (with or without TEMP/TEMPORARY keyword)
        if sql_upper.contains("CREATE") && sql_upper.contains("TABLE") {
            info!("Intercepted Tableau CREATE TABLE - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("CREATE TABLE".to_string()),
            });
        }

        // SELECT INTO (Tableau uses this for temp table creation)
        if sql_upper.contains("SELECT") && sql_upper.contains("INTO") {
            info!("Intercepted Tableau SELECT INTO - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("SELECT 0".to_string()),
            });
        }

        // DROP TABLE
        if sql_upper.contains("DROP") {
            info!("Intercepted Tableau DROP TABLE - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                command_tag: Some("DROP TABLE".to_string()),
            });
        }

        // INSERT INTO
        if sql_upper.contains("INSERT") {
            info!("Intercepted Tableau INSERT - returning success");
            return Some(QueryExecutionResult {
                columns: vec![],
                rows: vec![],
                row_count: 1,
                command_tag: Some("INSERT 0 1".to_string()),
            });
        }

        // SELECT FROM #Tableau_ table - return one success row
        if sql_upper.contains("SELECT") && sql_upper.contains("FROM") {
            info!("Intercepted Tableau SELECT FROM temp table - returning success row");
            return Some(QueryExecutionResult {
                columns: vec![("x".to_string(), "integer".to_string())],
                rows: vec![vec!["1".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }

        // Any other query referencing #Tableau_ - return empty success
        info!("Intercepted unknown Tableau temp table query - returning empty success");
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: Some("OK".to_string()),
        });
    }

    // IMPORTANT: Do NOT intercept pg_catalog queries!
    // DuckDB has full native pg_catalog support with real metadata:
    // - pg_class, pg_namespace, pg_type, pg_attribute, pg_proc, pg_settings, etc.
    // Let all pg_catalog queries pass through to DuckDB for proper client compatibility.
    // Previously this returned empty results which broke DBeaver and other clients.

    // =========================================================================
    // REDSHIFT SYSTEM TABLE INTERCEPTION
    // =========================================================================
    // The Redshift ODBC/JDBC driver queries Redshift-specific system tables
    // (stv_*, stl_*, svv_*, svl_*) during connection validation and metadata
    // discovery. Since Tavana uses DuckDB (which doesn't have these tables),
    // we return empty results to satisfy the driver's expectations.
    // =========================================================================

    // Check for Redshift system table queries
    if sql_upper.contains("STV_") || sql_upper.contains("STL_") 
        || sql_upper.contains("SVV_") || sql_upper.contains("SVL_")
        || sql_upper.contains("PG_USER_INFO") 
        || sql_upper.contains("PG_DEFAULT_ACL")
    {
        info!(
            "Intercepting Redshift system table query: {}",
            if sql.len() > 100 { &sql[..100] } else { sql }
        );

        // STV_SESSIONS - active sessions (used by driver for connection validation)
        if sql_upper.contains("STV_SESSIONS") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("user_name".to_string(), "text".to_string()),
                    ("db_name".to_string(), "text".to_string()),
                    ("process".to_string(), "int4".to_string()),
                ],
                rows: vec![vec!["tavana".to_string(), "main".to_string(), "1".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }

        // STV_INFLIGHT - running queries
        if sql_upper.contains("STV_INFLIGHT") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("userid".to_string(), "int4".to_string()),
                    ("query".to_string(), "text".to_string()),
                ],
                rows: vec![],
                row_count: 0,
                command_tag: None,
            });
        }

        // SVV_TABLES - all tables (for metadata discovery)
        if sql_upper.contains("SVV_TABLES") || sql_upper.contains("SVV_ALL_TABLES") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("table_catalog".to_string(), "text".to_string()),
                    ("table_schema".to_string(), "text".to_string()),
                    ("table_name".to_string(), "text".to_string()),
                    ("table_type".to_string(), "text".to_string()),
                ],
                rows: vec![],
                row_count: 0,
                command_tag: None,
            });
        }

        // SVV_COLUMNS - column metadata
        if sql_upper.contains("SVV_COLUMNS") || sql_upper.contains("SVV_ALL_COLUMNS") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("table_catalog".to_string(), "text".to_string()),
                    ("table_schema".to_string(), "text".to_string()),
                    ("table_name".to_string(), "text".to_string()),
                    ("column_name".to_string(), "text".to_string()),
                    ("data_type".to_string(), "text".to_string()),
                ],
                rows: vec![],
                row_count: 0,
                command_tag: None,
            });
        }

        // SVV_EXTERNAL_SCHEMAS - external schemas (Spectrum)
        if sql_upper.contains("SVV_EXTERNAL") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("schemaname".to_string(), "text".to_string()),
                    ("databasename".to_string(), "text".to_string()),
                ],
                rows: vec![],
                row_count: 0,
                command_tag: None,
            });
        }

        // PG_USER_INFO - Redshift-specific user info
        if sql_upper.contains("PG_USER_INFO") {
            return Some(QueryExecutionResult {
                columns: vec![
                    ("usename".to_string(), "text".to_string()),
                    ("usesysid".to_string(), "int4".to_string()),
                ],
                rows: vec![vec!["tavana".to_string(), "1".to_string()]],
                row_count: 1,
                command_tag: None,
            });
        }

        // Default: return empty result for any other Redshift system table
        return Some(QueryExecutionResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            command_tag: None,
        });
    }

    None
}

/// Execute local fallback for simple queries when worker is unavailable
pub async fn execute_local_fallback(sql: &str) -> anyhow::Result<QueryExecutionResult> {
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
