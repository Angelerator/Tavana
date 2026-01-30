//! Error handling and classification for Tavana Gateway
//!
//! This module provides structured error handling that:
//! - Maps errors to proper PostgreSQL SQLSTATE codes
//! - Provides user-friendly messages with hints
//! - Categorizes errors for logging and metrics

use std::fmt;

/// PostgreSQL SQLSTATE error codes
/// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
pub mod sqlstate {
    // Class 00 — Successful Completion
    pub const SUCCESSFUL_COMPLETION: &str = "00000";

    // Class 02 — No Data
    pub const NO_DATA: &str = "02000";

    // Class 08 — Connection Exception
    pub const CONNECTION_EXCEPTION: &str = "08000";
    pub const CONNECTION_FAILURE: &str = "08006";

    // Class 22 — Data Exception
    pub const DATA_EXCEPTION: &str = "22000";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";

    // Class 23 — Integrity Constraint Violation
    pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";

    // Class 28 — Invalid Authorization Specification
    pub const INVALID_AUTHORIZATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";

    // Class 3D — Invalid Catalog Name
    pub const INVALID_CATALOG_NAME: &str = "3D000";

    // Class 3F — Invalid Schema Name
    pub const INVALID_SCHEMA_NAME: &str = "3F000";

    // Class 42 — Syntax Error or Access Rule Violation
    pub const SYNTAX_ERROR: &str = "42601";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const AMBIGUOUS_COLUMN: &str = "42702";
    pub const DUPLICATE_TABLE: &str = "42P07";

    // Class 53 — Insufficient Resources
    pub const INSUFFICIENT_RESOURCES: &str = "53000";
    pub const OUT_OF_MEMORY: &str = "53200";
    pub const DISK_FULL: &str = "53100";

    // Class 54 — Program Limit Exceeded
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const STATEMENT_TOO_COMPLEX: &str = "54001";

    // Class 55 — Object Not In Prerequisite State
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";

    // Class 57 — Operator Intervention
    pub const OPERATOR_INTERVENTION: &str = "57000";
    pub const QUERY_CANCELED: &str = "57014";

    // Class 58 — System Error (External)
    pub const SYSTEM_ERROR: &str = "58000";
    pub const IO_ERROR: &str = "58030";

    // Class XX — Internal Error
    pub const INTERNAL_ERROR: &str = "XX000";
}

/// Error category for classification and metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Data source errors (Delta Lake, Azure, S3, etc.)
    DataSource,
    /// SQL syntax errors
    Syntax,
    /// Object not found (table, column, function)
    NotFound,
    /// Query timeout
    Timeout,
    /// Authentication/authorization errors
    Auth,
    /// Resource limits (memory, connections)
    Resource,
    /// Connection errors
    Connection,
    /// Internal/unknown errors
    Internal,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::DataSource => write!(f, "data_source"),
            ErrorCategory::Syntax => write!(f, "syntax"),
            ErrorCategory::NotFound => write!(f, "not_found"),
            ErrorCategory::Timeout => write!(f, "timeout"),
            ErrorCategory::Auth => write!(f, "auth"),
            ErrorCategory::Resource => write!(f, "resource"),
            ErrorCategory::Connection => write!(f, "connection"),
            ErrorCategory::Internal => write!(f, "internal"),
        }
    }
}

/// A classified error with all information needed for proper error responses
#[derive(Debug, Clone)]
pub struct ClassifiedError {
    /// PostgreSQL SQLSTATE code
    pub sqlstate: &'static str,
    /// Error category for metrics
    pub category: ErrorCategory,
    /// User-friendly message
    pub message: String,
    /// Optional hint for the user
    pub hint: Option<String>,
    /// Optional detail with more context
    pub detail: Option<String>,
    /// Original raw error (for logging)
    pub raw_error: String,
}

impl ClassifiedError {
    /// Create a new classified error
    pub fn new(
        sqlstate: &'static str,
        category: ErrorCategory,
        message: impl Into<String>,
        raw_error: impl Into<String>,
    ) -> Self {
        Self {
            sqlstate,
            category,
            message: message.into(),
            hint: None,
            detail: None,
            raw_error: raw_error.into(),
        }
    }

    /// Add a hint to the error
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Add detail to the error
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Format for user display (includes hint if present)
    pub fn user_message(&self) -> String {
        let mut msg = self.message.clone();
        if let Some(ref hint) = self.hint {
            msg.push_str("\nHINT: ");
            msg.push_str(hint);
        }
        msg
    }
}

/// Classify an error from DuckDB/worker into a structured error
pub fn classify_error(raw_error: &str) -> ClassifiedError {
    let error_lower = raw_error.to_lowercase();

    // Delta Lake / Data Source errors
    if error_lower.contains("deltakernelgenericerror")
        || error_lower.contains("delta kernel error")
        || error_lower.contains("no files in log segment")
    {
        return classify_delta_error(raw_error);
    }

    // Azure/S3/GCS storage errors
    if error_lower.contains("azure")
        || error_lower.contains("blob")
        || error_lower.contains("s3")
        || error_lower.contains("storage")
    {
        return classify_storage_error(raw_error);
    }

    // IO errors
    if error_lower.contains("io error") {
        return ClassifiedError::new(
            sqlstate::IO_ERROR,
            ErrorCategory::DataSource,
            format!("Data access error: {}", extract_core_message(raw_error)),
            raw_error,
        )
        .with_hint("Check if the data path exists and you have access permissions.");
    }

    // Syntax errors
    if error_lower.contains("syntax error")
        || error_lower.contains("parser error")
        || error_lower.contains("parse error")
    {
        return ClassifiedError::new(
            sqlstate::SYNTAX_ERROR,
            ErrorCategory::Syntax,
            extract_core_message(raw_error),
            raw_error,
        )
        .with_hint("Check SQL syntax. Use single quotes for strings, double quotes for identifiers.");
    }

    // Catalog errors (table not found, etc.)
    if error_lower.contains("catalog error") || error_lower.contains("does not exist") {
        if error_lower.contains("table") || error_lower.contains("relation") {
            return ClassifiedError::new(
                sqlstate::UNDEFINED_TABLE,
                ErrorCategory::NotFound,
                extract_core_message(raw_error),
                raw_error,
            )
            .with_hint("Verify the table name and schema. Use SHOW TABLES to list available tables.");
        }
        if error_lower.contains("function") || error_lower.contains("table function") {
            return ClassifiedError::new(
                sqlstate::UNDEFINED_FUNCTION,
                ErrorCategory::NotFound,
                extract_core_message(raw_error),
                raw_error,
            )
            .with_hint("Check if the extension is loaded. Use LOAD extension_name; first.");
        }
        if error_lower.contains("column") {
            return ClassifiedError::new(
                sqlstate::UNDEFINED_COLUMN,
                ErrorCategory::NotFound,
                extract_core_message(raw_error),
                raw_error,
            )
            .with_hint("Verify column name. Use DESCRIBE table_name to see available columns.");
        }
    }

    // Binder errors (type mismatches, etc.)
    if error_lower.contains("binder error") {
        return ClassifiedError::new(
            sqlstate::DATA_EXCEPTION,
            ErrorCategory::Syntax,
            extract_core_message(raw_error),
            raw_error,
        )
        .with_hint("Check data types match. Use CAST() or :: for type conversion.");
    }

    // Conversion/type errors
    if error_lower.contains("conversion error")
        || error_lower.contains("could not convert")
        || error_lower.contains("type mismatch")
    {
        return ClassifiedError::new(
            sqlstate::DATA_EXCEPTION,
            ErrorCategory::Syntax,
            extract_core_message(raw_error),
            raw_error,
        )
        .with_hint("Use explicit CAST(value AS type) to convert between types.");
    }

    // Out of memory
    if error_lower.contains("out of memory")
        || error_lower.contains("memory limit")
        || error_lower.contains("oom")
    {
        return ClassifiedError::new(
            sqlstate::OUT_OF_MEMORY,
            ErrorCategory::Resource,
            "Query exceeded available memory",
            raw_error,
        )
        .with_hint(
            "Reduce result size with LIMIT, add filters in WHERE clause, or use COPY TO for large exports.",
        );
    }

    // Timeout
    if error_lower.contains("timeout") || error_lower.contains("timed out") {
        return ClassifiedError::new(
            sqlstate::QUERY_CANCELED,
            ErrorCategory::Timeout,
            extract_core_message(raw_error),
            raw_error,
        )
        .with_hint("Add LIMIT clause, use more specific filters, or request a timeout extension.");
    }

    // Transaction errors
    if error_lower.contains("transaction") {
        return ClassifiedError::new(
            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            ErrorCategory::Internal,
            extract_core_message(raw_error),
            raw_error,
        )
        .with_hint("Transaction was aborted. Start a new query or use ROLLBACK to clear state.");
    }

    // Connection errors
    if error_lower.contains("connection")
        || error_lower.contains("disconnected")
        || error_lower.contains("broken pipe")
    {
        return ClassifiedError::new(
            sqlstate::CONNECTION_FAILURE,
            ErrorCategory::Connection,
            "Connection to worker lost",
            raw_error,
        )
        .with_hint("Retry the query. If the issue persists, the worker may be overloaded.");
    }

    // Authentication errors
    if error_lower.contains("authentication")
        || error_lower.contains("unauthorized")
        || error_lower.contains("permission denied")
        || error_lower.contains("access denied")
    {
        return ClassifiedError::new(
            sqlstate::INVALID_AUTHORIZATION,
            ErrorCategory::Auth,
            "Authentication or authorization failed",
            raw_error,
        )
        .with_hint("Check your credentials and permissions for this resource.");
    }

    // Default: internal error
    ClassifiedError::new(
        sqlstate::INTERNAL_ERROR,
        ErrorCategory::Internal,
        extract_core_message(raw_error),
        raw_error,
    )
}

/// Classify Delta Lake specific errors
fn classify_delta_error(raw_error: &str) -> ClassifiedError {
    let error_lower = raw_error.to_lowercase();

    if error_lower.contains("no files in log segment") {
        return ClassifiedError::new(
            sqlstate::UNDEFINED_TABLE,
            ErrorCategory::DataSource,
            "Delta table not found or is empty",
            raw_error,
        )
        .with_hint(
            "The Delta table path does not contain valid Delta log files. \
             Verify the path exists and contains a _delta_log directory with checkpoint/json files.",
        )
        .with_detail(extract_path_from_error(raw_error));
    }

    if error_lower.contains("kernel") && error_lower.contains("error") {
        return ClassifiedError::new(
            sqlstate::IO_ERROR,
            ErrorCategory::DataSource,
            "Delta Lake error reading table",
            raw_error,
        )
        .with_hint(
            "The Delta table may be corrupted or incompatible. \
             Check the Delta table version and try running VACUUM or OPTIMIZE on the source.",
        );
    }

    // Default delta error
    ClassifiedError::new(
        sqlstate::IO_ERROR,
        ErrorCategory::DataSource,
        format!("Delta Lake error: {}", extract_core_message(raw_error)),
        raw_error,
    )
    .with_hint("Check the Delta table path and ensure it's accessible.")
}

/// Classify storage/cloud specific errors
fn classify_storage_error(raw_error: &str) -> ClassifiedError {
    let error_lower = raw_error.to_lowercase();

    if error_lower.contains("404") || error_lower.contains("not found") {
        return ClassifiedError::new(
            sqlstate::UNDEFINED_TABLE,
            ErrorCategory::DataSource,
            "Storage path not found",
            raw_error,
        )
        .with_hint("Verify the storage path (az://, s3://, gs://) is correct and the data exists.");
    }

    if error_lower.contains("403")
        || error_lower.contains("forbidden")
        || error_lower.contains("access denied")
    {
        return ClassifiedError::new(
            sqlstate::INVALID_AUTHORIZATION,
            ErrorCategory::Auth,
            "Access denied to storage location",
            raw_error,
        )
        .with_hint(
            "Check storage permissions. Ensure the service account has read access to this path.",
        );
    }

    if error_lower.contains("401") || error_lower.contains("unauthorized") {
        return ClassifiedError::new(
            sqlstate::INVALID_AUTHORIZATION,
            ErrorCategory::Auth,
            "Storage authentication failed",
            raw_error,
        )
        .with_hint("Storage credentials may have expired. Contact support if issue persists.");
    }

    if error_lower.contains("timeout") || error_lower.contains("timed out") {
        return ClassifiedError::new(
            sqlstate::CONNECTION_FAILURE,
            ErrorCategory::Connection,
            "Storage request timed out",
            raw_error,
        )
        .with_hint("The storage service is slow or unreachable. Retry the query.");
    }

    // Default storage error
    ClassifiedError::new(
        sqlstate::IO_ERROR,
        ErrorCategory::DataSource,
        format!("Storage error: {}", extract_core_message(raw_error)),
        raw_error,
    )
    .with_hint("Check storage connectivity and permissions.")
}

/// Extract the core message from a verbose error string
fn extract_core_message(error: &str) -> String {
    // Remove common prefixes
    let mut msg = error.to_string();

    // Remove QUERY_FAILED: prefix
    if let Some(pos) = msg.find("QUERY_FAILED:") {
        msg = msg[pos + 13..].trim().to_string();
    }

    // Remove IO Error: prefix
    if let Some(pos) = msg.find("IO Error:") {
        msg = msg[pos + 9..].trim().to_string();
    }

    // Remove DeltaKernel GenericError (N): prefix
    if let Some(start) = msg.find("DeltaKernel") {
        if let Some(end) = msg[start..].find(':') {
            msg = msg[start + end + 1..].trim().to_string();
        }
    }

    // Remove "Generic delta kernel error:" prefix
    if let Some(pos) = msg.find("Generic delta kernel error:") {
        msg = msg[pos + 27..].trim().to_string();
    }

    // Truncate at LINE N: for cleaner display (keep the error, not the pointer)
    if let Some(pos) = msg.find("\n\nLINE") {
        msg = msg[..pos].trim().to_string();
    }
    if let Some(pos) = msg.find("\nLINE") {
        msg = msg[..pos].trim().to_string();
    }

    // Limit length
    if msg.len() > 500 {
        msg = format!("{}...", &msg[..497]);
    }

    msg
}

/// Extract file path from error message for detail field
fn extract_path_from_error(error: &str) -> String {
    // Look for common path patterns
    for prefix in ["az://", "s3://", "gs://", "file://", "http://", "https://"] {
        if let Some(start) = error.find(prefix) {
            let rest = &error[start..];
            // Find end of path (whitespace, quote, or paren)
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == ')')
                .unwrap_or(rest.len());
            return format!("Path: {}", &rest[..end]);
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_no_files_error() {
        let error = "QUERY_FAILED: IO Error: DeltaKernel GenericError (5): Generic delta kernel error: No files in log segment\n\nLINE 1: SELECT * FROM delta_scan('az://container/path/')";

        let classified = classify_error(error);

        assert_eq!(classified.sqlstate, sqlstate::UNDEFINED_TABLE);
        assert_eq!(classified.category, ErrorCategory::DataSource);
        assert!(classified.message.contains("Delta table not found"));
        assert!(classified.hint.is_some());
        assert!(classified.hint.unwrap().contains("_delta_log"));
    }

    #[test]
    fn test_syntax_error() {
        let error = "Parser Error: syntax error at or near 'SELEC'";

        let classified = classify_error(error);

        assert_eq!(classified.sqlstate, sqlstate::SYNTAX_ERROR);
        assert_eq!(classified.category, ErrorCategory::Syntax);
    }

    #[test]
    fn test_table_not_found() {
        let error = "Catalog Error: Table 'foo' does not exist!";

        let classified = classify_error(error);

        assert_eq!(classified.sqlstate, sqlstate::UNDEFINED_TABLE);
        assert_eq!(classified.category, ErrorCategory::NotFound);
    }

    #[test]
    fn test_out_of_memory() {
        let error = "Out of Memory Error: failed to allocate 1GB";

        let classified = classify_error(error);

        assert_eq!(classified.sqlstate, sqlstate::OUT_OF_MEMORY);
        assert_eq!(classified.category, ErrorCategory::Resource);
        assert!(classified.hint.unwrap().contains("LIMIT"));
    }

    #[test]
    fn test_extract_core_message() {
        let error = "QUERY_FAILED: IO Error: DeltaKernel GenericError (5): Generic delta kernel error: No files in log segment";
        let core = extract_core_message(error);
        assert_eq!(core, "No files in log segment");
    }

    #[test]
    fn test_extract_path() {
        let error = "Error reading from az://container/path/to/data/";
        let path = extract_path_from_error(error);
        assert!(path.contains("az://container/path/to/data/"));
    }
}
