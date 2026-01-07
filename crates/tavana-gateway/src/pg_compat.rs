//! PostgreSQL to DuckDB Compatibility Layer
//!
//! Translates PostgreSQL-specific SQL syntax to DuckDB equivalents.
//! DuckDB already supports most PostgreSQL syntax natively (::casts, string_agg, etc.)
//! This module handles the edge cases that need rewriting.
//!
//! ## Supported Translations
//!
//! | PostgreSQL | DuckDB |
//! |------------|--------|
//! | `to_char(expr, 'format')` | `strftime(expr, 'format')` |
//! | `to_date(str, 'format')` | `strptime(str, 'format')::DATE` |
//! | `to_timestamp(str, 'format')` | `strptime(str, 'format')` |
//! | `to_number(str, 'format')` | `CAST(str AS DOUBLE)` |
//! | `regexp_matches(str, pattern)` | `regexp_extract_all(str, pattern)` |
//! | `age(timestamp)` | `current_date - timestamp` |
//!
//! ## Already Supported by DuckDB (no translation needed)
//!
//! - `value::text`, `value::varchar` (PostgreSQL-style casts)
//! - `string_agg()`, `array_agg()`
//! - `COALESCE()`, `NULLIF()`, `GREATEST()`, `LEAST()`
//! - `generate_series()`
//! - `now()`, `current_timestamp`, `current_date`
//! - `information_schema.*`

use regex::Regex;
use std::sync::LazyLock;
use tracing::debug;

/// PostgreSQL format codes to strftime format mapping
static PG_TO_STRFTIME: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    vec![
        // Year
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("IYYY", "%G"), // ISO year
        ("IY", "%g"),
        // Month
        ("MM", "%m"),
        ("Mon", "%b"),
        ("MON", "%b"),
        ("Month", "%B"),
        ("MONTH", "%B"),
        // Day
        ("DD", "%d"),
        ("DDD", "%j"), // Day of year
        ("D", "%w"),   // Day of week (0-6)
        ("Day", "%A"),
        ("DAY", "%A"),
        ("Dy", "%a"),
        ("DY", "%a"),
        // Hour
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%H"),
        // Minute/Second
        ("MI", "%M"),
        ("SS", "%S"),
        ("MS", "%f"), // Milliseconds (approximate)
        // AM/PM
        ("AM", "%p"),
        ("PM", "%p"),
        ("am", "%p"),
        ("pm", "%p"),
        // Week
        ("WW", "%W"),
        ("IW", "%V"), // ISO week
        // Quarter (no direct equivalent, needs special handling)
        // Timezone
        ("TZ", "%Z"),
        ("OF", "%z"),
    ]
});

/// Regex patterns for PostgreSQL function matching
static TO_CHAR_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Match quoted strings or expressions (handles commas inside quotes)
    Regex::new(r"(?i)\bto_char\s*\(\s*('(?:[^']|'')*'|[^,]+)\s*,\s*'([^']+)'\s*\)").unwrap()
});

static TO_DATE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bto_date\s*\(\s*('(?:[^']|'')*'|[^,]+)\s*,\s*'([^']+)'\s*\)").unwrap()
});

static TO_TIMESTAMP_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bto_timestamp\s*\(\s*('(?:[^']|'')*'|[^,]+)\s*,\s*'([^']+)'\s*\)").unwrap()
});

static TO_NUMBER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Match quoted strings or expressions before the comma
    Regex::new(r"(?i)\bto_number\s*\(\s*('(?:[^']|'')*'|[^,]+)\s*,\s*'[^']+'\s*\)").unwrap()
});

static REGEXP_MATCHES_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bregexp_matches\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)").unwrap()
});

static AGE_SINGLE_ARG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bage\s*\(\s*([^,)]+)\s*\)").unwrap()
});

/// Convert PostgreSQL date format to strftime format
fn pg_format_to_strftime(pg_format: &str) -> String {
    let mut result = pg_format.to_string();
    
    // Sort by length (descending) to avoid partial replacements
    let mut formats: Vec<_> = PG_TO_STRFTIME.iter().collect();
    formats.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    
    for (pg, strftime) in formats {
        result = result.replace(pg, strftime);
    }
    
    result
}

/// Rewrite PostgreSQL-specific SQL to DuckDB-compatible SQL
///
/// Returns the rewritten SQL if any changes were made, or the original SQL if not.
///
/// # Example
///
/// ```
/// use tavana_gateway::pg_compat::rewrite_pg_to_duckdb;
///
/// let pg_sql = "SELECT to_char(created_at, 'YYYY-MM-DD') FROM orders";
/// let duckdb_sql = rewrite_pg_to_duckdb(pg_sql);
/// assert!(duckdb_sql.contains("strftime"));
/// ```
pub fn rewrite_pg_to_duckdb(sql: &str) -> String {
    let mut result = sql.to_string();
    let mut modified = false;

    // to_char(expr, 'format') → strftime(expr, 'format')
    if TO_CHAR_REGEX.is_match(&result) {
        result = TO_CHAR_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let expr = &caps[1];
                let pg_format = &caps[2];
                let strftime_format = pg_format_to_strftime(pg_format);
                format!("strftime({}, '{}')", expr.trim(), strftime_format)
            })
            .to_string();
        modified = true;
    }

    // to_date(str, 'format') → strptime(str, 'format')::DATE
    if TO_DATE_REGEX.is_match(&result) {
        result = TO_DATE_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let expr = &caps[1];
                let pg_format = &caps[2];
                let strftime_format = pg_format_to_strftime(pg_format);
                format!("CAST(strptime({}, '{}') AS DATE)", expr.trim(), strftime_format)
            })
            .to_string();
        modified = true;
    }

    // to_timestamp(str, 'format') → strptime(str, 'format')
    if TO_TIMESTAMP_REGEX.is_match(&result) {
        result = TO_TIMESTAMP_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let expr = &caps[1];
                let pg_format = &caps[2];
                let strftime_format = pg_format_to_strftime(pg_format);
                format!("strptime({}, '{}')", expr.trim(), strftime_format)
            })
            .to_string();
        modified = true;
    }

    // to_number(str, 'format') → CAST(str AS DOUBLE)
    // Note: DuckDB doesn't have format-aware number parsing
    if TO_NUMBER_REGEX.is_match(&result) {
        result = TO_NUMBER_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let expr = &caps[1];
                format!("CAST({} AS DOUBLE)", expr.trim())
            })
            .to_string();
        modified = true;
    }

    // regexp_matches(str, pattern) → regexp_extract_all(str, pattern)
    if REGEXP_MATCHES_REGEX.is_match(&result) {
        result = REGEXP_MATCHES_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let str_expr = &caps[1];
                let pattern = &caps[2];
                format!("regexp_extract_all({}, {})", str_expr.trim(), pattern.trim())
            })
            .to_string();
        modified = true;
    }

    // age(timestamp) → current_date - timestamp (simplified)
    // Note: PostgreSQL's age() returns an interval, this is an approximation
    if AGE_SINGLE_ARG_REGEX.is_match(&result) {
        result = AGE_SINGLE_ARG_REGEX
            .replace_all(&result, |caps: &regex::Captures| {
                let timestamp = &caps[1];
                format!("(current_date - CAST({} AS DATE))", timestamp.trim())
            })
            .to_string();
        modified = true;
    }

    if modified {
        debug!(
            original = sql,
            rewritten = %result,
            "Rewrote PostgreSQL SQL to DuckDB"
        );
    }

    result
}

/// Check if SQL contains any PostgreSQL-specific syntax that needs rewriting
pub fn needs_rewrite(sql: &str) -> bool {
    let sql_upper = sql.to_uppercase();
    sql_upper.contains("TO_CHAR")
        || sql_upper.contains("TO_DATE")
        || sql_upper.contains("TO_TIMESTAMP")
        || sql_upper.contains("TO_NUMBER")
        || sql_upper.contains("REGEXP_MATCHES")
        || (sql_upper.contains("AGE(") && !sql_upper.contains("AGE(,"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_char_rewrite() {
        let input = "SELECT to_char(created_at, 'YYYY-MM-DD') FROM orders";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("strftime(created_at, '%Y-%m-%d')"));
    }

    #[test]
    fn test_to_char_with_time() {
        let input = "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS')";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("strftime(ts, '%Y-%m-%d %H:%M:%S')"));
    }

    #[test]
    fn test_to_date_rewrite() {
        let input = "SELECT to_date('2024-01-15', 'YYYY-MM-DD')";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("CAST(strptime('2024-01-15', '%Y-%m-%d') AS DATE)"));
    }

    #[test]
    fn test_to_timestamp_rewrite() {
        let input = "SELECT to_timestamp('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("strptime('2024-01-15 10:30:00', '%Y-%m-%d %H:%M:%S')"));
    }

    #[test]
    fn test_to_number_rewrite() {
        let input = "SELECT to_number('12,345.67', '99,999.99')";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("CAST('12,345.67' AS DOUBLE)"));
    }

    #[test]
    fn test_regexp_matches_rewrite() {
        let input = "SELECT regexp_matches(email, '@(.+)$')";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("regexp_extract_all(email, '@(.+)$')"));
    }

    #[test]
    fn test_age_rewrite() {
        let input = "SELECT age(birth_date) FROM users";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("current_date - CAST(birth_date AS DATE)"));
    }

    #[test]
    fn test_no_rewrite_needed() {
        let input = "SELECT * FROM users WHERE id = 1";
        let output = rewrite_pg_to_duckdb(input);
        assert_eq!(input, output);
    }

    #[test]
    fn test_native_pg_syntax_unchanged() {
        // These are already supported by DuckDB natively
        let input = "SELECT name::text, string_agg(tag, ',') FROM items GROUP BY name";
        let output = rewrite_pg_to_duckdb(input);
        assert_eq!(input, output);
    }

    #[test]
    fn test_multiple_rewrites() {
        let input = "SELECT to_char(d, 'YYYY'), to_date(s, 'MM-DD') FROM t";
        let output = rewrite_pg_to_duckdb(input);
        assert!(output.contains("strftime(d, '%Y')"));
        assert!(output.contains("strptime(s, '%m-%d')"));
    }

    #[test]
    fn test_needs_rewrite() {
        assert!(needs_rewrite("SELECT to_char(now(), 'YYYY')"));
        assert!(needs_rewrite("SELECT TO_DATE('2024', 'YYYY')"));
        assert!(!needs_rewrite("SELECT * FROM users"));
        assert!(!needs_rewrite("SELECT name::text FROM items"));
    }
}

