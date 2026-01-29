//! PostgreSQL type OID and length mappings
//!
//! Maps DuckDB/Arrow types to PostgreSQL wire protocol type identifiers.

/// Get the PostgreSQL type OID for a given type name
pub fn pg_type_oid(type_name: &str) -> u32 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" | "int32" => 23,
        "int8" | "bigint" | "int64" => 20,
        "int2" | "smallint" | "int16" => 21,
        "float4" | "real" | "float" => 700,
        "float8" | "double" | "float64" => 701,
        "bool" | "boolean" => 16,
        "timestamp" | "timestamptz" => 1184,
        "date" => 1082,
        _ => 25, // TEXT
    }
}

/// Get the PostgreSQL type length for a given type name
pub fn pg_type_len(type_name: &str) -> i16 {
    match type_name.to_lowercase().as_str() {
        "int4" | "integer" | "int" | "int32" => 4,
        "int8" | "bigint" | "int64" => 8,
        "int2" | "smallint" | "int16" => 2,
        "float4" | "real" | "float" => 4,
        "float8" | "double" | "float64" => 8,
        "bool" | "boolean" => 1,
        _ => -1, // Variable length
    }
}
