//! PostgreSQL type OID and length mappings
//!
//! Maps DuckDB/Arrow types to PostgreSQL wire protocol type identifiers.
//! Reference: https://duckdb.org/docs/stable/sql/data_types/overview
//! PostgreSQL OIDs: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

/// Get the PostgreSQL type OID for a given type name
pub fn pg_type_oid(type_name: &str) -> u32 {
    match type_name.to_lowercase().as_str() {
        // === SIGNED INTEGERS ===
        // TINYINT (INT1) - no native PG type, use INT2 (21) for compatibility
        "int1" | "tinyint" => 21, // Promoted to INT2 for client compatibility
        "int2" | "smallint" | "int16" | "short" => 21,
        "int4" | "integer" | "int" | "int32" | "signed" => 23,
        "int8" | "bigint" | "int64" | "long" => 20,
        // HUGEINT - no PG equivalent, use NUMERIC
        "int128" | "hugeint" => 1700, // NUMERIC
        
        // === UNSIGNED INTEGERS ===
        // Promoted to larger signed types for value range
        "uint1" | "utinyint" => 21,   // → INT2
        "uint2" | "usmallint" => 23,  // → INT4
        "uint4" | "uinteger" => 20,   // → INT8
        // UBIGINT/UHUGEINT - can overflow, use NUMERIC
        "uint8" | "ubigint" => 1700,  // NUMERIC
        "uint128" | "uhugeint" => 1700, // NUMERIC
        
        // === FLOATING POINT ===
        "float4" | "real" | "float" => 700,
        "float8" | "double" | "float64" | "double precision" => 701,
        
        // === DECIMAL/NUMERIC ===
        "decimal" | "numeric" => 1700,
        
        // === BOOLEAN ===
        "bool" | "boolean" | "logical" => 16,
        
        // === DATE/TIME ===
        "date" => 1082,
        "time" | "time without time zone" => 1083,
        "timetz" | "time with time zone" => 1266,
        "timestamp" | "datetime" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        "interval" => 1186,
        
        // === UUID ===
        "uuid" => 2950,
        
        // === BINARY DATA ===
        "bytea" | "blob" | "binary" | "varbinary" => 17,
        
        // === TEXT TYPES ===
        "varchar" | "char" | "bpchar" | "text" | "string" => 25,
        
        // === JSON ===
        "json" => 114,
        "jsonb" => 3802,
        
        // === BIT STRINGS ===
        "bit" | "bitstring" => 1560,
        "varbit" => 1562,
        
        // === NESTED TYPES (send as TEXT/JSON) ===
        "list" | "array" => 25, // TEXT representation
        "map" | "struct" | "union" => 25, // TEXT representation
        
        // Default: TEXT
        _ => 25,
    }
}

/// Get the PostgreSQL type length for a given type name
/// Returns fixed size for fixed-length types, -1 for variable-length
pub fn pg_type_len(type_name: &str) -> i16 {
    match type_name.to_lowercase().as_str() {
        // === SIGNED INTEGERS ===
        "int1" | "tinyint" => 2, // Promoted to INT2
        "int2" | "smallint" | "int16" | "short" => 2,
        "int4" | "integer" | "int" | "int32" | "signed" => 4,
        "int8" | "bigint" | "int64" | "long" => 8,
        "int128" | "hugeint" => -1, // Variable (NUMERIC)
        
        // === UNSIGNED INTEGERS ===
        "uint1" | "utinyint" => 2,  // → INT2
        "uint2" | "usmallint" => 4, // → INT4
        "uint4" | "uinteger" => 8,  // → INT8
        "uint8" | "ubigint" => -1,  // Variable (NUMERIC)
        "uint128" | "uhugeint" => -1, // Variable (NUMERIC)
        
        // === FLOATING POINT ===
        "float4" | "real" | "float" => 4,
        "float8" | "double" | "float64" | "double precision" => 8,
        "decimal" | "numeric" => -1, // Variable
        
        // === BOOLEAN ===
        "bool" | "boolean" | "logical" => 1,
        
        // === DATE/TIME ===
        "date" => 4,
        "time" | "time without time zone" => 8,
        "timetz" | "time with time zone" => 12,
        "timestamp" | "datetime" => 8,
        "timestamptz" | "timestamp with time zone" => 8,
        "interval" => 16,
        
        // === UUID ===
        "uuid" => 16,
        
        // === BINARY/TEXT (variable length) ===
        "bytea" | "blob" | "binary" | "varbinary" => -1,
        "varchar" | "char" | "bpchar" | "text" | "string" => -1,
        "json" | "jsonb" => -1,
        "bit" | "bitstring" | "varbit" => -1,
        
        // === NESTED TYPES ===
        "list" | "array" | "map" | "struct" | "union" => -1,
        
        // Default: variable length
        _ => -1,
    }
}
