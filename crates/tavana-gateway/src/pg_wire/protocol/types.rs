//! PostgreSQL type OID and length mappings
//!
//! Maps DuckDB/Arrow types to PostgreSQL wire protocol type identifiers.
//! Reference: https://duckdb.org/docs/stable/sql/data_types/overview
//! Arrow types: https://arrow.apache.org/docs/format/CDataInterface.html
//! PostgreSQL OIDs: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

/// Get the PostgreSQL type OID for a given type name
/// Supports both DuckDB type names and Arrow type names (from worker metadata)
pub fn pg_type_oid(type_name: &str) -> u32 {
    match type_name.to_lowercase().as_str() {
        // === SIGNED INTEGERS ===
        // Arrow: Int8, DuckDB: TINYINT/INT1 - promoted to INT2 (no PG int1)
        "int1" | "tinyint" | "int8" => 21, // Promoted to INT2
        // Arrow: Int16, DuckDB: SMALLINT/INT2
        "int2" | "smallint" | "int16" | "short" => 21,
        // Arrow: Int32, DuckDB: INTEGER/INT4/INT
        "int4" | "integer" | "int" | "int32" | "signed" => 23,
        // Arrow: Int64, DuckDB: BIGINT/INT64
        "bigint" | "int64" | "long" => 20,
        // HUGEINT - no PG equivalent, use NUMERIC
        "int128" | "hugeint" => 1700,
        
        // === UNSIGNED INTEGERS ===
        // Arrow: UInt8, DuckDB: UTINYINT - promoted to INT2
        "uint1" | "utinyint" | "uint8" => 21,
        // Arrow: UInt16, DuckDB: USMALLINT - promoted to INT4
        "uint2" | "usmallint" | "uint16" => 23,
        // Arrow: UInt32, DuckDB: UINTEGER - promoted to INT8
        "uint4" | "uinteger" | "uint32" => 20,
        // Arrow: UInt64, DuckDB: UBIGINT - use NUMERIC (can overflow int8)
        "ubigint" | "uint64" => 1700,
        "uint128" | "uhugeint" => 1700,
        
        // === FLOATING POINT ===
        // Arrow: Float32, DuckDB: REAL/FLOAT
        "float4" | "real" | "float" | "float32" => 700,
        // Arrow: Float64, DuckDB: DOUBLE
        "float8" | "double" | "float64" | "double precision" => 701,
        
        // === DECIMAL/NUMERIC ===
        // Arrow: Decimal128
        "decimal" | "numeric" | "decimal128" => 1700,
        
        // === BOOLEAN ===
        // Arrow: Boolean
        "bool" | "boolean" | "logical" => 16,
        
        // === DATE/TIME ===
        // Arrow: Date32
        "date" | "date32" => 1082,
        // Arrow: Time64
        "time" | "time without time zone" | "time64" => 1083,
        "timetz" | "time with time zone" => 1266,
        // Arrow: Timestamp
        "timestamp" | "datetime" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        // Arrow: Duration/Interval
        "interval" | "duration" => 1186,
        
        // === UUID ===
        "uuid" => 2950,
        
        // === BINARY DATA ===
        // Arrow: Binary/LargeBinary
        "bytea" | "blob" | "binary" | "varbinary" | "largebinary" => 17,
        
        // === TEXT TYPES ===
        // Arrow: Utf8/LargeUtf8
        "varchar" | "char" | "bpchar" | "text" | "string" | "utf8" | "largeutf8" => 25,
        
        // === JSON ===
        "json" => 114,
        "jsonb" => 3802,
        
        // === BIT STRINGS ===
        "bit" | "bitstring" => 1560,
        "varbit" => 1562,
        
        // === NESTED TYPES (send as TEXT/JSON) ===
        // Arrow: List/LargeList/FixedSizeList/Map/Struct/Union
        "list" | "array" | "largelist" | "fixedsizelist" => 25,
        "map" | "struct" | "union" => 25,
        
        // Default: TEXT
        _ => 25,
    }
}

/// Get the PostgreSQL type length for a given type name
/// Returns fixed size for fixed-length types, -1 for variable-length
/// Supports both DuckDB type names and Arrow type names
pub fn pg_type_len(type_name: &str) -> i16 {
    match type_name.to_lowercase().as_str() {
        // === SIGNED INTEGERS ===
        // Arrow: Int8 (promoted to Int16)
        "int1" | "tinyint" | "int8" => 2,
        // Arrow: Int16
        "int2" | "smallint" | "int16" | "short" => 2,
        // Arrow: Int32
        "int4" | "integer" | "int" | "int32" | "signed" => 4,
        // Arrow: Int64
        "bigint" | "int64" | "long" => 8,
        "int128" | "hugeint" => -1,
        
        // === UNSIGNED INTEGERS ===
        // Arrow: UInt8 (promoted to Int16)
        "uint1" | "utinyint" | "uint8" => 2,
        // Arrow: UInt16 (promoted to Int32)
        "uint2" | "usmallint" | "uint16" => 4,
        // Arrow: UInt32 (promoted to Int64)
        "uint4" | "uinteger" | "uint32" => 8,
        // Arrow: UInt64 (NUMERIC - variable)
        "ubigint" | "uint64" => -1,
        "uint128" | "uhugeint" => -1,
        
        // === FLOATING POINT ===
        // Arrow: Float32
        "float4" | "real" | "float" | "float32" => 4,
        // Arrow: Float64
        "float8" | "double" | "float64" | "double precision" => 8,
        // Arrow: Decimal128
        "decimal" | "numeric" | "decimal128" => -1,
        
        // === BOOLEAN ===
        "bool" | "boolean" | "logical" => 1,
        
        // === DATE/TIME ===
        // Arrow: Date32
        "date" | "date32" => 4,
        // Arrow: Time64
        "time" | "time without time zone" | "time64" => 8,
        "timetz" | "time with time zone" => 12,
        // Arrow: Timestamp
        "timestamp" | "datetime" => 8,
        "timestamptz" | "timestamp with time zone" => 8,
        // Arrow: Duration/Interval
        "interval" | "duration" => 16,
        
        // === UUID ===
        "uuid" => 16,
        
        // === BINARY/TEXT (variable length) ===
        // Arrow: Binary/LargeBinary
        "bytea" | "blob" | "binary" | "varbinary" | "largebinary" => -1,
        // Arrow: Utf8/LargeUtf8
        "varchar" | "char" | "bpchar" | "text" | "string" | "utf8" | "largeutf8" => -1,
        "json" | "jsonb" => -1,
        "bit" | "bitstring" | "varbit" => -1,
        
        // === NESTED TYPES ===
        // Arrow: List/LargeList/FixedSizeList/Map/Struct/Union
        "list" | "array" | "largelist" | "fixedsizelist" => -1,
        "map" | "struct" | "union" => -1,
        
        // Default: variable length
        _ => -1,
    }
}
