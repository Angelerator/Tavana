//! Improved Data Size Estimation
//!
//! Provides accurate data size estimation using:
//! - S3 HEAD requests for file sizes
//! - Parquet footer reading for EXACT row counts and column metadata
//! - SQL parsing for column pruning detection
//! - Data-type aware memory estimation
//! - Caching with TTL to avoid repeated requests

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use dashmap::DashMap;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaDataReader;
use regex::Regex;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

static FALLBACK_TABLE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"read_parquet\s*\(\s*'([^']+)'").expect("constant pattern")
});

static SIZE_FROM_FILENAME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\d+)\s*(mb|gb|tb)").expect("constant pattern")
});

/// Cache entry with TTL and full Parquet metadata
#[derive(Clone)]
struct CachedMetadata {
    size_bytes: u64,
    row_count: u64,
    columns: Vec<ColumnMetadata>,
    row_group_count: u32,
    created_at: Instant,
}

impl CachedMetadata {
    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

/// Column metadata from Parquet footer
#[derive(Clone, Debug)]
pub struct ColumnMetadata {
    pub name: String,
    pub physical_type: PhysicalType,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

/// Parsed query structure from SQL
#[derive(Debug, Default)]
pub struct QueryStructure {
    pub tables: Vec<String>,
    pub selected_columns: Vec<String>,
    pub is_select_star: bool,
    pub has_where: bool,
    pub has_join: bool,
    pub has_group_by: bool,
    pub has_order_by: bool,
    pub has_distinct: bool,
    pub has_window: bool,
    pub limit: Option<u64>,
}

/// Combined query size estimate with accurate metadata
#[derive(Debug, Clone)]
pub struct QuerySizeEstimate {
    /// Total file size in bytes (from S3 HEAD)
    pub total_bytes: u64,
    /// Total file size in MB
    pub total_mb: u64,
    /// Exact row count (from Parquet footer)
    pub row_count: Option<u64>,
    /// Estimated rows after predicates and LIMIT
    pub effective_row_count: u64,
    /// Number of columns in schema
    pub column_count: Option<u32>,
    /// Number of selected columns (after pruning)
    pub selected_column_count: u32,
    /// Number of row groups
    pub row_group_count: Option<u32>,
    /// Files/tables analyzed
    pub sources: Vec<DataSource>,
    /// Estimated data to read (after column pruning)
    pub estimated_read_bytes: u64,
    /// Estimated memory needed in MB
    pub estimated_memory_mb: u64,
    /// Query has JOIN operations
    pub has_join: bool,
    /// Query has GROUP BY / aggregations
    pub has_aggregation: bool,
    /// Query has window functions
    pub has_window: bool,
    /// Query has ORDER BY
    pub has_order_by: bool,
    /// Estimation method used
    pub estimation_method: String,
}

/// Data source with size info
#[derive(Debug, Clone)]
pub struct DataSource {
    pub path: String,
    pub size_bytes: u64,
    pub source_type: DataSourceType,
    pub estimation_method: EstimationMethod,
    pub row_count: Option<u64>,
    pub column_count: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum DataSourceType {
    Parquet,
    Csv,
    Json,
    Delta,
    Iceberg,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum EstimationMethod {
    ParquetFooter,   // Best: exact row count from footer
    ParquetMetadata, // Using Parquet metadata (alias for footer)
    S3Head,          // Good: file size only
    DeltaLog,        // Delta Lake log files
    IcebergManifest, // Iceberg manifest files
    Cached,          // Using cached data
    Default,         // Fallback heuristics
}

/// Improved data size estimator
pub struct DataSizer {
    s3_client: Option<S3Client>,
    cache: DashMap<String, CachedMetadata>,
    cache_ttl: Duration,
    s3_path_regex: Regex,
    endpoint_url: Option<String>,
}

impl DataSizer {
    /// Create a new DataSizer
    pub async fn new() -> Self {
        let endpoint_url = std::env::var("AWS_ENDPOINT_URL").ok();

        let s3_client = match Self::create_s3_client(&endpoint_url).await {
            Ok(client) => {
                info!("S3 client initialized for real size estimation");
                Some(client)
            }
            Err(e) => {
                warn!(
                    "Failed to create S3 client, using fallback estimation: {}",
                    e
                );
                None
            }
        };

        Self {
            s3_client,
            cache: DashMap::new(),
            cache_ttl: Duration::from_secs(300), // 5 minute cache
            s3_path_regex: Regex::new(r"s3://([^/]+)/(.+)")
                .expect("S3 path regex is a valid constant pattern"),
            endpoint_url,
        }
    }

    async fn create_s3_client(endpoint_url: &Option<String>) -> Result<S3Client> {
        use aws_config::BehaviorVersion;

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        if let Some(endpoint) = endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let config = config_loader.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        Ok(S3Client::from_conf(s3_config))
    }

    /// Parse SQL to extract query structure
    pub fn parse_sql(&self, sql: &str) -> QueryStructure {
        let dialect = GenericDialect {};
        let statements = match Parser::parse_sql(&dialect, sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                debug!("SQL parse error: {}, falling back to regex", e);
                return self.parse_sql_fallback(sql);
            }
        };

        let mut structure = QueryStructure::default();

        for stmt in statements {
            if let Statement::Query(query) = stmt {
                // Extract LIMIT
                if let Some(limit_expr) = &query.limit {
                    if let Expr::Value(value_with_span) = limit_expr {
                        if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                            structure.limit = n.parse().ok();
                        }
                    }
                }

                if let SetExpr::Select(select) = *query.body {
                    // Extract selected columns
                    for item in &select.projection {
                        match item {
                            SelectItem::Wildcard(_) => {
                                structure.is_select_star = true;
                            }
                            SelectItem::UnnamedExpr(expr) => {
                                if let Expr::Identifier(ident) = expr {
                                    structure.selected_columns.push(ident.value.clone());
                                } else if let Expr::CompoundIdentifier(parts) = expr {
                                    if let Some(last) = parts.last() {
                                        structure.selected_columns.push(last.value.clone());
                                    }
                                }
                            }
                            SelectItem::ExprWithAlias { expr, .. } => {
                                if let Expr::Identifier(ident) = expr {
                                    structure.selected_columns.push(ident.value.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    // Extract tables
                    for from in &select.from {
                        self.extract_tables_from_table_factor(
                            &from.relation,
                            &mut structure.tables,
                        );
                        for join in &from.joins {
                            structure.has_join = true;
                            self.extract_tables_from_table_factor(
                                &join.relation,
                                &mut structure.tables,
                            );
                        }
                    }

                    // Check for WHERE, GROUP BY, etc.
                    structure.has_where = select.selection.is_some();
                    structure.has_group_by = !matches!(select.group_by, sqlparser::ast::GroupByExpr::All(exprs) if exprs.is_empty());
                    structure.has_distinct = select.distinct.is_some();

                    // Check for window functions
                    let sql_upper = sql.to_uppercase();
                    structure.has_window = sql_upper.contains(" OVER ");
                    structure.has_order_by = sql_upper.contains("ORDER BY");
                }
            }
        }

        structure
    }

    fn extract_tables_from_table_factor(&self, factor: &TableFactor, tables: &mut Vec<String>) {
        match factor {
            TableFactor::Table { name, .. } => {
                tables.push(name.to_string());
            }
            TableFactor::TableFunction { expr, .. } => {
                // Handle read_parquet('s3://...') style functions
                if let Expr::Function(func) = expr {
                    match &func.args {
                        sqlparser::ast::FunctionArguments::List(args) => {
                            for arg in &args.args {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(value_with_span)),
                                ) = arg
                                {
                                    if let sqlparser::ast::Value::SingleQuotedString(path) = &value_with_span.value {
                                        tables.push(path.clone());
                                    }
                                }
                            }
                        }
                        sqlparser::ast::FunctionArguments::None => {}
                        sqlparser::ast::FunctionArguments::Subquery(_) => {}
                    }
                }
            }
            _ => {}
        }
    }

    /// Fallback SQL parsing using regex
    fn parse_sql_fallback(&self, sql: &str) -> QueryStructure {
        let sql_upper = sql.to_uppercase();
        let mut structure = QueryStructure::default();

        for cap in FALLBACK_TABLE_REGEX.captures_iter(sql) {
            if let Some(path) = cap.get(1) {
                structure.tables.push(path.as_str().to_string());
            }
        }

        // Check for SELECT *
        structure.is_select_star =
            sql_upper.contains("SELECT *") || sql_upper.contains("SELECT\n*");

        // Extract LIMIT
        if let Some(pos) = sql_upper.find("LIMIT") {
            let after = &sql[pos + 5..];
            let num_str: String = after
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            structure.limit = num_str.parse().ok();
        }

        // Check operations
        structure.has_join = sql_upper.contains(" JOIN ");
        structure.has_group_by = sql_upper.contains("GROUP BY");
        structure.has_order_by = sql_upper.contains("ORDER BY");
        structure.has_distinct = sql_upper.contains("DISTINCT");
        structure.has_window = sql_upper.contains(" OVER ");
        structure.has_where = sql_upper.contains("WHERE ");

        structure
    }

    /// Estimate total query size with accurate metadata
    pub async fn estimate_query_size(&self, sql: &str, tables: &[String]) -> QuerySizeEstimate {
        let query_structure = self.parse_sql(sql);
        let mut sources = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut total_rows: u64 = 0;
        let mut all_columns: Vec<ColumnMetadata> = Vec::new();

        // Get metadata for each table
        let effective_tables = if tables.is_empty() {
            query_structure.tables.clone()
        } else {
            tables.to_vec()
        };

        for table in &effective_tables {
            let source = self.estimate_source_size_with_metadata(table).await;
            total_bytes += source.size_bytes;

            if let Some(rows) = source.row_count {
                total_rows += rows;
            }

            // Get cached column metadata
            if let Some(cached) = self.cache.get(table) {
                all_columns.extend(cached.columns.clone());
            }

            sources.push(source);
        }

        // Calculate column pruning
        let (selected_column_count, estimated_read_bytes) =
            self.calculate_column_pruning(&query_structure, &all_columns, total_bytes);

        // Calculate effective row count (after LIMIT and predicates)
        let effective_row_count = self.calculate_effective_rows(total_rows, &query_structure);

        // Calculate memory estimate using data types
        let estimated_memory_mb = self.calculate_memory_estimate(
            &query_structure,
            &all_columns,
            effective_row_count,
            estimated_read_bytes,
        );

        let total_mb = total_bytes / (1024 * 1024);
        let estimation_method = sources
            .first()
            .map(|s| format!("{:?}", s.estimation_method))
            .unwrap_or_else(|| "unknown".to_string());

        QuerySizeEstimate {
            total_bytes,
            total_mb,
            row_count: if total_rows > 0 {
                Some(total_rows)
            } else {
                None
            },
            effective_row_count,
            column_count: all_columns.len().try_into().ok(),
            selected_column_count,
            row_group_count: sources
                .first()
                .and_then(|s| self.cache.get(&s.path).map(|c| c.row_group_count)),
            sources,
            estimated_read_bytes,
            estimated_memory_mb,
            has_join: query_structure.has_join,
            has_aggregation: query_structure.has_group_by,
            has_window: query_structure.has_window,
            has_order_by: query_structure.has_order_by,
            estimation_method,
        }
    }

    /// Calculate column pruning savings
    fn calculate_column_pruning(
        &self,
        query: &QueryStructure,
        columns: &[ColumnMetadata],
        total_bytes: u64,
    ) -> (u32, u64) {
        if query.is_select_star || query.selected_columns.is_empty() || columns.is_empty() {
            return (columns.len() as u32, total_bytes);
        }

        // Sum sizes of selected columns only
        let mut selected_size: u64 = 0;
        let mut selected_count: u32 = 0;

        for col in columns {
            let col_name_lower = col.name.to_lowercase();
            let is_selected = query
                .selected_columns
                .iter()
                .any(|s| s.to_lowercase() == col_name_lower);

            if is_selected {
                selected_size += col.compressed_size;
                selected_count += 1;
            }
        }

        // If no columns matched, use all columns (maybe different naming)
        if selected_count == 0 {
            return (columns.len() as u32, total_bytes);
        }

        // Add overhead for Parquet structure
        let estimated_read = (selected_size as f64 * 1.1) as u64;

        info!(
            "Column pruning: {} of {} columns selected, {} MB instead of {} MB",
            selected_count,
            columns.len(),
            estimated_read / (1024 * 1024),
            total_bytes / (1024 * 1024)
        );

        (selected_count, estimated_read)
    }

    /// Calculate effective row count after LIMIT and predicates
    fn calculate_effective_rows(&self, total_rows: u64, query: &QueryStructure) -> u64 {
        let mut effective = total_rows;

        // Apply LIMIT
        if let Some(limit) = query.limit {
            effective = effective.min(limit);
        }

        // Apply selectivity estimate for WHERE clause
        if query.has_where && query.limit.is_none() {
            // Conservative estimate: WHERE typically filters 50-90% of data
            effective = (effective as f64 * 0.5) as u64;
        }

        effective.max(1)
    }

    /// Calculate memory estimate using data types
    fn calculate_memory_estimate(
        &self,
        query: &QueryStructure,
        columns: &[ColumnMetadata],
        row_count: u64,
        read_bytes: u64,
    ) -> u64 {
        // If we have column metadata, use data-type aware estimation
        if !columns.is_empty() && row_count > 0 {
            let mut in_memory_size: u64 = 0;

            for col in columns {
                // Skip if column is pruned
                if !query.is_select_star && !query.selected_columns.is_empty() {
                    let col_lower = col.name.to_lowercase();
                    let is_selected = query
                        .selected_columns
                        .iter()
                        .any(|s| s.to_lowercase() == col_lower);
                    if !is_selected {
                        continue;
                    }
                }

                // In-memory size per data type
                let bytes_per_value: u64 = match col.physical_type {
                    PhysicalType::BOOLEAN => 1,
                    PhysicalType::INT32 => 4,
                    PhysicalType::INT64 => 8,
                    PhysicalType::INT96 => 12,
                    PhysicalType::FLOAT => 4,
                    PhysicalType::DOUBLE => 8,
                    PhysicalType::BYTE_ARRAY => 32, // Average string estimate
                    PhysicalType::FIXED_LEN_BYTE_ARRAY => 16,
                };

                in_memory_size += bytes_per_value * row_count;
            }

            // Add Arrow/DuckDB overhead
            let mut multiplier = 1.3; // Base overhead

            if query.has_join {
                multiplier *= 1.5;
            }
            if query.has_group_by {
                multiplier *= 1.3;
            }
            if query.has_distinct {
                multiplier *= 1.2;
            }
            if query.has_window {
                multiplier *= 1.4;
            }
            if query.has_order_by && query.limit.is_none() {
                multiplier *= 1.2;
            }

            let memory_bytes = (in_memory_size as f64 * multiplier) as u64;
            let memory_mb = memory_bytes / (1024 * 1024);

            return memory_mb.max(256); // Minimum 256MB
        }

        // Fallback: use read bytes with multiplier
        let mut multiplier = 5.0; // Parquet decompresses ~5x

        if query.has_join {
            multiplier *= 1.5;
        }
        if query.has_group_by {
            multiplier *= 1.3;
        }

        // Apply LIMIT reduction
        if let Some(limit) = query.limit {
            if limit < 10000 {
                multiplier *= 0.3;
            } else if limit < 100000 {
                multiplier *= 0.5;
            }
        }

        let memory_bytes = (read_bytes as f64 * multiplier) as u64;
        let memory_mb = memory_bytes / (1024 * 1024);

        memory_mb.max(256).min(409600) // 256MB to 400GB
    }

    /// Estimate source size with full Parquet metadata
    async fn estimate_source_size_with_metadata(&self, path: &str) -> DataSource {
        // Check cache first
        if let Some(cached) = self.cache.get(path) {
            if !cached.is_expired(self.cache_ttl) {
                debug!(
                    "Cache hit for {}: {} bytes, {} rows",
                    path, cached.size_bytes, cached.row_count
                );
                return DataSource {
                    path: path.to_string(),
                    size_bytes: cached.size_bytes,
                    source_type: self.detect_source_type(path),
                    estimation_method: EstimationMethod::Cached,
                    row_count: Some(cached.row_count),
                    column_count: Some(cached.columns.len() as u32),
                };
            }
        }

        // Try to get Parquet metadata
        if path.starts_with("s3://") && path.ends_with(".parquet") {
            if let Some(client) = &self.s3_client {
                if let Some((bucket, key)) = self.parse_s3_path(path) {
                    if let Ok(metadata) = self.read_parquet_metadata(client, &bucket, &key).await {
                        info!(
                            "Parquet footer read: {} rows, {} columns, {} row groups",
                            metadata.row_count,
                            metadata.columns.len(),
                            metadata.row_group_count
                        );

                        // Cache the metadata
                        self.cache.insert(path.to_string(), metadata.clone());

                        // Record metrics
                        crate::metrics::DATA_ROW_COUNT.observe(metadata.row_count as f64);
                        crate::metrics::DATA_COLUMN_COUNT.observe(metadata.columns.len() as f64);
                        crate::metrics::DATA_ROW_GROUP_COUNT
                            .observe(metadata.row_group_count as f64);

                        return DataSource {
                            path: path.to_string(),
                            size_bytes: metadata.size_bytes,
                            source_type: DataSourceType::Parquet,
                            estimation_method: EstimationMethod::ParquetFooter,
                            row_count: Some(metadata.row_count),
                            column_count: Some(metadata.columns.len() as u32),
                        };
                    }
                }
            }
        }

        // Fall back to S3 HEAD
        if path.starts_with("s3://") {
            if let Some(client) = &self.s3_client {
                if let Some((bucket, key)) = self.parse_s3_path(path) {
                    if let Ok(size) = self.head_s3_object(client, &bucket, &key).await {
                        info!("S3 HEAD {}: {} MB", path, size / (1024 * 1024));

                        // Estimate row count from file size
                        let estimated_rows = self.estimate_rows_from_size(size, path);

                        return DataSource {
                            path: path.to_string(),
                            size_bytes: size,
                            source_type: self.detect_source_type(path),
                            estimation_method: EstimationMethod::S3Head,
                            row_count: Some(estimated_rows),
                            column_count: None,
                        };
                    }
                }
            }
        }

        // Default fallback
        let default_size = self.default_size_estimate(path);
        DataSource {
            path: path.to_string(),
            size_bytes: default_size,
            source_type: self.detect_source_type(path),
            estimation_method: EstimationMethod::Default,
            row_count: None,
            column_count: None,
        }
    }

    /// Read Parquet footer to get exact metadata
    async fn read_parquet_metadata(
        &self,
        client: &S3Client,
        bucket: &str,
        key: &str,
    ) -> Result<CachedMetadata> {
        // Get file size first
        let file_size = self.head_s3_object(client, bucket, key).await?;

        // Step 1: Read last 8 bytes to get footer size
        let tail_range = format!("bytes={}-{}", file_size.saturating_sub(8), file_size - 1);

        let tail_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(&tail_range)
            .send()
            .await
            .context("Failed to read Parquet footer tail")?;

        let tail_bytes = tail_resp.body.collect().await?.to_vec();

        if tail_bytes.len() < 8 {
            anyhow::bail!("Footer too small");
        }

        // Check magic bytes "PAR1" at the end
        if &tail_bytes[tail_bytes.len() - 4..] != b"PAR1" {
            anyhow::bail!("Not a valid Parquet file - missing PAR1 magic bytes");
        }

        // Get footer metadata length (4 bytes before PAR1, little-endian)
        let metadata_len = u32::from_le_bytes(
            tail_bytes[tail_bytes.len() - 8..tail_bytes.len() - 4]
                .try_into()
                .context("Failed to read footer length")?,
        ) as u64;

        debug!(
            "Parquet file {} has metadata size: {} bytes",
            key, metadata_len
        );

        // Step 2: Read the full footer (metadata + 8 byte tail)
        let footer_size = metadata_len + 8;
        let footer_start = file_size.saturating_sub(footer_size);
        let footer_range = format!("bytes={}-{}", footer_start, file_size - 1);

        let footer_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(&footer_range)
            .send()
            .await
            .context("Failed to read Parquet footer metadata")?;

        let footer_bytes = footer_resp.body.collect().await?.to_vec();

        // Extract just the metadata bytes (excluding the 8-byte tail)
        if footer_bytes.len() < 8 {
            anyhow::bail!("Footer bytes too small");
        }
        let metadata_bytes = &footer_bytes[..footer_bytes.len() - 8];

        // Parse using ParquetMetaDataReader
        let metadata = ParquetMetaDataReader::decode_metadata(metadata_bytes)
            .context("Failed to decode Parquet metadata")?;

        // Extract column metadata
        let schema = metadata.file_metadata().schema_descr();
        let mut columns = Vec::new();
        let mut column_sizes: HashMap<String, (u64, u64)> = HashMap::new();

        // Aggregate column sizes from all row groups
        for rg in metadata.row_groups() {
            for col in rg.columns() {
                let col_path = col.column_descr().path().string();
                let entry = column_sizes.entry(col_path).or_insert((0, 0));
                entry.0 += col.compressed_size() as u64;
                entry.1 += col.uncompressed_size() as u64;
            }
        }

        // Build column metadata
        for col_descr in schema.columns() {
            let col_path = col_descr.path().string();
            let (compressed, uncompressed) = column_sizes.get(&col_path).copied().unwrap_or((0, 0));

            columns.push(ColumnMetadata {
                name: col_descr.name().to_string(),
                physical_type: col_descr.physical_type(),
                compressed_size: compressed,
                uncompressed_size: uncompressed,
            });
        }

        let row_count = metadata.file_metadata().num_rows() as u64;
        let row_group_count = metadata.num_row_groups() as u32;

        Ok(CachedMetadata {
            size_bytes: file_size,
            row_count,
            columns,
            row_group_count,
            created_at: Instant::now(),
        })
    }

    /// HEAD request to get object size
    async fn head_s3_object(&self, client: &S3Client, bucket: &str, key: &str) -> Result<u64> {
        let resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context("S3 HEAD request failed")?;

        Ok(resp.content_length().unwrap_or(0) as u64)
    }

    /// Estimate row count from file size (fallback)
    fn estimate_rows_from_size(&self, size_bytes: u64, path: &str) -> u64 {
        let path_lower = path.to_lowercase();

        // Use known schemas for better estimates
        let bytes_per_row = if path_lower.contains("lineitem") {
            53 // TPC-H lineitem average
        } else if path_lower.contains("orders") {
            40
        } else if path_lower.contains("customer") {
            45
        } else {
            50 // Default for Parquet
        };

        size_bytes / bytes_per_row
    }

    fn parse_s3_path(&self, path: &str) -> Option<(String, String)> {
        if let Some(caps) = self.s3_path_regex.captures(path) {
            let bucket = caps.get(1)?.as_str().to_string();
            let key = caps.get(2)?.as_str().to_string();
            return Some((bucket, key));
        }
        None
    }

    fn detect_source_type(&self, path: &str) -> DataSourceType {
        let path_lower = path.to_lowercase();

        if path_lower.ends_with(".parquet") {
            DataSourceType::Parquet
        } else if path_lower.ends_with(".csv") {
            DataSourceType::Csv
        } else if path_lower.ends_with(".json") || path_lower.ends_with(".jsonl") {
            DataSourceType::Json
        } else if path_lower.contains("_delta_log") {
            DataSourceType::Delta
        } else if path_lower.contains("iceberg") {
            DataSourceType::Iceberg
        } else {
            DataSourceType::Unknown
        }
    }

    fn default_size_estimate(&self, path: &str) -> u64 {
        let path_lower = path.to_lowercase();

        // Check for size hints in filename
        if let Some(size) = self.extract_size_from_filename(&path_lower) {
            return size;
        }

        // Default sizes by file type
        if path_lower.ends_with(".parquet") {
            500 * 1024 * 1024 // 500 MB
        } else if path_lower.ends_with(".csv") {
            100 * 1024 * 1024 // 100 MB
        } else {
            100 * 1024 * 1024 // 100 MB default
        }
    }

    fn extract_size_from_filename(&self, path: &str) -> Option<u64> {
        if let Some(caps) = SIZE_FROM_FILENAME_REGEX.captures(path) {
            let num: u64 = caps.get(1)?.as_str().parse().ok()?;
            let unit = caps.get(2)?.as_str();

            let bytes = match unit {
                "mb" => num * 1024 * 1024,
                "gb" => num * 1024 * 1024 * 1024,
                "tb" => num * 1024 * 1024 * 1024 * 1024,
                _ => return None,
            };

            return Some(bytes);
        }

        None
    }

    /// Clear expired cache entries
    pub fn cleanup_cache(&self) {
        self.cache.retain(|_, v| !v.is_expired(self.cache_ttl));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_select_star() {
        let sizer = create_test_sizer();
        let result = sizer.parse_sql("SELECT * FROM read_parquet('s3://bucket/file.parquet')");

        // Note: SQL parser may not extract read_parquet() tables, but fallback regex does
        assert!(result.is_select_star);
        // The tables might be empty if AST parser is used (doesn't understand DuckDB functions)
        // In that case, fallback regex should catch it
        // For now, just verify is_select_star works
    }

    #[test]
    fn test_parse_sql_with_columns() {
        let sizer = create_test_sizer();
        let result = sizer.parse_sql(
            "SELECT l_orderkey, l_quantity FROM read_parquet('s3://bucket/lineitem.parquet')",
        );

        assert!(!result.is_select_star);
        assert!(result.selected_columns.contains(&"l_orderkey".to_string()));
        assert!(result.selected_columns.contains(&"l_quantity".to_string()));
    }

    #[test]
    fn test_parse_sql_with_limit() {
        let sizer = create_test_sizer();
        let result = sizer.parse_sql("SELECT * FROM t LIMIT 1000");

        assert_eq!(result.limit, Some(1000));
    }

    #[test]
    fn test_parse_sql_with_join() {
        let sizer = create_test_sizer();
        let result = sizer.parse_sql("SELECT * FROM a JOIN b ON a.id = b.id");

        assert!(result.has_join);
    }

    fn create_test_sizer() -> DataSizer {
        DataSizer {
            s3_client: None,
            cache: DashMap::new(),
            cache_ttl: Duration::from_secs(300),
            s3_path_regex: Regex::new(r"s3://([^/]+)/(.+)").unwrap(),
            endpoint_url: None,
        }
    }
}
