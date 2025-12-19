//! Query Router - Routes all queries to the HPA+VPA managed worker pool
//!
//! Simplified architecture:
//! - All queries go to worker pool (no ephemeral pods)
//! - HPA scales pod count based on load
//! - VPA scales pod resources based on query size
//! - K8s v1.35 in-place resize allows seamless vertical scaling

use regex::Regex;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tracing::{debug, info};

use crate::data_sizer::{DataSizer, EstimationMethod};
use crate::metrics;

/// Query execution target - always worker pool now
#[derive(Debug, Clone, PartialEq)]
pub enum QueryTarget {
    /// Route to HPA+VPA managed worker pool
    WorkerPool,
}

/// Estimated query resources (for metrics/logging)
#[derive(Debug, Clone)]
pub struct QueryEstimate {
    /// Estimated data size in MB
    pub data_size_mb: u64,
    /// Estimated memory needed in MB
    pub memory_mb: u64,
    /// Estimated CPU cores needed
    pub cpu_cores: f32,
    /// Tables/files referenced
    pub tables: Vec<String>,
    /// Has aggregations
    pub has_aggregation: bool,
    /// Has joins
    pub has_join: bool,
    /// Routing decision (always WorkerPool)
    pub target: QueryTarget,
    /// Estimation method used
    pub estimation_method: String,
}

/// Query Router for worker pool execution
pub struct QueryRouter {
    /// Data sizer for size estimation (for metrics)
    data_sizer: Arc<DataSizer>,
}

impl QueryRouter {
    /// Create a new query router
    pub fn new(data_sizer: Arc<DataSizer>) -> Self {
        Self { data_sizer }
    }

    /// Analyze and route a query (always to worker pool)
    pub async fn route(&self, sql: &str) -> QueryEstimate {
        // Parse SQL to extract table references
        let tables = self.extract_tables(sql);
        let (has_aggregation, has_join) = self.analyze_query_complexity(sql);

        // Get data size estimate (for metrics/logging, not routing)
        let size_estimate = self.data_sizer.estimate_query_size(sql, &tables).await;

        // All queries go to worker pool
        let target = QueryTarget::WorkerPool;

        // Record metrics
        metrics::record_query_routed("worker_pool", size_estimate.total_mb as f64);

        // Record estimation method
        for source in &size_estimate.sources {
            metrics::record_estimation_method(match source.estimation_method {
                EstimationMethod::S3Head => "s3_head",
                EstimationMethod::ParquetMetadata => "parquet_metadata",
                EstimationMethod::DeltaLog => "delta_log",
                EstimationMethod::IcebergManifest => "iceberg_manifest",
                EstimationMethod::Cached => "cached",
                EstimationMethod::Default => "default",
            });
        }

        let estimation_method = size_estimate
            .sources
            .first()
            .map(|s| format!("{:?}", s.estimation_method))
            .unwrap_or_else(|| "unknown".to_string());

        let estimate = QueryEstimate {
            data_size_mb: size_estimate.total_mb,
            memory_mb: size_estimate.estimated_memory_mb,
            cpu_cores: self.estimate_cpu_cores(size_estimate.total_mb, has_join, has_aggregation),
            tables,
            has_aggregation,
            has_join,
            target,
            estimation_method,
        };

        info!(
            "Query routed to WorkerPool: data={}MB, memory={}MB, method={}, tables={:?}",
            estimate.data_size_mb,
            estimate.memory_mb,
            estimate.estimation_method,
            estimate.tables
        );

        estimate
    }

    /// Synchronous route for simpler cases
    pub fn route_sync(&self, sql: &str) -> QueryEstimate {
        let tables = self.extract_tables(sql);
        let (has_aggregation, has_join) = self.analyze_query_complexity(sql);

        // Simple size estimation
        let data_size_mb = self.estimate_size_sync(&tables, sql);
        let memory_mb = self.estimate_memory(data_size_mb, has_join, has_aggregation);
        let cpu_cores = self.estimate_cpu_cores(data_size_mb, has_join, has_aggregation);

        metrics::record_query_routed("worker_pool", data_size_mb as f64);

        QueryEstimate {
            data_size_mb,
            memory_mb,
            cpu_cores,
            tables,
            has_aggregation,
            has_join,
            target: QueryTarget::WorkerPool,
            estimation_method: "sync".to_string(),
        }
    }

    /// Extract table references from SQL
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let mut tables = Vec::new();

        // Parse SQL
        let dialect = GenericDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
            for stmt in &statements {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        for table in &select.from {
                            self.extract_table_factor(&table.relation, &mut tables);
                            for join in &table.joins {
                                self.extract_table_factor(&join.relation, &mut tables);
                            }
                        }
                    }
                }
            }
        }

        // Also extract from raw SQL for DuckDB functions
        self.extract_tables_from_sql(sql, &mut tables);

        tables
    }

    /// Extract table from TableFactor
    fn extract_table_factor(&self, factor: &TableFactor, tables: &mut Vec<String>) {
        match factor {
            TableFactor::Table { name, .. } => {
                tables.push(name.to_string());
            }
            TableFactor::TableFunction { expr, .. } => {
                tables.push(format!("{}", expr));
            }
            _ => {}
        }
    }

    /// Extract tables from raw SQL (for DuckDB functions)
    fn extract_tables_from_sql(&self, sql: &str, tables: &mut Vec<String>) {
        let patterns = [
            r"read_csv_auto\s*\(\s*'([^']+)'",
            r"read_csv\s*\(\s*'([^']+)'",
            r"read_parquet\s*\(\s*'([^']+)'",
            r"read_json\s*\(\s*'([^']+)'",
            r"read_json_auto\s*\(\s*'([^']+)'",
            r"delta_scan\s*\(\s*'([^']+)'",
            r"iceberg_scan\s*\(\s*'([^']+)'",
        ];

        for pattern in patterns {
            if let Ok(re) = Regex::new(pattern) {
                for cap in re.captures_iter(sql) {
                    if let Some(m) = cap.get(1) {
                        let path = m.as_str().to_string();
                        if !tables.contains(&path) {
                            tables.push(path);
                        }
                    }
                }
            }
        }
    }

    /// Analyze query complexity
    fn analyze_query_complexity(&self, sql: &str) -> (bool, bool) {
        let sql_upper = sql.to_uppercase();

        let has_join = sql_upper.contains(" JOIN ");

        let has_aggregation = sql_upper.contains("GROUP BY")
            || sql_upper.contains("SUM(")
            || sql_upper.contains("COUNT(")
            || sql_upper.contains("AVG(")
            || sql_upper.contains("MIN(")
            || sql_upper.contains("MAX(");

        (has_aggregation, has_join)
    }

    /// Estimate CPU cores needed
    fn estimate_cpu_cores(&self, data_size_mb: u64, has_join: bool, has_aggregation: bool) -> f32 {
        let mut cores: f32 = 1.0;

        if data_size_mb > 1000 {
            cores += 1.0;
        }
        if has_join {
            cores += 1.0;
        }
        if has_aggregation {
            cores += 0.5;
        }

        cores.min(8.0)
    }

    /// Simple size estimation for sync path
    fn estimate_size_sync(&self, tables: &[String], sql: &str) -> u64 {
        let mut total_mb: u64 = 0;

        for table in tables {
            if table.starts_with("s3://") || table.contains(".parquet") || table.contains(".csv") {
                total_mb += 100; // Default estimate
            } else {
                total_mb += 50;
            }
        }

        // Check for LIMIT
        if let Some(limit) = self.extract_limit(sql) {
            if limit < 10000 {
                total_mb = (total_mb / 10).max(10);
            }
        }

        total_mb.max(10)
    }

    /// Estimate memory based on data size and complexity
    fn estimate_memory(&self, data_size_mb: u64, has_join: bool, has_aggregation: bool) -> u64 {
        let mut multiplier = 2.0;

        if has_join {
            multiplier *= 1.5;
        }
        if has_aggregation {
            multiplier *= 1.2;
        }

        let memory = (data_size_mb as f64 * multiplier) as u64;
        memory.max(256)
    }

    /// Extract LIMIT value
    fn extract_limit(&self, sql: &str) -> Option<u64> {
        let sql_upper = sql.to_uppercase();
        if let Some(pos) = sql_upper.find("LIMIT") {
            let after = &sql[pos + 5..];
            let num_str: String = after
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            return num_str.parse().ok();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_complexity_analysis() {
        // Create a minimal router for testing would require mocking DataSizer
    }
}
