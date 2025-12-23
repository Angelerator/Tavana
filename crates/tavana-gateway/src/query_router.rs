//! Query Router - Routes queries to pre-sized workers or tenant pools
//!
//! Query-aware pre-sizing architecture:
//! 1. Estimate query data size
//! 2. Calculate required memory (data_size × 0.5)
//! 3. Find idle worker and pre-size if needed (K8s v1.35 in-place resize)
//! 4. Route query to pre-sized worker
//!
//! Tenant isolation architecture:
//! - Each tenant gets a dedicated worker pool (HPA + VPA per tenant)
//! - Large queries (>1GB) always go to tenant's dedicated pool
//! - Cache is preserved per tenant for efficiency
//! - Anonymous/public queries go to shared pool

use regex::Regex;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::data_sizer::{DataSizer, EstimationMethod};
use crate::metrics;
use crate::worker_pool::WorkerPoolManager;

/// Query execution target
#[derive(Debug, Clone, PartialEq)]
pub enum QueryTarget {
    /// Route to HPA+VPA managed shared worker pool (default service)
    WorkerPool,
    /// Route to a specific pre-sized worker (pod IP)
    PreSizedWorker {
        address: String,
        worker_name: String,
    },
    /// Route to tenant's dedicated worker pool
    TenantPool {
        tenant_id: String,
        service_addr: String,
    },
}

/// Estimated query resources
#[derive(Debug, Clone)]
pub struct QueryEstimate {
    /// Estimated data size in MB
    pub data_size_mb: u64,
    /// Required memory in MB (after applying multiplier)
    pub required_memory_mb: u64,
    /// Estimated CPU cores needed
    pub cpu_cores: f32,
    /// Tables/files referenced
    pub tables: Vec<String>,
    /// Has aggregations
    pub has_aggregation: bool,
    /// Has joins
    pub has_join: bool,
    /// Routing decision
    pub target: QueryTarget,
    /// Estimation method used
    pub estimation_method: String,
    /// Whether worker was resized
    pub was_resized: bool,
    /// Tenant ID if routed to tenant pool
    pub tenant_id: Option<String>,
}

/// Query Router with pre-sizing support
pub struct QueryRouter {
    /// Data sizer for size estimation
    data_sizer: Arc<DataSizer>,
    /// Worker pool manager for pre-sizing (shared pool)
    pool_manager: Option<Arc<WorkerPoolManager>>,
    // tenant_manager: Option<Arc<TenantPoolManager>>,  // Removed - not implemented
}

impl QueryRouter {
    /// Create a new query router without pre-sizing
    pub fn new(data_sizer: Arc<DataSizer>) -> Self {
        Self {
            data_sizer,
            pool_manager: None,
        }
    }

    /// Create a new query router with pre-sizing support (shared pool only)
    pub fn with_pool_manager(
        data_sizer: Arc<DataSizer>,
        pool_manager: Arc<WorkerPoolManager>,
    ) -> Self {
        Self {
            data_sizer,
            pool_manager: Some(pool_manager),
        }
    }

    /// Analyze, pre-size, and route a query
    ///
    /// Routing priority:
    /// 1. If tenant_id provided and tenant has dedicated pool → TenantPool
    /// 2. If tenant_id provided and query is large → Create tenant pool
    /// 3. Otherwise → Shared pre-sized worker pool
    pub async fn route(&self, sql: &str) -> QueryEstimate {
        self.route_with_tenant(sql, None).await
    }

    /// Route a query with tenant context
    pub async fn route_with_tenant(&self, sql: &str, tenant_id: Option<&str>) -> QueryEstimate {
        // Parse SQL to extract table references
        let tables = self.extract_tables(sql);
        let (has_aggregation, has_join) = self.analyze_query_complexity(sql);

        // Get data size estimate
        let size_estimate = self.data_sizer.estimate_query_size(sql, &tables).await;

        let estimation_method = size_estimate
            .sources
            .first()
            .map(|s| format!("{:?}", s.estimation_method))
            .unwrap_or_else(|| "unknown".to_string());

        // Record estimation method metrics
        for source in &size_estimate.sources {
            metrics::record_estimation_method(match source.estimation_method {
                EstimationMethod::S3Head => "s3_head",
                EstimationMethod::ParquetFooter | EstimationMethod::ParquetMetadata => {
                    "parquet_metadata"
                }
                EstimationMethod::DeltaLog => "delta_log",
                EstimationMethod::IcebergManifest => "iceberg_manifest",
                EstimationMethod::Cached => "cached",
                EstimationMethod::Default => "default",
            });
        }

        // Calculate CPU cores estimate for metrics
        let cpu_cores = self.estimate_cpu_cores(size_estimate.total_mb, has_join, has_aggregation);

        // Tenant routing removed - not implemented
        // if let Some(ref tm) = self.tenant_manager { ... }

        // Pre-sizing on shared pool
        let (target, required_memory_mb, was_resized) = if let Some(ref pm) = self.pool_manager {
            if pm.is_enabled() {
                // Calculate required memory using pool manager's formula
                let required_memory = pm.calculate_memory_mb(size_estimate.total_mb);

                match pm.get_presized_worker(required_memory, cpu_cores).await {
                    Ok(worker) => {
                        info!(
                            "Pre-sized worker {} to {}MB for query (data={}MB, resized={})",
                            worker.name, worker.memory_mb, size_estimate.total_mb, worker.resized
                        );
                        metrics::record_query_routed(
                            "presized_worker",
                            size_estimate.total_mb as f64,
                        );
                        (
                            QueryTarget::PreSizedWorker {
                                address: worker.address,
                                worker_name: worker.name,
                            },
                            worker.memory_mb,
                            worker.resized,
                        )
                    }
                    Err(e) => {
                        warn!(
                            "Failed to get pre-sized worker: {}, falling back to pool",
                            e
                        );
                        metrics::record_query_routed("worker_pool", size_estimate.total_mb as f64);
                        (
                            QueryTarget::WorkerPool,
                            size_estimate.estimated_memory_mb,
                            false,
                        )
                    }
                }
            } else {
                // Pre-sizing disabled
                metrics::record_query_routed("worker_pool", size_estimate.total_mb as f64);
                (
                    QueryTarget::WorkerPool,
                    size_estimate.estimated_memory_mb,
                    false,
                )
            }
        } else {
            // No pool manager
            metrics::record_query_routed("worker_pool", size_estimate.total_mb as f64);
            (
                QueryTarget::WorkerPool,
                size_estimate.estimated_memory_mb,
                false,
            )
        };

        let estimate = QueryEstimate {
            data_size_mb: size_estimate.total_mb,
            required_memory_mb,
            cpu_cores: self.estimate_cpu_cores(size_estimate.total_mb, has_join, has_aggregation),
            tables,
            has_aggregation,
            has_join,
            target,
            estimation_method,
            was_resized,
            tenant_id: tenant_id.map(|s| s.to_string()),
        };

        info!(
            "Query routed: data={}MB, memory={}MB, method={}, target={:?}, resized={}",
            estimate.data_size_mb,
            estimate.required_memory_mb,
            estimate.estimation_method,
            match &estimate.target {
                QueryTarget::WorkerPool => "WorkerPool".to_string(),
                QueryTarget::PreSizedWorker { worker_name, .. } =>
                    format!("PreSized({})", worker_name),
                QueryTarget::TenantPool { tenant_id, .. } => format!("TenantPool({})", tenant_id),
            },
            estimate.was_resized
        );

        estimate
    }

    /// Release a worker after query completion
    pub async fn release_worker(&self, worker_name: &str, actual_memory_used_mb: Option<u64>) {
        if let Some(ref pm) = self.pool_manager {
            pm.release_worker(worker_name, actual_memory_used_mb).await;
        }
    }

    /// Synchronous route for simpler cases (no pre-sizing or tenant isolation)
    pub fn route_sync(&self, sql: &str) -> QueryEstimate {
        let tables = self.extract_tables(sql);
        let (has_aggregation, has_join) = self.analyze_query_complexity(sql);

        let data_size_mb = self.estimate_size_sync(&tables, sql);
        let memory_mb = self.estimate_memory(data_size_mb, has_join, has_aggregation);
        let cpu_cores = self.estimate_cpu_cores(data_size_mb, has_join, has_aggregation);

        metrics::record_query_routed("worker_pool", data_size_mb as f64);

        QueryEstimate {
            data_size_mb,
            required_memory_mb: memory_mb,
            cpu_cores,
            tables,
            has_aggregation,
            has_join,
            target: QueryTarget::WorkerPool,
            estimation_method: "sync".to_string(),
            was_resized: false,
            tenant_id: None,
        }
    }

    /// Extract table references from SQL
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let mut tables = Vec::new();

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

        self.extract_tables_from_sql(sql, &mut tables);
        tables
    }

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

    fn estimate_size_sync(&self, tables: &[String], sql: &str) -> u64 {
        let mut total_mb: u64 = 0;
        for table in tables {
            if table.starts_with("s3://") || table.contains(".parquet") || table.contains(".csv") {
                total_mb += 100;
            } else {
                total_mb += 50;
            }
        }
        if let Some(limit) = self.extract_limit(sql) {
            if limit < 10000 {
                total_mb = (total_mb / 10).max(10);
            }
        }
        total_mb.max(10)
    }

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
