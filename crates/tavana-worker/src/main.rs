//! Tavana Worker Service
//!
//! Executes DuckDB queries and streams results back to the gateway.
//!
//! Supports two modes:
//! 1. gRPC service mode (default): Long-running service for query execution
//! 2. One-shot mode: Execute a single query from QUERY_SQL env var and exit

mod cursor_manager;
mod executor;
mod grpc;

pub use cursor_manager::{CursorManager, CursorManagerConfig};

use crate::executor::{DuckDbExecutor, ExecutorConfig};
use crate::grpc::QueryServiceImpl;
use clap::Parser;
use std::net::SocketAddr;
use tavana_common::proto::query_service_server::QueryServiceServer;
use tonic::transport::Server;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "tavana-worker")]
#[command(about = "Tavana DuckDB Worker Service - HPA pool for small/medium queries")]
struct Args {
    /// gRPC server port
    #[arg(long, env = "GRPC_PORT", default_value = "50053")]
    grpc_port: u16,

    /// Maximum memory in GB for DuckDB
    /// Set to 0 for auto-detect from container limits (cgroups)
    /// DuckDB recommends 1-4 GB per thread:
    /// - Aggregation-heavy: 1-2 GB/thread
    /// - Join-heavy: 3-4 GB/thread
    /// Default: 0 (auto-detect, use 80% of container limit)
    #[arg(long, env = "MAX_MEMORY_GB", default_value = "0")]
    max_memory_gb: u64,

    /// Number of threads for DuckDB (defaults to all CPUs)
    #[arg(long, env = "THREADS")]
    threads: Option<u32>,

    /// Enable TLS
    #[arg(long, env = "TLS_ENABLED", default_value = "false")]
    tls_enabled: bool,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// One-shot mode: execute query and exit (for ephemeral pods)
    #[arg(long, env = "ONE_SHOT", default_value = "false")]
    one_shot: bool,

    /// Connection pool size (parallel queries per worker)
    /// For HPA workers handling multiple small queries
    #[arg(long, env = "POOL_SIZE", default_value = "4")]
    pool_size: usize,

    /// Temp directory for out-of-core processing (spill to disk)
    #[arg(long, env = "DUCKDB_TEMP_DIR", default_value = "/tmp/duckdb")]
    temp_dir: String,

    /// Max temp directory size for spilling (e.g., "100GB", "500GB")
    #[arg(long, env = "DUCKDB_MAX_TEMP_SIZE", default_value = "100GB")]
    max_temp_size: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env if present
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    // Check for one-shot mode via environment variable
    let query_sql = std::env::var("QUERY_SQL").ok();
    let one_shot = args.one_shot || query_sql.is_some();

    if one_shot {
        // One-shot mode: execute query and exit
        return run_one_shot(args, query_sql).await;
    }

    // Normal gRPC server mode
    info!(
        "Starting Tavana Worker on port {} (TLS: {})",
        args.grpc_port, args.tls_enabled
    );

    // Determine memory limit - auto-detect from container if not specified
    let max_memory_bytes = if args.max_memory_gb == 0 {
        let detected = detect_container_memory_limit();
        info!(
            "Auto-detected container memory limit: {}GB",
            detected / 1024 / 1024 / 1024
        );
        detected
    } else {
        args.max_memory_gb * 1024 * 1024 * 1024
    };

    // Create executor config with connection pool
    let executor_config = ExecutorConfig {
        max_memory: max_memory_bytes,
        threads: args.threads.unwrap_or_else(|| num_cpus::get() as u32),
        enable_profiling: false,
        pool_size: args.pool_size,
    };

    info!(
        "Connection pool size: {} (parallel queries per worker)",
        args.pool_size
    );

    // Initialize gRPC service (this performs infrastructure warmup: extensions, credentials)
    let query_service = QueryServiceImpl::new(executor_config)?;

    // Set up gRPC Health service (standard grpc.health.v1.Health)
    // K8s readiness probe uses this to gate traffic until worker is ready
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    // Mark the query service as SERVING after warmup is complete
    health_reporter
        .set_serving::<QueryServiceServer<QueryServiceImpl>>()
        .await;
    info!("gRPC health service: SERVING (extensions loaded, credentials acquired)");

    let addr: SocketAddr = format!("0.0.0.0:{}", args.grpc_port).parse()?;

    info!("Tavana Worker listening on {}", addr);

    // Start gRPC server with health service + query service
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

    Server::builder()
        // HTTP2 settings for large data streaming
        .initial_connection_window_size(1024 * 1024 * 1024) // 1GB connection window
        .initial_stream_window_size(512 * 1024 * 1024) // 512MB per stream
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(60)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .add_service(health_service)
        .add_service(
            QueryServiceServer::new(query_service)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down Tavana Worker");
        })
        .await?;

    Ok(())
}

/// Run in one-shot mode: execute a single query and exit
async fn run_one_shot(args: Args, query_sql: Option<String>) -> anyhow::Result<()> {
    let query_id = std::env::var("QUERY_ID").unwrap_or_else(|_| "one-shot".to_string());

    let sql = query_sql.ok_or_else(|| anyhow::anyhow!("QUERY_SQL environment variable not set"))?;

    info!("One-shot mode: Executing query {}", query_id);
    info!("SQL: {}", sql);

    // Determine memory limit - auto-detect from container if not specified
    let max_memory_bytes = if args.max_memory_gb == 0 {
        let detected = detect_container_memory_limit();
        info!(
            "Auto-detected container memory limit: {}GB",
            detected / 1024 / 1024 / 1024
        );
        detected
    } else {
        args.max_memory_gb * 1024 * 1024 * 1024
    };

    // Create executor (single connection for one-shot mode)
    let executor_config = ExecutorConfig {
        max_memory: max_memory_bytes,
        threads: args.threads.unwrap_or_else(|| num_cpus::get() as u32),
        enable_profiling: false,
        pool_size: 1, // One-shot mode only needs 1 connection
    };

    let executor = DuckDbExecutor::new(executor_config)?;

    // Execute query
    let start = std::time::Instant::now();
    match executor.execute_query(&sql) {
        Ok(batches) => {
            let elapsed = start.elapsed();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            info!("Query completed successfully");
            info!("  Rows: {}", total_rows);
            info!("  Batches: {}", batches.len());
            info!("  Time: {:?}", elapsed);

            // Convert Arrow batches to JSON format expected by gateway
            // Gateway expects: {"columns": [...], "column_types": [...], "rows": [[...], ...]}
            if let Some(first_batch) = batches.first() {
                let schema = first_batch.schema();
                let columns: Vec<String> =
                    schema.fields().iter().map(|f| f.name().clone()).collect();
                let column_types: Vec<String> = schema
                    .fields()
                    .iter()
                    .map(|f| format!("{:?}", f.data_type()))
                    .collect();

                // Convert rows to strings (limited for one-shot mode)
                let mut rows: Vec<Vec<String>> = Vec::new();
                for batch in &batches {
                    for row_idx in 0..batch.num_rows().min(1000) {
                        // Limit to 1000 rows
                        let mut row: Vec<String> = Vec::new();
                        for col_idx in 0..batch.num_columns() {
                            let col = batch.column(col_idx);
                            // Use duckdb's arrow display utility
                            let val =
                                duckdb::arrow::util::display::array_value_to_string(col, row_idx)
                                    .unwrap_or_else(|_| "NULL".to_string());
                            row.push(val);
                        }
                        rows.push(row);
                    }
                }

                // Use serde_json for proper JSON serialization
                let result = serde_json::json!({
                    "columns": columns,
                    "column_types": column_types,
                    "rows": rows
                });
                println!("{}", result);
            } else {
                println!(r#"{{"columns":[],"column_types":[],"rows":[]}}"#);
            }

            Ok(())
        }
        Err(e) => {
            error!("Query failed: {}", e);
            println!("{{\"status\": \"error\", \"message\": \"{}\"}}", e);
            Err(e)
        }
    }
}

/// Detect container memory limit from cgroups (v1 and v2)
/// Returns 80% of the detected limit to leave headroom for the OS and other processes
/// Falls back to 8GB if detection fails
fn detect_container_memory_limit() -> u64 {
    // Try cgroups v2 first (unified hierarchy)
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Ok(limit) = contents.trim().parse::<u64>() {
            // Use 80% of container limit for DuckDB
            let usable = (limit as f64 * 0.8) as u64;
            info!(
                "Detected cgroups v2 memory limit: {}GB, using {}GB (80%)",
                limit / 1024 / 1024 / 1024,
                usable / 1024 / 1024 / 1024
            );
            return usable;
        }
    }

    // Try cgroups v1
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(limit) = contents.trim().parse::<u64>() {
            // Check if it's the "unlimited" value (very large number)
            if limit < 9_000_000_000_000_000_000 {
                let usable = (limit as f64 * 0.8) as u64;
                info!(
                    "Detected cgroups v1 memory limit: {}GB, using {}GB (80%)",
                    limit / 1024 / 1024 / 1024,
                    usable / 1024 / 1024 / 1024
                );
                return usable;
            }
        }
    }

    // Try reading from /proc/meminfo as fallback (total system memory)
    if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        // Use 50% of total system memory as fallback
                        let limit = kb * 1024;
                        let usable = (limit as f64 * 0.5) as u64;
                        info!(
                            "Using 50% of system memory: {}GB",
                            usable / 1024 / 1024 / 1024
                        );
                        return usable;
                    }
                }
            }
        }
    }

    // Default fallback: 8GB
    info!("Could not detect memory limit, using default 8GB");
    8 * 1024 * 1024 * 1024
}
