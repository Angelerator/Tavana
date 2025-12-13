//! Tavana Worker Service
//!
//! Executes DuckDB queries and streams results back to the gateway.
//! 
//! Supports two modes:
//! 1. gRPC service mode (default): Long-running service for query execution
//! 2. One-shot mode: Execute a single query from QUERY_SQL env var and exit

mod executor;
mod grpc;

use crate::executor::{DuckDbExecutor, ExecutorConfig};
use crate::grpc::QueryServiceImpl;
use clap::Parser;
use std::net::SocketAddr;
use tavana_common::proto::query_service_server::QueryServiceServer;
use tonic::transport::Server;
use tracing::{info, error};

#[derive(Parser, Debug)]
#[command(name = "tavana-worker")]
#[command(about = "Tavana DuckDB Worker Service - HPA pool for small/medium queries")]
struct Args {
    /// gRPC server port
    #[arg(long, env = "GRPC_PORT", default_value = "50053")]
    grpc_port: u16,

    /// Maximum memory in GB for DuckDB
    /// DuckDB recommends 1-4 GB per thread:
    /// - Aggregation-heavy: 1-2 GB/thread
    /// - Join-heavy: 3-4 GB/thread
    /// Default: 8GB (for 4 threads with 2GB each)
    #[arg(long, env = "MAX_MEMORY_GB", default_value = "8")]
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

    // Create executor config with connection pool
    let executor_config = ExecutorConfig {
        max_memory: args.max_memory_gb * 1024 * 1024 * 1024,
        threads: args.threads.unwrap_or_else(|| num_cpus::get() as u32),
        enable_profiling: false,
        pool_size: args.pool_size,
    };
    
    info!("Connection pool size: {} (parallel queries per worker)", args.pool_size);

    // Initialize gRPC service
    let query_service = QueryServiceImpl::new(executor_config)?;
    
    let addr: SocketAddr = format!("0.0.0.0:{}", args.grpc_port).parse()?;
    
    info!("Tavana Worker listening on {}", addr);

    // Start gRPC server with increased message size limits for large results
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    
    Server::builder()
        .add_service(
            QueryServiceServer::new(query_service)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE)
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
    
    // Create executor (single connection for one-shot mode)
    let executor_config = ExecutorConfig {
        max_memory: args.max_memory_gb * 1024 * 1024 * 1024,
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
                let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
                let column_types: Vec<String> = schema.fields().iter()
                    .map(|f| format!("{:?}", f.data_type()))
                    .collect();
                
                // Convert rows to strings (limited for one-shot mode)
                let mut rows: Vec<Vec<String>> = Vec::new();
                for batch in &batches {
                    for row_idx in 0..batch.num_rows().min(1000) { // Limit to 1000 rows
                        let mut row: Vec<String> = Vec::new();
                        for col_idx in 0..batch.num_columns() {
                            let col = batch.column(col_idx);
                            // Use duckdb's arrow display utility
                            let val = duckdb::arrow::util::display::array_value_to_string(col, row_idx)
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
