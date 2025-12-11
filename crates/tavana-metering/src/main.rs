//! Tavana Metering Service
//!
//! Collects usage metrics from worker pods and exports to:
//! - PostgreSQL (hot storage for recent data)
//! - Object storage as Parquet (cold storage for historical data)
//! - Control plane (for billing)

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "tavana-metering")]
#[command(about = "Tavana Metering Agent - Usage Tracking")]
struct Args {
    /// gRPC port for receiving metrics
    #[arg(long, env = "GRPC_PORT", default_value = "50054")]
    grpc_port: u16,

    /// HTTP port for Prometheus metrics
    #[arg(long, env = "METRICS_PORT", default_value = "9090")]
    metrics_port: u16,

    /// PostgreSQL connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Control plane URL for pushing aggregated metrics
    #[arg(long, env = "CONTROL_PLANE_URL")]
    control_plane_url: Option<String>,

    /// Push interval in seconds
    #[arg(long, env = "PUSH_INTERVAL_SECS", default_value = "60")]
    push_interval_secs: u64,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
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

    info!("Starting Tavana Metering Agent");
    info!("  gRPC port: {}", args.grpc_port);
    info!("  Metrics port: {}", args.metrics_port);
    info!("  Push interval: {}s", args.push_interval_secs);

    // TODO: Initialize database connection
    // TODO: Start gRPC server for receiving usage events
    // TODO: Start Prometheus metrics server
    // TODO: Start background task for pushing to control plane
    // TODO: Start background task for archiving to Parquet

    info!("Tavana Metering Agent started successfully");

    // Keep running until shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down Tavana Metering Agent");

    Ok(())
}

