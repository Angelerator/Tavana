//! Tavana Catalog Service
//!
//! Manages table metadata with Unity Catalog compatible REST API
//! and internal gRPC API for low-latency lookups.

mod db;
mod grpc_api;
mod unity_api;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "tavana-catalog")]
#[command(about = "Tavana Catalog Service - Unity Catalog Compatible")]
struct Args {
    /// gRPC port for internal API
    #[arg(long, env = "GRPC_PORT", default_value = "50052")]
    grpc_port: u16,

    /// HTTP port for Unity Catalog REST API
    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,

    /// PostgreSQL connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Enable TLS
    #[arg(long, env = "TLS_ENABLED", default_value = "true")]
    tls_enabled: bool,

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

    info!("Starting Tavana Catalog Service");
    info!("  gRPC port: {}", args.grpc_port);
    info!("  HTTP port: {}", args.http_port);
    info!("  TLS enabled: {}", args.tls_enabled);

    // TODO: Initialize database connection pool
    // TODO: Run migrations
    // TODO: Start gRPC server
    // TODO: Start HTTP server for Unity Catalog API

    info!("Tavana Catalog started successfully");

    // Keep running until shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down Tavana Catalog");

    Ok(())
}

