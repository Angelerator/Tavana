//! Tavana Gateway Service
//!
//! Main entry point for client connections. Supports:
//! - PostgreSQL wire protocol (for Tableau, PowerBI, DBeaver)
//! - Arrow Flight SQL (for Python, Polars, DuckDB)
//! - REST API (for management)
//! - Prometheus metrics (/metrics)
//!
//! Simplified architecture:
//! - All queries go to HPA+VPA managed worker pool
//! - No ephemeral pods or adaptive thresholds
//! - K8s v1.35 handles scaling automatically

mod auth;
mod data_sizer;
mod flight;
mod http_api;
mod metrics;
mod pg_wire;
mod query;
mod query_router;
mod telemetry;
mod worker_client;

use crate::auth::AuthService;
use crate::data_sizer::DataSizer;
use crate::flight::FlightSqlServer;
use crate::http_api::{execute_query, health, prometheus_metrics, ready, root, AppState};
use crate::pg_wire::PgWireServer;
use crate::query_router::QueryRouter;
use crate::worker_client::WorkerClient;
use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "tavana-gateway")]
#[command(about = "Tavana Gateway Service - Query Entry Point")]
struct Args {
    /// PostgreSQL wire protocol port
    #[arg(long, env = "PG_PORT", default_value = "15432")]
    pg_port: u16,

    /// Arrow Flight SQL port
    #[arg(long, env = "FLIGHT_PORT", default_value = "8815")]
    flight_port: u16,

    /// REST API port
    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,

    /// Catalog service address
    #[arg(long, env = "CATALOG_ADDR", default_value = "http://localhost:50052")]
    catalog_addr: String,

    /// Worker service address
    #[arg(long, env = "WORKER_ADDR", default_value = "http://localhost:50053")]
    worker_addr: String,

    /// Enable TLS
    #[arg(long, env = "TLS_ENABLED", default_value = "false")]
    tls_enabled: bool,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize rustls crypto provider (required by kube client)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load environment variables from .env if present
    dotenvy::dotenv().ok();
    
    let args = Args::parse();

    // Initialize telemetry
    telemetry::init(&args.log_level)?;

    info!("Starting Tavana Gateway");
    info!("  PostgreSQL port: {}", args.pg_port);
    info!("  Arrow Flight port: {}", args.flight_port);
    info!("  HTTP port: {}", args.http_port);
    info!("  TLS enabled: {}", args.tls_enabled);
    info!("  Worker address: {}", args.worker_addr);

    // Initialize Prometheus metrics
    metrics::init_metrics();

    // Initialize shared auth service
    let auth_service = Arc::new(AuthService::new());

    // Initialize DataSizer for data estimation (for metrics)
    let data_sizer = Arc::new(DataSizer::new().await);
    info!("DataSizer initialized");

    // Initialize QueryRouter (simplified - all queries to worker pool)
    let query_router = Arc::new(QueryRouter::new(data_sizer.clone()));
    info!("QueryRouter initialized - all queries go to worker pool");

    // Create worker client
    let worker_client = Arc::new(WorkerClient::new(args.worker_addr.clone()));

    // Start PostgreSQL wire protocol server
    let pg_auth = auth_service.clone();
    let pg_port = args.pg_port;
    let pg_worker = worker_client.clone();
    let pg_router = query_router.clone();
    let pg_handle = tokio::spawn(async move {
        let server = PgWireServer::new(pg_port, pg_auth, pg_worker, pg_router);
        if let Err(e) = server.start().await {
            tracing::error!("PostgreSQL server error: {}", e);
        }
    });

    // Start Arrow Flight SQL server
    let flight_auth = auth_service.clone();
    let flight_port = args.flight_port;
    let flight_handle = tokio::spawn(async move {
        let server = FlightSqlServer::new(flight_port, flight_auth);
        if let Err(e) = server.start().await {
            tracing::error!("Flight SQL server error: {}", e);
        }
    });

    // HTTP API state
    let app_state = AppState {
        worker_client,
        query_router,
    };

    // CORS layer for frontend access
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Start HTTP server for management and query API
    let http_addr: SocketAddr = format!("0.0.0.0:{}", args.http_port).parse()?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/", get(root))
        .route("/api/query", post(execute_query))
        .route("/api/export", post(http_api::export_query))
        .route("/metrics", get(prometheus_metrics))
        .layer(cors)
        .with_state(app_state);

    info!("HTTP server listening on {}", http_addr);
    info!("  /api/query - Execute queries via worker pool");
    info!("  /metrics - Prometheus metrics");

    let http_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    info!("Tavana Gateway started successfully");
    info!("Architecture: HPA+VPA managed worker pool (K8s v1.35)");

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down Tavana Gateway");
        }
        _ = pg_handle => {}
        _ = flight_handle => {}
        _ = http_handle => {}
    }

    Ok(())
}
