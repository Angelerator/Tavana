//! Tavana Gateway Service
//!
//! Main entry point for client connections. Supports:
//! - PostgreSQL wire protocol (for Tableau, PowerBI)
//! - Arrow Flight SQL (for Python, Polars, DuckDB)
//! - REST API (for management)
//! - Prometheus metrics (/metrics)
//! - Adaptive scaling state (/api/adaptive)

mod adaptive;
mod auth;
mod data_sizer;
mod flight;
mod http_api;
mod k8s_client;
mod metrics;
mod pg_wire;
mod query;
mod query_router;
mod telemetry;
mod worker_client;

use crate::adaptive::{AdaptiveConfig, AdaptiveController};
use crate::auth::AuthService;
use crate::data_sizer::DataSizer;
use crate::flight::FlightSqlServer;
use crate::http_api::{adaptive_state, execute_query, health, prometheus_metrics, ready, root, AppState};
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

    /// Operator service address
    #[arg(long, env = "OPERATOR_ADDR", default_value = "http://localhost:50051")]
    operator_addr: String,

    /// Enable TLS
    #[arg(long, env = "TLS_ENABLED", default_value = "false")]
    tls_enabled: bool,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Kubernetes namespace
    #[arg(long, env = "NAMESPACE", default_value = "tavana")]
    namespace: String,

    /// Base threshold in GB for query routing
    #[arg(long, env = "BASE_THRESHOLD_GB", default_value = "2")]
    base_threshold_gb: f64,
    
    /// Minimum threshold in GB (after adaptive factors)
    #[arg(long, env = "MIN_THRESHOLD_GB", default_value = "0.5")]
    min_threshold_gb: f64,
    
    /// Maximum threshold in GB (after adaptive factors)
    #[arg(long, env = "MAX_THRESHOLD_GB", default_value = "20")]
    max_threshold_gb: f64,
    
    /// Timezone for time-based patterns (e.g., "UTC", "America/New_York", "Europe/London")
    #[arg(long, env = "TIMEZONE", default_value = "UTC")]
    timezone: String,

    /// Base HPA minimum replicas
    #[arg(long, env = "BASE_HPA_MIN", default_value = "2")]
    base_hpa_min: u32,

    /// Base HPA maximum replicas
    #[arg(long, env = "BASE_HPA_MAX", default_value = "10")]
    base_hpa_max: u32,
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
    info!("  Namespace: {}", args.namespace);
    info!("  Base threshold: {}GB", args.base_threshold_gb);
    info!("  Base HPA: {}-{}", args.base_hpa_min, args.base_hpa_max);

    // Initialize Prometheus metrics
    metrics::init_metrics();

    // Initialize shared auth service
    let auth_service = Arc::new(AuthService::new());

    // Initialize DataSizer for real data estimation
    let data_sizer = Arc::new(DataSizer::new().await);
    info!("DataSizer initialized for real data estimation");

    // Initialize AdaptiveController for dynamic thresholds
    let adaptive_config = AdaptiveConfig {
        timezone: args.timezone.clone(),
        base_threshold_gb: args.base_threshold_gb,
        min_threshold_gb: args.min_threshold_gb,
        max_threshold_gb: args.max_threshold_gb,
        base_hpa_min: args.base_hpa_min,
        base_hpa_max: args.base_hpa_max,
        ..Default::default()
    };
    info!("  Min/Max threshold: {}GB / {}GB", args.min_threshold_gb, args.max_threshold_gb);
    info!("  Timezone: {}", args.timezone);
    let adaptive_controller = Arc::new(
        AdaptiveController::new(adaptive_config, args.namespace.clone()).await
    );
    info!("AdaptiveController initialized");

    // Start adaptive controller background loop
    adaptive_controller.clone().start_background_loop();
    info!("Adaptive scaling loop started (recalculating every 60s)");

    // Initialize QueryRouter with real estimation and adaptive thresholds
    let query_router = Arc::new(QueryRouter::new(
        data_sizer.clone(),
        adaptive_controller.clone(),
    ));
    info!("QueryRouter initialized with real estimation");

    // Create worker client (shared between pg_wire and HTTP API)
    let pg_worker_client = Arc::new(WorkerClient::new(args.worker_addr.clone()));
    
    // Create K8s client for ephemeral pods (shared)
    let k8s_client = Arc::new(k8s_client::K8sQueryClient::new(args.namespace.clone()).await);
    info!("K8s ephemeral pods: {}", if k8s_client.is_available() { "enabled" } else { "disabled (not in cluster)" });

    // Start PostgreSQL wire protocol server with full hybrid routing support
    let pg_auth = auth_service.clone();
    let pg_port = args.pg_port;
    let pg_worker = pg_worker_client.clone();
    let pg_router = query_router.clone();
    let pg_k8s = k8s_client.clone();
    let pg_adaptive = adaptive_controller.clone();
    let pg_handle = tokio::spawn(async move {
        let server = PgWireServer::new(
            pg_port,
            pg_auth,
            pg_worker,
            pg_router,
            pg_k8s,
            pg_adaptive,
        );
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

    // HTTP API uses the same shared clients
    let app_state = AppState {
        worker_client: pg_worker_client,
        k8s_client,
        query_router,
        adaptive_controller,
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
        .route("/api/export", post(http_api::export_query))  // For 10TB+ exports to S3
        .route("/api/adaptive", get(adaptive_state))
        .route("/metrics", get(prometheus_metrics))
        .layer(cors)
        .with_state(app_state);

    info!("HTTP server listening on {}", http_addr);
    info!("  /api/query - Execute queries with hybrid routing");
    info!("  /api/adaptive - View adaptive scaling state");
    info!("  /metrics - Prometheus metrics");

    let http_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    info!("Tavana Gateway started successfully");

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
