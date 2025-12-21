//! Tavana Gateway Service
//!
//! Main entry point for client connections. Supports:
//! - PostgreSQL wire protocol (for Tableau, PowerBI, DBeaver)
//! - Arrow Flight SQL (for Python, Polars, DuckDB)
//! - REST API (for management)
//! - Prometheus metrics (/metrics)
//!
//! Query-aware pre-sizing architecture:
//! - Estimate query size before execution
//! - Pre-size worker to 50% of data size (configurable)
//! - K8s v1.35 in-place resize for instant scaling
//! - HPA+VPA for automatic scaling

mod adaptive_queue;
mod auth;
mod data_sizer;
mod flight;
mod http_api;
mod metrics;
mod pg_wire;
mod query;
mod query_queue;
mod query_router;
mod redis_queue;
mod smart_scaler;
mod telemetry;
mod tenant_pool;
mod worker_client;
mod worker_pool;

use crate::auth::AuthService;
use crate::data_sizer::DataSizer;
use crate::flight::FlightSqlServer;
use crate::http_api::{execute_query, health, prometheus_metrics, ready, root, AppState};
use crate::pg_wire::PgWireServer;
use crate::query_router::QueryRouter;
use crate::worker_client::WorkerClient;
use crate::worker_pool::{PreSizingConfig, WorkerPoolManager};
use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

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

    /// Worker service address (fallback when pre-sizing unavailable)
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

    // Initialize DataSizer for data estimation
    let data_sizer = Arc::new(DataSizer::new().await);
    info!("DataSizer initialized");

    // Initialize WorkerPoolManager for pre-sizing
    let pre_sizing_config = PreSizingConfig::from_env();
    info!(
        "Pre-sizing config: enabled={}, multiplier={}, min={}MB, max={}MB",
        pre_sizing_config.enabled,
        pre_sizing_config.memory_multiplier,
        pre_sizing_config.min_memory_mb,
        pre_sizing_config.max_memory_mb
    );

    let pool_manager = match WorkerPoolManager::new(pre_sizing_config.clone()).await {
        Ok(pm) => {
            info!("WorkerPoolManager initialized - query-aware pre-sizing enabled");
            Some(Arc::new(pm))
        }
        Err(e) => {
            warn!("Failed to initialize WorkerPoolManager: {} - pre-sizing disabled", e);
            None
        }
    };

    // Initialize SmartScaler (Adaptive Formula-Based Scaling)
    let smart_scaler = match smart_scaler::SmartScaler::new("tavana", "worker").await {
        Ok(ss) => {
            // Logging is done inside SmartScaler::new()
            Some(Arc::new(ss))
        }
        Err(e) => {
            warn!("Failed to initialize SmartScaler: {} - using legacy pre-sizing", e);
            None
        }
    };

    // Initialize QueryRouter with or without pre-sizing
    let query_router = if let Some(ref pm) = pool_manager {
        Arc::new(QueryRouter::with_pool_manager(data_sizer.clone(), pm.clone()))
    } else {
        Arc::new(QueryRouter::new(data_sizer.clone()))
    };
    info!("QueryRouter initialized");

    // Create worker client (fallback for when pre-sizing is unavailable)
    let worker_client = Arc::new(WorkerClient::new(args.worker_addr.clone()));

    // Create SHARED QueryQueue for true FIFO queuing with K8s capacity awareness
    // This queue:
    // 1. Never rejects queries (always enqueues)
    // 2. Awaits until real K8s capacity is available
    // 3. Signals HPA when queue is backing up
    let query_queue = query_queue::QueryQueue::new();
    info!("QueryQueue initialized (K8s capacity-aware FIFO queue)");

    // Start QueryQueue dispatcher loop (processes waiting queries)
    let dispatcher_queue = query_queue.clone();
    tokio::spawn(async move {
        dispatcher_queue.start_dispatcher().await;
    });

    // Start K8s capacity updater (queries real worker memory from K8s every 1s)
    if let Ok(k8s_client) = kube::Client::try_default().await {
        let capacity_queue = query_queue.clone();
        tokio::spawn(async move {
            capacity_queue.start_capacity_updater(k8s_client).await;
        });
        info!("QueryQueue K8s capacity updater started (interval=1s)");
    } else {
        warn!("Failed to create K8s client - using default capacity estimates");
    }

    // Start PostgreSQL wire protocol server with SmartScaler + shared QueryQueue
    let pg_auth = auth_service.clone();
    let pg_port = args.pg_port;
    let pg_worker = worker_client.clone();
    let pg_router = query_router.clone();
    let pg_pool = pool_manager.clone();
    let pg_scaler = smart_scaler.clone();
    let pg_queue = query_queue.clone();
    let pg_handle = tokio::spawn(async move {
        let server = pg_wire::PgWireServer::with_smart_scaler_and_queue(
            pg_port,
            pg_auth,
            pg_worker,
            pg_router,
            pg_pool,
            pg_scaler,
            pg_queue,
        ).await;
        if let Err(e) = server.start().await {
            tracing::error!("PostgreSQL server error: {}", e);
        }
    });
    
    // Start SmartScaler monitoring with shared QueryQueue
    // HPA decisions now based on: queue depth, wait time, capacity utilization
    if let Some(ref scaler) = smart_scaler {
        let scaler_clone = scaler.clone();
        let queue_clone = query_queue.clone();
        scaler_clone.start_monitoring_with_queue(queue_clone);
        info!("SmartScaler monitoring started with QueryQueue integration (interval={}ms)", smart_scaler::MONITOR_INTERVAL_MS);
    }

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
    info!("  /api/query - Execute queries with query-aware pre-sizing");
    info!("  /metrics - Prometheus metrics");

    let http_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    info!("Tavana Gateway started successfully");
    info!("Architecture: Query-aware pre-sizing with K8s v1.35 in-place resize");

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
