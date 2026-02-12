//! Tavana Gateway Service
//!
//! Main entry point for client connections. Supports:
//! - PostgreSQL wire protocol (for Tableau, PowerBI, DBeaver)
//! - Prometheus metrics (/metrics)
//! - Health checks (/health, /ready)
//!
//! Smart scaling architecture:
//! - QueryQueue: FIFO queue with K8s capacity awareness
//! - SmartScaler: VPA-first, HPA-second scaling
//! - Resource ceiling detection for saturation mode
//! - Streaming results for unlimited data size

mod auth;
mod cursors;
mod data_sizer;
mod errors;
mod flight_sql;
mod metrics;
mod pg_wire;
mod query_queue;
mod query_router;
mod smart_scaler;
mod telemetry;
mod tls_config;
mod pg_compat;
mod worker_client;
mod worker_pool;

use crate::auth::AuthService;
use crate::data_sizer::DataSizer;
use crate::pg_wire::PgWireServer;
use crate::query_router::QueryRouter;
use crate::worker_client::WorkerClient;
use crate::worker_pool::{PreSizingConfig, WorkerPoolManager};
use axum::{routing::get, Router};
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

    /// Arrow Flight SQL port (high-performance binary protocol)
    /// Default 443 for HTTPS/gRPC standard port (compatible with corporate firewalls)
    #[arg(long, env = "FLIGHT_SQL_PORT", default_value = "443")]
    flight_sql_port: u16,

    /// HTTP metrics and health check port
    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,

    /// Worker service address (fallback when pre-sizing unavailable)
    #[arg(long, env = "WORKER_ADDR", default_value = "http://localhost:50053")]
    worker_addr: String,

    /// Enable TLS
    #[arg(long, env = "TLS_ENABLED", default_value = "false")]
    tls_enabled: bool,

    /// Enable Flight SQL server
    #[arg(long, env = "FLIGHT_SQL_ENABLED", default_value = "true")]
    flight_sql_enabled: bool,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Authentication mode: passthrough, required, or optional
    #[arg(long, env = "TAVANA_AUTH_MODE", default_value = "passthrough")]
    auth_mode: String,
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
    info!("  Flight SQL port: {} (enabled: {})", args.flight_sql_port, args.flight_sql_enabled);
    info!("  HTTP port: {}", args.http_port);
    info!("  TLS enabled: {}", args.tls_enabled);
    info!("  Worker address: {}", args.worker_addr);
    info!("  Auth mode: {}", args.auth_mode);

    // Initialize Prometheus metrics
    metrics::init_metrics();

    // Initialize auth gateway based on configuration
    let auth_config = auth::AuthConfig::from_env();
    let auth_gateway = match auth::AuthGateway::new(auth_config).await {
        Ok(gateway) => {
            info!(
                "Auth gateway initialized (mode: {:?})",
                gateway.mode()
            );
            Arc::new(gateway)
        }
        Err(e) => {
            warn!("Failed to initialize auth gateway: {} - using passthrough", e);
            Arc::new(auth::AuthGateway::passthrough())
        }
    };

    // Initialize shared auth service (with gateway for new auth)
    let auth_service = Arc::new(AuthService::with_gateway(auth_gateway.clone()));

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
            warn!(
                "Failed to initialize WorkerPoolManager: {} - pre-sizing disabled",
                e
            );
            None
        }
    };

    // Initialize SmartScaler (Adaptive Formula-Based Scaling)
    let namespace = std::env::var("KUBERNETES_NAMESPACE").unwrap_or_else(|_| "tavana".to_string());
    let smart_scaler = match smart_scaler::SmartScaler::new(&namespace, "worker").await {
        Ok(ss) => {
            // Logging is done inside SmartScaler::new()
            Some(Arc::new(ss))
        }
        Err(e) => {
            warn!(
                "Failed to initialize SmartScaler: {} - using legacy pre-sizing",
                e
            );
            None
        }
    };

    // Initialize QueryRouter with or without pre-sizing
    let query_router = if let Some(ref pm) = pool_manager {
        Arc::new(QueryRouter::with_pool_manager(
            data_sizer.clone(),
            pm.clone(),
        ))
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

    // Load TLS configuration if enabled
    let tls_config = if args.tls_enabled {
        tls_config::load_tls_config_from_env()
    } else {
        info!("TLS disabled via --tls-enabled=false");
        None
    };

    // Start PostgreSQL wire protocol server with SmartScaler + shared QueryQueue
    let pg_auth = auth_service.clone();
    let pg_port = args.pg_port;
    let pg_worker = worker_client.clone();
    let pg_router = query_router.clone();
    let pg_pool = pool_manager.clone();
    let pg_scaler = smart_scaler.clone();
    let pg_queue = query_queue.clone();
    let pg_tls = tls_config.clone();
    let pg_handle = tokio::spawn(async move {
        let mut server = pg_wire::PgWireServer::with_smart_scaler_and_queue(
            pg_port, pg_auth, pg_worker, pg_router, pg_pool, pg_scaler, pg_queue,
        )
        .await;
        server.set_tls(pg_tls);
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
        info!(
            "SmartScaler monitoring started with QueryQueue integration (interval={}ms)",
            smart_scaler::MONITOR_INTERVAL_MS
        );
    }

    // Start Arrow Flight SQL server if enabled (ADBC-compatible, high-performance binary protocol)
    let flight_handle = if args.flight_sql_enabled {
        let flight_port = args.flight_sql_port;
        let flight_worker_addr = args.worker_addr.clone();
        Some(tokio::spawn(async move {
            use arrow_flight::flight_service_server::FlightServiceServer;
            use flight_sql::TavanaFlightSqlService;
            use tonic::transport::Server;

            let addr: SocketAddr = format!("0.0.0.0:{}", flight_port)
                .parse()
                .expect("Invalid Flight SQL address");

            // Create ADBC-compatible Flight SQL service
            let flight_service = TavanaFlightSqlService::new(flight_worker_addr);
            let svc = FlightServiceServer::new(flight_service);

            info!("Arrow Flight SQL server (ADBC-compatible) listening on {}", addr);
            info!("  ADBC clients:");
            info!("    - Python: adbc_driver_flightsql.dbapi.connect('grpc://host:{}')", flight_port);
            info!("    - Go: flightsql.NewDriver().Open(\"grpc://host:{}\")", flight_port);
            info!("    - Java: AdbcDriver.open(\"grpc://host:{}\")", flight_port);
            info!("  Other clients:");
            info!("    - pyarrow: pyarrow.flight.connect('grpc://host:{}')", flight_port);
            info!("    - JDBC: jdbc:arrow-flight-sql://host:{}/?useEncryption=false", flight_port);

            if let Err(e) = Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
            {
                tracing::error!("Flight SQL server error: {}", e);
            }
        }))
    } else {
        info!("Flight SQL server disabled");
        None
    };

    // Simple HTTP server for health checks and metrics only
    async fn health() -> &'static str {
        "ok"
    }

    async fn ready() -> &'static str {
        "ready"
    }

    async fn root() -> &'static str {
        "Tavana Gateway - PostgreSQL Wire Protocol"
    }

    async fn metrics() -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
            tracing::error!("Failed to encode prometheus metrics: {}", e);
            return "# Error encoding metrics".to_string();
        }
        String::from_utf8(buffer).unwrap_or_else(|e| {
            tracing::error!("Failed to convert metrics to UTF-8: {}", e);
            "# Error converting metrics to string".to_string()
        })
    }

    // CORS layer
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Start HTTP server for health and metrics
    let http_addr: SocketAddr = format!("0.0.0.0:{}", args.http_port).parse()?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/", get(root))
        .route("/metrics", get(metrics))
        .layer(cors);

    info!("HTTP server listening on {}", http_addr);
    info!("  /health - Health check");
    info!("  /ready - Readiness check");
    info!("  /metrics - Prometheus metrics");

    let http_handle = tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&http_addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("HTTP server error: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Failed to bind HTTP server to {}: {}", http_addr, e);
            }
        }
    });

    info!("Tavana Gateway started successfully");
    info!("Architecture: QueryQueue + SmartScaler (VPA-first, HPA-second)");
    info!("Protocols: PostgreSQL Wire (port {}), Flight SQL (port {})", 
          args.pg_port, 
          if args.flight_sql_enabled { args.flight_sql_port.to_string() } else { "disabled".to_string() });

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down Tavana Gateway");
        }
        _ = pg_handle => {}
        _ = http_handle => {}
        _ = async {
            if let Some(h) = flight_handle {
                h.await.ok();
            } else {
                std::future::pending::<()>().await;
            }
        } => {}
    }

    Ok(())
}
