//! PgWireServer - PostgreSQL wire protocol server
//!
//! Main server struct with constructors and the start() method.

use crate::auth::AuthService;
use crate::metrics;
use crate::query_queue::QueryQueue;
use crate::query_router::QueryRouter;
use crate::smart_scaler::SmartScaler;
use crate::tls_config::TlsConfig;
use crate::worker_client::{WorkerClient, WorkerClientPool};
use crate::worker_pool::WorkerPoolManager;
use super::config::PgWireConfig;
use super::connection::{configure_tcp_keepalive, handle_connection_with_tls};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

/// PostgreSQL wire protocol server with SmartScaler and Query Queue
#[allow(dead_code)]
pub struct PgWireServer {
    addr: SocketAddr,
    auth_service: Arc<AuthService>,
    worker_client: Arc<WorkerClient>,
    worker_client_pool: Arc<WorkerClientPool>,
    query_router: Arc<QueryRouter>,
    pool_manager: Option<Arc<WorkerPoolManager>>,
    smart_scaler: Option<Arc<SmartScaler>>,
    query_queue: Arc<QueryQueue>,
    tls_config: Option<Arc<TlsConfig>>,
    config: Arc<PgWireConfig>,
}

impl PgWireServer {
    pub fn new(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid port number for SocketAddr");
        
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        let config = Arc::new(PgWireConfig::default());
        info!(
            "PgWireServer config: batch_size={}, timeout={}s, buffer={}KB",
            config.streaming_batch_size,
            config.query_timeout_secs,
            config.write_buffer_size / 1024
        );
        
        Self {
            addr,
            auth_service,
            worker_client,
            worker_client_pool,
            query_router,
            pool_manager: None,
            smart_scaler: None,
            query_queue: QueryQueue::new(),
            tls_config: None,
            config,
        }
    }

    pub fn with_config(mut self, config: PgWireConfig) -> Self {
        self.config = Arc::new(config);
        self
    }
    
    pub fn with_query_timeout(mut self, timeout_secs: u64) -> Self {
        let mut config = (*self.config).clone();
        config.query_timeout_secs = timeout_secs;
        self.config = Arc::new(config);
        self
    }

    pub fn with_tls(mut self, tls_config: Option<TlsConfig>) -> Self {
        self.tls_config = tls_config.map(Arc::new);
        self
    }

    pub async fn with_pool_and_queue(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().expect("Invalid port number for SocketAddr");
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        let config = Arc::new(PgWireConfig::default());

        Self {
            addr, auth_service, worker_client, worker_client_pool,
            query_router, pool_manager, smart_scaler: None,
            query_queue: QueryQueue::new(), tls_config: None, config,
        }
    }

    pub async fn with_smart_scaler(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
        smart_scaler: Option<Arc<SmartScaler>>,
    ) -> Self {
        let query_queue = QueryQueue::new();
        Self::with_smart_scaler_and_queue(
            port, auth_service, worker_client, query_router, pool_manager, smart_scaler, query_queue,
        ).await
    }

    pub async fn with_smart_scaler_and_queue(
        port: u16,
        auth_service: Arc<AuthService>,
        worker_client: Arc<WorkerClient>,
        query_router: Arc<QueryRouter>,
        pool_manager: Option<Arc<WorkerPoolManager>>,
        smart_scaler: Option<Arc<SmartScaler>>,
        query_queue: Arc<QueryQueue>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().expect("Invalid port number for SocketAddr");
        let worker_client_pool = Arc::new(WorkerClientPool::new(worker_client.worker_addr().to_string()));
        let config = Arc::new(PgWireConfig::default());

        info!(
            "PgWireServer initialized: SmartScaler={}, batch_size={}, timeout={}s",
            if smart_scaler.is_some() { "enabled" } else { "disabled" },
            config.streaming_batch_size,
            config.query_timeout_secs
        );

        Self {
            addr, auth_service, worker_client, worker_client_pool,
            query_router, pool_manager, smart_scaler, query_queue, tls_config: None, config,
        }
    }

    pub fn set_tls(&mut self, tls_config: Option<TlsConfig>) {
        self.tls_config = tls_config.map(Arc::new);
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let tls_status = if self.tls_config.is_some() {
            "TLS enabled (SSL + non-SSL)"
        } else {
            "TLS disabled (non-SSL only)"
        };
        info!(
            "Starting PostgreSQL wire protocol server on {} ({}, SmartScaler + QueryQueue mode)",
            self.addr, tls_status
        );

        let queue_for_stats = self.query_queue.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                let stats = queue_for_stats.stats().await;
                metrics::set_queue_depth(stats.queue_depth);
                metrics::set_current_usage_mb(stats.current_usage_mb);
                metrics::set_cluster_capacity_mb(stats.total_capacity_mb);
                metrics::set_available_capacity_mb(stats.available_mb);
                metrics::set_hpa_scale_up_signal(stats.hpa_scale_up_signal);

                if stats.queue_depth > 0 || stats.active_queries > 0 {
                    tracing::debug!(
                        queue_depth = stats.queue_depth,
                        active = stats.active_queries,
                        usage_mb = stats.current_usage_mb,
                        capacity_mb = stats.total_capacity_mb,
                        avg_wait_ms = stats.avg_queue_wait_ms,
                        "QueryQueue status"
                    );
                }
            }
        });
        info!("QueryQueue stats logging started");

        let listener = TcpListener::bind(&self.addr).await?;

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            info!("New PostgreSQL connection from {}", peer_addr);

            configure_tcp_keepalive(&socket, self.config.tcp_keepalive_secs);

            let auth_service = self.auth_service.clone();
            let worker_client = self.worker_client.clone();
            let worker_client_pool = self.worker_client_pool.clone();
            let query_router = self.query_router.clone();
            let smart_scaler = self.smart_scaler.clone();
            let query_queue = self.query_queue.clone();
            let tls_config = self.tls_config.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection_with_tls(
                    socket, auth_service, worker_client, worker_client_pool,
                    query_router, smart_scaler, query_queue, tls_config, config,
                ).await {
                    let err_str = e.to_string();
                    if err_str.contains("early eof") || err_str.contains("connection reset") {
                        debug!("Client disconnected: {}", err_str);
                    } else {
                        error!("Error handling PostgreSQL connection: {}", e);
                    }
                }
            });
        }
    }
}
