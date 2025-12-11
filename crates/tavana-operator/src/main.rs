//! Tavana Operator Service
//!
//! Kubernetes operator that manages DuckDB worker pods.
//! Watches DuckDBQuery CRDs and creates appropriately-sized pods.

mod crd;
mod estimation;
mod reconciler;

use crate::crd::DuckDBQuery;
use crate::reconciler::{reconcile, error_policy, Context};
use clap::Parser;
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::controller::Controller,
    Client,
};
use std::sync::Arc;
use tracing::{info, error};

#[derive(Parser, Debug)]
#[command(name = "tavana-operator")]
#[command(about = "Tavana Kubernetes Operator")]
struct Args {
    /// Kubernetes namespace to watch
    #[arg(long, env = "NAMESPACE", default_value = "tavana")]
    namespace: String,

    /// Worker image to use
    #[arg(long, env = "WORKER_IMAGE", default_value = "tavana-worker:latest")]
    worker_image: String,

    /// Metrics port
    #[arg(long, env = "METRICS_PORT", default_value = "8080")]
    metrics_port: u16,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install rustls crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    
    // Load environment variables from .env if present
    dotenvy::dotenv().ok();
    
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    info!("Starting Tavana Operator");
    info!("  Namespace: {}", args.namespace);
    info!("  Worker image: {}", args.worker_image);

    // Initialize Kubernetes client
    let client = Client::try_default().await?;
    info!("Connected to Kubernetes cluster");

    // Create API for DuckDBQuery resources
    let queries: Api<DuckDBQuery> = Api::namespaced(client.clone(), &args.namespace);

    // Create context for reconciler
    let ctx = Arc::new(Context {
        client: client.clone(),
        namespace: args.namespace.clone(),
        worker_image: args.worker_image.clone(),
    });

    info!("Starting controller for DuckDBQuery resources");

    // Start the controller
    Controller::new(queries, Default::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok(action) => {
                    info!("Reconciliation successful: {:?}", action);
                }
                Err(e) => {
                    error!("Reconciliation error: {:?}", e);
                }
            }
        })
        .await;

    info!("Tavana Operator shutting down");

    Ok(())
}
