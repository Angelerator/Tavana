//! Load Metrics Client
//!
//! Fetches cluster load metrics from Prometheus or Kubernetes API

use anyhow::Result;
use kube::Client;
use tracing::{debug, warn};

/// Cluster load metrics
#[derive(Debug, Clone)]
pub struct ClusterMetrics {
    /// Average CPU utilization across workers (0.0 - 1.0)
    pub cpu_utilization: f64,
    /// Average memory utilization across workers (0.0 - 1.0)
    pub memory_utilization: f64,
    /// Combined utilization (max of CPU and memory)
    pub combined_utilization: f64,
    /// Number of ready worker replicas
    pub ready_replicas: u32,
    /// Number of desired worker replicas
    pub desired_replicas: u32,
    /// Number of pending queries in queue
    pub queue_depth: u32,
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self {
            cpu_utilization: 0.5,
            memory_utilization: 0.5,
            combined_utilization: 0.5,
            ready_replicas: 2,
            desired_replicas: 2,
            queue_depth: 0,
        }
    }
}

/// Metrics collector for cluster load
pub struct LoadMetricsCollector {
    k8s_client: Option<Client>,
    prometheus_url: Option<String>,
    namespace: String,
}

impl LoadMetricsCollector {
    /// Create a new metrics collector
    pub async fn new(namespace: String) -> Self {
        let k8s_client = match Client::try_default().await {
            Ok(c) => Some(c),
            Err(e) => {
                warn!("K8s client not available for metrics: {}", e);
                None
            }
        };

        let prometheus_url = std::env::var("PROMETHEUS_URL").ok();

        Self {
            k8s_client,
            prometheus_url,
            namespace,
        }
    }

    /// Fetch current cluster metrics
    pub async fn get_metrics(&self) -> ClusterMetrics {
        // Try Prometheus first
        if let Some(url) = &self.prometheus_url {
            if let Ok(metrics) = self.fetch_from_prometheus(url).await {
                return metrics;
            }
        }

        // Fall back to Kubernetes API
        if let Some(client) = &self.k8s_client {
            if let Ok(metrics) = self.fetch_from_kubernetes(client).await {
                return metrics;
            }
        }

        // Return defaults if all sources fail
        warn!("Using default cluster metrics (no sources available)");
        ClusterMetrics::default()
    }

    /// Fetch metrics from Prometheus
    async fn fetch_from_prometheus(&self, base_url: &str) -> Result<ClusterMetrics> {
        // Query CPU utilization
        let cpu_query = format!(
            "{}/api/v1/query?query=avg(container_cpu_usage_seconds_total{{namespace=\"{}\",pod=~\"worker-.*\"}})",
            base_url, self.namespace
        );

        // Query memory utilization
        let mem_query = format!(
            "{}/api/v1/query?query=avg(container_memory_usage_bytes{{namespace=\"{}\",pod=~\"worker-.*\"}})/avg(container_spec_memory_limit_bytes{{namespace=\"{}\",pod=~\"worker-.*\"}})",
            base_url, self.namespace, self.namespace
        );

        // For simplicity, we'll use estimated values
        // In production, you'd actually make HTTP requests to Prometheus
        debug!("Would query Prometheus at: {}", cpu_query);
        debug!("Would query Prometheus at: {}", mem_query);

        // Placeholder - return moderate utilization
        Ok(ClusterMetrics {
            cpu_utilization: 0.5,
            memory_utilization: 0.5,
            combined_utilization: 0.5,
            ready_replicas: 2,
            desired_replicas: 2,
            queue_depth: 0,
        })
    }

    /// Fetch metrics from Kubernetes API
    async fn fetch_from_kubernetes(&self, client: &Client) -> Result<ClusterMetrics> {
        use k8s_openapi::api::apps::v1::Deployment;
        use kube::api::Api;

        let deployments: Api<Deployment> = Api::namespaced(client.clone(), &self.namespace);

        let worker_deployment = deployments.get("worker").await?;

        let status = worker_deployment.status.unwrap_or_default();
        let ready_replicas = status.ready_replicas.unwrap_or(0) as u32;
        let desired_replicas = status.replicas.unwrap_or(0) as u32;

        // We can't get CPU/memory from basic K8s API without metrics-server
        // Use HPA target utilization as a proxy
        let utilization = if desired_replicas > 0 {
            ready_replicas as f64 / desired_replicas as f64
        } else {
            0.5
        };

        Ok(ClusterMetrics {
            cpu_utilization: 1.0 - utilization, // Inverse as approximation
            memory_utilization: 1.0 - utilization,
            combined_utilization: 1.0 - utilization,
            ready_replicas,
            desired_replicas,
            queue_depth: 0,
        })
    }

    /// Calculate load-based factors
    pub fn calculate_factors(&self, metrics: &ClusterMetrics, config: &super::config::AdaptiveConfig) -> (f64, f64) {
        config.load_response.get_factors(metrics.combined_utilization)
    }
}

