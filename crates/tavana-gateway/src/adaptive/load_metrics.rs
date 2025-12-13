//! Load Metrics Client
//!
//! Fetches cluster load metrics from Prometheus or Kubernetes API

use anyhow::Result;
use kube::Client;
use serde::Deserialize;
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

/// Prometheus query response structures
#[derive(Debug, Deserialize)]
struct PrometheusResponse {
    status: String,
    data: PrometheusData,
}

#[derive(Debug, Deserialize)]
struct PrometheusData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusResult>,
}

#[derive(Debug, Deserialize)]
struct PrometheusResult {
    #[serde(default)]
    metric: serde_json::Value,
    value: (f64, String),
}

/// Metrics collector for cluster load
pub struct LoadMetricsCollector {
    k8s_client: Option<Client>,
    prometheus_url: Option<String>,
    namespace: String,
    http_client: reqwest::Client,
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

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        Self {
            k8s_client,
            prometheus_url,
            namespace,
            http_client,
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

    /// Query Prometheus for a single value
    async fn query_prometheus(&self, base_url: &str, query: &str) -> Result<f64> {
        let url = format!("{}/api/v1/query?query={}", base_url, urlencoding::encode(query));
        
        let response = self.http_client
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Prometheus query failed with status: {}", response.status());
        }

        let prom_response: PrometheusResponse = response.json().await?;
        
        if prom_response.status != "success" {
            anyhow::bail!("Prometheus query status: {}", prom_response.status);
        }

        if let Some(result) = prom_response.data.result.first() {
            let value: f64 = result.value.1.parse()?;
            return Ok(value);
        }

        anyhow::bail!("No results from Prometheus query")
    }

    /// Fetch metrics from Prometheus
    async fn fetch_from_prometheus(&self, base_url: &str) -> Result<ClusterMetrics> {
        debug!("Fetching metrics from Prometheus at {}", base_url);

        // Query CPU utilization (rate of CPU usage for worker pods)
        let cpu_query = format!(
            r#"avg(sum by (pod) (rate(container_cpu_usage_seconds_total{{namespace="{}",pod=~"worker-.*",container!="",container!="POD"}}[5m])) / on(pod) sum by (pod) (kube_pod_container_resource_limits{{namespace="{}",pod=~"worker-.*",resource="cpu"}})) or vector(0)"#,
            self.namespace, self.namespace
        );

        // Query memory utilization
        let mem_query = format!(
            r#"avg(sum by (pod) (container_memory_working_set_bytes{{namespace="{}",pod=~"worker-.*",container!="",container!="POD"}}) / on(pod) sum by (pod) (kube_pod_container_resource_limits{{namespace="{}",pod=~"worker-.*",resource="memory"}})) or vector(0)"#,
            self.namespace, self.namespace
        );

        // Query ready replicas
        let replicas_query = format!(
            r#"count(kube_pod_status_phase{{namespace="{}",pod=~"worker-.*",phase="Running"}} == 1) or vector(0)"#,
            self.namespace
        );

        // Query desired replicas from deployment
        let desired_query = format!(
            r#"kube_deployment_spec_replicas{{namespace="{}",deployment="worker"}} or vector(2)"#,
            self.namespace
        );

        // Execute queries in parallel
        let (cpu_result, mem_result, replicas_result, desired_result) = tokio::join!(
            self.query_prometheus(base_url, &cpu_query),
            self.query_prometheus(base_url, &mem_query),
            self.query_prometheus(base_url, &replicas_query),
            self.query_prometheus(base_url, &desired_query)
        );

        let cpu_utilization = cpu_result.unwrap_or(0.5).min(1.0).max(0.0);
        let memory_utilization = mem_result.unwrap_or(0.5).min(1.0).max(0.0);
        let ready_replicas = replicas_result.unwrap_or(2.0) as u32;
        let desired_replicas = desired_result.unwrap_or(2.0) as u32;

        let combined_utilization = cpu_utilization.max(memory_utilization);

        debug!(
            "Prometheus metrics: cpu={:.2}, mem={:.2}, ready={}, desired={}",
            cpu_utilization, memory_utilization, ready_replicas, desired_replicas
        );

        Ok(ClusterMetrics {
            cpu_utilization,
            memory_utilization,
            combined_utilization,
            ready_replicas,
            desired_replicas,
            queue_depth: 0, // TODO: Add queue depth metric
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

        // If all pods are ready, assume moderate utilization
        // If some pods are not ready, we're likely under high load
        let estimated_load = if ready_replicas >= desired_replicas {
            0.5 // Healthy state, moderate load
        } else {
            0.8 // Some pods not ready, likely high load
        };

        Ok(ClusterMetrics {
            cpu_utilization: estimated_load,
            memory_utilization: estimated_load,
            combined_utilization: estimated_load,
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
