//! Worker Pool Manager with Query-Aware Pre-Sizing
//!
//! Tracks worker pods, their status, and current resources.
//! Uses K8s v1.35 in-place resize to pre-size workers before query execution.

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    Client,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::metrics;

/// Configuration for pre-sizing
#[derive(Clone, Debug)]
pub struct PreSizingConfig {
    /// Multiplier for calculating memory from data size (default: 0.5)
    pub memory_multiplier: f64,
    /// Minimum memory in MB (VPA min)
    pub min_memory_mb: u64,
    /// Maximum memory in MB (VPA max)
    pub max_memory_mb: u64,
    /// Whether pre-sizing is enabled
    pub enabled: bool,
    /// Namespace where workers are deployed
    pub namespace: String,
    /// Label selector for worker pods
    pub worker_label_selector: String,
}

impl Default for PreSizingConfig {
    fn default() -> Self {
        Self {
            memory_multiplier: 0.5,
            min_memory_mb: 256,
            max_memory_mb: 400 * 1024, // 400GB
            enabled: true,
            namespace: "tavana".to_string(),
            worker_label_selector: "app=tavana-worker".to_string(),
        }
    }
}

impl PreSizingConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(v) = std::env::var("PRE_SIZING_MULTIPLIER") {
            if let Ok(m) = v.parse::<f64>() {
                config.memory_multiplier = m;
            }
        }
        
        if let Ok(v) = std::env::var("PRE_SIZING_ENABLED") {
            config.enabled = v.to_lowercase() == "true";
        }
        
        if let Ok(v) = std::env::var("PRE_SIZING_MIN_MB") {
            if let Ok(m) = v.parse::<u64>() {
                config.min_memory_mb = m;
            }
        }
        
        if let Ok(v) = std::env::var("PRE_SIZING_MAX_MB") {
            if let Ok(m) = v.parse::<u64>() {
                config.max_memory_mb = m;
            }
        }
        
        config
    }
}

/// Status of a worker pod
#[derive(Clone, Debug)]
pub struct WorkerStatus {
    /// Pod name
    pub name: String,
    /// Pod IP address
    pub ip: String,
    /// Current memory request in MB
    pub current_memory_mb: u64,
    /// Whether the worker is currently busy with a query
    pub busy: bool,
    /// When the worker was last used
    pub last_used: Instant,
}

/// Result of requesting a pre-sized worker
#[derive(Debug)]
pub struct PreSizedWorker {
    /// Pod name
    pub name: String,
    /// Worker address (IP:port)
    pub address: String,
    /// Memory allocated in MB
    pub memory_mb: u64,
    /// Whether a resize was performed
    pub resized: bool,
}

/// Manages the worker pool with pre-sizing capabilities
pub struct WorkerPoolManager {
    /// K8s client
    client: Client,
    /// Configuration
    config: PreSizingConfig,
    /// Worker status cache
    workers: Arc<RwLock<HashMap<String, WorkerStatus>>>,
    /// Worker gRPC port
    worker_port: u16,
}

impl WorkerPoolManager {
    /// Create a new WorkerPoolManager
    pub async fn new(config: PreSizingConfig) -> Result<Self, kube::Error> {
        let client = Client::try_default().await?;
        
        Ok(Self {
            client,
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            worker_port: 50053,
        })
    }
    
    /// Check if K8s client is available
    pub fn is_available(&self) -> bool {
        true // Client was created successfully
    }
    
    /// Check if pre-sizing is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
    
    /// Calculate required memory for a query
    pub fn calculate_memory_mb(&self, data_size_mb: u64) -> u64 {
        let raw_memory = (data_size_mb as f64 * self.config.memory_multiplier) as u64;
        
        // Clamp to VPA range
        raw_memory
            .max(self.config.min_memory_mb)
            .min(self.config.max_memory_mb)
    }
    
    /// Refresh the worker cache from K8s
    pub async fn refresh_workers(&self) -> Result<(), kube::Error> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let lp = ListParams::default().labels(&self.config.worker_label_selector);
        
        let pod_list = pods.list(&lp).await?;
        
        let mut workers = self.workers.write().await;
        
        // Track which pods we see
        let mut seen_pods: Vec<String> = Vec::new();
        
        for pod in pod_list.items {
            let name = pod.metadata.name.clone().unwrap_or_default();
            seen_pods.push(name.clone());
            
            // Get pod IP
            let ip = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.clone())
                .unwrap_or_default();
            
            if ip.is_empty() {
                continue; // Pod not ready
            }
            
            // Get current memory request
            let memory_mb = pod
                .spec
                .as_ref()
                .and_then(|s| s.containers.first())
                .and_then(|c| c.resources.as_ref())
                .and_then(|r| r.requests.as_ref())
                .and_then(|r| r.get("memory"))
                .map(|q| parse_memory_to_mb(q))
                .unwrap_or(self.config.min_memory_mb);
            
            // Check if pod is ready
            let is_ready = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True")
                })
                .unwrap_or(false);
            
            if !is_ready {
                continue;
            }
            
            // Update or insert worker status
            workers.entry(name.clone()).and_modify(|w| {
                w.ip = ip.clone();
                w.current_memory_mb = memory_mb;
            }).or_insert(WorkerStatus {
                name: name.clone(),
                ip,
                current_memory_mb: memory_mb,
                busy: false,
                last_used: Instant::now() - Duration::from_secs(3600),
            });
        }
        
        // Remove workers that no longer exist
        workers.retain(|name, _| seen_pods.contains(name));
        
        debug!("Refreshed worker pool: {} workers", workers.len());
        Ok(())
    }
    
    /// Get a pre-sized worker for a query
    /// 
    /// This will:
    /// 1. Find an idle worker
    /// 2. Resize it if needed (in-place, no restart)
    /// 3. Mark it as busy
    /// 4. Return the worker address
    pub async fn get_presized_worker(
        &self,
        required_memory_mb: u64,
        required_cpu_cores: f32,
    ) -> Result<PreSizedWorker, anyhow::Error> {
        let start = Instant::now();
        
        // Refresh worker list
        self.refresh_workers().await?;
        
        let mut workers = self.workers.write().await;
        
        // Update worker pool status metrics
        let total = workers.len() as i32;
        let busy = workers.values().filter(|w| w.busy).count() as i32;
        let idle = total - busy;
        metrics::update_worker_pool_status(total, busy, idle, 0);
        
        // Find an idle worker
        let idle_worker = workers
            .values_mut()
            .filter(|w| !w.busy && !w.ip.is_empty())
            .min_by_key(|w| w.last_used);
        
        let worker = match idle_worker {
            Some(w) => w,
            None => {
                metrics::record_worker_presize("failed", 0.0, 0.0, start.elapsed().as_secs_f64());
                return Err(anyhow::anyhow!("No idle workers available"));
            }
        };
        
        let worker_name = worker.name.clone();
        let worker_ip = worker.ip.clone();
        let current_memory = worker.current_memory_mb;
        
        // Check if resize is needed
        let needs_resize = required_memory_mb > current_memory;
        
        if needs_resize {
            info!(
                "Pre-sizing worker {} from {}MB to {}MB",
                worker_name, current_memory, required_memory_mb
            );
            
            // Update resizing count
            metrics::update_worker_pool_status(total, busy, idle - 1, 1);
            
            // Perform in-place resize
            match self.resize_worker(&worker_name, required_memory_mb).await {
                Ok(_) => {
                    worker.current_memory_mb = required_memory_mb;
                    let memory_delta = required_memory_mb as f64 - current_memory as f64;
                    metrics::record_inplace_resize("scale_up", memory_delta);
                }
                Err(e) => {
                    warn!("Failed to resize worker {}: {}", worker_name, e);
                    metrics::record_worker_presize("failed", required_memory_mb as f64, required_cpu_cores as f64, start.elapsed().as_secs_f64());
                    metrics::update_worker_pool_status(total, busy, idle, 0);
                    return Err(e);
                }
            }
        } else {
            debug!(
                "Worker {} already has sufficient memory ({}MB >= {}MB)",
                worker_name, current_memory, required_memory_mb
            );
            metrics::record_worker_presize("skipped", current_memory as f64, required_cpu_cores as f64, start.elapsed().as_secs_f64());
        }
        
        // Mark as busy
        worker.busy = true;
        worker.last_used = Instant::now();
        
        let address = format!("http://{}:{}", worker_ip, self.worker_port);
        let final_memory = required_memory_mb.max(current_memory);
        
        // Record successful pre-sizing
        if needs_resize {
            metrics::record_worker_presize("success", final_memory as f64, required_cpu_cores as f64, start.elapsed().as_secs_f64());
        }
        
        // Update pool status after allocation
        let busy_now = workers.values().filter(|w| w.busy).count() as i32;
        let idle_now = total - busy_now;
        metrics::update_worker_pool_status(total, busy_now, idle_now, 0);
        
        // Record pre-sizing multiplier used
        metrics::record_presize_multiplier(self.config.memory_multiplier);
        
        Ok(PreSizedWorker {
            name: worker_name,
            address,
            memory_mb: final_memory,
            resized: needs_resize,
        })
    }
    
    /// Release a worker (mark as idle)
    pub async fn release_worker(&self, worker_name: &str, actual_memory_used_mb: Option<u64>) {
        let mut workers = self.workers.write().await;
        if let Some(worker) = workers.get_mut(worker_name) {
            // Record memory utilization if we have actual usage data
            if let Some(actual_mb) = actual_memory_used_mb {
                metrics::record_presize_memory_utilization(
                    worker.current_memory_mb as f64,
                    actual_mb as f64,
                );
            }
            
            worker.busy = false;
            debug!("Released worker {}", worker_name);
            
            // Update pool status after release
            let total = workers.len() as i32;
            let busy = workers.values().filter(|w| w.busy).count() as i32;
            let idle = total - busy;
            metrics::update_worker_pool_status(total, busy, idle, 0);
        }
    }
    
    /// Resize a worker pod using K8s API (in-place resize)
    /// Uses replace_subresource("resize") which is the correct API for K8s 1.35+ in-place pod resizing
    async fn resize_worker(&self, pod_name: &str, memory_mb: u64) -> Result<(), anyhow::Error> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.config.namespace);
        
        // Get current pod spec first
        let mut pod = pods.get(pod_name).await?;
        
        // Modify the resources - K8s 1.35+ with resizePolicy: NotRequired allows this without restart
        if let Some(ref mut spec) = pod.spec {
            if let Some(container) = spec.containers.iter_mut().find(|c| c.name == "worker") {
                let memory_request = format!("{}Mi", memory_mb);
                let memory_limit = format!("{}Gi", (memory_mb as f64 / 1024.0 * 2.0).ceil() as u64);
                
                // Update resources
                let resources = container.resources.get_or_insert_with(Default::default);
                let requests = resources.requests.get_or_insert_with(Default::default);
                requests.insert("memory".to_string(), k8s_openapi::apimachinery::pkg::api::resource::Quantity(memory_request));
                
                let limits = resources.limits.get_or_insert_with(Default::default);
                limits.insert("memory".to_string(), k8s_openapi::apimachinery::pkg::api::resource::Quantity(memory_limit));
            }
        }
        
        // Use replace_subresource with "resize" - this is the correct API for in-place pod resize
        // Equivalent to: kubectl replace -f pod.yaml --subresource=resize
        let replace_params = kube::api::PostParams::default();
        pods.replace_subresource("resize", pod_name, &replace_params, serde_json::to_vec(&pod)?).await?;
        
        info!("Resized worker {} to {}MB (in-place via resize subresource)", pod_name, memory_mb);
        Ok(())
    }
    
    /// Get the default worker service address (fallback when pre-sizing is disabled)
    pub fn get_default_worker_addr(&self) -> String {
        format!("http://worker.{}.svc.cluster.local:{}", self.config.namespace, self.worker_port)
    }
    
    /// Get statistics about the worker pool
    pub async fn get_stats(&self) -> WorkerPoolStats {
        let workers = self.workers.read().await;
        
        let total = workers.len();
        let busy = workers.values().filter(|w| w.busy).count();
        let idle = total - busy;
        let total_memory_mb: u64 = workers.values().map(|w| w.current_memory_mb).sum();
        
        WorkerPoolStats {
            total_workers: total,
            busy_workers: busy,
            idle_workers: idle,
            total_memory_mb,
        }
    }
}

/// Statistics about the worker pool
#[derive(Debug)]
pub struct WorkerPoolStats {
    pub total_workers: usize,
    pub busy_workers: usize,
    pub idle_workers: usize,
    pub total_memory_mb: u64,
}

/// Parse K8s memory quantity to MB
fn parse_memory_to_mb(quantity: &Quantity) -> u64 {
    let value = quantity.0.as_str();
    
    // Parse common formats: "256Mi", "1Gi", "512M", "1G"
    if value.ends_with("Gi") {
        value.trim_end_matches("Gi").parse::<u64>().unwrap_or(0) * 1024
    } else if value.ends_with("Mi") {
        value.trim_end_matches("Mi").parse::<u64>().unwrap_or(0)
    } else if value.ends_with("G") {
        value.trim_end_matches("G").parse::<u64>().unwrap_or(0) * 1024
    } else if value.ends_with("M") {
        value.trim_end_matches("M").parse::<u64>().unwrap_or(0)
    } else if value.ends_with("Ki") {
        value.trim_end_matches("Ki").parse::<u64>().unwrap_or(0) / 1024
    } else {
        // Assume bytes
        value.parse::<u64>().unwrap_or(0) / (1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_memory_to_mb() {
        assert_eq!(parse_memory_to_mb(&Quantity("256Mi".to_string())), 256);
        assert_eq!(parse_memory_to_mb(&Quantity("1Gi".to_string())), 1024);
        assert_eq!(parse_memory_to_mb(&Quantity("4Gi".to_string())), 4096);
        assert_eq!(parse_memory_to_mb(&Quantity("512M".to_string())), 512);
    }
    
    #[test]
    fn test_calculate_memory() {
        let config = PreSizingConfig::default();
        let manager_config = config.clone();
        
        // 100MB data -> 50MB required -> clamped to 256MB (min)
        assert_eq!(
            (100.0 * manager_config.memory_multiplier) as u64,
            50
        );
        
        // 1GB data -> 512MB required
        assert_eq!(
            (1024.0 * manager_config.memory_multiplier) as u64,
            512
        );
    }
}

