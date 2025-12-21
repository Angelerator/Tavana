//! Smart Scaler - Adaptive Formula-Based Scaling
//!
//! Integrates with QueryQueue for intelligent scaling decisions:
//! - Pre-assignment VPA sizing (resize before query)
//! - Elastic VPA during execution (grow when needed)
//! - Smart shrink after completion (reduce when idle)
//! - Adaptive HPA (scale based on queue depth + wait time + utilization)
//!
//! KEY INTEGRATION:
//! - QueryQueue provides: queue depth, wait time, capacity utilization
//! - SmartScaler uses these to decide HPA scale up/down
//!
//! SCALE-UP TRIGGERS (from QueryQueue signals):
//!   - Queue depth > 10 queries → scale up
//!   - Average wait time > 5s → scale up
//!   - HPA signal flag from queue → immediate scale up
//!
//! SCALE-DOWN TRIGGERS:
//!   - Worker idle (0 queries) for 30 minutes AND CPU < 2%
//!   - Worker age > 5 hours (force recycle for cache cleanup)
//!   - Rejection rate = 0% AND utilization < 30% for 10 min
//!
//! MONITORING: Every 1 second

use k8s_openapi::api::apps::v1::Deployment;
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

use crate::query_queue::QueryQueue;
use crate::metrics;

// ═══════════════════════════════════════════════════════════════════════════
// ADAPTIVE SCALING CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

// --- BOUNDS (fixed, not learned) ---

/// Minimum number of workers (HPA floor)
pub const MIN_WORKERS: i32 = 2;

/// Maximum number of workers (HPA ceiling)
pub const MAX_WORKERS: i32 = 100;

/// Minimum memory per worker in MB
pub const MIN_MEMORY_MB: u64 = 256;

/// Maximum memory per worker in MB (400GB)
pub const MAX_MEMORY_MB: u64 = 400 * 1024;

// --- ADAPTIVE TARGETS (configurable, not thresholds) ---

/// Target wait time for queries in queue (ms) - scale up if exceeded
pub const TARGET_WAIT_TIME_MS: u64 = 2000;

/// Target utilization ratio (for VPA sizing)
pub const TARGET_UTILIZATION: f64 = 0.70;

/// Safety margin for memory estimation errors (add 30%)
pub const ESTIMATION_SAFETY: f64 = 1.3;

// --- VPA ELASTIC SCALING ---

/// Elastic resize threshold (resize when utilization > 80%)
pub const ELASTIC_THRESHOLD: f64 = 0.8;

/// Elastic resize growth factor (grow by 50%)
pub const ELASTIC_GROWTH: f64 = 1.5;

/// Idle timeout before shrinking worker memory (30 seconds)
pub const SHRINK_IDLE_SECONDS: u64 = 30;

// --- HPA SCALE-DOWN (idleness-based, not resource-based) ---

/// Idle duration required for scale-down (30 minutes = 1800 seconds)
pub const IDLE_DURATION_FOR_SCALEDOWN_SECS: u64 = 1800;

/// CPU threshold for "truly idle" (2% = 0.02)
pub const IDLE_CPU_THRESHOLD: f64 = 0.02;

/// Maximum worker age before force recycle (5 hours = 18000 seconds)
pub const MAX_WORKER_AGE_SECS: u64 = 18000;

// --- MONITORING ---

/// Monitoring interval (1 second = 1000ms)
pub const MONITOR_INTERVAL_MS: u64 = 1000;

/// History window for learning (5 minutes of samples at 1s interval = 300)
pub const HISTORY_WINDOW_SIZE: usize = 300;

// --- LEARNING DEFAULTS (initial values, will be updated from history) ---

/// Default queries per worker (learned from history)
pub const DEFAULT_QUERIES_PER_WORKER: f64 = 8.0;

/// Default worker memory capacity in MB (learned from history)  
pub const DEFAULT_WORKER_MEMORY_MB: u64 = 1024;

// --- LEGACY (kept for backward compatibility) ---
pub const MEMORY_HEADROOM: f64 = 0.2;
pub const CONCURRENT_QUERIES_PER_WORKER: u64 = 8;
pub const SCALEDOWN_THRESHOLD: f64 = 0.3;
pub const HPA_IDLE_SECONDS: u64 = 300;
pub const ELASTIC_CHECK_INTERVAL_MS: u64 = 1000; // Updated to 1 second

// ═══════════════════════════════════════════════════════════════════════════
// DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════

/// Worker state tracked by SmartScaler
#[derive(Debug, Clone)]
pub struct WorkerState {
    /// Pod name
    pub name: String,
    /// Pod IP address
    pub address: String,
    /// Current memory limit in MB
    pub memory_limit_mb: u64,
    /// Current memory usage in MB (from metrics)
    pub memory_used_mb: u64,
    /// Current CPU limit in millicores
    pub cpu_limit_m: u64,
    /// Current CPU usage in millicores
    pub cpu_used_m: u64,
    /// Queries currently running on this worker
    pub running_queries: Vec<QueryState>,
    /// Last activity time (last query started or completed)
    pub last_activity: Instant,
    /// Whether a resize is in progress
    pub resize_in_progress: bool,
    
    // --- ADAPTIVE SCALING FIELDS ---
    
    /// When this worker was created (for 5-hour age limit)
    pub created_at: Instant,
    /// When this worker became idle (no queries running)
    pub idle_since: Option<Instant>,
    /// Rolling CPU usage history (for 30-min average)
    pub cpu_history: std::collections::VecDeque<f64>,
    /// Total queries completed on this worker (for learning)
    pub queries_completed: u64,
    /// Total memory used by completed queries (for learning avg)
    pub total_query_memory_mb: u64,
}

impl WorkerState {
    /// Calculate available memory
    pub fn available_memory_mb(&self) -> u64 {
        self.memory_limit_mb.saturating_sub(self.memory_used_mb)
    }

    /// Calculate memory utilization ratio
    pub fn memory_utilization(&self) -> f64 {
        if self.memory_limit_mb == 0 {
            return 0.0;
        }
        self.memory_used_mb as f64 / self.memory_limit_mb as f64
    }

    /// Calculate effective capacity (with headroom)
    pub fn effective_capacity_mb(&self) -> u64 {
        ((self.memory_limit_mb as f64) * (1.0 - MEMORY_HEADROOM)) as u64
    }

    /// Check if worker can accept a new query with given memory requirement
    pub fn can_accept_query(&self, required_mb: u64) -> bool {
        let required_with_safety = (required_mb as f64 * ESTIMATION_SAFETY) as u64;
        self.available_memory_mb() >= required_with_safety
    }

    /// Calculate new limit needed to run a query
    pub fn required_limit_for_query(&self, query_estimated_mb: u64) -> u64 {
        let required_with_safety = (query_estimated_mb as f64 * ESTIMATION_SAFETY) as u64;
        self.memory_used_mb + required_with_safety
    }
    
    // --- ADAPTIVE SCALING METHODS ---
    
    /// Get worker age in seconds
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }
    
    /// Check if worker is too old (> 5 hours)
    pub fn is_too_old(&self) -> bool {
        self.age_secs() > MAX_WORKER_AGE_SECS
    }
    
    /// Get idle duration in seconds (None if not idle)
    pub fn idle_duration_secs(&self) -> Option<u64> {
        self.idle_since.map(|t| t.elapsed().as_secs())
    }
    
    /// Check if worker is truly idle (no queries for 30min AND CPU < 2%)
    pub fn is_truly_idle(&self) -> bool {
        // Must have no running queries
        if !self.running_queries.is_empty() {
            return false;
        }
        
        // Must be idle for at least 30 minutes
        let idle_long_enough = self.idle_duration_secs()
            .map(|d| d >= IDLE_DURATION_FOR_SCALEDOWN_SECS)
            .unwrap_or(false);
        
        if !idle_long_enough {
            return false;
        }
        
        // CPU must be < 2% on average
        self.avg_cpu_utilization() < IDLE_CPU_THRESHOLD
    }
    
    /// Calculate average CPU utilization from history
    pub fn avg_cpu_utilization(&self) -> f64 {
        if self.cpu_history.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.cpu_history.iter().sum();
        sum / self.cpu_history.len() as f64
    }
    
    /// Record CPU sample for history (call every 1 second)
    pub fn record_cpu_sample(&mut self) {
        let cpu_util = if self.cpu_limit_m > 0 {
            self.cpu_used_m as f64 / self.cpu_limit_m as f64
        } else {
            0.0
        };
        
        self.cpu_history.push_back(cpu_util);
        
        // Keep only last 30 minutes (1800 samples at 1/sec)
        while self.cpu_history.len() > 1800 {
            self.cpu_history.pop_front();
        }
    }
    
    /// Get learned average queries per worker
    pub fn learned_queries_per_worker(&self) -> f64 {
        if self.queries_completed == 0 {
            return DEFAULT_QUERIES_PER_WORKER;
        }
        // This worker's running queries at peak
        self.running_queries.len().max(1) as f64
    }
    
    /// Get learned average memory per query
    pub fn learned_memory_per_query_mb(&self) -> u64 {
        if self.queries_completed == 0 {
            return DEFAULT_WORKER_MEMORY_MB / 8; // Default estimate
        }
        self.total_query_memory_mb / self.queries_completed
    }
}

/// Learned metrics for adaptive scaling (cluster-wide)
#[derive(Debug, Clone)]
pub struct AdaptiveMetrics {
    /// Average queries per worker (learned)
    pub avg_queries_per_worker: f64,
    /// Average worker memory capacity in MB (learned)
    pub avg_worker_memory_mb: u64,
    /// Average query memory usage in MB (learned)
    pub avg_query_memory_mb: u64,
    /// Average query wait time in ms (learned)
    pub avg_wait_time_ms: u64,
    /// Total queries completed (for learning)
    pub total_queries_completed: u64,
    /// Query wait time history for averaging
    pub wait_time_history: std::collections::VecDeque<u64>,
}

impl Default for AdaptiveMetrics {
    fn default() -> Self {
        Self {
            avg_queries_per_worker: DEFAULT_QUERIES_PER_WORKER,
            avg_worker_memory_mb: DEFAULT_WORKER_MEMORY_MB,
            avg_query_memory_mb: DEFAULT_WORKER_MEMORY_MB / 8,
            avg_wait_time_ms: 0,
            total_queries_completed: 0,
            wait_time_history: std::collections::VecDeque::new(),
        }
    }
}

impl AdaptiveMetrics {
    /// Record a query completion for learning
    pub fn record_query(&mut self, memory_mb: u64, wait_time_ms: u64) {
        self.total_queries_completed += 1;
        
        // Update rolling average for memory
        let alpha = 0.1; // Exponential moving average factor
        self.avg_query_memory_mb = (
            (1.0 - alpha) * self.avg_query_memory_mb as f64 + 
            alpha * memory_mb as f64
        ) as u64;
        
        // Track wait time
        self.wait_time_history.push_back(wait_time_ms);
        while self.wait_time_history.len() > HISTORY_WINDOW_SIZE {
            self.wait_time_history.pop_front();
        }
        
        // Update average wait time
        if !self.wait_time_history.is_empty() {
            let sum: u64 = self.wait_time_history.iter().sum();
            self.avg_wait_time_ms = sum / self.wait_time_history.len() as u64;
        }
    }
    
    /// Update learned queries per worker from cluster state
    pub fn update_from_workers(&mut self, workers: &HashMap<String, WorkerState>) {
        if workers.is_empty() {
            return;
        }
        
        let total_queries: usize = workers.values()
            .map(|w| w.running_queries.len())
            .sum();
        
        let active_workers = workers.values()
            .filter(|w| !w.running_queries.is_empty())
            .count();
        
        if active_workers > 0 {
            let observed = total_queries as f64 / active_workers as f64;
            // Exponential moving average
            let alpha = 0.1;
            self.avg_queries_per_worker = (1.0 - alpha) * self.avg_queries_per_worker + alpha * observed;
        }
        
        // Update average worker memory
        let total_memory: u64 = workers.values()
            .map(|w| w.memory_limit_mb)
            .sum();
        self.avg_worker_memory_mb = total_memory / workers.len() as u64;
    }
}

/// Query state tracked per worker
#[derive(Debug, Clone)]
pub struct QueryState {
    /// Unique query ID
    pub id: String,
    /// Estimated memory requirement in MB
    pub estimated_memory_mb: u64,
    /// Start time
    pub started_at: Instant,
    /// Pre-sized memory allocated for this query
    pub allocated_memory_mb: u64,
}

/// Scaling decision result
#[derive(Debug, Clone)]
pub enum ScalingDecision {
    /// No scaling needed
    NoAction,
    /// Scale up HPA by N workers
    ScaleUp { additional_workers: i32, reason: String },
    /// Scale down HPA by N workers
    ScaleDown { remove_workers: i32, reason: String },
    /// Resize a specific worker's memory
    ResizeWorker { worker_name: String, new_limit_mb: u64, reason: String },
}

/// Worker selection result
#[derive(Debug, Clone)]
pub struct WorkerSelection {
    /// Selected worker name
    pub worker_name: String,
    /// Worker address
    pub worker_address: String,
    /// Whether resize is needed before query
    pub needs_resize: bool,
    /// New memory limit if resize needed
    pub new_limit_mb: u64,
    /// Current memory limit
    pub current_limit_mb: u64,
}

/// Cluster capacity summary
#[derive(Debug, Clone)]
pub struct ClusterCapacity {
    /// Total memory limit across all workers
    pub total_limit_mb: u64,
    /// Total memory used across all workers
    pub total_used_mb: u64,
    /// Total available memory
    pub total_available_mb: u64,
    /// Number of workers
    pub worker_count: i32,
    /// Total running queries
    pub running_queries: u64,
    /// Total queued queries
    pub queued_queries: u64,
    /// Average worker utilization
    pub avg_utilization: f64,
    /// Queries per worker ratio
    pub queries_per_worker: f64,
}

// ═══════════════════════════════════════════════════════════════════════════
// SMART SCALER IMPLEMENTATION
// ═══════════════════════════════════════════════════════════════════════════

/// Smart Scaler - manages HPA and VPA decisions
pub struct SmartScaler {
    /// K8s client
    client: Client,
    /// Namespace
    namespace: String,
    /// Deployment name for workers
    deployment_name: String,
    /// Worker states
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    /// Queued queries (waiting for workers)
    queued_queries: Arc<RwLock<Vec<QueryState>>>,
    /// Last HPA scale time (for stabilization)
    last_scale_time: Arc<RwLock<Option<Instant>>>,
    /// Adaptive metrics for learning-based scaling
    adaptive_metrics: Arc<RwLock<AdaptiveMetrics>>,
}

impl SmartScaler {
    /// Create a new SmartScaler
    pub async fn new(namespace: &str, deployment_name: &str) -> Result<Self, kube::Error> {
        let client = Client::try_default().await?;
        
        let scaler = Self {
            client,
            namespace: namespace.to_string(),
            deployment_name: deployment_name.to_string(),
            workers: Arc::new(RwLock::new(HashMap::new())),
            queued_queries: Arc::new(RwLock::new(Vec::new())),
            last_scale_time: Arc::new(RwLock::new(None)),
            adaptive_metrics: Arc::new(RwLock::new(AdaptiveMetrics::default())),
        };
        
        // Initial worker discovery
        scaler.refresh_workers().await?;
        
        info!(
            "SmartScaler initialized (ADAPTIVE MODE): namespace={}, deployment={}",
            namespace, deployment_name
        );
        info!(
            "  Bounds: MIN_WORKERS={}, MAX_WORKERS={}, MAX_MEMORY_MB={}",
            MIN_WORKERS, MAX_WORKERS, MAX_MEMORY_MB
        );
        info!(
            "  Targets: wait_time={}ms, utilization={}%, idle_cpu={}%",
            TARGET_WAIT_TIME_MS, (TARGET_UTILIZATION * 100.0) as u32, (IDLE_CPU_THRESHOLD * 100.0) as u32
        );
        info!(
            "  Timeouts: idle_scaledown={}min, max_age={}h, monitor_interval={}ms",
            IDLE_DURATION_FOR_SCALEDOWN_SECS / 60, MAX_WORKER_AGE_SECS / 3600, MONITOR_INTERVAL_MS
        );
        
        Ok(scaler)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WORKER DISCOVERY & STATE MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    /// Refresh worker states from K8s
    pub async fn refresh_workers(&self) -> Result<(), kube::Error> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp = ListParams::default().labels(&format!("app=tavana-{}", self.deployment_name));
        
        let pod_list = pods.list(&lp).await?;
        
        // Fetch metrics for each pod
        let metrics = self.fetch_pod_metrics().await;
        
        let mut workers = self.workers.write().await;
        let mut discovered = HashMap::new();
        
        for pod in pod_list.items {
            let name = pod.metadata.name.clone().unwrap_or_default();
            let status = pod.status.as_ref();
            
            // Only include running pods
            if status.and_then(|s| s.phase.as_ref()).map(|p| p == "Running").unwrap_or(false) {
                let pod_ip = status.and_then(|s| s.pod_ip.clone()).unwrap_or_default();
                
                // Get resource limits from spec
                let (memory_limit, cpu_limit) = pod.spec.as_ref()
                    .and_then(|s| s.containers.first())
                    .and_then(|c| c.resources.as_ref())
                    .and_then(|r| r.limits.as_ref())
                    .map(|limits| {
                        let mem = parse_memory_quantity(limits.get("memory"));
                        let cpu = parse_cpu_quantity(limits.get("cpu"));
                        (mem, cpu)
                    })
                    .unwrap_or((MIN_MEMORY_MB, 1000));
                
                // Get current usage from metrics
                let (memory_used, cpu_used) = metrics.get(&name)
                    .cloned()
                    .unwrap_or((0, 0));
                
                // Preserve existing state if worker was known
                let existing = workers.get(&name);
                
                let running_queries = existing
                    .map(|w| w.running_queries.clone())
                    .unwrap_or_default();
                
                let last_activity = existing
                    .map(|w| w.last_activity)
                    .unwrap_or_else(Instant::now);
                
                // Preserve adaptive fields from existing worker
                let created_at = existing
                    .map(|w| w.created_at)
                    .unwrap_or_else(Instant::now);
                
                let idle_since = existing
                    .map(|w| w.idle_since)
                    .unwrap_or_else(|| {
                        if running_queries.is_empty() { Some(Instant::now()) } else { None }
                    });
                
                let cpu_history = existing
                    .map(|w| w.cpu_history.clone())
                    .unwrap_or_else(std::collections::VecDeque::new);
                
                let queries_completed = existing
                    .map(|w| w.queries_completed)
                    .unwrap_or(0);
                
                let total_query_memory_mb = existing
                    .map(|w| w.total_query_memory_mb)
                    .unwrap_or(0);
                
                discovered.insert(name.clone(), WorkerState {
                    name: name.clone(),
                    address: format!("http://{}:50053", pod_ip),
                    memory_limit_mb: memory_limit,
                    memory_used_mb: memory_used,
                    cpu_limit_m: cpu_limit,
                    cpu_used_m: cpu_used,
                    running_queries,
                    last_activity,
                    resize_in_progress: false,
                    // Adaptive fields
                    created_at,
                    idle_since,
                    cpu_history,
                    queries_completed,
                    total_query_memory_mb,
                });
            }
        }
        
        *workers = discovered;
        
        // Update metrics
        let capacity = self.calculate_capacity_internal(&workers).await;
        metrics::update_worker_pool_status(
            capacity.worker_count,
            capacity.running_queries as i32,
            (capacity.worker_count as u64 - capacity.running_queries.min(capacity.worker_count as u64)) as i32,
            0
        );
        
        debug!("Refreshed {} workers", workers.len());
        Ok(())
    }

    /// Fetch pod metrics from metrics server
    async fn fetch_pod_metrics(&self) -> HashMap<String, (u64, u64)> {
        let mut metrics = HashMap::new();
        
        // Try to fetch from metrics API using kube's dynamic API
        let api: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        
        // Use kubectl top pods equivalent - fetch from metrics.k8s.io
        let url = format!(
            "/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods",
            self.namespace
        );
        
        match self.client.request_text(
            http::Request::get(&url)
                .body(Default::default())
                .unwrap()
        ).await {
            Ok(response) => {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&response) {
                    if let Some(items) = parsed.get("items").and_then(|i| i.as_array()) {
                        for item in items {
                            let name = item.get("metadata")
                                .and_then(|m| m.get("name"))
                                .and_then(|n| n.as_str())
                                .unwrap_or("");
                            
                            if let Some(containers) = item.get("containers").and_then(|c| c.as_array()) {
                                for container in containers {
                                    let memory = container.get("usage")
                                        .and_then(|u| u.get("memory"))
                                        .and_then(|m| m.as_str())
                                        .map(|s| parse_k8s_memory(s))
                                        .unwrap_or(0);
                                    
                                    let cpu = container.get("usage")
                                        .and_then(|u| u.get("cpu"))
                                        .and_then(|c| c.as_str())
                                        .map(|s| parse_k8s_cpu(s))
                                        .unwrap_or(0);
                                    
                                    metrics.insert(name.to_string(), (memory, cpu));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Failed to fetch metrics: {}", e);
            }
        }
        
        // Fallback: use resource usage from pod container status if metrics server unavailable
        if metrics.is_empty() {
            if let Ok(pods) = api.list(&ListParams::default().labels(&format!("app=tavana-{}", self.deployment_name))).await {
                for pod in pods.items {
                    let name = pod.metadata.name.clone().unwrap_or_default();
                    // Default to minimal usage if metrics unavailable
                    metrics.insert(name, (MIN_MEMORY_MB, 100));
                }
            }
        }
        
        metrics
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CAPACITY CALCULATION
    // ═══════════════════════════════════════════════════════════════════════

    /// Calculate current cluster capacity
    pub async fn calculate_capacity(&self) -> ClusterCapacity {
        let workers = self.workers.read().await;
        self.calculate_capacity_internal(&workers).await
    }

    async fn calculate_capacity_internal(&self, workers: &HashMap<String, WorkerState>) -> ClusterCapacity {
        let queued = self.queued_queries.read().await;
        
        let mut total_limit: u64 = 0;
        let mut total_used: u64 = 0;
        let mut running_queries: u64 = 0;
        
        for worker in workers.values() {
            total_limit += worker.memory_limit_mb;
            total_used += worker.memory_used_mb;
            running_queries += worker.running_queries.len() as u64;
        }
        
        let worker_count = workers.len() as i32;
        let queued_queries = queued.len() as u64;
        let total_available = total_limit.saturating_sub(total_used);
        
        let avg_utilization = if total_limit > 0 {
            total_used as f64 / total_limit as f64
        } else {
            0.0
        };
        
        let queries_per_worker = if worker_count > 0 {
            (running_queries + queued_queries) as f64 / worker_count as f64
        } else {
            0.0
        };
        
        ClusterCapacity {
            total_limit_mb: total_limit,
            total_used_mb: total_used,
            total_available_mb: total_available,
            worker_count,
            running_queries,
            queued_queries,
            avg_utilization,
            queries_per_worker,
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HPA SCALING WITH QUERY QUEUE SIGNALS (VPA-first, HPA-second)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Evaluate HPA scaling using QueryQueue signals
    /// 
    /// Priority order:
    /// 1. PROACTIVE REQUEST: Queue explicitly asked for more workers
    /// 2. HPA SIGNAL: Queue is backing up
    /// 3. QUEUE DEPTH: Too many queries waiting
    /// 4. WAIT TIME: Queries waiting too long
    /// 5. SCALE-DOWN: Only when truly idle
    pub async fn evaluate_hpa_with_queue(&self, queue: &QueryQueue) -> ScalingDecision {
        use crate::query_queue::OperationMode;
        
        let workers = self.workers.read().await;
        let current_workers = workers.len() as i32;
        
        if current_workers == 0 {
            return ScalingDecision::ScaleUp {
                additional_workers: MIN_WORKERS,
                reason: "No workers available".to_string(),
            };
        }
        
        let stats = queue.stats().await;
        let queue_depth = stats.queue_depth;
        let avg_wait_ms = stats.avg_queue_wait_ms;
        let mode = stats.mode;
        let utilization = if stats.total_capacity_mb > 0 {
            stats.current_usage_mb as f64 / stats.total_capacity_mb as f64
        } else {
            0.0
        };
        
        // In SATURATION MODE: No scale-up possible (at ceiling)
        if mode == OperationMode::Saturation {
            debug!(
                mode = ?mode,
                ceiling_mb = stats.ceiling_mb,
                total_mb = stats.total_capacity_mb,
                "Saturation mode: no HPA scale-up possible"
            );
            // Still check for scale-down of idle workers
            return self.evaluate_scale_down(&workers, current_workers).await;
        }
        
        // ═══════════════════════════════════════════════════════════════════
        // SCALE-UP TRIGGERS (SCALING MODE only)
        // ═══════════════════════════════════════════════════════════════════
        
        // Trigger 0 (HIGHEST PRIORITY): Proactive HPA request from queue
        if let Some(requested_workers) = queue.take_hpa_request() {
            let additional = (requested_workers as i32).min(MAX_WORKERS - current_workers);
            
            if additional > 0 {
                info!(
                    requested = requested_workers,
                    additional = additional,
                    current = current_workers,
                    "Proactive HPA scale-up from queue"
                );
                return ScalingDecision::ScaleUp {
                    additional_workers: additional,
                    reason: format!(
                        "Proactive: queue requested {} workers (depth={}, wait={:.0}ms)",
                        requested_workers, queue_depth, avg_wait_ms
                    ),
                };
            }
        }
        
        // Trigger 1: HPA signal flag from queue (queue backing up)
        if stats.hpa_scale_up_signal {
            let additional = ((current_workers as f64 * 1.5) as i32 - current_workers).max(1);
            let additional = additional.min(MAX_WORKERS - current_workers);
            
            if additional > 0 {
                return ScalingDecision::ScaleUp {
                    additional_workers: additional,
                    reason: format!(
                        "Queue backing up: {} waiting, avg_wait={:.0}ms",
                        queue_depth,
                        avg_wait_ms
                    ),
                };
            }
        }
        
        // Trigger 2: Queue depth > 5 queries → scale up (more aggressive)
        if queue_depth > 5 {
            // 1 new worker per 5 queued queries
            let additional = ((queue_depth as f64 / 5.0).ceil() as i32).min(MAX_WORKERS - current_workers);
            
            if additional > 0 {
                return ScalingDecision::ScaleUp {
                    additional_workers: additional,
                    reason: format!(
                        "Queue depth: {} queries waiting (adding {} workers)",
                        queue_depth, additional
                    ),
                };
            }
        }
        
        // Trigger 3: High average wait time (> 5s) → scale up
        if avg_wait_ms > 5000.0 {
            let additional = 1.min(MAX_WORKERS - current_workers);
            
            if additional > 0 {
                return ScalingDecision::ScaleUp {
                    additional_workers: additional,
                    reason: format!(
                        "High wait time: {:.0}ms average",
                        avg_wait_ms
                    ),
                };
            }
        }
        
        // Trigger 4: High capacity utilization (> 80%) → proactive scale up
        if utilization > 0.8 {
            let additional = ((current_workers as f64 * 1.5) as i32 - current_workers).max(1);
            let additional = additional.min(MAX_WORKERS - current_workers);
            
            if additional > 0 {
                return ScalingDecision::ScaleUp {
                    additional_workers: additional,
                    reason: format!(
                        "High capacity utilization: {:.1}% ({}MB/{}MB)",
                        utilization * 100.0,
                        stats.current_usage_mb,
                        stats.total_capacity_mb
                    ),
                };
            }
        }
        
        // ═══════════════════════════════════════════════════════════════════
        // SCALE-DOWN (only when truly idle)
        // ═══════════════════════════════════════════════════════════════════
        
        // Scale down if: empty queue AND low utilization AND have spare workers
        if queue_depth == 0 && stats.active_queries == 0 && utilization < 0.3 && current_workers > MIN_WORKERS {
            // Find truly idle workers
            let idle_workers: Vec<_> = workers.values()
                .filter(|w| w.is_truly_idle() || w.is_too_old())
                .collect();
            
            if !idle_workers.is_empty() {
                return ScalingDecision::ScaleDown {
                    remove_workers: 1,
                    reason: format!(
                        "Queue empty, {} idle workers available",
                        idle_workers.len()
                    ),
                };
            }
        }
        
        ScalingDecision::NoAction
    }
    
    /// Helper: Evaluate scale-down only (for saturation mode)
    async fn evaluate_scale_down(
        &self,
        workers: &std::collections::HashMap<String, WorkerState>,
        current_workers: i32,
    ) -> ScalingDecision {
        if current_workers <= MIN_WORKERS {
            return ScalingDecision::NoAction;
        }
        
        // Find truly idle or too-old workers
        let removable: Vec<_> = workers.values()
            .filter(|w| w.is_truly_idle() || w.is_too_old())
            .collect();
        
        if !removable.is_empty() {
            let first = removable.first().unwrap();
            let reason = if first.is_too_old() {
                format!(
                    "Force recycle: {} age={}h (max={}h)",
                    first.name,
                    first.age_secs() / 3600,
                    MAX_WORKER_AGE_SECS / 3600
                )
            } else {
                format!(
                    "Truly idle: {} idle={}min, cpu={:.1}%",
                    first.name,
                    first.idle_duration_secs().unwrap_or(0) / 60,
                    first.avg_cpu_utilization() * 100.0
                )
            };
            
            return ScalingDecision::ScaleDown {
                remove_workers: 1,
                reason,
            };
        }
        
        ScalingDecision::NoAction
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HPA SCALING DECISIONS (Legacy - without AdaptiveQueue)
    // ═══════════════════════════════════════════════════════════════════════

    /// Evaluate HPA scaling using ADAPTIVE FORMULA (legacy method)
    /// 
    /// SCALE-UP FORMULA (no hardcoded thresholds):
    ///   desired_workers = max(
    ///     demand_factor:  (running + queued) / learned_queries_per_worker,
    ///     memory_factor:  pending_memory / (learned_worker_memory * 0.8),
    ///     latency_factor: current * (1 + (avg_wait / target_wait - 1).max(0)),
    ///     MIN_WORKERS
    ///   )
    ///
    /// SCALE-DOWN (idleness-based, not resource-based):
    ///   - Worker truly idle (0 queries + CPU < 2%) for 30 min
    ///   - Worker age > 5 hours (force recycle)
    pub async fn evaluate_hpa_scaling(&self, pending_demand_mb: u64) -> ScalingDecision {
        let workers = self.workers.read().await;
        let metrics = self.adaptive_metrics.read().await;
        let capacity = self.calculate_capacity_internal(&workers).await;
        
        let current_workers = capacity.worker_count;
        let total_queries = capacity.running_queries + capacity.queued_queries;
        
        // ═══════════════════════════════════════════════════════════════════
        // ADAPTIVE SCALE-UP FORMULA
        // ═══════════════════════════════════════════════════════════════════
        
        // Factor 1: Demand-based (queries per worker)
        let demand_factor = if metrics.avg_queries_per_worker > 0.0 {
            (total_queries as f64 / metrics.avg_queries_per_worker).ceil() as i32
        } else {
            current_workers
        };
        
        // Factor 2: Memory-based (pending memory vs capacity)
        let effective_worker_memory = (metrics.avg_worker_memory_mb as f64 * TARGET_UTILIZATION) as u64;
        let memory_factor = if effective_worker_memory > 0 {
            let pending_with_safety = (pending_demand_mb as f64 * ESTIMATION_SAFETY) as u64;
            (pending_with_safety as f64 / effective_worker_memory as f64).ceil() as i32
        } else {
            current_workers
        };
        
        // Factor 3: Latency-based (wait time exceeds target)
        let latency_factor = if metrics.avg_wait_time_ms > TARGET_WAIT_TIME_MS {
            // Scale proportionally to how much we exceed target
            let ratio = metrics.avg_wait_time_ms as f64 / TARGET_WAIT_TIME_MS as f64;
            (current_workers as f64 * ratio).ceil() as i32
        } else {
            current_workers
        };
        
        // Desired = max of all factors, but at least MIN_WORKERS
        let desired_workers = demand_factor
            .max(memory_factor)
            .max(latency_factor)
            .max(MIN_WORKERS)
            .min(MAX_WORKERS);
        
        // Scale up if desired > current
        if desired_workers > current_workers {
            let additional = desired_workers - current_workers;
            let reason = format!(
                "Adaptive scale-up: desired={} (demand={}, memory={}, latency={}), current={}",
                desired_workers, demand_factor, memory_factor, latency_factor, current_workers
            );
            
            info!("{} - adding {} workers", reason, additional);
            
            return ScalingDecision::ScaleUp {
                additional_workers: additional,
                reason,
            };
        }
        
        // ═══════════════════════════════════════════════════════════════════
        // ADAPTIVE SCALE-DOWN (idleness-based, NOT resource-based)
        // ═══════════════════════════════════════════════════════════════════
        
        // Find workers eligible for removal
        let removable_workers: Vec<&WorkerState> = workers.values()
            .filter(|w| {
                // Criterion 1: Worker is truly idle (no queries + CPU < 2% for 30 min)
                let truly_idle = w.is_truly_idle();
                
                // Criterion 2: Worker is too old (> 5 hours)
                let too_old = w.is_too_old();
                
                truly_idle || too_old
            })
            .collect();
        
        // Only remove if we'll stay above MIN_WORKERS
        let can_remove = current_workers - MIN_WORKERS;
        let to_remove = (removable_workers.len() as i32).min(can_remove).min(1); // Max 1 at a time
        
        if to_remove > 0 {
            let first_removable = removable_workers.first().unwrap();
            let reason = if first_removable.is_too_old() {
                format!(
                    "Force recycle: worker {} age={}h (max={}h)",
                    first_removable.name,
                    first_removable.age_secs() / 3600,
                    MAX_WORKER_AGE_SECS / 3600
                )
            } else {
                format!(
                    "Truly idle: worker {} idle={}min, cpu_avg={:.1}%",
                    first_removable.name,
                    first_removable.idle_duration_secs().unwrap_or(0) / 60,
                    first_removable.avg_cpu_utilization() * 100.0
                )
            };
            
            info!("{} - removing 1 worker", reason);
            
            return ScalingDecision::ScaleDown {
                remove_workers: 1,
                reason,
            };
        }
        
        ScalingDecision::NoAction
    }

    /// Apply HPA scaling decision
    pub async fn apply_hpa_scaling(&self, decision: &ScalingDecision) -> Result<(), anyhow::Error> {
        match decision {
            ScalingDecision::ScaleUp { additional_workers, reason } => {
                let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
                
                // Get current replicas
                let deploy = deployments.get(&self.deployment_name).await?;
                let current = deploy.spec.and_then(|s| s.replicas).unwrap_or(MIN_WORKERS);
                let new_replicas = (current + additional_workers).min(MAX_WORKERS);
                
                // Patch deployment
                let patch = json!({
                    "spec": {
                        "replicas": new_replicas
                    }
                });
                
                deployments.patch(
                    &self.deployment_name,
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                ).await?;
                
                info!("HPA scaled up: {} -> {} workers. Reason: {}", current, new_replicas, reason);
                metrics::record_inplace_resize("hpa_scale_up", *additional_workers as f64);
                
                *self.last_scale_time.write().await = Some(Instant::now());
            }
            ScalingDecision::ScaleDown { remove_workers, reason } => {
                let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
                
                let deploy = deployments.get(&self.deployment_name).await?;
                let current = deploy.spec.and_then(|s| s.replicas).unwrap_or(MIN_WORKERS);
                let new_replicas = (current - remove_workers).max(MIN_WORKERS);
                
                let patch = json!({
                    "spec": {
                        "replicas": new_replicas
                    }
                });
                
                deployments.patch(
                    &self.deployment_name,
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                ).await?;
                
                info!("HPA scaled down: {} -> {} workers. Reason: {}", current, new_replicas, reason);
                metrics::record_inplace_resize("hpa_scale_down", -(*remove_workers as f64));
                
                *self.last_scale_time.write().await = Some(Instant::now());
            }
            _ => {}
        }
        
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WORKER SELECTION & PRE-ASSIGNMENT VPA
    // ═══════════════════════════════════════════════════════════════════════

    /// Select best worker for a query and determine if resize needed
    pub async fn select_worker(&self, query_id: &str, estimated_memory_mb: u64) -> Option<WorkerSelection> {
        // Refresh worker states first
        if let Err(e) = self.refresh_workers().await {
            warn!("Failed to refresh workers: {}", e);
        }
        
        let workers = self.workers.read().await;
        
        if workers.is_empty() {
            warn!("No workers available");
            return None;
        }
        
        let required_with_safety = (estimated_memory_mb as f64 * ESTIMATION_SAFETY) as u64;
        
        // Find best fit worker
        // Priority: 1) Can accept without resize, 2) Least utilized, 3) Fewest queries
        let mut candidates: Vec<_> = workers.values()
            .filter(|w| !w.resize_in_progress)
            .collect();
        
        // Sort by: can accept > utilization > query count
        candidates.sort_by(|a, b| {
            let a_can_accept = a.can_accept_query(estimated_memory_mb);
            let b_can_accept = b.can_accept_query(estimated_memory_mb);
            
            if a_can_accept != b_can_accept {
                return b_can_accept.cmp(&a_can_accept); // Prefer can accept
            }
            
            let util_cmp = a.memory_utilization().partial_cmp(&b.memory_utilization())
                .unwrap_or(std::cmp::Ordering::Equal);
            if util_cmp != std::cmp::Ordering::Equal {
                return util_cmp; // Prefer lower utilization
            }
            
            a.running_queries.len().cmp(&b.running_queries.len()) // Prefer fewer queries
        });
        
        let selected = candidates.first()?;
        
        // Determine if resize is needed
        let needs_resize = !selected.can_accept_query(estimated_memory_mb);
        let new_limit_mb = if needs_resize {
            selected.required_limit_for_query(estimated_memory_mb).min(MAX_MEMORY_MB)
        } else {
            selected.memory_limit_mb
        };
        
        info!(
            "Selected worker {} for query {}: needs_resize={}, limit={}MB->{}MB, estimated={}MB",
            selected.name, query_id, needs_resize, selected.memory_limit_mb, new_limit_mb, estimated_memory_mb
        );
        
        Some(WorkerSelection {
            worker_name: selected.name.clone(),
            worker_address: selected.address.clone(),
            needs_resize,
            new_limit_mb,
            current_limit_mb: selected.memory_limit_mb,
        })
    }

    /// Pre-size worker before assigning query (VPA pre-assignment)
    pub async fn presize_worker(&self, worker_name: &str, new_limit_mb: u64) -> Result<bool, anyhow::Error> {
        if new_limit_mb > MAX_MEMORY_MB {
            warn!("Requested limit {}MB exceeds max {}MB", new_limit_mb, MAX_MEMORY_MB);
            return Ok(false);
        }
        
        // Mark resize in progress
        {
            let mut workers = self.workers.write().await;
            if let Some(worker) = workers.get_mut(worker_name) {
                if worker.resize_in_progress {
                    debug!("Resize already in progress for {}", worker_name);
                    return Ok(false);
                }
                worker.resize_in_progress = true;
            }
        }
        
        let result = self.resize_worker_internal(worker_name, new_limit_mb, "pre-assignment").await;
        
        // Clear resize in progress
        {
            let mut workers = self.workers.write().await;
            if let Some(worker) = workers.get_mut(worker_name) {
                worker.resize_in_progress = false;
                if result.is_ok() {
                    worker.memory_limit_mb = new_limit_mb;
                }
            }
        }
        
        result
    }

    /// Internal resize implementation using K8s in-place resize
    async fn resize_worker_internal(&self, pod_name: &str, new_limit_mb: u64, reason: &str) -> Result<bool, anyhow::Error> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        
        let start = Instant::now();
        
        // Use strategic merge patch for in-place resize
        let patch = json!({
            "spec": {
                "containers": [{
                    "name": "worker",
                    "resources": {
                        "limits": {
                            "memory": format!("{}Mi", new_limit_mb)
                        },
                        "requests": {
                            "memory": format!("{}Mi", new_limit_mb.min(MIN_MEMORY_MB * 2))
                        }
                    }
                }]
            }
        });
        
        match pods.patch_subresource(
            pod_name,
            "resize",
            &PatchParams::apply("smart-scaler").force(),
            &Patch::Apply(&patch),
        ).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                info!(
                    "Resized worker {} to {}MB ({}) in {:?}",
                    pod_name, new_limit_mb, reason, elapsed
                );
                metrics::record_worker_presize("success", new_limit_mb as f64, 0.0, elapsed.as_secs_f64());
                Ok(true)
            }
            Err(e) => {
                warn!("Failed to resize worker {}: {}", pod_name, e);
                metrics::record_worker_presize("failed", new_limit_mb as f64, 0.0, start.elapsed().as_secs_f64());
                Err(anyhow::anyhow!("Resize failed: {}", e))
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // QUERY LIFECYCLE MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    /// Register a query starting on a worker
    pub async fn query_started(&self, worker_name: &str, query_id: &str, estimated_memory_mb: u64, allocated_mb: u64) {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get_mut(worker_name) {
            worker.running_queries.push(QueryState {
                id: query_id.to_string(),
                estimated_memory_mb,
                started_at: Instant::now(),
                allocated_memory_mb: allocated_mb,
            });
            worker.last_activity = Instant::now();
            
            debug!("Query {} started on worker {}, running: {}", 
                query_id, worker_name, worker.running_queries.len());
        }
        
        metrics::query_started();
    }

    /// Register a query completed on a worker
    pub async fn query_completed(&self, worker_name: &str, query_id: &str) {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get_mut(worker_name) {
            worker.running_queries.retain(|q| q.id != query_id);
            worker.last_activity = Instant::now();
            
            debug!("Query {} completed on worker {}, remaining: {}",
                query_id, worker_name, worker.running_queries.len());
        }
        
        metrics::query_ended();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ELASTIC VPA MONITORING
    // ═══════════════════════════════════════════════════════════════════════

    /// Check if worker needs elastic resize and apply if needed
    pub async fn elastic_check(&self, worker_name: &str) -> Result<bool, anyhow::Error> {
        // Refresh metrics for this worker
        let metrics = self.fetch_pod_metrics().await;
        
        let (needs_resize, new_limit) = {
            let mut workers = self.workers.write().await;
            
            let worker = match workers.get_mut(worker_name) {
                Some(w) => w,
                None => return Ok(false),
            };
            
            // Update metrics
            if let Some((mem, cpu)) = metrics.get(worker_name) {
                worker.memory_used_mb = *mem;
                worker.cpu_used_m = *cpu;
            }
            
            let utilization = worker.memory_utilization();
            
            if utilization > ELASTIC_THRESHOLD && !worker.resize_in_progress {
                let new_limit = (worker.memory_limit_mb as f64 * ELASTIC_GROWTH) as u64;
                let capped_limit = new_limit.min(MAX_MEMORY_MB);
                
                if capped_limit > worker.memory_limit_mb {
                    info!(
                        "Elastic resize triggered for {}: utilization={:.1}%, limit={}MB->{}MB",
                        worker_name, utilization * 100.0, worker.memory_limit_mb, capped_limit
                    );
                    (true, capped_limit)
                } else {
                    debug!("Worker {} at max capacity, cannot grow further", worker_name);
                    (false, 0)
                }
            } else {
                (false, 0)
            }
        };
        
        if needs_resize {
            metrics::record_elastic_scaleup("memory_pressure");
            return self.resize_worker_internal(worker_name, new_limit, "elastic").await;
        }
        
        Ok(false)
    }

    /// Start elastic monitoring for a worker (runs in background)
    pub fn start_elastic_monitoring(&self, worker_name: String) -> tokio::task::JoinHandle<()> {
        let scaler = self.clone_arc();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(ELASTIC_CHECK_INTERVAL_MS)).await;
                
                // Check if query is still running
                let still_running = {
                    let workers = scaler.workers.read().await;
                    workers.get(&worker_name)
                        .map(|w| !w.running_queries.is_empty())
                        .unwrap_or(false)
                };
                
                if !still_running {
                    debug!("Stopping elastic monitoring for {} - no queries", worker_name);
                    break;
                }
                
                if let Err(e) = scaler.elastic_check(&worker_name).await {
                    warn!("Elastic check failed for {}: {}", worker_name, e);
                }
            }
        })
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SMART SHRINK
    // ═══════════════════════════════════════════════════════════════════════

    /// Shrink idle workers back to minimum
    pub async fn shrink_idle_workers(&self) -> Result<u32, anyhow::Error> {
        let now = Instant::now();
        let idle_threshold = Duration::from_secs(SHRINK_IDLE_SECONDS);
        
        let workers_to_shrink: Vec<(String, u64)> = {
            let workers = self.workers.read().await;
            
            workers.values()
                .filter(|w| {
                    w.running_queries.is_empty() &&
                    now.duration_since(w.last_activity) > idle_threshold &&
                    w.memory_limit_mb > MIN_MEMORY_MB &&
                    !w.resize_in_progress
                })
                .map(|w| {
                    // Shrink to max of MIN_MEMORY or current usage + 20%
                    let target = (w.memory_used_mb as f64 * 1.2) as u64;
                    let new_limit = target.max(MIN_MEMORY_MB);
                    (w.name.clone(), new_limit)
                })
                .collect()
        };
        
        let mut shrunk = 0;
        for (name, new_limit) in workers_to_shrink {
            match self.resize_worker_internal(&name, new_limit, "shrink-idle").await {
                Ok(true) => {
                    shrunk += 1;
                    metrics::record_inplace_resize("shrink", -(new_limit as f64));
                }
                Ok(false) => {}
                Err(e) => warn!("Failed to shrink worker {}: {}", name, e),
            }
        }
        
        if shrunk > 0 {
            info!("Shrunk {} idle workers", shrunk);
        }
        
        Ok(shrunk)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // QUEUE MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    /// Add query to queue
    pub async fn enqueue_query(&self, query_id: &str, estimated_memory_mb: u64) {
        let mut queued = self.queued_queries.write().await;
        
        queued.push(QueryState {
            id: query_id.to_string(),
            estimated_memory_mb,
            started_at: Instant::now(),
            allocated_memory_mb: 0,
        });
        
        metrics::record_queue_depth(queued.len());
        debug!("Query {} queued, queue depth: {}", query_id, queued.len());
    }

    /// Remove query from queue
    pub async fn dequeue_query(&self, query_id: &str) -> Option<QueryState> {
        let mut queued = self.queued_queries.write().await;
        
        let idx = queued.iter().position(|q| q.id == query_id)?;
        let query = queued.remove(idx);
        
        metrics::record_queue_depth(queued.len());
        metrics::record_queue_wait_time(query.started_at.elapsed().as_secs_f64());
        
        Some(query)
    }

    /// Get total pending demand (running + queued)
    pub async fn get_pending_demand_mb(&self) -> u64 {
        let workers = self.workers.read().await;
        let queued = self.queued_queries.read().await;
        
        let running_demand: u64 = workers.values()
            .flat_map(|w| w.running_queries.iter())
            .map(|q| q.estimated_memory_mb)
            .sum();
        
        let queued_demand: u64 = queued.iter()
            .map(|q| q.estimated_memory_mb)
            .sum();
        
        running_demand + queued_demand
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    /// Clone self as Arc for async tasks
    fn clone_arc(&self) -> Arc<SmartScaler> {
        // This is a workaround - in production you'd use Arc<SmartScaler> from the start
        // For now, we create a new instance that shares the same state
        Arc::new(SmartScaler {
            client: self.client.clone(),
            namespace: self.namespace.clone(),
            deployment_name: self.deployment_name.clone(),
            workers: self.workers.clone(),
            queued_queries: self.queued_queries.clone(),
            last_scale_time: self.last_scale_time.clone(),
            adaptive_metrics: self.adaptive_metrics.clone(),
        })
    }

    /// Get worker stats for debugging
    pub async fn get_worker_stats(&self) -> Vec<WorkerState> {
        let workers = self.workers.read().await;
        workers.values().cloned().collect()
    }
    
    // ═══════════════════════════════════════════════════════════════════════
    // ADAPTIVE MONITORING LOOP (runs every 1 second)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Start the adaptive monitoring background task with QueryQueue integration
    /// This is the preferred method - uses queue signals for HPA decisions
    pub fn start_monitoring_with_queue(
        self: Arc<Self>,
        query_queue: Arc<QueryQueue>,
    ) -> tokio::task::JoinHandle<()> {
        let scaler = self;
        
        tokio::spawn(async move {
            info!("Starting SmartScaler monitoring with QueryQueue integration (interval={}ms)", MONITOR_INTERVAL_MS);
            
            let mut last_hpa_check = Instant::now();
            let hpa_interval = Duration::from_secs(10); // Check HPA every 10 seconds
            
            loop {
                tokio::time::sleep(Duration::from_millis(MONITOR_INTERVAL_MS)).await;
                
                // Step 1: Refresh worker states and metrics
                if let Err(e) = scaler.refresh_workers().await {
                    warn!("Failed to refresh workers: {}", e);
                    continue;
                }
                
                // Step 2: Record CPU samples for each worker
                scaler.record_cpu_samples().await;
                
                // Step 3: Update adaptive metrics from cluster state
                scaler.update_adaptive_metrics().await;
                
                // Step 4: Evaluate HPA scaling using QueryQueue signals (every 10s)
                if last_hpa_check.elapsed() > hpa_interval {
                    let decision = scaler.evaluate_hpa_with_queue(&query_queue).await;
                    
                    if !matches!(decision, ScalingDecision::NoAction) {
                        info!("HPA decision from QueryQueue signals: {:?}", decision);
                        if let Err(e) = scaler.apply_hpa_scaling(&decision).await {
                            warn!("Failed to apply HPA scaling: {}", e);
                        }
                    }
                    
                    last_hpa_check = Instant::now();
                }
                
                // Step 5: Shrink idle worker memory (VPA) - temporarily disabled for stability
                // if let Err(e) = scaler.shrink_idle_workers().await {
                //     debug!("Shrink check: {}", e);
                // }
            }
        })
    }
    
    /// Start the adaptive monitoring background task (legacy, without QueryQueue)
    pub fn start_adaptive_monitoring(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let scaler = self;
        
        tokio::spawn(async move {
            info!("Starting adaptive monitoring loop (interval={}ms)", MONITOR_INTERVAL_MS);
            
            loop {
                tokio::time::sleep(Duration::from_millis(MONITOR_INTERVAL_MS)).await;
                
                // Step 1: Refresh worker states and metrics
                if let Err(e) = scaler.refresh_workers().await {
                    warn!("Failed to refresh workers: {}", e);
                    continue;
                }
                
                // Step 2: Record CPU samples for each worker
                scaler.record_cpu_samples().await;
                
                // Step 3: Update adaptive metrics from cluster state
                scaler.update_adaptive_metrics().await;
                
                // Step 4: Check for idle workers to scale down
                if let Err(e) = scaler.check_adaptive_scaledown().await {
                    warn!("Failed to check scale-down: {}", e);
                }
                
                // Step 5: Shrink idle worker memory (VPA) - temporarily disabled
                // if let Err(e) = scaler.shrink_idle_workers().await {
                //     debug!("Shrink check: {}", e);
                // }
            }
        })
    }
    
    /// Record CPU sample for all workers
    async fn record_cpu_samples(&self) {
        let metrics = self.fetch_pod_metrics().await;
        let mut workers = self.workers.write().await;
        
        for (name, (mem, cpu)) in metrics {
            if let Some(worker) = workers.get_mut(&name) {
                worker.memory_used_mb = mem;
                worker.cpu_used_m = cpu;
                worker.record_cpu_sample();
                
                // Update idle tracking
                if worker.running_queries.is_empty() {
                    if worker.idle_since.is_none() {
                        worker.idle_since = Some(Instant::now());
                    }
                } else {
                    worker.idle_since = None;
                }
            }
        }
    }
    
    /// Update adaptive metrics from cluster state
    async fn update_adaptive_metrics(&self) {
        let workers = self.workers.read().await;
        let mut metrics = self.adaptive_metrics.write().await;
        metrics.update_from_workers(&workers);
    }
    
    /// Check for workers eligible for adaptive scale-down
    async fn check_adaptive_scaledown(&self) -> Result<(), anyhow::Error> {
        let decision = self.evaluate_hpa_scaling(0).await;
        
        if let ScalingDecision::ScaleDown { remove_workers, ref reason } = decision {
            info!("Adaptive scale-down triggered: {} - removing {} workers", reason, remove_workers);
            self.apply_hpa_scaling(&decision).await?;
        }
        
        Ok(())
    }
    
    /// Record a completed query for adaptive learning
    pub async fn record_query_completion(&self, memory_mb: u64, wait_time_ms: u64, worker_name: &str) {
        // Update adaptive metrics
        {
            let mut metrics = self.adaptive_metrics.write().await;
            metrics.record_query(memory_mb, wait_time_ms);
        }
        
        // Update worker stats
        {
            let mut workers = self.workers.write().await;
            if let Some(worker) = workers.get_mut(worker_name) {
                worker.queries_completed += 1;
                worker.total_query_memory_mb += memory_mb;
            }
        }
    }
    
    /// Get current adaptive metrics for debugging
    pub async fn get_adaptive_metrics(&self) -> AdaptiveMetrics {
        self.adaptive_metrics.read().await.clone()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Parse K8s memory quantity (e.g., "1Gi", "512Mi", "1000000Ki")
fn parse_memory_quantity(quantity: Option<&Quantity>) -> u64 {
    quantity
        .map(|q| parse_k8s_memory(&q.0))
        .unwrap_or(MIN_MEMORY_MB)
}

/// Parse K8s CPU quantity (e.g., "1", "500m", "2000m")
fn parse_cpu_quantity(quantity: Option<&Quantity>) -> u64 {
    quantity
        .map(|q| parse_k8s_cpu(&q.0))
        .unwrap_or(1000)
}

/// Parse K8s memory string to MB
fn parse_k8s_memory(s: &str) -> u64 {
    let s = s.trim();
    
    if s.ends_with("Gi") {
        s.trim_end_matches("Gi").parse::<u64>().unwrap_or(0) * 1024
    } else if s.ends_with("Mi") {
        s.trim_end_matches("Mi").parse::<u64>().unwrap_or(0)
    } else if s.ends_with("Ki") {
        s.trim_end_matches("Ki").parse::<u64>().unwrap_or(0) / 1024
    } else if s.ends_with("G") {
        s.trim_end_matches("G").parse::<u64>().unwrap_or(0) * 1024
    } else if s.ends_with("M") {
        s.trim_end_matches("M").parse::<u64>().unwrap_or(0)
    } else if s.ends_with("K") {
        s.trim_end_matches("K").parse::<u64>().unwrap_or(0) / 1024
    } else {
        // Assume bytes
        s.parse::<u64>().unwrap_or(0) / (1024 * 1024)
    }
}

/// Parse K8s CPU string to millicores
fn parse_k8s_cpu(s: &str) -> u64 {
    let s = s.trim();
    
    if s.ends_with("m") {
        s.trim_end_matches("m").parse::<u64>().unwrap_or(0)
    } else if s.ends_with("n") {
        s.trim_end_matches("n").parse::<u64>().unwrap_or(0) / 1_000_000
    } else {
        // Assume cores
        (s.parse::<f64>().unwrap_or(0.0) * 1000.0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_k8s_memory() {
        assert_eq!(parse_k8s_memory("1Gi"), 1024);
        assert_eq!(parse_k8s_memory("512Mi"), 512);
        assert_eq!(parse_k8s_memory("256Mi"), 256);
        assert_eq!(parse_k8s_memory("2Gi"), 2048);
        assert_eq!(parse_k8s_memory("100G"), 100 * 1024);
    }

    #[test]
    fn test_parse_k8s_cpu() {
        assert_eq!(parse_k8s_cpu("1"), 1000);
        assert_eq!(parse_k8s_cpu("500m"), 500);
        assert_eq!(parse_k8s_cpu("2"), 2000);
        assert_eq!(parse_k8s_cpu("100m"), 100);
    }

    #[test]
    fn test_worker_state_calculations() {
        let worker = WorkerState {
            name: "test".to_string(),
            address: "http://test:50053".to_string(),
            memory_limit_mb: 1024,
            memory_used_mb: 512,
            cpu_limit_m: 2000,
            cpu_used_m: 500,
            running_queries: vec![],
            last_activity: Instant::now(),
            resize_in_progress: false,
            // Adaptive fields
            created_at: Instant::now(),
            idle_since: Some(Instant::now()),
            cpu_history: std::collections::VecDeque::new(),
            queries_completed: 0,
            total_query_memory_mb: 0,
        };

        assert_eq!(worker.available_memory_mb(), 512);
        assert!((worker.memory_utilization() - 0.5).abs() < 0.01);
        assert_eq!(worker.effective_capacity_mb(), 819); // 1024 * 0.8
        
        // Can accept 300MB query: available=512, required=300*1.3=390
        assert!(worker.can_accept_query(300));
        
        // Cannot accept 500MB query: available=512, required=500*1.3=650
        assert!(!worker.can_accept_query(500));
    }
    
    #[test]
    fn test_adaptive_metrics() {
        let mut metrics = AdaptiveMetrics::default();
        
        // Initial values
        assert_eq!(metrics.avg_queries_per_worker, DEFAULT_QUERIES_PER_WORKER);
        
        // Record some queries
        metrics.record_query(100, 50);
        metrics.record_query(200, 100);
        metrics.record_query(150, 75);
        
        assert_eq!(metrics.total_queries_completed, 3);
        assert!(metrics.avg_wait_time_ms > 0);
    }
    
    #[test]
    fn test_worker_idle_detection() {
        let mut worker = WorkerState {
            name: "test".to_string(),
            address: "http://test:50053".to_string(),
            memory_limit_mb: 1024,
            memory_used_mb: 100,
            cpu_limit_m: 2000,
            cpu_used_m: 10,  // Very low CPU
            running_queries: vec![],
            last_activity: Instant::now(),
            resize_in_progress: false,
            created_at: Instant::now(),
            idle_since: Some(Instant::now()),
            cpu_history: std::collections::VecDeque::new(),
            queries_completed: 10,
            total_query_memory_mb: 500,
        };
        
        // Not truly idle yet (need 30 min)
        assert!(!worker.is_truly_idle());
        
        // Not too old (need 5 hours)
        assert!(!worker.is_too_old());
        
        // Test CPU sample recording
        worker.record_cpu_sample();
        assert_eq!(worker.cpu_history.len(), 1);
        assert!(worker.avg_cpu_utilization() < 0.01); // Very low
    }
}

