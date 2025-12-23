//! Query Queue - Ceiling-Aware FIFO Queue with VPA-First, HPA-Second Logic
//!
//! This is a PROPER queue that:
//! 1. Queues all queries (never rejects unless queue is full)
//! 2. Detects K8s resource ceiling (node limits)
//! 3. Operates in TWO MODES:
//!    - SCALING MODE: Can grow capacity (proactive HPA/VPA)
//!    - SATURATION MODE: At ceiling, manages within fixed capacity
//! 4. VPA-first: Try to resize existing worker
//! 5. HPA-second: If VPA can't help, add new workers
//!
//! ARCHITECTURE:
//! ```
//! Query → enqueue() → [FIFO Queue] → dispatcher checks:
//!     │
//!     ├─ Worker with capacity? → Execute (no resize)
//!     │
//!     ├─ VPA possible? → Resize worker → Execute
//!     │
//!     ├─ Below ceiling? → Trigger HPA → Wait for new worker → Execute
//!     │
//!     └─ At ceiling? → Wait in queue OR return 429 if wait > threshold
//! ```
//!
//! CEILING DETECTION:
//! - Query K8s nodes for allocatable memory
//! - Compare with MAX_WORKERS × MAX_MEMORY_PER_WORKER
//! - at_ceiling = current_total >= 90% of ceiling
//!
//! PROACTIVE HPA:
//! - When queue grows AND below ceiling → IMMEDIATELY trigger HPA
//! - Don't wait for SmartScaler to notice

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify, RwLock};
use tracing::{debug, error, info, warn};

use crate::metrics;

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════

/// Maximum queue size (to prevent unbounded memory growth)
const MAX_QUEUE_SIZE: usize = 10_000;

/// How often to check K8s for capacity updates (ms)
const CAPACITY_REFRESH_INTERVAL_MS: u64 = 1000;

/// How often to check K8s nodes for ceiling (ms) - less frequent
const CEILING_REFRESH_INTERVAL_MS: u64 = 10_000;

/// Queue depth threshold to trigger HPA scale-up signal
const QUEUE_DEPTH_SCALE_UP_THRESHOLD: usize = 5;

/// Time waiting in queue before triggering HPA signal (ms)
const QUEUE_WAIT_SCALE_UP_THRESHOLD_MS: u64 = 2000;

/// Maximum wait time in queue before returning 429 in saturation mode (ms)
const SATURATION_MAX_WAIT_MS: u64 = 120_000; // 2 minutes

/// Ceiling detection threshold (90% of max = at ceiling)
const CEILING_THRESHOLD_RATIO: f64 = 0.90;

/// Dispatcher check interval (ms)
const DISPATCHER_INTERVAL_MS: u64 = 50; // 50ms for responsiveness

// ═══════════════════════════════════════════════════════════════════════════
// QUERY TOKEN (returned when query is dequeued)
// ═══════════════════════════════════════════════════════════════════════════

/// Token given to a query when it's dispatched from the queue
/// Caller MUST call `queue.complete(token, outcome)` when done
#[derive(Debug)]
pub struct QueryToken {
    pub query_id: String,
    pub cost_mb: u64,
    pub enqueued_at: Instant,
    pub dispatched_at: Instant,
}

impl QueryToken {
    /// How long the query waited in queue
    pub fn queue_wait_ms(&self) -> u64 {
        self.dispatched_at
            .duration_since(self.enqueued_at)
            .as_millis() as u64
    }
}

/// Outcome of a query execution
#[derive(Debug, Clone)]
pub enum QueryOutcome {
    Success {
        rows: u64,
        bytes: u64,
        duration_ms: u64,
    },
    Failure {
        error: String,
        duration_ms: u64,
    },
    Timeout {
        duration_ms: u64,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL QUEUE ENTRY
// ═══════════════════════════════════════════════════════════════════════════

struct QueueEntry {
    query_id: String,
    cost_mb: u64,
    enqueued_at: Instant,
    /// Channel to notify the waiting caller when dispatched
    ready_tx: oneshot::Sender<()>,
}

// ═══════════════════════════════════════════════════════════════════════════
// RESOURCE CEILING (hard limits from K8s nodes + config)
// ═══════════════════════════════════════════════════════════════════════════

/// Represents the absolute resource ceiling (hard limit)
#[derive(Debug, Clone)]
pub struct ResourceCeiling {
    /// Total allocatable memory across all K8s nodes (MB)
    pub node_allocatable_mb: u64,
    /// Maximum workers from config
    pub max_workers: u32,
    /// Maximum memory per worker from config (MB)
    pub max_memory_per_worker_mb: u64,
    /// Computed ceiling = MIN(node_allocatable, max_workers × max_memory)
    pub ceiling_mb: u64,
    /// Last time ceiling was detected
    pub last_refresh: Instant,
}

impl Default for ResourceCeiling {
    fn default() -> Self {
        // Conservative default - will be updated from K8s
        Self {
            node_allocatable_mb: 16 * 1024, // 16GB default (laptop-sized)
            max_workers: 100,
            max_memory_per_worker_mb: 400 * 1024, // 400GB from config
            ceiling_mb: 16 * 1024,                // Limited by node
            last_refresh: Instant::now(),
        }
    }
}

impl ResourceCeiling {
    /// Check if we're at the ceiling
    pub fn at_ceiling(&self, current_total_mb: u64) -> bool {
        current_total_mb >= (self.ceiling_mb as f64 * CEILING_THRESHOLD_RATIO) as u64
    }

    /// How much headroom we have
    pub fn headroom_mb(&self, current_total_mb: u64) -> u64 {
        self.ceiling_mb.saturating_sub(current_total_mb)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// OPERATION MODE (Scaling vs Saturation)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OperationMode {
    /// Below ceiling - can scale up (HPA/VPA)
    /// Behavior: Aggressive, proactive scaling
    Scaling,
    /// At ceiling - cannot scale further
    /// Behavior: Defensive, careful queue management
    Saturation,
}

// ═══════════════════════════════════════════════════════════════════════════
// K8S CAPACITY (learned from cluster, not hardcoded)
// ═══════════════════════════════════════════════════════════════════════════

/// Represents the current K8s cluster capacity
#[derive(Debug, Clone)]
pub struct ClusterCapacity {
    /// Total memory across all worker pods (MB)
    pub total_memory_mb: u64,
    /// Number of worker pods
    pub worker_count: u32,
    /// Average memory per worker (MB)
    pub avg_memory_per_worker_mb: u64,
    /// Memory currently in use (MB)
    pub used_memory_mb: u64,
    /// Last time capacity was refreshed
    pub last_refresh: Instant,
}

impl Default for ClusterCapacity {
    fn default() -> Self {
        Self {
            // Start with conservative estimate, will be updated from K8s
            total_memory_mb: 512, // 2 workers × 256MB default
            worker_count: 2,
            avg_memory_per_worker_mb: 256,
            used_memory_mb: 0,
            last_refresh: Instant::now(),
        }
    }
}

impl ClusterCapacity {
    /// Available capacity (total - used)
    pub fn available_mb(&self) -> u64 {
        self.total_memory_mb.saturating_sub(self.used_memory_mb)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUEUE STATISTICS
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct QueueStats {
    /// Current queue depth (waiting queries)
    pub queue_depth: usize,
    /// Current memory in use by active queries (MB)
    pub current_usage_mb: u64,
    /// Total cluster capacity (MB)
    pub total_capacity_mb: u64,
    /// Available capacity (MB)
    pub available_mb: u64,
    /// Resource ceiling (MB)
    pub ceiling_mb: u64,
    /// Current operation mode
    pub mode: OperationMode,
    /// Number of active (executing) queries
    pub active_queries: u64,
    /// Total queries processed
    pub total_processed: u64,
    /// Successful queries
    pub successful: u64,
    /// Failed queries  
    pub failed: u64,
    /// Average queue wait time (ms) - rolling window
    pub avg_queue_wait_ms: f64,
    /// P99 queue wait time (ms)
    pub p99_queue_wait_ms: u64,
    /// Average query execution time (ms)
    pub avg_execution_ms: f64,
    /// HPA scale-up signal active
    pub hpa_scale_up_signal: bool,
    /// Estimated wait time for new queries (ms)
    pub estimated_wait_ms: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// QUERY QUEUE
// ═══════════════════════════════════════════════════════════════════════════

pub struct QueryQueue {
    /// FIFO queue of waiting queries
    queue: RwLock<VecDeque<QueueEntry>>,

    /// Current memory in use by active queries (MB)
    current_usage_mb: AtomicU64,

    /// Current cluster capacity (from K8s workers)
    capacity: RwLock<ClusterCapacity>,

    /// Resource ceiling (from K8s nodes + config)
    ceiling: RwLock<ResourceCeiling>,

    /// Current operation mode
    mode: RwLock<OperationMode>,

    /// Notifier for when capacity becomes available
    capacity_available: Arc<Notify>,

    /// Active query count
    active_queries: AtomicU64,

    /// HPA scale-up requested (proactive)
    hpa_scale_requested: AtomicBool,

    /// Number of workers requested via HPA (pending)
    hpa_pending_workers: AtomicU64,

    // ═══════════════════════════════════════════════════════════════════
    // METRICS
    // ═══════════════════════════════════════════════════════════════════
    total_processed: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,

    /// Rolling window of queue wait times (for avg/p99)
    wait_times: RwLock<Vec<u64>>,

    /// Rolling window of execution times
    execution_times: RwLock<Vec<u64>>,

    /// HPA scale-up signal flag (for SmartScaler to read)
    hpa_scale_up_signal: RwLock<bool>,
}

impl QueryQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: RwLock::new(VecDeque::new()),
            current_usage_mb: AtomicU64::new(0),
            capacity: RwLock::new(ClusterCapacity::default()),
            ceiling: RwLock::new(ResourceCeiling::default()),
            mode: RwLock::new(OperationMode::Scaling),
            capacity_available: Arc::new(Notify::new()),
            active_queries: AtomicU64::new(0),
            hpa_scale_requested: AtomicBool::new(false),
            hpa_pending_workers: AtomicU64::new(0),

            total_processed: AtomicU64::new(0),
            successful: AtomicU64::new(0),
            failed: AtomicU64::new(0),

            wait_times: RwLock::new(Vec::with_capacity(1000)),
            execution_times: RwLock::new(Vec::with_capacity(1000)),
            hpa_scale_up_signal: RwLock::new(false),
        })
    }

    // ═══════════════════════════════════════════════════════════════════
    // OPERATION MODE
    // ═══════════════════════════════════════════════════════════════════

    /// Get current operation mode
    pub async fn get_mode(&self) -> OperationMode {
        *self.mode.read().await
    }

    /// Update operation mode based on ceiling
    async fn update_mode(&self) {
        let capacity = self.capacity.read().await;
        let ceiling = self.ceiling.read().await;

        let new_mode = if ceiling.at_ceiling(capacity.total_memory_mb) {
            OperationMode::Saturation
        } else {
            OperationMode::Scaling
        };

        let mut mode = self.mode.write().await;
        if *mode != new_mode {
            info!(
                old_mode = ?*mode,
                new_mode = ?new_mode,
                total_mb = capacity.total_memory_mb,
                ceiling_mb = ceiling.ceiling_mb,
                "Operation mode changed"
            );
            *mode = new_mode;

            metrics::set_operation_mode(match new_mode {
                OperationMode::Scaling => "scaling",
                OperationMode::Saturation => "saturation",
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // PROACTIVE HPA SIGNALING
    // ═══════════════════════════════════════════════════════════════════

    /// Request HPA to scale up (proactive)
    pub fn request_hpa_scale_up(&self, additional_workers: u64) {
        let current = self.hpa_pending_workers.load(Ordering::SeqCst);
        if additional_workers > current {
            self.hpa_pending_workers
                .store(additional_workers, Ordering::SeqCst);
            self.hpa_scale_requested.store(true, Ordering::SeqCst);
            debug!("HPA scale-up requested: {} workers", additional_workers);
        }
    }

    /// Check if HPA scale-up was requested and clear the flag
    pub fn take_hpa_request(&self) -> Option<u64> {
        if self.hpa_scale_requested.swap(false, Ordering::SeqCst) {
            let workers = self.hpa_pending_workers.swap(0, Ordering::SeqCst);
            if workers > 0 {
                return Some(workers);
            }
        }
        None
    }

    // ═══════════════════════════════════════════════════════════════════
    // WAIT TIME ESTIMATION (for saturation mode)
    // ═══════════════════════════════════════════════════════════════════

    async fn estimate_wait_time(&self) -> u64 {
        let queue = self.queue.read().await;
        let exec_times = self.execution_times.read().await;
        let capacity = self.capacity.read().await;

        if queue.is_empty() {
            return 0;
        }

        // Average execution time
        let avg_exec = if exec_times.is_empty() {
            1000 // Default 1 second if no data
        } else {
            exec_times.iter().sum::<u64>() / exec_times.len() as u64
        };

        // Estimate based on queue depth and parallelism
        let parallelism = (capacity.worker_count as u64).max(1);
        let queue_depth = queue.len() as u64;

        // estimated_wait = (queue_depth / parallelism) * avg_execution_time
        (queue_depth / parallelism) * avg_exec
    }

    // ═══════════════════════════════════════════════════════════════════
    // ENQUEUE - Two-mode operation
    // ═══════════════════════════════════════════════════════════════════

    /// Enqueue a query and wait until capacity is available
    ///
    /// SCALING MODE (below ceiling):
    /// - Always accepts
    /// - Proactively triggers HPA if queue grows
    /// - Query waits briefly for new capacity
    ///
    /// SATURATION MODE (at ceiling):
    /// - Accepts if estimated wait < threshold
    /// - Returns 429 if wait would be too long
    /// - Manages carefully within fixed capacity
    pub async fn enqueue(
        self: &Arc<Self>,
        query_id: String,
        estimated_data_mb: u64,
    ) -> Result<QueryToken, QueueError> {
        let enqueued_at = Instant::now();
        let mode = self.get_mode().await;

        // Check queue size limit
        {
            let queue = self.queue.read().await;
            if queue.len() >= MAX_QUEUE_SIZE {
                warn!(
                    query_id = %query_id,
                    queue_depth = queue.len(),
                    "Queue full, cannot accept more queries"
                );
                return Err(QueueError::QueueFull {
                    max_size: MAX_QUEUE_SIZE,
                });
            }

            // SATURATION MODE: Check if wait would be too long
            if mode == OperationMode::Saturation {
                let estimated_wait = self.estimate_wait_time().await;
                if estimated_wait > SATURATION_MAX_WAIT_MS {
                    let retry_after_ms = estimated_wait.min(60_000); // Cap at 1 minute
                    warn!(
                        query_id = %query_id,
                        estimated_wait_ms = estimated_wait,
                        mode = ?mode,
                        "Saturation mode: queue backed up, suggest retry later"
                    );
                    return Err(QueueError::AtCapacity {
                        retry_after_ms,
                        queue_depth: queue.len(),
                    });
                }
            }
        }

        // Create a channel to be notified when we're dispatched
        let (ready_tx, ready_rx) = oneshot::channel();

        // Create queue entry
        let entry = QueueEntry {
            query_id: query_id.clone(),
            cost_mb: estimated_data_mb,
            enqueued_at,
            ready_tx,
        };

        // Add to queue
        let queue_depth = {
            let mut queue = self.queue.write().await;
            queue.push_back(entry);

            let depth = queue.len();
            debug!(
                query_id = %query_id,
                cost_mb = estimated_data_mb,
                queue_depth = depth,
                mode = ?mode,
                "Query enqueued"
            );

            // Update metrics
            metrics::set_queue_depth(depth);
            depth
        };

        // SCALING MODE: Proactively request HPA scale-up
        if mode == OperationMode::Scaling && queue_depth >= QUEUE_DEPTH_SCALE_UP_THRESHOLD {
            // Request 1 additional worker per 5 queries in queue
            let additional = ((queue_depth / 5) as u64).max(1);
            self.request_hpa_scale_up(additional);
            *self.hpa_scale_up_signal.write().await = true;
            metrics::set_hpa_scale_up_signal(true);
        }

        // Trigger dispatcher to check queue
        self.capacity_available.notify_one();

        // Determine timeout based on mode
        let dispatch_timeout = match mode {
            OperationMode::Scaling => Duration::from_secs(3600), // 1 hour - HPA will add capacity
            OperationMode::Saturation => Duration::from_millis(SATURATION_MAX_WAIT_MS), // 2 min max
        };

        // Wait until we're dispatched
        match tokio::time::timeout(dispatch_timeout, ready_rx).await {
            Ok(Ok(())) => {
                let dispatched_at = Instant::now();
                let wait_ms = dispatched_at.duration_since(enqueued_at).as_millis() as u64;

                // Record wait time
                {
                    let mut waits = self.wait_times.write().await;
                    waits.push(wait_ms);
                    if waits.len() > 1000 {
                        waits.remove(0);
                    }
                }

                // SCALING MODE: Long wait should trigger more HPA
                if mode == OperationMode::Scaling && wait_ms > QUEUE_WAIT_SCALE_UP_THRESHOLD_MS {
                    self.request_hpa_scale_up(2);
                    *self.hpa_scale_up_signal.write().await = true;
                    metrics::set_hpa_scale_up_signal(true);
                }

                debug!(
                    query_id = %query_id,
                    wait_ms = wait_ms,
                    mode = ?mode,
                    "Query dispatched from queue"
                );

                Ok(QueryToken {
                    query_id,
                    cost_mb: estimated_data_mb,
                    enqueued_at,
                    dispatched_at,
                })
            }
            Ok(Err(_)) => {
                error!(query_id = %query_id, "Queue dispatcher dropped unexpectedly");
                Err(QueueError::DispatcherError)
            }
            Err(_) => {
                // Remove from queue if still there
                {
                    let mut queue = self.queue.write().await;
                    queue.retain(|e| e.query_id != query_id);
                    metrics::set_queue_depth(queue.len());
                }

                warn!(query_id = %query_id, mode = ?mode, "Query timed out waiting in queue");
                Err(QueueError::Timeout {
                    wait_seconds: dispatch_timeout.as_secs(),
                })
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // COMPLETE - Release capacity after query finishes
    // ═══════════════════════════════════════════════════════════════════

    pub async fn complete(&self, token: QueryToken, outcome: QueryOutcome) {
        // Release capacity
        self.current_usage_mb
            .fetch_sub(token.cost_mb, Ordering::SeqCst);
        self.active_queries.fetch_sub(1, Ordering::SeqCst);
        self.total_processed.fetch_add(1, Ordering::SeqCst);

        // Record outcome
        let duration_ms = match &outcome {
            QueryOutcome::Success { duration_ms, .. } => {
                self.successful.fetch_add(1, Ordering::SeqCst);
                *duration_ms
            }
            QueryOutcome::Failure { duration_ms, .. } => {
                self.failed.fetch_add(1, Ordering::SeqCst);
                *duration_ms
            }
            QueryOutcome::Timeout { duration_ms } => {
                self.failed.fetch_add(1, Ordering::SeqCst);
                *duration_ms
            }
        };

        // Record execution time
        {
            let mut times = self.execution_times.write().await;
            times.push(duration_ms);
            if times.len() > 1000 {
                times.remove(0);
            }
        }

        debug!(
            query_id = %token.query_id,
            cost_mb = token.cost_mb,
            wait_ms = token.queue_wait_ms(),
            exec_ms = duration_ms,
            "Query completed"
        );

        // Notify dispatcher that capacity is available
        self.capacity_available.notify_one();

        // Update metrics
        let current_usage = self.current_usage_mb.load(Ordering::SeqCst);
        let capacity = self.capacity.read().await;
        metrics::set_current_usage_mb(current_usage);
        metrics::set_available_capacity_mb(capacity.total_memory_mb.saturating_sub(current_usage));
    }

    // ═══════════════════════════════════════════════════════════════════
    // DISPATCHER LOOP - Runs continuously, dispatches when capacity available
    // ═══════════════════════════════════════════════════════════════════

    pub async fn start_dispatcher(self: Arc<Self>) {
        info!("Starting query dispatcher loop (VPA-first, HPA-second)");

        loop {
            // Wait for notification or short timeout for responsiveness
            tokio::select! {
                _ = self.capacity_available.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(DISPATCHER_INTERVAL_MS)) => {},
            }

            // Try to dispatch waiting queries
            self.try_dispatch().await;

            // Update operation mode based on capacity vs ceiling
            self.update_mode().await;
        }
    }

    /// Try to dispatch queries from the queue
    ///
    /// Priority order:
    /// 1. If worker has capacity → dispatch immediately
    /// 2. If VPA can resize → dispatch (SmartScaler handles resize)
    /// 3. If below ceiling → keep in queue, HPA will add workers
    /// 4. If at ceiling → query waits or times out
    async fn try_dispatch(&self) {
        let capacity = self.capacity.read().await;
        let current_usage = self.current_usage_mb.load(Ordering::SeqCst);
        let available = capacity.available_mb();
        let mode = *self.mode.read().await;

        // Look at front of queue
        let mut queue = self.queue.write().await;

        // Dispatch as many queries as fit
        while let Some(entry) = queue.front() {
            let can_dispatch =
                // Case 1: We have capacity
                entry.cost_mb <= available
                // Case 2: No queries running (must dispatch at least one to make progress)
                || self.active_queries.load(Ordering::SeqCst) == 0
                // Case 3: Query is small enough (<100MB) - always try
                || entry.cost_mb < 100;

            if can_dispatch {
                if let Some(entry) = queue.pop_front() {
                    // Reserve capacity
                    self.current_usage_mb
                        .fetch_add(entry.cost_mb, Ordering::SeqCst);
                    self.active_queries.fetch_add(1, Ordering::SeqCst);

                    // Notify the waiting caller
                    let _ = entry.ready_tx.send(());

                    debug!(
                        query_id = %entry.query_id,
                        cost_mb = entry.cost_mb,
                        available_mb = available,
                        mode = ?mode,
                        "Query dispatched"
                    );

                    // Update queue depth metric
                    metrics::set_queue_depth(queue.len());
                }
            } else {
                // Not enough capacity for front query
                // In SCALING MODE, this triggers HPA
                if mode == OperationMode::Scaling && !queue.is_empty() {
                    // Request HPA to add capacity
                    let needed_workers = ((entry.cost_mb as f64
                        / capacity.avg_memory_per_worker_mb as f64)
                        .ceil() as u64)
                        .max(1);
                    self.hpa_scale_requested.store(true, Ordering::SeqCst);
                    if needed_workers > self.hpa_pending_workers.load(Ordering::SeqCst) {
                        self.hpa_pending_workers
                            .store(needed_workers, Ordering::SeqCst);
                    }
                }
                break;
            }
        }

        // Clear HPA signal if queue is draining
        if queue.len() < QUEUE_DEPTH_SCALE_UP_THRESHOLD {
            *self.hpa_scale_up_signal.write().await = false;
            metrics::set_hpa_scale_up_signal(false);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // CAPACITY UPDATER - Periodically queries K8s for real capacity
    // ═══════════════════════════════════════════════════════════════════

    pub async fn start_capacity_updater(self: Arc<Self>, k8s_client: kube::Client) {
        info!("Starting K8s capacity updater loop");

        let mut ceiling_refresh_counter = 0u64;

        loop {
            // Update worker capacity every 1 second
            if let Err(e) = self.update_capacity_from_k8s(&k8s_client).await {
                warn!("Failed to update capacity from K8s: {}", e);
            }

            // Update ceiling less frequently (every 10 seconds)
            ceiling_refresh_counter += CAPACITY_REFRESH_INTERVAL_MS;
            if ceiling_refresh_counter >= CEILING_REFRESH_INTERVAL_MS {
                ceiling_refresh_counter = 0;
                if let Err(e) = self.update_ceiling_from_k8s(&k8s_client).await {
                    debug!("Failed to update ceiling from K8s: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(CAPACITY_REFRESH_INTERVAL_MS)).await;
        }
    }

    /// Update worker capacity from K8s pods
    async fn update_capacity_from_k8s(&self, client: &kube::Client) -> anyhow::Result<()> {
        use k8s_openapi::api::core::v1::Pod;
        use kube::api::{Api, ListParams};

        let pods: Api<Pod> = Api::namespaced(client.clone(), "tavana");
        let lp = ListParams::default().labels("app=worker");

        let pod_list = pods.list(&lp).await?;

        let mut total_memory_mb: u64 = 0;
        let mut used_memory_mb: u64 = 0;
        let mut worker_count: u32 = 0;

        for pod in pod_list.items {
            // Only count running pods
            if let Some(status) = &pod.status {
                if status.phase != Some("Running".to_string()) {
                    continue;
                }
            }

            // Get memory limit and usage from container spec
            if let Some(spec) = &pod.spec {
                for container in &spec.containers {
                    if let Some(resources) = &container.resources {
                        if let Some(limits) = &resources.limits {
                            if let Some(mem) = limits.get("memory") {
                                let mem_mb = parse_k8s_memory_to_mb(&mem.0);
                                total_memory_mb += mem_mb;
                                worker_count += 1;
                            }
                        }
                        // Estimate used memory from requests as a proxy
                        // (Real usage requires metrics-server)
                        if let Some(requests) = &resources.requests {
                            if let Some(mem) = requests.get("memory") {
                                let mem_mb = parse_k8s_memory_to_mb(&mem.0);
                                used_memory_mb += mem_mb / 2; // Conservative estimate
                            }
                        }
                    }
                }
            }
        }

        if worker_count > 0 {
            let avg = total_memory_mb / worker_count as u64;

            let mut capacity = self.capacity.write().await;
            let old_total = capacity.total_memory_mb;

            capacity.total_memory_mb = total_memory_mb;
            capacity.worker_count = worker_count;
            capacity.avg_memory_per_worker_mb = avg;
            capacity.used_memory_mb = self.current_usage_mb.load(Ordering::SeqCst);
            capacity.last_refresh = Instant::now();

            if old_total != total_memory_mb {
                let mode = self.get_mode().await;
                info!(
                    workers = worker_count,
                    total_mb = total_memory_mb,
                    available_mb = capacity.available_mb(),
                    avg_mb = avg,
                    mode = ?mode,
                    "Cluster capacity updated from K8s"
                );
            }

            // Update metrics
            metrics::set_cluster_capacity_mb(total_memory_mb);
            metrics::set_worker_count(worker_count);

            // Notify dispatcher in case new capacity allows more dispatches
            self.capacity_available.notify_one();
        }

        Ok(())
    }

    /// Update resource ceiling from K8s nodes
    async fn update_ceiling_from_k8s(&self, client: &kube::Client) -> anyhow::Result<()> {
        use k8s_openapi::api::core::v1::Node;
        use kube::api::{Api, ListParams};

        let nodes: Api<Node> = Api::all(client.clone());
        let node_list = nodes.list(&ListParams::default()).await?;

        let mut total_allocatable_mb: u64 = 0;

        for node in node_list.items {
            if let Some(status) = &node.status {
                if let Some(allocatable) = &status.allocatable {
                    if let Some(mem) = allocatable.get("memory") {
                        total_allocatable_mb += parse_k8s_memory_to_mb(&mem.0);
                    }
                }
            }
        }

        if total_allocatable_mb > 0 {
            let mut ceiling = self.ceiling.write().await;
            let old_ceiling = ceiling.ceiling_mb;

            ceiling.node_allocatable_mb = total_allocatable_mb;
            // max_workers and max_memory_per_worker come from config
            // We use conservative defaults
            let config_limit = ceiling.max_workers as u64 * ceiling.max_memory_per_worker_mb;
            ceiling.ceiling_mb = total_allocatable_mb.min(config_limit);
            ceiling.last_refresh = Instant::now();

            if old_ceiling != ceiling.ceiling_mb {
                info!(
                    node_allocatable_mb = total_allocatable_mb,
                    config_limit_mb = config_limit,
                    ceiling_mb = ceiling.ceiling_mb,
                    "Resource ceiling updated from K8s nodes"
                );

                metrics::set_resource_ceiling_mb(ceiling.ceiling_mb);
            }

            // Drop lock before updating mode
            drop(ceiling);
        }

        // Update operation mode
        self.update_mode().await;

        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════
    // MANUAL CAPACITY UPDATE (for testing or when K8s is unavailable)
    // ═══════════════════════════════════════════════════════════════════

    pub async fn set_capacity(&self, total_memory_mb: u64, worker_count: u32) {
        let mut capacity = self.capacity.write().await;
        capacity.total_memory_mb = total_memory_mb;
        capacity.worker_count = worker_count;
        capacity.avg_memory_per_worker_mb = if worker_count > 0 {
            total_memory_mb / worker_count as u64
        } else {
            0
        };
        capacity.last_refresh = Instant::now();

        info!(
            total_mb = total_memory_mb,
            workers = worker_count,
            "Capacity manually set"
        );

        // Notify dispatcher
        self.capacity_available.notify_one();
    }

    // ═══════════════════════════════════════════════════════════════════
    // STATISTICS
    // ═══════════════════════════════════════════════════════════════════

    pub async fn stats(&self) -> QueueStats {
        let queue = self.queue.read().await;
        let capacity = self.capacity.read().await;
        let ceiling = self.ceiling.read().await;
        let mode = *self.mode.read().await;
        let current_usage = self.current_usage_mb.load(Ordering::SeqCst);

        let wait_times = self.wait_times.read().await;
        let avg_wait = if wait_times.is_empty() {
            0.0
        } else {
            wait_times.iter().sum::<u64>() as f64 / wait_times.len() as f64
        };
        let p99_wait = if wait_times.is_empty() {
            0
        } else {
            let mut sorted = wait_times.clone();
            sorted.sort();
            sorted[sorted.len() * 99 / 100]
        };

        let exec_times = self.execution_times.read().await;
        let avg_exec = if exec_times.is_empty() {
            0.0
        } else {
            exec_times.iter().sum::<u64>() as f64 / exec_times.len() as f64
        };

        // Estimate wait time
        let estimated_wait = self.estimate_wait_time_internal(&queue, &exec_times, &capacity);

        QueueStats {
            queue_depth: queue.len(),
            current_usage_mb: current_usage,
            total_capacity_mb: capacity.total_memory_mb,
            available_mb: capacity.total_memory_mb.saturating_sub(current_usage),
            ceiling_mb: ceiling.ceiling_mb,
            mode,
            active_queries: self.active_queries.load(Ordering::SeqCst),
            total_processed: self.total_processed.load(Ordering::SeqCst),
            successful: self.successful.load(Ordering::SeqCst),
            failed: self.failed.load(Ordering::SeqCst),
            avg_queue_wait_ms: avg_wait,
            p99_queue_wait_ms: p99_wait,
            avg_execution_ms: avg_exec,
            hpa_scale_up_signal: *self.hpa_scale_up_signal.read().await,
            estimated_wait_ms: estimated_wait,
        }
    }

    /// Internal wait time estimation (no locks)
    fn estimate_wait_time_internal(
        &self,
        queue: &VecDeque<QueueEntry>,
        exec_times: &Vec<u64>,
        capacity: &ClusterCapacity,
    ) -> u64 {
        if queue.is_empty() {
            return 0;
        }

        let avg_exec = if exec_times.is_empty() {
            1000
        } else {
            exec_times.iter().sum::<u64>() / exec_times.len() as u64
        };

        let parallelism = (capacity.worker_count as u64).max(1);
        let queue_depth = queue.len() as u64;

        (queue_depth / parallelism) * avg_exec
    }

    // ═══════════════════════════════════════════════════════════════════
    // CEILING CONFIGURATION (for manual/config-based limits)
    // ═══════════════════════════════════════════════════════════════════

    /// Set ceiling from config (max_workers, max_memory_per_worker)
    pub async fn set_ceiling_config(&self, max_workers: u32, max_memory_per_worker_mb: u64) {
        let mut ceiling = self.ceiling.write().await;
        ceiling.max_workers = max_workers;
        ceiling.max_memory_per_worker_mb = max_memory_per_worker_mb;

        let config_limit = max_workers as u64 * max_memory_per_worker_mb;
        ceiling.ceiling_mb = ceiling.node_allocatable_mb.min(config_limit);
        ceiling.last_refresh = Instant::now();

        info!(
            max_workers = max_workers,
            max_memory_per_worker_mb = max_memory_per_worker_mb,
            ceiling_mb = ceiling.ceiling_mb,
            "Resource ceiling config updated"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ERRORS
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum QueueError {
    /// Queue is full (only happens if MAX_QUEUE_SIZE reached)
    QueueFull { max_size: usize },
    /// Timeout waiting for capacity
    Timeout { wait_seconds: u64 },
    /// At capacity ceiling (saturation mode) - suggest retry
    AtCapacity {
        retry_after_ms: u64,
        queue_depth: usize,
    },
    /// Internal dispatcher error
    DispatcherError,
}

impl QueueError {
    /// Get the PostgreSQL error code for this error
    pub fn pg_error_code(&self) -> &'static str {
        match self {
            QueueError::QueueFull { .. } => "53300", // too_many_connections
            QueueError::Timeout { .. } => "57014",   // query_canceled
            QueueError::AtCapacity { .. } => "53300", // too_many_connections
            QueueError::DispatcherError => "XX000",  // internal_error
        }
    }

    /// Get retry hint in milliseconds (for 429-like behavior)
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            QueueError::AtCapacity { retry_after_ms, .. } => Some(*retry_after_ms),
            _ => None,
        }
    }
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::QueueFull { max_size } => {
                write!(f, "Query queue full (max {} queries)", max_size)
            }
            QueueError::Timeout { wait_seconds } => {
                write!(
                    f,
                    "Timed out after {} seconds waiting in queue",
                    wait_seconds
                )
            }
            QueueError::AtCapacity {
                retry_after_ms,
                queue_depth,
            } => {
                write!(
                    f,
                    "Server at capacity ({} queries queued). Retry after {} ms",
                    queue_depth, retry_after_ms
                )
            }
            QueueError::DispatcherError => {
                write!(f, "Internal queue dispatcher error")
            }
        }
    }
}

impl std::error::Error for QueueError {}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/// Parse K8s memory string (e.g., "512Mi", "2Gi", "1024") to MB
fn parse_k8s_memory_to_mb(mem_str: &str) -> u64 {
    let mem_str = mem_str.trim();

    if let Some(val) = mem_str.strip_suffix("Gi") {
        val.parse::<u64>().unwrap_or(0) * 1024
    } else if let Some(val) = mem_str.strip_suffix("Mi") {
        val.parse::<u64>().unwrap_or(0)
    } else if let Some(val) = mem_str.strip_suffix("Ki") {
        val.parse::<u64>().unwrap_or(0) / 1024
    } else if let Some(val) = mem_str.strip_suffix("G") {
        val.parse::<u64>().unwrap_or(0) * 1000
    } else if let Some(val) = mem_str.strip_suffix("M") {
        val.parse::<u64>().unwrap_or(0)
    } else if let Some(val) = mem_str.strip_suffix("K") {
        val.parse::<u64>().unwrap_or(0) / 1000
    } else {
        // Assume bytes
        mem_str.parse::<u64>().unwrap_or(0) / (1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory() {
        assert_eq!(parse_k8s_memory_to_mb("512Mi"), 512);
        assert_eq!(parse_k8s_memory_to_mb("2Gi"), 2048);
        assert_eq!(parse_k8s_memory_to_mb("1024Ki"), 1);
        assert_eq!(parse_k8s_memory_to_mb("1G"), 1000);
        assert_eq!(parse_k8s_memory_to_mb("1073741824"), 1024); // 1GB in bytes
    }
}
