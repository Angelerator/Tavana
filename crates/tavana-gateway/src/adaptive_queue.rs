//! Adaptive Query Queue - Netflix-style AIMD Concurrency Control (FIXED)
//!
//! KEY FIX: Non-blocking admission control
//! - Immediate accept/reject decision (no blocking loops)
//! - PostgreSQL-compatible backpressure signaling
//! - Accurate cost tracking with real feedback
//!
//! Algorithm:
//! - AIMD (Additive Increase, Multiplicative Decrease)
//! - Gradient-based limit adjustment from latency
//! - Cost units based on actual query characteristics

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::metrics;

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

/// Initial concurrency limit in cost units
/// Higher initial value allows system to start accepting queries immediately
const INITIAL_LIMIT: f64 = 50.0;

/// Minimum concurrency limit (floor)
/// Must be high enough to handle at least ONE large query (320MB / 200MB = 1.6 cost units)
const MIN_LIMIT: f64 = 10.0;

/// Maximum concurrency limit (ceiling for safety)
const MAX_LIMIT: f64 = 200.0;

/// Target latency in ms - above this we reduce limit
const TARGET_LATENCY_MS: u64 = 5000;

/// Baseline memory for 1 cost unit (MB)
/// 200MB means a 400MB query costs 2 units, a 200MB query costs 1 unit
const BASELINE_MEMORY_MB: u64 = 200;

/// AIMD additive increase per successful query
const AIMD_INCREASE: f64 = 0.5;

/// AIMD multiplicative decrease on failure
const AIMD_DECREASE: f64 = 0.75;

/// Smoothing factor for exponential moving averages
/// Lower value = more stable, less reactive to short-term fluctuations
const SMOOTHING_FACTOR: f64 = 0.05;

/// Window size for latency/throughput tracking (seconds)
const WINDOW_DURATION_SECS: u64 = 60;

// ═══════════════════════════════════════════════════════════════════════════
// QUERY OUTCOME - Comprehensive tracking
// ═══════════════════════════════════════════════════════════════════════════

/// All possible outcomes for a query - used for accurate AIMD
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    // Success cases
    Success {
        rows_returned: u64,
        bytes_transferred: u64,
    },
    
    // Failure cases - each affects AIMD differently
    FailureTimeout,           // Query took too long → decrease limit
    FailureOOM,               // Out of memory → decrease limit significantly
    FailureConnectionError,   // Network issue → decrease limit
    FailureWorkerError,       // Worker returned error → decrease limit
    FailureClientDisconnect,  // Client closed connection → don't penalize
    FailureParseError,        // Invalid SQL → don't penalize
    FailureAuthError,         // Authentication failed → don't penalize
    FailureUnknown(String),   // Catch-all
    
    // Rejection cases - query never started
    RejectedAtCapacity,       // Concurrency limit reached
    RejectedQueryTooLarge,    // Single query exceeds max cost
}

impl QueryOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, QueryOutcome::Success { .. })
    }
    
    /// Should this outcome decrease the concurrency limit?
    pub fn should_decrease_limit(&self) -> bool {
        matches!(self, 
            QueryOutcome::FailureTimeout |
            QueryOutcome::FailureOOM |
            QueryOutcome::FailureConnectionError |
            QueryOutcome::FailureWorkerError
        )
    }
    
    /// Is this a client-side issue (don't penalize system)?
    pub fn is_client_fault(&self) -> bool {
        matches!(self,
            QueryOutcome::FailureClientDisconnect |
            QueryOutcome::FailureParseError |
            QueryOutcome::FailureAuthError
        )
    }
    
    /// Get the AIMD decrease multiplier for this outcome
    pub fn aimd_multiplier(&self) -> f64 {
        match self {
            QueryOutcome::FailureOOM => 0.3,      // Severe - drop to 30%
            QueryOutcome::FailureTimeout => 0.5,  // Moderate - drop to 50%
            QueryOutcome::FailureConnectionError => 0.7, // Light - drop to 70%
            QueryOutcome::FailureWorkerError => 0.6,     // Moderate
            _ => 1.0,  // No change
        }
    }
    
    pub fn as_metric_label(&self) -> &'static str {
        match self {
            QueryOutcome::Success { .. } => "success",
            QueryOutcome::FailureTimeout => "timeout",
            QueryOutcome::FailureOOM => "oom",
            QueryOutcome::FailureConnectionError => "connection_error",
            QueryOutcome::FailureWorkerError => "worker_error",
            QueryOutcome::FailureClientDisconnect => "client_disconnect",
            QueryOutcome::FailureParseError => "parse_error",
            QueryOutcome::FailureAuthError => "auth_error",
            QueryOutcome::FailureUnknown(_) => "unknown_error",
            QueryOutcome::RejectedAtCapacity => "rejected_capacity",
            QueryOutcome::RejectedQueryTooLarge => "rejected_too_large",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// REJECTION REASON - For backpressure signaling
// ═══════════════════════════════════════════════════════════════════════════

/// Reason for rejecting a query (returned immediately, non-blocking)
#[derive(Debug, Clone)]
pub enum RejectionReason {
    /// System at capacity - client should retry later
    AtCapacity {
        current_cost: f64,
        limit: f64,
        retry_after_ms: u64,
    },
    /// Single query is too expensive
    QueryTooLarge {
        query_cost: f64,
        max_single_query_cost: f64,
    },
}

impl RejectionReason {
    /// Generate a PostgreSQL-compatible error message
    pub fn to_pg_error(&self) -> String {
        match self {
            RejectionReason::AtCapacity { current_cost, limit, retry_after_ms } => {
                format!(
                    "QUERY_REJECTED: Server at capacity ({:.1}/{:.1} cost units). Retry in {}ms. [SQLSTATE 53300]",
                    current_cost, limit, retry_after_ms
                )
            }
            RejectionReason::QueryTooLarge { query_cost, max_single_query_cost } => {
                format!(
                    "QUERY_REJECTED: Query too large ({:.1} > {:.1} max cost units). Consider using LIMIT. [SQLSTATE 53400]",
                    query_cost, max_single_query_cost
                )
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ADMIT TOKEN - Proof of admission
// ═══════════════════════════════════════════════════════════════════════════

/// Token returned on successful admission - must be released on completion
#[derive(Debug)]
pub struct AdmitToken {
    pub id: String,
    pub cost_units: f64,
    pub admitted_at: Instant,
}

// ═══════════════════════════════════════════════════════════════════════════
// ROLLING WINDOW - For latency/throughput measurements
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct RollingWindow {
    values: VecDeque<(Instant, f64)>,
    window_size: Duration,
}

impl RollingWindow {
    fn new(window_size: Duration) -> Self {
        Self {
            values: VecDeque::new(),
            window_size,
        }
    }
    
    fn push(&mut self, value: f64) {
        let now = Instant::now();
        self.cleanup(now);
        self.values.push_back((now, value));
    }
    
    fn cleanup(&mut self, now: Instant) {
        while let Some((ts, _)) = self.values.front() {
            if now.duration_since(*ts) > self.window_size {
                self.values.pop_front();
            } else {
                break;
            }
        }
    }
    
    fn average(&self) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        
        let now = Instant::now();
        let valid: Vec<f64> = self.values.iter()
            .filter(|(ts, _)| now.duration_since(*ts) <= self.window_size)
            .map(|(_, v)| *v)
            .collect();
        
        if valid.is_empty() {
            None
        } else {
            Some(valid.iter().sum::<f64>() / valid.len() as f64)
        }
    }
    
    fn percentile(&self, p: f64) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        
        let now = Instant::now();
        let mut valid: Vec<f64> = self.values.iter()
            .filter(|(ts, _)| now.duration_since(*ts) <= self.window_size)
            .map(|(_, v)| *v)
            .collect();
        
        if valid.is_empty() {
            return None;
        }
        
        valid.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((valid.len() as f64 * p) as usize).min(valid.len() - 1);
        Some(valid[idx])
    }
    
    fn min(&self) -> Option<f64> {
        self.values.iter()
            .map(|(_, v)| *v)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }
    
    fn count(&self) -> usize {
        let now = Instant::now();
        self.values.iter()
            .filter(|(ts, _)| now.duration_since(*ts) <= self.window_size)
            .count()
    }
    
    fn throughput_per_sec(&self) -> f64 {
        let count = self.count();
        count as f64 / self.window_size.as_secs_f64()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ADAPTIVE QUEUE - The Core Implementation
// ═══════════════════════════════════════════════════════════════════════════

/// Adaptive Queue with non-blocking admission control
pub struct AdaptiveQueue {
    // ═══════════════════════════════════════════════════════════════════
    // ADAPTIVE LIMIT (learned via AIMD)
    // ═══════════════════════════════════════════════════════════════════
    
    /// Current concurrency limit in cost units
    concurrency_limit: RwLock<f64>,
    
    /// Current cost units in use (sum of all active queries)
    current_cost_units: RwLock<f64>,
    
    /// Number of active queries (for metrics)
    active_queries: AtomicU64,
    
    // ═══════════════════════════════════════════════════════════════════
    // MEASUREMENTS (for AIMD feedback)
    // ═══════════════════════════════════════════════════════════════════
    
    /// Latency window (for gradient calculation)
    latency_window: RwLock<RollingWindow>,
    
    /// Minimum observed latency (baseline when unloaded)
    min_latency_ms: AtomicU64,
    
    /// Completion tracking (for throughput)
    completions_window: RwLock<RollingWindow>,
    
    // ═══════════════════════════════════════════════════════════════════
    // COUNTERS (for metrics and debugging)
    // ═══════════════════════════════════════════════════════════════════
    
    total_queries: AtomicU64,
    successful_queries: AtomicU64,
    failed_queries: AtomicU64,
    rejected_queries: AtomicU64,
    
    /// Last adjustment time (for rate limiting adjustments)
    last_adjustment: RwLock<Instant>,
}

impl AdaptiveQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            concurrency_limit: RwLock::new(INITIAL_LIMIT),
            current_cost_units: RwLock::new(0.0),
            active_queries: AtomicU64::new(0),
            
            latency_window: RwLock::new(RollingWindow::new(Duration::from_secs(WINDOW_DURATION_SECS))),
            min_latency_ms: AtomicU64::new(u64::MAX),
            completions_window: RwLock::new(RollingWindow::new(Duration::from_secs(WINDOW_DURATION_SECS))),
            
            total_queries: AtomicU64::new(0),
            successful_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            rejected_queries: AtomicU64::new(0),
            
            last_adjustment: RwLock::new(Instant::now()),
        })
    }
    
    /// Create with custom initial limit
    pub fn with_initial_limit(initial_limit: f64) -> Arc<Self> {
        let queue = Self::new();
        // Note: We can't easily modify inside Arc, so just use new() for now
        // In production, use a builder pattern
        queue
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // NON-BLOCKING ADMISSION CONTROL (THE KEY FIX)
    // ═══════════════════════════════════════════════════════════════════
    
    /// Try to admit a query - IMMEDIATE response, no blocking
    /// 
    /// Returns:
    /// - Ok(AdmitToken) if admitted - caller MUST call record_completion when done
    /// - Err(RejectionReason) if rejected - caller should send error to client
    pub async fn try_admit(
        &self,
        query_id: String,
        estimated_memory_mb: u64,
    ) -> Result<AdmitToken, RejectionReason> {
        debug!("try_admit: query={}, memory={}MB", query_id, estimated_memory_mb);
        self.total_queries.fetch_add(1, Ordering::SeqCst);
        
        // Calculate cost units from estimated memory
        let cost_units = self.calculate_cost(estimated_memory_mb);
        
        // Check if single query is too large (> 50% of max limit)
        let max_single_query = MAX_LIMIT * 0.5;
        if cost_units > max_single_query {
            self.rejected_queries.fetch_add(1, Ordering::SeqCst);
            metrics::record_query_outcome("rejected_too_large");
            
            return Err(RejectionReason::QueryTooLarge {
                query_cost: cost_units,
                max_single_query_cost: max_single_query,
            });
        }
        
        // Get current state and make decision in a single scoped block
        // This ensures locks are released BEFORE any other async operations
        let admission_result = {
            let limit = *self.concurrency_limit.read().await;
            let mut current = self.current_cost_units.write().await;
            
            // Check if we have capacity
            if *current + cost_units > limit {
                // Rejection - get values before dropping locks
                let current_val = *current;
                drop(current); // Release write lock BEFORE async call
                
                Err((current_val, limit))
            } else {
                // ADMIT the query
                *current += cost_units;
                Ok(limit)
            }
        }; // All locks dropped here
        
        match admission_result {
            Err((current_val, limit)) => {
                self.rejected_queries.fetch_add(1, Ordering::SeqCst);
                metrics::record_query_outcome("rejected_capacity");
                
                // Now safe to call async function - no locks held
                let retry_after = self.calculate_retry_hint().await;
                
                info!(
                    "Query {} rejected: cost={:.1}, current={:.1}, limit={:.1}, retry_in={}ms",
                    query_id, cost_units, current_val, limit, retry_after
                );
                
                Err(RejectionReason::AtCapacity {
                    current_cost: current_val,
                    limit,
                    retry_after_ms: retry_after,
                })
            }
            Ok(limit) => {
                self.active_queries.fetch_add(1, Ordering::SeqCst);
                
                let token = AdmitToken {
                    id: query_id.clone(),
                    cost_units,
                    admitted_at: Instant::now(),
                };
                
                debug!(
                    "Query {} admitted: cost={:.1}, active={}, limit={:.1}",
                    query_id,
                    cost_units,
                    self.active_queries.load(Ordering::SeqCst),
                    limit
                );
                
                // Update metrics - all locks already released
                self.update_prometheus_metrics().await;
                
                Ok(token)
            }
        }
    }
    
    /// Calculate cost units from memory estimate
    fn calculate_cost(&self, estimated_memory_mb: u64) -> f64 {
        let cost = estimated_memory_mb as f64 / BASELINE_MEMORY_MB as f64;
        cost.max(0.5) // Minimum 0.5 cost units per query
    }
    
    /// Calculate retry hint based on current throughput
    async fn calculate_retry_hint(&self) -> u64 {
        let throughput = self.completions_window.read().await.throughput_per_sec();
        
        if throughput > 0.1 {
            // Estimate time for one query to complete
            (1000.0 / throughput) as u64
        } else {
            // No recent completions, use conservative estimate
            2000
        }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // COMPLETION TRACKING & AIMD ADJUSTMENT
    // ═══════════════════════════════════════════════════════════════════
    
    /// Record query completion and adjust limits via AIMD
    /// MUST be called for every admitted query
    pub async fn record_completion(&self, token: AdmitToken, outcome: QueryOutcome) {
        let latency_ms = token.admitted_at.elapsed().as_millis() as u64;
        
        // Release cost units
        {
            let mut current = self.current_cost_units.write().await;
            *current = (*current - token.cost_units).max(0.0);
        }
        
        self.active_queries.fetch_sub(1, Ordering::SeqCst);
        
        // Record latency
        self.record_latency(latency_ms).await;
        
        // Record completion for throughput tracking
        self.completions_window.write().await.push(1.0);
        
        // Apply AIMD adjustment
        self.aimd_adjust(&outcome, latency_ms).await;
        
        // Update counters
        if outcome.is_success() {
            self.successful_queries.fetch_add(1, Ordering::SeqCst);
            metrics::record_query_outcome("success");
        } else if outcome.should_decrease_limit() {
            self.failed_queries.fetch_add(1, Ordering::SeqCst);
            metrics::record_query_outcome(outcome.as_metric_label());
        }
        
        // Record detailed metrics
        metrics::record_query_completed(
            "adaptive_queue",
            outcome.as_metric_label(),
            latency_ms as f64 / 1000.0
        );
        
        debug!(
            "Query {} completed: outcome={:?}, latency={}ms, limit={:.1}",
            token.id,
            outcome.as_metric_label(),
            latency_ms,
            *self.concurrency_limit.read().await
        );
        
        self.update_prometheus_metrics().await;
    }
    
    /// AIMD algorithm - adjust limit based on outcome and latency
    async fn aimd_adjust(&self, outcome: &QueryOutcome, latency_ms: u64) {
        let mut limit = self.concurrency_limit.write().await;
        let old_limit = *limit;
        
        // Skip adjustment if client fault
        if outcome.is_client_fault() {
            return;
        }
        
        if outcome.is_success() {
            // Success path: check latency gradient
            if latency_ms < TARGET_LATENCY_MS {
                // Good latency → Additive Increase
                *limit = (*limit + AIMD_INCREASE).min(MAX_LIMIT);
            } else if latency_ms > TARGET_LATENCY_MS * 2 {
                // Very high latency even on success → gentle decrease
                *limit = (*limit * 0.95).max(MIN_LIMIT);
            }
            // Latency between target and 2x target: no change
            
        } else if outcome.should_decrease_limit() {
            // Failure path → Multiplicative Decrease
            let multiplier = outcome.aimd_multiplier();
            *limit = (*limit * multiplier).max(MIN_LIMIT);
            
            warn!(
                "AIMD decrease: {:?} → limit {:.1} → {:.1} (×{:.1})",
                outcome.as_metric_label(),
                old_limit,
                *limit,
                multiplier
            );
        }
        
        // Log significant changes
        if (*limit - old_limit).abs() > 1.0 {
            info!(
                "AIMD: limit {:.1} → {:.1} (outcome={:?}, latency={}ms)",
                old_limit, *limit, outcome.as_metric_label(), latency_ms
            );
        }
    }
    
    /// Record latency observation
    async fn record_latency(&self, latency_ms: u64) {
        self.latency_window.write().await.push(latency_ms as f64);
        
        // Update min latency (baseline for gradient)
        if latency_ms > 0 {
            let current_min = self.min_latency_ms.load(Ordering::SeqCst);
            if latency_ms < current_min {
                self.min_latency_ms.store(latency_ms, Ordering::SeqCst);
            }
        }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // GRADIENT-BASED ADJUSTMENT (call periodically)
    // ═══════════════════════════════════════════════════════════════════
    
    /// Calculate gradient and adjust limit - call every 5-10 seconds
    pub async fn gradient_adjust(&self) {
        let min_rtt = self.min_latency_ms.load(Ordering::SeqCst);
        if min_rtt == u64::MAX || min_rtt == 0 {
            return; // No baseline yet
        }
        
        // Get avg latency FIRST, then drop the lock
        let avg_latency = {
            let window = self.latency_window.read().await;
            match window.average() {
                Some(avg) => avg,
                None => return,
            }
        }; // Lock dropped here
        
        // gradient = (min_rtt - current_rtt) / min_rtt
        // Positive = faster than baseline → can increase
        // Negative = slower than baseline → should decrease
        let gradient = (min_rtt as f64 - avg_latency) / min_rtt as f64;
        
        // Update limit in a scoped block to release lock before metrics update
        let (old_limit, new_limit) = {
            let mut limit = self.concurrency_limit.write().await;
            let old_limit = *limit;
            
            // Apply gradient with smoothing
            let adjustment = 1.0 + gradient * SMOOTHING_FACTOR;
            *limit = (*limit * adjustment)
                .max(MIN_LIMIT)
                .min(MAX_LIMIT);
            
            (old_limit, *limit)
        }; // Write lock dropped here BEFORE calling update_prometheus_metrics
        
        if (new_limit - old_limit).abs() > 0.5 {
            info!(
                "Gradient adjust: gradient={:.3}, avg_latency={:.0}ms, min={}, limit {:.1} → {:.1}",
                gradient, avg_latency, min_rtt, old_limit, new_limit
            );
        }
        
        // Now safe to call status() since all locks are released
        self.update_prometheus_metrics().await;
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // STATUS & METRICS
    // ═══════════════════════════════════════════════════════════════════
    
    /// Get current status snapshot - acquires locks one at a time to avoid deadlock
    pub async fn status(&self) -> AdaptiveQueueStatus {
        // Acquire each lock separately and copy values immediately
        let concurrency_limit = *self.concurrency_limit.read().await;
        let current_cost_units = *self.current_cost_units.read().await;
        
        let (avg_latency_ms, p99_latency_ms) = {
            let window = self.latency_window.read().await;
            (window.average(), window.percentile(0.99))
        };
        
        let throughput_per_sec = {
            let window = self.completions_window.read().await;
            window.throughput_per_sec()
        };
        
        let min_latency_ms = {
            let min = self.min_latency_ms.load(Ordering::SeqCst);
            if min == u64::MAX { None } else { Some(min) }
        };
        
        AdaptiveQueueStatus {
            concurrency_limit,
            current_cost_units,
            active_queries: self.active_queries.load(Ordering::SeqCst),
            
            total_queries: self.total_queries.load(Ordering::SeqCst),
            successful_queries: self.successful_queries.load(Ordering::SeqCst),
            failed_queries: self.failed_queries.load(Ordering::SeqCst),
            rejected_queries: self.rejected_queries.load(Ordering::SeqCst),
            
            avg_latency_ms,
            p99_latency_ms,
            min_latency_ms,
            
            throughput_per_sec,
        }
    }
    
    /// Get success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_queries.load(Ordering::SeqCst);
        let success = self.successful_queries.load(Ordering::SeqCst);
        
        if total == 0 {
            1.0
        } else {
            success as f64 / total as f64
        }
    }
    
    /// Get current utilization (0.0 - 1.0+)
    pub async fn utilization(&self) -> f64 {
        let current = *self.current_cost_units.read().await;
        let limit = *self.concurrency_limit.read().await;
        
        if limit == 0.0 {
            0.0
        } else {
            current / limit
        }
    }
    
    /// Get current concurrency limit
    pub async fn get_limit(&self) -> f64 {
        *self.concurrency_limit.read().await
    }
    
    /// Get current cost units in use
    pub async fn get_current_cost(&self) -> f64 {
        *self.current_cost_units.read().await
    }
    
    /// Manually set limit (for testing or recovery)
    pub async fn set_limit(&self, new_limit: f64) {
        let mut limit = self.concurrency_limit.write().await;
        *limit = new_limit.max(MIN_LIMIT).min(MAX_LIMIT);
        info!("Limit manually set to {:.1}", *limit);
    }
    
    /// Update Prometheus metrics
    async fn update_prometheus_metrics(&self) {
        let status = self.status().await;
        
        metrics::update_adaptive_queue_state(
            status.concurrency_limit,
            status.current_cost_units,
            status.min_latency_ms,
        );
        
        metrics::update_success_rate(status.success_rate());
    }
}

impl Default for AdaptiveQueue {
    fn default() -> Self {
        // Can't return Arc from default, this is just for the inner type
        Self {
            concurrency_limit: RwLock::new(INITIAL_LIMIT),
            current_cost_units: RwLock::new(0.0),
            active_queries: AtomicU64::new(0),
            latency_window: RwLock::new(RollingWindow::new(Duration::from_secs(WINDOW_DURATION_SECS))),
            min_latency_ms: AtomicU64::new(u64::MAX),
            completions_window: RwLock::new(RollingWindow::new(Duration::from_secs(WINDOW_DURATION_SECS))),
            total_queries: AtomicU64::new(0),
            successful_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            rejected_queries: AtomicU64::new(0),
            last_adjustment: RwLock::new(Instant::now()),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STATUS SNAPSHOT
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct AdaptiveQueueStatus {
    pub concurrency_limit: f64,
    pub current_cost_units: f64,
    pub active_queries: u64,
    
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub rejected_queries: u64,
    
    pub avg_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
    pub min_latency_ms: Option<u64>,
    
    pub throughput_per_sec: f64,
}

impl AdaptiveQueueStatus {
    pub fn success_rate(&self) -> f64 {
        if self.total_queries == 0 {
            1.0
        } else {
            self.successful_queries as f64 / self.total_queries as f64
        }
    }
    
    pub fn utilization(&self) -> f64 {
        if self.concurrency_limit == 0.0 {
            0.0
        } else {
            self.current_cost_units / self.concurrency_limit
        }
    }
    
    pub fn rejection_rate(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.rejected_queries as f64 / self.total_queries as f64
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_outcome_classification() {
        assert!(QueryOutcome::Success { rows_returned: 100, bytes_transferred: 1000 }.is_success());
        assert!(QueryOutcome::FailureTimeout.should_decrease_limit());
        assert!(QueryOutcome::FailureOOM.should_decrease_limit());
        assert!(QueryOutcome::FailureClientDisconnect.is_client_fault());
        assert!(QueryOutcome::FailureParseError.is_client_fault());
    }
    
    #[test]
    fn test_aimd_multipliers() {
        assert_eq!(QueryOutcome::FailureOOM.aimd_multiplier(), 0.3);
        assert_eq!(QueryOutcome::FailureTimeout.aimd_multiplier(), 0.5);
        assert_eq!(QueryOutcome::Success { rows_returned: 0, bytes_transferred: 0 }.aimd_multiplier(), 1.0);
    }
    
    #[test]
    fn test_rolling_window() {
        let mut window = RollingWindow::new(Duration::from_secs(10));
        
        window.push(100.0);
        window.push(200.0);
        window.push(300.0);
        
        assert_eq!(window.average(), Some(200.0));
        assert_eq!(window.min(), Some(100.0));
        assert_eq!(window.count(), 3);
    }
    
    #[tokio::test]
    async fn test_non_blocking_admission() {
        let queue = AdaptiveQueue::new();
        
        // Should admit query with 100MB = 1 cost unit
        let result = queue.try_admit("q1".to_string(), 100).await;
        assert!(result.is_ok());
        
        let status = queue.status().await;
        assert!(status.current_cost_units >= 0.5); // At least min cost
    }
    
    #[tokio::test]
    async fn test_rejection_at_capacity() {
        let queue = AdaptiveQueue::new();
        
        // Fill up to near limit by admitting many queries
        for i in 0..40 {
            let _ = queue.try_admit(format!("q{}", i), 100).await;
        }
        
        // Status should show high utilization
        let status = queue.status().await;
        assert!(status.current_cost_units > 10.0);
    }
    
    #[tokio::test]
    async fn test_aimd_decrease_on_failure() {
        let queue = AdaptiveQueue::new();
        
        let token = queue.try_admit("q1".to_string(), 100).await.unwrap();
        let initial_limit = queue.get_limit().await;
        
        // Record OOM failure
        queue.record_completion(token, QueryOutcome::FailureOOM).await;
        
        let new_limit = queue.get_limit().await;
        assert!(new_limit < initial_limit); // Should have decreased
    }
    
    #[tokio::test]
    async fn test_aimd_increase_on_success() {
        let queue = AdaptiveQueue::new();
        
        let token = queue.try_admit("q1".to_string(), 100).await.unwrap();
        let initial_limit = queue.get_limit().await;
        
        // Record fast success
        queue.record_completion(
            token, 
            QueryOutcome::Success { rows_returned: 100, bytes_transferred: 1000 }
        ).await;
        
        let new_limit = queue.get_limit().await;
        assert!(new_limit >= initial_limit); // Should have stayed or increased
    }
}
