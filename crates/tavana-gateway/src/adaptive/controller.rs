//! Adaptive Scaling Controller
//!
//! Main controller that coordinates all adaptive scaling decisions
//! Uses EMA (Exponential Moving Average) of actual query patterns

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use super::config::AdaptiveConfig;
use super::event_calendar;
use super::history::QueryHistory;
use super::hpa_manager::HpaManager;
use super::load_metrics::{ClusterMetrics, LoadMetricsCollector};
use super::time_patterns;
use crate::metrics;

/// Current adaptive state
#[derive(Debug, Clone)]
pub struct AdaptiveState {
    /// Current threshold in MB
    pub threshold_mb: u64,
    /// Current HPA min replicas
    pub hpa_min: u32,
    /// Current HPA max replicas
    pub hpa_max: u32,
    /// Active factors
    pub factors: AdaptiveFactors,
    /// Active events
    pub active_events: Vec<String>,
    /// Cluster metrics
    pub cluster_metrics: ClusterMetrics,
}

/// Individual adjustment factors
#[derive(Debug, Clone, Default)]
pub struct AdaptiveFactors {
    pub time_factor: f64,
    pub day_factor: f64,
    pub load_factor: f64,
    pub event_factor: f64,
    pub history_factor: f64,  // EMA-based factor from query history
    pub combined_threshold_factor: f64,
    pub combined_hpa_factor: f64,
}

/// Adaptive Scaling Controller
pub struct AdaptiveController {
    config: Arc<RwLock<AdaptiveConfig>>,
    current_state: Arc<RwLock<AdaptiveState>>,
    metrics_collector: Arc<LoadMetricsCollector>,
    hpa_manager: Arc<HpaManager>,
    /// Query history for EMA-based adaptation
    query_history: Arc<QueryHistory>,
    // Atomic for fast reads in hot path
    current_threshold_mb: AtomicU64,
}

impl AdaptiveController {
    /// Create a new adaptive controller
    pub async fn new(config: AdaptiveConfig, namespace: String) -> Self {
        let initial_threshold = (config.base_threshold_gb * 1024.0) as u64;

        let metrics_collector = Arc::new(LoadMetricsCollector::new(namespace.clone()).await);
        let hpa_manager = Arc::new(HpaManager::new(namespace, "worker-hpa".to_string()).await);
        let query_history = Arc::new(QueryHistory::new());

        let initial_state = AdaptiveState {
            threshold_mb: initial_threshold,
            hpa_min: config.base_hpa_min,
            hpa_max: config.base_hpa_max,
            factors: AdaptiveFactors {
                time_factor: 1.0,
                day_factor: 1.0,
                load_factor: 1.0,
                event_factor: 1.0,
                history_factor: 1.0,
                combined_threshold_factor: 1.0,
                combined_hpa_factor: 1.0,
            },
            active_events: vec![],
            cluster_metrics: ClusterMetrics::default(),
        };

        Self {
            config: Arc::new(RwLock::new(config)),
            current_state: Arc::new(RwLock::new(initial_state)),
            metrics_collector,
            hpa_manager,
            query_history,
            current_threshold_mb: AtomicU64::new(initial_threshold),
        }
    }
    
    /// Get query history for recording completed queries
    pub fn get_history(&self) -> Arc<QueryHistory> {
        Arc::clone(&self.query_history)
    }

    /// Get current threshold (fast, atomic read)
    pub fn get_threshold_mb(&self) -> u64 {
        self.current_threshold_mb.load(Ordering::Relaxed)
    }

    /// Get full current state
    pub async fn get_state(&self) -> AdaptiveState {
        self.current_state.read().await.clone()
    }

    /// Start the background recalculation loop
    pub fn start_background_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let config = self.config.read().await;
            let recalc_interval = config.recalc_interval_secs;
            drop(config);

            let mut ticker = interval(Duration::from_secs(recalc_interval));

            loop {
                ticker.tick().await;
                if let Err(e) = self.recalculate().await {
                    warn!("Adaptive recalculation failed: {}", e);
                }
            }
        });
    }

    /// Recalculate adaptive state
    pub async fn recalculate(&self) -> anyhow::Result<()> {
        let config = self.config.read().await;
        
        // 1. Get cluster metrics
        let cluster_metrics = self.metrics_collector.get_metrics().await;
        debug!("Cluster metrics: utilization={:.2}", cluster_metrics.combined_utilization);

        // 2. Calculate time-based factors
        let time_threshold_factor = time_patterns::time_of_day_factor(&config);
        let time_hpa_factor = time_patterns::time_of_day_hpa_factor(&config);

        // 3. Calculate day-based factors
        let day_threshold_factor = time_patterns::day_of_week_factor(&config);
        let day_hpa_factor = time_patterns::day_of_week_hpa_factor(&config);

        // 4. Calculate load-based factors
        let (load_threshold_factor, load_hpa_factor) = 
            self.metrics_collector.calculate_factors(&cluster_metrics, &config);

        // 5. Calculate event factors
        let event_threshold_factor = event_calendar::event_threshold_factor(&config);
        let event_hpa_factor = event_calendar::event_hpa_factor(&config);
        let active_events = event_calendar::get_active_events(&config);

        // 6. Calculate history-based EMA factor (most important!)
        let history_factor = self.query_history.calculate_threshold_factor(config.base_threshold_gb);
        let emas = self.query_history.get_emas();
        debug!(
            "EMA stats: size={:.1}MB, freq={:.1}qpm, accuracy={:.2}",
            emas.size_ema_mb, emas.frequency_ema_qpm, emas.accuracy_ema
        );

        // 7. Combine factors
        // History factor is weighted highest (50%), then load (30%), then time/day/event (20%)
        let combined_threshold_factor = 
            (history_factor * 0.5) +
            (load_threshold_factor * 0.3) +
            (time_threshold_factor * day_threshold_factor * event_threshold_factor * 0.2);

        // For HPA: take weighted average with priority to load and events
        let combined_hpa_factor = time_hpa_factor * 0.2
            + day_hpa_factor * 0.1
            + load_hpa_factor * 0.4
            + event_hpa_factor * 0.3;

        // 7. Calculate final values
        let threshold_gb = config.base_threshold_gb * combined_threshold_factor;
        let threshold_gb = threshold_gb.clamp(config.min_threshold_gb, config.max_threshold_gb);
        let threshold_mb = (threshold_gb * 1024.0) as u64;

        let hpa_min = (config.base_hpa_min as f64 * combined_hpa_factor) as u32;
        let hpa_min = hpa_min.clamp(config.absolute_hpa_min, config.base_hpa_max);

        let hpa_max = (config.base_hpa_max as f64 * combined_hpa_factor) as u32;
        let hpa_max = hpa_max.clamp(hpa_min, config.absolute_hpa_max);

        // 8. Update state
        let factors = AdaptiveFactors {
            time_factor: time_threshold_factor,
            day_factor: day_threshold_factor,
            load_factor: load_threshold_factor,
            event_factor: event_threshold_factor,
            history_factor,
            combined_threshold_factor,
            combined_hpa_factor,
        };

        let new_state = AdaptiveState {
            threshold_mb,
            hpa_min,
            hpa_max,
            factors: factors.clone(),
            active_events: active_events.clone(),
            cluster_metrics: cluster_metrics.clone(),
        };

        // Update atomic threshold
        self.current_threshold_mb.store(threshold_mb, Ordering::Relaxed);

        // Update full state
        {
            let mut state = self.current_state.write().await;
            *state = new_state;
        }

        // 9. Update Prometheus metrics
        metrics::update_adaptive_state(
            threshold_mb as f64,
            hpa_min as f64,
            hpa_max as f64,
        );
        metrics::update_adaptive_factor("time", time_threshold_factor);
        metrics::update_adaptive_factor("day", day_threshold_factor);
        metrics::update_adaptive_factor("load", load_threshold_factor);
        metrics::update_adaptive_factor("event", event_threshold_factor);
        metrics::update_adaptive_factor("history", history_factor);
        metrics::update_worker_pool_metrics(
            cluster_metrics.cpu_utilization,
            cluster_metrics.memory_utilization,
            cluster_metrics.desired_replicas as i32,
            cluster_metrics.ready_replicas as i32,
            cluster_metrics.ready_replicas as i32,
        );

        // 10. Update HPA if available
        if self.hpa_manager.is_available() {
            // Only update if significantly different from current
            if let Ok(current_hpa) = self.hpa_manager.get_current_state().await {
                if current_hpa.min_replicas != hpa_min || current_hpa.max_replicas != hpa_max {
                    if let Err(e) = self.hpa_manager.update_replicas(hpa_min, hpa_max).await {
                        warn!("Failed to update HPA: {}", e);
                    }
                }
            }
        }

        info!(
            "Adaptive state updated: threshold={}GB, hpa_min={}, hpa_max={}, events={:?}",
            threshold_gb, hpa_min, hpa_max, active_events
        );

        drop(config);
        Ok(())
    }

    /// Update configuration
    pub async fn update_config(&self, config: AdaptiveConfig) {
        let mut cfg = self.config.write().await;
        *cfg = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_controller_creation() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config, "tavana".to_string()).await;
        
        // Default threshold should be 2GB = 2048MB
        assert_eq!(controller.get_threshold_mb(), 2048);
    }
}

