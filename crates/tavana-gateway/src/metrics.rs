//! Prometheus Metrics for Tavana Gateway
//!
//! Exports metrics for:
//! - Adaptive scaling state (threshold, HPA settings)
//! - Query routing decisions
//! - Query execution performance
//! - Estimation accuracy

use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram,
    register_histogram_vec, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramVec,
    TextEncoder,
};

// ═══════════════════════════════════════════════════════════════════════════
// ADAPTIVE SCALING METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Current routing threshold in MB
pub static ADAPTIVE_THRESHOLD_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_threshold_mb",
        "Current routing threshold in megabytes"
    )
    .unwrap()
});

/// Current HPA minimum replicas
pub static ADAPTIVE_HPA_MIN: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_hpa_min_replicas",
        "Current HPA minimum replicas setting"
    )
    .unwrap()
});

/// Current HPA maximum replicas
pub static ADAPTIVE_HPA_MAX: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_hpa_max_replicas",
        "Current HPA maximum replicas setting"
    )
    .unwrap()
});

/// Active adjustment factors by type (time, load, day, event)
pub static ADAPTIVE_FACTOR: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_adaptive_factor",
        "Active adjustment factors",
        &["factor_type"]
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// QUERY ROUTING METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Total queries routed by target
pub static QUERIES_ROUTED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_queries_routed_total",
        "Total queries routed by target",
        &["target"]
    )
    .unwrap()
});

/// Query estimated data size in MB
pub static QUERY_ESTIMATED_SIZE_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_query_estimated_size_mb",
        "Estimated query data size in megabytes",
        vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]
    )
    .unwrap()
});

/// Query actual data size in MB (after execution)
pub static QUERY_ACTUAL_SIZE_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_query_actual_size_mb",
        "Actual query data size in megabytes",
        vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]
    )
    .unwrap()
});

/// Estimation accuracy ratio (actual / estimated)
pub static ESTIMATION_ACCURACY_RATIO: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_estimation_accuracy_ratio",
        "Ratio of actual to estimated resources (1.0 = perfect)",
        vec![0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0]
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// QUERY EXECUTION METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Query latency by route
pub static QUERY_LATENCY_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "tavana_query_latency_seconds",
        "Query execution latency in seconds",
        &["route", "status"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    )
    .unwrap()
});

/// Query throughput by status
pub static QUERY_THROUGHPUT_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_query_throughput_total",
        "Total queries processed by status",
        &["status"]
    )
    .unwrap()
});

/// Total data scanned in bytes
pub static DATA_SCANNED_BYTES_TOTAL: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "tavana_data_scanned_bytes_total",
        "Total bytes scanned across all queries"
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// EPHEMERAL POD METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Currently active ephemeral pods
pub static EPHEMERAL_PODS_ACTIVE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_ephemeral_pods_active",
        "Currently running ephemeral query pods"
    )
    .unwrap()
});

/// Ephemeral pod startup time (CRD creation to pod running)
pub static EPHEMERAL_POD_STARTUP_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_ephemeral_pod_startup_seconds",
        "Time from CRD creation to pod running",
        vec![1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0]
    )
    .unwrap()
});

/// Ephemeral pod execution duration
pub static EPHEMERAL_POD_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_ephemeral_pod_duration_seconds",
        "Ephemeral pod total execution duration",
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0]
    )
    .unwrap()
});

/// Ephemeral pod memory allocation in MB
pub static EPHEMERAL_POD_MEMORY_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_ephemeral_pod_memory_mb",
        "Memory allocated to ephemeral pods in MB",
        vec![512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0]
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// WORKER POOL METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Worker pool CPU utilization (0-1)
pub static WORKER_POOL_CPU_UTILIZATION: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_worker_pool_cpu_utilization",
        "Worker pool average CPU utilization ratio"
    )
    .unwrap()
});

/// Worker pool memory utilization (0-1)
pub static WORKER_POOL_MEMORY_UTILIZATION: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_worker_pool_memory_utilization",
        "Worker pool average memory utilization ratio"
    )
    .unwrap()
});

/// Worker replica counts
pub static WORKER_REPLICAS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_worker_replicas",
        "Worker replica counts",
        &["state"]
    )
    .unwrap()
});

/// Worker query queue depth
pub static WORKER_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_worker_queue_depth",
        "Number of queries waiting in worker queue"
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// DATA ESTIMATION METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Estimation method used
pub static ESTIMATION_METHOD_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_estimation_method_total",
        "Estimation methods used",
        &["method"]
    )
    .unwrap()
});

/// S3 HEAD request latency
pub static S3_HEAD_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_s3_head_latency_seconds",
        "S3 HEAD request latency",
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
    )
    .unwrap()
});

/// Cache hit rate
pub static CACHE_HITS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!("tavana_cache_total", "Cache hits and misses", &["result"]).unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// RESOURCE ESTIMATION METRICS (for ML training data collection)
// ═══════════════════════════════════════════════════════════════════════════

/// Last resource estimate: memory in MB
pub static RESOURCE_ESTIMATE_MEMORY_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_resource_estimate_memory_mb",
        "Last calculated memory estimate in MB for ephemeral pod"
    )
    .unwrap()
});

/// Last resource estimate: CPU in millicores
pub static RESOURCE_ESTIMATE_CPU_MILLICORES: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_resource_estimate_cpu_millicores",
        "Last calculated CPU estimate in millicores for ephemeral pod"
    )
    .unwrap()
});

/// Last resource estimate: data size in MB
pub static RESOURCE_ESTIMATE_DATA_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_resource_estimate_data_mb",
        "Last estimated data size in MB"
    )
    .unwrap()
});

/// Last resource estimate: memory multiplier
pub static RESOURCE_ESTIMATE_MULTIPLIER: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_resource_estimate_multiplier",
        "Last calculated memory multiplier from query complexity"
    )
    .unwrap()
});

/// Resource allocation histogram: memory by query type
pub static RESOURCE_ALLOC_MEMORY_MB: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "tavana_resource_alloc_memory_mb",
        "Memory allocated to queries by operation type",
        &["has_join", "has_agg", "has_window"],
        vec![1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0]
    )
    .unwrap()
});

/// Resource allocation histogram: CPU by query type
pub static RESOURCE_ALLOC_CPU_MILLICORES: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "tavana_resource_alloc_cpu_millicores",
        "CPU allocated to queries by operation type",
        &["has_join", "has_agg", "has_window"],
        vec![1000.0, 2000.0, 4000.0, 8000.0, 16000.0, 32000.0, 64000.0, 128000.0]
    )
    .unwrap()
});

/// Data characteristics: row count
pub static DATA_ROW_COUNT: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_data_row_count",
        "Row count from data sources",
        vec![
            1000.0,
            10000.0,
            100000.0,
            1000000.0,
            10000000.0,
            100000000.0,
            1000000000.0
        ]
    )
    .unwrap()
});

/// Data characteristics: column count
pub static DATA_COLUMN_COUNT: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_data_column_count",
        "Column count from data sources",
        vec![5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0]
    )
    .unwrap()
});

/// Data characteristics: row group count
pub static DATA_ROW_GROUP_COUNT: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_data_row_group_count",
        "Row group count from Parquet files",
        vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]
    )
    .unwrap()
});

/// Actual vs estimated resource ratio (for ML model accuracy tracking)
pub static RESOURCE_ACTUAL_VS_ESTIMATED: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "tavana_resource_actual_vs_estimated",
        "Ratio of actual to estimated resources",
        &["resource_type"],
        vec![0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0]
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// PRE-SIZING METRICS (K8s v1.35 In-Place Resource Updates)
// ═══════════════════════════════════════════════════════════════════════════

/// Total number of worker pre-sizing operations
pub static WORKER_PRESIZE_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_worker_presize_total",
        "Total worker pre-sizing operations by result",
        &["result"] // "success", "failed", "skipped"
    )
    .unwrap()
});

/// Memory requested during pre-sizing (MB)
pub static WORKER_PRESIZE_MEMORY_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_worker_presize_memory_mb",
        "Memory requested during worker pre-sizing in MB",
        vec![
            256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0,
            262144.0, 409600.0
        ]
    )
    .unwrap()
});

/// CPU requested during pre-sizing (cores)
pub static WORKER_PRESIZE_CPU_CORES: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_worker_presize_cpu_cores",
        "CPU cores requested during worker pre-sizing",
        vec![0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]
    )
    .unwrap()
});

/// Pre-sizing latency (time to resize worker pod)
pub static WORKER_PRESIZE_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_worker_presize_latency_seconds",
        "Time taken to pre-size worker pod",
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
    )
    .unwrap()
});

/// Current worker pool status
pub static WORKER_POOL_STATUS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_worker_pool_status",
        "Worker pool status by state",
        &["state"] // "total", "busy", "idle", "resizing"
    )
    .unwrap()
});

/// Pre-sizing memory utilization ratio (actual_used / pre_sized)
pub static PRESIZE_MEMORY_UTILIZATION: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_presize_memory_utilization",
        "Ratio of actual memory used vs pre-sized memory",
        vec![0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0]
    )
    .unwrap()
});

/// Pre-sizing multiplier used
pub static PRESIZE_MULTIPLIER_USED: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_presize_multiplier_used",
        "Pre-sizing multiplier applied to estimated data size",
        vec![0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0]
    )
    .unwrap()
});

/// In-place resize events by type
pub static INPLACE_RESIZE_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_inplace_resize_total",
        "In-place resize events by direction",
        &["direction"] // "scale_up", "scale_down", "initial"
    )
    .unwrap()
});

/// Memory before resize (for tracking deltas)
pub static RESIZE_MEMORY_DELTA_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_resize_memory_delta_mb",
        "Memory change during resize (MB, can be negative for scale-down)",
        vec![
            -4096.0, -2048.0, -1024.0, -512.0, 0.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0
        ]
    )
    .unwrap()
});

/// Elastic scale-up events during query execution
pub static ELASTIC_SCALEUP_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_elastic_scaleup_total",
        "Elastic scale-up events during query execution",
        &["trigger"] // "memory_pressure", "oom_risk", "cpu_throttle"
    )
    .unwrap()
});

/// VPA recommendations vs actual allocation
pub static VPA_RECOMMENDATION_MB: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_vpa_recommendation_mb",
        "VPA memory recommendations in MB",
        &["type"] // "target", "lower_bound", "upper_bound", "uncapped_target"
    )
    .unwrap()
});

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Initialize all metrics (call at startup)
pub fn init_metrics() {
    // Touch all lazy statics to register them
    let _ = &*ADAPTIVE_THRESHOLD_MB;
    let _ = &*ADAPTIVE_HPA_MIN;
    let _ = &*ADAPTIVE_HPA_MAX;
    let _ = &*ADAPTIVE_FACTOR;
    let _ = &*QUERIES_ROUTED_TOTAL;
    let _ = &*QUERY_ESTIMATED_SIZE_MB;
    let _ = &*QUERY_ACTUAL_SIZE_MB;
    let _ = &*ESTIMATION_ACCURACY_RATIO;
    let _ = &*QUERY_LATENCY_SECONDS;
    let _ = &*QUERY_THROUGHPUT_TOTAL;
    let _ = &*DATA_SCANNED_BYTES_TOTAL;
    let _ = &*EPHEMERAL_PODS_ACTIVE;
    let _ = &*EPHEMERAL_POD_STARTUP_SECONDS;
    let _ = &*EPHEMERAL_POD_DURATION_SECONDS;
    let _ = &*EPHEMERAL_POD_MEMORY_MB;
    let _ = &*WORKER_POOL_CPU_UTILIZATION;
    let _ = &*WORKER_POOL_MEMORY_UTILIZATION;
    let _ = &*WORKER_REPLICAS;
    let _ = &*WORKER_QUEUE_DEPTH;
    let _ = &*ESTIMATION_METHOD_TOTAL;
    let _ = &*S3_HEAD_LATENCY_SECONDS;
    let _ = &*CACHE_HITS_TOTAL;
    // Resource estimation metrics
    let _ = &*RESOURCE_ESTIMATE_MEMORY_MB;
    let _ = &*RESOURCE_ESTIMATE_CPU_MILLICORES;
    let _ = &*RESOURCE_ESTIMATE_DATA_MB;
    let _ = &*RESOURCE_ESTIMATE_MULTIPLIER;
    let _ = &*RESOURCE_ALLOC_MEMORY_MB;
    let _ = &*RESOURCE_ALLOC_CPU_MILLICORES;
    let _ = &*DATA_ROW_COUNT;
    let _ = &*DATA_COLUMN_COUNT;
    let _ = &*DATA_ROW_GROUP_COUNT;
    let _ = &*RESOURCE_ACTUAL_VS_ESTIMATED;
    // Pre-sizing metrics (K8s v1.35)
    let _ = &*WORKER_PRESIZE_TOTAL;
    let _ = &*WORKER_PRESIZE_MEMORY_MB;
    let _ = &*WORKER_PRESIZE_CPU_CORES;
    let _ = &*WORKER_PRESIZE_LATENCY_SECONDS;
    let _ = &*WORKER_POOL_STATUS;
    let _ = &*PRESIZE_MEMORY_UTILIZATION;
    let _ = &*PRESIZE_MULTIPLIER_USED;
    let _ = &*INPLACE_RESIZE_TOTAL;
    let _ = &*RESIZE_MEMORY_DELTA_MB;
    let _ = &*ELASTIC_SCALEUP_TOTAL;
    let _ = &*VPA_RECOMMENDATION_MB;
    // Queue metrics
    let _ = &*QUERY_QUEUE_DEPTH;
    let _ = &*QUERY_QUEUE_DEPTH_BY_PRIORITY;
    let _ = &*QUERY_QUEUE_REJECTED_TOTAL;
    let _ = &*QUERY_QUEUE_TIMEOUT_TOTAL;
    let _ = &*QUERY_QUEUE_WAIT_SECONDS;
    // Active query metrics (for HPA/KEDA)
    let _ = &*ACTIVE_QUERIES;
    let _ = &*ACTIVE_QUERIES_PER_WORKER;
    let _ = &*QUERY_RATE;
    let _ = &*ELASTIC_RESIZE_TOTAL;
    let _ = &*ELASTIC_RESIZE_MEMORY_MB;
    // Adaptive queue metrics
    let _ = &*QUERY_OUTCOME_TOTAL;
    let _ = &*ADAPTIVE_CONCURRENCY_LIMIT;
    let _ = &*ADAPTIVE_COST_UNITS_IN_USE;
    let _ = &*ADAPTIVE_AIMD_EVENTS;
    let _ = &*ADAPTIVE_AIMD_DELTA;
    let _ = &*ADAPTIVE_MIN_LATENCY_MS;
    let _ = &*QUERY_COST_UNITS;
    let _ = &*QUERY_SUCCESS_RATE;
    let _ = &*FAILURE_BREAKDOWN;

    // Set initial values
    ADAPTIVE_THRESHOLD_MB.set(2048.0); // 2GB default
    ADAPTIVE_HPA_MIN.set(2.0);
    ADAPTIVE_HPA_MAX.set(100.0);

    // Initialize worker pool status
    WORKER_POOL_STATUS.with_label_values(&["total"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["busy"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["idle"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["resizing"]).set(0.0);

    // Initialize queue metrics
    QUERY_QUEUE_DEPTH.set(0.0);
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["high"])
        .set(0.0);
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["normal"])
        .set(0.0);
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["low"])
        .set(0.0);
}

/// Encode all metrics as Prometheus text format
pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
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

/// Record query routing decision
pub fn record_query_routed(target: &str, estimated_size_mb: f64) {
    QUERIES_ROUTED_TOTAL.with_label_values(&[target]).inc();
    QUERY_ESTIMATED_SIZE_MB.observe(estimated_size_mb);
}

/// Record query completion
pub fn record_query_completed(route: &str, status: &str, duration_secs: f64) {
    QUERY_LATENCY_SECONDS
        .with_label_values(&[route, status])
        .observe(duration_secs);
    QUERY_THROUGHPUT_TOTAL.with_label_values(&[status]).inc();
}

/// Record estimation method used
pub fn record_estimation_method(method: &str) {
    ESTIMATION_METHOD_TOTAL.with_label_values(&[method]).inc();
}

/// Record cache access
pub fn record_cache_access(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    CACHE_HITS_TOTAL.with_label_values(&[result]).inc();
}

/// Update adaptive state metrics
pub fn update_adaptive_state(threshold_mb: f64, hpa_min: f64, hpa_max: f64) {
    ADAPTIVE_THRESHOLD_MB.set(threshold_mb);
    ADAPTIVE_HPA_MIN.set(hpa_min);
    ADAPTIVE_HPA_MAX.set(hpa_max);
}

/// Update adaptive factor
pub fn update_adaptive_factor(factor_type: &str, value: f64) {
    ADAPTIVE_FACTOR.with_label_values(&[factor_type]).set(value);
}

/// Update worker pool metrics
pub fn update_worker_pool_metrics(
    cpu_util: f64,
    mem_util: f64,
    desired: i32,
    ready: i32,
    available: i32,
) {
    WORKER_POOL_CPU_UTILIZATION.set(cpu_util);
    WORKER_POOL_MEMORY_UTILIZATION.set(mem_util);
    WORKER_REPLICAS
        .with_label_values(&["desired"])
        .set(desired as f64);
    WORKER_REPLICAS
        .with_label_values(&["ready"])
        .set(ready as f64);
    WORKER_REPLICAS
        .with_label_values(&["available"])
        .set(available as f64);
}

/// Record ephemeral pod creation
pub fn record_ephemeral_pod_created(memory_mb: f64) {
    EPHEMERAL_PODS_ACTIVE.inc();
    EPHEMERAL_POD_MEMORY_MB.observe(memory_mb);
}

/// Record ephemeral pod completion
pub fn record_ephemeral_pod_completed(startup_secs: f64, duration_secs: f64) {
    EPHEMERAL_PODS_ACTIVE.dec();
    EPHEMERAL_POD_STARTUP_SECONDS.observe(startup_secs);
    EPHEMERAL_POD_DURATION_SECONDS.observe(duration_secs);
}

/// Record ephemeral pod failure (decrements active count)
pub fn record_ephemeral_pod_failed() {
    EPHEMERAL_PODS_ACTIVE.dec();
}

/// Record data scanned in bytes
pub fn record_data_scanned(bytes: u64) {
    DATA_SCANNED_BYTES_TOTAL.inc_by(bytes as f64);
}

/// Record actual query size after execution
pub fn record_actual_query_size(size_mb: f64) {
    QUERY_ACTUAL_SIZE_MB.observe(size_mb);
}

/// Record estimation accuracy
pub fn record_estimation_accuracy(estimated_mb: f64, actual_mb: f64) {
    if estimated_mb > 0.0 {
        let ratio = actual_mb / estimated_mb;
        ESTIMATION_ACCURACY_RATIO.observe(ratio);
    }
}

/// Record resource estimation details for ML training
pub fn record_resource_estimation(
    data_mb: f64,
    memory_mb: f64,
    cpu_millicores: f64,
    multiplier: f64,
    has_join: bool,
    has_agg: bool,
    has_window: bool,
) {
    RESOURCE_ESTIMATE_DATA_MB.set(data_mb);
    RESOURCE_ESTIMATE_MEMORY_MB.set(memory_mb);
    RESOURCE_ESTIMATE_CPU_MILLICORES.set(cpu_millicores);
    RESOURCE_ESTIMATE_MULTIPLIER.set(multiplier);

    RESOURCE_ALLOC_MEMORY_MB
        .with_label_values(&[
            if has_join { "true" } else { "false" },
            if has_agg { "true" } else { "false" },
            if has_window { "true" } else { "false" },
        ])
        .observe(memory_mb);

    RESOURCE_ALLOC_CPU_MILLICORES
        .with_label_values(&[
            if has_join { "true" } else { "false" },
            if has_agg { "true" } else { "false" },
            if has_window { "true" } else { "false" },
        ])
        .observe(cpu_millicores);
}

// ═══════════════════════════════════════════════════════════════════════════
// PRE-SIZING HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Record worker pre-sizing operation
pub fn record_worker_presize(result: &str, memory_mb: f64, cpu_cores: f64, latency_secs: f64) {
    WORKER_PRESIZE_TOTAL.with_label_values(&[result]).inc();
    if result == "success" {
        WORKER_PRESIZE_MEMORY_MB.observe(memory_mb);
        WORKER_PRESIZE_CPU_CORES.observe(cpu_cores);
    }
    WORKER_PRESIZE_LATENCY_SECONDS.observe(latency_secs);
}

/// Update worker pool status
pub fn update_worker_pool_status(total: i32, busy: i32, idle: i32, resizing: i32) {
    WORKER_POOL_STATUS
        .with_label_values(&["total"])
        .set(total as f64);
    WORKER_POOL_STATUS
        .with_label_values(&["busy"])
        .set(busy as f64);
    WORKER_POOL_STATUS
        .with_label_values(&["idle"])
        .set(idle as f64);
    WORKER_POOL_STATUS
        .with_label_values(&["resizing"])
        .set(resizing as f64);
}

/// Record pre-sizing memory utilization after query completes
pub fn record_presize_memory_utilization(presized_mb: f64, actual_used_mb: f64) {
    if presized_mb > 0.0 {
        let utilization = actual_used_mb / presized_mb;
        PRESIZE_MEMORY_UTILIZATION.observe(utilization);
    }
}

/// Record the pre-sizing multiplier used
pub fn record_presize_multiplier(multiplier: f64) {
    PRESIZE_MULTIPLIER_USED.observe(multiplier);
}

/// Record in-place resize event
pub fn record_inplace_resize(direction: &str, memory_delta_mb: f64) {
    INPLACE_RESIZE_TOTAL.with_label_values(&[direction]).inc();
    RESIZE_MEMORY_DELTA_MB.observe(memory_delta_mb);
}

/// Record elastic scale-up during query execution
pub fn record_elastic_scaleup(trigger: &str) {
    ELASTIC_SCALEUP_TOTAL.with_label_values(&[trigger]).inc();
}

/// Update VPA recommendations
pub fn update_vpa_recommendations(
    target_mb: f64,
    lower_bound_mb: f64,
    upper_bound_mb: f64,
    uncapped_mb: f64,
) {
    VPA_RECOMMENDATION_MB
        .with_label_values(&["target"])
        .set(target_mb);
    VPA_RECOMMENDATION_MB
        .with_label_values(&["lower_bound"])
        .set(lower_bound_mb);
    VPA_RECOMMENDATION_MB
        .with_label_values(&["upper_bound"])
        .set(upper_bound_mb);
    VPA_RECOMMENDATION_MB
        .with_label_values(&["uncapped_target"])
        .set(uncapped_mb);
}

// ═══════════════════════════════════════════════════════════════════════════
// QUERY QUEUE METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Query queue depth
pub static QUERY_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_query_queue_depth",
        "Current number of queries waiting in the queue"
    )
    .unwrap()
});

/// Query queue depth by priority
pub static QUERY_QUEUE_DEPTH_BY_PRIORITY: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_query_queue_depth_by_priority",
        "Query queue depth by priority level",
        &["priority"] // "high", "normal", "low"
    )
    .unwrap()
});

/// Queries rejected due to queue full
pub static QUERY_QUEUE_REJECTED_TOTAL: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "tavana_query_queue_rejected_total",
        "Total queries rejected because queue was full"
    )
    .unwrap()
});

/// Queries that timed out in queue
pub static QUERY_QUEUE_TIMEOUT_TOTAL: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "tavana_query_queue_timeout_total",
        "Total queries that timed out while waiting in queue"
    )
    .unwrap()
});

/// Queue wait time
pub static QUERY_QUEUE_WAIT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_query_queue_wait_seconds",
        "Time queries spent waiting in queue",
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    )
    .unwrap()
});

/// Record queue depth
pub fn record_queue_depth(depth: usize) {
    QUERY_QUEUE_DEPTH.set(depth as f64);
}

/// Record queue depth by priority
pub fn record_queue_depth_by_priority(high: usize, normal: usize, low: usize) {
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["high"])
        .set(high as f64);
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["normal"])
        .set(normal as f64);
    QUERY_QUEUE_DEPTH_BY_PRIORITY
        .with_label_values(&["low"])
        .set(low as f64);
}

/// Record query rejected due to queue full
pub fn record_queue_rejected() {
    QUERY_QUEUE_REJECTED_TOTAL.inc();
}

/// Record query timeout in queue
pub fn record_queue_timeout() {
    QUERY_QUEUE_TIMEOUT_TOTAL.inc();
}

/// Record queue wait time
pub fn record_queue_wait_time(wait_seconds: f64) {
    QUERY_QUEUE_WAIT_SECONDS.observe(wait_seconds);
}

// ═══════════════════════════════════════════════════════════════════════════
// ACTIVE QUERY METRICS (for HPA scaling)
// ═══════════════════════════════════════════════════════════════════════════

/// Currently executing queries (for HPA/KEDA scaling)
pub static ACTIVE_QUERIES: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_active_queries",
        "Number of queries currently being executed (for HPA scaling)"
    )
    .unwrap()
});

/// Active queries per worker (for load balancing insights)
pub static ACTIVE_QUERIES_PER_WORKER: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_active_queries_per_worker",
        "Active queries per worker pod",
        &["worker"]
    )
    .unwrap()
});

/// Query rate (queries started per second, for HPA scaling)
pub static QUERY_RATE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_query_rate",
        "Queries started per second (moving average)"
    )
    .unwrap()
});

/// Elastic resize operations
pub static ELASTIC_RESIZE_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_elastic_resize_total",
        "Total elastic resize operations during query execution",
        &["result"] // "success", "failed", "at_limit"
    )
    .unwrap()
});

/// Elastic resize memory delta
pub static ELASTIC_RESIZE_MEMORY_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_elastic_resize_memory_mb",
        "Memory added during elastic resize (MB)",
        vec![64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0]
    )
    .unwrap()
});

/// Increment active query count (call when query starts)
pub fn query_started() {
    ACTIVE_QUERIES.inc();
}

/// Decrement active query count (call when query ends)
pub fn query_ended() {
    ACTIVE_QUERIES.dec();
}

/// Update active queries for a specific worker
pub fn update_worker_active_queries(worker: &str, count: i64) {
    ACTIVE_QUERIES_PER_WORKER
        .with_label_values(&[worker])
        .set(count as f64);
}

/// Update query rate
pub fn update_query_rate(rate: f64) {
    QUERY_RATE.set(rate);
}

/// Record elastic resize operation
pub fn record_elastic_resize(result: &str, memory_delta_mb: f64) {
    ELASTIC_RESIZE_TOTAL.with_label_values(&[result]).inc();
    if memory_delta_mb > 0.0 {
        ELASTIC_RESIZE_MEMORY_MB.observe(memory_delta_mb);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TENANT POOL METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Total tenant pools
pub static TENANT_POOLS_TOTAL: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_tenant_pools_total",
        "Total number of tenant-dedicated worker pools"
    )
    .unwrap()
});

/// Active tenant pools (with workers > 0)
pub static TENANT_POOLS_ACTIVE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_tenant_pools_active",
        "Number of active tenant pools (with running workers)"
    )
    .unwrap()
});

/// Workers per tenant
pub static TENANT_POOL_WORKERS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_tenant_pool_workers",
        "Number of workers in each tenant pool",
        &["tenant_id"]
    )
    .unwrap()
});

/// Queries per tenant
pub static TENANT_POOL_QUERIES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_tenant_pool_queries_total",
        "Total queries processed per tenant pool",
        &["tenant_id"]
    )
    .unwrap()
});

/// Tenant pool routing decisions
pub static TENANT_ROUTING_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_tenant_routing_total",
        "Routing decisions by type",
        &["type"] // "dedicated", "shared", "new_pool"
    )
    .unwrap()
});

/// Tenant pool creation events
pub static TENANT_POOL_CREATED_TOTAL: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "tavana_tenant_pool_created_total",
        "Total tenant pools created"
    )
    .unwrap()
});

/// Tenant pool deletion events
pub static TENANT_POOL_DELETED_TOTAL: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "tavana_tenant_pool_deleted_total",
        "Total tenant pools deleted (idle cleanup)"
    )
    .unwrap()
});

/// Record tenant pool created
pub fn record_tenant_pool_created(tenant_id: &str) {
    TENANT_POOL_CREATED_TOTAL.inc();
    TENANT_POOLS_TOTAL.inc();
    TENANT_POOLS_ACTIVE.inc();
    TENANT_POOL_WORKERS.with_label_values(&[tenant_id]).set(1.0);
}

/// Record tenant pool deleted
pub fn record_tenant_pool_deleted(tenant_id: &str) {
    TENANT_POOL_DELETED_TOTAL.inc();
    TENANT_POOLS_TOTAL.dec();
    TENANT_POOLS_ACTIVE.dec();
    TENANT_POOL_WORKERS.with_label_values(&[tenant_id]).set(0.0);
}

/// Record tenant query routed
pub fn record_tenant_query(tenant_id: &str, routing_type: &str) {
    TENANT_POOL_QUERIES.with_label_values(&[tenant_id]).inc();
    TENANT_ROUTING_TOTAL
        .with_label_values(&[routing_type])
        .inc();
}

/// Update tenant pool workers count
pub fn update_tenant_pool_workers(tenant_id: &str, count: i32) {
    TENANT_POOL_WORKERS
        .with_label_values(&[tenant_id])
        .set(count as f64);
}

// ═══════════════════════════════════════════════════════════════════════════
// ADAPTIVE QUEUE METRICS - Comprehensive Success/Failure Tracking
// ═══════════════════════════════════════════════════════════════════════════

/// Query outcome counter by type - CRITICAL for success rate calculation
pub static QUERY_OUTCOME_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_query_outcome_total",
        "Total queries by outcome type",
        &["outcome"] // Possible values:
                     // "success", "timeout", "oom", "connection_error", "worker_error",
                     // "client_disconnect", "parse_error", "auth_error", "unknown_error",
                     // "rejected_capacity", "rejected_queue_full", "rejected_too_large", "rejected_overload"
    )
    .unwrap()
});

/// Current adaptive concurrency limit (learned via AIMD)
pub static ADAPTIVE_CONCURRENCY_LIMIT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_concurrency_limit",
        "Current concurrency limit in cost units (learned via AIMD)"
    )
    .unwrap()
});

/// Current cost units in use
pub static ADAPTIVE_COST_UNITS_IN_USE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_cost_units_in_use",
        "Current cost units being used (in-flight queries)"
    )
    .unwrap()
});

/// AIMD adjustment events
pub static ADAPTIVE_AIMD_EVENTS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_adaptive_aimd_events_total",
        "AIMD limit adjustment events",
        &["direction"] // "increase", "decrease"
    )
    .unwrap()
});

/// AIMD limit adjustment magnitude
pub static ADAPTIVE_AIMD_DELTA: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_adaptive_aimd_delta",
        "Magnitude of AIMD limit adjustments",
        vec![-50.0, -20.0, -10.0, -5.0, -1.0, 0.0, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
    )
    .unwrap()
});

/// Minimum observed latency (baseline for gradient)
pub static ADAPTIVE_MIN_LATENCY_MS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_adaptive_min_latency_ms",
        "Minimum observed latency in ms (baseline when unloaded)"
    )
    .unwrap()
});

/// Query cost units distribution
pub static QUERY_COST_UNITS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_query_cost_units",
        "Cost units assigned to each query",
        vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0]
    )
    .unwrap()
});

/// Query success rate (calculated)
pub static QUERY_SUCCESS_RATE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_query_success_rate",
        "Rolling success rate (0.0 - 1.0)"
    )
    .unwrap()
});

/// Failure breakdown by type (for debugging)
pub static FAILURE_BREAKDOWN: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_failure_breakdown",
        "Recent failure counts by type",
        &["type"]
    )
    .unwrap()
});

/// Record query outcome (call for EVERY query)
pub fn record_query_outcome(outcome: &str) {
    QUERY_OUTCOME_TOTAL.with_label_values(&[outcome]).inc();
}

/// Update adaptive queue state
pub fn update_adaptive_queue_state(limit: f64, in_use: f64, min_latency_ms: Option<u64>) {
    ADAPTIVE_CONCURRENCY_LIMIT.set(limit);
    ADAPTIVE_COST_UNITS_IN_USE.set(in_use);
    if let Some(min_lat) = min_latency_ms {
        if min_lat < u64::MAX {
            ADAPTIVE_MIN_LATENCY_MS.set(min_lat as f64);
        }
    }
}

/// Record AIMD adjustment
pub fn record_aimd_adjustment(direction: &str, delta: f64) {
    ADAPTIVE_AIMD_EVENTS.with_label_values(&[direction]).inc();
    ADAPTIVE_AIMD_DELTA.observe(delta);
}

/// Record query cost units
pub fn record_query_cost(cost_units: f64) {
    QUERY_COST_UNITS.observe(cost_units);
}

/// Update success rate gauge
pub fn update_success_rate(rate: f64) {
    QUERY_SUCCESS_RATE.set(rate);
}

/// Update failure breakdown
pub fn update_failure_breakdown(
    timeout: u64,
    oom: u64,
    connection: u64,
    worker: u64,
    client: u64,
    other: u64,
) {
    FAILURE_BREAKDOWN
        .with_label_values(&["timeout"])
        .set(timeout as f64);
    FAILURE_BREAKDOWN
        .with_label_values(&["oom"])
        .set(oom as f64);
    FAILURE_BREAKDOWN
        .with_label_values(&["connection"])
        .set(connection as f64);
    FAILURE_BREAKDOWN
        .with_label_values(&["worker"])
        .set(worker as f64);
    FAILURE_BREAKDOWN
        .with_label_values(&["client"])
        .set(client as f64);
    FAILURE_BREAKDOWN
        .with_label_values(&["other"])
        .set(other as f64);
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW QUERY QUEUE METRICS (K8s Capacity-Aware)
// ═══════════════════════════════════════════════════════════════════════════

/// Cluster total memory capacity in MB (from K8s)
pub static CLUSTER_CAPACITY_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_cluster_capacity_mb",
        "Total memory capacity across all worker pods (MB)"
    )
    .unwrap()
});

/// Current memory usage in MB (sum of active query estimates)
pub static CURRENT_USAGE_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_current_usage_mb",
        "Current estimated memory usage by active queries (MB)"
    )
    .unwrap()
});

/// Available capacity in MB
pub static AVAILABLE_CAPACITY_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_available_capacity_mb",
        "Available memory capacity for new queries (MB)"
    )
    .unwrap()
});

/// Worker count from K8s
pub static K8S_WORKER_COUNT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_k8s_worker_count",
        "Number of running worker pods from K8s"
    )
    .unwrap()
});

/// HPA scale-up signal (true when queue is backing up)
pub static HPA_SCALE_UP_SIGNAL: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_hpa_scale_up_signal",
        "1 if HPA should scale up (queue backing up), 0 otherwise"
    )
    .unwrap()
});

/// Set queue depth
pub fn set_queue_depth(depth: usize) {
    QUERY_QUEUE_DEPTH.set(depth as f64);
}

/// Set cluster capacity
pub fn set_cluster_capacity_mb(capacity_mb: u64) {
    CLUSTER_CAPACITY_MB.set(capacity_mb as f64);
}

/// Set current usage
pub fn set_current_usage_mb(usage_mb: u64) {
    CURRENT_USAGE_MB.set(usage_mb as f64);
}

/// Set available capacity
pub fn set_available_capacity_mb(available_mb: u64) {
    AVAILABLE_CAPACITY_MB.set(available_mb as f64);
}

/// Set worker count
pub fn set_worker_count(count: u32) {
    K8S_WORKER_COUNT.set(count as f64);
}

/// Set HPA scale-up signal
pub fn set_hpa_scale_up_signal(signal: bool) {
    HPA_SCALE_UP_SIGNAL.set(if signal { 1.0 } else { 0.0 });
}

// ═══════════════════════════════════════════════════════════════════════════
// CEILING-AWARE QUEUE METRICS
// ═══════════════════════════════════════════════════════════════════════════

/// Resource ceiling (from K8s nodes)
pub static RESOURCE_CEILING_MB: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_resource_ceiling_mb",
        "Resource ceiling in MB (from K8s nodes)"
    )
    .unwrap()
});

/// Operation mode (0=scaling, 1=saturation)
pub static OPERATION_MODE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_operation_mode",
        "Operation mode: 0=scaling (can grow), 1=saturation (at ceiling)"
    )
    .unwrap()
});

/// Estimated wait time for new queries (ms)
pub static ESTIMATED_WAIT_MS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "tavana_estimated_wait_ms",
        "Estimated wait time for new queries in milliseconds"
    )
    .unwrap()
});

/// Set resource ceiling
pub fn set_resource_ceiling_mb(ceiling_mb: u64) {
    RESOURCE_CEILING_MB.set(ceiling_mb as f64);
}

/// Set operation mode
pub fn set_operation_mode(mode: &str) {
    let value = match mode {
        "scaling" => 0.0,
        "saturation" => 1.0,
        _ => 0.0,
    };
    OPERATION_MODE.set(value);
}

/// Set estimated wait time
pub fn set_estimated_wait_ms(wait_ms: u64) {
    ESTIMATED_WAIT_MS.set(wait_ms as f64);
}
