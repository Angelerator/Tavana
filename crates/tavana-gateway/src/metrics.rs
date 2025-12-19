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
    register_counter_vec!(
        "tavana_cache_total",
        "Cache hits and misses",
        &["result"]
    )
    .unwrap()
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
        vec![1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0, 1000000000.0]
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
        &["result"]  // "success", "failed", "skipped"
    )
    .unwrap()
});

/// Memory requested during pre-sizing (MB)
pub static WORKER_PRESIZE_MEMORY_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_worker_presize_memory_mb",
        "Memory requested during worker pre-sizing in MB",
        vec![256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0, 262144.0, 409600.0]
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
        &["state"]  // "total", "busy", "idle", "resizing"
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
        &["direction"]  // "scale_up", "scale_down", "initial"
    )
    .unwrap()
});

/// Memory before resize (for tracking deltas)
pub static RESIZE_MEMORY_DELTA_MB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "tavana_resize_memory_delta_mb",
        "Memory change during resize (MB, can be negative for scale-down)",
        vec![-4096.0, -2048.0, -1024.0, -512.0, 0.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0]
    )
    .unwrap()
});

/// Elastic scale-up events during query execution
pub static ELASTIC_SCALEUP_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tavana_elastic_scaleup_total",
        "Elastic scale-up events during query execution",
        &["trigger"]  // "memory_pressure", "oom_risk", "cpu_throttle"
    )
    .unwrap()
});

/// VPA recommendations vs actual allocation
pub static VPA_RECOMMENDATION_MB: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "tavana_vpa_recommendation_mb",
        "VPA memory recommendations in MB",
        &["type"]  // "target", "lower_bound", "upper_bound", "uncapped_target"
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

    // Set initial values
    ADAPTIVE_THRESHOLD_MB.set(2048.0); // 2GB default
    ADAPTIVE_HPA_MIN.set(2.0);
    ADAPTIVE_HPA_MAX.set(100.0);
    
    // Initialize worker pool status
    WORKER_POOL_STATUS.with_label_values(&["total"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["busy"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["idle"]).set(0.0);
    WORKER_POOL_STATUS.with_label_values(&["resizing"]).set(0.0);
}

/// Encode all metrics as Prometheus text format
pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
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
    WORKER_REPLICAS.with_label_values(&["desired"]).set(desired as f64);
    WORKER_REPLICAS.with_label_values(&["ready"]).set(ready as f64);
    WORKER_REPLICAS.with_label_values(&["available"]).set(available as f64);
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
    WORKER_POOL_STATUS.with_label_values(&["total"]).set(total as f64);
    WORKER_POOL_STATUS.with_label_values(&["busy"]).set(busy as f64);
    WORKER_POOL_STATUS.with_label_values(&["idle"]).set(idle as f64);
    WORKER_POOL_STATUS.with_label_values(&["resizing"]).set(resizing as f64);
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
pub fn update_vpa_recommendations(target_mb: f64, lower_bound_mb: f64, upper_bound_mb: f64, uncapped_mb: f64) {
    VPA_RECOMMENDATION_MB.with_label_values(&["target"]).set(target_mb);
    VPA_RECOMMENDATION_MB.with_label_values(&["lower_bound"]).set(lower_bound_mb);
    VPA_RECOMMENDATION_MB.with_label_values(&["upper_bound"]).set(upper_bound_mb);
    VPA_RECOMMENDATION_MB.with_label_values(&["uncapped_target"]).set(uncapped_mb);
}

