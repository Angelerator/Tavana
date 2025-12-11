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

    // Set initial values
    ADAPTIVE_THRESHOLD_MB.set(6144.0); // 6GB default
    ADAPTIVE_HPA_MIN.set(2.0);
    ADAPTIVE_HPA_MAX.set(10.0);
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

