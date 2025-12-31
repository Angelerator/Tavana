//! OpenTelemetry instrumentation
//!
//! Provides comprehensive observability for the Tavana platform:
//! - Distributed tracing with OTLP export
//! - Prometheus metrics
//! - Structured JSON logging

use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize telemetry (tracing, metrics, logs)
pub fn init(log_level: &str) -> Result<()> {
    // Set up tracing subscriber with env filter
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    // Check if OTLP endpoint is configured
    let otlp_endpoint = std::env::var("OTLP_ENDPOINT").ok();
    let json_logs = std::env::var("JSON_LOGS")
        .map(|v| v == "true")
        .unwrap_or(false);

    let subscriber = tracing_subscriber::registry().with(filter);

    if json_logs {
        // JSON formatted logs for production
        subscriber
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        // Pretty formatted logs for development
        subscriber.with(tracing_subscriber::fmt::layer()).init();
    }

    // NOTE: OpenTelemetry OTLP integration is planned for v1.1
    // Requires: opentelemetry, opentelemetry-otlp, and tracing-opentelemetry crates
    if let Some(endpoint) = otlp_endpoint {
        tracing::info!(endpoint = %endpoint, "OpenTelemetry OTLP endpoint configured - full integration in v1.1");
    }

    Ok(())
}

/// Prometheus metrics registry
pub mod metrics {
    use once_cell::sync::Lazy;
    use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};

    /// Query execution counter
    pub static QUERY_COUNTER: Lazy<CounterVec> = Lazy::new(|| {
        register_counter_vec!(
            "tavana_queries_total",
            "Total number of queries executed",
            &["status", "query_type"]
        )
        .unwrap()
    });

    /// Query execution duration histogram
    pub static QUERY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
        register_histogram_vec!(
            "tavana_query_duration_seconds",
            "Query execution duration in seconds",
            &["query_type"],
            vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
        )
        .unwrap()
    });

    /// Active connections gauge
    pub static ACTIVE_CONNECTIONS: Lazy<prometheus::IntGaugeVec> = Lazy::new(|| {
        prometheus::register_int_gauge_vec!(
            "tavana_active_connections",
            "Number of active client connections",
            &["protocol"]
        )
        .unwrap()
    });

    /// Data scanned counter
    pub static DATA_SCANNED_BYTES: Lazy<CounterVec> = Lazy::new(|| {
        register_counter_vec!(
            "tavana_data_scanned_bytes_total",
            "Total bytes of data scanned",
            &["table", "format"]
        )
        .unwrap()
    });
}
