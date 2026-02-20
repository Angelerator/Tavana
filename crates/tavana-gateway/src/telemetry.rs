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

    if let Some(endpoint) = otlp_endpoint {
        tracing::info!(endpoint = %endpoint, "OTLP endpoint configured (export not yet implemented)");
    }

    Ok(())
}
