//! Configuration utilities for Tavana services

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Common service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// Service name
    pub name: String,
    /// gRPC server address
    pub grpc_addr: SocketAddr,
    /// HTTP/REST server address (optional)
    pub http_addr: Option<SocketAddr>,
    /// TLS configuration
    pub tls: TlsSettings,
    /// Telemetry configuration
    pub telemetry: TelemetrySettings,
}

/// TLS configuration settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Enable TLS
    pub enabled: bool,
    /// Path to certificate file
    pub cert_path: Option<String>,
    /// Path to private key file
    pub key_path: Option<String>,
    /// Path to CA certificate file
    pub ca_path: Option<String>,
    /// Generate self-signed certificate if not provided
    pub auto_generate: bool,
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            cert_path: None,
            key_path: None,
            ca_path: None,
            auto_generate: true,
        }
    }
}

/// Telemetry/observability settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySettings {
    /// Enable OpenTelemetry
    pub enabled: bool,
    /// OTLP endpoint for traces/metrics
    pub otlp_endpoint: Option<String>,
    /// Service name for tracing
    pub service_name: String,
    /// Log level (trace, debug, info, warn, error)
    pub log_level: String,
    /// Enable JSON log format
    pub json_logs: bool,
}

impl Default for TelemetrySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            otlp_endpoint: None,
            service_name: "tavana".into(),
            log_level: "info".into(),
            json_logs: false,
        }
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    pub url: String,
    /// Maximum connections in pool
    pub max_connections: u32,
    /// Minimum connections in pool
    pub min_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Enable SSL
    pub ssl_mode: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://tavana:tavana@localhost:5432/tavana".into(),
            max_connections: 10,
            min_connections: 1,
            connect_timeout_secs: 30,
            ssl_mode: "prefer".into(),
        }
    }
}

/// Object storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Default storage type (s3, azure, gcs)
    pub storage_type: String,
    /// S3/MinIO configuration
    pub s3: Option<S3Config>,
    /// Azure ADLS configuration
    pub azure: Option<AzureConfig>,
    /// GCS configuration
    pub gcs: Option<GcsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// AWS region
    pub region: String,
    /// Custom endpoint (for MinIO, LocalStack)
    pub endpoint: Option<String>,
    /// Use path-style URLs (for MinIO)
    pub force_path_style: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Storage account name
    pub account_name: String,
    /// Use managed identity
    pub use_managed_identity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsConfig {
    /// GCP project ID
    pub project_id: String,
    /// Use workload identity
    pub use_workload_identity: bool,
}

/// Load configuration from environment variables
pub fn load_from_env<T: for<'de> Deserialize<'de>>(prefix: &str) -> Result<T, config::ConfigError> {
    config::Config::builder()
        .add_source(config::Environment::with_prefix(prefix).separator("__"))
        .build()?
        .try_deserialize()
}

/// Get environment variable with default
pub fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Get environment variable as parsed type with default
pub fn env_parse_or_default<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

