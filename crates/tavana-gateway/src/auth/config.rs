//! Authentication configuration
//!
//! Supports loading from environment variables, config files, or programmatically.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Authentication mode - determines how auth is handled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// No authentication required (development/internal use)
    Passthrough,
    /// Require authentication via configured providers
    #[default]
    Required,
    /// Authentication optional - allow anonymous if no credentials
    Optional,
}

/// Main authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication mode
    #[serde(default)]
    pub mode: AuthMode,

    /// Enable Separ integration
    #[serde(default)]
    pub separ: Option<SeparConfig>,

    /// Enable static API keys (for backwards compatibility)
    #[serde(default)]
    pub static_keys: Option<StaticKeysConfig>,

    /// JWT validation settings (for direct JWT without Separ)
    #[serde(default)]
    pub jwt: Option<JwtConfig>,

    /// Cache settings
    #[serde(default)]
    pub cache: CacheConfig,

    /// Rate limiting
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Passthrough,
            separ: None,
            static_keys: None,
            jwt: None,
            cache: CacheConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

impl AuthConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mode = match std::env::var("TAVANA_AUTH_MODE").as_deref() {
            Ok("passthrough") | Ok("none") => AuthMode::Passthrough,
            Ok("required") => AuthMode::Required,
            Ok("optional") => AuthMode::Optional,
            _ => AuthMode::Passthrough,
        };

        let separ = if std::env::var("SEPAR_ENDPOINT").is_ok() {
            Some(SeparConfig::from_env())
        } else {
            None
        };

        Self {
            mode,
            separ,
            static_keys: None,
            jwt: None,
            cache: CacheConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }

    /// Check if any authentication provider is configured
    pub fn has_provider(&self) -> bool {
        self.separ.is_some() || self.static_keys.is_some() || self.jwt.is_some()
    }

    /// Check if authentication is required
    pub fn is_required(&self) -> bool {
        self.mode == AuthMode::Required
    }

    /// Check if passthrough mode is enabled
    pub fn is_passthrough(&self) -> bool {
        self.mode == AuthMode::Passthrough
    }
}

/// Separ integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeparConfig {
    /// Separ API endpoint (e.g., "http://separ:8080")
    pub endpoint: String,

    /// API key for Tavana to authenticate with Separ
    pub api_key: String,

    /// Tenant hint extraction strategy
    #[serde(default)]
    pub tenant_extraction: TenantExtractionStrategy,

    /// Enable authorization checks via Separ
    #[serde(default = "default_true")]
    pub enable_authz: bool,

    /// Connection timeout
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Request timeout
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,

    /// Pool idle timeout (how long to keep idle connections)
    #[serde(default = "default_pool_idle_timeout")]
    pub pool_idle_timeout_secs: u64,

    /// Max idle connections per host
    #[serde(default = "default_pool_max_idle")]
    pub pool_max_idle_per_host: usize,
}

impl SeparConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("SEPAR_ENDPOINT")
                .unwrap_or_else(|_| "http://separ:8080".to_string()),
            api_key: std::env::var("SEPAR_API_KEY")
                .unwrap_or_else(|_| "".to_string()),
            tenant_extraction: TenantExtractionStrategy::default(),
            enable_authz: std::env::var("SEPAR_ENABLE_AUTHZ")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(true),
            connect_timeout_secs: std::env::var("SEPAR_CONNECT_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(|ms: u64| ms / 1000)
                .unwrap_or(5),
            request_timeout_secs: std::env::var("SEPAR_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(|ms: u64| ms / 1000)
                .unwrap_or(10),
            pool_idle_timeout_secs: std::env::var("SEPAR_POOL_IDLE_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(90),
            pool_max_idle_per_host: std::env::var("SEPAR_POOL_MAX_IDLE_PER_HOST")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    pub fn pool_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.pool_idle_timeout_secs)
    }
}

/// How to extract tenant from username
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TenantExtractionStrategy {
    /// Extract from domain part of email (user@tenant.com -> tenant)
    #[default]
    EmailDomain,
    /// Extract from prefix (tenant\user -> tenant)
    BackslashPrefix,
    /// No tenant extraction (use default tenant)
    None,
}

/// Static API keys configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticKeysConfig {
    /// Map of API key prefix to allowed keys
    /// Keys should start with a prefix like "tvn_" or "sk_"
    #[serde(default)]
    pub keys: Vec<StaticApiKey>,
}

/// A static API key entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticApiKey {
    /// The API key (hashed or plaintext depending on config)
    pub key: String,
    /// Principal ID to assign
    pub principal_id: String,
    /// Principal type
    #[serde(default = "default_api_key_principal_type")]
    pub principal_type: String,
    /// Tenant ID (optional)
    pub tenant_id: Option<String>,
    /// Scopes/permissions
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// JWT validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// JWT secret (for HS256) or public key path (for RS256)
    pub secret_or_key: String,
    /// Expected issuer
    pub issuer: Option<String>,
    /// Expected audience
    pub audience: Option<String>,
    /// Algorithm (HS256, RS256, etc.)
    #[serde(default = "default_jwt_algorithm")]
    pub algorithm: String,
    /// Clock skew tolerance in seconds
    #[serde(default = "default_clock_skew")]
    pub clock_skew_secs: u64,
}

/// Cache configuration for auth results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable caching of successful auth results
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// TTL for cached entries in seconds
    #[serde(default = "default_cache_ttl")]
    pub ttl_secs: u64,
    /// Maximum cache size
    #[serde(default = "default_cache_size")]
    pub max_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_secs: 300, // 5 minutes
            max_size: 10000,
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable rate limiting
    #[serde(default)]
    pub enabled: bool,
    /// Max failed attempts before temporary ban
    #[serde(default = "default_max_attempts")]
    pub max_failed_attempts: u32,
    /// Ban duration in seconds
    #[serde(default = "default_ban_duration")]
    pub ban_duration_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_failed_attempts: 5,
            ban_duration_secs: 300,
        }
    }
}

/// Provider configuration enum for dynamic provider loading
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderConfig {
    Separ(SeparConfig),
    StaticKeys(StaticKeysConfig),
    Jwt(JwtConfig),
    Passthrough,
}

// Default value helpers
fn default_true() -> bool { true }
fn default_connect_timeout() -> u64 { 5 }
fn default_request_timeout() -> u64 { 10 }
fn default_pool_idle_timeout() -> u64 { 90 }
fn default_pool_max_idle() -> usize { 10 }
fn default_cache_ttl() -> u64 { 300 }
fn default_cache_size() -> usize { 10000 }
fn default_max_attempts() -> u32 { 5 }
fn default_ban_duration() -> u64 { 300 }
fn default_jwt_algorithm() -> String { "HS256".to_string() }
fn default_clock_skew() -> u64 { 60 }
fn default_api_key_principal_type() -> String { "api_key".to_string() }

