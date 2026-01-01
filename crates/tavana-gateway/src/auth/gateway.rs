//! Authentication Gateway
//!
//! Main entry point for authentication in Tavana.
//! Coordinates multiple auth providers and handles caching.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use super::config::{AuthConfig, AuthMode};
use super::identity::{AuthResult, AuthenticatedPrincipal};
use super::providers::{
    AuthContext, AuthProvider, Credentials, PassthroughProvider, SeparProvider,
    StaticKeysProvider,
};
use super::tokens::{ApiKeyValidator, JwtValidator};

/// The main authentication gateway
pub struct AuthGateway {
    /// Configuration
    config: AuthConfig,
    /// Ordered list of providers to try
    providers: Vec<Arc<dyn AuthProvider>>,
    /// Fallback provider (passthrough)
    passthrough: Arc<PassthroughProvider>,
    /// Local API key validator
    api_key_validator: Arc<ApiKeyValidator>,
    /// Local JWT validator (optional)
    jwt_validator: Option<Arc<JwtValidator>>,
    /// Cache for successful authentications
    cache: Option<AuthCache>,
    /// Rate limiter for failed attempts
    rate_limiter: Option<RateLimiter>,
}

impl AuthGateway {
    /// Create a new auth gateway from configuration
    pub async fn new(config: AuthConfig) -> anyhow::Result<Self> {
        let mut providers: Vec<Arc<dyn AuthProvider>> = Vec::new();

        // Add Separ provider if configured
        if let Some(ref separ_config) = config.separ {
            info!(endpoint = %separ_config.endpoint, "Initializing Separ provider");
            let separ = SeparProvider::new(separ_config.clone())?;
            providers.push(Arc::new(separ));
        }

        // Add static keys provider if configured
        let api_key_validator = if let Some(ref keys_config) = config.static_keys {
            info!(key_count = keys_config.keys.len(), "Initializing static keys provider");
            let static_provider = StaticKeysProvider::from_config(keys_config);
            providers.push(Arc::new(static_provider));
            Arc::new(ApiKeyValidator::new())
        } else {
            Arc::new(ApiKeyValidator::new())
        };

        // Add JWT validator if configured
        let jwt_validator = if let Some(ref jwt_config) = config.jwt {
            info!("Initializing local JWT validator");
            Some(Arc::new(JwtValidator::new(jwt_config)?))
        } else {
            None
        };

        // Initialize cache if enabled
        let cache = if config.cache.enabled {
            Some(AuthCache::new(
                config.cache.max_size,
                Duration::from_secs(config.cache.ttl_secs),
            ))
        } else {
            None
        };

        // Initialize rate limiter if enabled
        let rate_limiter = if config.rate_limit.enabled {
            Some(RateLimiter::new(
                config.rate_limit.max_failed_attempts,
                Duration::from_secs(config.rate_limit.ban_duration_secs),
            ))
        } else {
            None
        };

        let passthrough = Arc::new(PassthroughProvider::new());

        info!(
            mode = ?config.mode,
            provider_count = providers.len(),
            cache_enabled = config.cache.enabled,
            "Auth gateway initialized"
        );

        Ok(Self {
            config,
            providers,
            passthrough,
            api_key_validator,
            jwt_validator,
            cache,
            rate_limiter,
        })
    }

    /// Create a passthrough-only gateway (no auth required)
    pub fn passthrough() -> Self {
        Self {
            config: AuthConfig {
                mode: AuthMode::Passthrough,
                ..Default::default()
            },
            providers: Vec::new(),
            passthrough: Arc::new(PassthroughProvider::new()),
            api_key_validator: Arc::new(ApiKeyValidator::new()),
            jwt_validator: None,
            cache: None,
            rate_limiter: None,
        }
    }

    /// Authenticate a connection
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
        context: AuthContext,
    ) -> AuthResult {
        // Check rate limiting
        if let Some(ref limiter) = self.rate_limiter {
            if let Some(ref ip) = context.client_ip {
                if limiter.is_blocked(ip) {
                    warn!(client_ip = %ip, "Rate limited - too many failed attempts");
                    return AuthResult::InvalidCredentials(
                        "Too many failed attempts. Please try again later.".to_string(),
                    );
                }
            }
        }

        // Passthrough mode - accept all
        if self.config.mode == AuthMode::Passthrough {
            debug!(username = %username, "Passthrough mode - accepting");
            return self.passthrough.authenticate(&Credentials::new(username, password)).await;
        }

        // Build credentials
        let credentials = Credentials::new(username, password).with_context(context.clone());

        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(&self.cache_key(&credentials)) {
                if !cached.is_expired() {
                    debug!(user_id = %cached.id, "Using cached authentication");
                    return AuthResult::Success(cached);
                }
            }
        }

        // Try local API key validation first (fast path)
        if ApiKeyValidator::is_api_key(password) {
            match self.api_key_validator.validate(password) {
                super::tokens::api_key::ApiKeyResult::Valid(principal) => {
                    self.cache_result(&credentials, &principal);
                    return AuthResult::Success(principal);
                }
                super::tokens::api_key::ApiKeyResult::Invalid(reason) => {
                    self.record_failure(&context);
                    return AuthResult::InvalidCredentials(reason);
                }
                super::tokens::api_key::ApiKeyResult::Unknown => {
                    // Continue to other providers
                }
            }
        }

        // Try local JWT validation if configured (fast path)
        if let Some(ref jwt_validator) = self.jwt_validator {
            if password.starts_with("eyJ") && password.contains('.') {
                match jwt_validator.validate(password) {
                    Ok(principal) => {
                        self.cache_result(&credentials, &principal);
                        return AuthResult::Success(principal);
                    }
                    Err(e) => {
                        debug!(error = %e, "Local JWT validation failed, trying providers");
                        // Continue to other providers - might be a token from Separ
                    }
                }
            }
        }

        // Try each configured provider
        for provider in &self.providers {
            if !provider.can_handle(&credentials) {
                continue;
            }

            debug!(provider = %provider.name(), "Trying provider");

            match provider.authenticate(&credentials).await {
                AuthResult::Success(principal) => {
                    info!(
                        provider = %provider.name(),
                        user_id = %principal.id,
                        principal_type = %principal.principal_type,
                        "Authentication successful"
                    );
                    self.cache_result(&credentials, &principal);
                    self.clear_failures(&context);
                    return AuthResult::Success(principal);
                }
                AuthResult::InvalidCredentials(reason) => {
                    debug!(provider = %provider.name(), reason = %reason, "Invalid credentials");
                    // Continue to next provider
                }
                AuthResult::ProviderError(error) => {
                    warn!(provider = %provider.name(), error = %error, "Provider error");
                    // Continue to next provider
                }
                AuthResult::Expired => {
                    debug!(provider = %provider.name(), "Credentials expired");
                    return AuthResult::Expired;
                }
                AuthResult::NotAuthenticated => {
                    // Continue to next provider
                }
            }
        }

        // All providers failed
        self.record_failure(&context);

        // Optional mode - fall back to passthrough
        if self.config.mode == AuthMode::Optional {
            debug!("Optional mode - falling back to passthrough");
            return self.passthrough.authenticate(&credentials).await;
        }

        AuthResult::InvalidCredentials("Authentication failed".to_string())
    }

    /// Check if a principal has permission on a resource
    pub async fn check_permission(
        &self,
        principal: &AuthenticatedPrincipal,
        permission: &str,
        resource: &str,
    ) -> bool {
        // Passthrough mode - always allow
        if self.config.mode == AuthMode::Passthrough {
            return true;
        }

        // Anonymous principals in optional mode - allow
        if self.config.mode == AuthMode::Optional
            && principal.principal_type == super::identity::PrincipalType::Anonymous
        {
            return true;
        }

        // Check with the provider that authenticated this principal
        for provider in &self.providers {
            if provider.name() == principal.provider {
                if let Some(allowed) = provider
                    .check_permission(principal, permission, resource)
                    .await
                {
                    return allowed;
                }
            }
        }

        // Fall back to scope check
        principal.has_scope(permission) || principal.has_scope(&format!("{}:{}", permission, resource))
    }

    /// Register a local API key
    pub fn register_api_key(
        &self,
        key: &str,
        principal: AuthenticatedPrincipal,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.api_key_validator.register(key, principal, expires_at);
    }

    /// Revoke an API key
    pub fn revoke_api_key(&self, key: &str) {
        self.api_key_validator.revoke(key);
    }

    /// Get the auth mode
    pub fn mode(&self) -> AuthMode {
        self.config.mode
    }

    /// Check if auth is required
    pub fn is_required(&self) -> bool {
        self.config.mode == AuthMode::Required
    }

    /// Check if passthrough mode
    pub fn is_passthrough(&self) -> bool {
        self.config.mode == AuthMode::Passthrough
    }

    // Private helpers

    fn cache_key(&self, credentials: &Credentials) -> String {
        format!("{}:{}", credentials.username, blake3::hash(credentials.secret.as_bytes()).to_hex())
    }

    fn cache_result(&self, credentials: &Credentials, principal: &AuthenticatedPrincipal) {
        if let Some(ref cache) = self.cache {
            cache.put(self.cache_key(credentials), principal.clone());
        }
    }

    fn record_failure(&self, context: &AuthContext) {
        if let Some(ref limiter) = self.rate_limiter {
            if let Some(ref ip) = context.client_ip {
                limiter.record_failure(ip);
            }
        }
    }

    fn clear_failures(&self, context: &AuthContext) {
        if let Some(ref limiter) = self.rate_limiter {
            if let Some(ref ip) = context.client_ip {
                limiter.clear(ip);
            }
        }
    }
}

// ============================================================================
// Auth Cache
// ============================================================================

struct AuthCache {
    entries: RwLock<HashMap<String, CacheEntry>>,
    max_size: usize,
    ttl: Duration,
}

struct CacheEntry {
    principal: AuthenticatedPrincipal,
    expires_at: Instant,
}

impl AuthCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_size,
            ttl,
        }
    }

    fn get(&self, key: &str) -> Option<AuthenticatedPrincipal> {
        let entries = self.entries.read();
        entries.get(key).and_then(|entry| {
            if Instant::now() < entry.expires_at {
                Some(entry.principal.clone())
            } else {
                None
            }
        })
    }

    fn put(&self, key: String, principal: AuthenticatedPrincipal) {
        let mut entries = self.entries.write();

        // Evict expired entries if at capacity
        if entries.len() >= self.max_size {
            let now = Instant::now();
            entries.retain(|_, entry| entry.expires_at > now);
        }

        // Still at capacity? Remove oldest
        if entries.len() >= self.max_size {
            if let Some(oldest_key) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest_key);
            }
        }

        entries.insert(
            key,
            CacheEntry {
                principal,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }
}

// ============================================================================
// Rate Limiter
// ============================================================================

struct RateLimiter {
    attempts: RwLock<HashMap<String, FailedAttempts>>,
    max_attempts: u32,
    ban_duration: Duration,
}

struct FailedAttempts {
    count: u32,
    first_attempt: Instant,
    blocked_until: Option<Instant>,
}

impl RateLimiter {
    fn new(max_attempts: u32, ban_duration: Duration) -> Self {
        Self {
            attempts: RwLock::new(HashMap::new()),
            max_attempts,
            ban_duration,
        }
    }

    fn is_blocked(&self, key: &str) -> bool {
        let attempts = self.attempts.read();
        if let Some(entry) = attempts.get(key) {
            if let Some(blocked_until) = entry.blocked_until {
                return Instant::now() < blocked_until;
            }
        }
        false
    }

    fn record_failure(&self, key: &str) {
        let mut attempts = self.attempts.write();
        let entry = attempts.entry(key.to_string()).or_insert(FailedAttempts {
            count: 0,
            first_attempt: Instant::now(),
            blocked_until: None,
        });

        entry.count += 1;

        if entry.count >= self.max_attempts {
            entry.blocked_until = Some(Instant::now() + self.ban_duration);
        }
    }

    fn clear(&self, key: &str) {
        self.attempts.write().remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_passthrough_gateway() {
        let gateway = AuthGateway::passthrough();
        
        let result = gateway
            .authenticate("test_user", "any_password", AuthContext::default())
            .await;

        assert!(result.is_success());
        let principal = result.into_principal().unwrap();
        assert_eq!(principal.id, "test_user");
    }

    #[tokio::test]
    async fn test_api_key_registration() {
        let gateway = AuthGateway::passthrough();
        
        let principal = AuthenticatedPrincipal::from_user_id("test_user", "test");
        gateway.register_api_key("tvn_test_key", principal.clone(), None);

        // Gateway is passthrough, but API key should still work
        let result = gateway
            .authenticate("test_user", "tvn_test_key", AuthContext::default())
            .await;

        assert!(result.is_success());
    }
}

