//! Authentication Gateway for Tavana
//!
//! This module provides a pluggable authentication system that can integrate with:
//! - Separ (centralized authorization platform)
//! - Other OAuth/OIDC providers
//! - Static API keys
//! - Passthrough (no auth) mode
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    AuthGateway                               │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
//! │  │ TokenParser │  │ Providers   │  │ PermissionChecker   │  │
//! │  │ - JWT       │  │ - Separ     │  │ (optional authz)    │  │
//! │  │ - API Key   │  │ - Passthru  │  │                     │  │
//! │  │ - SvcAcct   │  │ - Static    │  │                     │  │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! let config = AuthConfig::from_env();
//! let gateway = AuthGateway::new(config).await?;
//!
//! // Authenticate a connection
//! let identity = gateway.authenticate(username, password).await?;
//!
//! // Check permission (optional)
//! let allowed = gateway.check_permission(&identity, "query", "application:tavana").await?;
//! ```

pub mod config;
pub mod gateway;
pub mod identity;
pub mod providers;
pub mod tokens;

// Re-exports for convenient access
pub use config::{AuthConfig, AuthMode, ProviderConfig};
pub use gateway::AuthGateway;
pub use identity::{AuthenticatedPrincipal, PrincipalType};
pub use providers::{AuthContext, Credentials};

use parking_lot::RwLock;
use std::sync::Arc;
use tavana_common::{ApiKeyValidator, Result, TavanaError, UserIdentity};

/// Legacy authentication service for backwards compatibility
/// 
/// This wraps the new AuthGateway for existing code that expects the old interface.
/// New code should use AuthGateway directly.
pub struct AuthService {
    api_key_validator: Arc<RwLock<ApiKeyValidator>>,
    gateway: Option<Arc<AuthGateway>>,
}

impl AuthService {
    /// Create a new authentication service (passthrough mode)
    pub fn new() -> Self {
        Self {
            api_key_validator: Arc::new(RwLock::new(ApiKeyValidator::new())),
            gateway: None,
        }
    }

    /// Create with an auth gateway
    pub fn with_gateway(gateway: Arc<AuthGateway>) -> Self {
        Self {
            api_key_validator: Arc::new(RwLock::new(ApiKeyValidator::new())),
            gateway: Some(gateway),
        }
    }

    /// Get the underlying gateway if available
    pub fn gateway(&self) -> Option<&Arc<AuthGateway>> {
        self.gateway.as_ref()
    }

    /// Authenticate a request using the provided token
    pub async fn authenticate(&self, token: &str) -> Result<UserIdentity> {
        // Try API key first (starts with tvn_)
        if token.starts_with("tvn_") {
            return self.api_key_validator.read().validate(token);
        }

        // If we have a gateway, use it
        if let Some(ref gateway) = self.gateway {
            match gateway.authenticate("token", token, AuthContext::default()).await {
                identity::AuthResult::Success(principal) => {
                    return Ok(UserIdentity {
                        user_id: principal.id,
                        tenant_id: principal.tenant_id.unwrap_or_else(|| "default".to_string()),
                        scopes: principal.scopes,
                        claims: std::collections::HashMap::new(),
                    });
                }
                identity::AuthResult::InvalidCredentials(msg) => {
                    return Err(TavanaError::InvalidToken(msg));
                }
                identity::AuthResult::Expired => {
                    return Err(TavanaError::InvalidToken("Token expired".into()));
                }
                identity::AuthResult::ProviderError(msg) => {
                    return Err(TavanaError::Internal(msg));
                }
                identity::AuthResult::NotAuthenticated => {
                    return Err(TavanaError::InvalidToken("Not authenticated".into()));
                }
            }
        }

        // Fallback: not supported
        Err(TavanaError::InvalidToken("JWT authentication not yet supported - use API keys (tvn_*)".into()))
    }

    /// Register an API key
    pub fn register_api_key(&self, api_key: &str, identity: UserIdentity) {
        self.api_key_validator
            .write()
            .register_key(api_key, identity);
    }
}

impl Default for AuthService {
    fn default() -> Self {
        Self::new()
    }
}
