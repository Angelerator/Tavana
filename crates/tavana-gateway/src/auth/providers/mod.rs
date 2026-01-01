//! Authentication providers
//!
//! Each provider implements the `AuthProvider` trait and can be plugged into the gateway.

mod passthrough;
mod separ;
mod static_keys;

pub use passthrough::PassthroughProvider;
pub use separ::SeparProvider;
pub use static_keys::StaticKeysProvider;

use async_trait::async_trait;

use super::identity::{AuthResult, AuthenticatedPrincipal};

/// Credentials provided for authentication
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Username or principal identifier
    pub username: String,
    /// Password, token, or API key
    pub secret: String,
    /// Database/catalog being accessed (optional context)
    pub database: Option<String>,
    /// Additional context (e.g., client IP, application name)
    pub context: AuthContext,
}

impl Credentials {
    pub fn new(username: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            secret: secret.into(),
            database: None,
            context: AuthContext::default(),
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_context(mut self, context: AuthContext) -> Self {
        self.context = context;
        self
    }
}

/// Additional context for authentication
#[derive(Debug, Clone, Default)]
pub struct AuthContext {
    /// Client IP address
    pub client_ip: Option<String>,
    /// Application name (from connection params)
    pub application_name: Option<String>,
    /// Client identifier
    pub client_id: Option<String>,
}

/// Trait for authentication providers
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Provider name for logging/metrics
    fn name(&self) -> &str;

    /// Check if this provider can handle the given credentials
    /// Returns true if the provider should attempt authentication
    fn can_handle(&self, credentials: &Credentials) -> bool;

    /// Attempt to authenticate the given credentials
    async fn authenticate(&self, credentials: &Credentials) -> AuthResult;

    /// Check if a principal has a specific permission on a resource
    /// Returns None if this provider doesn't support authorization
    async fn check_permission(
        &self,
        _principal: &AuthenticatedPrincipal,
        _permission: &str,
        _resource: &str,
    ) -> Option<bool> {
        None // Default: authorization not supported
    }

    /// Refresh/extend authentication (for token refresh)
    async fn refresh(&self, _principal: &AuthenticatedPrincipal) -> AuthResult {
        AuthResult::InvalidCredentials("Token refresh not supported".to_string())
    }
}

