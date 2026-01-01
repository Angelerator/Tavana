//! Passthrough (no-auth) provider
//!
//! Always allows authentication - for development or trusted environments.

use async_trait::async_trait;

use super::{AuthContext, AuthProvider, Credentials};
use crate::auth::identity::{AuthResult, AuthenticatedPrincipal, PrincipalType};

/// Passthrough provider - always allows access
pub struct PassthroughProvider {
    /// Default tenant to assign
    default_tenant: Option<String>,
    /// Default scopes to grant
    default_scopes: Vec<String>,
}

impl PassthroughProvider {
    pub fn new() -> Self {
        Self {
            default_tenant: None,
            default_scopes: vec!["*".to_string()],
        }
    }

    pub fn with_default_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.default_tenant = Some(tenant.into());
        self
    }

    pub fn with_default_scopes(mut self, scopes: Vec<String>) -> Self {
        self.default_scopes = scopes;
        self
    }
}

impl Default for PassthroughProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AuthProvider for PassthroughProvider {
    fn name(&self) -> &str {
        "passthrough"
    }

    fn can_handle(&self, _credentials: &Credentials) -> bool {
        // Passthrough handles everything as last resort
        true
    }

    async fn authenticate(&self, credentials: &Credentials) -> AuthResult {
        tracing::debug!(
            username = %credentials.username,
            "Passthrough auth: accepting without validation"
        );

        // Create principal from provided username
        let principal = if credentials.username.is_empty() || credentials.username == "anonymous" {
            AuthenticatedPrincipal::anonymous()
        } else {
            AuthenticatedPrincipal {
                id: credentials.username.clone(),
                principal_type: PrincipalType::User,
                display_name: Some(credentials.username.clone()),
                email: if credentials.username.contains('@') {
                    Some(credentials.username.clone())
                } else {
                    None
                },
                tenant_id: self.default_tenant.clone(),
                tenant_name: self.default_tenant.clone(),
                groups: vec![],
                scopes: self.default_scopes.clone(),
                authenticated_at: chrono::Utc::now(),
                expires_at: None,
                provider: "passthrough".to_string(),
                attributes: std::collections::HashMap::new(),
            }
        };

        AuthResult::Success(principal)
    }

    async fn check_permission(
        &self,
        _principal: &AuthenticatedPrincipal,
        _permission: &str,
        _resource: &str,
    ) -> Option<bool> {
        // Passthrough mode: always allow
        Some(true)
    }
}

