//! Identity and principal types for authentication
//!
//! Defines the common identity types that all auth providers produce.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of principal (who is authenticating)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalType {
    /// Human user (interactive)
    User,
    /// Application/service calling the API
    Application,
    /// Service account (non-interactive)
    ServiceAccount,
    /// API key (legacy or simple auth)
    ApiKey,
    /// Anonymous/passthrough (no auth required)
    Anonymous,
}

impl std::fmt::Display for PrincipalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrincipalType::User => write!(f, "user"),
            PrincipalType::Application => write!(f, "application"),
            PrincipalType::ServiceAccount => write!(f, "service_account"),
            PrincipalType::ApiKey => write!(f, "api_key"),
            PrincipalType::Anonymous => write!(f, "anonymous"),
        }
    }
}

/// An authenticated principal with identity information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedPrincipal {
    /// Unique identifier for this principal
    pub id: String,

    /// Type of principal
    pub principal_type: PrincipalType,

    /// Display name (for logging/UI)
    pub display_name: Option<String>,

    /// Email address (if applicable)
    pub email: Option<String>,

    /// Tenant/organization this principal belongs to
    pub tenant_id: Option<String>,

    /// Tenant name (for display)
    pub tenant_name: Option<String>,

    /// Groups/roles this principal is a member of
    pub groups: Vec<String>,

    /// Permissions/scopes granted
    pub scopes: Vec<String>,

    /// When this authentication was performed
    pub authenticated_at: DateTime<Utc>,

    /// When this authentication expires
    pub expires_at: Option<DateTime<Utc>>,

    /// The auth provider that validated this principal
    pub provider: String,

    /// Additional claims/attributes (provider-specific)
    pub attributes: HashMap<String, serde_json::Value>,
}

impl AuthenticatedPrincipal {
    /// Create an anonymous principal (passthrough mode)
    pub fn anonymous() -> Self {
        Self {
            id: "anonymous".to_string(),
            principal_type: PrincipalType::Anonymous,
            display_name: Some("Anonymous".to_string()),
            email: None,
            tenant_id: None,
            tenant_name: None,
            groups: vec![],
            scopes: vec!["*".to_string()], // Full access in passthrough mode
            authenticated_at: Utc::now(),
            expires_at: None,
            provider: "passthrough".to_string(),
            attributes: HashMap::new(),
        }
    }

    /// Create from a simple user ID (minimal identity)
    pub fn from_user_id(user_id: impl Into<String>, provider: impl Into<String>) -> Self {
        let user_id = user_id.into();
        Self {
            id: user_id.clone(),
            principal_type: PrincipalType::User,
            display_name: Some(user_id),
            email: None,
            tenant_id: None,
            tenant_name: None,
            groups: vec![],
            scopes: vec![],
            authenticated_at: Utc::now(),
            expires_at: None,
            provider: provider.into(),
            attributes: HashMap::new(),
        }
    }

    /// Check if this principal has a specific scope
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == "*" || s == scope)
    }

    /// Check if this principal is in a specific group
    pub fn in_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }

    /// Check if this principal is expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Get the SpiceDB subject string for authz checks
    pub fn to_spicedb_subject(&self) -> String {
        format!("{}:{}", self.principal_type, self.id)
    }
}

impl Default for AuthenticatedPrincipal {
    fn default() -> Self {
        Self::anonymous()
    }
}

/// Result of an authentication attempt
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Authentication successful
    Success(AuthenticatedPrincipal),
    /// Invalid credentials
    InvalidCredentials(String),
    /// Credentials expired
    Expired,
    /// Provider error (temporary failure)
    ProviderError(String),
    /// Not authenticated (no credentials provided)
    NotAuthenticated,
}

impl AuthResult {
    pub fn is_success(&self) -> bool {
        matches!(self, AuthResult::Success(_))
    }

    pub fn into_principal(self) -> Option<AuthenticatedPrincipal> {
        match self {
            AuthResult::Success(p) => Some(p),
            _ => None,
        }
    }
}

