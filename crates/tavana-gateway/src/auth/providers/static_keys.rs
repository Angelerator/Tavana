//! Static API keys provider
//!
//! Validates against a pre-configured list of API keys.
//! Useful for backwards compatibility or simple deployments.

use async_trait::async_trait;
use std::collections::HashMap;
use parking_lot::RwLock;

use super::{AuthProvider, Credentials};
use crate::auth::config::StaticKeysConfig;
use crate::auth::identity::{AuthResult, AuthenticatedPrincipal, PrincipalType};

/// Static keys provider
pub struct StaticKeysProvider {
    /// Registered API keys (key -> principal info)
    keys: RwLock<HashMap<String, KeyEntry>>,
    /// Key prefix for detection
    prefix: String,
}

#[derive(Debug, Clone)]
struct KeyEntry {
    principal_id: String,
    principal_type: PrincipalType,
    tenant_id: Option<String>,
    scopes: Vec<String>,
}

impl StaticKeysProvider {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            prefix: prefix.into(),
        }
    }

    /// Create from configuration
    pub fn from_config(config: &StaticKeysConfig) -> Self {
        let provider = Self::new("tvn_");
        
        for key_config in &config.keys {
            let entry = KeyEntry {
                principal_id: key_config.principal_id.clone(),
                principal_type: match key_config.principal_type.as_str() {
                    "user" => PrincipalType::User,
                    "service_account" => PrincipalType::ServiceAccount,
                    "application" => PrincipalType::Application,
                    _ => PrincipalType::ApiKey,
                },
                tenant_id: key_config.tenant_id.clone(),
                scopes: key_config.scopes.clone(),
            };
            provider.register_key(&key_config.key, entry);
        }
        
        provider
    }

    /// Register an API key
    pub fn register_key(&self, key: &str, entry: KeyEntry) {
        self.keys.write().insert(key.to_string(), entry);
    }

    /// Register a simple key for a user
    pub fn register_user_key(
        &self,
        key: &str,
        user_id: &str,
        tenant_id: Option<String>,
        scopes: Vec<String>,
    ) {
        let entry = KeyEntry {
            principal_id: user_id.to_string(),
            principal_type: PrincipalType::User,
            tenant_id,
            scopes,
        };
        self.register_key(key, entry);
    }

    /// Register a service account key
    pub fn register_service_account_key(
        &self,
        key: &str,
        service_id: &str,
        tenant_id: Option<String>,
        scopes: Vec<String>,
    ) {
        let entry = KeyEntry {
            principal_id: service_id.to_string(),
            principal_type: PrincipalType::ServiceAccount,
            tenant_id,
            scopes,
        };
        self.register_key(key, entry);
    }

    /// Check if a string looks like an API key for this provider
    fn is_api_key(&self, s: &str) -> bool {
        s.starts_with(&self.prefix) || s.starts_with("sk_") || s.starts_with("pat_")
    }
}

#[async_trait]
impl AuthProvider for StaticKeysProvider {
    fn name(&self) -> &str {
        "static_keys"
    }

    fn can_handle(&self, credentials: &Credentials) -> bool {
        // Handle if the secret looks like an API key
        self.is_api_key(&credentials.secret)
    }

    async fn authenticate(&self, credentials: &Credentials) -> AuthResult {
        let key = &credentials.secret;
        
        // Look up the key
        let entry = {
            let keys = self.keys.read();
            keys.get(key).cloned()
        };

        match entry {
            Some(entry) => {
                tracing::debug!(
                    principal_id = %entry.principal_id,
                    principal_type = ?entry.principal_type,
                    "Static key authentication successful"
                );

                let principal = AuthenticatedPrincipal {
                    id: entry.principal_id,
                    principal_type: entry.principal_type,
                    display_name: None,
                    email: None,
                    tenant_id: entry.tenant_id.clone(),
                    tenant_name: entry.tenant_id,
                    groups: vec![],
                    scopes: entry.scopes,
                    authenticated_at: chrono::Utc::now(),
                    expires_at: None,
                    provider: "static_keys".to_string(),
                    attributes: std::collections::HashMap::new(),
                };

                AuthResult::Success(principal)
            }
            None => {
                tracing::warn!("Invalid API key attempted");
                AuthResult::InvalidCredentials("Invalid API key".to_string())
            }
        }
    }
}

