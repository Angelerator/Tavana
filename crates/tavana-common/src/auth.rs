//! Authentication and authorization utilities

use crate::error::{Result, TavanaError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authentication token types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenType {
    /// API Key authentication
    ApiKey,
    /// JWT/OIDC token
    Jwt,
}

/// Authentication token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthToken {
    /// The token string
    pub token: String,
    /// Type of token
    pub token_type: TokenType,
    /// Expiration time (None for non-expiring API keys)
    pub expires_at: Option<DateTime<Utc>>,
}

impl AuthToken {
    /// Create a new API key token
    pub fn api_key(key: String) -> Self {
        Self {
            token: key,
            token_type: TokenType::ApiKey,
            expires_at: None,
        }
    }

    /// Create a new JWT token
    pub fn jwt(token: String, expires_at: DateTime<Utc>) -> Self {
        Self {
            token,
            token_type: TokenType::Jwt,
            expires_at: Some(expires_at),
        }
    }

    /// Check if the token is expired
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => Utc::now() > exp,
            None => false,
        }
    }
}

/// User identity extracted from authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentity {
    /// Unique user identifier
    pub user_id: String,
    /// Tenant/organization identifier
    pub tenant_id: String,
    /// Permission scopes
    pub scopes: Vec<String>,
    /// Additional claims from the token
    pub claims: HashMap<String, String>,
}

impl UserIdentity {
    /// Create a new user identity
    pub fn new(user_id: String, tenant_id: String) -> Self {
        Self {
            user_id,
            tenant_id,
            scopes: Vec::new(),
            claims: HashMap::new(),
        }
    }

    /// Check if the user has a specific scope
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope || s == "*")
    }

    /// Check if the user has any of the specified scopes
    pub fn has_any_scope(&self, scopes: &[&str]) -> bool {
        scopes.iter().any(|s| self.has_scope(s))
    }

    /// Add a scope to the user
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes.push(scope.into());
        self
    }

    /// Add a claim to the user
    pub fn with_claim(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.claims.insert(key.into(), value.into());
        self
    }
}

/// API Key validator
pub struct ApiKeyValidator {
    /// Hashed API keys mapped to user identities
    keys: HashMap<String, UserIdentity>,
}

impl ApiKeyValidator {
    /// Create a new API key validator
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Register an API key (stores hash of the key)
    pub fn register_key(&mut self, api_key: &str, identity: UserIdentity) {
        let hash = Self::hash_key(api_key);
        self.keys.insert(hash, identity);
    }

    /// Validate an API key and return the user identity
    pub fn validate(&self, api_key: &str) -> Result<UserIdentity> {
        let hash = Self::hash_key(api_key);
        self.keys
            .get(&hash)
            .cloned()
            .ok_or_else(|| TavanaError::InvalidToken("Invalid API key".into()))
    }

    /// Hash an API key using BLAKE3
    fn hash_key(key: &str) -> String {
        let hash = blake3::hash(key.as_bytes());
        hash.to_hex().to_string()
    }

    /// Generate a new random API key
    pub fn generate_api_key() -> String {
        use base64::Engine;
        let mut bytes = [0u8; 32];
        // Use a simple random generation - in production use proper RNG
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let hash = blake3::hash(&timestamp.to_le_bytes());
        bytes.copy_from_slice(hash.as_bytes());
        
        format!("tvn_{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }
}

impl Default for ApiKeyValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Standard scopes for Tavana
pub mod scopes {
    /// Read data (execute queries)
    pub const READ: &str = "read";
    /// Write data (modify tables)
    pub const WRITE: &str = "write";
    /// Admin operations (manage users, settings)
    pub const ADMIN: &str = "admin";
    /// Catalog operations (create/modify tables)
    pub const CATALOG: &str = "catalog";
    /// View usage and billing
    pub const BILLING: &str = "billing";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_validation() {
        let mut validator = ApiKeyValidator::new();
        let key = "test_api_key_12345";
        let identity = UserIdentity::new("user1".into(), "tenant1".into())
            .with_scope(scopes::READ);

        validator.register_key(key, identity.clone());

        let result = validator.validate(key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().user_id, "user1");

        let invalid = validator.validate("wrong_key");
        assert!(invalid.is_err());
    }

    #[test]
    fn test_user_identity_scopes() {
        let user = UserIdentity::new("user1".into(), "tenant1".into())
            .with_scope(scopes::READ)
            .with_scope(scopes::CATALOG);

        assert!(user.has_scope(scopes::READ));
        assert!(user.has_scope(scopes::CATALOG));
        assert!(!user.has_scope(scopes::ADMIN));
        assert!(user.has_any_scope(&[scopes::READ, scopes::ADMIN]));
    }

    #[test]
    fn test_generate_api_key() {
        let key = ApiKeyValidator::generate_api_key();
        assert!(key.starts_with("tvn_"));
        assert!(key.len() > 40);
    }
}

