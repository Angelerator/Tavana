//! API Key validation utilities
//!
//! Provides validation and parsing for various API key formats.

use std::collections::HashMap;
use parking_lot::RwLock;

use crate::auth::identity::{AuthenticatedPrincipal, PrincipalType};

/// API key format prefixes
pub const PREFIX_TAVANA: &str = "tvn_";
pub const PREFIX_SECRET: &str = "sk_";
pub const PREFIX_PAT: &str = "pat_";
pub const PREFIX_SERVICE: &str = "sak_";

/// API key validation result
#[derive(Debug)]
pub enum ApiKeyResult {
    Valid(AuthenticatedPrincipal),
    Invalid(String),
    Unknown,
}

/// Validates API keys against a local registry
pub struct ApiKeyValidator {
    /// Registered keys (key -> principal)
    keys: RwLock<HashMap<String, RegisteredKey>>,
    /// Hashed keys support (for secure storage)
    use_hashing: bool,
}

#[derive(Debug, Clone)]
struct RegisteredKey {
    /// Hash of the key (if hashing enabled) or plaintext
    key_value: String,
    /// Principal info
    principal: AuthenticatedPrincipal,
    /// When this key was created
    created_at: chrono::DateTime<chrono::Utc>,
    /// Optional expiration
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ApiKeyValidator {
    /// Create a new validator
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            use_hashing: false,
        }
    }

    /// Create with hashing enabled
    pub fn with_hashing() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            use_hashing: true,
        }
    }

    /// Register an API key
    pub fn register(
        &self,
        key: &str,
        principal: AuthenticatedPrincipal,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let key_value = if self.use_hashing {
            self.hash_key(key)
        } else {
            key.to_string()
        };

        let registered = RegisteredKey {
            key_value: key_value.clone(),
            principal,
            created_at: chrono::Utc::now(),
            expires_at,
        };

        // Store by hash if hashing, otherwise by key
        let storage_key = if self.use_hashing {
            key_value
        } else {
            key.to_string()
        };

        self.keys.write().insert(storage_key, registered);
    }

    /// Validate an API key
    pub fn validate(&self, key: &str) -> ApiKeyResult {
        let lookup_key = if self.use_hashing {
            self.hash_key(key)
        } else {
            key.to_string()
        };

        let keys = self.keys.read();
        
        match keys.get(&lookup_key) {
            Some(registered) => {
                // Check expiration
                if let Some(expires_at) = registered.expires_at {
                    if chrono::Utc::now() > expires_at {
                        return ApiKeyResult::Invalid("API key expired".to_string());
                    }
                }

                let mut principal = registered.principal.clone();
                principal.authenticated_at = chrono::Utc::now();
                
                ApiKeyResult::Valid(principal)
            }
            None => ApiKeyResult::Unknown,
        }
    }

    /// Check if a string looks like an API key (any known key prefix)
    pub fn is_api_key(s: &str) -> bool {
        matches!(
            super::TokenType::detect(s),
            super::TokenType::ApiKey | super::TokenType::PersonalAccessToken | super::TokenType::ServiceAccountKey
        )
    }

    /// Get the key type from prefix
    pub fn key_type(key: &str) -> Option<PrincipalType> {
        if key.starts_with(PREFIX_PAT) {
            Some(PrincipalType::User)
        } else if key.starts_with(PREFIX_SERVICE) {
            Some(PrincipalType::ServiceAccount)
        } else if key.starts_with(PREFIX_TAVANA) || key.starts_with(PREFIX_SECRET) {
            Some(PrincipalType::ApiKey)
        } else {
            None
        }
    }

    /// Hash a key for secure storage
    fn hash_key(&self, key: &str) -> String {
        // Use blake3 for fast secure hashing
        let hash = blake3::hash(key.as_bytes());
        hash.to_hex().to_string()
    }

    /// Remove an API key
    pub fn revoke(&self, key: &str) {
        let lookup_key = if self.use_hashing {
            self.hash_key(key)
        } else {
            key.to_string()
        };
        
        self.keys.write().remove(&lookup_key);
    }

    /// Get count of registered keys
    pub fn count(&self) -> usize {
        self.keys.read().len()
    }
}

impl Default for ApiKeyValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_detection() {
        assert!(ApiKeyValidator::is_api_key("tvn_abc123"));
        assert!(ApiKeyValidator::is_api_key("sk_live_abc123"));
        assert!(ApiKeyValidator::is_api_key("pat_user_abc123"));
        assert!(ApiKeyValidator::is_api_key("sak_service_abc123"));
        assert!(!ApiKeyValidator::is_api_key("eyJhbGciOiJIUzI1NiJ9"));
        assert!(!ApiKeyValidator::is_api_key("regular_password"));
    }

    #[test]
    fn test_key_type() {
        assert_eq!(ApiKeyValidator::key_type("pat_abc"), Some(PrincipalType::User));
        assert_eq!(ApiKeyValidator::key_type("sak_abc"), Some(PrincipalType::ServiceAccount));
        assert_eq!(ApiKeyValidator::key_type("tvn_abc"), Some(PrincipalType::ApiKey));
        assert_eq!(ApiKeyValidator::key_type("unknown"), None);
    }
}

