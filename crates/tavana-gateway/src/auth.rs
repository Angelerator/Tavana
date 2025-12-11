//! Authentication middleware for the gateway

use tavana_common::{AuthToken, UserIdentity, ApiKeyValidator, TavanaError, Result};
use std::sync::Arc;
use parking_lot::RwLock;

/// Authentication service that validates both API keys and JWTs
pub struct AuthService {
    api_key_validator: Arc<RwLock<ApiKeyValidator>>,
    // TODO: Add OIDC provider for JWT validation
}

impl AuthService {
    /// Create a new authentication service
    pub fn new() -> Self {
        Self {
            api_key_validator: Arc::new(RwLock::new(ApiKeyValidator::new())),
        }
    }

    /// Authenticate a request using the provided token
    pub async fn authenticate(&self, token: &str) -> Result<UserIdentity> {
        // Try API key first (starts with tvn_)
        if token.starts_with("tvn_") {
            return self.api_key_validator.read().validate(token);
        }

        // Try JWT
        // TODO: Implement JWT validation
        Err(TavanaError::InvalidToken("Unknown token format".into()))
    }

    /// Register an API key
    pub fn register_api_key(&self, api_key: &str, identity: UserIdentity) {
        self.api_key_validator.write().register_key(api_key, identity);
    }
}

impl Default for AuthService {
    fn default() -> Self {
        Self::new()
    }
}

