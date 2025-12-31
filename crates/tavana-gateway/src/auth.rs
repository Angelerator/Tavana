//! Authentication middleware for the gateway

use parking_lot::RwLock;
use std::sync::Arc;
use tavana_common::{ApiKeyValidator, AuthToken, Result, TavanaError, UserIdentity};

/// Authentication service that validates both API keys and JWTs
/// NOTE: JWT/OIDC validation is planned for v1.1 - currently only API keys are supported
pub struct AuthService {
    api_key_validator: Arc<RwLock<ApiKeyValidator>>,
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

        // JWT validation is planned for v1.1 - for now, only API keys (tvn_*) are supported
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
