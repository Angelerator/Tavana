//! JWT token validation
//!
//! Validates JWT tokens without external service calls.
//! Used for local validation when Separ is not available or for performance.

use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::auth::config::JwtConfig;
use crate::auth::identity::{AuthenticatedPrincipal, PrincipalType};

/// JWT validator for local token validation
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
    issuer: Option<String>,
}

impl JwtValidator {
    /// Create a new JWT validator from config
    pub fn new(config: &JwtConfig) -> anyhow::Result<Self> {
        let algorithm = match config.algorithm.to_uppercase().as_str() {
            "HS256" => Algorithm::HS256,
            "HS384" => Algorithm::HS384,
            "HS512" => Algorithm::HS512,
            "RS256" => Algorithm::RS256,
            "RS384" => Algorithm::RS384,
            "RS512" => Algorithm::RS512,
            "ES256" => Algorithm::ES256,
            "ES384" => Algorithm::ES384,
            _ => return Err(anyhow::anyhow!("Unsupported algorithm: {}", config.algorithm)),
        };

        let decoding_key = match algorithm {
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
                DecodingKey::from_secret(config.secret_or_key.as_bytes())
            }
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
                // Load from PEM file or inline PEM
                if config.secret_or_key.starts_with("-----BEGIN") {
                    DecodingKey::from_rsa_pem(config.secret_or_key.as_bytes())?
                } else {
                    let pem = std::fs::read_to_string(&config.secret_or_key)?;
                    DecodingKey::from_rsa_pem(pem.as_bytes())?
                }
            }
            Algorithm::ES256 | Algorithm::ES384 => {
                if config.secret_or_key.starts_with("-----BEGIN") {
                    DecodingKey::from_ec_pem(config.secret_or_key.as_bytes())?
                } else {
                    let pem = std::fs::read_to_string(&config.secret_or_key)?;
                    DecodingKey::from_ec_pem(pem.as_bytes())?
                }
            }
            _ => return Err(anyhow::anyhow!("Unsupported algorithm")),
        };

        let mut validation = Validation::new(algorithm);
        
        if let Some(ref iss) = config.issuer {
            validation.set_issuer(&[iss.clone()]);
        }
        
        if let Some(ref aud) = config.audience {
            validation.set_audience(&[aud.clone()]);
        }

        // Allow clock skew
        validation.leeway = config.clock_skew_secs;

        Ok(Self {
            decoding_key,
            validation,
            issuer: config.issuer.clone(),
        })
    }

    /// Validate a JWT token and return the principal
    pub fn validate(&self, token: &str) -> Result<AuthenticatedPrincipal, JwtError> {
        let token_data = decode::<JwtClaims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| JwtError::ValidationFailed(e.to_string()))?;

        let claims = token_data.claims;

        // Check expiration
        if let Some(exp) = claims.exp {
            if exp < Utc::now().timestamp() {
                return Err(JwtError::Expired);
            }
        }

        let principal_type = claims
            .principal_type
            .as_ref()
            .map(|pt| match pt.as_str() {
                "user" => PrincipalType::User,
                "application" | "app" => PrincipalType::Application,
                "service_account" | "service" => PrincipalType::ServiceAccount,
                _ => PrincipalType::User,
            })
            .unwrap_or(PrincipalType::User);

        let principal = AuthenticatedPrincipal {
            id: claims.sub.clone(),
            principal_type,
            display_name: claims.name.clone(),
            email: claims.email.clone(),
            tenant_id: claims.tenant_id.clone(),
            tenant_name: claims.tenant_name.clone(),
            groups: claims.groups.unwrap_or_default(),
            scopes: claims.scopes.unwrap_or_default(),
            authenticated_at: Utc::now(),
            expires_at: claims.exp.map(|ts| {
                DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now)
            }),
            provider: self.issuer.clone().unwrap_or_else(|| "jwt".to_string()),
            attributes: HashMap::new(),
        };

        Ok(principal)
    }
}

/// JWT claims structure (flexible to support various issuers)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user/principal ID)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: Option<i64>,
    /// Issued at (Unix timestamp)
    pub iat: Option<i64>,
    /// Issuer
    pub iss: Option<String>,
    /// Audience
    pub aud: Option<serde_json::Value>,
    /// Email
    pub email: Option<String>,
    /// Display name
    pub name: Option<String>,
    /// Tenant ID
    pub tenant_id: Option<String>,
    /// Tenant name
    pub tenant_name: Option<String>,
    /// Principal type
    pub principal_type: Option<String>,
    /// Groups/roles
    pub groups: Option<Vec<String>>,
    /// Scopes/permissions
    pub scopes: Option<Vec<String>>,
    /// Additional claims
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// JWT validation errors
#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("Token validation failed: {0}")]
    ValidationFailed(String),
    #[error("Token expired")]
    Expired,
    #[error("Invalid token format")]
    InvalidFormat,
}

