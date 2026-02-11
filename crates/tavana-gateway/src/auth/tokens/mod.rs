//! Token parsers and validators
//!
//! Utilities for parsing and validating different token formats.

mod jwt;
pub mod api_key;

pub use jwt::JwtValidator;
pub use api_key::{ApiKeyValidator, ApiKeyResult};

use api_key::{PREFIX_TAVANA, PREFIX_SECRET, PREFIX_PAT, PREFIX_SERVICE};

/// JWT token prefix (base64-encoded JSON header)
pub const JWT_PREFIX: &str = "eyJ";

/// Token type detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    /// JWT (eyJ...)
    Jwt,
    /// Personal Access Token (pat_...)
    PersonalAccessToken,
    /// Service Account Key (sak_...)
    ServiceAccountKey,
    /// API Key (sk_, tvn_, ...)
    ApiKey,
    /// Unknown/password
    Unknown,
}

impl TokenType {
    /// Detect token type from a string
    pub fn detect(token: &str) -> Self {
        if token.starts_with(JWT_PREFIX) && token.contains('.') {
            TokenType::Jwt
        } else if token.starts_with(PREFIX_PAT) {
            TokenType::PersonalAccessToken
        } else if token.starts_with(PREFIX_SERVICE) {
            TokenType::ServiceAccountKey
        } else if token.starts_with(PREFIX_SECRET) || token.starts_with(PREFIX_TAVANA) {
            TokenType::ApiKey
        } else {
            TokenType::Unknown
        }
    }

    /// Check if this is a token type (vs password)
    pub fn is_token(&self) -> bool {
        !matches!(self, TokenType::Unknown)
    }
}

