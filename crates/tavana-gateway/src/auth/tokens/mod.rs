//! Token parsers and validators
//!
//! Utilities for parsing and validating different token formats.

mod jwt;
pub mod api_key;

pub use jwt::JwtValidator;
pub use api_key::{ApiKeyValidator, ApiKeyResult};

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
        if token.starts_with("eyJ") && token.contains('.') {
            TokenType::Jwt
        } else if token.starts_with("pat_") {
            TokenType::PersonalAccessToken
        } else if token.starts_with("sak_") {
            TokenType::ServiceAccountKey
        } else if token.starts_with("sk_") || token.starts_with("tvn_") {
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

