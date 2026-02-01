//! Tavana Common Library
//!
//! Shared types, utilities, and gRPC definitions for all Tavana services.

pub mod auth;
pub mod config;
pub mod error;
pub mod k8s;
pub mod proto;
pub mod tls;

// Re-export commonly used types
pub use auth::{ApiKeyValidator, AuthToken, TokenType, UserIdentity};
pub use config::{env_or_default, env_parse_or_default};
pub use error::{Result, TavanaError};
pub use tls::{create_client_tls_config, create_server_tls_config, TlsConfig};
