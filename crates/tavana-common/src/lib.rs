//! Tavana Common Library
//! 
//! Shared types, utilities, and gRPC definitions for all Tavana services.

pub mod auth;
pub mod error;
pub mod tls;
pub mod proto;
pub mod config;

// Re-export commonly used types
pub use auth::{AuthToken, TokenType, UserIdentity, ApiKeyValidator};
pub use error::{TavanaError, Result};
pub use tls::{TlsConfig, create_client_tls_config, create_server_tls_config};

