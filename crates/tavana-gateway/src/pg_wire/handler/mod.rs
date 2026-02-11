//! PostgreSQL wire protocol handler - modular implementation
//!
//! - `config` - Server configuration (environment-driven)
//! - `core` - PgWireServer struct and lifecycle
//! - `connection` - TLS negotiation, startup handling, TCP keepalive
//! - `query_loop` - Main query processing loop (generic, handles Simple + Extended)
//! - `execution` - Query execution (streaming, routing, SmartScaler, COPY)
//! - `messages` - Protocol message building and sending (all generic)
//! - `auth` - Authentication handler (generic)
//! - `portal` - Portal state for cursor streaming (Extended Protocol)
//! - `utils` - Utility functions, constants, and types

pub mod config;
pub mod core;
mod connection;
mod query_loop;
mod execution;
mod messages;
mod auth;
mod portal;
pub mod utils;

// Re-export the main public API
pub use self::core::PgWireServer;
pub use config::PgWireConfig;
