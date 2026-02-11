//! PostgreSQL wire protocol handler - modular implementation
//!
//! This module replaces the monolithic `server.rs` with a clean module structure:
//!
//! - `config` - Server configuration (environment-driven)
//! - `core` - PgWireServer struct and lifecycle
//! - `connection` - TLS negotiation, startup handling, TCP keepalive
//! - `query_loop` - Main query processing loops (TLS and non-TLS)
//! - `execution` - Query execution (streaming, buffered, SmartScaler)
//! - `messages` - Protocol message building and sending
//! - `auth` - Authentication handlers
//! - `extended` - Extended Query Protocol handlers (non-TLS)
//! - `portal` - Portal state for cursor streaming
//! - `utils` - Utility functions, constants, and types

pub mod config;
pub mod core;
mod connection;
mod query_loop;
mod execution;
mod messages;
mod auth;
mod extended;
mod portal;
pub mod utils;

// Re-export the main public API
pub use self::core::PgWireServer;
pub use config::PgWireConfig;
