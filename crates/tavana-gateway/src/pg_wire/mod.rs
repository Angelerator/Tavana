//! PostgreSQL wire protocol implementation with streaming support
//!
//! Enables Tableau, PowerBI, DBeaver and other BI tools to connect natively.
//! Uses true row-by-row streaming to handle unlimited result sizes without OOM.
//!
//! ## Module Structure
//!
//! - `config` - Server configuration (environment-driven)
//! - `protocol` - Wire protocol messages, types, and constants
//! - `query` - SQL manipulation and PostgreSQL command interception
//! - `auth` - Authentication handlers
//! - `connection` - TCP keepalive and connectivity utilities
//! - `server` - Main PgWireServer implementation
//!
//! ## Architecture
//!
//! 1. Connection arrives → authenticate via AuthGateway
//! 2. Query arrives → QueryQueue.enqueue() (blocks until capacity available)
//! 3. Queue depth signals HPA to scale up if needed
//! 4. When dispatched → SmartScaler selects worker + VPA pre-size
//! 5. Execute query with streaming (true streaming, OOM-proof)
//! 6. Complete → release capacity for next query

// Utility modules (new modular structure)
pub mod arrow_encoder;
pub mod auth;
pub mod backpressure;
pub mod config;
pub mod connection;
pub mod protocol;
pub mod query;

// Main server implementation (contains PgWireServer and connection handlers)
mod server;

// Re-export PgWireServer and PgWireConfig as the main public API
pub use server::PgWireServer;
pub use server::PgWireConfig;
