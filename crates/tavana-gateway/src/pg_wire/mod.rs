//! PostgreSQL wire protocol implementation with streaming support
//!
//! Enables Tableau, PowerBI, DBeaver and other BI tools to connect natively.
//! Uses true row-by-row streaming to handle unlimited result sizes without OOM.
//!
//! ## Module Structure
//!
//! - `handler` - Main PgWireServer implementation (modular)
//!   - `config` - Server configuration (environment-driven)
//!   - `core` - PgWireServer struct and lifecycle
//!   - `connection` - TLS negotiation, startup handling, TCP keepalive
//!   - `query_loop` - Main query processing loops
//!   - `execution` - Query execution (streaming, buffered, SmartScaler)
//!   - `messages` - Protocol message building and sending
//!   - `auth` - Authentication handlers
//!   - `extended` - Extended Query Protocol handlers
//!   - `portal` - Portal state for cursor streaming
//!   - `utils` - Utility functions, constants, and types
//! - `backpressure` - Backpressure-aware streaming writer
//! - `protocol` - Wire protocol types and constants
//!
//! ## Architecture
//!
//! 1. Connection arrives → authenticate via AuthGateway
//! 2. Query arrives → QueryQueue.enqueue() (blocks until capacity available)
//! 3. Queue depth signals HPA to scale up if needed
//! 4. When dispatched → SmartScaler selects worker + VPA pre-size
//! 5. Execute query with streaming (true streaming, OOM-proof)
//! 6. Complete → release capacity for next query

// Shared protocol helpers (used by handler modules)
pub mod backpressure;
pub mod protocol;

// Main server implementation
pub mod handler;

// Re-export PgWireServer and PgWireConfig as the main public API
pub use handler::PgWireServer;
pub use handler::PgWireConfig;
