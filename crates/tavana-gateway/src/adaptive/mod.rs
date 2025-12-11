//! Adaptive Scaling Module
//!
//! Dynamically adjusts routing thresholds and HPA settings based on:
//! - Time of day patterns
//! - Day of week patterns
//! - Cluster load metrics
//! - Special events (month-end, quarter-end, custom)
//! - Historical query patterns

mod config;
mod controller;
mod event_calendar;
mod history;
mod hpa_manager;
mod load_metrics;
mod time_patterns;

pub use config::AdaptiveConfig;
pub use controller::AdaptiveController;
pub use history::{QueryHistory, QueryRecord, HistoryEMAs};

