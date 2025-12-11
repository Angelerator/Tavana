//! Event Calendar
//!
//! Handles special events like month-end, quarter-end, and custom events

use chrono::Utc;
use super::config::AdaptiveConfig;

/// Calculate event factor (lowest factor wins for threshold, highest for HPA)
pub fn event_threshold_factor(config: &AdaptiveConfig) -> f64 {
    let now = Utc::now();
    let mut min_factor = 1.0;
    
    for event in &config.events {
        if event.condition.is_active(&now) {
            tracing::debug!(
                "Event '{}' active: threshold_factor={}, hpa_factor={}",
                event.name, event.threshold_factor, event.hpa_factor
            );
            if event.threshold_factor < min_factor {
                min_factor = event.threshold_factor;
            }
        }
    }
    
    min_factor
}

/// Calculate event HPA factor (highest factor wins)
pub fn event_hpa_factor(config: &AdaptiveConfig) -> f64 {
    let now = Utc::now();
    let mut max_factor = 1.0;
    
    for event in &config.events {
        if event.condition.is_active(&now) {
            if event.hpa_factor > max_factor {
                max_factor = event.hpa_factor;
            }
        }
    }
    
    max_factor
}

/// Get list of active events
pub fn get_active_events(config: &AdaptiveConfig) -> Vec<String> {
    let now = Utc::now();
    config.events
        .iter()
        .filter(|e| e.condition.is_active(&now))
        .map(|e| e.name.clone())
        .collect()
}

