//! Time-Based Pattern Matching
//!
//! Calculates adjustment factors based on time of day and day of week
//! Supports international locations with configurable timezone

use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use super::config::AdaptiveConfig;

/// Get current time in configured timezone
fn get_local_time(config: &AdaptiveConfig) -> chrono::DateTime<Tz> {
    let tz: Tz = config.timezone.parse().unwrap_or(chrono_tz::UTC);
    Utc::now().with_timezone(&tz)
}

/// Calculate time-of-day factor
pub fn time_of_day_factor(config: &AdaptiveConfig) -> f64 {
    let now = get_local_time(config);
    let hour = now.hour() as u8;
    
    for (name, pattern) in &config.time_patterns {
        if is_in_time_range(hour, pattern.start_hour, pattern.end_hour) {
            tracing::debug!(
                "Time pattern '{}' active (hour {} in {}): threshold_factor={}, hpa_factor={}",
                name, hour, config.timezone, pattern.threshold_factor, pattern.hpa_factor
            );
            return pattern.threshold_factor;
        }
    }
    
    1.0 // Default: no adjustment
}

/// Calculate time-of-day HPA factor
pub fn time_of_day_hpa_factor(config: &AdaptiveConfig) -> f64 {
    let now = get_local_time(config);
    let hour = now.hour() as u8;
    
    for (_, pattern) in &config.time_patterns {
        if is_in_time_range(hour, pattern.start_hour, pattern.end_hour) {
            return pattern.hpa_factor;
        }
    }
    
    1.0
}

/// Check if hour is in time range (handles overnight ranges)
fn is_in_time_range(hour: u8, start: u8, end: u8) -> bool {
    if start <= end {
        // Normal range (e.g., 9-17)
        hour >= start && hour < end
    } else {
        // Overnight range (e.g., 22-6)
        hour >= start || hour < end
    }
}

/// Calculate day-of-week factor
pub fn day_of_week_factor(config: &AdaptiveConfig) -> f64 {
    let now = get_local_time(config);
    let day = now.weekday().num_days_from_monday() as u8 + 1; // 1=Monday, 7=Sunday
    
    for (name, pattern) in &config.day_patterns {
        if pattern.days.contains(&day) {
            tracing::debug!(
                "Day pattern '{}' active (day {} in {}): threshold_factor={}, hpa_factor={}",
                name, day, config.timezone, pattern.threshold_factor, pattern.hpa_factor
            );
            return pattern.threshold_factor;
        }
    }
    
    1.0
}

/// Calculate day-of-week HPA factor
pub fn day_of_week_hpa_factor(config: &AdaptiveConfig) -> f64 {
    let now = get_local_time(config);
    let day = now.weekday().num_days_from_monday() as u8 + 1;
    
    for (_, pattern) in &config.day_patterns {
        if pattern.days.contains(&day) {
            return pattern.hpa_factor;
        }
    }
    
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range_normal() {
        // Business hours: 9-17
        assert!(is_in_time_range(9, 9, 17));
        assert!(is_in_time_range(12, 9, 17));
        assert!(is_in_time_range(16, 9, 17));
        assert!(!is_in_time_range(8, 9, 17));
        assert!(!is_in_time_range(17, 9, 17));
        assert!(!is_in_time_range(20, 9, 17));
    }

    #[test]
    fn test_time_range_overnight() {
        // Night: 22-6
        assert!(is_in_time_range(22, 22, 6));
        assert!(is_in_time_range(23, 22, 6));
        assert!(is_in_time_range(0, 22, 6));
        assert!(is_in_time_range(3, 22, 6));
        assert!(is_in_time_range(5, 22, 6));
        assert!(!is_in_time_range(6, 22, 6));
        assert!(!is_in_time_range(12, 22, 6));
        assert!(!is_in_time_range(21, 22, 6));
    }
}
