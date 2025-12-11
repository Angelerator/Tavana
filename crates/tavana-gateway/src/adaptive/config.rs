//! Adaptive Configuration
//!
//! Defines configuration structures for adaptive scaling behavior

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main adaptive scaling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Timezone for time-based patterns (e.g., "UTC", "America/New_York", "Europe/London", "Asia/Tokyo")
    /// Supports international locations with different business hours
    #[serde(default = "default_timezone")]
    pub timezone: String,
    
    /// Base threshold in GB (default: 2)
    #[serde(default = "default_base_threshold")]
    pub base_threshold_gb: f64,
    
    /// Minimum threshold in GB
    #[serde(default = "default_min_threshold")]
    pub min_threshold_gb: f64,
    
    /// Maximum threshold in GB
    #[serde(default = "default_max_threshold")]
    pub max_threshold_gb: f64,
    
    /// Base HPA minimum replicas
    #[serde(default = "default_hpa_min")]
    pub base_hpa_min: u32,
    
    /// Base HPA maximum replicas
    #[serde(default = "default_hpa_max")]
    pub base_hpa_max: u32,
    
    /// Absolute minimum HPA replicas
    #[serde(default = "default_abs_hpa_min")]
    pub absolute_hpa_min: u32,
    
    /// Absolute maximum HPA replicas
    #[serde(default = "default_abs_hpa_max")]
    pub absolute_hpa_max: u32,
    
    /// Time-based patterns
    #[serde(default)]
    pub time_patterns: HashMap<String, TimePatternConfig>,
    
    /// Day-based patterns
    #[serde(default)]
    pub day_patterns: HashMap<String, DayPatternConfig>,
    
    /// Special events
    #[serde(default)]
    pub events: Vec<EventConfig>,
    
    /// Load-based response
    #[serde(default)]
    pub load_response: LoadResponseConfig,
    
    /// Recalculation interval in seconds
    #[serde(default = "default_recalc_interval")]
    pub recalc_interval_secs: u64,
}

fn default_timezone() -> String { "UTC".to_string() }
fn default_base_threshold() -> f64 { 2.0 }
fn default_min_threshold() -> f64 { 0.5 }
fn default_max_threshold() -> f64 { 20.0 }
fn default_hpa_min() -> u32 { 2 }
fn default_hpa_max() -> u32 { 10 }
fn default_abs_hpa_min() -> u32 { 1 }
fn default_abs_hpa_max() -> u32 { 100 }
fn default_recalc_interval() -> u64 { 60 }

impl Default for AdaptiveConfig {
    fn default() -> Self {
        let mut time_patterns = HashMap::new();
        time_patterns.insert("business_hours".to_string(), TimePatternConfig {
            start_hour: 9,
            end_hour: 17,
            threshold_factor: 0.8,
            hpa_factor: 1.5,
        });
        time_patterns.insert("night".to_string(), TimePatternConfig {
            start_hour: 22,
            end_hour: 6,
            threshold_factor: 1.5,
            hpa_factor: 0.5,
        });

        let mut day_patterns = HashMap::new();
        day_patterns.insert("weekend".to_string(), DayPatternConfig {
            days: vec![6, 7], // Saturday, Sunday
            threshold_factor: 1.3,
            hpa_factor: 0.5,
        });
        day_patterns.insert("friday".to_string(), DayPatternConfig {
            days: vec![5],
            threshold_factor: 0.9,
            hpa_factor: 1.2,
        });

        let events = vec![
            EventConfig {
                name: "month_end".to_string(),
                condition: EventCondition::DayOfMonth { min_day: 28, max_day: 31 },
                threshold_factor: 0.5,
                hpa_factor: 2.0,
            },
            EventConfig {
                name: "quarter_end".to_string(),
                condition: EventCondition::QuarterEnd { start_day: 25 },
                threshold_factor: 0.3,
                hpa_factor: 3.0,
            },
        ];

        Self {
            timezone: "UTC".to_string(),  // Default UTC, configure per deployment
            base_threshold_gb: 2.0,  // Changed from 6GB to 2GB
            min_threshold_gb: 0.5,   // Allow as low as 500MB
            max_threshold_gb: 20.0,
            base_hpa_min: 2,
            base_hpa_max: 10,
            absolute_hpa_min: 1,
            absolute_hpa_max: 100,
            time_patterns,
            day_patterns,
            events,
            load_response: LoadResponseConfig::default(),
            recalc_interval_secs: 60,
        }
    }
}

/// Time-based pattern configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimePatternConfig {
    /// Start hour (0-23)
    pub start_hour: u8,
    /// End hour (0-23)
    pub end_hour: u8,
    /// Threshold multiplier
    pub threshold_factor: f64,
    /// HPA multiplier
    pub hpa_factor: f64,
}

/// Day-based pattern configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DayPatternConfig {
    /// Days of week (1=Monday, 7=Sunday)
    pub days: Vec<u8>,
    /// Threshold multiplier
    pub threshold_factor: f64,
    /// HPA multiplier
    pub hpa_factor: f64,
}

/// Event configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfig {
    /// Event name
    pub name: String,
    /// Condition for event
    pub condition: EventCondition,
    /// Threshold multiplier
    pub threshold_factor: f64,
    /// HPA multiplier
    pub hpa_factor: f64,
}

/// Event condition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventCondition {
    /// Day of month range
    DayOfMonth { min_day: u8, max_day: u8 },
    /// Quarter end (March, June, September, December)
    QuarterEnd { start_day: u8 },
    /// Specific dates
    Dates { dates: Vec<String> },
    /// Custom expression (for future use)
    Custom { expression: String },
}

impl EventCondition {
    /// Check if condition is met
    pub fn is_active(&self, now: &chrono::DateTime<chrono::Utc>) -> bool {
        use chrono::Datelike;
        
        match self {
            EventCondition::DayOfMonth { min_day, max_day } => {
                let day = now.day() as u8;
                day >= *min_day && day <= *max_day
            }
            EventCondition::QuarterEnd { start_day } => {
                let month = now.month();
                let day = now.day() as u8;
                // Quarter-end months: 3, 6, 9, 12
                (month % 3 == 0) && day >= *start_day
            }
            EventCondition::Dates { dates } => {
                let today = now.format("%Y-%m-%d").to_string();
                dates.contains(&today)
            }
            EventCondition::Custom { .. } => {
                // TODO: Implement expression parser
                false
            }
        }
    }
}

/// Load-based response configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadResponseConfig {
    /// Utilization thresholds (e.g., [0.3, 0.5, 0.7, 0.9])
    pub utilization_thresholds: Vec<f64>,
    /// Threshold factors for each level (e.g., [1.5, 1.0, 0.7, 0.5])
    pub threshold_factors: Vec<f64>,
    /// HPA factors for each level (e.g., [0.5, 1.0, 1.5, 2.0])
    pub hpa_factors: Vec<f64>,
}

impl Default for LoadResponseConfig {
    fn default() -> Self {
        Self {
            utilization_thresholds: vec![0.3, 0.5, 0.7, 0.9],
            threshold_factors: vec![1.5, 1.0, 0.7, 0.5],
            hpa_factors: vec![0.5, 1.0, 1.5, 2.0],
        }
    }
}

impl LoadResponseConfig {
    /// Get factors for a given utilization level
    pub fn get_factors(&self, utilization: f64) -> (f64, f64) {
        for (i, &threshold) in self.utilization_thresholds.iter().enumerate() {
            if utilization <= threshold {
                let threshold_factor = self.threshold_factors.get(i).copied().unwrap_or(1.0);
                let hpa_factor = self.hpa_factors.get(i).copied().unwrap_or(1.0);
                return (threshold_factor, hpa_factor);
            }
        }
        
        // Above all thresholds - use last values
        let threshold_factor = self.threshold_factors.last().copied().unwrap_or(0.5);
        let hpa_factor = self.hpa_factors.last().copied().unwrap_or(2.0);
        (threshold_factor, hpa_factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_response_factors() {
        let config = LoadResponseConfig::default();
        
        // Low utilization
        let (t, h) = config.get_factors(0.2);
        assert_eq!(t, 1.5);
        assert_eq!(h, 0.5);
        
        // Medium utilization
        let (t, h) = config.get_factors(0.6);
        assert_eq!(t, 0.7);
        assert_eq!(h, 1.5);
        
        // High utilization
        let (t, h) = config.get_factors(0.95);
        assert_eq!(t, 0.5);
        assert_eq!(h, 2.0);
    }

    #[test]
    fn test_event_condition() {
        use chrono::TimeZone;
        
        // Test month end
        let condition = EventCondition::DayOfMonth { min_day: 28, max_day: 31 };
        let jan_30 = chrono::Utc.with_ymd_and_hms(2024, 1, 30, 12, 0, 0).unwrap();
        assert!(condition.is_active(&jan_30));
        
        let jan_15 = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();
        assert!(!condition.is_active(&jan_15));
        
        // Test quarter end
        let quarter = EventCondition::QuarterEnd { start_day: 25 };
        let mar_28 = chrono::Utc.with_ymd_and_hms(2024, 3, 28, 12, 0, 0).unwrap();
        assert!(quarter.is_active(&mar_28));
        
        let feb_28 = chrono::Utc.with_ymd_and_hms(2024, 2, 28, 12, 0, 0).unwrap();
        assert!(!quarter.is_active(&feb_28));
    }
}

