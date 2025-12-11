//! Query History and EMA-based Adaptive Scaling
//!
//! Uses Exponential Moving Average of actual query patterns from the last 24 hours
//! to dynamically adjust thresholds instead of static time-based rules.

use std::collections::VecDeque;
use std::sync::RwLock;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// A single query execution record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRecord {
    pub timestamp: DateTime<Utc>,
    pub estimated_size_mb: u64,
    pub actual_size_mb: u64,
    pub execution_time_ms: u64,
    pub was_ephemeral: bool,
    pub success: bool,
}

/// Query history with EMA calculations
pub struct QueryHistory {
    /// Rolling window of query records (last 24 hours)
    records: RwLock<VecDeque<QueryRecord>>,
    /// EMA of query sizes (MB)
    size_ema: RwLock<f64>,
    /// EMA of query frequency (queries per minute)
    frequency_ema: RwLock<f64>,
    /// EMA of estimation accuracy (actual/estimated ratio)
    accuracy_ema: RwLock<f64>,
    /// EMA smoothing factor (0.1 = slow adaptation, 0.3 = fast)
    alpha: f64,
    /// Max records to keep
    max_records: usize,
}

impl QueryHistory {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(VecDeque::with_capacity(10000)),
            size_ema: RwLock::new(100.0),     // Start at 100MB
            frequency_ema: RwLock::new(1.0),   // Start at 1 qpm
            accuracy_ema: RwLock::new(1.0),    // Start at 100% accuracy
            alpha: 0.1,                        // 10% weight to new data
            max_records: 10000,
        }
    }

    /// Record a completed query
    pub fn record_query(&self, record: QueryRecord) {
        // Update EMAs
        {
            let mut size_ema = self.size_ema.write().unwrap();
            *size_ema = self.alpha * record.actual_size_mb as f64 + (1.0 - self.alpha) * *size_ema;
        }
        
        if record.estimated_size_mb > 0 {
            let accuracy = record.actual_size_mb as f64 / record.estimated_size_mb as f64;
            let mut accuracy_ema = self.accuracy_ema.write().unwrap();
            *accuracy_ema = self.alpha * accuracy + (1.0 - self.alpha) * *accuracy_ema;
        }

        // Update frequency
        self.update_frequency_ema();

        // Store record
        let mut records = self.records.write().unwrap();
        records.push_back(record);
        
        // Trim old records (older than 24 hours)
        let cutoff = Utc::now() - Duration::hours(24);
        while records.front().map_or(false, |r| r.timestamp < cutoff) {
            records.pop_front();
        }
        
        // Also enforce max records
        while records.len() > self.max_records {
            records.pop_front();
        }
    }

    fn update_frequency_ema(&self) {
        let records = self.records.read().unwrap();
        let now = Utc::now();
        let one_minute_ago = now - Duration::minutes(1);
        
        // Count queries in last minute
        let recent_count = records.iter()
            .filter(|r| r.timestamp > one_minute_ago)
            .count() as f64;
        
        drop(records);
        
        let mut freq_ema = self.frequency_ema.write().unwrap();
        *freq_ema = self.alpha * recent_count + (1.0 - self.alpha) * *freq_ema;
    }

    /// Get current EMA values
    pub fn get_emas(&self) -> HistoryEMAs {
        HistoryEMAs {
            size_ema_mb: *self.size_ema.read().unwrap(),
            frequency_ema_qpm: *self.frequency_ema.read().unwrap(),
            accuracy_ema: *self.accuracy_ema.read().unwrap(),
        }
    }

    /// Calculate adaptive threshold based on historical patterns
    /// 
    /// Logic:
    /// - If average query size is small, raise threshold (use worker pool more)
    /// - If average query size is large, lower threshold (use ephemeral pods more)
    /// - If frequency is high, lower threshold (isolate heavy queries)
    /// - If estimation accuracy is poor, be more conservative
    pub fn calculate_threshold_factor(&self, base_threshold_gb: f64) -> f64 {
        let emas = self.get_emas();
        
        // Size factor: if EMA size is much smaller than threshold, raise threshold
        // if EMA size is close to threshold, lower it
        let size_ratio = (emas.size_ema_mb / 1024.0) / base_threshold_gb;
        let size_factor = if size_ratio < 0.1 {
            1.5  // Queries are tiny, raise threshold
        } else if size_ratio < 0.5 {
            1.2
        } else if size_ratio < 1.0 {
            1.0
        } else if size_ratio < 2.0 {
            0.8
        } else {
            0.5  // Queries are huge, lower threshold
        };
        
        // Frequency factor: high frequency = lower threshold for isolation
        let freq_factor = if emas.frequency_ema_qpm < 1.0 {
            1.3  // Low traffic, relax threshold
        } else if emas.frequency_ema_qpm < 10.0 {
            1.0
        } else if emas.frequency_ema_qpm < 50.0 {
            0.8
        } else {
            0.6  // High traffic, be aggressive
        };
        
        // Accuracy factor: poor accuracy = be more conservative (lower threshold)
        let accuracy_factor = if emas.accuracy_ema < 0.5 || emas.accuracy_ema > 2.0 {
            0.7  // Estimation is way off, be conservative
        } else if emas.accuracy_ema < 0.8 || emas.accuracy_ema > 1.5 {
            0.9
        } else {
            1.0  // Estimation is accurate
        };
        
        let combined = size_factor * freq_factor * accuracy_factor;
        
        tracing::debug!(
            "EMA factors: size={:.2} freq={:.2} accuracy={:.2} -> combined={:.2}",
            size_factor, freq_factor, accuracy_factor, combined
        );
        
        combined
    }

    /// Get statistics for the last N hours
    pub fn get_hourly_stats(&self, hours: u32) -> Vec<HourlyStats> {
        let records = self.records.read().unwrap();
        let now = Utc::now();
        let mut stats = Vec::with_capacity(hours as usize);
        
        for h in 0..hours {
            let start = now - Duration::hours((h + 1) as i64);
            let end = now - Duration::hours(h as i64);
            
            let hour_records: Vec<_> = records.iter()
                .filter(|r| r.timestamp >= start && r.timestamp < end)
                .collect();
            
            let count = hour_records.len();
            let avg_size = if count > 0 {
                hour_records.iter().map(|r| r.actual_size_mb).sum::<u64>() as f64 / count as f64
            } else {
                0.0
            };
            let ephemeral_pct = if count > 0 {
                hour_records.iter().filter(|r| r.was_ephemeral).count() as f64 / count as f64 * 100.0
            } else {
                0.0
            };
            
            stats.push(HourlyStats {
                hours_ago: h,
                query_count: count,
                avg_size_mb: avg_size,
                ephemeral_percent: ephemeral_pct,
            });
        }
        
        stats
    }
}

impl Default for QueryHistory {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEMAs {
    pub size_ema_mb: f64,
    pub frequency_ema_qpm: f64,
    pub accuracy_ema: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyStats {
    pub hours_ago: u32,
    pub query_count: usize,
    pub avg_size_mb: f64,
    pub ephemeral_percent: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_updates() {
        let history = QueryHistory::new();
        
        // Record some queries
        for i in 0..10 {
            history.record_query(QueryRecord {
                timestamp: Utc::now(),
                estimated_size_mb: 100,
                actual_size_mb: 100 + i * 10,
                execution_time_ms: 100,
                was_ephemeral: false,
                success: true,
            });
        }
        
        let emas = history.get_emas();
        assert!(emas.size_ema_mb > 100.0); // Should have increased from initial
        assert!(emas.frequency_ema_qpm > 0.0);
    }
}

