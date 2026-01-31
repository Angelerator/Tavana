//! Result Spooler for Very Large Result Sets
//!
//! When result sets exceed memory capacity (configurable threshold), this module
//! spools results to Azure Blob Storage for efficient retrieval.
//!
//! ## Architecture
//!
//! ```text
//! Client (DBeaver) <--> Gateway <--> ResultSpooler <--> Azure Blob
//!                         |                ^
//!                         v                |
//!                      Worker (DuckDB) ----+
//! ```
//!
//! ## Flow
//!
//! 1. Query starts executing on worker
//! 2. Gateway receives streaming results
//! 3. If row count exceeds threshold, ResultSpooler starts spooling to Azure
//! 4. Subsequent FETCH operations read from Azure instead of worker
//! 5. On CLOSE or timeout, spool file is deleted
//!
//! ## Benefits
//!
//! - Handles result sets of any size (limited by Azure Blob capacity)
//! - Releases worker resources early (query completes, worker is free)
//! - Enables resume after gateway restart (not implemented yet)
//! - Provides progress tracking for long-running queries

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for result spooling
#[derive(Debug, Clone)]
pub struct SpoolerConfig {
    /// Minimum rows before spooling kicks in (default: 50,000)
    pub spool_threshold_rows: usize,
    /// Azure Blob container for spool files
    pub azure_container: String,
    /// Prefix for spool file names
    pub spool_prefix: String,
    /// TTL for spool files (cleanup after this duration)
    pub spool_ttl: Duration,
    /// Maximum spool file size before splitting (default: 1GB)
    pub max_spool_file_size: usize,
    /// Whether spooling is enabled
    pub enabled: bool,
}

impl Default for SpoolerConfig {
    fn default() -> Self {
        Self {
            spool_threshold_rows: std::env::var("TAVANA_SPOOL_THRESHOLD_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50_000),
            azure_container: std::env::var("TAVANA_SPOOL_CONTAINER")
                .unwrap_or_else(|_| "tavana-spool".to_string()),
            spool_prefix: std::env::var("TAVANA_SPOOL_PREFIX")
                .unwrap_or_else(|_| "results/".to_string()),
            spool_ttl: Duration::from_secs(
                std::env::var("TAVANA_SPOOL_TTL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600), // 1 hour default
            ),
            max_spool_file_size: std::env::var("TAVANA_SPOOL_MAX_FILE_SIZE")
                .ok()
                .and_then(|v| parse_size(&v))
                .unwrap_or(1024 * 1024 * 1024), // 1GB default
            enabled: std::env::var("TAVANA_SPOOL_ENABLED")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false), // Disabled by default
        }
    }
}

/// Parse a size string like "1GB", "512MB" to bytes
fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim().to_uppercase();
    if s.ends_with("GB") {
        s[..s.len()-2].trim().parse::<usize>().ok().map(|v| v * 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        s[..s.len()-2].trim().parse::<usize>().ok().map(|v| v * 1024 * 1024)
    } else if s.ends_with("KB") {
        s[..s.len()-2].trim().parse::<usize>().ok().map(|v| v * 1024)
    } else {
        s.parse::<usize>().ok()
    }
}

/// A spooled result set
#[derive(Debug)]
pub struct SpooledResult {
    /// Unique ID for this spool
    pub spool_id: String,
    /// User who created this spool
    pub user_id: String,
    /// Query that generated this result
    pub query: String,
    /// Column metadata
    pub columns: Vec<(String, String)>,
    /// Total rows spooled
    pub total_rows: usize,
    /// Current read offset
    pub read_offset: usize,
    /// Azure Blob URLs for spool files (may be multiple if split)
    pub blob_urls: Vec<String>,
    /// When the spool was created
    pub created_at: Instant,
    /// When the spool was last accessed
    pub last_accessed: Instant,
    /// Whether spooling is complete
    pub is_complete: bool,
}

impl SpooledResult {
    /// Check if this spool has expired
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.last_accessed.elapsed() > ttl
    }
    
    /// Mark as accessed (reset TTL timer)
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
    
    /// Calculate remaining rows
    pub fn remaining_rows(&self) -> usize {
        self.total_rows.saturating_sub(self.read_offset)
    }
}

/// Result spooler for managing large result sets
pub struct ResultSpooler {
    /// Configuration
    config: SpoolerConfig,
    /// Active spools (spool_id -> SpooledResult)
    spools: Arc<RwLock<HashMap<String, SpooledResult>>>,
    /// Azure storage client (lazy initialized)
    azure_client: Option<Arc<aws_sdk_s3::Client>>, // Using S3 API for Azure Blob
}

impl ResultSpooler {
    /// Create a new result spooler
    pub fn new(config: SpoolerConfig) -> Self {
        Self {
            config,
            spools: Arc::new(RwLock::new(HashMap::new())),
            azure_client: None,
        }
    }
    
    /// Create with default configuration from environment
    pub fn from_env() -> Self {
        Self::new(SpoolerConfig::default())
    }
    
    /// Check if spooling is enabled and should be used for this result size
    pub fn should_spool(&self, row_count: usize) -> bool {
        self.config.enabled && row_count >= self.config.spool_threshold_rows
    }
    
    /// Get spooler configuration
    pub fn config(&self) -> &SpoolerConfig {
        &self.config
    }
    
    /// Start spooling a result set
    /// Returns the spool_id for later retrieval
    pub async fn start_spool(
        &self,
        user_id: &str,
        query: &str,
        columns: Vec<(String, String)>,
    ) -> Result<String> {
        let spool_id = format!("{}_{}", user_id, uuid::Uuid::new_v4());
        
        info!(
            spool_id = %spool_id,
            user_id = %user_id,
            columns = columns.len(),
            "Starting result spool"
        );
        
        let spooled = SpooledResult {
            spool_id: spool_id.clone(),
            user_id: user_id.to_string(),
            query: query.to_string(),
            columns,
            total_rows: 0,
            read_offset: 0,
            blob_urls: Vec::new(),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            is_complete: false,
        };
        
        let mut spools = self.spools.write().await;
        spools.insert(spool_id.clone(), spooled);
        
        Ok(spool_id)
    }
    
    /// Append rows to a spool
    /// In production, this would upload to Azure Blob Storage
    pub async fn append_rows(
        &self,
        spool_id: &str,
        rows: Vec<Vec<String>>,
    ) -> Result<()> {
        let mut spools = self.spools.write().await;
        
        if let Some(spool) = spools.get_mut(spool_id) {
            let row_count = rows.len();
            spool.total_rows += row_count;
            spool.touch();
            
            // TODO: Actually upload to Azure Blob Storage
            // For now, just track metadata
            debug!(
                spool_id = %spool_id,
                rows_added = row_count,
                total_rows = spool.total_rows,
                "Appended rows to spool"
            );
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("Spool not found: {}", spool_id))
        }
    }
    
    /// Mark a spool as complete
    pub async fn complete_spool(&self, spool_id: &str) -> Result<()> {
        let mut spools = self.spools.write().await;
        
        if let Some(spool) = spools.get_mut(spool_id) {
            spool.is_complete = true;
            spool.touch();
            
            info!(
                spool_id = %spool_id,
                total_rows = spool.total_rows,
                "Spool completed"
            );
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("Spool not found: {}", spool_id))
        }
    }
    
    /// Fetch rows from a spool
    /// Returns (rows, has_more)
    pub async fn fetch_rows(
        &self,
        spool_id: &str,
        max_rows: usize,
    ) -> Result<(Vec<Vec<String>>, bool)> {
        let mut spools = self.spools.write().await;
        
        if let Some(spool) = spools.get_mut(spool_id) {
            spool.touch();
            
            // TODO: Actually read from Azure Blob Storage
            // For now, return empty (placeholder)
            let remaining = spool.remaining_rows();
            let to_fetch = max_rows.min(remaining);
            spool.read_offset += to_fetch;
            
            debug!(
                spool_id = %spool_id,
                fetched = to_fetch,
                remaining = spool.remaining_rows(),
                "Fetched rows from spool"
            );
            
            // Placeholder: return empty rows
            // In real implementation, read from Azure Blob
            let rows: Vec<Vec<String>> = Vec::new();
            let has_more = spool.remaining_rows() > 0;
            
            Ok((rows, has_more))
        } else {
            Err(anyhow::anyhow!("Spool not found: {}", spool_id))
        }
    }
    
    /// Close and cleanup a spool
    pub async fn close_spool(&self, spool_id: &str) -> Result<()> {
        let mut spools = self.spools.write().await;
        
        if let Some(spool) = spools.remove(spool_id) {
            info!(
                spool_id = %spool_id,
                total_rows = spool.total_rows,
                read_rows = spool.read_offset,
                elapsed_secs = spool.created_at.elapsed().as_secs(),
                "Closing and cleaning up spool"
            );
            
            // TODO: Delete Azure Blob files
            for blob_url in &spool.blob_urls {
                debug!(blob_url = %blob_url, "Deleting spool blob");
            }
            
            Ok(())
        } else {
            // Not an error if spool doesn't exist (idempotent)
            Ok(())
        }
    }
    
    /// Get spool info without modifying it
    pub async fn get_spool_info(&self, spool_id: &str) -> Option<SpoolInfo> {
        let spools = self.spools.read().await;
        spools.get(spool_id).map(|s| SpoolInfo {
            spool_id: s.spool_id.clone(),
            total_rows: s.total_rows,
            read_offset: s.read_offset,
            is_complete: s.is_complete,
            created_secs_ago: s.created_at.elapsed().as_secs(),
        })
    }
    
    /// Cleanup expired spools (call periodically)
    pub async fn cleanup_expired(&self) -> usize {
        let mut spools = self.spools.write().await;
        let before = spools.len();
        
        let expired: Vec<String> = spools
            .iter()
            .filter(|(_, s)| s.is_expired(self.config.spool_ttl))
            .map(|(id, _)| id.clone())
            .collect();
        
        for id in &expired {
            if let Some(spool) = spools.remove(id) {
                warn!(
                    spool_id = %id,
                    total_rows = spool.total_rows,
                    idle_secs = spool.last_accessed.elapsed().as_secs(),
                    "Cleaning up expired spool"
                );
                // TODO: Delete Azure Blob files
            }
        }
        
        before - spools.len()
    }
    
    /// Get statistics about active spools
    pub async fn stats(&self) -> SpoolerStats {
        let spools = self.spools.read().await;
        
        let total_rows: usize = spools.values().map(|s| s.total_rows).sum();
        let complete = spools.values().filter(|s| s.is_complete).count();
        
        SpoolerStats {
            active_spools: spools.len(),
            complete_spools: complete,
            pending_spools: spools.len() - complete,
            total_rows_spooled: total_rows,
        }
    }
}

/// Lightweight spool info for status checks
#[derive(Debug, Clone)]
pub struct SpoolInfo {
    pub spool_id: String,
    pub total_rows: usize,
    pub read_offset: usize,
    pub is_complete: bool,
    pub created_secs_ago: u64,
}

/// Spooler statistics
#[derive(Debug, Clone, Default)]
pub struct SpoolerStats {
    pub active_spools: usize,
    pub complete_spools: usize,
    pub pending_spools: usize,
    pub total_rows_spooled: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("512MB"), Some(512 * 1024 * 1024));
        assert_eq!(parse_size("1024KB"), Some(1024 * 1024));
        assert_eq!(parse_size("1024"), Some(1024));
    }
    
    #[test]
    fn test_spooler_config_default() {
        let config = SpoolerConfig::default();
        assert_eq!(config.spool_threshold_rows, 50_000);
        assert!(!config.enabled); // Disabled by default
    }
    
    #[tokio::test]
    async fn test_spool_lifecycle() {
        let spooler = ResultSpooler::from_env();
        
        // Start spool
        let spool_id = spooler.start_spool(
            "test_user",
            "SELECT * FROM test",
            vec![("col1".to_string(), "TEXT".to_string())],
        ).await.unwrap();
        
        // Append rows
        spooler.append_rows(&spool_id, vec![vec!["value".to_string()]]).await.unwrap();
        
        // Complete
        spooler.complete_spool(&spool_id).await.unwrap();
        
        // Check info
        let info = spooler.get_spool_info(&spool_id).await.unwrap();
        assert_eq!(info.total_rows, 1);
        assert!(info.is_complete);
        
        // Close
        spooler.close_spool(&spool_id).await.unwrap();
        
        // Should be gone
        assert!(spooler.get_spool_info(&spool_id).await.is_none());
    }
}
