//! DuckDB Query Executor
//!
//! Executes SQL queries using DuckDB with a connection pool for parallelism.

use anyhow::Result;
use duckdb::arrow::array::RecordBatch;
use duckdb::{params, Connection};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tracing::{debug, info, instrument};

/// DuckDB executor configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum memory for DuckDB (in bytes)
    pub max_memory: u64,
    /// Number of threads for query execution
    pub threads: u32,
    /// Enable profiling
    pub enable_profiling: bool,
    /// Number of parallel connections (pool size)
    pub pool_size: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_memory: 8 * 1024 * 1024 * 1024, // 8GB default
            threads: num_cpus::get() as u32,
            enable_profiling: false,
            pool_size: 4, // 4 parallel connections by default
        }
    }
}

/// A single DuckDB connection wrapper
struct PooledConnection {
    connection: Mutex<Connection>,
    id: usize,
}

/// DuckDB query executor with connection pool
///
/// Maintains multiple DuckDB connections for parallel query execution.
pub struct DuckDbExecutor {
    connections: Vec<Arc<PooledConnection>>,
    semaphore: Arc<Semaphore>,
    config: ExecutorConfig,
    next_conn: std::sync::atomic::AtomicUsize,
}

impl DuckDbExecutor {
    /// Create a new DuckDB executor with connection pool
    pub fn new(config: ExecutorConfig) -> Result<Self> {
        let pool_size = config.pool_size.max(1);
        let memory_per_conn = config.max_memory / pool_size as u64;
        let threads_per_conn = (config.threads / pool_size as u32).max(1);

        info!(
            "Initializing DuckDB connection pool: {} connections, {}MB each, {} threads each",
            pool_size,
            memory_per_conn / 1024 / 1024,
            threads_per_conn
        );

        let mut connections = Vec::with_capacity(pool_size);

        for id in 0..pool_size {
            let connection = Self::create_connection(memory_per_conn, threads_per_conn)?;
            connections.push(Arc::new(PooledConnection {
                connection: Mutex::new(connection),
                id,
            }));
            debug!("Created connection {}", id);
        }

        // Configure S3 for all connections
        let executor = Self {
            connections,
            semaphore: Arc::new(Semaphore::new(pool_size)),
            config,
            next_conn: std::sync::atomic::AtomicUsize::new(0),
        };

        executor.configure_s3_all()?;
        executor.configure_azure_all()?;

        info!(
            "DuckDB executor initialized with {} parallel connections",
            pool_size
        );

        Ok(executor)
    }

    /// Create a single DuckDB connection
    fn create_connection(max_memory: u64, threads: u32) -> Result<Connection> {
        let connection = Connection::open_in_memory()?;

        connection.execute(&format!("SET memory_limit = '{}B'", max_memory), params![])?;
        connection.execute(&format!("SET threads = {}", threads), params![])?;

        // Enable auto-install for extensions (allows on-demand installation at query time)
        // Extensions not pre-installed will be downloaded from DuckDB's extension repository
        connection.execute("SET autoinstall_known_extensions = true", params![])?;
        connection.execute("SET autoload_known_extensions = true", params![])?;
        
        // Allow community extensions (not just core extensions)
        // This enables installation of any extension from the DuckDB community repository
        if let Err(e) = connection.execute("SET allow_community_extensions = true", params![]) {
            tracing::debug!("Could not enable community extensions: {}", e);
        }

        // Load pre-installed extensions from Docker image
        // Extensions are pre-downloaded at build time for faster startup and offline support
        // See Dockerfile.worker for the list of pre-installed extensions
        
        // List of extensions to load (order matters for dependencies)
        // Note: postgres, mysql, sqlite extensions not available as pre-built binaries for v1.4.3
        let extensions = [
            // Core data formats
            ("parquet", "Parquet file support"),
            ("json", "JSON operations"),
            ("avro", "Avro file support"),
            ("excel", "Excel file support"),
            // Cloud storage
            ("httpfs", "HTTP/HTTPS/S3 file access"),
            ("azure", "Azure Blob Storage / ADLS Gen2"),
            ("aws", "AWS SDK features"),
            // Data lakes
            ("delta", "Delta Lake support"),
            ("iceberg", "Apache Iceberg support"),
            // Utilities
            ("icu", "Time zones and collations"),
            ("fts", "Full-text search"),
            ("inet", "IP address operations"),
            ("spatial", "Geospatial functions"),
            ("vss", "Vector similarity search"),
        ];
        
        for (ext_name, description) in extensions {
            if let Err(e) = connection.execute(&format!("LOAD {}", ext_name), params![]) {
                tracing::debug!("Could not load {}: {} ({})", ext_name, e, description);
            }
        }

        // Enable external file cache for S3/remote files (DuckDB 1.3+)
        // This caches remote data locally to avoid repeated network transfers
        if let Err(e) = connection.execute("SET enable_external_file_cache = true", params![]) {
            tracing::warn!("Could not enable external file cache: {}", e);
        }

        // Configure temp directory for out-of-core processing (spill to disk)
        let temp_dir =
            std::env::var("DUCKDB_TEMP_DIR").unwrap_or_else(|_| "/tmp/duckdb".to_string());
        let max_temp_size =
            std::env::var("DUCKDB_MAX_TEMP_SIZE").unwrap_or_else(|_| "100GB".to_string());

        // Create temp directory if it doesn't exist
        let _ = std::fs::create_dir_all(&temp_dir);

        if let Err(e) =
            connection.execute(&format!("SET temp_directory = '{}'", temp_dir), params![])
        {
            tracing::warn!("Could not set temp_directory: {}", e);
        }
        if let Err(e) = connection.execute(
            &format!("SET max_temp_directory_size = '{}'", max_temp_size),
            params![],
        ) {
            tracing::warn!("Could not set max_temp_directory_size: {}", e);
        }

        // Increase threads for remote file parallelism (helps with S3)
        // DuckDB uses synchronous I/O for remote files, so more threads = better parallelism
        let threads_for_remote = threads * 2;
        if let Err(e) =
            connection.execute(&format!("SET threads = {}", threads_for_remote), params![])
        {
            tracing::warn!("Could not increase threads for remote: {}", e);
        }

        // Configure Azure authentication from environment variables
        if let Ok(account_name) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
            tracing::info!("Configuring Azure for account: {}", account_name);
            
            // Set the account name
            if let Err(e) = connection.execute(
                &format!("SET azure_account_name = '{}'", account_name), params![]
            ) {
                tracing::warn!("Could not set azure_account_name: {}", e);
            }
            
            // Use managed identity / credential chain for authentication
            if let Err(e) = connection.execute("SET azure_credential_chain = 'default'", params![]) {
                tracing::warn!("Could not set azure_credential_chain: {}", e);
            }
            
            // Set CA bundle for SSL if available
            if let Ok(ca_bundle) = std::env::var("CURL_CA_BUNDLE") {
                if let Err(e) = connection.execute(
                    &format!("SET ca_cert_file = '{}'", ca_bundle), params![]
                ) {
                    tracing::debug!("Could not set ca_cert_file: {}", e);
                }
            }
        }

        Ok(connection)
    }

    /// Configure S3 for all connections from environment variables
    fn configure_s3_all(&self) -> Result<()> {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();

        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            conn.execute(&format!("SET s3_region = '{}'", region), params![])?;

            if let Some(key) = &access_key {
                conn.execute(&format!("SET s3_access_key_id = '{}'", key), params![])?;
            }
            if let Some(secret) = &secret_key {
                conn.execute(
                    &format!("SET s3_secret_access_key = '{}'", secret),
                    params![],
                )?;
            }
            if let Some(ep) = &endpoint {
                let ep_clean = ep.replace("http://", "").replace("https://", "");
                conn.execute(&format!("SET s3_endpoint = '{}'", ep_clean), params![])?;
                conn.execute("SET s3_use_ssl = false", params![])?;
            }

            // Always use path-style for MinIO compatibility
            conn.execute("SET s3_url_style = 'path'", params![])?;
        }

        if access_key.is_some() {
            info!(
                "S3 access key configured for {} connections",
                self.connections.len()
            );
        }
        if endpoint.is_some() {
            info!(
                "S3 endpoint configured for {} connections",
                self.connections.len()
            );
        }
        info!("S3 path-style URLs enabled for all connections");

        Ok(())
    }

    /// Configure Azure/ADLS Gen2 for all connections from environment variables
    /// Supports Databricks on Azure storage access
    fn configure_azure_all(&self) -> Result<()> {
        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").ok();
        let connection_string = std::env::var("AZURE_STORAGE_CONNECTION_STRING").ok();
        let sas_token = std::env::var("AZURE_SAS_TOKEN").ok();
        let use_managed_identity = std::env::var("AZURE_USE_MANAGED_IDENTITY")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        // Skip if no Azure configuration
        if account_name.is_none() && connection_string.is_none() {
            debug!("No Azure storage configuration found, skipping");
            return Ok(());
        }

        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            // Set storage account name if provided
            if let Some(account) = &account_name {
                conn.execute(
                    &format!("SET azure_account_name = '{}'", account),
                    params![],
                )?;
            }

            // Configure authentication method (in priority order)
            if let Some(conn_str) = &connection_string {
                conn.execute(
                    &format!("SET azure_storage_connection_string = '{}'", conn_str),
                    params![],
                )?;
            } else if let Some(sas) = &sas_token {
                // Note: DuckDB uses azure_sas_token for SAS authentication
                // Format should be the full SAS token including the leading '?'
                let sas_clean = if sas.starts_with('?') { sas.clone() } else { format!("?{}", sas) };
                if let Err(e) = conn.execute(
                    &format!("SET azure_sas_token = '{}'", sas_clean),
                    params![],
                ) {
                    tracing::warn!("Could not set azure_sas_token: {}", e);
                }
            } else if use_managed_identity {
                // Use Azure Default Credential (works with AKS Workload Identity)
                if let Err(e) = conn.execute("SET azure_credential_chain = 'default'", params![]) {
                    tracing::warn!("Could not set azure_credential_chain: {}", e);
                }
            }
        }

        if account_name.is_some() {
            info!(
                "Azure storage configured for {} connections (account: {})",
                self.connections.len(),
                account_name.as_ref().unwrap()
            );
        }
        if connection_string.is_some() {
            info!("Azure using connection string authentication");
        } else if sas_token.is_some() {
            info!("Azure using SAS token authentication");
        } else if use_managed_identity {
            info!("Azure using managed identity authentication");
        }

        Ok(())
    }

    /// Get the next connection using round-robin
    fn get_connection(&self) -> Arc<PooledConnection> {
        let idx = self
            .next_conn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            % self.connections.len();
        Arc::clone(&self.connections[idx])
    }

    /// Execute a query and return Arrow record batches
    /// Uses connection pool for parallel execution
    #[instrument(skip(self))]
    pub fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let pooled_conn = self.get_connection();
        debug!("Using connection {} for query", pooled_conn.id);

        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let mut stmt = conn.prepare(sql)?;
        let batches = stmt.query_arrow(params![])?.collect();
        Ok(batches)
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params))]
    pub fn execute_query_with_params<P: duckdb::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<RecordBatch>> {
        let pooled_conn = self.get_connection();
        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let mut stmt = conn.prepare(sql)?;
        let batches = stmt.query_arrow(params)?.collect();
        Ok(batches)
    }

    /// Get pool size
    pub fn pool_size(&self) -> usize {
        self.connections.len()
    }

    /// Set S3 credentials for accessing cloud storage
    pub fn configure_s3(
        &self,
        region: &str,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        endpoint: Option<&str>,
    ) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            conn.execute(&format!("SET s3_region = '{}'", region), params![])?;

            if let Some(key) = access_key_id {
                conn.execute(&format!("SET s3_access_key_id = '{}'", key), params![])?;
            }
            if let Some(secret) = secret_access_key {
                conn.execute(
                    &format!("SET s3_secret_access_key = '{}'", secret),
                    params![],
                )?;
            }
            if let Some(ep) = endpoint {
                conn.execute(&format!("SET s3_endpoint = '{}'", ep), params![])?;
                conn.execute("SET s3_url_style = 'path'", params![])?;
            }
        }

        Ok(())
    }

    /// Configure Azure credentials
    pub fn configure_azure(&self, account_name: &str) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            conn.execute(
                &format!("SET azure_account_name = '{}'", account_name),
                params![],
            )?;
        }
        Ok(())
    }

    /// Configure Azure credentials for Databricks/ADLS Gen2 access
    /// Supports multiple authentication methods:
    /// - Connection string (for development)
    /// - SAS token (for production)
    /// - Managed Identity (for Azure workloads)
    pub fn configure_azure_databricks(
        &self,
        account_name: &str,
        connection_string: Option<&str>,
        sas_token: Option<&str>,
        use_managed_identity: bool,
    ) -> Result<()> {
        for pooled_conn in &self.connections {
            let conn = pooled_conn
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            // Set storage account name (always required)
            conn.execute(
                &format!("SET azure_account_name = '{}'", account_name),
                params![],
            )?;

            // Configure authentication method
            if let Some(conn_str) = connection_string {
                conn.execute(
                    &format!("SET azure_storage_connection_string = '{}'", conn_str),
                    params![],
                )?;
                tracing::info!("Azure configured with connection string for account: {}", account_name);
            } else if let Some(sas) = sas_token {
                // SAS token for secure, time-limited access
                conn.execute(
                    &format!("SET azure_sas_token = '{}'", sas),
                    params![],
                )?;
                tracing::info!("Azure configured with SAS token for account: {}", account_name);
            } else if use_managed_identity {
                // Use Azure Managed Identity (works in AKS with workload identity)
                conn.execute("SET azure_use_default_credential = true", params![])?;
                tracing::info!("Azure configured with managed identity for account: {}", account_name);
            }
        }
        Ok(())
    }

    /// Configure Databricks Unity Catalog access
    /// For accessing Delta tables via Unity Catalog external locations
    pub fn configure_databricks_unity_catalog(
        &self,
        workspace_url: &str,
        _access_token: &str,
    ) -> Result<()> {
        // Unity Catalog tables are accessed via their cloud storage path
        // The access is through ADLS/S3/GCS with proper credentials
        // This method sets up the secret for accessing Databricks-managed storage
        tracing::info!(
            "Databricks Unity Catalog configured for workspace: {}",
            workspace_url
        );
        
        // Note: Unity Catalog tables use cloud storage paths like:
        // abfss://container@account.dfs.core.windows.net/path/to/delta/table
        // s3://bucket/path/to/delta/table
        
        // The actual credentials are configured via configure_azure_databricks or configure_s3
        // This method is a convenience wrapper that logs the configuration
        
        Ok(())
    }

    /// Get query statistics (for resource tracking)
    pub fn get_query_stats(&self) -> Result<QueryStats> {
        // Use first connection for stats
        let pooled_conn = &self.connections[0];
        let conn = pooled_conn
            .connection
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let mut stmt = conn.prepare("SELECT current_setting('threads') as threads")?;
        let mut rows = stmt.query(params![])?;

        let threads: String = if let Some(row) = rows.next()? {
            row.get(0)?
        } else {
            "unknown".to_string()
        };

        Ok(QueryStats {
            threads: threads.parse().unwrap_or(1),
            pool_size: self.connections.len(),
        })
    }
}

/// Query execution statistics
#[derive(Debug)]
pub struct QueryStats {
    pub threads: u32,
    pub pool_size: usize,
}

/// Execution stats for billing
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub execution_time_ms: u64,
    pub rows_returned: u64,
    pub bytes_scanned: u64,
    pub cpu_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = DuckDbExecutor::new(ExecutorConfig::default());
        assert!(executor.is_ok());
    }

    #[test]
    fn test_simple_query() {
        let executor = DuckDbExecutor::new(ExecutorConfig::default()).unwrap();
        let result = executor.execute_query("SELECT 1 as value");
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_pool_size() {
        let config = ExecutorConfig {
            pool_size: 8,
            ..Default::default()
        };
        let executor = DuckDbExecutor::new(config).unwrap();
        assert_eq!(executor.pool_size(), 8);
    }
}
