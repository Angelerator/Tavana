//! Worker gRPC client for forwarding queries to DuckDB workers
//! 
//! Supports TRUE STREAMING with Arrow IPC for high-performance data transfer.
//! Arrow 56 matches DuckDB's bundled version for zero-copy deserialization.

use arrow_ipc::reader::FileReader;
use std::io::Cursor;
use std::sync::Arc;
use tavana_common::proto;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Streaming batch types for true row-by-row streaming
pub enum StreamingBatch {
    Metadata {
        columns: Vec<String>,
        column_types: Vec<String>,
    },
    /// Rows as string values for PG wire protocol
    Rows(Vec<Vec<String>>),
    Error(String),
}

/// Stream wrapper for async iteration
pub struct StreamingResult {
    receiver: tokio::sync::mpsc::Receiver<Result<StreamingBatch, anyhow::Error>>,
}

impl StreamingResult {
    pub async fn next(&mut self) -> Option<Result<StreamingBatch, anyhow::Error>> {
        self.receiver.recv().await
    }
}

/// Client for communicating with DuckDB worker service
pub struct WorkerClient {
    client: Arc<RwLock<Option<proto::query_service_client::QueryServiceClient<Channel>>>>,
    worker_addr: String,
}

impl WorkerClient {
    pub fn new(worker_addr: String) -> Self {
        Self {
            client: Arc::new(RwLock::new(None)),
            worker_addr,
        }
    }

    /// Ensure we have a connected client
    async fn get_client(
        &self,
    ) -> Result<proto::query_service_client::QueryServiceClient<Channel>, anyhow::Error> {
        // Check if we have a cached client
        {
            let reader = self.client.read().await;
            if let Some(client) = reader.as_ref() {
                return Ok(client.clone());
            }
        }

        // Connect with large message size for big result sets
        info!("Connecting to worker at {}", self.worker_addr);
        const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

        let channel = Channel::from_shared(self.worker_addr.clone())?
            .timeout(std::time::Duration::from_secs(1800)) // 30 minutes
            .connect_timeout(std::time::Duration::from_secs(30))
            // TCP keepalive - OS-level connection health check
            .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
            // HTTP/2 keepalive - application-level PING frames for faster detection
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await?;

        let client = proto::query_service_client::QueryServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);

        // Cache the client
        {
            let mut writer = self.client.write().await;
            *writer = Some(client.clone());
        }

        Ok(client)
    }

    /// Execute a query and return results as column names and rows
    pub async fn execute_query(
        &self,
        sql: &str,
        user_id: &str,
    ) -> Result<QueryResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        let query_id = Uuid::new_v4().to_string();
        debug!("Executing query {} for user {}", query_id, user_id);

        let request = proto::ExecuteQueryRequest {
            query_id: query_id.clone(),
            sql: sql.to_string(),
            user: Some(proto::UserIdentity {
                user_id: user_id.to_string(),
                tenant_id: "default".to_string(),
                scopes: vec!["query:execute".to_string()],
                claims: Default::default(),
            }),
            options: Some(proto::QueryOptions {
                timeout_seconds: 300,
                max_rows: 0, // 0 = unlimited rows (streaming)
                max_bytes: 0,
                enable_profiling: false,
                session_params: Default::default(),
            }),
            allocated_resources: None,
        };

        let response = client.execute_query(request).await?;
        let mut stream = response.into_inner();

        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut total_rows: u64 = 0;

        while let Some(batch) = stream.message().await? {
            match batch.result {
                Some(proto::query_result_batch::Result::Metadata(meta)) => {
                    debug!("Received metadata: {} columns", meta.columns.len());
                    columns = meta
                        .columns
                        .iter()
                        .zip(meta.column_types.iter())
                        .map(|(name, type_name)| ColumnInfo {
                            name: name.clone(),
                            type_name: type_name.clone(),
                        })
                        .collect();
                    total_rows = meta.total_rows;
                }
                Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                    debug!("Received batch with {} rows", batch.row_count);
                    // Decode Arrow IPC data (10-100x faster than JSON!)
                    if !batch.data.is_empty() {
                        match deserialize_arrow_ipc(&batch.data) {
                            Ok(record_batches) => {
                                for rb in record_batches {
                                    let batch_rows = arrow_batch_to_string_rows(&rb);
                                    rows.extend(batch_rows);
                                }
                            }
                            Err(e) => {
                                warn!("Arrow IPC decode failed: {}, trying JSON fallback", e);
                                // Fallback to JSON for backwards compatibility
                                if let Ok(batch_rows) =
                                    serde_json::from_slice::<Vec<Vec<String>>>(&batch.data)
                                {
                                    rows.extend(batch_rows);
                                }
                            }
                        }
                    }
                }
                Some(proto::query_result_batch::Result::Profile(profile)) => {
                    debug!(
                        "Query completed: {} ms, {} rows returned",
                        profile.execution_time_ms, profile.rows_returned
                    );
                }
                Some(proto::query_result_batch::Result::Error(err)) => {
                    error!("Query error: {} - {}", err.code, err.message);
                    return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
                }
                None => {}
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            total_rows,
        })
    }

    /// Execute a query with TRUE STREAMING - returns results as they arrive
    /// This is the preferred method for large result sets as it never buffers everything
    pub async fn execute_query_streaming(
        &self,
        sql: &str,
        user_id: &str,
    ) -> Result<StreamingResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        let query_id = Uuid::new_v4().to_string();
        debug!(
            "Executing streaming query {} for user {}",
            query_id, user_id
        );

        let request = proto::ExecuteQueryRequest {
            query_id: query_id.clone(),
            sql: sql.to_string(),
            user: Some(proto::UserIdentity {
                user_id: user_id.to_string(),
                tenant_id: "default".to_string(),
                scopes: vec!["query:execute".to_string()],
                claims: Default::default(),
            }),
            options: Some(proto::QueryOptions {
                timeout_seconds: 600, // 10 minutes for large queries
                max_rows: 0,          // 0 = unlimited rows (streaming)
                max_bytes: 0,
                enable_profiling: false,
                session_params: Default::default(),
            }),
            allocated_resources: None,
        };

        let response = client.execute_query(request).await?;
        let mut stream = response.into_inner();

        // Create a channel for streaming results
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // Spawn a task to read from gRPC stream and forward to channel
        // TRUE STREAMING with Arrow IPC: 10-100x faster data transfer
        tokio::spawn(async move {
            while let Ok(Some(batch)) = stream.message().await {
                let result = match batch.result {
                    Some(proto::query_result_batch::Result::Metadata(meta)) => {
                        Ok(StreamingBatch::Metadata {
                            columns: meta.columns,
                            column_types: meta.column_types,
                        })
                    }
                    Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                        if !batch.data.is_empty() {
                            // Try JSON first (worker now sends JSON for reliability)
                            // Arrow IPC has issues with DuckDB's bundled Arrow vs standalone crate
                            match serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                                Ok(rows) => Ok(StreamingBatch::Rows(rows)),
                                Err(_) => {
                                    // Fallback to Arrow IPC for backwards compatibility
                                    match deserialize_arrow_ipc(&batch.data) {
                                        Ok(record_batches) => {
                                            let mut all_rows = Vec::new();
                                            for rb in record_batches {
                                                let rows = arrow_batch_to_string_rows(&rb);
                                                all_rows.extend(rows);
                                            }
                                            Ok(StreamingBatch::Rows(all_rows))
                                        }
                                        Err(e) => Err(anyhow::anyhow!("Failed to decode batch: {}", e)),
                                    }
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                    Some(proto::query_result_batch::Result::Error(err)) => Ok(
                        StreamingBatch::Error(format!("{}: {}", err.code, err.message)),
                    ),
                    _ => continue,
                };

                if tx.send(result).await.is_err() {
                    // Receiver dropped, stop streaming
                    break;
                }
            }
        });

        Ok(StreamingResult { receiver: rx })
    }

    // ============= Cursor Operations for True Streaming =============

    /// Declare a cursor on the worker - query is executed and iterator is held
    /// Returns cursor_id, worker_id (for affinity), and column metadata
    pub async fn declare_cursor(
        &self,
        cursor_id: &str,
        sql: &str,
        user_id: &str,
    ) -> Result<DeclareCursorResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        info!(cursor_id = %cursor_id, sql = %&sql[..sql.len().min(80)], "Declaring cursor on worker");

        let request = proto::DeclareCursorRequest {
            cursor_id: cursor_id.to_string(),
            sql: sql.to_string(),
            user: Some(proto::UserIdentity {
                user_id: user_id.to_string(),
                tenant_id: "default".to_string(),
                scopes: vec!["query:execute".to_string()],
                claims: Default::default(),
            }),
            options: Some(proto::QueryOptions {
                timeout_seconds: 600, // 10 minutes
                max_rows: 0,          // Unlimited for cursors
                max_bytes: 0,
                enable_profiling: false,
                session_params: Default::default(),
            }),
        };

        let response = client.declare_cursor(request).await?;
        let resp = response.into_inner();

        if !resp.success {
            return Err(anyhow::anyhow!("Failed to declare cursor: {}", resp.error_message));
        }

        let columns: Vec<ColumnInfo> = resp.columns.iter()
            .zip(resp.column_types.iter())
            .map(|(name, type_name)| ColumnInfo {
                name: name.clone(),
                type_name: type_name.clone(),
            })
            .collect();

        info!(
            cursor_id = %resp.cursor_id,
            worker_id = %resp.worker_id,
            columns = columns.len(),
            "Cursor declared successfully"
        );

        Ok(DeclareCursorResult {
            cursor_id: resp.cursor_id,
            worker_id: resp.worker_id,
            columns,
        })
    }

    /// Fetch rows from a cursor on the worker (true streaming - no re-scanning)
    pub async fn fetch_cursor(
        &self,
        cursor_id: &str,
        max_rows: usize,
    ) -> Result<FetchCursorResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        debug!(cursor_id = %cursor_id, max_rows = max_rows, "Fetching from cursor");

        let request = proto::FetchCursorRequest {
            cursor_id: cursor_id.to_string(),
            max_rows: max_rows as u64,
        };

        let response = client.fetch_cursor(request).await?;
        let mut stream = response.into_inner();

        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();

        while let Some(batch) = stream.message().await? {
            match batch.result {
                Some(proto::query_result_batch::Result::Metadata(meta)) => {
                    columns = meta.columns.iter()
                        .zip(meta.column_types.iter())
                        .map(|(name, type_name)| ColumnInfo {
                            name: name.clone(),
                            type_name: type_name.clone(),
                        })
                        .collect();
                }
                Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                    if !batch.data.is_empty() {
                        if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                            rows.extend(batch_rows);
                        }
                    }
                }
                Some(proto::query_result_batch::Result::Error(err)) => {
                    return Err(anyhow::anyhow!("{}: {}", err.code, err.message));
                }
                _ => {}
            }
        }

        let row_count = rows.len();
        debug!(cursor_id = %cursor_id, rows_fetched = row_count, "Fetch complete");

        Ok(FetchCursorResult {
            columns,
            rows,
            row_count,
        })
    }

    /// Close a cursor on the worker and release resources
    pub async fn close_cursor(&self, cursor_id: &str) -> Result<bool, anyhow::Error> {
        let mut client = self.get_client().await?;

        debug!(cursor_id = %cursor_id, "Closing cursor");

        let request = proto::CloseCursorRequest {
            cursor_id: cursor_id.to_string(),
        };

        let response = client.close_cursor(request).await?;
        let resp = response.into_inner();

        info!(cursor_id = %cursor_id, success = resp.success, "Cursor closed");

        Ok(resp.success)
    }

    /// Health check to verify worker is alive
    /// Returns true if worker responds to health check, false otherwise
    pub async fn health_check(&self) -> bool {
        match self.get_client().await {
            Ok(mut client) => {
                let request = proto::HealthCheckRequest {};
                match client.health_check(request).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        // ServiceStatus::Healthy = 1
                        resp.status == proto::ServiceStatus::Healthy as i32
                    }
                    Err(e) => {
                        debug!("Worker health check failed: {}", e);
                        false
                    }
                }
            }
            Err(e) => {
                debug!("Failed to connect to worker for health check: {}", e);
                false
            }
        }
    }

    /// Check if a specific cursor is still alive on the worker
    /// Returns true if cursor exists, false if it doesn't or worker is unreachable
    pub async fn cursor_exists(&self, cursor_id: &str) -> bool {
        // Try a minimal fetch (0 rows) to check if cursor exists
        match self.fetch_cursor(cursor_id, 0).await {
            Ok(_) => true,
            Err(e) => {
                let err_msg = e.to_string();
                // If the error is about cursor not found, it definitely doesn't exist
                if err_msg.contains("CURSOR_NOT_FOUND") || err_msg.contains("not found") {
                    debug!(cursor_id = %cursor_id, "Cursor no longer exists on worker");
                    false
                } else {
                    // Other errors might be transient - assume cursor exists
                    debug!(cursor_id = %cursor_id, error = %e, "Cursor check failed with error (assuming exists)");
                    true
                }
            }
        }
    }
}

// ============= Arrow IPC Deserialization Utilities =============

/// Deserialize Arrow IPC data to RecordBatches
/// This is the inverse of the worker's serialize_batch_to_arrow_ipc
fn deserialize_arrow_ipc(data: &[u8]) -> Result<Vec<arrow_array::RecordBatch>, anyhow::Error> {
    let cursor = Cursor::new(data);
    let reader = FileReader::try_new(cursor, None)?;
    
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    
    Ok(batches)
}

/// Convert an Arrow RecordBatch to string rows for PG wire protocol
/// Uses Arrow's built-in formatters for type-safe conversion
fn arrow_batch_to_string_rows(batch: &arrow_array::RecordBatch) -> Vec<Vec<String>> {
    use arrow_array::Array;
    use arrow::util::display::ArrayFormatter;
    
    let mut rows = Vec::with_capacity(batch.num_rows());
    
    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let value = if col.is_null(row_idx) {
                "NULL".to_string()
            } else {
                match ArrayFormatter::try_new(col.as_ref(), &Default::default()) {
                    Ok(formatter) => formatter.value(row_idx).to_string(),
                    Err(_) => format!("<{:?}>", col.data_type()),
                }
            };
            row.push(value);
        }
        rows.push(row);
    }
    
    rows
}

/// Result of declaring a cursor
#[derive(Debug)]
pub struct DeclareCursorResult {
    pub cursor_id: String,
    pub worker_id: String,
    pub columns: Vec<ColumnInfo>,
}

/// Result of fetching from a cursor
#[derive(Debug)]
pub struct FetchCursorResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
}

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: u64,
}
