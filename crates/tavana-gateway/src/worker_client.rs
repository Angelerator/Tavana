//! Worker gRPC client for forwarding queries to DuckDB workers
//! 
//! Supports TRUE STREAMING with optimized binary format for high-performance data transfer.
//! Uses a compact binary protocol that's 2-3x smaller and 5-10x faster than JSON.
//!
//! ## Cursor Affinity Routing
//! 
//! For server-side cursors, each cursor is bound to a specific worker.
//! The `WorkerClientPool` manages connections to multiple workers and routes
//! cursor operations (FETCH, CLOSE) to the correct worker based on the cursor's
//! `worker_addr`.

use arrow_ipc::reader::{FileReader, StreamReader};
use dashmap::DashMap;
use std::io::Cursor;
use std::sync::Arc;
use tavana_common::proto;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ============= Optimized Binary Deserialization =============

/// Deserialize rows from Tavana's optimized binary format
/// 
/// Format:
/// - Magic bytes: "TVNA" (4 bytes)
/// - Row count: u32 (4 bytes)
/// - Column count: u16 (2 bytes)
/// - For each row:
///   - For each column:
///     - Value length: i32 (4 bytes, -1 for NULL)
///     - Value bytes: UTF-8 string (if length >= 0)
fn deserialize_binary_batch(data: &[u8]) -> Result<Vec<Vec<String>>, anyhow::Error> {
    if data.len() < 10 {
        return Err(anyhow::anyhow!("Binary batch too short: {} bytes", data.len()));
    }
    
    // Check magic bytes
    if &data[0..4] != b"TVNA" {
        return Err(anyhow::anyhow!("Invalid magic bytes"));
    }
    
    let mut offset = 4;
    
    // Read header
    let row_count = u32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]) as usize;
    offset += 4;
    
    let col_count = u16::from_be_bytes([data[offset], data[offset+1]]) as usize;
    offset += 2;
    
    let mut rows = Vec::with_capacity(row_count);
    
    for _ in 0..row_count {
        let mut row = Vec::with_capacity(col_count);
        
        for _ in 0..col_count {
            if offset + 4 > data.len() {
                return Err(anyhow::anyhow!("Unexpected end of data"));
            }
            
            let len = i32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
            offset += 4;
            
            if len < 0 {
                // NULL value
                row.push("NULL".to_string());
            } else {
                let len = len as usize;
                if offset + len > data.len() {
                    return Err(anyhow::anyhow!("Unexpected end of data for value"));
                }
                let value = String::from_utf8_lossy(&data[offset..offset + len]).into_owned();
                offset += len;
                row.push(value);
            }
        }
        
        rows.push(row);
    }
    
    Ok(rows)
}

/// Default query timeout in seconds - aligned across gateway and worker
/// Can be overridden via TAVANA_QUERY_TIMEOUT_SECS environment variable
fn query_timeout_secs() -> u32 {
    std::env::var("TAVANA_QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600) // 10 minutes default
}

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

/// Native Arrow streaming batch types for Flight SQL (ZERO-COPY)
/// 
/// This preserves Arrow RecordBatches without string conversion,
/// enabling true zero-copy data transfer to Flight SQL clients.
pub enum ArrowStreamingBatch {
    /// Schema and type information
    Schema(arrow_schema::SchemaRef),
    /// Native Arrow RecordBatch (zero-copy from worker)
    RecordBatch(arrow_array::RecordBatch),
    /// Error message
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

/// Stream wrapper for native Arrow async iteration (Flight SQL)
pub struct ArrowStreamingResult {
    receiver: tokio::sync::mpsc::Receiver<Result<ArrowStreamingBatch, anyhow::Error>>,
}

impl ArrowStreamingResult {
    pub async fn next(&mut self) -> Option<Result<ArrowStreamingBatch, anyhow::Error>> {
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
                timeout_seconds: query_timeout_secs(),
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
                    if !batch.data.is_empty() {
                        // Priority 1: Try Arrow IPC format (new zero-copy format)
                        if let Ok(record_batches) = deserialize_arrow_ipc(&batch.data) {
                            for rb in record_batches {
                                let batch_rows = arrow_batch_to_string_rows(&rb);
                                rows.extend(batch_rows);
                            }
                        }
                        // Priority 2: Try TVNA binary format (legacy optimized format)
                        else if batch.data.starts_with(b"TVNA") {
                            match deserialize_binary_batch(&batch.data) {
                                Ok(batch_rows) => {
                                    rows.extend(batch_rows);
                                }
                                Err(e) => {
                                    error!("TVNA decode failed: {}", e);
                                }
                            }
                        }
                        // Priority 3: Fallback to JSON (oldest format)
                        else {
                            match serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                                Ok(batch_rows) => {
                                    rows.extend(batch_rows);
                                }
                                Err(json_err) => {
                                    error!("All decode methods failed: {}", json_err);
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
                timeout_seconds: query_timeout_secs(),
                max_rows: 0,          // 0 = unlimited rows (streaming)
                max_bytes: 0,
                enable_profiling: false,
                session_params: Default::default(),
            }),
            allocated_resources: None,
        };

        let response = client.execute_query(request).await?;
        let mut stream = response.into_inner();

        // Create a SMALL bounded channel for streaming results
        // This is critical for backpressure: when the client can't consume data fast enough,
        // the channel fills up, which causes tx.send() to block, which back-pressures the
        // gRPC stream reader, which back-pressures the worker via gRPC flow control.
        //
        // Channel size of 4 means at most 4 batches buffered between gRPC and client socket.
        // Each batch is typically 100-1000 rows, so this limits buffering to 400-4000 rows.
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Spawn a task to read from gRPC stream and forward to channel
        // TRUE STREAMING with Arrow IPC: 10-100x faster data transfer
        // NOTE: tx.send() will block when channel is full, providing backpressure
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
                            // Priority 1: Try Arrow IPC format (new zero-copy format)
                            // Arrow IPC preserves native types for Flight SQL passthrough
                            if let Ok(record_batches) = deserialize_arrow_ipc(&batch.data) {
                                let mut all_rows = Vec::new();
                                for rb in record_batches {
                                    let rows = arrow_batch_to_string_rows(&rb);
                                    all_rows.extend(rows);
                                }
                                Ok(StreamingBatch::Rows(all_rows))
                            }
                            // Priority 2: Try TVNA binary format (legacy optimized format)
                            else if batch.data.starts_with(b"TVNA") {
                                match deserialize_binary_batch(&batch.data) {
                                    Ok(rows) => Ok(StreamingBatch::Rows(rows)),
                                    Err(e) => Err(anyhow::anyhow!("Failed to decode TVNA batch: {}", e)),
                                }
                            }
                            // Priority 3: Fallback to JSON (oldest format)
                            else {
                                match serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                                    Ok(rows) => Ok(StreamingBatch::Rows(rows)),
                                    Err(e) => Err(anyhow::anyhow!("Failed to decode batch (tried Arrow IPC, TVNA, JSON): {}", e)),
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

    /// Execute a query with NATIVE ARROW STREAMING for Flight SQL (ZERO-COPY)
    /// 
    /// This method returns Arrow RecordBatches directly without string conversion,
    /// enabling true zero-copy data transfer to ADBC/Flight SQL clients.
    /// 
    /// Use this for:
    /// - Arrow Flight SQL endpoints
    /// - ADBC clients (Python, Go, Java)
    /// - Any client that can consume Arrow format directly
    pub async fn execute_query_arrow_streaming(
        &self,
        sql: &str,
        user_id: &str,
    ) -> Result<ArrowStreamingResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        let query_id = Uuid::new_v4().to_string();
        debug!(
            "Executing Arrow streaming query {} for user {}",
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
                timeout_seconds: query_timeout_secs(),
                max_rows: 0,          // 0 = unlimited rows (streaming)
                max_bytes: 0,
                enable_profiling: false,
                session_params: Default::default(),
            }),
            allocated_resources: None,
        };

        let response = client.execute_query(request).await?;
        let mut stream = response.into_inner();

        // Channel for streaming Arrow batches (small buffer for backpressure)
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Spawn a task to read from gRPC stream and forward Arrow batches
        tokio::spawn(async move {
            let mut schema_sent = false;
            
            while let Ok(Some(batch)) = stream.message().await {
                match batch.result {
                    Some(proto::query_result_batch::Result::Metadata(meta)) => {
                        // Build schema from metadata for clients that need it
                        if !schema_sent {
                            let fields: Vec<arrow_schema::Field> = meta.columns.iter()
                                .zip(meta.column_types.iter())
                                .map(|(name, type_name)| {
                                    let data_type = duckdb_type_to_arrow(type_name);
                                    arrow_schema::Field::new(name, data_type, true)
                                })
                                .collect();
                            let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
                            let _ = tx.send(Ok(ArrowStreamingBatch::Schema(schema))).await;
                            schema_sent = true;
                        }
                    }
                    Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                        if !batch.data.is_empty() {
                            // Deserialize Arrow IPC directly to RecordBatches (ZERO-COPY)
                            match deserialize_arrow_ipc_native(&batch.data) {
                                Ok((schema, record_batches)) => {
                                    // Send schema first if not already sent
                                    if !schema_sent {
                                        let _ = tx.send(Ok(ArrowStreamingBatch::Schema(schema))).await;
                                        schema_sent = true;
                                    }
                                    
                                    // Send each batch
                                    for rb in record_batches {
                                        if tx.send(Ok(ArrowStreamingBatch::RecordBatch(rb))).await.is_err() {
                                            return; // Receiver dropped
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Ok(ArrowStreamingBatch::Error(format!("Arrow IPC decode failed: {}", e)))).await;
                                }
                            }
                        }
                    }
                    Some(proto::query_result_batch::Result::Error(err)) => {
                        let _ = tx.send(Ok(ArrowStreamingBatch::Error(format!("{}: {}", err.code, err.message)))).await;
                        break;
                    }
                    _ => continue,
                }
            }
        });

        Ok(ArrowStreamingResult { receiver: rx })
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
                timeout_seconds: query_timeout_secs(),
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
                        // Priority 1: Try Arrow IPC format (new zero-copy format)
                        if let Ok(record_batches) = deserialize_arrow_ipc(&batch.data) {
                            for rb in record_batches {
                                let batch_rows = arrow_batch_to_string_rows(&rb);
                                rows.extend(batch_rows);
                            }
                        }
                        // Priority 2: Try TVNA binary format (legacy)
                        else if batch.data.starts_with(b"TVNA") {
                            if let Ok(batch_rows) = deserialize_binary_batch(&batch.data) {
                                rows.extend(batch_rows);
                            }
                        }
                        // Priority 3: Fallback to JSON
                        else if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
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

    /// Get the worker address this client connects to
    pub fn worker_addr(&self) -> &str {
        &self.worker_addr
    }
}

// ============= Worker Client Pool for Cursor Affinity Routing =============

/// Pool of WorkerClient connections for cursor affinity routing.
/// 
/// In a multi-worker deployment, each cursor is bound to a specific worker.
/// This pool manages connections to all workers and routes cursor operations
/// to the correct worker.
/// 
/// # Thread Safety
/// Uses DashMap for concurrent access without explicit locking.
pub struct WorkerClientPool {
    /// Map of worker address -> WorkerClient
    clients: DashMap<String, Arc<WorkerClient>>,
    /// Default worker address (for non-cursor operations)
    default_addr: String,
    /// Maximum number of cached worker connections
    max_pool_size: usize,
}

impl WorkerClientPool {
    /// Create a new pool with a default worker address
    pub fn new(default_addr: String) -> Self {
        let max_pool_size = std::env::var("TAVANA_WORKER_CLIENT_POOL_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let pool = Self {
            clients: DashMap::new(),
            default_addr: default_addr.clone(),
            max_pool_size,
        };

        // Pre-create the default client
        pool.clients.insert(
            default_addr.clone(),
            Arc::new(WorkerClient::new(default_addr)),
        );

        pool
    }

    /// Get the default worker client
    pub fn default_client(&self) -> Arc<WorkerClient> {
        self.clients
            .get(&self.default_addr)
            .map(|r| r.clone())
            .unwrap_or_else(|| {
                let client = Arc::new(WorkerClient::new(self.default_addr.clone()));
                self.clients.insert(self.default_addr.clone(), client.clone());
                client
            })
    }

    /// Get or create a worker client for a specific address
    /// 
    /// If the address is None or empty, returns the default client.
    /// This is the key method for cursor affinity routing.
    pub fn get_or_create(&self, addr: Option<&str>) -> Arc<WorkerClient> {
        let addr = match addr {
            Some(a) if !a.is_empty() => a,
            _ => return self.default_client(),
        };

        // Check if we already have this client
        if let Some(client) = self.clients.get(addr) {
            return client.clone();
        }

        // Evict old clients if pool is full
        if self.clients.len() >= self.max_pool_size {
            // Simple eviction: remove the first non-default entry
            // In production, consider LRU or time-based eviction
            let to_remove: Vec<String> = self.clients
                .iter()
                .filter(|r| r.key() != &self.default_addr)
                .take(1)
                .map(|r| r.key().clone())
                .collect();
            
            for key in to_remove {
                debug!(addr = %key, "Evicting worker client from pool");
                self.clients.remove(&key);
            }
        }

        // Create new client
        let client = Arc::new(WorkerClient::new(addr.to_string()));
        self.clients.insert(addr.to_string(), client.clone());
        
        info!(addr = %addr, pool_size = self.clients.len(), "Created new worker client for affinity routing");
        
        client
    }

    /// Get the number of cached worker connections
    pub fn pool_size(&self) -> usize {
        self.clients.len()
    }

    /// Remove a specific worker client from the pool
    /// Useful when a worker becomes unhealthy
    pub fn remove(&self, addr: &str) {
        if addr != self.default_addr {
            self.clients.remove(addr);
            debug!(addr = %addr, "Removed worker client from pool");
        }
    }

    /// Check health of all pooled workers
    pub async fn health_check_all(&self) -> Vec<(String, bool)> {
        let mut results = Vec::new();
        
        for entry in self.clients.iter() {
            let addr = entry.key().clone();
            let client = entry.value().clone();
            let healthy = client.health_check().await;
            results.push((addr, healthy));
        }
        
        results
    }
}

// ============= Arrow Type Mapping Utilities =============

/// Map DuckDB type names to Arrow DataTypes
/// 
/// This enables native type preservation when streaming Arrow data,
/// avoiding the overhead of string conversion.
fn duckdb_type_to_arrow(type_name: &str) -> arrow_schema::DataType {
    use arrow_schema::DataType;
    
    // Normalize type name
    let type_lower = type_name.to_lowercase();
    let type_clean = type_lower.trim();
    
    match type_clean {
        // Integers
        "tinyint" | "int8" | "int1" => DataType::Int8,
        "smallint" | "int16" | "int2" | "short" => DataType::Int16,
        "integer" | "int32" | "int4" | "int" | "signed" => DataType::Int32,
        "bigint" | "int64" | "long" | "int128" | "hugeint" => DataType::Int64,
        
        // Unsigned integers
        "utinyint" | "uint8" => DataType::UInt8,
        "usmallint" | "uint16" => DataType::UInt16,
        "uinteger" | "uint32" => DataType::UInt32,
        "ubigint" | "uint64" | "uhugeint" => DataType::UInt64,
        
        // Floats
        "float" | "float4" | "real" => DataType::Float32,
        "double" | "float8" | "double precision" => DataType::Float64,
        
        // Boolean
        "boolean" | "bool" | "logical" => DataType::Boolean,
        
        // Strings
        "varchar" | "text" | "string" | "char" | "bpchar" | "name" => DataType::Utf8,
        
        // Binary
        "blob" | "bytea" | "binary" | "varbinary" => DataType::Binary,
        
        // Date/Time
        "date" => DataType::Date32,
        "time" | "time without time zone" => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        "timestamp" | "datetime" | "timestamp without time zone" => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        }
        "timestamptz" | "timestamp with time zone" => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        }
        
        // Intervals
        "interval" => DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
        
        // UUID
        "uuid" => DataType::Utf8, // UUID represented as string
        
        // JSON
        "json" | "jsonb" => DataType::Utf8, // JSON as string
        
        // Decimal (default precision/scale)
        _ if type_clean.starts_with("decimal") || type_clean.starts_with("numeric") => {
            DataType::Decimal128(38, 10) // Default precision/scale
        }
        
        // List types
        _ if type_clean.ends_with("[]") => {
            let inner = &type_clean[..type_clean.len() - 2];
            let inner_type = duckdb_type_to_arrow(inner);
            DataType::List(std::sync::Arc::new(arrow_schema::Field::new("item", inner_type, true)))
        }
        
        // Default to Utf8 for unknown types
        _ => {
            debug!("Unknown DuckDB type '{}', defaulting to Utf8", type_name);
            DataType::Utf8
        }
    }
}

// ============= Arrow IPC Deserialization Utilities =============

/// Deserialize Arrow IPC data to RecordBatches (ZERO-COPY OPTIMIZED)
/// 
/// This is the inverse of the worker's serialize_batch_to_arrow_ipc.
/// Uses Arrow IPC stream format which preserves native types.
/// 
/// Tries StreamReader first (new format), falls back to FileReader (legacy).
fn deserialize_arrow_ipc(data: &[u8]) -> Result<Vec<arrow_array::RecordBatch>, anyhow::Error> {
    // Try StreamReader first (new Arrow IPC stream format)
    let cursor = Cursor::new(data);
    if let Ok(reader) = StreamReader::try_new(cursor, None) {
        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }
        return Ok(batches);
    }
    
    // Fallback to FileReader for backwards compatibility
    let cursor = Cursor::new(data);
    let reader = FileReader::try_new(cursor, None)?;
    
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    
    Ok(batches)
}

/// Deserialize Arrow IPC data directly to RecordBatches without string conversion
/// 
/// This is used by Flight SQL to preserve native Arrow types.
/// Returns the schema and batches for direct use.
pub fn deserialize_arrow_ipc_native(data: &[u8]) -> Result<(arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>), anyhow::Error> {
    // Try StreamReader first (new Arrow IPC stream format)
    let cursor = Cursor::new(data);
    if let Ok(reader) = StreamReader::try_new(cursor, None) {
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }
        return Ok((schema, batches));
    }
    
    // Fallback to FileReader
    let cursor = Cursor::new(data);
    let reader = FileReader::try_new(cursor, None)?;
    let schema = reader.schema();
    
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    
    Ok((schema, batches))
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
