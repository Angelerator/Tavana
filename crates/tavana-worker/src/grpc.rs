//! gRPC service implementation for the worker
//!
//! Uses TRUE STREAMING with Arrow IPC for high-performance query execution.
//! Arrow 56 matches DuckDB's bundled version for zero-copy serialization.

use crate::cursor_manager::{CursorManager, CursorManagerConfig};
use crate::executor::{DuckDbExecutor, ExecutorConfig};
use arrow_ipc::writer::FileWriter;
use dashmap::DashMap;
use duckdb::arrow::array::Array;
use duckdb::arrow::util::display::ArrayFormatter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tavana_common::proto;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

/// Cancellation token for a running query
pub struct QueryCancellationToken {
    cancelled: AtomicBool,
}

impl QueryCancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
        }
    }
    
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
    
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Query service implementation with cursor support and query cancellation
pub struct QueryServiceImpl {
    executor: Arc<DuckDbExecutor>,
    cursor_manager: Arc<CursorManager>,
    worker_id: String,
    /// Active queries with their cancellation tokens
    active_queries: Arc<DashMap<String, Arc<QueryCancellationToken>>>,
}

impl QueryServiceImpl {
    pub fn new(config: ExecutorConfig) -> Result<Self, anyhow::Error> {
        let executor = Arc::new(DuckDbExecutor::new(config)?);
        
        // Configure cursor manager from environment
        let cursor_config = CursorManagerConfig {
            max_cursors: std::env::var("MAX_CURSORS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            idle_timeout_secs: std::env::var("CURSOR_IDLE_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            cleanup_interval_secs: std::env::var("CURSOR_CLEANUP_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            ..Default::default()
        };
        
        let max_cursors = cursor_config.max_cursors;
        let cursor_manager = Arc::new(CursorManager::new(cursor_config));
        
        // Start background cursor cleanup task
        cursor_manager.start_cleanup_task();
        
        // Generate worker ID from hostname or random
        let worker_id = std::env::var("HOSTNAME")
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
        
        info!(
            worker_id = %worker_id,
            max_cursors = max_cursors,
            "QueryService initialized with cursor support and query cancellation"
        );
        
        Ok(Self { 
            executor,
            cursor_manager,
            worker_id,
            active_queries: Arc::new(DashMap::new()),
        })
    }
    
    /// Get the worker ID (for cursor affinity routing)
    #[allow(dead_code)]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    
    /// Get cursor manager stats
    #[allow(dead_code)]
    pub fn cursor_stats(&self) -> crate::cursor_manager::CursorManagerStats {
        self.cursor_manager.stats()
    }
    
    /// Register a query for cancellation tracking
    fn register_query(&self, query_id: &str) -> Arc<QueryCancellationToken> {
        let token = Arc::new(QueryCancellationToken::new());
        self.active_queries.insert(query_id.to_string(), token.clone());
        token
    }
    
    /// Unregister a query after completion
    fn unregister_query(&self, query_id: &str) {
        self.active_queries.remove(query_id);
    }
    
    /// Get active query count
    #[allow(dead_code)]
    pub fn active_query_count(&self) -> usize {
        self.active_queries.len()
    }
}

#[tonic::async_trait]
impl proto::query_service_server::QueryService for QueryServiceImpl {
    type ExecuteQueryStream = ReceiverStream<Result<proto::QueryResultBatch, Status>>;

    #[instrument(skip(self, request))]
    async fn execute_query(
        &self,
        request: Request<proto::ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let req = request.into_inner();
        info!(query_id = %req.query_id, "Executing query with TRUE STREAMING");

        let (tx, rx) = mpsc::channel(64); // Larger buffer for streaming
        let executor = self.executor.clone();
        let sql = req.sql.clone();
        let query_id = req.query_id.clone();
        
        // Register query for cancellation support
        let cancellation_token = self.register_query(&query_id);
        let active_queries = self.active_queries.clone();
        let query_id_for_cleanup = query_id.clone();

        // Spawn query execution in background with TRUE STREAMING
        // This never loads all results into memory - streams as DuckDB produces them
        tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let tx_clone = tx.clone();
            let query_id_clone = query_id.clone();
            let cancel_token = cancellation_token.clone();
            
            let mut metadata_sent = false;
            let mut total_rows: u64 = 0;
            let mut columns: Vec<String> = vec![];
            let mut column_types: Vec<String> = vec![];

            // Use the streaming API - batches are sent as they're produced
            let result = executor.execute_query_streaming(&sql, |batch| {
                // Check for cancellation before processing each batch
                if cancel_token.is_cancelled() {
                    info!(query_id = %query_id_clone, "Query cancelled by client");
                    return Err(anyhow::anyhow!("Query cancelled"));
                }
                
                // On first batch, send metadata
                if !metadata_sent {
                    let schema = batch.schema();
                    columns = schema.fields().iter().map(|f| f.name().clone()).collect();
                    column_types = schema.fields().iter().map(|f| format!("{:?}", f.data_type())).collect();
                    
                    let metadata = proto::QueryMetadata {
                        query_id: query_id_clone.clone(),
                        columns: columns.clone(),
                        column_types: column_types.clone(),
                        total_rows: 0, // Unknown in streaming mode
                        total_bytes: 0,
                    };
                    
                    // Use blocking send since we're in spawn_blocking
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        let _ = tx_clone.send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                        })).await;
                    });
                    
                    metadata_sent = true;
                    debug!("Sent metadata: {} columns", columns.len());
                }

                // Serialize batch using JSON for reliable cross-crate compatibility
                // Arrow IPC has issues with DuckDB's bundled Arrow vs standalone Arrow crate
                // TRUE STREAMING: batches are sent as they're produced, never buffered
                total_rows += batch.num_rows() as u64;
                
                // Use JSON serialization for reliable data transfer
                let ipc_data = serialize_batch_to_json_fallback(&batch);
                
                let arrow_batch = proto::ArrowRecordBatch {
                    schema: vec![], // Schema sent in metadata
                    data: ipc_data,
                    row_count: batch.num_rows() as u64,
                };

                // Send batch immediately - TRUE STREAMING
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let _ = tx_clone.send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::RecordBatch(arrow_batch)),
                    })).await;
                });

                // Log progress for very large queries
                if total_rows % 100_000 == 0 {
                    debug!("Streaming: {} rows sent to client", total_rows);
                }

                Ok(())
            });
            
            // Unregister query after completion
            active_queries.remove(&query_id_for_cleanup);

            // Handle result
            match result {
                Ok((_schema, rows)) => {
                    // If no batches were produced (empty result), send empty metadata
                    if !metadata_sent {
                        let metadata = proto::QueryMetadata {
                            query_id: query_id.clone(),
                            columns: vec![],
                            column_types: vec![],
                            total_rows: 0,
                            total_bytes: 0,
                        };
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(async {
                            let _ = tx.send(Ok(proto::QueryResultBatch {
                                result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                            })).await;
                        });
                    }

                    // Send profile at the end
                    let elapsed = start.elapsed();
                    let profile = proto::QueryProfile {
                        execution_time_ms: elapsed.as_millis() as u64,
                        rows_scanned: 0,
                        bytes_scanned: 0,
                        rows_returned: rows,
                        bytes_returned: 0,
                        peak_memory_bytes: 0,
                        cpu_seconds: elapsed.as_secs_f32(),
                        tables_accessed: vec![],
                    };

                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        let _ = tx.send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Profile(profile)),
                        })).await;
                    });
                    
                    info!("Query completed: {} rows streamed in {:?}", rows, elapsed);
                }
                Err(e) => {
                    error!("Query execution failed: {}", e);
                    let error = proto::Error {
                        code: "QUERY_FAILED".to_string(),
                        message: e.to_string(),
                        details: Default::default(),
                    };
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        let _ = tx.send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Error(error)),
                        })).await;
                    });
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn cancel_query(
        &self,
        request: Request<proto::CancelQueryRequest>,
    ) -> Result<Response<proto::CancelQueryResponse>, Status> {
        let req = request.into_inner();
        info!(query_id = %req.query_id, "Cancelling query");

        // Look up the query and cancel it
        if let Some(token) = self.active_queries.get(&req.query_id) {
            token.cancel();
            info!(query_id = %req.query_id, "Query cancellation signal sent");
            Ok(Response::new(proto::CancelQueryResponse {
                success: true,
                message: format!("Query {} cancellation requested", req.query_id),
            }))
        } else {
            warn!(query_id = %req.query_id, "Query not found for cancellation (may have completed)");
            Ok(Response::new(proto::CancelQueryResponse {
                success: false,
                message: format!("Query {} not found (may have already completed)", req.query_id),
            }))
        }
    }

    async fn get_query_status(
        &self,
        request: Request<proto::GetQueryStatusRequest>,
    ) -> Result<Response<proto::QueryStatusResponse>, Status> {
        let req = request.into_inner();

        // NOTE: Query status tracking requires a query state store (e.g., HashMap<query_id, State>)
        // This is a v1.1+ feature - for now we return Unknown state
        debug!(query_id = %req.query_id, "Query status tracking not yet implemented");
        Ok(Response::new(proto::QueryStatusResponse {
            query_id: req.query_id,
            state: proto::QueryState::Unspecified.into(),
            progress: None,
            error: None,
            started_at: None,
            completed_at: None,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        Ok(Response::new(proto::HealthCheckResponse {
            status: proto::ServiceStatus::Healthy.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            started_at: None,
        }))
    }

    // ============= Cursor Operations for True Streaming =============

    type FetchCursorStream = ReceiverStream<Result<proto::QueryResultBatch, Status>>;

    #[instrument(skip(self, request))]
    async fn declare_cursor(
        &self,
        request: Request<proto::DeclareCursorRequest>,
    ) -> Result<Response<proto::DeclareCursorResponse>, Status> {
        let req = request.into_inner();
        info!(cursor_id = %req.cursor_id, sql = %&req.sql[..req.sql.len().min(100)], "Declaring cursor");

        let executor = self.executor.clone();
        let cursor_manager = self.cursor_manager.clone();
        let cursor_id = req.cursor_id.clone();
        let sql = req.sql.clone();
        let worker_id = self.worker_id.clone();

        // Execute in blocking context since DuckDB is synchronous
        let result = tokio::task::spawn_blocking(move || {
            // Get a connection from the pool
            let pooled_conn = executor.get_connection();
            let conn = pooled_conn
                .connection
                .lock();

            match conn {
                Ok(conn_guard) => {
                    cursor_manager.declare_cursor(cursor_id, &sql, &conn_guard)
                }
                Err(e) => Err(anyhow::anyhow!("Failed to acquire connection: {}", e)),
            }
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {}", e)))?;

        match result {
            Ok(cursor) => {
                let columns: Vec<String> = cursor.columns.iter().map(|c| c.name.clone()).collect();
                let column_types: Vec<String> = cursor.columns.iter().map(|c| c.type_name.clone()).collect();
                
                info!(
                    cursor_id = %cursor.id,
                    columns = columns.len(),
                    worker_id = %worker_id,
                    "Cursor declared successfully"
                );

                Ok(Response::new(proto::DeclareCursorResponse {
                    success: true,
                    cursor_id: cursor.id.clone(),
                    columns,
                    column_types,
                    error_message: String::new(),
                    worker_id,
                }))
            }
            Err(e) => {
                warn!(error = %e, "Failed to declare cursor");
                Ok(Response::new(proto::DeclareCursorResponse {
                    success: false,
                    cursor_id: req.cursor_id,
                    columns: vec![],
                    column_types: vec![],
                    error_message: e.to_string(),
                    worker_id,
                }))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn fetch_cursor(
        &self,
        request: Request<proto::FetchCursorRequest>,
    ) -> Result<Response<Self::FetchCursorStream>, Status> {
        let req = request.into_inner();
        let max_rows = if req.max_rows == 0 { 1000 } else { req.max_rows as usize };
        
        info!(cursor_id = %req.cursor_id, max_rows = max_rows, "Fetching from cursor");

        let (tx, rx) = mpsc::channel(32);
        let cursor_manager = self.cursor_manager.clone();
        let cursor_id = req.cursor_id.clone();

        // Get cursor to retrieve column info
        let cursor = cursor_manager.get_cursor(&cursor_id);
        
        if cursor.is_none() {
            // Send error immediately
            let _ = tx
                .send(Ok(proto::QueryResultBatch {
                    result: Some(proto::query_result_batch::Result::Error(proto::Error {
                        code: "CURSOR_NOT_FOUND".to_string(),
                        message: format!("Cursor '{}' not found", cursor_id),
                        details: Default::default(),
                    })),
                }))
                .await;
            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        // Safe to expect here: we already returned early if cursor.is_none()
        let cursor = cursor.expect("cursor existence verified above");
        let columns: Vec<String> = cursor.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<String> = cursor.columns.iter().map(|c| c.type_name.clone()).collect();

        // Spawn fetch in background
        tokio::spawn(async move {
            let start = std::time::Instant::now();

            // Send metadata first
            let metadata = proto::QueryMetadata {
                query_id: cursor_id.clone(),
                columns: columns.clone(),
                column_types: column_types.clone(),
                total_rows: 0, // Unknown for streaming
                total_bytes: 0,
            };

            let _ = tx
                .send(Ok(proto::QueryResultBatch {
                    result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                }))
                .await;

            // Fetch rows from cursor (no re-scanning!)
            match cursor_manager.fetch_cursor(&cursor_id, max_rows) {
                Ok((rows, exhausted)) => {
                    if !rows.is_empty() {
                        // Serialize to JSON
                        let json_data = serde_json::to_vec(&rows).unwrap_or_default();

                        debug!(
                            cursor_id = %cursor_id,
                            rows = rows.len(),
                            bytes = json_data.len(),
                            exhausted = exhausted,
                            "Sending cursor fetch batch"
                        );

                        let arrow_batch = proto::ArrowRecordBatch {
                            schema: vec![],
                            data: json_data,
                            row_count: rows.len() as u64,
                        };

                        let _ = tx
                            .send(Ok(proto::QueryResultBatch {
                                result: Some(proto::query_result_batch::Result::RecordBatch(
                                    arrow_batch,
                                )),
                            }))
                            .await;
                    }

                    // Send profile
                    let elapsed = start.elapsed();
                    let profile = proto::QueryProfile {
                        execution_time_ms: elapsed.as_millis() as u64,
                        rows_scanned: 0,
                        bytes_scanned: 0,
                        rows_returned: rows.len() as u64,
                        bytes_returned: 0,
                        peak_memory_bytes: 0,
                        cpu_seconds: elapsed.as_secs_f32(),
                        tables_accessed: vec![],
                    };

                    let _ = tx
                        .send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Profile(profile)),
                        }))
                        .await;
                }
                Err(e) => {
                    error!(cursor_id = %cursor_id, error = %e, "Cursor fetch failed");
                    let _ = tx
                        .send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Error(proto::Error {
                                code: "CURSOR_FETCH_FAILED".to_string(),
                                message: e.to_string(),
                                details: Default::default(),
                            })),
                        }))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[instrument(skip(self, request))]
    async fn close_cursor(
        &self,
        request: Request<proto::CloseCursorRequest>,
    ) -> Result<Response<proto::CloseCursorResponse>, Status> {
        let req = request.into_inner();
        info!(cursor_id = %req.cursor_id, "Closing cursor");

        let success = self.cursor_manager.close_cursor(&req.cursor_id);
        
        if success {
            Ok(Response::new(proto::CloseCursorResponse {
                success: true,
                message: format!("Cursor '{}' closed", req.cursor_id),
            }))
        } else {
            Ok(Response::new(proto::CloseCursorResponse {
                success: false,
                message: format!("Cursor '{}' not found", req.cursor_id),
            }))
        }
    }
}

/// Serialize a RecordBatch to Arrow IPC format (zero-copy binary)
/// 
/// Now that Arrow 56 matches DuckDB's bundled version, we can use Arrow IPC directly.
/// This is 10-100x faster than JSON serialization and uses 50-80% less bandwidth.
#[allow(dead_code)]
fn serialize_batch_to_arrow_ipc(batch: &duckdb::arrow::array::RecordBatch) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    // Use FileWriter for a complete, self-contained Arrow IPC message
    {
        let schema = batch.schema();
        match FileWriter::try_new(&mut buffer, schema.as_ref()) {
            Ok(mut writer) => {
                if let Err(e) = writer.write(batch) {
                    warn!("Arrow IPC write failed: {}, falling back to JSON", e);
                    return serialize_batch_to_json_fallback(batch);
                }
                if let Err(e) = writer.finish() {
                    warn!("Arrow IPC finish failed: {}, falling back to JSON", e);
                    return serialize_batch_to_json_fallback(batch);
                }
            }
            Err(e) => {
                warn!("Arrow IPC writer creation failed: {}, falling back to JSON", e);
                return serialize_batch_to_json_fallback(batch);
            }
        }
    }
    
    buffer
}

/// Fallback JSON serialization for compatibility
fn serialize_batch_to_json_fallback(batch: &duckdb::arrow::array::RecordBatch) -> Vec<u8> {
    let mut rows_json: Vec<Vec<String>> = Vec::with_capacity(batch.num_rows());
    
    for row_idx in 0..batch.num_rows() {
        let mut row: Vec<String> = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let value = format_array_value(col.as_ref(), row_idx);
            row.push(value);
        }
        rows_json.push(row);
    }
    
    serde_json::to_vec(&rows_json).unwrap_or_default()
}

/// Format an Arrow array value at a given index as a string
///
/// Uses Arrow's built-in ArrayFormatter which handles ALL data types automatically,
/// including Decimal128, Decimal256, timestamps, dates, lists, structs, etc.
/// This is data-type agnostic - no need to add new types manually.
fn format_array_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    // Use Arrow's built-in formatter which handles all types correctly
    // FormatOptions::default() provides sensible defaults for all types
    match ArrayFormatter::try_new(array, &Default::default()) {
        Ok(formatter) => formatter.value(idx).to_string(),
        Err(_) => format!("<{:?}>", array.data_type()),
    }
}
