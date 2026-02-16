//! gRPC service implementation for the worker
//!
//! Uses TRUE STREAMING for high-performance query execution.

use crate::cursor_manager::{CursorManager, CursorManagerConfig};
use crate::executor::{DuckDbExecutor, ExecutorConfig};
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

        let (tx, rx) = mpsc::channel(16); // Bounded buffer: limits memory to ~16 batches
        let executor = self.executor.clone();
        let sql = req.sql.clone();
        let query_id = req.query_id.clone();
        
        // Extract session credentials from request options
        let session_params = req.options
            .as_ref()
            .map(|o| o.session_params.clone())
            .unwrap_or_default();
        
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
            let mut first_batch_time: Option<std::time::Duration> = None;
            let mut last_progress_log = std::time::Instant::now();

            info!(query_id = %query_id_clone, sql_preview = %&sql[..sql.len().min(100)], session_creds = session_params.len(), "Starting query execution");

            // Use the streaming API - batches are sent as they're produced
            let result = executor.execute_query_streaming_with_session(&sql, &session_params, |batch| {
                // Check for cancellation before processing each batch
                if cancel_token.is_cancelled() {
                    info!(query_id = %query_id_clone, "Query cancelled by client");
                    return Err(anyhow::anyhow!("Query cancelled"));
                }
                
                // On first batch, send metadata and log timing
                if !metadata_sent {
                    let elapsed = start.elapsed();
                    first_batch_time = Some(elapsed);
                    info!(
                        query_id = %query_id_clone,
                        first_batch_ms = elapsed.as_millis(),
                        batch_rows = batch.num_rows(),
                        "First batch received from DuckDB"
                    );
                    
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
                    
                    // blocking_send: efficient for spawn_blocking (no nested async overhead)
                    let _ = tx_clone.blocking_send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                    }));
                    
                    metadata_sent = true;
                    debug!("Sent metadata: {} columns", columns.len());
                }

                // Serialize batch using Arrow IPC for ~10-50x faster serialization
                // Arrow IPC preserves native typed columnar data (no per-cell string conversion)
                // TRUE STREAMING: batches are sent as they're produced, never buffered
                total_rows += batch.num_rows() as u64;
                
                // Dual format: IPC stream (for PG wire) + split header/body (for Flight SQL passthrough)
                let ipc_data = serialize_batch_to_arrow_ipc(&batch);
                let (ipc_header, ipc_body) = serialize_batch_to_flight_format(&batch);
                
                let arrow_batch = proto::ArrowRecordBatch {
                    schema: vec![],
                    data: ipc_data,
                    row_count: batch.num_rows() as u64,
                    ipc_header,
                    ipc_body,
                };

                // blocking_send: efficient for spawn_blocking (no nested async overhead)
                let _ = tx_clone.blocking_send(Ok(proto::QueryResultBatch {
                    result: Some(proto::query_result_batch::Result::RecordBatch(arrow_batch)),
                }));

                // Log progress every 10 seconds for visibility
                if last_progress_log.elapsed().as_secs() >= 10 {
                    info!(
                        query_id = %query_id_clone,
                        rows_streamed = total_rows,
                        elapsed_secs = start.elapsed().as_secs(),
                        "Streaming progress"
                    );
                    last_progress_log = std::time::Instant::now();
                }

                Ok(())
            });
            
            // Unregister query after completion
            active_queries.remove(&query_id_for_cleanup);

            // Handle result
            match result {
                Ok((schema, rows)) => {
                    // If no batches were produced (empty result like LIMIT 0),
                    // we still need to send the schema for schema detection to work
                    if !metadata_sent {
                        // Extract column info from the schema - this is critical for LIMIT 0 queries!
                        let schema_columns: Vec<String> = schema.fields().iter()
                            .map(|f| f.name().clone())
                            .collect();
                        let schema_types: Vec<String> = schema.fields().iter()
                            .map(|f| format!("{:?}", f.data_type()))
                            .collect();
                        
                        debug!("Empty result set, sending schema with {} columns from schema", schema_columns.len());
                        
                        let metadata = proto::QueryMetadata {
                            query_id: query_id.clone(),
                            columns: schema_columns,
                            column_types: schema_types,
                            total_rows: 0,
                            total_bytes: 0,
                        };
                        let _ = tx.blocking_send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                        }));
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

                    let _ = tx.blocking_send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::Profile(profile)),
                    }));
                    
                    info!("Query completed: {} rows streamed in {:?}", rows, elapsed);
                }
                Err(e) => {
                    error!("Query execution failed: {}", e);
                    let error = proto::Error {
                        code: "QUERY_FAILED".to_string(),
                        message: e.to_string(),
                        details: Default::default(),
                    };
                    let _ = tx.blocking_send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::Error(error)),
                    }));
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
        
        // Extract session credentials from request options
        let session_params = req.options
            .as_ref()
            .map(|o| o.session_params.clone())
            .unwrap_or_default();
        
        info!(cursor_id = %req.cursor_id, sql = %&req.sql[..req.sql.len().min(100)], session_creds = session_params.len(), "Declaring cursor");

        let executor = self.executor.clone();
        let cursor_manager = self.cursor_manager.clone();
        let cursor_id = req.cursor_id.clone();
        let sql = req.sql.clone();
        let worker_id = self.worker_id.clone();

        // Execute in blocking context since DuckDB is synchronous
        let result = tokio::task::spawn_blocking(move || {
            let pooled_conn = executor.get_connection();
            let conn = pooled_conn
                .connection
                .lock();

            match conn {
                Ok(conn_guard) => {
                    let has_session_creds = !session_params.is_empty();
                    
                    // Apply user session credentials (SET/CREATE SECRET) before cursor creation
                    if has_session_creds {
                        if let Err(e) = crate::executor::DuckDbExecutor::apply_session_credentials(&conn_guard, &session_params) {
                            return Err(anyhow::anyhow!("Failed to apply session credentials: {}", e));
                        }
                    }
                    
                    let conn_id = pooled_conn.id;
                    let result = cursor_manager.declare_cursor(cursor_id, &sql, &conn_guard, session_params.clone(), conn_id);
                    
                    // Restore infra credentials after cursor creation to prevent leakage
                    if has_session_creds {
                        if let Err(e) = crate::executor::DuckDbExecutor::restore_after_session_credentials(&conn_guard, &session_params) {
                            warn!("Failed to restore credentials after cursor declare: {}", e);
                        }
                    }
                    
                    result
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
        let cursor_session_params = cursor.session_params.clone();
        let cursor_conn_id = cursor.connection_id;

        let executor = self.executor.clone();

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

            // Fetch rows from cursor buffer, refilling from DuckDB when needed.
            // The cursor uses LIMIT/OFFSET pagination internally — when the in-memory
            // buffer is exhausted, we load the next batch from DuckDB before retrying.
            let mut total_rows: usize = 0;
            let mut rows_remaining = max_rows;

            loop {
                // Try fetching from the current buffer
                let fetch_result = cursor_manager.fetch_cursor_arrow(&cursor_id, rows_remaining);

                match fetch_result {
                    Ok((batches, exhausted)) => {
                        let batch_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        total_rows += batch_rows;
                        rows_remaining = rows_remaining.saturating_sub(batch_rows);

                        for batch in &batches {
                            if batch.num_rows() == 0 { continue; }

                            let ipc_data = serialize_batch_to_arrow_ipc(batch);

                            debug!(
                                cursor_id = %cursor_id,
                                batch_rows = batch.num_rows(),
                                bytes = ipc_data.len(),
                                exhausted = exhausted,
                                "Sending cursor fetch Arrow IPC batch"
                            );

                            let arrow_batch = proto::ArrowRecordBatch {
                                schema: vec![],
                                data: ipc_data,
                                row_count: batch.num_rows() as u64,
                                ipc_header: vec![],
                                ipc_body: vec![],
                            };

                            let _ = tx
                                .send(Ok(proto::QueryResultBatch {
                                    result: Some(proto::query_result_batch::Result::RecordBatch(
                                        arrow_batch,
                                    )),
                                }))
                                .await;
                        }

                        // If cursor is fully exhausted or we have enough rows, stop
                        if exhausted || rows_remaining == 0 {
                            break;
                        }

                        // Buffer is empty but cursor has more data — refill from DuckDB
                        let cm = cursor_manager.clone();
                        let cid = cursor_id.clone();
                        let exec = executor.clone();
                        let refill_creds = cursor_session_params.clone();

                        let refill_ok = tokio::task::spawn_blocking(move || {
                            // Pin to the same connection that created the materialized table
                            let pooled_conn = exec.get_connection_by_id(cursor_conn_id);
                            let conn = pooled_conn.connection.lock();
                            match conn {
                                Ok(conn_guard) => {
                                    let has_creds = !refill_creds.is_empty();
                                    
                                    // Apply session credentials before refill query
                                    if has_creds {
                                        if let Err(e) = crate::executor::DuckDbExecutor::apply_session_credentials(&conn_guard, &refill_creds) {
                                            warn!(cursor_id = %cid, error = %e, "Failed to apply session creds for cursor refill");
                                            return false;
                                        }
                                    }
                                    
                                    let result = match cm.fetch_more_for_cursor(&cid, &conn_guard) {
                                        Ok(has_more) => {
                                            debug!(cursor_id = %cid, has_more = has_more, "Refilled cursor buffer");
                                            true
                                        }
                                        Err(e) => {
                                            warn!(cursor_id = %cid, error = %e, "Failed to refill cursor buffer");
                                            false
                                        }
                                    };
                                    
                                    // Restore infra credentials after refill
                                    if has_creds {
                                        if let Err(e) = crate::executor::DuckDbExecutor::restore_after_session_credentials(&conn_guard, &refill_creds) {
                                            warn!(cursor_id = %cid, error = %e, "Failed to restore creds after cursor refill");
                                        }
                                    }
                                    
                                    result
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to acquire connection for cursor refill");
                                    false
                                }
                            }
                        })
                        .await
                        .unwrap_or(false);

                        if !refill_ok {
                            break;
                        }
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
                        break;
                    }
                }
            }

            // Send profile after all batches
            let elapsed = start.elapsed();
            let profile = proto::QueryProfile {
                execution_time_ms: elapsed.as_millis() as u64,
                rows_scanned: 0,
                bytes_scanned: 0,
                rows_returned: total_rows as u64,
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
        
        // Drop any materialized cursor tables synchronously to prevent race conditions
        // (reused cursor IDs could otherwise see a stale drop for a new table).
        if success {
            let executor = self.executor.clone();
            let cm = self.cursor_manager.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let pooled_conn = executor.get_connection();
                let conn = pooled_conn.connection.lock();
                if let Ok(conn_guard) = conn {
                    cm.drop_pending_tables(&conn_guard);
                    drop(conn_guard);
                }
            })
            .await;
        }
        
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

/// Serialize a RecordBatch to FlightData-compatible split format (header + body).
/// 
/// Produces the exact bytes that FlightData.data_header and FlightData.data_body need,
/// enabling zero-copy passthrough in the gateway for Flight SQL clients.
/// The gateway can forward these directly without deserializing to RecordBatch.
fn serialize_batch_to_flight_format(batch: &duckdb::arrow::array::RecordBatch) -> (Vec<u8>, Vec<u8>) {
    use duckdb::arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions, DictionaryTracker};
    
    let gen = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let write_options = IpcWriteOptions::default();
    
    match gen.encoded_batch(batch, &mut tracker, &write_options) {
        Ok((_encoded_dicts, encoded_batch)) => {
            // encoded_batch.ipc_message = Flatbuffer Message header
            // encoded_batch.arrow_data = Raw buffer data
            (encoded_batch.ipc_message, encoded_batch.arrow_data)
        }
        Err(e) => {
            tracing::warn!("Flight format serialization failed: {}", e);
            (vec![], vec![])
        }
    }
}

/// Get the configured IPC compression type from environment.
///
/// Supports: "lz4" (default), "zstd", "none"
/// LZ4 decompresses at ~1 GB/s per core with negligible CPU overhead.
/// ZSTD gives 15-20% better compression than LZ4 at slightly higher CPU cost.
fn ipc_compression() -> Option<arrow_ipc::CompressionType> {
    match std::env::var("TAVANA_IPC_COMPRESSION")
        .unwrap_or_else(|_| "lz4".to_string())
        .to_lowercase()
        .as_str()
    {
        "none" | "off" | "false" => None,
        "zstd" => Some(arrow_ipc::CompressionType::ZSTD),
        _ => Some(arrow_ipc::CompressionType::LZ4_FRAME), // default: lz4
    }
}

/// Build IPC write options with the configured compression.
fn ipc_write_options() -> arrow_ipc::writer::IpcWriteOptions {
    let compression = ipc_compression();
    arrow_ipc::writer::IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5)
        .and_then(|opts| opts.try_with_compression(compression))
        .unwrap_or_default()
}

/// Serialize a RecordBatch to Arrow IPC streaming format with LZ4 compression.
/// 
/// Arrow IPC is ~10-50x faster than JSON serialization because:
/// - No per-cell string conversion (preserves native typed data)
/// - Near zero-copy serialization of columnar buffers
/// - Gateway can deserialize with matching arrow-rs v56
///
/// LZ4 compression reduces wire bytes by 2-5x with <1ms/MB overhead.
fn serialize_batch_to_arrow_ipc(batch: &duckdb::arrow::array::RecordBatch) -> Vec<u8> {
    use arrow_ipc::writer::StreamWriter;

    let options = ipc_write_options();
    
    // Pre-allocate: ~8 bytes per cell + schema overhead is a reasonable estimate
    let estimated_size = batch.num_rows() * batch.num_columns() * 8 + 1024;
    let mut buf = Vec::with_capacity(estimated_size);
    let schema = batch.schema();
    match StreamWriter::try_new_with_options(&mut buf, &schema, options) {
        Ok(mut writer) => {
            if let Err(e) = writer.write(batch) {
                tracing::warn!("Arrow IPC write failed, falling back to JSON: {}", e);
                return serialize_batch_to_json_fallback(batch);
            }
            if let Err(e) = writer.finish() {
                tracing::warn!("Arrow IPC finish failed, falling back to JSON: {}", e);
                return serialize_batch_to_json_fallback(batch);
            }
        }
        Err(e) => {
            tracing::warn!("Arrow IPC writer creation failed, falling back to JSON: {}", e);
            return serialize_batch_to_json_fallback(batch);
        }
    }
    buf
}

/// JSON serialization fallback (only used if Arrow IPC fails)
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
