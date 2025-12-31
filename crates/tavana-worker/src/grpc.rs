//! gRPC service implementation for the worker
//!
//! Uses Arrow's built-in display utilities for data-type agnostic value formatting.
//! This eliminates the need to manually handle each Arrow data type.

use crate::cursor_manager::{CursorManager, CursorManagerConfig};
use crate::executor::{DuckDbExecutor, ExecutorConfig};
use duckdb::arrow::array::Array;
use duckdb::arrow::util::display::ArrayFormatter;
use std::sync::Arc;
use tavana_common::proto;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

/// Query service implementation with cursor support
pub struct QueryServiceImpl {
    executor: Arc<DuckDbExecutor>,
    cursor_manager: Arc<CursorManager>,
    worker_id: String,
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
            "QueryService initialized with cursor support"
        );
        
        Ok(Self { 
            executor,
            cursor_manager,
            worker_id,
        })
    }
    
    /// Get the worker ID (for cursor affinity routing)
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    
    /// Get cursor manager stats
    pub fn cursor_stats(&self) -> crate::cursor_manager::CursorManagerStats {
        self.cursor_manager.stats()
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
        info!(query_id = %req.query_id, "Executing query");

        let (tx, rx) = mpsc::channel(32);
        let executor = self.executor.clone();
        let sql = req.sql.clone();
        let query_id = req.query_id.clone();

        // Spawn query execution in background
        tokio::spawn(async move {
            let start = std::time::Instant::now();

            match executor.execute_query(&sql) {
                Ok(batches) => {
                    if batches.is_empty() {
                        debug!("Query returned no batches");
                        // Send empty metadata
                        let metadata = proto::QueryMetadata {
                            query_id: query_id.clone(),
                            columns: vec![],
                            column_types: vec![],
                            total_rows: 0,
                            total_bytes: 0,
                        };
                        let _ = tx
                            .send(Ok(proto::QueryResultBatch {
                                result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                            }))
                            .await;
                    } else {
                        // Extract column info from first batch's schema
                        let schema = batches[0].schema();
                        let columns: Vec<String> =
                            schema.fields().iter().map(|f| f.name().clone()).collect();
                        let column_types: Vec<String> = schema
                            .fields()
                            .iter()
                            .map(|f| format!("{:?}", f.data_type()))
                            .collect();
                        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

                        debug!(
                            "Sending metadata: {} columns, {} total rows",
                            columns.len(),
                            total_rows
                        );

                        // Send metadata first
                        let metadata = proto::QueryMetadata {
                            query_id: query_id.clone(),
                            columns: columns.clone(),
                            column_types: column_types.clone(),
                            total_rows,
                            total_bytes: 0,
                        };

                        let _ = tx
                            .send(Ok(proto::QueryResultBatch {
                                result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                            }))
                            .await;

                        // Send each batch with serialized data as JSON rows
                        for batch in &batches {
                            // Serialize rows as JSON array
                            let mut rows_json: Vec<Vec<String>> = Vec::new();
                            for row_idx in 0..batch.num_rows() {
                                let mut row: Vec<String> = Vec::new();
                                for col_idx in 0..batch.num_columns() {
                                    let col = batch.column(col_idx);
                                    let value = format_array_value(col.as_ref(), row_idx);
                                    row.push(value);
                                }
                                rows_json.push(row);
                            }

                            // Serialize to JSON
                            let json_data = serde_json::to_vec(&rows_json).unwrap_or_default();

                            debug!(
                                "Sending batch: {} rows, {} bytes",
                                batch.num_rows(),
                                json_data.len()
                            );

                            let arrow_batch = proto::ArrowRecordBatch {
                                schema: vec![],
                                data: json_data,
                                row_count: batch.num_rows() as u64,
                            };

                            let _ = tx
                                .send(Ok(proto::QueryResultBatch {
                                    result: Some(proto::query_result_batch::Result::RecordBatch(
                                        arrow_batch,
                                    )),
                                }))
                                .await;
                        }
                    }

                    // Send profile at the end
                    let elapsed = start.elapsed();
                    let profile = proto::QueryProfile {
                        execution_time_ms: elapsed.as_millis() as u64,
                        rows_scanned: 0,
                        bytes_scanned: 0,
                        rows_returned: batches.iter().map(|b| b.num_rows() as u64).sum(),
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
                    error!("Query execution failed: {}", e);
                    let error = proto::Error {
                        code: "QUERY_FAILED".to_string(),
                        message: e.to_string(),
                        details: Default::default(),
                    };
                    let _ = tx
                        .send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Error(error)),
                        }))
                        .await;
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

        // TODO: Implement query cancellation
        Ok(Response::new(proto::CancelQueryResponse {
            success: true,
            message: "Query cancelled".to_string(),
        }))
    }

    async fn get_query_status(
        &self,
        request: Request<proto::GetQueryStatusRequest>,
    ) -> Result<Response<proto::QueryStatusResponse>, Status> {
        let req = request.into_inner();

        // TODO: Implement status tracking
        Ok(Response::new(proto::QueryStatusResponse {
            query_id: req.query_id,
            state: proto::QueryState::Running.into(),
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

        let cursor = cursor.unwrap();
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
