//! gRPC service implementation for the worker

use crate::executor::{DuckDbExecutor, ExecutorConfig};
use duckdb::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array, StringArray, LargeStringArray,
    Date32Array, Date64Array, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::DataType;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument};
use tavana_common::proto;

/// Query service implementation
pub struct QueryServiceImpl {
    executor: Arc<DuckDbExecutor>,
}

impl QueryServiceImpl {
    pub fn new(config: ExecutorConfig) -> Result<Self, anyhow::Error> {
        let executor = Arc::new(DuckDbExecutor::new(config)?);
        Ok(Self { executor })
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
                        let _ = tx.send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                        })).await;
                    } else {
                        // Extract column info from first batch's schema
                        let schema = batches[0].schema();
                        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
                        let column_types: Vec<String> = schema.fields().iter().map(|f| format!("{:?}", f.data_type())).collect();
                        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                        
                        debug!("Sending metadata: {} columns, {} total rows", columns.len(), total_rows);
                        
                        // Send metadata first
                        let metadata = proto::QueryMetadata {
                            query_id: query_id.clone(),
                            columns: columns.clone(),
                            column_types: column_types.clone(),
                            total_rows,
                            total_bytes: 0,
                        };
                        
                        let _ = tx.send(Ok(proto::QueryResultBatch {
                            result: Some(proto::query_result_batch::Result::Metadata(metadata)),
                        })).await;

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
                            
                            debug!("Sending batch: {} rows, {} bytes", batch.num_rows(), json_data.len());
                            
                            let arrow_batch = proto::ArrowRecordBatch {
                                schema: vec![],
                                data: json_data,
                                row_count: batch.num_rows() as u64,
                            };

                            let _ = tx.send(Ok(proto::QueryResultBatch {
                                result: Some(proto::query_result_batch::Result::RecordBatch(arrow_batch)),
                            })).await;
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

                    let _ = tx.send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::Profile(profile)),
                    })).await;
                }
                Err(e) => {
                    error!("Query execution failed: {}", e);
                    let error = proto::Error {
                        code: "QUERY_FAILED".to_string(),
                        message: e.to_string(),
                        details: Default::default(),
                    };
                    let _ = tx.send(Ok(proto::QueryResultBatch {
                        result: Some(proto::query_result_batch::Result::Error(error)),
                    })).await;
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
}

/// Format an Arrow array value at a given index as a string
fn format_array_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Boolean => {
            array.as_any().downcast_ref::<BooleanArray>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Int8 => {
            array.as_any().downcast_ref::<Int8Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Int16 => {
            array.as_any().downcast_ref::<Int16Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Int32 => {
            array.as_any().downcast_ref::<Int32Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Int64 => {
            array.as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::UInt8 => {
            array.as_any().downcast_ref::<UInt8Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::UInt16 => {
            array.as_any().downcast_ref::<UInt16Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::UInt32 => {
            array.as_any().downcast_ref::<UInt32Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::UInt64 => {
            array.as_any().downcast_ref::<UInt64Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Float32 => {
            array.as_any().downcast_ref::<Float32Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Float64 => {
            array.as_any().downcast_ref::<Float64Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Utf8 => {
            array.as_any().downcast_ref::<StringArray>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::LargeUtf8 => {
            array.as_any().downcast_ref::<LargeStringArray>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Date32 => {
            array.as_any().downcast_ref::<Date32Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Date64 => {
            array.as_any().downcast_ref::<Date64Array>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        }
        DataType::Timestamp(_, _) => {
            array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_else(|| "<timestamp>".to_string())
        }
        _ => format!("<{:?}>", array.data_type()),
    }
}

