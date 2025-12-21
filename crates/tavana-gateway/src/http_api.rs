//! HTTP REST API for query execution and management
//!
//! Uses query-aware pre-sizing to resize workers before query execution.

use crate::metrics;
use crate::query_router::{QueryRouter, QueryTarget};
use crate::worker_client::WorkerClient;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tavana_common::proto;
use tonic::transport::Channel;
use tracing::{error, info, warn};

/// Application state shared across HTTP handlers
#[derive(Clone)]
pub struct AppState {
    pub worker_client: Arc<WorkerClient>,
    pub query_router: Arc<QueryRouter>,
}

/// Query execution request
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default = "default_user")]
    pub user_id: String,
}

fn default_user() -> String {
    "anonymous".to_string()
}

/// Query execution response
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    pub bytes_scanned: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_size_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_memory_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_resized: Option<bool>,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Execute a SQL query with query-aware pre-sizing
pub async fn execute_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    info!("HTTP query request: {}", request.sql);

    let estimate = state.query_router.route(&request.sql).await;
    let start_time = std::time::Instant::now();

    // Extract worker info for releasing later
    let worker_name = match &estimate.target {
        QueryTarget::PreSizedWorker { worker_name, .. } => Some(worker_name.clone()),
        QueryTarget::WorkerPool => None,
        QueryTarget::TenantPool { .. } => None, // Tenant pools handle their own lifecycle
    };
    
    info!(
        "Query: {}MB data, {}MB memory, resized={}, target={:?}",
        estimate.data_size_mb,
        estimate.required_memory_mb,
        estimate.was_resized,
        match &estimate.target {
            QueryTarget::WorkerPool => "WorkerPool".to_string(),
            QueryTarget::PreSizedWorker { worker_name, .. } => format!("PreSized({})", worker_name),
            QueryTarget::TenantPool { tenant_id, .. } => format!("TenantPool({})", tenant_id),
        }
    );

    // Execute query based on target
    let result = match &estimate.target {
        QueryTarget::PreSizedWorker { address, .. } => {
            execute_on_presized_worker(address, &request.sql, &request.user_id).await
        }
        QueryTarget::TenantPool { service_addr, .. } => {
            // Tenant pools use the same execution path as pre-sized workers
            execute_on_presized_worker(service_addr, &request.sql, &request.user_id).await
        }
        QueryTarget::WorkerPool => {
            execute_on_worker_pool(&state.worker_client, &request.sql, &request.user_id).await
        }
    };

    // Release worker after query
    if let Some(name) = worker_name {
        // TODO: Get actual memory usage from worker for better pre-sizing accuracy
        state.query_router.release_worker(&name, None).await;
    }

    let route_label = match &estimate.target {
        QueryTarget::PreSizedWorker { .. } => "presized_worker",
        QueryTarget::TenantPool { .. } => "tenant_pool",
        QueryTarget::WorkerPool => "worker_pool",
    };

    match result {
        Ok((columns, column_types, rows, total_rows)) => {
            let elapsed = start_time.elapsed();
            metrics::record_query_completed(route_label, "success", elapsed.as_secs_f64());
            
            let estimated_bytes = total_rows as u64 * 100;
            metrics::record_data_scanned(estimated_bytes);
            metrics::record_actual_query_size((estimated_bytes as f64) / (1024.0 * 1024.0));
            
            let response = QueryResponse {
                columns,
                column_types,
                rows,
                row_count: total_rows,
                execution_time_ms: elapsed.as_millis() as u64,
                bytes_scanned: estimated_bytes,
                estimated_size_mb: Some(estimate.data_size_mb),
                required_memory_mb: Some(estimate.required_memory_mb),
                worker_resized: Some(estimate.was_resized),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let elapsed = start_time.elapsed();
            metrics::record_query_completed(route_label, "error", elapsed.as_secs_f64());
            
            error!("Query failed: {}", e);
            let error_response = ErrorResponse {
                error: e.to_string(),
                code: "QUERY_ERROR".to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Execute query on a specific pre-sized worker
async fn execute_on_presized_worker(
    worker_addr: &str,
    sql: &str,
    user_id: &str,
) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>, usize), anyhow::Error> {
    info!("Executing on pre-sized worker: {}", worker_addr);

    const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

    let channel = Channel::from_shared(worker_addr.to_string())?
        .timeout(std::time::Duration::from_secs(1800))
        .connect_timeout(std::time::Duration::from_secs(30))
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .connect()
        .await?;

    let mut client = proto::query_service_client::QueryServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let query_id = uuid::Uuid::new_v4().to_string();

    let request = proto::ExecuteQueryRequest {
        query_id,
        sql: sql.to_string(),
        user: Some(proto::UserIdentity {
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            scopes: vec!["query:execute".to_string()],
            claims: Default::default(),
        }),
        options: Some(proto::QueryOptions {
            timeout_seconds: 300,
            max_rows: 0,  // 0 = unlimited rows (streaming)
            max_bytes: 0,
            enable_profiling: false,
            session_params: Default::default(),
        }),
        allocated_resources: None,
    };

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    let mut columns: Vec<String> = Vec::new();
    let mut column_types: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut total_rows: u64 = 0;

    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(proto::query_result_batch::Result::Metadata(meta)) => {
                columns = meta.columns;
                column_types = meta.column_types;
                total_rows = meta.total_rows;
            }
            Some(proto::query_result_batch::Result::RecordBatch(batch)) => {
                if !batch.data.is_empty() {
                    if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data)
                    {
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

    Ok((columns, column_types, rows, total_rows as usize))
}

/// Execute query on default worker pool (fallback)
async fn execute_on_worker_pool(
    worker_client: &WorkerClient,
    sql: &str,
    user_id: &str,
) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>, usize), anyhow::Error> {
    match worker_client.execute_query(sql, user_id).await {
        Ok(result) => Ok((
            result.columns.iter().map(|c| c.name.clone()).collect(),
            result.columns.iter().map(|c| c.type_name.clone()).collect(),
            result.rows,
            result.total_rows as usize,
        )),
        Err(e) => {
            warn!("Worker pool error: {}", e);
            Err(e)
        }
    }
}

/// Prometheus metrics endpoint
pub async fn prometheus_metrics() -> impl IntoResponse {
    let body = metrics::encode_metrics();
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        body,
    )
}

/// Export request for large query results
#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    pub sql: String,
    pub output_path: String,
    #[serde(default)]
    pub format: ExportFormat,
    #[serde(default)]
    pub partition_by: Vec<String>,
    #[serde(default = "default_user")]
    pub user_id: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    #[default]
    Parquet,
    Csv,
    Json,
}

/// Export response
#[derive(Debug, Serialize)]
pub struct ExportResponse {
    pub output_path: String,
    pub message: String,
    pub execution_time_ms: u64,
}

/// Export large query results directly to S3/cloud storage
pub async fn export_query(
    State(state): State<AppState>,
    Json(request): Json<ExportRequest>,
) -> impl IntoResponse {
    info!("Export request to {}: {}", request.output_path, request.sql);
    
    let start = std::time::Instant::now();
    
    let format_options = match request.format {
        ExportFormat::Parquet if !request.partition_by.is_empty() => {
            format!(
                "FORMAT PARQUET, COMPRESSION 'ZSTD', PARTITION_BY ({})",
                request.partition_by.join(", ")
            )
        }
        ExportFormat::Parquet => "FORMAT PARQUET, COMPRESSION 'ZSTD'".to_string(),
        ExportFormat::Csv => "FORMAT CSV, HEADER true".to_string(),
        ExportFormat::Json => "FORMAT JSON".to_string(),
    };
    
    let export_sql = format!(
        "COPY ({}) TO '{}' ({})",
        request.sql, request.output_path, format_options
    );
    
    match state
        .worker_client
        .execute_query(&export_sql, &request.user_id)
        .await
    {
        Ok(_) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            info!(
                "Export completed in {}ms to {}",
                duration_ms, request.output_path
            );
            
            (
                StatusCode::OK,
                Json(ExportResponse {
                output_path: request.output_path,
                message: "Export completed successfully".to_string(),
                execution_time_ms: duration_ms,
                }),
            )
        }
        Err(e) => {
            error!("Export failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ExportResponse {
                output_path: request.output_path,
                message: format!("Export failed: {}", e),
                execution_time_ms: start.elapsed().as_millis() as u64,
                }),
            )
        }
    }
}

/// Health check endpoint
pub async fn health() -> &'static str {
    "OK"
}

/// Ready check endpoint
pub async fn ready() -> &'static str {
    "OK"
}

/// Root endpoint
pub async fn root() -> &'static str {
    "Tavana Gateway - Query-Aware Pre-Sizing (K8s v1.35)"
}
