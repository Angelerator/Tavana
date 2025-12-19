//! HTTP REST API for query execution and management
//!
//! Simplified: All queries go to HPA+VPA managed worker pool

use crate::query_router::QueryRouter;
use crate::worker_client::WorkerClient;
use crate::metrics;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

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
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Execute a SQL query via worker pool
pub async fn execute_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    info!("HTTP query request: {}", request.sql);

    let estimate = state.query_router.route(&request.sql).await;
    let start_time = std::time::Instant::now();
    
    info!(
        "Query: {}MB data → Worker Pool (method={})",
        estimate.data_size_mb, estimate.estimation_method
    );
    
    // All queries go to worker pool
    match state.worker_client.execute_query(&request.sql, &request.user_id).await {
        Ok(result) => {
            let elapsed = start_time.elapsed();
            
            metrics::record_query_completed("worker_pool", "success", elapsed.as_secs_f64());
            
            let estimated_bytes = result.total_rows as u64 * 100;
            metrics::record_data_scanned(estimated_bytes);
            metrics::record_actual_query_size((estimated_bytes as f64) / (1024.0 * 1024.0));
            
            let response = QueryResponse {
                columns: result.columns.iter().map(|c| c.name.clone()).collect(),
                column_types: result.columns.iter().map(|c| c.type_name.clone()).collect(),
                rows: result.rows,
                row_count: result.total_rows as usize,
                execution_time_ms: elapsed.as_millis() as u64,
                bytes_scanned: estimated_bytes,
                estimated_size_mb: Some(estimate.data_size_mb),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let elapsed = start_time.elapsed();
            metrics::record_query_completed("worker_pool", "error", elapsed.as_secs_f64());
            
            error!("Query failed: {}", e);
            let error_response = ErrorResponse {
                error: e.to_string(),
                code: "QUERY_ERROR".to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
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
    
    match state.worker_client.execute_query(&export_sql, &request.user_id).await {
        Ok(_) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            info!("Export completed in {}ms to {}", duration_ms, request.output_path);
            
            (StatusCode::OK, Json(ExportResponse {
                output_path: request.output_path,
                message: "Export completed successfully".to_string(),
                execution_time_ms: duration_ms,
            }))
        }
        Err(e) => {
            error!("Export failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ExportResponse {
                output_path: request.output_path,
                message: format!("Export failed: {}", e),
                execution_time_ms: start.elapsed().as_millis() as u64,
            }))
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
    "Tavana Gateway - HPA+VPA Managed Worker Pool (K8s v1.35)"
}
