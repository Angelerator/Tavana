//! HTTP REST API for query execution and management

use crate::adaptive::{AdaptiveController, QueryRecord};
use crate::k8s_client::K8sQueryClient;
use crate::query_router::{QueryRouter, QueryTarget};
use crate::worker_client::WorkerClient;
use crate::metrics;
use chrono::Utc;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

/// Application state shared across HTTP handlers
#[derive(Clone)]
pub struct AppState {
    pub worker_client: Arc<WorkerClient>,
    pub k8s_client: Arc<K8sQueryClient>,
    pub query_router: Arc<QueryRouter>,
    pub adaptive_controller: Arc<AdaptiveController>,
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
    pub execution_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_size_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold_mb: Option<u64>,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Execute a SQL query with hybrid routing
pub async fn execute_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    info!("HTTP query request: {}", request.sql);

    // Use async routing with real data estimation
    let estimate = state.query_router.route(&request.sql).await;
    let start_time = std::time::Instant::now();
    
    info!(
        "Routing decision: {}MB data, threshold={}MB, method={}, target={:?}",
        estimate.data_size_mb, estimate.threshold_mb, estimate.estimation_method, estimate.target
    );
    
    match estimate.target {
        QueryTarget::WorkerPool => {
            info!(
                "HYBRID: Small query ({}MB < {}MB) → Worker Pool",
                estimate.data_size_mb, estimate.threshold_mb
            );
            execute_via_worker_pool(
                &state.worker_client, 
                &state.adaptive_controller,
                &request.sql, 
                &request.user_id, 
                start_time,
                Some(estimate.data_size_mb),
                Some(estimate.threshold_mb),
            ).await
        }
        QueryTarget::EphemeralPod { memory_mb, cpu_millicores } => {
            info!(
                "HYBRID: Large query ({}MB >= {}MB) → Ephemeral Pod ({}MB RAM, {}m CPU)",
                estimate.data_size_mb, estimate.threshold_mb, memory_mb, cpu_millicores
            );
            
            // Record ephemeral pod creation
            metrics::record_ephemeral_pod_created(memory_mb as f64);
            
            // Try ephemeral pod execution if K8s is available
            if state.k8s_client.is_available() {
                execute_via_ephemeral_pod(
                    &state.k8s_client,
                    &state.worker_client,
                    &state.adaptive_controller,
                    &request.sql,
                    &request.user_id,
                    memory_mb,
                    cpu_millicores,
                    start_time,
                    Some(estimate.data_size_mb),
                    Some(estimate.threshold_mb),
                ).await
            } else {
                warn!("K8s not available, falling back to worker pool");
                execute_via_worker_pool(
                    &state.worker_client, 
                    &state.adaptive_controller,
                    &request.sql, 
                    &request.user_id, 
                    start_time,
                    Some(estimate.data_size_mb),
                    Some(estimate.threshold_mb),
                ).await
            }
        }
    }
}

/// Execute query via worker pool (HPA managed)
async fn execute_via_worker_pool(
    worker_client: &WorkerClient,
    adaptive_controller: &AdaptiveController,
    sql: &str,
    user_id: &str,
    start_time: std::time::Instant,
    estimated_size_mb: Option<u64>,
    threshold_mb: Option<u64>,
) -> axum::response::Response {
    match worker_client.execute_query(sql, user_id).await {
        Ok(result) => {
            let elapsed = start_time.elapsed();
            
            // Record success metrics
            metrics::record_query_completed("worker_pool", "success", elapsed.as_secs_f64());
            
            // Record data size metrics
            // Estimate actual size from row count * avg row size (assume 100 bytes per row)
            let estimated_bytes = result.total_rows as u64 * 100;
            let actual_size_mb = (estimated_bytes as f64) / (1024.0 * 1024.0);
            metrics::record_data_scanned(estimated_bytes);
            metrics::record_actual_query_size(actual_size_mb);
            if let Some(est) = estimated_size_mb {
                metrics::record_estimation_accuracy(est as f64, actual_size_mb);
            }
            
            // Record to history for EMA calculation
            adaptive_controller.get_history().record_query(QueryRecord {
                timestamp: Utc::now(),
                estimated_size_mb: estimated_size_mb.unwrap_or(0),
                actual_size_mb: actual_size_mb as u64,
                execution_time_ms: elapsed.as_millis() as u64,
                was_ephemeral: false,
                success: true,
            });
            
            let response = QueryResponse {
                columns: result.columns.iter().map(|c| c.name.clone()).collect(),
                column_types: result.columns.iter().map(|c| c.type_name.clone()).collect(),
                rows: result.rows,
                row_count: result.total_rows as usize,
                execution_time_ms: elapsed.as_millis() as u64,
                bytes_scanned: 0,
                execution_mode: Some("worker_pool".to_string()),
                estimated_size_mb,
                threshold_mb,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let elapsed = start_time.elapsed();
            metrics::record_query_completed("worker_pool", "error", elapsed.as_secs_f64());
            
            // Record failure to history
            adaptive_controller.get_history().record_query(QueryRecord {
                timestamp: Utc::now(),
                estimated_size_mb: estimated_size_mb.unwrap_or(0),
                actual_size_mb: 0,
                execution_time_ms: elapsed.as_millis() as u64,
                was_ephemeral: false,
                success: false,
            });
            
            error!("Worker pool query failed: {}", e);
            let error_response = ErrorResponse {
                error: e.to_string(),
                code: "QUERY_ERROR".to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Execute query via ephemeral pod
async fn execute_via_ephemeral_pod(
    k8s_client: &K8sQueryClient,
    worker_client: &WorkerClient,
    adaptive_controller: &AdaptiveController,
    sql: &str,
    user_id: &str,
    memory_mb: u32,
    cpu_millicores: u32,
    start_time: std::time::Instant,
    estimated_size_mb: Option<u64>,
    threshold_mb: Option<u64>,
) -> axum::response::Response {
    info!("Creating ephemeral pod for large query...");
    
    match k8s_client.execute_query(sql, user_id, memory_mb, cpu_millicores, 300).await {
        Ok(result) => {
            let elapsed = start_time.elapsed();
            
            // Record success metrics
            metrics::record_query_completed("ephemeral_pod", "success", elapsed.as_secs_f64());
            metrics::record_ephemeral_pod_completed(5.0, elapsed.as_secs_f64()); // Assume 5s startup
            
            // Record data size metrics
            let estimated_bytes = result.row_count as u64 * 100; // Assume 100 bytes per row
            let actual_size_mb = (estimated_bytes as f64) / (1024.0 * 1024.0);
            metrics::record_data_scanned(estimated_bytes);
            metrics::record_actual_query_size(actual_size_mb);
            if let Some(est) = estimated_size_mb {
                metrics::record_estimation_accuracy(est as f64, actual_size_mb);
            }
            
            // Record to history for EMA
            adaptive_controller.get_history().record_query(QueryRecord {
                timestamp: Utc::now(),
                estimated_size_mb: estimated_size_mb.unwrap_or(0),
                actual_size_mb: actual_size_mb as u64,
                execution_time_ms: elapsed.as_millis() as u64,
                was_ephemeral: true,
                success: true,
            });
            
            let response = QueryResponse {
                columns: result.columns.iter().map(|(name, _)| name.clone()).collect(),
                column_types: result.columns.iter().map(|(_, type_name)| type_name.clone()).collect(),
                rows: result.rows,
                row_count: result.row_count,
                execution_time_ms: elapsed.as_millis() as u64,
                bytes_scanned: 0,
                execution_mode: Some("ephemeral_pod".to_string()),
                estimated_size_mb,
                threshold_mb,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            warn!("Ephemeral pod failed ({}), falling back to worker pool", e);
            // Record failure
            metrics::record_query_completed("ephemeral_pod", "fallback", start_time.elapsed().as_secs_f64());
            
            // Record failure to history
            adaptive_controller.get_history().record_query(QueryRecord {
                timestamp: Utc::now(),
                estimated_size_mb: estimated_size_mb.unwrap_or(0),
                actual_size_mb: 0,
                execution_time_ms: start_time.elapsed().as_millis() as u64,
                was_ephemeral: true,
                success: false,
            });
            
            // Fallback to worker pool
            execute_via_worker_pool(worker_client, adaptive_controller, sql, user_id, start_time, estimated_size_mb, threshold_mb).await
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

/// Adaptive state endpoint
pub async fn adaptive_state(State(state): State<AppState>) -> impl IntoResponse {
    let adaptive_state = state.adaptive_controller.get_state().await;
    
    let response = serde_json::json!({
        "threshold_mb": adaptive_state.threshold_mb,
        "threshold_gb": adaptive_state.threshold_mb as f64 / 1024.0,
        "hpa_min": adaptive_state.hpa_min,
        "hpa_max": adaptive_state.hpa_max,
        "factors": {
            "time": adaptive_state.factors.time_factor,
            "day": adaptive_state.factors.day_factor,
            "load": adaptive_state.factors.load_factor,
            "event": adaptive_state.factors.event_factor,
            "combined_threshold": adaptive_state.factors.combined_threshold_factor,
            "combined_hpa": adaptive_state.factors.combined_hpa_factor,
        },
        "active_events": adaptive_state.active_events,
        "cluster_metrics": {
            "cpu_utilization": adaptive_state.cluster_metrics.cpu_utilization,
            "memory_utilization": adaptive_state.cluster_metrics.memory_utilization,
            "ready_replicas": adaptive_state.cluster_metrics.ready_replicas,
            "desired_replicas": adaptive_state.cluster_metrics.desired_replicas,
        }
    });
    
    (StatusCode::OK, Json(response))
}

/// Export request for large query results (10TB+)
#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    pub sql: String,
    pub output_path: String, // e.g., "s3://bucket/path/output.parquet"
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
/// 
/// For 10TB+ queries where streaming results to client isn't feasible,
/// export directly to object storage.
pub async fn export_query(
    State(state): State<AppState>,
    Json(request): Json<ExportRequest>,
) -> impl IntoResponse {
    info!("Export request to {}: {}", request.output_path, request.sql);
    
    let start = std::time::Instant::now();
    
    // Build COPY TO query
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
    
    // Execute export via worker
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
    "Tavana Gateway - Cloud-Agnostic DuckDB Query Platform (Supports up to 10TB queries)"
}
