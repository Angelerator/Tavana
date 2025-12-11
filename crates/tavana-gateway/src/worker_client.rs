//! Worker gRPC client for forwarding queries to DuckDB workers

use std::sync::Arc;
use tavana_common::proto;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info};
use uuid::Uuid;

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

        // Need to connect
        info!("Connecting to worker at {}", self.worker_addr);
        let client =
            proto::query_service_client::QueryServiceClient::connect(self.worker_addr.clone())
                .await?;

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
                max_rows: 10000,
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
                    // Decode JSON data
                    if !batch.data.is_empty() {
                        if let Ok(batch_rows) = serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                            rows.extend(batch_rows);
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

