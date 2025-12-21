//! Worker gRPC client for forwarding queries to DuckDB workers
//! Supports both buffered and streaming execution modes

use std::sync::Arc;
use tavana_common::proto;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Streaming batch types for true row-by-row streaming
pub enum StreamingBatch {
    Metadata { columns: Vec<String>, column_types: Vec<String> },
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
            .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
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
                max_rows: 0,  // 0 = unlimited rows (streaming)
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

    /// Execute a query with TRUE STREAMING - returns results as they arrive
    /// This is the preferred method for large result sets as it never buffers everything
    pub async fn execute_query_streaming(
        &self,
        sql: &str,
        user_id: &str,
    ) -> Result<StreamingResult, anyhow::Error> {
        let mut client = self.get_client().await?;

        let query_id = Uuid::new_v4().to_string();
        debug!("Executing streaming query {} for user {}", query_id, user_id);

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
                max_rows: 0,  // 0 = unlimited rows (streaming)
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
                            match serde_json::from_slice::<Vec<Vec<String>>>(&batch.data) {
                                Ok(rows) => Ok(StreamingBatch::Rows(rows)),
                                Err(e) => Err(anyhow::anyhow!("Failed to decode batch: {}", e)),
                            }
                        } else {
                            continue;
                        }
                    }
                    Some(proto::query_result_batch::Result::Error(err)) => {
                        Ok(StreamingBatch::Error(format!("{}: {}", err.code, err.message)))
                    }
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

