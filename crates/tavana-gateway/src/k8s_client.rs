//! Kubernetes client for creating ephemeral DuckDB pods
//!
//! Creates DuckDBQuery CRDs for queries >= 6GB threshold

use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, DeleteParams, LogParams, PostParams},
    Client, CustomResource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// DuckDBQuery spec matching the CRD
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "tavana.io",
    version = "v1",
    kind = "DuckDBQuery",
    namespaced,
    status = "DuckDBQueryStatus"
)]
pub struct DuckDBQuerySpec {
    pub sql: String,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "tenantId")]
    pub tenant_id: String,
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: u32,
    pub resources: QueryResources,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct QueryResources {
    #[serde(rename = "memoryMb")]
    pub memory_mb: u32,
    #[serde(rename = "cpuMillicores")]
    pub cpu_millicores: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DuckDBQueryStatus {
    pub phase: String,
    pub result: Option<String>,
    pub error: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<String>,
    #[serde(rename = "endTime")]
    pub end_time: Option<String>,
}

/// Result from ephemeral pod execution
#[derive(Debug, Clone)]
pub struct EphemeralQueryResult {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
}

/// Kubernetes client for ephemeral pod management
pub struct K8sQueryClient {
    client: Option<Client>,
    namespace: String,
}

impl K8sQueryClient {
    /// Create a new K8s client (will be None if not in cluster)
    pub async fn new(namespace: String) -> Self {
        let client = match Client::try_default().await {
            Ok(c) => {
                info!("Kubernetes client initialized for namespace: {}", namespace);
                Some(c)
            }
            Err(e) => {
                warn!("Kubernetes client not available (not in cluster?): {}", e);
                None
            }
        };
        Self { client, namespace }
    }

    /// Check if K8s is available
    pub fn is_available(&self) -> bool {
        self.client.is_some()
    }

    /// Execute a query via ephemeral pod
    pub async fn execute_query(
        &self,
        sql: &str,
        user_id: &str,
        memory_mb: u32,
        cpu_millicores: u32,
        timeout_seconds: u32,
    ) -> Result<EphemeralQueryResult> {
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kubernetes not available"))?;

        let query_id = format!("query-{}", Uuid::new_v4().to_string()[..8].to_string());
        info!(
            "Creating ephemeral pod for query {}: {}MB RAM, {}m CPU",
            query_id, memory_mb, cpu_millicores
        );

        // Create the DuckDBQuery CRD
        let queries: Api<DuckDBQuery> = Api::namespaced(client.clone(), &self.namespace);
        
        let query_cr = DuckDBQuery::new(&query_id, DuckDBQuerySpec {
            sql: sql.to_string(),
            user_id: user_id.to_string(),
            tenant_id: "default".to_string(),
            timeout_seconds,
            resources: QueryResources {
                memory_mb,
                cpu_millicores,
            },
        });

        // Create the CR
        let pp = PostParams::default();
        let _created = queries.create(&pp, &query_cr).await?;
        info!("Created DuckDBQuery CR: {}", query_id);

        // Wait for completion
        let result = self.wait_for_completion(&queries, &query_id, timeout_seconds).await;

        // Cleanup - delete the CR
        let dp = DeleteParams::default();
        if let Err(e) = queries.delete(&query_id, &dp).await {
            warn!("Failed to cleanup CR {}: {}", query_id, e);
        }

        result
    }

    /// Wait for the query to complete and get results
    /// 
    /// This watches the pod directly instead of relying on the operator's CR status update,
    /// which can be slow (5s reconciliation interval).
    async fn wait_for_completion(
        &self,
        queries: &Api<DuckDBQuery>,
        query_id: &str,
        timeout_seconds: u32,
    ) -> Result<EphemeralQueryResult> {
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kubernetes not available"))?;
        
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds as u64);
        let pod_name = format!("duckdb-worker-{}", query_id);
        let pods: Api<Pod> = Api::namespaced(client.clone(), &self.namespace);

        info!("Waiting for pod {} to complete...", pod_name);

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Query timeout after {} seconds", timeout_seconds));
            }

            // Watch the pod directly for faster completion detection
            match pods.get(&pod_name).await {
                Ok(pod) => {
                    if let Some(status) = &pod.status {
                        let phase = status.phase.as_deref().unwrap_or("Unknown");
                        
                        match phase {
                            "Succeeded" => {
                                info!("Pod {} completed successfully, reading logs...", pod_name);
                                
                                // Read pod logs directly to get the result
                                match pods.logs(&pod_name, &LogParams::default()).await {
                                    Ok(logs) => {
                                        info!("Pod logs length: {} bytes", logs.len());
                                        
                                        // Debug: show first few lines of logs
                                        for (i, line) in logs.lines().enumerate().take(5) {
                                            debug!("Log line {}: {} chars, starts_with='{}...'", 
                                                i, line.len(), &line.chars().take(50).collect::<String>());
                                        }
                                        
                                        // Find the JSON result line (starts with {"columns")
                                        // Note: Worker outputs JSON via println() which should be clean
                                        if let Some(json_line) = logs.lines()
                                            .find(|line| line.trim().starts_with(r#"{"columns"#))
                                        {
                                            info!("Found result JSON: {} bytes", json_line.len());
                                            return self.parse_result(json_line.trim());
                                        }
                                        
                                        // Try to find any JSON line with "columns" key
                                        if let Some(json_line) = logs.lines()
                                            .find(|line| line.trim().starts_with('{') && line.contains(r#""columns""#))
                                        {
                                            info!("Found alternative JSON: {} bytes", json_line.len());
                                            return self.parse_result(json_line.trim());
                                        }
                                        
                                        warn!("No JSON result found in pod logs, sample: {}", 
                                            logs.lines().last().unwrap_or("(empty)").chars().take(100).collect::<String>());
                                        return Ok(EphemeralQueryResult {
                                            columns: vec![],
                                            rows: vec![],
                                            row_count: 0,
                                        });
                                    }
                                    Err(e) => {
                                        error!("Failed to read pod logs: {}", e);
                                        return Err(anyhow::anyhow!("Failed to read pod logs: {}", e));
                                    }
                                }
                            }
                            "Failed" => {
                                // Check container status for error message
                                let error_msg = status.container_statuses
                                    .as_ref()
                                    .and_then(|cs| cs.first())
                                    .and_then(|c| c.state.as_ref())
                                    .and_then(|s| s.terminated.as_ref())
                                    .and_then(|t| t.message.clone())
                                    .unwrap_or_else(|| "Pod failed".to_string());
                                
                                error!("Pod {} failed: {}", pod_name, error_msg);
                                return Err(anyhow::anyhow!("Query pod failed: {}", error_msg));
                            }
                            _ => {
                                debug!("Pod {} phase: {}", pod_name, phase);
                            }
                        }
                    }
                }
                Err(kube::Error::Api(err)) if err.code == 404 => {
                    // Pod not found yet, check CR status as fallback
                    debug!("Pod {} not found yet, checking CR...", pod_name);
                    if let Ok(query) = queries.get(query_id).await {
                        if let Some(status) = &query.status {
                            if status.phase == "Failed" {
                                let error = status.error.clone().unwrap_or_else(|| "Unknown error".to_string());
                                return Err(anyhow::anyhow!("Query failed: {}", error));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error getting pod status: {}", e);
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Parse JSON result from pod
    fn parse_result(&self, result_json: &str) -> Result<EphemeralQueryResult> {
        #[derive(Deserialize)]
        struct JsonResult {
            columns: Vec<String>,
            column_types: Vec<String>,
            rows: Vec<Vec<String>>,
        }

        let parsed: JsonResult = serde_json::from_str(result_json)?;
        
        let columns: Vec<(String, String)> = parsed.columns
            .into_iter()
            .zip(parsed.column_types.into_iter())
            .collect();
        
        let row_count = parsed.rows.len();

        Ok(EphemeralQueryResult {
            columns,
            rows: parsed.rows,
            row_count,
        })
    }
}

