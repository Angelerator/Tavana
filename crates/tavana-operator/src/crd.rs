//! Kubernetes Custom Resource Definitions for Tavana

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// DuckDBQuery Custom Resource
/// 
/// This CRD represents a query execution request. The operator watches
/// for new DuckDBQuery resources and creates appropriately-sized pods.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[kube(
    group = "tavana.io",
    version = "v1",
    kind = "DuckDBQuery",
    plural = "duckdbqueries",
    shortname = "ddq",
    status = "DuckDBQueryStatus",
    namespaced,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Pod","type":"string","jsonPath":".status.podName"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct DuckDBQuerySpec {
    /// SQL query to execute
    pub sql: String,
    
    /// User ID who submitted the query
    pub user_id: String,
    
    /// Tenant ID for multi-tenancy
    pub tenant_id: String,
    
    /// Query timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u32,
    
    /// Resource requirements
    pub resources: QueryResources,
    
    /// Storage configuration for accessing data
    #[serde(default)]
    pub storage_config: StorageConfig,
}

fn default_timeout() -> u32 {
    300 // 5 minutes default
}

/// Resource requirements for the query
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResources {
    /// Memory in MB
    pub memory_mb: u32,
    
    /// CPU in millicores
    pub cpu_millicores: u32,
}

/// Storage configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageConfig {
    /// S3 configuration
    pub s3: Option<S3Config>,
    
    /// Azure configuration
    pub azure: Option<AzureConfig>,
    
    /// GCS configuration
    pub gcs: Option<GcsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3Config {
    pub region: String,
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureConfig {
    pub account_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GcsConfig {
    pub project_id: String,
}

/// Status of a DuckDB query
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DuckDBQueryStatus {
    /// Current phase of the query
    pub phase: QueryPhase,
    
    /// Name of the worker pod
    pub pod_name: Option<String>,
    
    /// Worker node the pod is running on
    pub node_name: Option<String>,
    
    /// Location of query results
    pub result_location: Option<String>,
    
    /// Start time of query execution
    pub start_time: Option<String>,
    
    /// Completion time
    pub completion_time: Option<String>,
    
    /// Error message if failed
    pub error_message: Option<String>,
    
    /// Execution metrics
    pub metrics: Option<QueryMetrics>,
}

/// Query execution phase
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
pub enum QueryPhase {
    #[default]
    Pending,
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Query execution metrics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryMetrics {
    pub execution_time_ms: u64,
    pub rows_returned: u64,
    pub bytes_scanned: u64,
    pub peak_memory_bytes: u64,
}

