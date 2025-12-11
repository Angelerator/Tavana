//! Kubernetes reconciliation logic for DuckDBQuery resources

use crate::crd::{DuckDBQuery, DuckDBQueryStatus, QueryPhase};
use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, ResourceRequirements, SecurityContext};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{
    api::{Api, Patch, PatchParams, PostParams},
    runtime::controller::{Action, Controller},
    Client, Resource, ResourceExt,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, instrument};

/// Reconciler context
pub struct Context {
    pub client: Client,
    pub namespace: String,
    pub worker_image: String,
}

/// Reconcile a DuckDBQuery resource
#[instrument(skip(ctx, query), fields(name = %query.name_any()))]
pub async fn reconcile(
    query: Arc<DuckDBQuery>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = query.name_any();
    let namespace = query.namespace().unwrap_or(ctx.namespace.clone());
    
    info!("Reconciling DuckDBQuery: {}", name);
    
    let queries: Api<DuckDBQuery> = Api::namespaced(ctx.client.clone(), &namespace);
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &namespace);
    
    // Get current status
    let status = query.status.clone().unwrap_or_default();
    
    match status.phase {
        QueryPhase::Pending => {
            // Create worker pod
            let pod = create_worker_pod(&query, &ctx.worker_image, &namespace);
            
            match pods.create(&PostParams::default(), &pod).await {
                Ok(created_pod) => {
                    info!("Created worker pod: {}", created_pod.name_any());
                    
                    // Update status to Scheduled
                    let new_status = DuckDBQueryStatus {
                        phase: QueryPhase::Scheduled,
                        pod_name: Some(created_pod.name_any()),
                        start_time: Some(chrono::Utc::now().to_rfc3339()),
                        ..status
                    };
                    update_status(&queries, &name, new_status).await?;
                }
                Err(e) => {
                    error!("Failed to create pod: {}", e);
                    let new_status = DuckDBQueryStatus {
                        phase: QueryPhase::Failed,
                        error_message: Some(format!("Failed to create pod: {}", e)),
                        ..status
                    };
                    update_status(&queries, &name, new_status).await?;
                }
            }
        }
        QueryPhase::Scheduled | QueryPhase::Running => {
            // Check pod status
            if let Some(pod_name) = &status.pod_name {
                match pods.get(pod_name).await {
                    Ok(pod) => {
                        if let Some(pod_status) = pod.status {
                            let phase = pod_status.phase.unwrap_or_default();
                            
                            match phase.as_str() {
                                "Running" => {
                                    if status.phase != QueryPhase::Running {
                                        let new_status = DuckDBQueryStatus {
                                            phase: QueryPhase::Running,
                                            node_name: pod_status.host_ip,
                                            ..status
                                        };
                                        update_status(&queries, &name, new_status).await?;
                                    }
                                }
                                "Succeeded" => {
                                    let new_status = DuckDBQueryStatus {
                                        phase: QueryPhase::Succeeded,
                                        completion_time: Some(chrono::Utc::now().to_rfc3339()),
                                        ..status
                                    };
                                    update_status(&queries, &name, new_status).await?;
                                }
                                "Failed" => {
                                    let error_msg = pod_status
                                        .container_statuses
                                        .and_then(|cs| cs.first().cloned())
                                        .and_then(|c| c.state)
                                        .and_then(|s| s.terminated)
                                        .and_then(|t| t.message)
                                        .unwrap_or_else(|| "Unknown error".to_string());
                                    
                                    let new_status = DuckDBQueryStatus {
                                        phase: QueryPhase::Failed,
                                        error_message: Some(error_msg),
                                        completion_time: Some(chrono::Utc::now().to_rfc3339()),
                                        ..status
                                    };
                                    update_status(&queries, &name, new_status).await?;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to get pod: {}", e);
                    }
                }
            }
        }
        QueryPhase::Succeeded | QueryPhase::Failed | QueryPhase::Cancelled => {
            // Clean up pod after some time
            // TODO: Implement cleanup logic
        }
    }
    
    // Requeue after 5 seconds
    Ok(Action::requeue(Duration::from_secs(5)))
}

/// Create a worker pod for executing the query
fn create_worker_pod(query: &DuckDBQuery, image: &str, namespace: &str) -> Pod {
    let name = format!("duckdb-worker-{}", query.name_any());
    let resources = &query.spec.resources;
    
    let mut resource_limits = BTreeMap::new();
    resource_limits.insert("memory".to_string(), Quantity(format!("{}Mi", resources.memory_mb)));
    resource_limits.insert("cpu".to_string(), Quantity(format!("{}m", resources.cpu_millicores)));
    
    let mut resource_requests = BTreeMap::new();
    resource_requests.insert("memory".to_string(), Quantity(format!("{}Mi", resources.memory_mb / 2)));
    resource_requests.insert("cpu".to_string(), Quantity(format!("{}m", resources.cpu_millicores / 2)));
    
    Pod {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            labels: Some({
                let mut labels = BTreeMap::new();
                labels.insert("app".to_string(), "tavana-worker".to_string());
                labels.insert("query".to_string(), query.name_any());
                labels.insert("tavana.io/component".to_string(), "worker".to_string());
                labels
            }),
            owner_references: Some(vec![query.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
            containers: vec![Container {
                name: "duckdb".to_string(),
                image: Some(image.to_string()),
                image_pull_policy: Some("Never".to_string()),
                // No args needed - worker uses QUERY_SQL env var in one-shot mode
                env: Some(vec![
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "QUERY_SQL".to_string(),
                        value: Some(query.spec.sql.clone()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "QUERY_TIMEOUT".to_string(),
                        value: Some(query.spec.timeout_seconds.to_string()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "QUERY_ID".to_string(),
                        value: Some(query.name_any()),
                        ..Default::default()
                    },
                ]),
                env_from: Some(vec![
                    k8s_openapi::api::core::v1::EnvFromSource {
                        config_map_ref: Some(k8s_openapi::api::core::v1::ConfigMapEnvSource {
                            name: "tavana-config".to_string(),
                            optional: Some(true),
                        }),
                        ..Default::default()
                    },
                ]),
                resources: Some(ResourceRequirements {
                    limits: Some(resource_limits),
                    requests: Some(resource_requests),
                    ..Default::default()
                }),
                security_context: Some(SecurityContext {
                    run_as_non_root: Some(true),
                    run_as_user: Some(1000),
                    read_only_root_filesystem: Some(true),
                    allow_privilege_escalation: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                fs_group: Some(1000),
                run_as_non_root: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Update the status of a DuckDBQuery
async fn update_status(
    api: &Api<DuckDBQuery>,
    name: &str,
    status: DuckDBQueryStatus,
) -> Result<(), kube::Error> {
    let patch = serde_json::json!({
        "status": status
    });
    
    api.patch_status(
        name,
        &PatchParams::apply("tavana-operator"),
        &Patch::Merge(&patch),
    )
    .await?;
    
    Ok(())
}

/// Error handler for the controller
pub fn error_policy(
    _query: Arc<DuckDBQuery>,
    error: &kube::Error,
    _ctx: Arc<Context>,
) -> Action {
    error!("Reconciliation error: {}", error);
    Action::requeue(Duration::from_secs(30))
}

