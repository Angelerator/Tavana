//! Tenant-Dedicated Worker Pool Manager
//!
//! Provides tenant isolation with dedicated worker pools:
//! - Each tenant gets their own set of workers
//! - Workers cache tenant-specific data (fast!)
//! - HPA scales each tenant pool independently
//! - VPA optimizes resources per tenant's query patterns
//!
//! Routing rules:
//! - Tenant with dedicated pool → Route to their workers
//! - Small queries from authenticated users → Shared pool
//! - Large queries (>1GB) → Tenant's dedicated pool
//! - Anonymous/public queries → Shared pool only

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, ListParams, PostParams, DeleteParams},
    Client,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::metrics;

/// Configuration for tenant pool management
#[derive(Clone, Debug)]
pub struct TenantPoolConfig {
    /// Namespace for tenant pools
    pub namespace: String,
    /// Base worker image
    pub worker_image: String,
    /// Minimum workers per tenant pool (default: 1)
    pub min_workers: i32,
    /// Maximum workers per tenant pool (default: 20)
    pub max_workers: i32,
    /// Minimum memory per worker (MB)
    pub min_memory_mb: u64,
    /// Maximum memory per worker (MB)
    pub max_memory_mb: u64,
    /// Query size threshold for dedicated pool (MB) - queries larger than this go to dedicated pool
    pub dedicated_threshold_mb: u64,
    /// Idle timeout before scaling tenant pool to 0 (seconds)
    pub idle_timeout_secs: u64,
    /// Whether to auto-create tenant pools on first query
    pub auto_create_pools: bool,
}

impl Default for TenantPoolConfig {
    fn default() -> Self {
        Self {
            namespace: "tavana".to_string(),
            worker_image: "tavana-worker:duckdb14".to_string(),
            min_workers: 1,
            max_workers: 20,
            min_memory_mb: 256,
            max_memory_mb: 100 * 1024, // 100GB per tenant
            dedicated_threshold_mb: 1024, // 1GB - queries larger go to dedicated pool
            idle_timeout_secs: 300, // 5 minutes
            auto_create_pools: true,
        }
    }
}

impl TenantPoolConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(v) = std::env::var("TENANT_MIN_WORKERS") {
            if let Ok(n) = v.parse() {
                config.min_workers = n;
            }
        }
        
        if let Ok(v) = std::env::var("TENANT_MAX_WORKERS") {
            if let Ok(n) = v.parse() {
                config.max_workers = n;
            }
        }
        
        if let Ok(v) = std::env::var("TENANT_DEDICATED_THRESHOLD_MB") {
            if let Ok(n) = v.parse() {
                config.dedicated_threshold_mb = n;
            }
        }
        
        if let Ok(v) = std::env::var("TENANT_IDLE_TIMEOUT_SECS") {
            if let Ok(n) = v.parse() {
                config.idle_timeout_secs = n;
            }
        }
        
        if let Ok(v) = std::env::var("TENANT_AUTO_CREATE") {
            config.auto_create_pools = v.to_lowercase() == "true";
        }
        
        config
    }
}

/// Represents a tenant's dedicated worker pool
#[derive(Clone, Debug)]
pub struct TenantPool {
    /// Tenant identifier
    pub tenant_id: String,
    /// Deployment name
    pub deployment_name: String,
    /// Service name
    pub service_name: String,
    /// Service address (for routing)
    pub service_addr: String,
    /// Current number of workers
    pub current_workers: i32,
    /// Last activity time
    pub last_activity: Instant,
    /// Total queries processed
    pub queries_processed: u64,
    /// Whether pool is active (has workers)
    pub is_active: bool,
}

/// Routing decision for a query
#[derive(Debug, Clone)]
pub enum TenantRouting {
    /// Route to tenant's dedicated pool
    DedicatedPool {
        tenant_id: String,
        service_addr: String,
    },
    /// Route to shared worker pool
    SharedPool {
        reason: String,
    },
}

/// Manages tenant-dedicated worker pools
pub struct TenantPoolManager {
    /// K8s client
    client: Client,
    /// Configuration
    config: TenantPoolConfig,
    /// Active tenant pools
    pools: Arc<RwLock<HashMap<String, TenantPool>>>,
    /// Worker gRPC port
    worker_port: u16,
}

impl TenantPoolManager {
    /// Create a new TenantPoolManager
    pub async fn new(config: TenantPoolConfig) -> Result<Self, kube::Error> {
        let client = Client::try_default().await?;
        
        let manager = Self {
            client,
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            worker_port: 50053,
        };
        
        // Load existing tenant pools from K8s
        manager.discover_existing_pools().await?;
        
        Ok(manager)
    }
    
    /// Discover existing tenant pools in K8s
    async fn discover_existing_pools(&self) -> Result<(), kube::Error> {
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let lp = ListParams::default().labels("tavana.io/tenant-pool=true");
        
        let deploy_list = deployments.list(&lp).await?;
        
        let mut pools = self.pools.write().await;
        
        for deploy in deploy_list.items {
            if let Some(name) = deploy.metadata.name {
                if let Some(labels) = deploy.metadata.labels {
                    if let Some(tenant_id) = labels.get("tavana.io/tenant-id") {
                        let service_name = format!("worker-{}", tenant_id);
                        let service_addr = format!(
                            "http://{}.{}.svc.cluster.local:{}",
                            service_name, self.config.namespace, self.worker_port
                        );
                        
                        let current_workers = deploy.spec
                            .and_then(|s| s.replicas)
                            .unwrap_or(0);
                        
                        pools.insert(tenant_id.clone(), TenantPool {
                            tenant_id: tenant_id.clone(),
                            deployment_name: name,
                            service_name,
                            service_addr,
                            current_workers,
                            last_activity: Instant::now(),
                            queries_processed: 0,
                            is_active: current_workers > 0,
                        });
                        
                        info!("Discovered existing tenant pool: {}", tenant_id);
                    }
                }
            }
        }
        
        info!("Discovered {} existing tenant pools", pools.len());
        Ok(())
    }
    
    /// Determine routing for a query based on tenant and size
    pub async fn route_query(
        &self,
        tenant_id: Option<&str>,
        query_size_mb: u64,
    ) -> TenantRouting {
        // No tenant = shared pool
        let tenant_id = match tenant_id {
            Some(id) if !id.is_empty() => id,
            _ => {
                return TenantRouting::SharedPool {
                    reason: "No tenant ID provided".to_string(),
                };
            }
        };
        
        // Check if tenant has a pool
        let service_addr = {
            let pools = self.pools.read().await;
            pools.get(tenant_id).map(|p| p.service_addr.clone())
        };
        
        if let Some(addr) = service_addr {
            // Tenant has pool - use it for all their queries (cache benefit)
            self.update_activity(tenant_id).await;
            
            return TenantRouting::DedicatedPool {
                tenant_id: tenant_id.to_string(),
                service_addr: addr,
            };
        }
        
        // No pool exists - should we create one?
        if self.config.auto_create_pools && query_size_mb >= self.config.dedicated_threshold_mb {
            // Large query from new tenant - create dedicated pool
            match self.create_tenant_pool(tenant_id).await {
                Ok(pool) => {
                    return TenantRouting::DedicatedPool {
                        tenant_id: tenant_id.to_string(),
                        service_addr: pool.service_addr,
                    };
                }
                Err(e) => {
                    warn!("Failed to create tenant pool for {}: {}. Using shared pool.", tenant_id, e);
                }
            }
        }
        
        // Default to shared pool for small queries or failed pool creation
        TenantRouting::SharedPool {
            reason: format!("Query size {}MB < threshold {}MB", query_size_mb, self.config.dedicated_threshold_mb),
        }
    }
    
    /// Create a new dedicated pool for a tenant
    pub async fn create_tenant_pool(&self, tenant_id: &str) -> Result<TenantPool, anyhow::Error> {
        let sanitized_id = sanitize_k8s_name(tenant_id);
        let deployment_name = format!("worker-{}", sanitized_id);
        let service_name = format!("worker-{}", sanitized_id);
        let hpa_name = format!("worker-{}-hpa", sanitized_id);
        
        info!("Creating dedicated pool for tenant: {}", tenant_id);
        
        // Create Deployment
        let deployment = self.build_tenant_deployment(tenant_id, &deployment_name);
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.config.namespace);
        deployments.create(&PostParams::default(), &deployment).await?;
        
        // Create Service
        let service = self.build_tenant_service(tenant_id, &service_name, &deployment_name);
        let services: Api<Service> = Api::namespaced(self.client.clone(), &self.config.namespace);
        services.create(&PostParams::default(), &service).await?;
        
        // Create HPA
        let hpa = self.build_tenant_hpa(tenant_id, &hpa_name, &deployment_name);
        let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(self.client.clone(), &self.config.namespace);
        hpas.create(&PostParams::default(), &hpa).await?;
        
        let service_addr = format!(
            "http://{}.{}.svc.cluster.local:{}",
            service_name, self.config.namespace, self.worker_port
        );
        
        let pool = TenantPool {
            tenant_id: tenant_id.to_string(),
            deployment_name,
            service_name,
            service_addr: service_addr.clone(),
            current_workers: self.config.min_workers,
            last_activity: Instant::now(),
            queries_processed: 0,
            is_active: true,
        };
        
        // Store in cache
        let mut pools = self.pools.write().await;
        pools.insert(tenant_id.to_string(), pool.clone());
        
        info!("Created dedicated pool for tenant {} at {}", tenant_id, service_addr);
        metrics::record_tenant_pool_created(tenant_id);
        
        Ok(pool)
    }
    
    /// Build tenant-specific Deployment
    fn build_tenant_deployment(&self, tenant_id: &str, name: &str) -> Deployment {
        use k8s_openapi::api::core::v1::{Container, ContainerPort, EnvVar, ResourceRequirements, PodSpec, PodTemplateSpec};
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
        use std::collections::BTreeMap;
        
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), format!("tavana-worker-{}", sanitize_k8s_name(tenant_id)));
        labels.insert("tavana.io/tenant-pool".to_string(), "true".to_string());
        labels.insert("tavana.io/tenant-id".to_string(), tenant_id.to_string());
        
        let mut requests = BTreeMap::new();
        requests.insert("memory".to_string(), Quantity(format!("{}Mi", self.config.min_memory_mb)));
        requests.insert("cpu".to_string(), Quantity("100m".to_string()));
        
        let mut limits = BTreeMap::new();
        limits.insert("memory".to_string(), Quantity(format!("{}Gi", self.config.max_memory_mb / 1024)));
        limits.insert("cpu".to_string(), Quantity("16".to_string()));
        
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.config.namespace.clone()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(self.config.min_workers),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some(labels.clone()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        termination_grace_period_seconds: Some(600),
                        containers: vec![Container {
                            name: "worker".to_string(),
                            image: Some(self.config.worker_image.clone()),
                            image_pull_policy: Some("Never".to_string()),
                            ports: Some(vec![ContainerPort {
                                container_port: self.worker_port as i32,
                                ..Default::default()
                            }]),
                            env: Some(vec![
                                EnvVar {
                                    name: "TENANT_ID".to_string(),
                                    value: Some(tenant_id.to_string()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "LOG_LEVEL".to_string(),
                                    value: Some("info".to_string()),
                                    ..Default::default()
                                },
                            ]),
                            resources: Some(ResourceRequirements {
                                requests: Some(requests),
                                limits: Some(limits),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    
    /// Build tenant-specific Service
    fn build_tenant_service(&self, tenant_id: &str, name: &str, deployment_name: &str) -> Service {
        use std::collections::BTreeMap;
        
        let mut labels = BTreeMap::new();
        labels.insert("tavana.io/tenant-pool".to_string(), "true".to_string());
        labels.insert("tavana.io/tenant-id".to_string(), tenant_id.to_string());
        
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), format!("tavana-worker-{}", sanitize_k8s_name(tenant_id)));
        
        Service {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.config.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(selector),
                ports: Some(vec![ServicePort {
                    port: self.worker_port as i32,
                    target_port: Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(self.worker_port as i32)),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    
    /// Build tenant-specific HPA
    fn build_tenant_hpa(&self, tenant_id: &str, name: &str, deployment_name: &str) -> HorizontalPodAutoscaler {
        use k8s_openapi::api::autoscaling::v2::{
            HorizontalPodAutoscalerSpec, CrossVersionObjectReference,
            MetricSpec, ResourceMetricSource, MetricTarget,
        };
        use std::collections::BTreeMap;
        
        let mut labels = BTreeMap::new();
        labels.insert("tavana.io/tenant-pool".to_string(), "true".to_string());
        labels.insert("tavana.io/tenant-id".to_string(), tenant_id.to_string());
        
        HorizontalPodAutoscaler {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.config.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(HorizontalPodAutoscalerSpec {
                scale_target_ref: CrossVersionObjectReference {
                    api_version: Some("apps/v1".to_string()),
                    kind: "Deployment".to_string(),
                    name: deployment_name.to_string(),
                },
                min_replicas: Some(self.config.min_workers),
                max_replicas: self.config.max_workers,
                metrics: Some(vec![
                    MetricSpec {
                        type_: "Resource".to_string(),
                        resource: Some(ResourceMetricSource {
                            name: "memory".to_string(),
                            target: MetricTarget {
                                type_: "Utilization".to_string(),
                                average_utilization: Some(80),
                                ..Default::default()
                            },
                        }),
                        ..Default::default()
                    },
                    MetricSpec {
                        type_: "Resource".to_string(),
                        resource: Some(ResourceMetricSource {
                            name: "cpu".to_string(),
                            target: MetricTarget {
                                type_: "Utilization".to_string(),
                                average_utilization: Some(70),
                                ..Default::default()
                            },
                        }),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    
    /// Update last activity for a tenant
    async fn update_activity(&self, tenant_id: &str) {
        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(tenant_id) {
            pool.last_activity = Instant::now();
            pool.queries_processed += 1;
        }
    }
    
    /// Delete a tenant pool (scale to 0 or remove)
    pub async fn delete_tenant_pool(&self, tenant_id: &str) -> Result<(), anyhow::Error> {
        let pools = self.pools.read().await;
        let pool = pools.get(tenant_id)
            .ok_or_else(|| anyhow::anyhow!("Tenant pool not found"))?;
        
        let deployment_name = pool.deployment_name.clone();
        let service_name = pool.service_name.clone();
        let hpa_name = format!("worker-{}-hpa", sanitize_k8s_name(tenant_id));
        
        drop(pools);
        
        // Delete HPA
        let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(self.client.clone(), &self.config.namespace);
        hpas.delete(&hpa_name, &DeleteParams::default()).await.ok();
        
        // Delete Service
        let services: Api<Service> = Api::namespaced(self.client.clone(), &self.config.namespace);
        services.delete(&service_name, &DeleteParams::default()).await.ok();
        
        // Delete Deployment
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.config.namespace);
        deployments.delete(&deployment_name, &DeleteParams::default()).await.ok();
        
        // Remove from cache
        let mut pools = self.pools.write().await;
        pools.remove(tenant_id);
        
        info!("Deleted tenant pool for {}", tenant_id);
        metrics::record_tenant_pool_deleted(tenant_id);
        
        Ok(())
    }
    
    /// Cleanup idle tenant pools
    pub async fn cleanup_idle_pools(&self) -> Result<usize, anyhow::Error> {
        let now = Instant::now();
        let idle_timeout = Duration::from_secs(self.config.idle_timeout_secs);
        
        let pools = self.pools.read().await;
        let idle_tenants: Vec<String> = pools
            .iter()
            .filter(|(_, pool)| now.duration_since(pool.last_activity) > idle_timeout)
            .map(|(id, _)| id.clone())
            .collect();
        
        drop(pools);
        
        let mut deleted = 0;
        for tenant_id in idle_tenants {
            if let Ok(()) = self.delete_tenant_pool(&tenant_id).await {
                deleted += 1;
            }
        }
        
        if deleted > 0 {
            info!("Cleaned up {} idle tenant pools", deleted);
        }
        
        Ok(deleted)
    }
    
    /// Get stats about all tenant pools
    pub async fn get_stats(&self) -> TenantPoolStats {
        let pools = self.pools.read().await;
        
        TenantPoolStats {
            total_pools: pools.len(),
            active_pools: pools.values().filter(|p| p.is_active).count(),
            total_workers: pools.values().map(|p| p.current_workers as usize).sum(),
            total_queries: pools.values().map(|p| p.queries_processed).sum(),
        }
    }
    
    /// List all tenant pools
    pub async fn list_pools(&self) -> Vec<TenantPool> {
        let pools = self.pools.read().await;
        pools.values().cloned().collect()
    }
}

/// Stats about tenant pools
#[derive(Debug, Clone)]
pub struct TenantPoolStats {
    pub total_pools: usize,
    pub active_pools: usize,
    pub total_workers: usize,
    pub total_queries: u64,
}

/// Sanitize tenant ID for use in K8s resource names
fn sanitize_k8s_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
        .chars()
        .take(63) // K8s name limit
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sanitize_k8s_name() {
        assert_eq!(sanitize_k8s_name("tenant-123"), "tenant-123");
        assert_eq!(sanitize_k8s_name("Tenant_ABC"), "tenant-abc");
        assert_eq!(sanitize_k8s_name("user@example.com"), "user-example-com");
        assert_eq!(sanitize_k8s_name("--test--"), "test");
    }
}

