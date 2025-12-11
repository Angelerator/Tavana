//! HPA Manager
//!
//! Dynamically updates Kubernetes HPA min/max replicas based on adaptive state

use anyhow::{Context, Result};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use kube::{
    api::{Api, Patch, PatchParams},
    Client,
};
use serde_json::json;
use tracing::{info, warn};

/// HPA Manager for dynamic replica adjustment
pub struct HpaManager {
    client: Option<Client>,
    namespace: String,
    hpa_name: String,
}

impl HpaManager {
    /// Create a new HPA manager
    pub async fn new(namespace: String, hpa_name: String) -> Self {
        let client = match Client::try_default().await {
            Ok(c) => {
                info!("HPA Manager initialized for {}/{}", namespace, hpa_name);
                Some(c)
            }
            Err(e) => {
                warn!("K8s client not available for HPA management: {}", e);
                None
            }
        };

        Self {
            client,
            namespace,
            hpa_name,
        }
    }

    /// Check if HPA manager is available
    pub fn is_available(&self) -> bool {
        self.client.is_some()
    }

    /// Update HPA min and max replicas
    pub async fn update_replicas(&self, min_replicas: u32, max_replicas: u32) -> Result<()> {
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("K8s client not available"))?;

        let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(client.clone(), &self.namespace);

        // Create patch
        let patch = json!({
            "spec": {
                "minReplicas": min_replicas,
                "maxReplicas": max_replicas
            }
        });

        let params = PatchParams::apply("tavana-adaptive");
        
        hpas.patch(&self.hpa_name, &params, &Patch::Merge(&patch))
            .await
            .context("Failed to patch HPA")?;

        info!(
            "Updated HPA {}/{}: min={}, max={}",
            self.namespace, self.hpa_name, min_replicas, max_replicas
        );

        Ok(())
    }

    /// Get current HPA state
    pub async fn get_current_state(&self) -> Result<HpaState> {
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("K8s client not available"))?;

        let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(client.clone(), &self.namespace);
        let hpa = hpas.get(&self.hpa_name).await.context("Failed to get HPA")?;

        let spec = hpa.spec.unwrap_or_default();
        let status = hpa.status.unwrap_or_default();

        Ok(HpaState {
            min_replicas: spec.min_replicas.unwrap_or(1) as u32,
            max_replicas: spec.max_replicas as u32,
            current_replicas: status.current_replicas.unwrap_or(0) as u32,
            desired_replicas: status.desired_replicas as u32,
        })
    }
}

/// Current HPA state
#[derive(Debug, Clone)]
pub struct HpaState {
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub current_replicas: u32,
    pub desired_replicas: u32,
}

impl Default for HpaState {
    fn default() -> Self {
        Self {
            min_replicas: 2,
            max_replicas: 10,
            current_replicas: 2,
            desired_replicas: 2,
        }
    }
}

