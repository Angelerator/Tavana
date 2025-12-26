# Tavana Deployment Roadmap

> A phased approach to building a scalable, secure, and customer-agnostic deployment system.

## Table of Contents

1. [Vision](#vision)
2. [Architecture Overview](#architecture-overview)
3. [Phase 1: No Control Plane](#phase-1-no-control-plane)
4. [Phase 2: License Validation](#phase-2-license-validation)
5. [Phase 3: Full Control Plane](#phase-3-full-control-plane)
6. [Technology Decisions](#technology-decisions)
7. [Deployment Scenarios](#deployment-scenarios)
8. [Security Model](#security-model)
9. [Cost Analysis](#cost-analysis)
10. [Implementation Timeline](#implementation-timeline)

---

## Vision

Tavana aims to provide **Databricks-style deployment simplicity** while maintaining:
- **Customer data sovereignty**: Data never leaves customer's cloud
- **Air-gapped support**: Works in network-restricted environments
- **Cloud agnosticism**: Azure, AWS, GCP, and on-prem support
- **Zero vendor lock-in**: Customers can self-manage if needed

### Deployment Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DEPLOYMENT MODEL                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────┐         ┌─────────────────────────────────────────┐    │
│  │   VENDOR SIDE       │         │          CUSTOMER SIDE                  │    │
│  │   (Optional)        │         │          (Always)                       │    │
│  ├─────────────────────┤         ├─────────────────────────────────────────┤    │
│  │                     │         │                                         │    │
│  │  Control Plane      │◄────────│  Tavana Agent                          │    │
│  │  (Phase 2+)         │ HTTPS   │    │                                   │    │
│  │                     │ Outbound│    ▼                                   │    │
│  │  • License check    │ Only    │  ┌─────────────────────────────────┐   │    │
│  │  • Config delivery  │         │  │ Customer's Kubernetes Cluster  │   │    │
│  │  • Usage metrics    │         │  │                                 │   │    │
│  │  • Version updates  │         │  │  ┌─────────┐    ┌───────────┐  │   │    │
│  │                     │         │  │  │ Gateway │───▶│  Workers  │  │   │    │
│  └─────────────────────┘         │  │  └─────────┘    └───────────┘  │   │    │
│                                  │  │       │                        │   │    │
│                                  │  │       ▼                        │   │    │
│                                  │  │  ┌─────────────────────────┐   │   │    │
│                                  │  │  │ Customer's Data Lake    │   │   │    │
│                                  │  │  │ (ADLS, S3, GCS, MinIO)  │   │   │    │
│                                  │  │  └─────────────────────────┘   │   │    │
│                                  │  └─────────────────────────────────┘   │    │
│                                  └─────────────────────────────────────────┘    │
│                                                                                  │
│  KEY PRINCIPLE: Customer data NEVER leaves customer's cloud                     │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture Overview

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **Control Plane** | Vendor Cloud (Optional) | License validation, config delivery, usage tracking |
| **Tavana Agent** | Customer K8s | Manages Tavana data plane, syncs config |
| **Gateway** | Customer K8s | Query routing, smart queuing, autoscaling |
| **Workers** | Customer K8s | Query execution with DuckDB |
| **Admin CLI** | Developer Machine | Customer onboarding, config generation |

### Communication Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           COMMUNICATION FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Phase 1 (No Control Plane):                                                    │
│  ──────────────────────────────                                                 │
│                                                                                  │
│  [Admin CLI] ──generates──▶ [values.yaml] ──helm install──▶ [Customer K8s]     │
│                                                                                  │
│  Phase 2+ (With Control Plane):                                                 │
│  ──────────────────────────────                                                 │
│                                                                                  │
│  [Control Plane]                                                                 │
│       │                                                                          │
│       │ 1. Agent registers (HTTPS outbound)                                     │
│       │ 2. Returns config + license                                             │
│       │ 3. Periodic heartbeats                                                  │
│       │ 4. Usage metrics (optional)                                             │
│       ▼                                                                          │
│  [Agent in Customer K8s]                                                        │
│       │                                                                          │
│       │ Reconciles (Helm/kube-rs)                                               │
│       ▼                                                                          │
│  [Gateway + Workers]                                                             │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: No Control Plane

> **Goal**: Enable customer deployments with zero vendor infrastructure.  
> **Cost**: $0/month  
> **Timeline**: 1-2 weeks  

### What Gets Built

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1 DELIVERABLES                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. tavana-admin CLI (Rust)                                                     │
│     ────────────────────────                                                    │
│     • Generates customer-specific Helm values                                   │
│     • Validates customer environment (ACR access, storage, etc.)                │
│     • No backend required                                                        │
│                                                                                  │
│  2. Helm Chart (Enhanced)                                                        │
│     ────────────────────────                                                    │
│     • Fully parameterized for any cloud                                         │
│     • OCI-hosted (ghcr.io/angelerator/charts/tavana)                           │
│     • Includes all security configs                                             │
│                                                                                  │
│  3. Documentation                                                                │
│     ───────────────                                                             │
│     • Step-by-step guides for each cloud                                        │
│     • Troubleshooting guide                                                     │
│     • Architecture deep-dive                                                    │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### CLI Usage

```bash
# Step 1: Generate customer values (you run this)
tavana-admin generate \
  --customer-name "Acme Corp" \
  --cloud azure \
  --acr acme.azurecr.io \
  --storage-account adlsacme \
  --container datalake \
  --namespace tavana \
  --output deployments/acme/values.yaml

# Step 2: Customer runs Helm install
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  -f values.yaml \
  -n tavana \
  --create-namespace
```

### Generated values.yaml Structure

```yaml
# Auto-generated by tavana-admin
# Customer: Acme Corp
# Generated: 2025-12-25T10:00:00Z

global:
  cloud: azure
  imageRegistry: acme.azurecr.io
  imageTag: v1.0.1
  namespace: tavana

gateway:
  replicas: 2
  resources:
    requests:
      memory: 512Mi
      cpu: 250m
    limits:
      memory: 2Gi
      cpu: 1000m
  service:
    type: ClusterIP
    port: 8081  # PostgreSQL wire protocol

worker:
  minReplicas: 2
  maxReplicas: 10
  resources:
    requests:
      memory: 1Gi
      cpu: 500m
    limits:
      memory: 8Gi
      cpu: 4000m

storage:
  type: azure
  azure:
    storageAccount: adlsacme
    container: datalake
    useWorkloadIdentity: true

autoscaling:
  hpa:
    enabled: true
    targetCPU: 70
    targetMemory: 80
  vpa:
    enabled: true
    updateMode: Auto

security:
  podSecurityStandards: restricted
  networkPolicies: true
  resourceQuotas: true
```

### Deployment Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 1 DEPLOYMENT FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  VENDOR SIDE                          CUSTOMER SIDE                             │
│  ────────────                          ─────────────                             │
│                                                                                  │
│  1. Customer Request                                                             │
│     └─ Email/call with requirements                                             │
│                                                                                  │
│  2. Generate Config                   3. Prepare Environment                    │
│     └─ tavana-admin generate ...         └─ Create ACR (if needed)              │
│     └─ Review & customize                └─ Create storage account              │
│                                          └─ Set up Workload Identity            │
│                                                                                  │
│  4. Send to Customer                  5. Import Images                          │
│     └─ values.yaml                       └─ az acr import (if air-gapped)       │
│     └─ Installation instructions         └─ Or pull from Docker Hub             │
│                                                                                  │
│                                       6. Install                                 │
│                                          └─ helm install tavana ...              │
│                                                                                  │
│                                       7. Verify                                  │
│                                          └─ psql -h gateway.tavana ...           │
│                                                                                  │
│  8. Support                                                                      │
│     └─ Troubleshooting via logs                                                 │
│     └─ Version updates (new values.yaml)                                        │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 2: License Validation

> **Goal**: Add license enforcement without full control plane.  
> **Cost**: $5-10/month (serverless)  
> **Timeline**: 1 week  
> **When**: After 5-10 paying customers  

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 2 ARCHITECTURE                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  VENDOR CLOUD (Serverless)                                                       │
│  ────────────────────────────                                                   │
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────┐                  │
│  │  Azure Function / AWS Lambda                              │                  │
│  │                                                           │                  │
│  │  POST /api/v1/license/validate                           │                  │
│  │  ─────────────────────────────                           │                  │
│  │  Request:  { "license_key": "TAVANA-XXXX-XXXX" }         │                  │
│  │  Response: { "valid": true, "expires": "2026-12-25",     │                  │
│  │              "features": ["hpa", "vpa", "multi-tenant"]} │                  │
│  │                                                           │                  │
│  │  Storage: Embedded JSON or Cosmos DB (serverless tier)   │                  │
│  │                                                           │                  │
│  └───────────────────────────────────────────────────────────┘                  │
│                          ▲                                                       │
│                          │ HTTPS (outbound from customer)                       │
│                          │                                                       │
│  CUSTOMER K8S            │                                                       │
│  ────────────            │                                                       │
│                          │                                                       │
│  ┌───────────────────────┴───────────────────────────────────┐                  │
│  │  Gateway (Modified)                                       │                  │
│  │                                                           │                  │
│  │  On Startup:                                              │                  │
│  │  ├─ Check license with vendor endpoint                   │                  │
│  │  ├─ Cache result for 24 hours                            │                  │
│  │  └─ If unreachable, use cached license (grace period)    │                  │
│  │                                                           │                  │
│  │  Every 24 hours:                                          │                  │
│  │  └─ Revalidate license                                   │                  │
│  │                                                           │                  │
│  │  If license invalid:                                      │                  │
│  │  ├─ Log warning                                          │                  │
│  │  ├─ Continue working (grace period: 7 days)              │                  │
│  │  └─ After grace period: reject new queries               │                  │
│  │                                                           │                  │
│  └───────────────────────────────────────────────────────────┘                  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Gateway License Integration

```rust
// Addition to tavana-gateway/src/license.rs

use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub struct LicenseValidator {
    endpoint: Option<String>,
    license_key: String,
    cached_result: RwLock<Option<LicenseResult>>,
    last_check: RwLock<Instant>,
    grace_period: Duration,
}

#[derive(Clone)]
struct LicenseResult {
    valid: bool,
    expires: chrono::DateTime<chrono::Utc>,
    features: Vec<String>,
}

impl LicenseValidator {
    pub async fn check(&self) -> bool {
        // 1. Check cache
        if let Some(cached) = self.cached_result.read().await.as_ref() {
            if self.last_check.read().await.elapsed() < Duration::from_secs(86400) {
                return cached.valid && cached.expires > chrono::Utc::now();
            }
        }
        
        // 2. If no endpoint (Phase 1), always valid
        let endpoint = match &self.endpoint {
            Some(ep) => ep,
            None => return true,
        };
        
        // 3. Validate with vendor
        match self.validate_remote(endpoint).await {
            Ok(result) => {
                *self.cached_result.write().await = Some(result.clone());
                *self.last_check.write().await = Instant::now();
                result.valid
            }
            Err(_) => {
                // Network error: use grace period
                self.cached_result.read().await
                    .as_ref()
                    .map(|r| r.valid)
                    .unwrap_or(true) // First-time failure: allow
            }
        }
    }
}
```

### License Key Format

```
Format: TAVANA-{TIER}-{RANDOM}-{CHECKSUM}

Examples:
  TAVANA-COMM-A1B2C3D4-E5F6   # Community (free)
  TAVANA-TEAM-X9Y8Z7W6-V5U4   # Team (paid)
  TAVANA-ENTP-M3N4O5P6-Q7R8   # Enterprise (paid)

Tiers:
  COMM: Community
    - Single namespace
    - 5 workers max
    - No SLA
    
  TEAM: Team ($X/month)
    - Multi-namespace
    - 20 workers max
    - Email support
    
  ENTP: Enterprise ($X/month)
    - Unlimited
    - Priority support
    - Custom SLA
```

---

## Phase 3: Full Control Plane

> **Goal**: Full visibility, automated onboarding, usage-based billing.  
> **Cost**: $50-200/month (managed K8s or VM)  
> **Timeline**: 4-6 weeks  
> **When**: After 50+ customers or enterprise contracts  

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      PHASE 3 ARCHITECTURE                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  VENDOR CLOUD (Managed)                                                          │
│  ─────────────────────────                                                      │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         CONTROL PLANE                                    │    │
│  │                                                                          │    │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │    │
│  │  │   API Server     │  │   PostgreSQL     │  │   Redis Cache    │       │    │
│  │  │   (Axum/Rust)    │  │   (Customers,    │  │   (Sessions,     │       │    │
│  │  │                  │  │    Deployments,  │  │    Rate limits)  │       │    │
│  │  │  /api/v1/*       │  │    Usage)        │  │                  │       │    │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘       │    │
│  │           │                                                              │    │
│  │           ▼                                                              │    │
│  │  ┌──────────────────────────────────────────────────────────────┐       │    │
│  │  │                    CUSTOMER PORTAL (Optional)                 │       │    │
│  │  │                                                               │       │    │
│  │  │  • Self-service deployment creation                          │       │    │
│  │  │  • Usage dashboards                                          │       │    │
│  │  │  • Billing history                                           │       │    │
│  │  │  • Configuration management                                  │       │    │
│  │  │                                                               │       │    │
│  │  └──────────────────────────────────────────────────────────────┘       │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                          ▲                                                       │
│                          │ HTTPS (Mutual TLS)                                   │
│                          │                                                       │
│  CUSTOMER ENVIRONMENTS   │                                                       │
│  ─────────────────────   │                                                       │
│                          │                                                       │
│  ┌───────────────────────┴──────────────┐    ┌─────────────────────────────┐    │
│  │  Customer A (Azure)                   │    │  Customer B (AWS)           │    │
│  │  ┌─────────────┐                     │    │  ┌─────────────┐            │    │
│  │  │ Tavana Agent│──┐                  │    │  │ Tavana Agent│            │    │
│  │  └─────────────┘  │                  │    │  └─────────────┘            │    │
│  │         │         │ Heartbeats       │    │         │                   │    │
│  │         ▼         │ + Metrics        │    │         ▼                   │    │
│  │  ┌─────────────┐  │                  │    │  ┌─────────────┐            │    │
│  │  │Gateway+Workers│                   │    │  │Gateway+Workers│           │    │
│  │  └─────────────┘                     │    │  └─────────────┘            │    │
│  └──────────────────────────────────────┘    └─────────────────────────────┘    │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### API Endpoints

```yaml
# Control Plane API

# Authentication
POST   /api/v1/auth/login           # Vendor admin login
POST   /api/v1/auth/token/refresh   # Refresh JWT

# Customers
GET    /api/v1/customers            # List all customers
POST   /api/v1/customers            # Create customer
GET    /api/v1/customers/:id        # Get customer details
PATCH  /api/v1/customers/:id        # Update customer
DELETE /api/v1/customers/:id        # Delete customer

# Deployments
GET    /api/v1/deployments                    # List all deployments
POST   /api/v1/deployments                    # Create deployment
GET    /api/v1/deployments/:id                # Get deployment
PATCH  /api/v1/deployments/:id                # Update deployment
DELETE /api/v1/deployments/:id                # Delete deployment
GET    /api/v1/deployments/:id/config         # Get config (agent calls this)
GET    /api/v1/deployments/:id/install-cmd    # Get Helm install command

# Agent Communication
POST   /api/v1/agent/register       # Agent registration
POST   /api/v1/agent/heartbeat      # Agent heartbeat
POST   /api/v1/agent/metrics        # Push metrics

# License
POST   /api/v1/license/validate     # Validate license key
POST   /api/v1/license/generate     # Generate new license

# Usage & Billing
GET    /api/v1/usage/:customer_id   # Get usage stats
GET    /api/v1/billing/:customer_id # Get billing info
```

### Database Schema

```sql
-- Customers
CREATE TABLE customers (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    tier VARCHAR(50) NOT NULL,  -- 'community', 'team', 'enterprise'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Deployments
CREATE TABLE deployments (
    id UUID PRIMARY KEY,
    customer_id UUID REFERENCES customers(id),
    name VARCHAR(255) NOT NULL,
    cloud VARCHAR(50) NOT NULL,  -- 'azure', 'aws', 'gcp', 'onprem'
    region VARCHAR(100),
    namespace VARCHAR(255) NOT NULL,
    config JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    license_key VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Agent Tokens (for secure communication)
CREATE TABLE agent_tokens (
    id UUID PRIMARY KEY,
    deployment_id UUID REFERENCES deployments(id),
    token_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    revoked BOOLEAN DEFAULT FALSE
);

-- Heartbeats (for monitoring)
CREATE TABLE heartbeats (
    id BIGSERIAL PRIMARY KEY,
    deployment_id UUID REFERENCES deployments(id),
    agent_version VARCHAR(50),
    gateway_pods INT,
    worker_pods INT,
    status VARCHAR(50),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Usage Metrics
CREATE TABLE usage_metrics (
    id BIGSERIAL PRIMARY KEY,
    deployment_id UUID REFERENCES deployments(id),
    metric_date DATE NOT NULL,
    queries_executed BIGINT DEFAULT 0,
    data_scanned_bytes BIGINT DEFAULT 0,
    compute_seconds BIGINT DEFAULT 0,
    peak_workers INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(deployment_id, metric_date)
);
```

### Agent Implementation

```rust
// tavana-agent/src/main.rs

use kube::Client;
use tokio::time::{interval, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config from environment
    let config = AgentConfig::from_env()?;
    
    // Initialize K8s client
    let k8s_client = Client::try_default().await?;
    
    // Create agent
    let agent = TavanaAgent::new(config, k8s_client);
    
    // Run agent loops
    tokio::select! {
        _ = agent.reconcile_loop() => {},
        _ = agent.heartbeat_loop() => {},
        _ = agent.metrics_loop() => {},
    }
    
    Ok(())
}

struct TavanaAgent {
    config: AgentConfig,
    k8s_client: Client,
    http_client: reqwest::Client,
}

impl TavanaAgent {
    /// Main reconciliation loop
    async fn reconcile_loop(&self) {
        let mut interval = interval(Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.reconcile().await {
                tracing::error!("Reconcile failed: {}", e);
            }
        }
    }
    
    /// Reconcile desired state with actual state
    async fn reconcile(&self) -> anyhow::Result<()> {
        // 1. Fetch config from control plane
        let config = self.fetch_config().await?;
        
        // 2. Ensure images are available in customer ACR
        self.ensure_images(&config).await?;
        
        // 3. Apply Helm release
        self.apply_helm(&config).await?;
        
        // 4. Verify deployment
        self.verify_deployment(&config).await?;
        
        Ok(())
    }
    
    /// Heartbeat loop
    async fn heartbeat_loop(&self) {
        let mut interval = interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            let status = self.collect_status().await;
            
            if let Err(e) = self.send_heartbeat(&status).await {
                tracing::warn!("Heartbeat failed: {}", e);
            }
        }
    }
    
    /// Metrics collection loop
    async fn metrics_loop(&self) {
        let mut interval = interval(Duration::from_secs(300)); // 5 minutes
        
        loop {
            interval.tick().await;
            
            let metrics = self.collect_metrics().await;
            
            if let Err(e) = self.push_metrics(&metrics).await {
                tracing::warn!("Metrics push failed: {}", e);
            }
        }
    }
}
```

---

## Technology Decisions

### Why Rust (Not Go)?

| Factor | Go | Rust | Decision |
|--------|----|----|----------|
| **Existing codebase** | New language | Same as Tavana | ✅ Rust |
| **Team knowledge** | Need to learn | Already expert | ✅ Rust |
| **Shared code** | None | Reuse proto, types, utils | ✅ Rust |
| **Binary size** | ~10MB | ~5MB | ✅ Rust |
| **Memory usage** | ~20MB | ~5MB | ✅ Rust |
| **K8s client** | client-go (native) | kube-rs (excellent) | Tie |
| **Helm library** | helm-go (native) | None (shell out) | Go |
| **HTTP server** | net/http | axum | Tie |
| **Compile time** | Fast | Slower | Go |

**Decision**: Use **Rust** for consistency with Tavana codebase.

### Helm Integration

Since there's no native Helm library for Rust, we have two options:

#### Option 1: Shell Out to Helm CLI (Recommended for MVP)

```rust
async fn apply_helm(&self, config: &DeploymentConfig) -> anyhow::Result<()> {
    let output = tokio::process::Command::new("helm")
        .args([
            "upgrade", "--install",
            "tavana",
            "oci://ghcr.io/angelerator/charts/tavana",
            "--namespace", &config.namespace,
            "--create-namespace",
            "--set", &format!("global.imageRegistry={}", config.acr),
            "--set", &format!("global.imageTag={}", config.version),
            "--wait",
            "--timeout", "5m",
        ])
        .output()
        .await?;
    
    if !output.status.success() {
        anyhow::bail!("Helm failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    Ok(())
}
```

#### Option 2: Direct Kubernetes API (More Control)

```rust
async fn apply_manifests(&self, config: &DeploymentConfig) -> anyhow::Result<()> {
    // Generate manifests from templates
    let manifests = self.render_templates(config)?;
    
    // Apply each manifest
    for manifest in manifests {
        self.apply_manifest(&manifest).await?;
    }
    
    Ok(())
}
```

---

## Deployment Scenarios

### Scenario 1: Standard Deployment (Internet Access)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    STANDARD DEPLOYMENT                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Prerequisites:                                                                  │
│  • Kubernetes cluster with internet access                                      │
│  • Container registry (optional, can use Docker Hub)                            │
│  • Storage account (ADLS, S3, GCS)                                             │
│                                                                                  │
│  Steps:                                                                          │
│  1. tavana-admin generate --customer acme --cloud azure ...                     │
│  2. Customer runs: helm install tavana ... -f values.yaml                       │
│  3. Images pulled from Docker Hub / GHCR                                        │
│  4. Done!                                                                        │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Scenario 2: Private ACR Deployment

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    PRIVATE ACR DEPLOYMENT                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Prerequisites:                                                                  │
│  • AKS cluster with ACR attachment                                              │
│  • Private ACR with network restrictions                                        │
│  • AKS outbound IP whitelisted in ACR firewall                                 │
│                                                                                  │
│  Steps:                                                                          │
│  1. Import images to customer ACR:                                              │
│     az acr import --name customeracr \                                          │
│       --source docker.io/angelerator/tavana-gateway:v1.0.1                     │
│                                                                                  │
│  2. Generate values:                                                            │
│     tavana-admin generate --acr customeracr.azurecr.io ...                     │
│                                                                                  │
│  3. Customer runs: helm install ...                                             │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Scenario 3: Air-Gapped Deployment

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    AIR-GAPPED DEPLOYMENT                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Prerequisites:                                                                  │
│  • Kubernetes cluster with NO internet access                                   │
│  • Private container registry                                                   │
│  • Secure file transfer mechanism (USB, SFTP)                                  │
│                                                                                  │
│  Steps:                                                                          │
│                                                                                  │
│  ON INTERNET-CONNECTED MACHINE:                                                 │
│  1. Generate offline package:                                                   │
│     ./scripts/prepare-offline-package.sh \                                      │
│       --version v1.0.1 \                                                        │
│       --output ./tavana-offline-v1.0.1.tar.gz                                  │
│                                                                                  │
│  2. Transfer package to air-gapped environment                                  │
│                                                                                  │
│  ON AIR-GAPPED MACHINE:                                                         │
│  3. Extract and load images:                                                    │
│     tar xzf tavana-offline-v1.0.1.tar.gz                                       │
│     ./load-images.sh --registry internal-registry:5000                          │
│                                                                                  │
│  4. Install from local registry:                                                │
│     helm install tavana ./helm/tavana \                                         │
│       --set global.imageRegistry=internal-registry:5000 \                       │
│       -f values.yaml                                                            │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Security Model

### Network Security

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         NETWORK SECURITY                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────┐              │
│  │                    CUSTOMER'S KUBERNETES                      │              │
│  │                                                               │              │
│  │  ┌─────────────────────────────────────────────────────────┐  │              │
│  │  │                 TAVANA NAMESPACE                         │  │              │
│  │  │                                                          │  │              │
│  │  │  NetworkPolicy: deny-all (default)                       │  │              │
│  │  │                                                          │  │              │
│  │  │  ┌────────────┐      ┌────────────┐                     │  │              │
│  │  │  │  Gateway   │─────▶│  Workers   │  ✓ Allowed          │  │              │
│  │  │  └────────────┘      └────────────┘                     │  │              │
│  │  │       │                    │                             │  │              │
│  │  │       │                    │                             │  │              │
│  │  │       ▼                    ▼                             │  │              │
│  │  │  ┌─────────────────────────────────────────┐            │  │              │
│  │  │  │  Customer's Storage (ADLS/S3)          │ ✓ Allowed   │  │              │
│  │  │  └─────────────────────────────────────────┘            │  │              │
│  │  │                                                          │  │              │
│  │  │  External Internet: ✗ Blocked (except egress to control │  │              │
│  │  │                      plane if Phase 2+)                  │  │              │
│  │  │                                                          │  │              │
│  │  └─────────────────────────────────────────────────────────┘  │              │
│  │                                                               │              │
│  └───────────────────────────────────────────────────────────────┘              │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Identity & Access

```yaml
# Workload Identity (Azure)
# No secrets stored in cluster

Azure AD App Registration
    └── Federated Identity Credential
            ├── Issuer: AKS OIDC Issuer
            ├── Subject: system:serviceaccount:tavana:tavana-gateway
            └── Subject: system:serviceaccount:tavana:tavana-worker

# Role Assignments
Storage Blob Data Contributor
    └── Assigned to: App Registration
    └── Scope: Storage Account / Container
```

### Image Security

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         IMAGE SECURITY                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  BUILD PIPELINE (GitHub Actions)                                                │
│  ─────────────────────────────────                                              │
│                                                                                  │
│  1. Build                                                                        │
│     └─ Multi-arch (amd64 + arm64)                                              │
│     └─ From distroless base (minimal attack surface)                           │
│                                                                                  │
│  2. Scan                                                                         │
│     └─ Trivy vulnerability scan                                                 │
│     └─ Fail on HIGH/CRITICAL                                                    │
│                                                                                  │
│  3. Sign                                                                         │
│     └─ Cosign signature                                                         │
│     └─ SBOM generation                                                          │
│                                                                                  │
│  4. Push                                                                         │
│     └─ Docker Hub: angelerator/tavana-*                                        │
│     └─ GHCR: ghcr.io/angelerator/tavana-*                                      │
│                                                                                  │
│  VERIFICATION (Customer Side)                                                   │
│  ────────────────────────────────                                               │
│                                                                                  │
│  # Verify image signature                                                       │
│  cosign verify --key tavana-cosign.pub \                                       │
│    docker.io/angelerator/tavana-gateway:v1.0.1                                 │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Cost Analysis

### Phase 1: No Control Plane

| Item | Cost |
|------|------|
| Control Plane Infrastructure | $0 |
| Docker Hub (public images) | $0 |
| GHCR (public images) | $0 |
| **Total** | **$0/month** |

### Phase 2: License Validation

| Item | Cost |
|------|------|
| Azure Function (1M executions) | $0.20 |
| Cosmos DB (1GB, serverless) | $5 |
| Custom Domain + SSL | $0 (Let's Encrypt) |
| **Total** | **~$5/month** |

### Phase 3: Full Control Plane

| Option | Cost |
|--------|------|
| **Option A: Single VM** | |
| Azure B2s (2 vCPU, 4GB RAM) | $15/month |
| Managed PostgreSQL (Basic) | $25/month |
| **Total** | **$40/month** |
| | |
| **Option B: Small AKS** | |
| AKS (1 B2s node) | $30/month |
| Managed PostgreSQL (Basic) | $25/month |
| **Total** | **$55/month** |
| | |
| **Option C: Serverless** | |
| Azure Container Apps | $20/month |
| Cosmos DB | $15/month |
| **Total** | **$35/month** |

---

## Implementation Timeline

### Phase 1 (Weeks 1-2)

```
Week 1:
├── Day 1-2: Design tavana-admin CLI structure
├── Day 3-4: Implement generate command
├── Day 5: Test with Azure deployment
└── Weekend: Documentation

Week 2:
├── Day 1-2: Enhance Helm chart parameterization
├── Day 3: Add AWS support to CLI
├── Day 4: Add GCP support to CLI
├── Day 5: End-to-end testing
└── Weekend: Release v1.0.0-cli
```

### Phase 2 (Week 3)

```
Week 3:
├── Day 1: Create Azure Function for license validation
├── Day 2: Add license checking to Gateway
├── Day 3: Implement grace period logic
├── Day 4: Testing and documentation
└── Day 5: Deploy to production
```

### Phase 3 (Weeks 4-8)

```
Week 4-5:
├── Control Plane API (Axum + PostgreSQL)
├── Customer/Deployment CRUD
└── Agent token management

Week 6:
├── Tavana Agent implementation
├── Reconcile loop
└── Heartbeat/metrics

Week 7:
├── Integration testing
├── Security audit
└── Load testing

Week 8:
├── Customer portal (optional)
├── Documentation
└── Release
```

---

## Quick Reference

### Commands

```bash
# Phase 1: Generate customer config
tavana-admin generate \
  --customer-name "Acme Corp" \
  --cloud azure \
  --acr acme.azurecr.io \
  --storage-account adlsacme \
  --output values.yaml

# Customer installs
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  -f values.yaml \
  -n tavana \
  --create-namespace

# Verify deployment
kubectl get pods -n tavana
psql -h gateway.tavana.svc -p 8081 -U postgres
```

### Environment Variables

```bash
# Gateway
KUBERNETES_NAMESPACE=tavana
WORKER_LABEL_SELECTOR=app=tavana-worker
STORAGE_TYPE=azure
AZURE_STORAGE_ACCOUNT=adlsacme
AZURE_STORAGE_CONTAINER=datalake

# Agent (Phase 3)
TAVANA_CONTROL_PLANE=https://api.tavana.io
TAVANA_DEPLOYMENT_ID=dep_xxxx
TAVANA_AGENT_TOKEN=tat_xxxx
```

---

## Next Steps

1. **Immediate**: Build `tavana-admin` CLI with `generate` command
2. **Short-term**: Test deployment flow with Nokia
3. **Medium-term**: Add license validation (Phase 2)
4. **Long-term**: Full control plane when customer base grows

---

*Last updated: December 2025*

