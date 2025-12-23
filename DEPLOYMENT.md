# Tavana Deployment Guide

This guide covers deploying Tavana to production using the GitOps approach with Terraform and ArgoCD.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              YOUR INFRASTRUCTURE                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────────┐ │
│  │   GitHub Actions   │    │   Docker Hub/GHCR  │    │   GitHub (GitOps)      │ │
│  │   (CI/CD)          │───▶│   (Images)         │    │   (Config)             │ │
│  └────────────────────┘    └─────────┬──────────┘    └──────────┬─────────────┘ │
│                                      │                           │               │
│                                      ▼                           ▼               │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                           CUSTOMER'S CLOUD                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                     Terraform (Infrastructure)                       │  │  │
│  │  │  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │  │  │
│  │  │  │   AKS   │  │   ACR   │  │   ADLS   │  │ KeyVault │  │ App GW  │  │  │  │
│  │  │  └────┬────┘  └─────────┘  └──────────┘  └──────────┘  └─────────┘  │  │  │
│  │  └───────┼───────────────────────────────────────────────────────────────┘  │  │
│  │          │                                                                   │  │
│  │          ▼                                                                   │  │
│  │  ┌─────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                           AKS Cluster                                │  │  │
│  │  │  ┌──────────────────────────────────────────────────────────────┐   │  │  │
│  │  │  │                      ArgoCD (GitOps)                          │   │  │  │
│  │  │  │           Watches GitHub ──▶ Deploys Helm Chart               │   │  │  │
│  │  │  └──────────────────────────────────────────────────────────────┘   │  │  │
│  │  │                               │                                      │  │  │
│  │  │                               ▼                                      │  │  │
│  │  │  ┌──────────────────────────────────────────────────────────────┐   │  │  │
│  │  │  │                        Tavana                                 │   │  │  │
│  │  │  │   ┌───────────┐          ┌────────────────────────────────┐  │   │  │  │
│  │  │  │   │  Gateway  │──────────│     Workers (auto-scaled)      │  │   │  │  │
│  │  │  │   │  (2 pods) │   gRPC   │     (2-20 pods, HPA/VPA)       │  │   │  │  │
│  │  │  │   └───────────┘          └────────────────────────────────┘  │   │  │  │
│  │  │  └──────────────────────────────────────────────────────────────┘   │  │  │
│  │  └─────────────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Deployment Options

### Option 1: One-Click Script (Fastest)

```bash
./deploy.sh --subscription-id YOUR_SUBSCRIPTION_ID --env prod
```

### Option 2: Step-by-Step (Recommended for Production)

See sections below.

---

## 📋 Prerequisites

| Tool | Version | Installation |
|------|---------|--------------|
| Azure CLI | >= 2.50 | `brew install azure-cli` |
| Terraform | >= 1.5 | `brew install terraform` |
| kubectl | >= 1.28 | `brew install kubectl` |
| Helm | >= 3.12 | `brew install helm` |
| Git | >= 2.0 | `brew install git` |

### Azure Requirements

- Azure subscription with Owner or Contributor access
- Ability to create: Resource Groups, AKS, ACR, Storage Account, Managed Identities
- Quotas for D-series VMs in your target region

---

## 🔧 Step 1: Deploy Infrastructure

### 1.1 Clone and Configure

```bash
git clone https://github.com/tavana/tavana.git
cd tavana/terraform/azure/examples/quickstart  # or /enterprise for production

# Copy and edit the configuration
cp terraform.tfvars.example terraform.tfvars
vim terraform.tfvars
```

### 1.2 Configure terraform.tfvars

```hcl
# Required
subscription_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# Project
project_name = "tavana"
environment  = "prod"
location     = "westeurope"

# For enterprise:
acr_geo_replication_locations = ["northeurope"]
argocd_hostname = "argocd.tavana.yourcompany.com"
```

### 1.3 Deploy

```bash
# Login to Azure
az login
az account set --subscription YOUR_SUBSCRIPTION_ID

# Initialize and apply
terraform init
terraform plan -out=tfplan
terraform apply tfplan

# Save outputs
terraform output -json > outputs.json
```

### 1.4 Configure kubectl

```bash
# Command is in Terraform output
az aks get-credentials --resource-group tavana-prod-rg-we --name tavana-prod-aks-we
kubectl get nodes
```

---

## 📦 Step 2: Import Images to ACR

Tavana images are published to Docker Hub. Import them to your private ACR:

```bash
# Get ACR name from Terraform output
ACR_NAME=$(terraform output -raw acr_name)
VERSION="1.0.0"

# Import images
az acr import --name $ACR_NAME \
  --source docker.io/tavana/gateway:$VERSION \
  --image tavana/gateway:$VERSION

az acr import --name $ACR_NAME \
  --source docker.io/tavana/worker:$VERSION \
  --image tavana/worker:$VERSION
```

---

## 🔄 Step 3: Set Up GitOps

### 3.1 Create Your Config Repository

Use the template at `gitops-template/` or create your own:

```bash
# Create new repo from template
gh repo create mycompany/tavana-config --template tavana/tavana-config-template

# Clone and configure
git clone https://github.com/mycompany/tavana-config
cd tavana-config
```

### 3.2 Update Configuration

Edit `apps/tavana.yaml` with values from Terraform outputs:

```yaml
source:
  helm:
    values: |
      global:
        imageRegistry: "YOUR_ACR.azurecr.io"  # From: terraform output acr_login_server
        imageTag: "1.0.0"
      
      serviceAccount:
        annotations:
          azure.workload.identity/client-id: "xxx"  # From: terraform output tavana_identity_client_id
          azure.workload.identity/tenant-id: "xxx"  # From: terraform output tavana_identity_tenant_id
      
      gateway:
        env:
          - name: AZURE_STORAGE_ACCOUNT
            value: "xxx"  # From: terraform output storage_account_name
```

### 3.3 Push and Connect ArgoCD

```bash
git add .
git commit -m "Configure for production"
git push

# Get ArgoCD password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d

# Port-forward ArgoCD
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

Open https://localhost:8080, login with `admin` and the password above.

### 3.4 Connect Repository to ArgoCD

1. Settings → Repositories → Connect Repo
2. Enter your GitHub repo URL
3. Configure authentication (SSH key or GitHub App)
4. Apply the Application:

```bash
kubectl apply -f apps/tavana.yaml
```

---

## ✅ Step 4: Verify Deployment

```bash
# Check pods
kubectl get pods -n tavana

# Check services
kubectl get svc -n tavana

# Port-forward gateway
kubectl port-forward svc/gateway -n tavana 5432:5432

# Test connection
PGPASSWORD=tavana psql -h localhost -p 5432 -U tavana -d tavana -c "SELECT 1"
```

---

## 🔄 Upgrading

### Via GitOps (Recommended)

1. Update version in `apps/tavana.yaml`:
   ```yaml
   source:
     targetRevision: "1.1.0"
     helm:
       values: |
         global:
           imageTag: "1.1.0"
   ```

2. Import new images to ACR:
   ```bash
   az acr import --name $ACR_NAME --source docker.io/tavana/gateway:1.1.0 --image tavana/gateway:1.1.0
   az acr import --name $ACR_NAME --source docker.io/tavana/worker:1.1.0 --image tavana/worker:1.1.0
   ```

3. Commit and push:
   ```bash
   git commit -am "Upgrade to 1.1.0"
   git push
   ```

4. ArgoCD auto-syncs the change.

---

## 🔐 Security Considerations

### 1. Image Signing

All Tavana images are signed with Cosign. Verify before deploying:

```bash
cosign verify --key https://tavana.io/cosign.pub tavana/gateway:1.0.0
```

### 2. Network Policies

Network policies are enabled by default. Review in `helm/tavana/templates/networkpolicy.yaml`.

### 3. Pod Security

Pods run as non-root with restricted security contexts. Review in `values.yaml`:
- `runAsNonRoot: true`
- `readOnlyRootFilesystem: true`
- `allowPrivilegeEscalation: false`

### 4. Secrets

Store secrets in Azure Key Vault and use External Secrets Operator:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: tavana-secrets
spec:
  secretStoreRef:
    name: azure-keyvault
    kind: ClusterSecretStore
  target:
    name: tavana-secrets
  data:
    - secretKey: api-key
      remoteRef:
        key: tavana-api-key
```

---

## 🆘 Troubleshooting

### Pods not starting

```bash
kubectl describe pod -n tavana <pod-name>
kubectl logs -n tavana <pod-name>
```

### ArgoCD sync issues

```bash
argocd app get tavana
argocd app sync tavana --force
```

### Image pull errors

```bash
# Verify ACR integration
az aks check-acr --name $AKS_NAME --resource-group $RG_NAME --acr $ACR_NAME
```

### Workload Identity issues

```bash
# Check service account
kubectl get sa -n tavana tavana -o yaml

# Check federated credential
az identity federated-credential list --identity-name tavana-identity --resource-group $RG_NAME
```

---

## 📊 Monitoring

### Built-in Metrics

Tavana exposes Prometheus metrics at `/metrics`:

```bash
kubectl port-forward svc/gateway -n tavana 8080:8080
curl http://localhost:8080/metrics
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| `tavana_query_queue_depth` | Queries waiting in queue |
| `tavana_query_queue_wait_seconds` | Time queries wait in queue |
| `tavana_query_duration_seconds` | Query execution time |
| `tavana_worker_memory_bytes` | Worker memory usage |
| `tavana_active_queries` | Currently executing queries |

### Grafana Dashboards

Import dashboards from `k8s/monitoring/`:
- `tavana-complete-dashboard.json`
- `resource-dashboard.json`

---

## 📚 Additional Resources

- [Helm Chart Documentation](./helm/tavana/README.md)
- [Terraform Module Documentation](./terraform/README.md)
- [Architecture Overview](./docs/architecture.md)
- [API Reference](./docs/api.md)

