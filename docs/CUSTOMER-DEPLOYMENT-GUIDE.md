# Tavana Customer Deployment Guide

## Complete Step-by-Step Instructions for All Environment Types

---

## 📋 Table of Contents

1. [Environment Types Overview](#environment-types-overview)
2. [Prerequisites Checklist](#prerequisites-checklist)
3. [Scenario A: Fully Restricted (Air-Gapped)](#scenario-a-fully-restricted-air-gapped)
4. [Scenario B: Restricted with ACR Access](#scenario-b-restricted-with-acr-access)
5. [Scenario C: Standard Enterprise](#scenario-c-standard-enterprise)
6. [Scenario D: Open/Dev Environment](#scenario-d-opendev-environment)
7. [Post-Deployment Verification](#post-deployment-verification)
8. [Troubleshooting](#troubleshooting)

---

## Environment Types Overview

| Scenario | Internet Access | Can Pull Public Images | Typical Customer |
|----------|-----------------|------------------------|------------------|
| **A: Air-Gapped** | ❌ None | ❌ No | Banks, Government, Military |
| **B: ACR Only** | ⚠️ ACR Only | ❌ No | Enterprise with strict policies |
| **C: Standard** | ✅ Outbound | ✅ Yes | Most enterprises |
| **D: Open** | ✅ Full | ✅ Yes | Startups, Dev environments |

---

## Prerequisites Checklist

### Your Side (Tavana Vendor)

- [ ] Docker installed (for building offline packages)
- [ ] Helm v3 installed
- [ ] Access to Tavana source code
- [ ] Docker Hub credentials (angelerator account)

### Customer Side (All Scenarios)

- [ ] Kubernetes cluster (AKS, EKS, GKE, or on-prem)
- [ ] kubectl configured and connected
- [ ] Helm v3 installed
- [ ] Sufficient cluster resources (min: 4 vCPU, 8GB RAM)

### Customer Side (Azure-Specific)

- [ ] Azure CLI installed and logged in
- [ ] Azure Container Registry (ACR) access
- [ ] Storage Account (for data lake)

---

## Scenario A: Fully Restricted (Air-Gapped)

**For: Banks, Military, Government, Enterprises with strict security**

This is the most secure deployment where the customer's Kubernetes cluster has **zero internet access**.

### Step 1: Prepare Offline Package (Your Side)

```bash
# On your machine with internet access
cd /path/to/tavana

# Create the offline package
./scripts/prepare-offline-package.sh v1.0.0
```

**What this creates:**
```
dist/tavana-offline-v1.0.0/
├── images/
│   ├── tavana-gateway-v1.0.0.tar.gz    # ~200MB
│   └── tavana-worker-v1.0.0.tar.gz     # ~300MB
├── helm/
│   └── tavana-1.0.0.tgz                # ~50KB
├── scripts/
│   ├── customer-deploy.sh              # One-click wizard
│   ├── import-to-acr.sh                # Manual import
│   └── deploy-tavana.sh                # Manual deploy
├── terraform/azure/                     # Optional infra
├── values-customer.yaml                # Config template
└── README.md                           # Instructions
```

### Step 2: Transfer Package to Customer

**Option A: Secure File Transfer**
```bash
# SCP to customer's bastion host
scp dist/tavana-offline-v1.0.0.tar.gz customer@bastion.example.com:/packages/
```

**Option B: Azure Storage (if they have storage access)**
```bash
# Upload to shared Azure Storage
az storage blob upload \
  --account-name customersharedstorage \
  --container-name packages \
  --file dist/tavana-offline-v1.0.0.tar.gz \
  --name tavana-offline-v1.0.0.tar.gz
```

**Option C: Physical Media (USB)**
- Copy `dist/tavana-offline-v1.0.0.tar.gz` to encrypted USB
- Physically deliver to customer

### Step 3: Customer Extracts Package

```bash
# On customer's machine with ACR + kubectl access
cd /packages
tar -xzf tavana-offline-v1.0.0.tar.gz
cd tavana-offline-v1.0.0
```

### Step 4: Customer Runs One-Click Deployment

```bash
# Interactive wizard - handles everything
./scripts/customer-deploy.sh
```

**Wizard prompts:**
```
═══════════════════════════════════════════════════════════════════════════════
  STEP 2/5: Configuration
═══════════════════════════════════════════════════════════════════════════════

  Please provide the following information:

  Azure Container Registry name (e.g., mycompanyacr): mycompany-prod-acr
  ▶ Fetching ACR details...
  ✓ ACR found: mycompany-prod-acr.azurecr.io

  Kubernetes namespace [tavana]: tavana-prod

  ▶ Detected version: v1.0.0

  Azure Storage Account name (for data lake, optional): mycompanydatalake

  Configuration Summary:
    ACR:             mycompany-prod-acr.azurecr.io
    Namespace:       tavana-prod
    Version:         v1.0.0
    Storage Account: mycompanydatalake

  Proceed with deployment? (Y/n): Y
```

**Wizard actions (automatic):**
1. Logs into customer's ACR
2. Loads Docker images from tarballs
3. Tags and pushes to customer's ACR
4. Creates Kubernetes namespace
5. Deploys via Helm with their config
6. Verifies pods are running
7. Shows connection instructions

### Step 5: Verify Deployment

```bash
# Check pods
kubectl get pods -n tavana-prod

# Expected output:
# NAME                       READY   STATUS    RESTARTS   AGE
# gateway-6f7b8c9d4-abc12    1/1     Running   0          2m
# gateway-6f7b8c9d4-def34    1/1     Running   0          2m
# worker-5d6e7f8a9-ghi56     1/1     Running   0          2m
# worker-5d6e7f8a9-jkl78     1/1     Running   0          2m
```

### Step 6: Connect and Test

```bash
# Port forward
kubectl port-forward svc/gateway -n tavana-prod 5432:5432 &

# Connect
psql -h localhost -p 5432 -U tavana -d tavana

# Test query
SELECT 'Tavana is working!' AS message;
```

---

## Scenario B: Restricted with ACR Access

**For: Enterprises with private registries but some Azure connectivity**

Customer can access their ACR from the internet (for image imports) but cluster has no outbound access.

### Step 1: Customer Creates ACR (If Not Exists)

```bash
# Customer runs this
az acr create \
  --resource-group mycompany-rg \
  --name mycompanyacr \
  --sku Standard
```

### Step 2: You Import Images Directly to Their ACR

```bash
# You run this (requires customer to grant you ACR push permission)
# Or they run it with access to Docker Hub

# Login to customer's ACR
az acr login --name mycompanyacr

# Import from Docker Hub to their ACR
az acr import \
  --name mycompanyacr \
  --source docker.io/angelerator/tavana-gateway:v1.0.0 \
  --image tavana-gateway:v1.0.0

az acr import \
  --name mycompanyacr \
  --source docker.io/angelerator/tavana-worker:v1.0.0 \
  --image tavana-worker:v1.0.0
```

### Step 3: Create Values File for Customer

```yaml
# values-mycompany.yaml
global:
  imageRegistry: "mycompanyacr.azurecr.io"
  imageTag: "v1.0.0"
  imagePullPolicy: IfNotPresent

gateway:
  replicaCount: 2
  image:
    repository: tavana-gateway
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "4Gi"
      cpu: "2000m"

worker:
  replicaCount: 2
  minReplicas: 2
  maxReplicas: 10
  image:
    repository: tavana-worker
  resources:
    requests:
      memory: "2Gi"
      cpu: "500m"
    limits:
      memory: "12Gi"
      cpu: "3500m"

serviceAccount:
  create: true
  name: "tavana"
  annotations:
    azure.workload.identity/client-id: "CUSTOMER_CLIENT_ID"
    azure.workload.identity/tenant-id: "CUSTOMER_TENANT_ID"
```

### Step 4: Deploy via Helm

```bash
# Customer runs this (or you run via their bastion)

# Add Helm chart (if they have internet to GHCR)
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  --version 1.0.0 \
  --namespace tavana \
  --create-namespace \
  -f values-mycompany.yaml

# OR use local chart
helm install tavana ./helm/tavana \
  --namespace tavana \
  --create-namespace \
  -f values-mycompany.yaml
```

---

## Scenario C: Standard Enterprise

**For: Most enterprise customers with standard Azure setup**

Customer has outbound internet access but wants proper infrastructure.

### Step 1: Deploy Infrastructure with Terraform

```bash
# Customer clones infra repo
git clone https://github.com/Angelerator/Tavana.git
cd Tavana/terraform/azure/examples/quickstart

# Configure
cp terraform.tfvars.example terraform.tfvars
```

**Edit `terraform.tfvars`:**
```hcl
subscription_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
project_name    = "tavana"
environment     = "prod"
location        = "westeurope"
```

```bash
# Deploy
terraform init
terraform apply
```

**This creates:**
- AKS cluster with system + worker node pools
- ACR (private container registry)
- Storage Account (ADLS Gen2)
- Managed Identity with Workload Identity
- ArgoCD for GitOps
- VNet with proper subnets

### Step 2: Import Images to ACR

```bash
# Get ACR name from Terraform output
ACR_NAME=$(terraform output -raw acr_name)

# Import images
az acr import --name $ACR_NAME \
  --source docker.io/angelerator/tavana-gateway:v1.0.0 \
  --image tavana-gateway:v1.0.0

az acr import --name $ACR_NAME \
  --source docker.io/angelerator/tavana-worker:v1.0.0 \
  --image tavana-worker:v1.0.0
```

### Step 3: Get Kubectl Credentials

```bash
# Configure kubectl
$(terraform output -raw kube_config_command)

# Verify
kubectl get nodes
```

### Step 4: Deploy Tavana

```bash
# Use the generated Helm values
terraform output -raw helm_values_snippet > values-generated.yaml

# Deploy
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  --version 1.0.0 \
  --namespace tavana \
  --create-namespace \
  -f values-generated.yaml
```

### Step 5: Set Up GitOps (Optional but Recommended)

```bash
# Get ArgoCD password
$(terraform output -raw argocd_initial_password_command)

# Access ArgoCD UI
kubectl port-forward svc/argocd-server -n argocd 8080:443 &
open https://localhost:8080

# Create GitOps repo for this customer
# See: gitops-template/README.md
```

---

## Scenario D: Open/Dev Environment

**For: Startups, development, testing**

Simplest deployment with direct access to public registries.

### Step 1: Quick Helm Install

```bash
# Create namespace
kubectl create namespace tavana

# Install directly from GHCR
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  --version 1.0.0 \
  --namespace tavana
```

That's it! Uses Docker Hub images directly.

### Step 2: For Local Development with Kind

```bash
# Create Kind cluster
kind create cluster --name tavana-dev

# Pull and load images
docker pull angelerator/tavana-gateway:v1.0.0
docker pull angelerator/tavana-worker:v1.0.0
kind load docker-image angelerator/tavana-gateway:v1.0.0 --name tavana-dev
kind load docker-image angelerator/tavana-worker:v1.0.0 --name tavana-dev

# Install
helm install tavana ./helm/tavana \
  --namespace tavana \
  --create-namespace \
  --set global.imageRegistry=angelerator \
  --set global.imageTag=v1.0.0 \
  --set gateway.image.repository=tavana-gateway \
  --set worker.image.repository=tavana-worker
```

---

## Post-Deployment Verification

### Health Checks

```bash
# 1. Check all pods are running
kubectl get pods -n tavana -w

# 2. Check services
kubectl get svc -n tavana

# 3. Check logs (gateway)
kubectl logs -n tavana -l app=tavana-gateway --tail=100

# 4. Check logs (worker)
kubectl logs -n tavana -l app=tavana-worker --tail=100

# 5. Check HPA (if enabled)
kubectl get hpa -n tavana
```

### Connectivity Test

```bash
# Port forward
kubectl port-forward svc/gateway -n tavana 5432:5432 &

# Connect with psql
PGPASSWORD=tavana psql -h localhost -p 5432 -U tavana -d tavana

# Run test queries
SELECT version();
SELECT 'Connection successful!' AS status;
```

### Data Lake Test (if configured)

```sql
-- Test reading from S3/ADLS
SELECT * FROM read_parquet('s3://your-bucket/test/*.parquet') LIMIT 10;

-- Or for Azure ADLS Gen2
SELECT * FROM read_parquet('azure://container/path/*.parquet') LIMIT 10;
```

### Metrics Verification

```bash
# Check Prometheus metrics endpoint
kubectl port-forward svc/gateway -n tavana 8080:8080 &
curl http://localhost:8080/metrics | grep tavana_

# Expected metrics:
# tavana_query_queue_depth
# tavana_active_queries
# tavana_query_duration_seconds
```

---

## Troubleshooting

### Issue: Pods stuck in ImagePullBackOff

```bash
# Check what image it's trying to pull
kubectl describe pod <pod-name> -n tavana | grep -A5 "Events:"

# Solution: Ensure images are in ACR and AKS has pull access
az aks update -n <aks-name> -g <rg-name> --attach-acr <acr-name>
```

### Issue: Gateway can't connect to workers

```bash
# Check worker service
kubectl get svc worker -n tavana

# Check network policies
kubectl get networkpolicies -n tavana

# Check worker logs
kubectl logs -n tavana -l app=tavana-worker
```

### Issue: Can't query data lake

```bash
# Check storage credentials
kubectl get secret -n tavana

# Check Workload Identity
kubectl describe serviceaccount tavana -n tavana

# Verify identity has storage permissions in Azure
az role assignment list --assignee <identity-client-id>
```

### Issue: HPA not scaling

```bash
# Check HPA status
kubectl describe hpa -n tavana

# Check metrics-server
kubectl get pods -n kube-system | grep metrics-server

# Check custom metrics (if using queue-based scaling)
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
```

---

## Quick Reference: Deployment Commands

| Scenario | Command |
|----------|---------|
| **A: Air-Gapped** | `./scripts/customer-deploy.sh` |
| **B: ACR Only** | `az acr import` + `helm install` |
| **C: Standard** | `terraform apply` + `helm install` |
| **D: Open/Dev** | `helm install oci://ghcr.io/angelerator/charts/tavana` |

---

## Support Contacts

- **Documentation**: https://github.com/Angelerator/Tavana
- **Issues**: https://github.com/Angelerator/Tavana/issues
- **Email**: support@tavana.io

