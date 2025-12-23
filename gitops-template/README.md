# Tavana GitOps Configuration Template

This is a template repository for managing your Tavana deployment using GitOps with ArgoCD.

## 🚀 Quick Start

### 1. Use This Template

Click "Use this template" on GitHub to create your own repository.

### 2. Configure Your Values

Edit `apps/tavana.yaml` with your specific values from the Terraform outputs:

```yaml
global:
  imageRegistry: "your-acr.azurecr.io"      # From: terraform output acr_login_server
  imageTag: "v1.0.0"

serviceAccount:
  annotations:
    azure.workload.identity/client-id: "xxx"  # From: terraform output tavana_identity_client_id
    azure.workload.identity/tenant-id: "xxx"  # From: terraform output tavana_identity_tenant_id

gateway:
  env:
    - name: AZURE_STORAGE_ACCOUNT
      value: "your-storage-account"           # From: terraform output storage_account_name
```

### 3. Connect ArgoCD to This Repository

```bash
# Create a deploy key or use a GitHub App for ArgoCD access
# Then add the repository to ArgoCD:

argocd repo add https://github.com/YOUR_ORG/tavana-config \
  --ssh-private-key-path ~/.ssh/argocd-deploy-key

# Or using the ArgoCD UI:
# Settings → Repositories → Connect Repo
```

### 4. Apply the Application

```bash
kubectl apply -f apps/tavana.yaml
```

ArgoCD will now continuously sync your Tavana deployment!

## 📁 Directory Structure

```
├── apps/
│   ├── tavana.yaml      # Main Tavana application
│   └── monitoring.yaml  # Optional: Prometheus + Grafana
├── values/
│   └── overrides.yaml   # Additional value overrides (optional)
├── secrets/
│   └── .gitkeep         # Encrypted secrets (SealedSecrets/SOPS)
└── README.md
```

## 🔄 Upgrading Tavana

To upgrade to a new version:

1. Update `targetRevision` in `apps/tavana.yaml`:
   ```yaml
   source:
     targetRevision: "1.1.0"  # New version
   ```

2. Commit and push:
   ```bash
   git add apps/tavana.yaml
   git commit -m "chore: upgrade Tavana to v1.1.0"
   git push
   ```

3. ArgoCD will automatically detect the change and upgrade.

## 🔐 Managing Secrets

For sensitive values, use one of these approaches:

### Option 1: Sealed Secrets

```bash
# Install kubeseal
brew install kubeseal

# Create sealed secret
kubectl create secret generic tavana-secrets \
  --from-literal=api-key=xxx \
  --dry-run=client -o yaml | \
  kubeseal --format yaml > secrets/tavana-secrets.yaml
```

### Option 2: External Secrets Operator

```yaml
# secrets/external-secret.yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: tavana-secrets
  namespace: tavana
spec:
  refreshInterval: 1h
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

## 📊 Monitoring (Optional)

The `apps/monitoring.yaml` deploys Prometheus and Grafana. If you're using Azure Monitor, you can delete this file.

To access Grafana:

```bash
kubectl port-forward svc/monitoring-grafana -n monitoring 3000:80
# Open http://localhost:3000
# Default: admin / changeme
```

## 🆘 Troubleshooting

### Check Sync Status

```bash
argocd app get tavana
```

### View Sync Diff

```bash
argocd app diff tavana
```

### Force Sync

```bash
argocd app sync tavana --force
```

### View Logs

```bash
argocd app logs tavana
```

