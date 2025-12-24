# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Azure Outputs
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
#                              GENERAL
# ─────────────────────────────────────────────────────────────────────────────

output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.main.name
}

output "resource_group_id" {
  description = "Resource group ID"
  value       = azurerm_resource_group.main.id
}

# ─────────────────────────────────────────────────────────────────────────────
#                                AKS
# ─────────────────────────────────────────────────────────────────────────────

output "aks_cluster_name" {
  description = "AKS cluster name"
  value       = azurerm_kubernetes_cluster.main.name
}

output "aks_cluster_id" {
  description = "AKS cluster ID"
  value       = azurerm_kubernetes_cluster.main.id
}

output "aks_cluster_fqdn" {
  description = "AKS cluster FQDN"
  value       = azurerm_kubernetes_cluster.main.fqdn
}

output "aks_oidc_issuer_url" {
  description = "AKS OIDC issuer URL for Workload Identity"
  value       = azurerm_kubernetes_cluster.main.oidc_issuer_url
}

output "kube_config" {
  description = "Kubernetes config for connecting to cluster"
  value       = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive   = true
}

output "kube_config_command" {
  description = "Command to configure kubectl"
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.main.name} --name ${azurerm_kubernetes_cluster.main.name}"
}

# ─────────────────────────────────────────────────────────────────────────────
#                          CONTAINER REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

output "acr_name" {
  description = "ACR name"
  value       = azurerm_container_registry.main.name
}

output "acr_login_server" {
  description = "ACR login server URL"
  value       = azurerm_container_registry.main.login_server
}

output "acr_id" {
  description = "ACR resource ID"
  value       = azurerm_container_registry.main.id
}

# ─────────────────────────────────────────────────────────────────────────────
#                              STORAGE
# ─────────────────────────────────────────────────────────────────────────────

output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.main.name
}

output "storage_account_id" {
  description = "Storage account ID"
  value       = azurerm_storage_account.main.id
}

output "storage_primary_dfs_endpoint" {
  description = "Primary DFS endpoint for ADLS Gen2"
  value       = azurerm_storage_account.main.primary_dfs_endpoint
}

output "storage_primary_blob_endpoint" {
  description = "Primary blob endpoint"
  value       = azurerm_storage_account.main.primary_blob_endpoint
}

output "data_container_name" {
  description = "Name of the data container"
  value       = azurerm_storage_container.data.name
}

# ─────────────────────────────────────────────────────────────────────────────
#                           IDENTITIES
# ─────────────────────────────────────────────────────────────────────────────

output "tavana_identity_client_id" {
  description = "Tavana managed identity client ID (for Workload Identity)"
  value       = azurerm_user_assigned_identity.tavana.client_id
}

output "tavana_identity_principal_id" {
  description = "Tavana managed identity principal ID"
  value       = azurerm_user_assigned_identity.tavana.principal_id
}

output "tavana_identity_tenant_id" {
  description = "Tavana managed identity tenant ID"
  value       = data.azuread_client_config.current.tenant_id
}

# ─────────────────────────────────────────────────────────────────────────────
#                             ARGOCD
# ─────────────────────────────────────────────────────────────────────────────

output "argocd_server_url" {
  description = "ArgoCD server URL (if installed)"
  value       = var.install_argocd ? (var.argocd_hostname != "" ? "https://${var.argocd_hostname}" : "Run: kubectl port-forward svc/argocd-server -n argocd 8080:443") : "ArgoCD not installed"
}

output "argocd_initial_password_command" {
  description = "Command to get ArgoCD initial admin password"
  value       = var.install_argocd ? "kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d" : "N/A"
}

# ─────────────────────────────────────────────────────────────────────────────
#                              TAVANA
# ─────────────────────────────────────────────────────────────────────────────

output "tavana_namespace" {
  description = "Tavana Kubernetes namespace"
  value       = kubernetes_namespace.tavana.metadata[0].name
}

output "tavana_service_account" {
  description = "Tavana service account name"
  value       = kubernetes_service_account.tavana.metadata[0].name
}

# ─────────────────────────────────────────────────────────────────────────────
#                         HELM VALUES (for ArgoCD)
# ─────────────────────────────────────────────────────────────────────────────

output "helm_values_snippet" {
  description = "Helm values snippet for Tavana deployment"
  value       = <<-EOT
# Copy this to your tavana-config repo's values file
global:
  imageRegistry: "${azurerm_container_registry.main.login_server}"
  imageTag: "v1.0.0"  # Replace with specific version

gateway:
  image:
    repository: tavana-gateway
  env:
    - name: AZURE_STORAGE_ACCOUNT
      value: "${azurerm_storage_account.main.name}"
    - name: AZURE_STORAGE_CONTAINER
      value: "${azurerm_storage_container.data.name}"
    - name: AZURE_STORAGE_ENDPOINT
      value: "${azurerm_storage_account.main.primary_dfs_endpoint}"

worker:
  image:
    repository: tavana-worker
  nodeSelector:
    nodepool: tavana
  tolerations:
    - key: "workload"
      operator: "Equal"
      value: "tavana"
      effect: "NoSchedule"

serviceAccount:
  create: false  # Already created by Terraform
  name: "${var.tavana_service_account_name}"
EOT
}

# ─────────────────────────────────────────────────────────────────────────────
#                       IMPORT IMAGES SCRIPT
# ─────────────────────────────────────────────────────────────────────────────

output "import_images_script" {
  description = "Script to import Tavana images from Docker Hub to ACR"
  value       = <<-EOT
#!/bin/bash
# Import Tavana images from Docker Hub to your private ACR
# Run this after initial deployment or when upgrading

VERSION="${"$"}{1:-latest}"
ACR_NAME="${azurerm_container_registry.main.name}"

echo "Importing Tavana images version: $VERSION"

az acr login --name $ACR_NAME

# Import gateway (from angelerator Docker Hub)
az acr import \
  --name $ACR_NAME \
  --source docker.io/angelerator/tavana-gateway:$VERSION \
  --image tavana-gateway:$VERSION \
  --force

# Import worker (from angelerator Docker Hub)
az acr import \
  --name $ACR_NAME \
  --source docker.io/angelerator/tavana-worker:$VERSION \
  --image tavana-worker:$VERSION \
  --force

echo "✓ Images imported successfully"
echo "Use: ${azurerm_container_registry.main.login_server}/tavana-gateway:$VERSION"
echo "Use: ${azurerm_container_registry.main.login_server}/tavana-worker:$VERSION"
EOT
}

