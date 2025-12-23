# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Azure Enterprise Example
# ═══════════════════════════════════════════════════════════════════════════════
# Full production-ready configuration with:
# - Application Gateway for ingress with TLS
# - Private AKS cluster (optional)
# - Premium ACR with geo-replication
# - Larger node pools with proper taints
# - Azure Monitor integration
# ═══════════════════════════════════════════════════════════════════════════════

module "tavana" {
  source = "../../"
  
  # Required
  subscription_id = var.subscription_id
  
  # Project settings
  project_name = var.project_name
  environment  = var.environment
  location     = var.location
  
  # Networking
  vnet_address_space = "10.100.0.0/16"
  aks_subnet_cidr    = "10.100.0.0/18"
  appgw_subnet_cidr  = "10.100.64.0/24"
  
  # AKS configuration (production-sized)
  kubernetes_version    = "1.29"
  auto_upgrade_channel  = "stable"
  
  # System pool
  system_node_vm_size   = "Standard_D4s_v3"
  system_node_min_count = 2
  system_node_max_count = 4
  
  # Tavana workload pool (memory-optimized for DuckDB)
  tavana_node_vm_size   = "Standard_E8s_v3"  # 8 vCPU, 64 GB RAM
  tavana_node_min_count = 3
  tavana_node_max_count = 20
  tavana_node_taints    = ["workload=tavana:NoSchedule"]
  
  # Container Registry (Premium for geo-replication)
  acr_sku                       = "Premium"
  acr_geo_replication_locations = var.acr_geo_replication_locations
  
  # Storage (GRS for production)
  storage_replication_type             = "GRS"
  storage_network_rules_default_action = "Deny"
  
  # Enable Application Gateway
  enable_application_gateway = true
  
  # Monitoring
  enable_monitoring  = true
  log_retention_days = 90
  
  # ArgoCD
  install_argocd   = true
  argocd_version   = "5.51.6"
  argocd_hostname  = var.argocd_hostname
  
  # Tags
  tags = {
    Owner       = var.owner_email
    CostCenter  = var.cost_center
    Compliance  = var.compliance_level
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
#                              DNS ZONE (Optional)
# ═══════════════════════════════════════════════════════════════════════════════

data "azurerm_dns_zone" "main" {
  count               = var.dns_zone_name != "" ? 1 : 0
  name                = var.dns_zone_name
  resource_group_name = var.dns_zone_resource_group
}

# ═══════════════════════════════════════════════════════════════════════════════
#                                 OUTPUTS
# ═══════════════════════════════════════════════════════════════════════════════

output "deployment_info" {
  value = <<-EOT

═══════════════════════════════════════════════════════════════════════════════
                   TAVANA ENTERPRISE DEPLOYMENT COMPLETE!
═══════════════════════════════════════════════════════════════════════════════

Resource Group: ${module.tavana.resource_group_name}
AKS Cluster:    ${module.tavana.aks_cluster_name}
ACR:            ${module.tavana.acr_login_server}
Storage:        ${module.tavana.storage_account_name}

─────────────────────────────────────────────────────────────────────────────
                              NEXT STEPS
─────────────────────────────────────────────────────────────────────────────

1. Configure kubectl:
   ${module.tavana.kube_config_command}

2. Import images to ACR (replace VERSION with actual version):
   ./import-images.sh v1.0.0

3. Create GitOps config repository and configure ArgoCD

4. Configure DNS to point to Application Gateway

═══════════════════════════════════════════════════════════════════════════════
EOT
}

output "kube_config_command" {
  value = module.tavana.kube_config_command
}

output "acr_login_server" {
  value = module.tavana.acr_login_server
}

output "helm_values" {
  value     = module.tavana.helm_values_snippet
  sensitive = false
}

# Generate import script
resource "local_file" "import_script" {
  filename = "${path.module}/import-images.sh"
  content  = module.tavana.import_images_script
  file_permission = "0755"
}

