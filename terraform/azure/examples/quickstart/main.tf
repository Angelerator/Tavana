# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Azure Quickstart Example
# ═══════════════════════════════════════════════════════════════════════════════
# This is a minimal example to get Tavana running on Azure.
# For production, see the 'enterprise' example.
# ═══════════════════════════════════════════════════════════════════════════════

module "tavana" {
  source = "../../"
  
  # Required
  subscription_id = var.subscription_id
  
  # Project settings
  project_name = var.project_name
  environment  = var.environment
  location     = var.location
  
  # AKS configuration (small for quickstart)
  kubernetes_version    = "1.29"
  system_node_vm_size   = "Standard_D2s_v3"
  system_node_min_count = 1
  system_node_max_count = 2
  tavana_node_vm_size   = "Standard_E4s_v3"  # 4 vCPU, 32 GB (memory optimized)
  tavana_node_min_count = 1
  tavana_node_max_count = 5
  
  # Disable Application Gateway for simplicity (use LoadBalancer instead)
  enable_application_gateway = false
  
  # Enable ArgoCD
  install_argocd = true
  
  # Tags
  tags = {
    Owner = var.owner_email
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
#                                 OUTPUTS
# ═══════════════════════════════════════════════════════════════════════════════

output "next_steps" {
  value = <<-EOT

═══════════════════════════════════════════════════════════════════════════════
                        TAVANA DEPLOYMENT COMPLETE!
═══════════════════════════════════════════════════════════════════════════════

1. Configure kubectl:
   ${module.tavana.kube_config_command}

2. Import Tavana images to your ACR (run this script):
   Save the import_images_script output and execute it with version tag:
   ./import_images.sh v1.0.0

3. Get ArgoCD password:
   ${module.tavana.argocd_initial_password_command}

4. Access ArgoCD:
   ${module.tavana.argocd_server_url}

5. Create your GitOps config repository using the template at:
   https://github.com/Angelerator/Tavana/tree/main/gitops-template

6. Add the following to your GitOps values file:
${module.tavana.helm_values_snippet}

7. Install Tavana via Helm (or use ArgoCD):
   helm upgrade --install tavana oci://ghcr.io/angelerator/charts/tavana \
     --namespace tavana \
     -f your-values.yaml

═══════════════════════════════════════════════════════════════════════════════
EOT
}

output "acr_login_server" {
  value = module.tavana.acr_login_server
}

output "storage_account" {
  value = module.tavana.storage_account_name
}

