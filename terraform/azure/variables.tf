# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Azure Variables
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
#                              GENERAL
# ─────────────────────────────────────────────────────────────────────────────

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "project_name" {
  description = "Project name used in resource naming"
  type        = string
  default     = "tavana"
  
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.project_name))
    error_message = "Project name must be lowercase alphanumeric with hyphens, 2-21 chars, starting with a letter."
  }
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope"
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

# ─────────────────────────────────────────────────────────────────────────────
#                              NETWORKING
# ─────────────────────────────────────────────────────────────────────────────

variable "vnet_address_space" {
  description = "Virtual network address space"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aks_subnet_cidr" {
  description = "AKS subnet CIDR"
  type        = string
  default     = "10.0.0.0/20"
}

variable "appgw_subnet_cidr" {
  description = "Application Gateway subnet CIDR"
  type        = string
  default     = "10.0.16.0/24"
}

variable "service_cidr" {
  description = "Kubernetes service CIDR"
  type        = string
  default     = "10.1.0.0/16"
}

variable "dns_service_ip" {
  description = "Kubernetes DNS service IP"
  type        = string
  default     = "10.1.0.10"
}

# ─────────────────────────────────────────────────────────────────────────────
#                                AKS
# ─────────────────────────────────────────────────────────────────────────────

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "auto_upgrade_channel" {
  description = "Auto-upgrade channel for AKS (none, patch, rapid, stable, node-image)"
  type        = string
  default     = "stable"
}

# System node pool
variable "system_node_vm_size" {
  description = "VM size for system node pool"
  type        = string
  default     = "Standard_D2s_v3"
}

variable "system_node_min_count" {
  description = "Minimum nodes in system pool"
  type        = number
  default     = 1
}

variable "system_node_max_count" {
  description = "Maximum nodes in system pool"
  type        = number
  default     = 3
}

# Tavana workload node pool
variable "tavana_node_vm_size" {
  description = "VM size for Tavana workload nodes (memory-optimized recommended)"
  type        = string
  default     = "Standard_E4s_v3"  # 4 vCPU, 32 GB RAM
}

variable "tavana_node_min_count" {
  description = "Minimum nodes for Tavana workloads"
  type        = number
  default     = 2
}

variable "tavana_node_max_count" {
  description = "Maximum nodes for Tavana workloads"
  type        = number
  default     = 10
}

variable "tavana_node_taints" {
  description = "Taints for Tavana node pool"
  type        = list(string)
  default     = ["workload=tavana:NoSchedule"]
}

# ─────────────────────────────────────────────────────────────────────────────
#                          CONTAINER REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

variable "acr_sku" {
  description = "ACR SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Standard"
}

variable "acr_geo_replication_locations" {
  description = "Geo-replication locations for Premium ACR"
  type        = list(string)
  default     = []
}

# ─────────────────────────────────────────────────────────────────────────────
#                              STORAGE
# ─────────────────────────────────────────────────────────────────────────────

variable "storage_replication_type" {
  description = "Storage replication type (LRS, GRS, ZRS, RAGRS)"
  type        = string
  default     = "ZRS"
}

variable "storage_network_rules_default_action" {
  description = "Default action for storage network rules (Allow, Deny)"
  type        = string
  default     = "Allow"
}

# ─────────────────────────────────────────────────────────────────────────────
#                             MONITORING
# ─────────────────────────────────────────────────────────────────────────────

variable "enable_monitoring" {
  description = "Enable Azure Monitor for AKS"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Log Analytics retention in days"
  type        = number
  default     = 30
}

# ─────────────────────────────────────────────────────────────────────────────
#                         APPLICATION GATEWAY
# ─────────────────────────────────────────────────────────────────────────────

variable "enable_application_gateway" {
  description = "Enable Azure Application Gateway as Ingress"
  type        = bool
  default     = true
}

# ─────────────────────────────────────────────────────────────────────────────
#                              ARGOCD
# ─────────────────────────────────────────────────────────────────────────────

variable "install_argocd" {
  description = "Install ArgoCD for GitOps"
  type        = bool
  default     = true
}

variable "argocd_version" {
  description = "ArgoCD Helm chart version"
  type        = string
  default     = "5.51.6"
}

variable "argocd_hostname" {
  description = "ArgoCD hostname (leave empty for no ingress)"
  type        = string
  default     = ""
}

# ─────────────────────────────────────────────────────────────────────────────
#                              TAVANA
# ─────────────────────────────────────────────────────────────────────────────

variable "tavana_namespace" {
  description = "Kubernetes namespace for Tavana"
  type        = string
  default     = "tavana"
}

variable "tavana_service_account_name" {
  description = "Service account name for Tavana pods"
  type        = string
  default     = "tavana"
}

