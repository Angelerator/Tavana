# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Azure Infrastructure Module
# ═══════════════════════════════════════════════════════════════════════════════
# This Terraform module provisions all Azure infrastructure required for Tavana:
# - Azure Kubernetes Service (AKS) with autoscaling
# - Azure Container Registry (ACR) for private images
# - Azure Data Lake Storage Gen2 (ADLS) for data
# - Managed Identities for secure access
# - Application Gateway for ingress (optional)
# - ArgoCD for GitOps deployments
# ═══════════════════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# ═══════════════════════════════════════════════════════════════════════════════
#                                 PROVIDERS
# ═══════════════════════════════════════════════════════════════════════════════

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
}

provider "azuread" {}

provider "helm" {
  kubernetes {
    host                   = azurerm_kubernetes_cluster.main.kube_config[0].host
    client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_certificate)
    client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].cluster_ca_certificate)
  }
}

provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.main.kube_config[0].host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].cluster_ca_certificate)
}

# ═══════════════════════════════════════════════════════════════════════════════
#                              DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

data "azurerm_subscription" "current" {}

data "azuread_client_config" "current" {}

# ═══════════════════════════════════════════════════════════════════════════════
#                              LOCALS
# ═══════════════════════════════════════════════════════════════════════════════

locals {
  # Naming convention: {project}-{environment}-{resource}-{location_short}
  name_prefix = "${var.project_name}-${var.environment}"
  
  location_short = {
    "westeurope"    = "we"
    "northeurope"   = "ne"
    "eastus"        = "eus"
    "eastus2"       = "eus2"
    "westus"        = "wus"
    "westus2"       = "wus2"
    "centralus"     = "cus"
    "southeastasia" = "sea"
  }
  
  loc_short = lookup(local.location_short, var.location, substr(var.location, 0, 3))
  
  common_tags = merge(var.tags, {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Component   = "Tavana"
  })
}

# ═══════════════════════════════════════════════════════════════════════════════
#                            RESOURCE GROUP
# ═══════════════════════════════════════════════════════════════════════════════

resource "azurerm_resource_group" "main" {
  name     = "${local.name_prefix}-rg-${local.loc_short}"
  location = var.location
  tags     = local.common_tags
}

# ═══════════════════════════════════════════════════════════════════════════════
#                            NETWORKING
# ═══════════════════════════════════════════════════════════════════════════════

resource "azurerm_virtual_network" "main" {
  name                = "${local.name_prefix}-vnet-${local.loc_short}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = [var.vnet_address_space]
  tags                = local.common_tags
}

resource "azurerm_subnet" "aks" {
  name                 = "aks-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.aks_subnet_cidr]
}

resource "azurerm_subnet" "appgw" {
  count                = var.enable_application_gateway ? 1 : 0
  name                 = "appgw-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.appgw_subnet_cidr]
}

# ═══════════════════════════════════════════════════════════════════════════════
#                         CONTAINER REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

resource "random_string" "acr_suffix" {
  length  = 4
  special = false
  upper   = false
}

resource "azurerm_container_registry" "main" {
  name                = "${replace(local.name_prefix, "-", "")}acr${random_string.acr_suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.acr_sku
  admin_enabled       = false
  
  # Enable geo-replication for Premium SKU
  dynamic "georeplications" {
    for_each = var.acr_sku == "Premium" ? var.acr_geo_replication_locations : []
    content {
      location                = georeplications.value
      zone_redundancy_enabled = true
    }
  }
  
  tags = local.common_tags
}

# ═══════════════════════════════════════════════════════════════════════════════
#                            STORAGE ACCOUNT
# ═══════════════════════════════════════════════════════════════════════════════

resource "random_string" "storage_suffix" {
  length  = 4
  special = false
  upper   = false
}

resource "azurerm_storage_account" "main" {
  name                     = "${replace(var.project_name, "-", "")}${var.environment}sa${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Enable hierarchical namespace for ADLS Gen2
  
  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 7
    }
    
    container_delete_retention_policy {
      days = 7
    }
  }
  
  network_rules {
    default_action             = var.storage_network_rules_default_action
    virtual_network_subnet_ids = [azurerm_subnet.aks.id]
    bypass                     = ["AzureServices"]
  }
  
  tags = local.common_tags
}

resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# ═══════════════════════════════════════════════════════════════════════════════
#                           MANAGED IDENTITIES
# ═══════════════════════════════════════════════════════════════════════════════

# User-assigned identity for AKS
resource "azurerm_user_assigned_identity" "aks" {
  name                = "${local.name_prefix}-aks-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}

# User-assigned identity for Tavana workloads (Workload Identity)
resource "azurerm_user_assigned_identity" "tavana" {
  name                = "${local.name_prefix}-tavana-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}

# ═══════════════════════════════════════════════════════════════════════════════
#                             ROLE ASSIGNMENTS
# ═══════════════════════════════════════════════════════════════════════════════

# AKS identity can pull images from ACR
resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}

# Tavana identity can read/write to storage
resource "azurerm_role_assignment" "tavana_storage" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.tavana.principal_id
}

# ═══════════════════════════════════════════════════════════════════════════════
#                        AZURE KUBERNETES SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

resource "azurerm_kubernetes_cluster" "main" {
  name                = "${local.name_prefix}-aks-${local.loc_short}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = "${var.project_name}-${var.environment}"
  kubernetes_version  = var.kubernetes_version
  
  # System node pool
  default_node_pool {
    name                = "system"
    vm_size             = var.system_node_vm_size
    enable_auto_scaling = true
    min_count           = var.system_node_min_count
    max_count           = var.system_node_max_count
    vnet_subnet_id      = azurerm_subnet.aks.id
    os_disk_size_gb     = 128
    os_disk_type        = "Managed"
    
    node_labels = {
      "nodepool" = "system"
    }
    
    upgrade_settings {
      max_surge = "33%"
    }
  }
  
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aks.id]
  }
  
  # Enable OIDC for Workload Identity
  oidc_issuer_enabled       = true
  workload_identity_enabled = true
  
  network_profile {
    network_plugin    = "azure"
    network_policy    = "calico"
    load_balancer_sku = "standard"
    service_cidr      = var.service_cidr
    dns_service_ip    = var.dns_service_ip
  }
  
  # Enable Azure Monitor
  dynamic "oms_agent" {
    for_each = var.enable_monitoring ? [1] : []
    content {
      log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id
    }
  }
  
  # Enable Application Gateway Ingress Controller
  dynamic "ingress_application_gateway" {
    for_each = var.enable_application_gateway ? [1] : []
    content {
      subnet_id = azurerm_subnet.appgw[0].id
    }
  }
  
  # Auto-upgrade settings
  automatic_channel_upgrade = var.auto_upgrade_channel
  
  maintenance_window_auto_upgrade {
    frequency   = "Weekly"
    interval    = 1
    duration    = 4
    day_of_week = "Sunday"
    start_time  = "02:00"
    utc_offset  = "+00:00"
  }
  
  tags = local.common_tags
}

# Tavana workload node pool (optimized for DuckDB)
resource "azurerm_kubernetes_cluster_node_pool" "tavana" {
  name                  = "tavana"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size               = var.tavana_node_vm_size
  enable_auto_scaling   = true
  min_count             = var.tavana_node_min_count
  max_count             = var.tavana_node_max_count
  vnet_subnet_id        = azurerm_subnet.aks.id
  os_disk_size_gb       = 256
  os_disk_type          = "Managed"
  
  node_labels = {
    "nodepool"              = "tavana"
    "workload"              = "duckdb"
    "tavana.io/node-type"   = "worker"
  }
  
  node_taints = var.tavana_node_taints
  
  upgrade_settings {
    max_surge = "33%"
  }
  
  tags = local.common_tags
}

# ═══════════════════════════════════════════════════════════════════════════════
#                          WORKLOAD IDENTITY
# ═══════════════════════════════════════════════════════════════════════════════

resource "azurerm_federated_identity_credential" "tavana" {
  name                = "tavana-federated-credential"
  resource_group_name = azurerm_resource_group.main.name
  parent_id           = azurerm_user_assigned_identity.tavana.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.main.oidc_issuer_url
  subject             = "system:serviceaccount:${var.tavana_namespace}:${var.tavana_service_account_name}"
}

# ═══════════════════════════════════════════════════════════════════════════════
#                           LOG ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════

resource "azurerm_log_analytics_workspace" "main" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "${local.name_prefix}-law-${local.loc_short}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  tags                = local.common_tags
}

# ═══════════════════════════════════════════════════════════════════════════════
#                              ARGOCD
# ═══════════════════════════════════════════════════════════════════════════════

resource "kubernetes_namespace" "argocd" {
  count = var.install_argocd ? 1 : 0
  
  metadata {
    name = "argocd"
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
  
  depends_on = [azurerm_kubernetes_cluster.main]
}

resource "helm_release" "argocd" {
  count = var.install_argocd ? 1 : 0
  
  name       = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  version    = var.argocd_version
  namespace  = kubernetes_namespace.argocd[0].metadata[0].name
  
  values = [
    yamlencode({
      server = {
        service = {
          type = "LoadBalancer"
        }
        ingress = {
          enabled = var.enable_application_gateway
          ingressClassName = "azure-application-gateway"
          hosts = var.argocd_hostname != "" ? [var.argocd_hostname] : []
          tls = var.argocd_hostname != "" ? [{
            secretName = "argocd-tls"
            hosts      = [var.argocd_hostname]
          }] : []
        }
      }
      configs = {
        params = {
          "server.insecure" = !var.enable_application_gateway
        }
      }
    })
  ]
  
  depends_on = [
    azurerm_kubernetes_cluster.main,
    kubernetes_namespace.argocd
  ]
}

# ═══════════════════════════════════════════════════════════════════════════════
#                           TAVANA NAMESPACE
# ═══════════════════════════════════════════════════════════════════════════════

resource "kubernetes_namespace" "tavana" {
  metadata {
    name = var.tavana_namespace
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
      "azure.workload.identity/use"  = "true"
    }
  }
  
  depends_on = [azurerm_kubernetes_cluster.main]
}

# Service account for Tavana with Workload Identity
resource "kubernetes_service_account" "tavana" {
  metadata {
    name      = var.tavana_service_account_name
    namespace = kubernetes_namespace.tavana.metadata[0].name
    annotations = {
      "azure.workload.identity/client-id" = azurerm_user_assigned_identity.tavana.client_id
      "azure.workload.identity/tenant-id" = data.azuread_client_config.current.tenant_id
    }
    labels = {
      "azure.workload.identity/use" = "true"
    }
  }
  
  depends_on = [
    kubernetes_namespace.tavana,
    azurerm_federated_identity_credential.tavana
  ]
}

