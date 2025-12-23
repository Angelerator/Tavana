# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Enterprise Variables
# ═══════════════════════════════════════════════════════════════════════════════

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "project_name" {
  description = "Project name (used in resource naming)"
  type        = string
  default     = "tavana"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope"
}

variable "owner_email" {
  description = "Owner email for tagging"
  type        = string
}

variable "cost_center" {
  description = "Cost center for billing"
  type        = string
  default     = ""
}

variable "compliance_level" {
  description = "Compliance level (e.g., HIPAA, SOC2, GDPR)"
  type        = string
  default     = "standard"
}

variable "acr_geo_replication_locations" {
  description = "ACR geo-replication locations"
  type        = list(string)
  default     = ["northeurope"]
}

variable "argocd_hostname" {
  description = "ArgoCD hostname for ingress"
  type        = string
  default     = ""
}

variable "dns_zone_name" {
  description = "Azure DNS zone name (if using Azure DNS)"
  type        = string
  default     = ""
}

variable "dns_zone_resource_group" {
  description = "Resource group containing DNS zone"
  type        = string
  default     = ""
}

