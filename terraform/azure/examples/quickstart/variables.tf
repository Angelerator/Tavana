# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Quickstart Variables
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
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope"
}

variable "owner_email" {
  description = "Owner email for tagging"
  type        = string
  default     = ""
}

