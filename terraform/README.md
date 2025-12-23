# Tavana Infrastructure as Code

This directory contains Terraform modules for deploying Tavana infrastructure across cloud providers.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TAVANA INFRASTRUCTURE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         AZURE (Primary)                                  │ │
│  │                                                                          │ │
│  │   ┌───────────────┐    ┌───────────────┐    ┌───────────────────────┐  │ │
│  │   │     AKS       │    │     ACR       │    │       ADLS Gen2       │  │ │
│  │   │               │    │               │    │                       │  │ │
│  │   │  ┌─────────┐  │    │  tavana/gw    │    │  /data                │  │ │
│  │   │  │ ArgoCD  │  │◄───│  tavana/wk    │    │  /raw                 │  │ │
│  │   │  └─────────┘  │    │               │    │  /processed           │  │ │
│  │   │               │    └───────────────┘    └───────────────────────┘  │ │
│  │   │  ┌─────────┐  │              ▲                      ▲              │ │
│  │   │  │ Tavana  │  │              │                      │              │ │
│  │   │  │ (Helm)  │──┼──────────────┴──────────────────────┘              │ │
│  │   │  └─────────┘  │    Workload Identity (no credentials)              │ │
│  │   │               │                                                     │ │
│  │   └───────────────┘                                                     │ │
│  │                                                                          │ │
│  │   ┌───────────────┐    ┌───────────────┐    ┌───────────────────────┐  │ │
│  │   │  App Gateway  │    │   Key Vault   │    │   Log Analytics       │  │ │
│  │   │  (Ingress)    │    │  (Secrets)    │    │   (Monitoring)        │  │ │
│  │   └───────────────┘    └───────────────┘    └───────────────────────┘  │ │
│  │                                                                          │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    AWS (Coming Soon)      GCP (Coming Soon)             │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📁 Directory Structure

```
terraform/
├── azure/                      # Azure module
│   ├── main.tf                 # Main resources
│   ├── variables.tf            # Input variables
│   ├── outputs.tf              # Output values
│   ├── versions.tf             # Provider versions
│   └── examples/
│       ├── quickstart/         # Minimal setup for testing
│       └── enterprise/         # Production-ready setup
├── aws/                        # AWS module (coming soon)
├── gcp/                        # GCP module (coming soon)
└── README.md                   # This file
```

## 🚀 Quick Start (Azure)

### Prerequisites

1. [Terraform](https://www.terraform.io/downloads) >= 1.5.0
2. [Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli)
3. Azure subscription with Owner or Contributor access

### Step 1: Authenticate

```bash
az login
az account set --subscription "YOUR_SUBSCRIPTION_ID"
```

### Step 2: Clone and Configure

```bash
cd terraform/azure/examples/quickstart
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
```

### Step 3: Deploy

```bash
terraform init
terraform plan
terraform apply
```

### Step 4: Configure kubectl

```bash
# The command is output by Terraform
az aks get-credentials --resource-group my-resource-group --name my-aks-cluster
```

### Step 5: Import Images to ACR

```bash
# From the quickstart directory, run the generated script
./import-images.sh v1.0.0
```

### Step 6: Deploy Tavana with ArgoCD

1. Get ArgoCD password:
   ```bash
   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d
   ```

2. Port-forward ArgoCD:
   ```bash
   kubectl port-forward svc/argocd-server -n argocd 8080:443
   ```

3. Open https://localhost:8080 and login with `admin` and the password from step 1

4. Add your GitOps repository and create an Application

## 🏢 Enterprise Deployment

For production deployments, use the enterprise example:

```bash
cd terraform/azure/examples/enterprise
cp terraform.tfvars.example terraform.tfvars
# Edit with production values
terraform init
terraform plan
terraform apply
```

### Enterprise Features

- **Application Gateway** - L7 load balancer with WAF, TLS termination
- **Premium ACR** - Geo-replication, content trust, retention policies
- **GRS Storage** - Geo-redundant storage for disaster recovery
- **Larger Nodes** - Memory-optimized VMs (E8s_v3: 8 vCPU, 64 GB)
- **Azure Monitor** - Container Insights, Log Analytics
- **Private Cluster** - Optional VNet-only access

## 📋 Input Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `subscription_id` | Azure Subscription ID | (required) |
| `project_name` | Project name for resource naming | `tavana` |
| `environment` | Environment (dev/staging/prod) | `prod` |
| `location` | Azure region | `westeurope` |
| `kubernetes_version` | AKS Kubernetes version | `1.29` |
| `tavana_node_vm_size` | VM size for Tavana nodes | `Standard_E4s_v3` |
| `tavana_node_min_count` | Min nodes for autoscaling | `2` |
| `tavana_node_max_count` | Max nodes for autoscaling | `10` |
| `install_argocd` | Install ArgoCD for GitOps | `true` |
| `enable_application_gateway` | Enable App Gateway ingress | `true` |

See `variables.tf` for complete list.

## 📤 Outputs

| Output | Description |
|--------|-------------|
| `kube_config_command` | Command to configure kubectl |
| `acr_login_server` | ACR login URL |
| `storage_account_name` | ADLS storage account |
| `helm_values_snippet` | Helm values for Tavana deployment |
| `import_images_script` | Script to import images to ACR |

## 🔐 Security Best Practices

1. **Workload Identity** - Pods authenticate to Azure services without credentials
2. **Private Endpoints** - Storage and ACR accessible only from VNet
3. **Network Policies** - Calico for pod-to-pod traffic control
4. **RBAC** - Kubernetes RBAC integrated with Azure AD
5. **Secrets** - Use External Secrets Operator with Key Vault

## 🔧 Customization

### Using Existing Resources

If you have existing VNet, ACR, or Storage:

```hcl
module "tavana" {
  source = "../../"
  
  # Use existing VNet
  create_vnet = false
  existing_vnet_id = "/subscriptions/.../resourceGroups/.../providers/Microsoft.Network/virtualNetworks/existing-vnet"
  existing_subnet_id = "/subscriptions/.../resourceGroups/.../providers/Microsoft.Network/virtualNetworks/existing-vnet/subnets/aks"
  
  # ... other config
}
```

### Custom Node Pools

```hcl
module "tavana" {
  source = "../../"
  
  # Memory-optimized for large datasets
  tavana_node_vm_size = "Standard_E16s_v3"  # 16 vCPU, 128 GB
  tavana_node_min_count = 4
  tavana_node_max_count = 50
  
  # ... other config
}
```

## 🧹 Cleanup

```bash
terraform destroy
```

## 📚 Related Documentation

- [Helm Chart README](../helm/tavana/README.md)
- [GitOps Config Template](https://github.com/tavana/tavana-config-template)
- [Azure AKS Best Practices](https://docs.microsoft.com/azure/aks/best-practices)

