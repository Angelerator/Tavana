#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                TAVANA - Prepare Offline/Restricted Deployment Package
# ═══════════════════════════════════════════════════════════════════════════════
# This script creates a self-contained package for deploying Tavana in 
# network-restricted environments (like Nokia's Azure setup).
#
# The package includes:
#   - Docker images as tarballs (can be imported to customer's ACR)
#   - Helm chart as a .tgz file
#   - Import scripts
#   - Customer values template
#
# Usage:
#   ./prepare-offline-package.sh [VERSION]
#   Example: ./prepare-offline-package.sh v1.0.0
# ═══════════════════════════════════════════════════════════════════════════════

set -e

VERSION="${1:-latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PACKAGE_DIR="$PROJECT_ROOT/dist/tavana-offline-$VERSION"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_step() { echo -e "\n${GREEN}▶ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_error() { echo -e "${RED}✖ $1${NC}"; }
print_success() { echo -e "${GREEN}✔ $1${NC}"; }

echo -e "${BLUE}"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "              TAVANA - Offline Package Builder"
echo "              Version: $VERSION"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo -e "${NC}"

# ─────────────────────────────────────────────────────────────────────────────
#                           PREPARE DIRECTORIES
# ─────────────────────────────────────────────────────────────────────────────

print_step "Creating package directory..."
rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR/images"
mkdir -p "$PACKAGE_DIR/helm"
mkdir -p "$PACKAGE_DIR/scripts"
mkdir -p "$PACKAGE_DIR/terraform"

# ─────────────────────────────────────────────────────────────────────────────
#                           PULL & SAVE IMAGES
# ─────────────────────────────────────────────────────────────────────────────

print_step "Pulling Docker images..."

GATEWAY_IMAGE="angelerator/tavana-gateway:$VERSION"
WORKER_IMAGE="angelerator/tavana-worker:$VERSION"

docker pull "$GATEWAY_IMAGE" --platform linux/amd64
docker pull "$WORKER_IMAGE" --platform linux/amd64

print_step "Saving images as tarballs..."

docker save "$GATEWAY_IMAGE" | gzip > "$PACKAGE_DIR/images/tavana-gateway-$VERSION.tar.gz"
docker save "$WORKER_IMAGE" | gzip > "$PACKAGE_DIR/images/tavana-worker-$VERSION.tar.gz"

print_success "Images saved"

# ─────────────────────────────────────────────────────────────────────────────
#                           PACKAGE HELM CHART
# ─────────────────────────────────────────────────────────────────────────────

print_step "Packaging Helm chart..."

helm package "$PROJECT_ROOT/helm/tavana" --destination "$PACKAGE_DIR/helm" --version "${VERSION#v}"

print_success "Helm chart packaged"

# ─────────────────────────────────────────────────────────────────────────────
#                           CREATE IMPORT SCRIPT
# ─────────────────────────────────────────────────────────────────────────────

print_step "Creating import scripts..."

cat > "$PACKAGE_DIR/scripts/import-to-acr.sh" << 'SCRIPT'
#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                    Import Tavana Images to Azure ACR
# ═══════════════════════════════════════════════════════════════════════════════
# This script imports the pre-downloaded Tavana images into your private ACR.
#
# Prerequisites:
#   - Azure CLI installed and logged in
#   - Access to the target ACR
#
# Usage:
#   ./import-to-acr.sh <ACR_NAME> [VERSION]
#   Example: ./import-to-acr.sh mycompanyacr v1.0.0
# ═══════════════════════════════════════════════════════════════════════════════

set -e

ACR_NAME="${1:?Usage: $0 <ACR_NAME> [VERSION]}"
VERSION="${2:-latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGES_DIR="$(dirname "$SCRIPT_DIR")/images"

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    Importing Tavana Images to ACR"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "  ACR:     $ACR_NAME"
echo "  Version: $VERSION"
echo ""

# Login to ACR
echo "▶ Logging into ACR..."
az acr login --name "$ACR_NAME"

ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)

# Load and push gateway
echo "▶ Importing Gateway image..."
docker load < "$IMAGES_DIR/tavana-gateway-$VERSION.tar.gz"
docker tag "angelerator/tavana-gateway:$VERSION" "$ACR_LOGIN_SERVER/tavana-gateway:$VERSION"
docker push "$ACR_LOGIN_SERVER/tavana-gateway:$VERSION"

# Load and push worker
echo "▶ Importing Worker image..."
docker load < "$IMAGES_DIR/tavana-worker-$VERSION.tar.gz"
docker tag "angelerator/tavana-worker:$VERSION" "$ACR_LOGIN_SERVER/tavana-worker:$VERSION"
docker push "$ACR_LOGIN_SERVER/tavana-worker:$VERSION"

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    ✓ Images imported successfully!"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Images available at:"
echo "  $ACR_LOGIN_SERVER/tavana-gateway:$VERSION"
echo "  $ACR_LOGIN_SERVER/tavana-worker:$VERSION"
echo ""
SCRIPT

chmod +x "$PACKAGE_DIR/scripts/import-to-acr.sh"

# ─────────────────────────────────────────────────────────────────────────────
#                      CREATE DEPLOYMENT SCRIPT
# ─────────────────────────────────────────────────────────────────────────────

cat > "$PACKAGE_DIR/scripts/deploy-tavana.sh" << 'DEPLOY_SCRIPT'
#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                    Deploy Tavana to AKS (Restricted Network)
# ═══════════════════════════════════════════════════════════════════════════════
# This script deploys Tavana using local Helm chart and customer's ACR.
#
# Prerequisites:
#   - kubectl configured for target AKS cluster
#   - Images already imported to customer's ACR (run import-to-acr.sh first)
#   - Helm installed
#
# Usage:
#   ./deploy-tavana.sh <ACR_LOGIN_SERVER> [VERSION] [NAMESPACE]
#   Example: ./deploy-tavana.sh mycompanyacr.azurecr.io v1.0.0 tavana
# ═══════════════════════════════════════════════════════════════════════════════

set -e

ACR_LOGIN_SERVER="${1:?Usage: $0 <ACR_LOGIN_SERVER> [VERSION] [NAMESPACE]}"
VERSION="${2:-latest}"
NAMESPACE="${3:-tavana}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(dirname "$SCRIPT_DIR")"
HELM_CHART="$PACKAGE_DIR/helm/tavana-${VERSION#v}.tgz"

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    Deploying Tavana to AKS"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "  ACR:       $ACR_LOGIN_SERVER"
echo "  Version:   $VERSION"
echo "  Namespace: $NAMESPACE"
echo "  Chart:     $HELM_CHART"
echo ""

# Check prerequisites
if ! kubectl get nodes &> /dev/null; then
    echo "ERROR: kubectl not configured or cannot connect to cluster"
    exit 1
fi

if [ ! -f "$HELM_CHART" ]; then
    echo "ERROR: Helm chart not found at $HELM_CHART"
    exit 1
fi

# Create namespace
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Deploy with Helm
echo "▶ Installing Tavana with Helm..."
helm upgrade --install tavana "$HELM_CHART" \
    --namespace "$NAMESPACE" \
    --set global.imageRegistry="$ACR_LOGIN_SERVER" \
    --set global.imageTag="$VERSION" \
    --set gateway.image.repository="tavana-gateway" \
    --set worker.image.repository="tavana-worker" \
    --wait \
    --timeout 10m

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    ✓ Tavana deployed successfully!"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "To connect:"
echo "  kubectl port-forward svc/gateway -n $NAMESPACE 5432:5432"
echo "  psql -h localhost -p 5432 -U tavana"
echo ""
echo "To check status:"
echo "  kubectl get pods -n $NAMESPACE"
echo "  kubectl logs -n $NAMESPACE -l app=tavana-gateway"
echo ""
DEPLOY_SCRIPT

chmod +x "$PACKAGE_DIR/scripts/deploy-tavana.sh"

# ─────────────────────────────────────────────────────────────────────────────
#                      CREATE VALUES TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────

print_step "Creating customer values template..."

cat > "$PACKAGE_DIR/values-customer.yaml" << 'VALUES'
# ═══════════════════════════════════════════════════════════════════════════════
#                    Tavana - Customer Values Template
# ═══════════════════════════════════════════════════════════════════════════════
# Customize these values for your environment.
# ═══════════════════════════════════════════════════════════════════════════════

global:
  # Your private ACR login server
  imageRegistry: "YOUR_ACR.azurecr.io"
  imageTag: "v1.0.0"
  imagePullPolicy: IfNotPresent

# Gateway configuration
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
  
  # Add your storage configuration
  env:
    - name: AZURE_STORAGE_ACCOUNT
      value: "YOUR_STORAGE_ACCOUNT"
    - name: AZURE_STORAGE_CONTAINER
      value: "data"

# Worker configuration
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
  
  # Node selector for dedicated nodes (if applicable)
  # nodeSelector:
  #   nodepool: tavana
  
  # Tolerations for dedicated nodes (if applicable)
  # tolerations:
  #   - key: "workload"
  #     operator: "Equal"
  #     value: "tavana"
  #     effect: "NoSchedule"

# Service Account (if using Azure Workload Identity)
serviceAccount:
  create: true
  name: "tavana"
  annotations: {}
    # azure.workload.identity/client-id: "YOUR_CLIENT_ID"
    # azure.workload.identity/tenant-id: "YOUR_TENANT_ID"

# Ingress (optional - for external access)
ingress:
  enabled: false
  # className: azure/application-gateway
  # hosts:
  #   - host: tavana.yourcompany.com
  #     paths:
  #       - path: /
  #         pathType: Prefix

# Monitoring (if Prometheus is available)
monitoring:
  prometheus:
    enabled: true
VALUES

# ─────────────────────────────────────────────────────────────────────────────
#                      COPY TERRAFORM
# ─────────────────────────────────────────────────────────────────────────────

print_step "Copying Terraform files..."

cp -r "$PROJECT_ROOT/terraform/azure" "$PACKAGE_DIR/terraform/"

# ─────────────────────────────────────────────────────────────────────────────
#                      CREATE README
# ─────────────────────────────────────────────────────────────────────────────

print_step "Creating README..."

cat > "$PACKAGE_DIR/README.md" << README
# Tavana Offline Deployment Package

**Version:** $VERSION  
**Created:** $(date -u +"%Y-%m-%dT%H:%M:%SZ")

## Package Contents

\`\`\`
tavana-offline-$VERSION/
├── images/
│   ├── tavana-gateway-$VERSION.tar.gz    # Gateway Docker image
│   └── tavana-worker-$VERSION.tar.gz     # Worker Docker image
├── helm/
│   └── tavana-${VERSION#v}.tgz           # Helm chart package
├── scripts/
│   ├── import-to-acr.sh                  # Import images to your ACR
│   └── deploy-tavana.sh                  # Deploy to AKS
├── terraform/
│   └── azure/                            # Terraform for Azure infrastructure
├── values-customer.yaml                  # Helm values template
└── README.md                             # This file
\`\`\`

## Deployment Steps (Network-Restricted Environment)

### Prerequisites

- A machine with Docker and internet access (for initial package download)
- Azure CLI installed
- kubectl configured for your AKS cluster
- Helm v3 installed
- Access to your Azure Container Registry (ACR)

### Step 1: Transfer Package

Transfer this entire package to a machine that can access your Azure environment:

\`\`\`bash
# Option 1: SCP
scp -r tavana-offline-$VERSION user@bastion:/path/

# Option 2: Azure Storage
az storage blob upload-batch -d packages -s tavana-offline-$VERSION --account-name mystorageaccount
\`\`\`

### Step 2: Import Images to ACR

From a machine with Docker and ACR access:

\`\`\`bash
cd tavana-offline-$VERSION/scripts
./import-to-acr.sh YOUR_ACR_NAME $VERSION
\`\`\`

This will:
1. Load the Docker images from tarballs
2. Tag them for your ACR
3. Push them to your private registry

### Step 3: Configure Values

Edit \`values-customer.yaml\` with your specific settings:

\`\`\`bash
# Update with your ACR and storage details
sed -i 's/YOUR_ACR.azurecr.io/mycompany.azurecr.io/g' values-customer.yaml
sed -i 's/YOUR_STORAGE_ACCOUNT/mystorageaccount/g' values-customer.yaml
\`\`\`

### Step 4: Deploy Tavana

Option A - Quick deployment:

\`\`\`bash
cd scripts
./deploy-tavana.sh mycompany.azurecr.io $VERSION tavana
\`\`\`

Option B - Custom deployment with values file:

\`\`\`bash
helm upgrade --install tavana helm/tavana-${VERSION#v}.tgz \\
  --namespace tavana \\
  --create-namespace \\
  -f values-customer.yaml
\`\`\`

### Step 5: Verify Deployment

\`\`\`bash
# Check pods
kubectl get pods -n tavana

# Check services
kubectl get svc -n tavana

# Port forward for testing
kubectl port-forward svc/gateway -n tavana 5432:5432

# Connect with psql
psql -h localhost -p 5432 -U tavana -d tavana
\`\`\`

## Infrastructure (Optional)

If you need to provision Azure infrastructure:

\`\`\`bash
cd terraform/azure/examples/quickstart
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your settings
terraform init
terraform apply
\`\`\`

## Support

For issues or questions, contact the Tavana team.
README

# ─────────────────────────────────────────────────────────────────────────────
#                      CREATE ARCHIVE
# ─────────────────────────────────────────────────────────────────────────────

print_step "Creating final archive..."

cd "$PROJECT_ROOT/dist"
tar -czvf "tavana-offline-$VERSION.tar.gz" "tavana-offline-$VERSION"

# ─────────────────────────────────────────────────────────────────────────────
#                      SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    ✓ Offline Package Created Successfully!"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo -e "${NC}"
echo ""
echo "Package location:"
echo "  Directory: $PACKAGE_DIR"
echo "  Archive:   $PROJECT_ROOT/dist/tavana-offline-$VERSION.tar.gz"
echo ""
echo "Package size:"
du -sh "$PROJECT_ROOT/dist/tavana-offline-$VERSION.tar.gz"
echo ""
echo "Contents:"
ls -la "$PACKAGE_DIR"
echo ""
echo "To deploy to a restricted environment:"
echo "  1. Transfer the .tar.gz to customer's network"
echo "  2. Extract: tar -xzf tavana-offline-$VERSION.tar.gz"
echo "  3. Run: ./scripts/import-to-acr.sh CUSTOMER_ACR_NAME"
echo "  4. Run: ./scripts/deploy-tavana.sh CUSTOMER_ACR.azurecr.io"
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"

