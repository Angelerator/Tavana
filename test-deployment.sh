#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - Quick Test Deployment Script
# ═══════════════════════════════════════════════════════════════════════════════
# This script deploys Tavana to Azure for testing the v1.0.0 release
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_step() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✔ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Configuration
SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-}"
LOCATION="${LOCATION:-westeurope}"
PROJECT_NAME="tavana-test"
ENVIRONMENT="dev"
VERSION="v1.0.0"

if [ -z "$SUBSCRIPTION_ID" ]; then
    echo "ERROR: SUBSCRIPTION_ID not set"
    echo "Usage: SUBSCRIPTION_ID=xxx ./test-deployment.sh"
    exit 1
fi

print_step "Test Deployment Configuration"
echo "Subscription: $SUBSCRIPTION_ID"
echo "Location:     $LOCATION"
echo "Project:      $PROJECT_NAME"
echo "Environment:  $ENVIRONMENT"
echo "Version:      $VERSION"

print_step "Checking prerequisites..."

# Check Azure CLI
if ! command -v az &> /dev/null; then
    echo "ERROR: Azure CLI not installed"
    exit 1
fi
print_success "Azure CLI installed"

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    print_warning "kubectl not installed (will need it later)"
fi

# Check Helm
if ! command -v helm &> /dev/null; then
    print_warning "Helm not installed (will need it later)"
fi

# Check Azure login
if ! az account show &> /dev/null; then
    echo "ERROR: Not logged into Azure"
    echo "Run: az login"
    exit 1
fi
print_success "Azure CLI authenticated"

# Set subscription
az account set --subscription "$SUBSCRIPTION_ID"
print_success "Subscription set"

print_step "Creating test directory..."
TEST_DIR="/tmp/tavana-test-deployment"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

cat > terraform.tfvars << EOF
subscription_id = "$SUBSCRIPTION_ID"
project_name    = "$PROJECT_NAME"
environment     = "$ENVIRONMENT"
location        = "$LOCATION"
EOF

print_success "Test directory created: $TEST_DIR"

print_step "Next steps:"
echo ""
echo "1. Wait for release build to complete (~30 min)"
echo "   https://github.com/Angelerator/Tavana/actions"
echo ""
echo "2. Then run these commands:"
echo ""
echo "   cd $TEST_DIR"
echo ""
echo "   # Initialize Terraform"
echo "   terraform init -from-module=https://github.com/Angelerator/Tavana//terraform/azure/examples/quickstart"
echo ""
echo "   # Deploy infrastructure"
echo "   terraform apply"
echo ""
echo "   # Configure kubectl"
echo "   az aks get-credentials --resource-group tavana-test-dev-rg-we --name tavana-test-dev-aks-we"
echo ""
echo "   # Install Tavana"
echo "   helm install tavana oci://ghcr.io/angelerator/charts/tavana --version 1.0.0 -n tavana --create-namespace"
echo ""
echo "   # Test connection"
echo "   kubectl port-forward svc/tavana-gateway -n tavana 5432:5432"
echo "   PGPASSWORD=tavana psql -h localhost -p 5432 -U tavana -d tavana -c 'SELECT 1'"
echo ""
print_success "Test deployment prepared!"

