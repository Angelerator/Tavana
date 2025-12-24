#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                     TAVANA - One-Click Deployment Script
# ═══════════════════════════════════════════════════════════════════════════════
# This script deploys Tavana to Azure with a single command.
#
# Usage:
#   ./deploy.sh                    # Interactive mode
#   ./deploy.sh --subscription-id xxx --location westeurope --env prod
#
# Prerequisites:
#   - Azure CLI (az) installed and logged in
#   - Terraform >= 1.5.0 installed
#   - kubectl installed
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Defaults
PROJECT_NAME="tavana"
ENVIRONMENT="dev"
LOCATION="westeurope"
TAVANA_VERSION="latest"
SKIP_CONFIRM=false

# ─────────────────────────────────────────────────────────────────────────────
#                                 FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

print_banner() {
    echo -e "${BLUE}"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo "                         TAVANA DEPLOYMENT"
    echo "              Cloud-Agnostic Auto-Scaling DuckDB Query Platform"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
}

print_step() {
    echo -e "\n${GREEN}▶ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✖ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✔ $1${NC}"
}

check_prerequisites() {
    print_step "Checking prerequisites..."
    
    # Check Azure CLI
    if ! command -v az &> /dev/null; then
        print_error "Azure CLI (az) is not installed"
        echo "Install: https://docs.microsoft.com/cli/azure/install-azure-cli"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed"
        echo "Install: https://www.terraform.io/downloads"
        exit 1
    fi
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        print_warning "kubectl is not installed (will be needed later)"
    fi
    
    # Check Azure login
    if ! az account show &> /dev/null; then
        print_error "Not logged into Azure CLI"
        echo "Run: az login"
        exit 1
    fi
    
    print_success "All prerequisites met"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --subscription-id)
                SUBSCRIPTION_ID="$2"
                shift 2
                ;;
            --project)
                PROJECT_NAME="$2"
                shift 2
                ;;
            --env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            --location)
                LOCATION="$2"
                shift 2
                ;;
            --version)
                TAVANA_VERSION="$2"
                shift 2
                ;;
            --yes|-y)
                SKIP_CONFIRM=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  --subscription-id ID   Azure subscription ID (required)"
                echo "  --project NAME         Project name (default: tavana)"
                echo "  --env ENV              Environment: dev|staging|prod (default: dev)"
                echo "  --location REGION      Azure region (default: westeurope)"
                echo "  --version VERSION      Tavana version (default: latest)"
                echo "  --yes, -y              Skip confirmation prompts"
                echo "  --help, -h             Show this help"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

interactive_setup() {
    if [ -z "$SUBSCRIPTION_ID" ]; then
        echo ""
        echo "Available Azure subscriptions:"
        az account list --output table
        echo ""
        read -p "Enter Subscription ID: " SUBSCRIPTION_ID
    fi
    
    read -p "Project name [$PROJECT_NAME]: " input
    PROJECT_NAME="${input:-$PROJECT_NAME}"
    
    read -p "Environment (dev/staging/prod) [$ENVIRONMENT]: " input
    ENVIRONMENT="${input:-$ENVIRONMENT}"
    
    read -p "Azure region [$LOCATION]: " input
    LOCATION="${input:-$LOCATION}"
}

confirm_deployment() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo "                          DEPLOYMENT CONFIGURATION"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo ""
    echo "  Subscription:  $SUBSCRIPTION_ID"
    echo "  Project:       $PROJECT_NAME"
    echo "  Environment:   $ENVIRONMENT"
    echo "  Location:      $LOCATION"
    echo "  Version:       $TAVANA_VERSION"
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
    
    if [ "$SKIP_CONFIRM" = false ]; then
        read -p "Proceed with deployment? (y/N): " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo "Deployment cancelled."
            exit 0
        fi
    fi
}

deploy_infrastructure() {
    print_step "Deploying Azure infrastructure with Terraform..."
    
    # Determine which example to use
    if [ "$ENVIRONMENT" = "prod" ]; then
        EXAMPLE_DIR="terraform/azure/examples/enterprise"
    else
        EXAMPLE_DIR="terraform/azure/examples/quickstart"
    fi
    
    cd "$EXAMPLE_DIR"
    
    # Create terraform.tfvars
    cat > terraform.tfvars << EOF
subscription_id = "$SUBSCRIPTION_ID"
project_name    = "$PROJECT_NAME"
environment     = "$ENVIRONMENT"
location        = "$LOCATION"
EOF
    
    # Initialize and apply
    terraform init -upgrade
    terraform apply -auto-approve
    
    # Capture outputs
    ACR_LOGIN_SERVER=$(terraform output -raw acr_login_server)
    KUBE_CONFIG_CMD=$(terraform output -raw kube_config_command)
    
    cd - > /dev/null
    
    print_success "Infrastructure deployed"
}

configure_kubectl() {
    print_step "Configuring kubectl..."
    eval "$KUBE_CONFIG_CMD"
    kubectl get nodes
    print_success "kubectl configured"
}

import_images() {
    print_step "Importing Tavana images to ACR..."
    
    ACR_NAME=$(echo "$ACR_LOGIN_SERVER" | cut -d'.' -f1)
    
    # Import from Docker Hub (angelerator namespace)
    az acr import \
        --name "$ACR_NAME" \
        --source "docker.io/angelerator/tavana-gateway:$TAVANA_VERSION" \
        --image "tavana-gateway:$TAVANA_VERSION" \
        --force || true
    
    az acr import \
        --name "$ACR_NAME" \
        --source "docker.io/angelerator/tavana-worker:$TAVANA_VERSION" \
        --image "tavana-worker:$TAVANA_VERSION" \
        --force || true
    
    print_success "Images imported to $ACR_LOGIN_SERVER"
}

deploy_tavana() {
    print_step "Deploying Tavana..."
    
    # Get values from Terraform
    cd "$EXAMPLE_DIR"
    IDENTITY_CLIENT_ID=$(terraform output -raw tavana_identity_client_id 2>/dev/null || echo "")
    TENANT_ID=$(terraform output -raw tavana_identity_tenant_id 2>/dev/null || echo "")
    STORAGE_ACCOUNT=$(terraform output -raw storage_account_name 2>/dev/null || echo "")
    STORAGE_ENDPOINT=$(terraform output -raw storage_primary_dfs_endpoint 2>/dev/null || echo "")
    cd - > /dev/null
    
    # Deploy with Helm from GHCR (angelerator namespace)
    helm upgrade --install tavana oci://ghcr.io/angelerator/charts/tavana \
        --namespace tavana \
        --create-namespace \
        --set global.imageRegistry="$ACR_LOGIN_SERVER" \
        --set global.imageTag="$TAVANA_VERSION" \
        --set serviceAccount.annotations."azure\.workload\.identity/client-id"="$IDENTITY_CLIENT_ID" \
        --set serviceAccount.annotations."azure\.workload\.identity/tenant-id"="$TENANT_ID" \
        --set gateway.env[0].name="AZURE_STORAGE_ACCOUNT" \
        --set gateway.env[0].value="$STORAGE_ACCOUNT" \
        --set gateway.env[1].name="AZURE_STORAGE_ENDPOINT" \
        --set gateway.env[1].value="$STORAGE_ENDPOINT" \
        --set worker.nodeSelector.nodepool="tavana" \
        --set worker.tolerations[0].key="workload" \
        --set worker.tolerations[0].operator="Equal" \
        --set worker.tolerations[0].value="tavana" \
        --set worker.tolerations[0].effect="NoSchedule" \
        --wait \
        --timeout 10m
    
    print_success "Tavana deployed"
}

print_summary() {
    echo ""
    echo -e "${GREEN}"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo "                          DEPLOYMENT COMPLETE!"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
    echo ""
    echo "  ACR:     $ACR_LOGIN_SERVER"
    echo "  Gateway: kubectl port-forward svc/tavana-gateway -n tavana 5432:5432"
    echo "  ArgoCD:  kubectl port-forward svc/argocd-server -n argocd 8080:443"
    echo ""
    echo "  Connect with psql:"
    echo "    PGPASSWORD=tavana psql -h localhost -p 5432 -U tavana -d tavana"
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
}

# ─────────────────────────────────────────────────────────────────────────────
#                                    MAIN
# ─────────────────────────────────────────────────────────────────────────────

main() {
    print_banner
    parse_args "$@"
    check_prerequisites
    
    if [ -z "$SUBSCRIPTION_ID" ]; then
        interactive_setup
    fi
    
    confirm_deployment
    deploy_infrastructure
    configure_kubectl
    import_images
    deploy_tavana
    print_summary
}

main "$@"

