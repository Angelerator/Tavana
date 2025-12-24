#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
#                    TAVANA - One-Click Customer Deployment
# ═══════════════════════════════════════════════════════════════════════════════
# This is the ONLY script customers need to run.
# It handles everything: image import, Helm deployment, verification.
#
# For network-restricted environments like Nokia's Azure setup.
#
# Usage:
#   ./customer-deploy.sh
#
# The script will prompt for all required information interactively.
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(dirname "$SCRIPT_DIR")"

# ─────────────────────────────────────────────────────────────────────────────
#                              FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

print_banner() {
    clear
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                               ║"
    echo "║                         ████████╗ █████╗ ██╗   ██╗ █████╗ ███╗   ██╗ █████╗   ║"
    echo "║                         ╚══██╔══╝██╔══██╗██║   ██║██╔══██╗████╗  ██║██╔══██╗  ║"
    echo "║                            ██║   ███████║██║   ██║███████║██╔██╗ ██║███████║  ║"
    echo "║                            ██║   ██╔══██║╚██╗ ██╔╝██╔══██║██║╚██╗██║██╔══██║  ║"
    echo "║                            ██║   ██║  ██║ ╚████╔╝ ██║  ██║██║ ╚████║██║  ██║  ║"
    echo "║                            ╚═╝   ╚═╝  ╚═╝  ╚═══╝  ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝  ║"
    echo "║                                                                               ║"
    echo "║                    Cloud-Agnostic Auto-Scaling DuckDB Platform                ║"
    echo "║                                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo ""
}

print_step() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}  $1${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_substep() {
    echo -e "  ${BLUE}▶${NC} $1"
}

print_success() {
    echo -e "  ${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "  ${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "  ${RED}✗${NC} $1"
}

check_prerequisites() {
    print_step "STEP 1/5: Checking Prerequisites"
    
    local all_ok=true
    
    # Check Docker
    if command -v docker &> /dev/null; then
        print_success "Docker installed"
    else
        print_error "Docker not found"
        all_ok=false
    fi
    
    # Check Azure CLI
    if command -v az &> /dev/null; then
        print_success "Azure CLI installed"
    else
        print_error "Azure CLI not found"
        all_ok=false
    fi
    
    # Check kubectl
    if command -v kubectl &> /dev/null; then
        print_success "kubectl installed"
    else
        print_error "kubectl not found"
        all_ok=false
    fi
    
    # Check Helm
    if command -v helm &> /dev/null; then
        print_success "Helm installed"
    else
        print_error "Helm not found"
        all_ok=false
    fi
    
    # Check kubectl connectivity
    if kubectl get nodes &> /dev/null; then
        print_success "kubectl connected to cluster"
        CLUSTER_INFO=$(kubectl config current-context)
        print_substep "Context: $CLUSTER_INFO"
    else
        print_warning "kubectl not connected (will configure later)"
    fi
    
    # Check package contents
    if [ -d "$PACKAGE_DIR/images" ] && [ -d "$PACKAGE_DIR/helm" ]; then
        print_success "Package contents verified"
    else
        print_error "Package incomplete - missing images or helm directories"
        all_ok=false
    fi
    
    if [ "$all_ok" = false ]; then
        echo ""
        print_error "Please install missing prerequisites and try again."
        exit 1
    fi
    
    echo ""
    print_success "All prerequisites met!"
}

gather_configuration() {
    print_step "STEP 2/5: Configuration"
    
    echo ""
    echo "  Please provide the following information:"
    echo ""
    
    # ACR Name
    read -p "  Azure Container Registry name (e.g., mycompanyacr): " ACR_NAME
    ACR_NAME="${ACR_NAME:-}"
    
    if [ -z "$ACR_NAME" ]; then
        print_error "ACR name is required"
        exit 1
    fi
    
    # Get ACR login server
    print_substep "Fetching ACR details..."
    if az acr show --name "$ACR_NAME" &> /dev/null; then
        ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)
        print_success "ACR found: $ACR_LOGIN_SERVER"
    else
        print_error "Cannot access ACR '$ACR_NAME'. Please check permissions."
        exit 1
    fi
    
    # Namespace
    read -p "  Kubernetes namespace [tavana]: " NAMESPACE
    NAMESPACE="${NAMESPACE:-tavana}"
    
    # Version (detect from package)
    VERSION=$(ls "$PACKAGE_DIR/images/" | head -1 | sed 's/tavana-gateway-//' | sed 's/.tar.gz//')
    print_substep "Detected version: $VERSION"
    
    # Storage account (optional)
    echo ""
    read -p "  Azure Storage Account name (for data lake, optional): " STORAGE_ACCOUNT
    
    # Confirm
    echo ""
    echo -e "  ${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  ${CYAN}Configuration Summary:${NC}"
    echo -e "  ${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "    ACR:             $ACR_LOGIN_SERVER"
    echo "    Namespace:       $NAMESPACE"
    echo "    Version:         $VERSION"
    [ -n "$STORAGE_ACCOUNT" ] && echo "    Storage Account: $STORAGE_ACCOUNT"
    echo ""
    
    read -p "  Proceed with deployment? (Y/n): " CONFIRM
    CONFIRM="${CONFIRM:-Y}"
    
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        echo "  Cancelled."
        exit 0
    fi
}

import_images() {
    print_step "STEP 3/5: Importing Images to ACR"
    
    # Login to ACR
    print_substep "Logging into ACR..."
    az acr login --name "$ACR_NAME"
    print_success "Logged into ACR"
    
    # Import gateway
    print_substep "Loading Gateway image..."
    docker load < "$PACKAGE_DIR/images/tavana-gateway-$VERSION.tar.gz"
    print_success "Gateway image loaded"
    
    print_substep "Pushing Gateway to ACR..."
    docker tag "angelerator/tavana-gateway:$VERSION" "$ACR_LOGIN_SERVER/tavana-gateway:$VERSION"
    docker push "$ACR_LOGIN_SERVER/tavana-gateway:$VERSION"
    print_success "Gateway pushed to ACR"
    
    # Import worker
    print_substep "Loading Worker image..."
    docker load < "$PACKAGE_DIR/images/tavana-worker-$VERSION.tar.gz"
    print_success "Worker image loaded"
    
    print_substep "Pushing Worker to ACR..."
    docker tag "angelerator/tavana-worker:$VERSION" "$ACR_LOGIN_SERVER/tavana-worker:$VERSION"
    docker push "$ACR_LOGIN_SERVER/tavana-worker:$VERSION"
    print_success "Worker pushed to ACR"
}

deploy_helm() {
    print_step "STEP 4/5: Deploying Tavana"
    
    # Find helm chart
    HELM_CHART=$(ls "$PACKAGE_DIR/helm/"*.tgz | head -1)
    
    if [ ! -f "$HELM_CHART" ]; then
        print_error "Helm chart not found in $PACKAGE_DIR/helm/"
        exit 1
    fi
    
    print_substep "Using Helm chart: $(basename "$HELM_CHART")"
    
    # Create namespace
    print_substep "Creating namespace..."
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    print_success "Namespace ready"
    
    # Build Helm values
    HELM_ARGS=(
        upgrade --install tavana "$HELM_CHART"
        --namespace "$NAMESPACE"
        --set "global.imageRegistry=$ACR_LOGIN_SERVER"
        --set "global.imageTag=$VERSION"
        --set "gateway.image.repository=tavana-gateway"
        --set "worker.image.repository=tavana-worker"
        --wait
        --timeout 10m
    )
    
    # Add storage if provided
    if [ -n "$STORAGE_ACCOUNT" ]; then
        HELM_ARGS+=(--set "gateway.env[0].name=AZURE_STORAGE_ACCOUNT")
        HELM_ARGS+=(--set "gateway.env[0].value=$STORAGE_ACCOUNT")
    fi
    
    # Deploy
    print_substep "Installing Tavana with Helm..."
    helm "${HELM_ARGS[@]}"
    print_success "Tavana deployed"
}

verify_deployment() {
    print_step "STEP 5/5: Verifying Deployment"
    
    # Wait for pods
    print_substep "Waiting for pods to be ready..."
    kubectl wait --for=condition=ready pod -l app=tavana-gateway -n "$NAMESPACE" --timeout=300s || true
    kubectl wait --for=condition=ready pod -l app=tavana-worker -n "$NAMESPACE" --timeout=300s || true
    
    # Show status
    echo ""
    print_substep "Pod Status:"
    kubectl get pods -n "$NAMESPACE" -o wide
    
    echo ""
    print_substep "Service Status:"
    kubectl get svc -n "$NAMESPACE"
    
    # Check if gateway is running
    GATEWAY_READY=$(kubectl get pods -n "$NAMESPACE" -l app=tavana-gateway -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -c True || echo 0)
    
    if [ "$GATEWAY_READY" -gt 0 ]; then
        print_success "Gateway is running"
    else
        print_warning "Gateway may still be starting"
    fi
}

print_summary() {
    echo ""
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                               ║"
    echo "║                    ✓ TAVANA DEPLOYMENT COMPLETE!                              ║"
    echo "║                                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo ""
    echo "  To connect to Tavana:"
    echo ""
    echo -e "    ${CYAN}# Port forward the gateway${NC}"
    echo "    kubectl port-forward svc/gateway -n $NAMESPACE 5432:5432"
    echo ""
    echo -e "    ${CYAN}# Connect with psql${NC}"
    echo "    psql -h localhost -p 5432 -U tavana -d tavana"
    echo ""
    echo -e "    ${CYAN}# Run a test query${NC}"
    echo "    SELECT 'Tavana is working!' as message;"
    echo ""
    echo "  Useful commands:"
    echo ""
    echo "    kubectl get pods -n $NAMESPACE           # Check pod status"
    echo "    kubectl logs -n $NAMESPACE -l app=tavana-gateway  # Gateway logs"
    echo "    kubectl logs -n $NAMESPACE -l app=tavana-worker   # Worker logs"
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo ""
}

# ─────────────────────────────────────────────────────────────────────────────
#                                  MAIN
# ─────────────────────────────────────────────────────────────────────────────

main() {
    print_banner
    check_prerequisites
    gather_configuration
    import_images
    deploy_helm
    verify_deployment
    print_summary
}

main "$@"

