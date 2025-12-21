#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# Tavana Kind Cluster Deployment Script
# 
# Creates a Kind cluster with K8s 1.35 features (InPlacePodVerticalScaling),
# builds and deploys all components.
#
# Usage: ./scripts/deploy-kind.sh [--clean] [--rebuild]
#   --clean   : Delete existing cluster and start fresh
#   --rebuild : Rebuild Docker images even if they exist
# ═══════════════════════════════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_NAME="tavana"

# Parse arguments
CLEAN=false
REBUILD=false
for arg in "$@"; do
    case $arg in
        --clean) CLEAN=true ;;
        --rebuild) REBUILD=true ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "  Tavana Kind Cluster Deployment"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Project directory: $PROJECT_DIR"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Prerequisites check
# ═══════════════════════════════════════════════════════════════════════════════
echo "Checking prerequisites..."

for cmd in docker kind kubectl; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd is not installed or not in PATH"
        exit 1
    fi
done

# Ensure Docker is running
if ! docker info &> /dev/null; then
    echo "Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi
echo "✓ All prerequisites satisfied"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Cluster Management
# ═══════════════════════════════════════════════════════════════════════════════

# Check if cluster exists
CLUSTER_EXISTS=$(kind get clusters 2>/dev/null | grep -c "^${CLUSTER_NAME}$" || true)

if [[ "$CLEAN" == "true" && "$CLUSTER_EXISTS" -gt 0 ]]; then
    echo "Deleting existing Kind cluster: $CLUSTER_NAME"
    kind delete cluster --name $CLUSTER_NAME
    CLUSTER_EXISTS=0
fi

if [[ "$CLUSTER_EXISTS" -eq 0 ]]; then
    echo "Creating Kind cluster with K8s 1.35 features..."
    kind create cluster --name $CLUSTER_NAME --config "$PROJECT_DIR/k8s/kind-config.yaml"
    echo "✓ Kind cluster created"
else
    echo "✓ Using existing Kind cluster: $CLUSTER_NAME"
    kubectl config use-context "kind-$CLUSTER_NAME"
fi

echo ""
kubectl cluster-info | head -2
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Build Docker Images
# ═══════════════════════════════════════════════════════════════════════════════
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Building Docker Images..."
echo "═══════════════════════════════════════════════════════════════════════════════"

cd "$PROJECT_DIR"

# Build gateway
if [[ "$REBUILD" == "true" ]] || ! docker image inspect tavana-gateway:latest &>/dev/null; then
    echo "Building tavana-gateway..."
    docker build -f Dockerfile.gateway -t tavana-gateway:latest .
else
    echo "✓ tavana-gateway:latest already exists (use --rebuild to force)"
fi

# Build worker
if [[ "$REBUILD" == "true" ]] || ! docker image inspect tavana-worker:latest &>/dev/null; then
    echo "Building tavana-worker..."
    docker build -f Dockerfile.worker -t tavana-worker:latest .
else
    echo "✓ tavana-worker:latest already exists (use --rebuild to force)"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Load Images into Kind
# ═══════════════════════════════════════════════════════════════════════════════
echo "Loading Docker images into Kind cluster..."
kind load docker-image tavana-gateway:latest --name $CLUSTER_NAME
kind load docker-image tavana-worker:latest --name $CLUSTER_NAME
echo "✓ Images loaded into Kind"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Deploy Kubernetes Manifests
# ═══════════════════════════════════════════════════════════════════════════════
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Deploying Kubernetes Resources..."
echo "═══════════════════════════════════════════════════════════════════════════════"

MANIFESTS_DIR="$PROJECT_DIR/k8s/manifests"

# Apply in order
kubectl apply -f "$MANIFESTS_DIR/namespace.yaml"
kubectl apply -f "$MANIFESTS_DIR/configmap.yaml"
kubectl apply -f "$MANIFESTS_DIR/gateway-rbac.yaml"
kubectl apply -f "$MANIFESTS_DIR/redis.yaml"
kubectl apply -f "$MANIFESTS_DIR/minio.yaml"
kubectl apply -f "$MANIFESTS_DIR/worker.yaml"
kubectl apply -f "$MANIFESTS_DIR/gateway.yaml"
kubectl apply -f "$MANIFESTS_DIR/hpa.yaml"

echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Wait for Deployments
# ═══════════════════════════════════════════════════════════════════════════════
echo "Waiting for deployments to be ready..."

kubectl wait --for=condition=available --timeout=180s deployment/redis -n tavana || true
kubectl wait --for=condition=available --timeout=180s deployment/minio -n tavana || true
kubectl wait --for=condition=available --timeout=180s deployment/worker -n tavana || true
kubectl wait --for=condition=available --timeout=180s deployment/gateway -n tavana || true

echo ""
echo "✓ All deployments ready"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Deployment Status
# ═══════════════════════════════════════════════════════════════════════════════
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Deployment Status"
echo "═══════════════════════════════════════════════════════════════════════════════"
kubectl get pods -n tavana
echo ""
kubectl get svc -n tavana
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# Access Information
# ═══════════════════════════════════════════════════════════════════════════════
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Access Information (via Kind NodePorts)"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "PostgreSQL Wire Protocol (psql, DBeaver, Tableau):"
echo "  PGPASSWORD=tavana psql -h localhost -p 25432 -U tavana -d tavana"
echo ""
echo "HTTP API Health Check:"
echo "  curl http://localhost:28080/health"
echo ""
echo "Arrow Flight SQL:"
echo "  grpc://localhost:28815"
echo ""
echo "MinIO S3 Console:"
echo "  http://localhost:29001 (minioadmin/minioadmin)"
echo ""
echo "Quick Test:"
echo "  PGPASSWORD=tavana psql -h localhost -p 25432 -U tavana -d tavana -c 'SELECT 1'"
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "  Deployment Complete!"
echo "═══════════════════════════════════════════════════════════════════════════════"


