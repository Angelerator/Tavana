#!/bin/bash
# Deploy Tavana to Docker Desktop Kubernetes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Tavana Kubernetes Deployment ==="
echo "Project directory: $PROJECT_DIR"
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed or not in PATH"
    exit 1
fi

# Check if Docker Desktop K8s is running
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster"
    echo "Make sure Docker Desktop Kubernetes is enabled:"
    echo "  Docker Desktop > Settings > Kubernetes > Enable Kubernetes"
    exit 1
fi

# Check current context
CURRENT_CONTEXT=$(kubectl config current-context)
echo "Current kubectl context: $CURRENT_CONTEXT"

if [[ "$CURRENT_CONTEXT" != "docker-desktop" ]]; then
    echo ""
    echo "WARNING: You are not using docker-desktop context"
    echo "To switch to Docker Desktop Kubernetes, run:"
    echo "  kubectl config use-context docker-desktop"
    echo ""
    read -p "Continue with current context? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "Connected to Kubernetes cluster:"
kubectl cluster-info | head -2
echo ""

# Build Docker images
echo "=== Building Docker Images ==="

echo "Building tavana-gateway..."
docker build -f "$PROJECT_DIR/Dockerfile.gateway" -t tavana-gateway:latest "$PROJECT_DIR"

echo "Building tavana-worker..."
docker build -f "$PROJECT_DIR/Dockerfile.worker" -t tavana-worker:latest "$PROJECT_DIR"

echo ""
echo "=== Deploying to Kubernetes ==="

# Apply Kubernetes manifests
cd "$PROJECT_DIR/k8s/base"
kubectl apply -k .

echo ""
echo "=== Waiting for deployments ==="

# Wait for deployments to be ready
kubectl wait --for=condition=available --timeout=120s deployment/tavana-postgres -n tavana || true
kubectl wait --for=condition=available --timeout=120s deployment/tavana-minio -n tavana || true
kubectl wait --for=condition=available --timeout=120s deployment/tavana-worker -n tavana || true
kubectl wait --for=condition=available --timeout=120s deployment/tavana-gateway -n tavana || true

echo ""
echo "=== Deployment Status ==="
kubectl get pods -n tavana
kubectl get svc -n tavana

echo ""
echo "=== Access Information ==="
echo ""
echo "PostgreSQL wire protocol (psql, Tableau, PowerBI):"
echo "  psql -h localhost -p 30432 -U tavana"
echo ""
echo "Arrow Flight SQL (Python):"
echo "  import adbc_driver_flightsql.dbapi as flight"
echo "  conn = flight.connect('grpc://localhost:30815')"
echo ""
echo "HTTP API:"
echo "  curl http://localhost:30080/health"
echo ""
echo "MinIO Console (S3 storage):"
echo "  kubectl port-forward svc/tavana-minio 9001:9001 -n tavana"
echo "  Then open http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "=== Done ==="

