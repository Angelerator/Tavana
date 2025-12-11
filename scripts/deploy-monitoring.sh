#!/bin/bash
# Deploy Prometheus and Grafana monitoring stack to Tavana cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "═══════════════════════════════════════════════════════════════"
echo "  Deploying Tavana Monitoring Stack"
echo "═══════════════════════════════════════════════════════════════"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl not found"
    exit 1
fi

# Check Kind cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster"
    echo "Make sure Kind cluster is running: kind get clusters"
    exit 1
fi

echo ""
echo "1. Creating namespace (if not exists)..."
kubectl apply -f "$PROJECT_DIR/k8s/manifests/namespace.yaml"

echo ""
echo "2. Deploying Prometheus..."
kubectl apply -f "$PROJECT_DIR/k8s/monitoring/prometheus.yaml"

echo ""
echo "3. Deploying Dashboard ConfigMap..."
kubectl apply -f "$PROJECT_DIR/k8s/monitoring/dashboard-configmap.yaml"

echo ""
echo "4. Deploying Grafana..."
kubectl apply -f "$PROJECT_DIR/k8s/monitoring/grafana.yaml"

echo ""
echo "5. Deploying Adaptive Config..."
kubectl apply -f "$PROJECT_DIR/k8s/manifests/adaptive-config.yaml"

echo ""
echo "6. Waiting for pods to be ready..."
kubectl wait --for=condition=Ready pod -l app=prometheus -n tavana --timeout=120s || echo "Prometheus not ready yet"
kubectl wait --for=condition=Ready pod -l app=grafana -n tavana --timeout=120s || echo "Grafana not ready yet"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Monitoring Stack Deployed!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Access URLs (via NodePort):"
echo "  Prometheus: http://localhost:30090"
echo "  Grafana:    http://localhost:30300"
echo "              Username: admin"
echo "              Password: tavana"
echo ""
echo "Gateway metrics endpoint: http://localhost:8080/metrics"
echo "Adaptive state endpoint:  http://localhost:8080/api/adaptive"
echo ""
echo "Check pod status:"
echo "  kubectl get pods -n tavana"
echo ""

