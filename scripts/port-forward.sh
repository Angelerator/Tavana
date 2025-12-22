#!/bin/bash
# Port-forward all Tavana services from Kind cluster to localhost
# Usage: ./scripts/port-forward.sh

set -e

echo "═══════════════════════════════════════════════════════════════════"
echo "  Starting port-forwarding for Tavana services..."
echo "═══════════════════════════════════════════════════════════════════"

# Kill existing port-forwards
pkill -f "kubectl port-forward" 2>/dev/null || true
sleep 1

# Gateway - PostgreSQL and HTTP only
echo "→ Gateway (15432, 8080)..."
kubectl port-forward -n tavana svc/gateway 15432:15432 8080:8080 &

# Prometheus
echo "→ Prometheus (9090)..."
kubectl port-forward -n tavana svc/prometheus 9090:9090 &

# Grafana (use 3001 since 3000 is used by frontend)
echo "→ Grafana (3001)..."
kubectl port-forward -n tavana svc/grafana 3001:3000 &

# MinIO
echo "→ MinIO (9000, 9001)..."
kubectl port-forward -n tavana svc/minio 9000:9000 9001:9001 &

sleep 2

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  All services forwarded! Access via:"
echo "═══════════════════════════════════════════════════════════════════"
echo ""
echo "  PostgreSQL (DBeaver):   localhost:15432"
echo "  HTTP Health:            http://localhost:8080/health"
echo "  HTTP Metrics:           http://localhost:8080/metrics"
echo ""
echo "  Prometheus:             http://localhost:9090"
echo "  Grafana:                http://localhost:3001  (admin / tavana)"
echo ""
echo "  MinIO Console:          http://localhost:9001  (minioadmin / minioadmin)"
echo "  MinIO S3 API:           http://localhost:9000"
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Press Ctrl+C to stop all port-forwards"
echo "═══════════════════════════════════════════════════════════════════"

# Wait for all background jobs
wait

