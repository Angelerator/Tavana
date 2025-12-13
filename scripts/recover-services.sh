#!/bin/bash
# Tavana Services Auto-Recovery Script
# Checks and restores services to healthy state

set -e

NAMESPACE="${NAMESPACE:-tavana}"
TIMEOUT="${TIMEOUT:-120}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi
}

# Check if cluster is accessible
check_cluster() {
    log_info "Checking cluster connectivity..."
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        log_warn "Please ensure Docker Desktop/Kind is running"
        
        # Check if Docker is running
        if command -v docker &> /dev/null; then
            if ! docker info &> /dev/null 2>&1; then
                log_error "Docker is not running. Please start Docker Desktop."
                exit 1
            fi
        fi
        
        # Try to get Kind clusters
        if command -v kind &> /dev/null; then
            log_info "Available Kind clusters:"
            kind get clusters 2>/dev/null || echo "None"
            
            # Try to set context to tavana cluster
            if kind get clusters 2>/dev/null | grep -q "tavana"; then
                log_info "Setting kubectl context to kind-tavana..."
                kubectl config use-context kind-tavana 2>/dev/null || true
            fi
        fi
        
        # Retry cluster check
        if ! kubectl cluster-info &> /dev/null; then
            log_error "Still cannot connect. Please restart Docker Desktop and try again."
            exit 1
        fi
    fi
    log_info "Cluster is accessible"
}

# Check namespace exists
check_namespace() {
    log_info "Checking namespace ${NAMESPACE}..."
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_warn "Namespace $NAMESPACE not found. Creating..."
        kubectl create namespace "$NAMESPACE"
    fi
}

# Restart unhealthy pods
restart_unhealthy_pods() {
    log_info "Checking for unhealthy pods..."
    
    # Get pods in error states
    UNHEALTHY_PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | \
        grep -E "Error|CrashLoopBackOff|ImagePullBackOff|ErrImagePull|OOMKilled" | \
        awk '{print $1}' || true)
    
    if [ -n "$UNHEALTHY_PODS" ]; then
        log_warn "Found unhealthy pods: $UNHEALTHY_PODS"
        for pod in $UNHEALTHY_PODS; do
            log_info "Deleting unhealthy pod: $pod"
            kubectl delete pod "$pod" -n "$NAMESPACE" --force --grace-period=0 2>/dev/null || true
        done
        sleep 5
    else
        log_info "All pods are healthy"
    fi
}

# Check and restart deployments with zero ready replicas
check_deployments() {
    log_info "Checking deployments..."
    
    for deployment in gateway worker operator; do
        READY=$(kubectl get deployment "$deployment" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        DESIRED=$(kubectl get deployment "$deployment" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
        
        if [ "$READY" = "" ]; then READY=0; fi
        if [ "$DESIRED" = "" ]; then DESIRED=0; fi
        
        if [ "$READY" -lt "$DESIRED" ]; then
            log_warn "$deployment: $READY/$DESIRED ready. Triggering rollout restart..."
            kubectl rollout restart deployment/"$deployment" -n "$NAMESPACE" 2>/dev/null || true
        else
            log_info "$deployment: $READY/$DESIRED ready ✓"
        fi
    done
}

# Wait for all pods to be ready
wait_for_pods() {
    log_info "Waiting for pods to be ready (timeout: ${TIMEOUT}s)..."
    
    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $TIMEOUT ]; then
            log_error "Timeout waiting for pods to be ready"
            kubectl get pods -n "$NAMESPACE"
            exit 1
        fi
        
        # Count ready pods
        local total=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
        local ready=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -c "Running" || echo "0")
        
        if [ "$total" -gt 0 ] && [ "$ready" -eq "$total" ]; then
            log_info "All pods ready ($ready/$total)"
            break
        fi
        
        echo -ne "\r  Pods ready: $ready/$total (${elapsed}s elapsed)..."
        sleep 2
    done
    echo
}

# Setup port forwarding
setup_port_forwards() {
    log_info "Setting up port forwarding..."
    
    # Kill existing port forwards
    pkill -f "kubectl port-forward.*$NAMESPACE" 2>/dev/null || true
    sleep 1
    
    # Start new port forwards
    kubectl port-forward -n "$NAMESPACE" svc/gateway 8080:8080 15432:15432 &>/dev/null &
    kubectl port-forward -n "$NAMESPACE" svc/grafana 3001:3000 &>/dev/null &
    kubectl port-forward -n "$NAMESPACE" svc/prometheus 9090:9090 &>/dev/null &
    
    sleep 3
    log_info "Port forwards started:"
    log_info "  Gateway:    localhost:8080 (HTTP), localhost:15432 (PostgreSQL)"
    log_info "  Grafana:    localhost:3001"
    log_info "  Prometheus: localhost:9090"
}

# Test connectivity
test_connectivity() {
    log_info "Testing service connectivity..."
    
    # Test gateway health
    if curl -s http://localhost:8080/health &>/dev/null; then
        log_info "Gateway HTTP: ✓"
    else
        log_warn "Gateway HTTP: connection failed"
    fi
    
    # Test Grafana
    if curl -s http://localhost:3001/api/health &>/dev/null; then
        log_info "Grafana: ✓"
    else
        log_warn "Grafana: connection failed"
    fi
    
    # Test PostgreSQL port
    if nc -z localhost 15432 2>/dev/null; then
        log_info "Gateway PostgreSQL: ✓"
    else
        log_warn "Gateway PostgreSQL: connection failed (may need psql test)"
    fi
}

# Show status summary
show_status() {
    echo
    log_info "=== Service Status ==="
    kubectl get pods -n "$NAMESPACE" -o wide
    echo
    log_info "=== Port Forwards ==="
    pgrep -f "kubectl port-forward" &>/dev/null && echo "Active" || echo "None"
}

# Main recovery flow
main() {
    echo "========================================"
    echo "  Tavana Services Auto-Recovery"
    echo "========================================"
    echo
    
    check_kubectl
    check_cluster
    check_namespace
    restart_unhealthy_pods
    check_deployments
    wait_for_pods
    setup_port_forwards
    test_connectivity
    show_status
    
    echo
    log_info "Recovery complete!"
    log_info "Connect to PostgreSQL: psql -h localhost -p 15432 -U tavana -d tavana"
}

# Run main function
main "$@"

