#!/bin/bash
# Tavana Stress Test - 1000 Concurrent Heavy Queries
# Measures: latency, throughput, pod resources, VPA, HPA, queuing

set -e

# Configuration
HOST="${TAVANA_HOST:-tavana-dev.int.nokia.com}"
PORT="${TAVANA_PORT:-443}"
USER="${TAVANA_USER:-postgres}"
PASSWORD="${TAVANA_PASSWORD:-postgres}"
CONCURRENT="${CONCURRENT:-1000}"
QUERY="${QUERY:-SELECT COUNT(*) FROM delta_scan('az://dagster-data-pipelines/dev/bronze/memotech/fabric/memotech_imt_patent/');}"
NAMESPACE="${NAMESPACE:-tavana}"
RESULTS_DIR="/tmp/tavana-stress-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"
echo "Results will be saved to: $RESULTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  TAVANA STRESS TEST - $CONCURRENT CONCURRENT QUERIES${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to get pod metrics
get_pod_metrics() {
    echo -e "${YELLOW}📊 Pod Metrics:${NC}"
    kubectl top pods -n "$NAMESPACE" 2>/dev/null || echo "Metrics not available"
}

# Function to get node metrics
get_node_metrics() {
    echo -e "${YELLOW}📊 Node Metrics:${NC}"
    kubectl top nodes 2>/dev/null || echo "Metrics not available"
}

# Function to get VPA recommendations
get_vpa_status() {
    echo -e "${YELLOW}📊 VPA Recommendations:${NC}"
    kubectl get vpa -n "$NAMESPACE" -o wide 2>/dev/null || echo "VPA not configured"
    echo ""
    kubectl get vpa -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{": CPU="}{.status.recommendation.containerRecommendations[0].target.cpu}{", Memory="}{.status.recommendation.containerRecommendations[0].target.memory}{"\n"}{end}' 2>/dev/null || true
}

# Function to get HPA status
get_hpa_status() {
    echo -e "${YELLOW}📊 HPA Status:${NC}"
    kubectl get hpa -n "$NAMESPACE" -o wide 2>/dev/null || echo "HPA not configured"
}

# Function to get pod resource requests/limits
get_pod_resources() {
    echo -e "${YELLOW}📊 Pod Resource Requests/Limits:${NC}"
    kubectl get pods -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n  Requests: CPU="}{.spec.containers[0].resources.requests.cpu}{", Memory="}{.spec.containers[0].resources.requests.memory}{"\n  Limits: CPU="}{.spec.containers[0].resources.limits.cpu}{", Memory="}{.spec.containers[0].resources.limits.memory}{"\n"}{end}' 2>/dev/null
}

# Function to get queue depth (from gateway logs)
get_queue_depth() {
    echo -e "${YELLOW}📊 Query Queue Status:${NC}"
    kubectl logs -n "$NAMESPACE" -l app=gateway --tail=20 2>/dev/null | grep -i "queue\|pending\|waiting" | tail -5 || echo "No queue info in recent logs"
}

# Pre-test baseline
echo -e "${GREEN}▶ BASELINE METRICS (Before Test)${NC}"
echo "=================================="
get_pod_metrics | tee "$RESULTS_DIR/baseline-pods.txt"
echo ""
get_node_metrics | tee "$RESULTS_DIR/baseline-nodes.txt"
echo ""
get_vpa_status | tee "$RESULTS_DIR/baseline-vpa.txt"
echo ""
get_hpa_status | tee "$RESULTS_DIR/baseline-hpa.txt"
echo ""
get_pod_resources | tee "$RESULTS_DIR/baseline-resources.txt"
echo ""

# Count pods before
GATEWAY_PODS_BEFORE=$(kubectl get pods -n "$NAMESPACE" -l app=gateway --no-headers 2>/dev/null | wc -l | tr -d ' ')
WORKER_PODS_BEFORE=$(kubectl get pods -n "$NAMESPACE" -l app=worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo -e "${YELLOW}Pods before test: Gateway=$GATEWAY_PODS_BEFORE, Worker=$WORKER_PODS_BEFORE${NC}"

# Start background monitoring
echo ""
echo -e "${GREEN}▶ STARTING BACKGROUND MONITORING${NC}"
echo "=================================="

# Monitor pods every 5 seconds during test
(
    while true; do
        echo "$(date +%H:%M:%S) - $(kubectl top pods -n $NAMESPACE 2>/dev/null | grep -E 'gateway|worker' | tr '\n' ' ')"
        sleep 5
    done
) > "$RESULTS_DIR/pod-metrics-timeline.txt" 2>&1 &
MONITOR_PID=$!
echo "Started pod monitor (PID: $MONITOR_PID)"

# Run stress test
echo ""
echo -e "${GREEN}▶ RUNNING STRESS TEST ($CONCURRENT CONCURRENT QUERIES)${NC}"
echo "========================================================="
echo "Query: $QUERY"
echo ""

START_TIME=$(date +%s.%N)

# Create a temp script for parallel execution
cat > /tmp/run_query.sh << 'QUERY_SCRIPT'
#!/bin/bash
HOST=$1
PORT=$2
USER=$3
PASSWORD=$4
QUERY=$5
ID=$6
START=$(date +%s.%N)
RESULT=$(PGPASSWORD="$PASSWORD" psql -h "$HOST" -p "$PORT" -U "$USER" -t -c "$QUERY" 2>&1)
END=$(date +%s.%N)
DURATION=$(echo "$END - $START" | bc)
if echo "$RESULT" | grep -q "ERROR\|error\|failed"; then
    echo "$ID,FAIL,$DURATION,\"$RESULT\""
else
    echo "$ID,OK,$DURATION,\"$RESULT\""
fi
QUERY_SCRIPT
chmod +x /tmp/run_query.sh

# Run concurrent queries using xargs for parallelism
echo "Launching $CONCURRENT queries..."
seq 1 "$CONCURRENT" | xargs -P "$CONCURRENT" -I {} /tmp/run_query.sh "$HOST" "$PORT" "$USER" "$PASSWORD" "$QUERY" {} > "$RESULTS_DIR/query-results.csv" 2>&1

END_TIME=$(date +%s.%N)
TOTAL_DURATION=$(echo "$END_TIME - $START_TIME" | bc)

# Stop monitoring
kill $MONITOR_PID 2>/dev/null || true

echo ""
echo -e "${GREEN}▶ TEST COMPLETE${NC}"
echo "==============="

# Analyze results
echo ""
echo -e "${GREEN}▶ RESULTS ANALYSIS${NC}"
echo "==================="

TOTAL=$(wc -l < "$RESULTS_DIR/query-results.csv" | tr -d ' ')
SUCCESS=$(grep -c ",OK," "$RESULTS_DIR/query-results.csv" 2>/dev/null || echo 0)
FAILED=$(grep -c ",FAIL," "$RESULTS_DIR/query-results.csv" 2>/dev/null || echo 0)

# Calculate latency statistics
if [ -f "$RESULTS_DIR/query-results.csv" ]; then
    LATENCIES=$(grep ",OK," "$RESULTS_DIR/query-results.csv" | cut -d',' -f3 | sort -n)
    if [ -n "$LATENCIES" ]; then
        MIN_LATENCY=$(echo "$LATENCIES" | head -1)
        MAX_LATENCY=$(echo "$LATENCIES" | tail -1)
        MEDIAN_LATENCY=$(echo "$LATENCIES" | awk '{a[NR]=$1} END {print a[int(NR/2)]}')
        AVG_LATENCY=$(echo "$LATENCIES" | awk '{sum+=$1} END {print sum/NR}')
        P95_LATENCY=$(echo "$LATENCIES" | awk '{a[NR]=$1} END {print a[int(NR*0.95)]}')
        P99_LATENCY=$(echo "$LATENCIES" | awk '{a[NR]=$1} END {print a[int(NR*0.99)]}')
    fi
fi

echo -e "${BLUE}Summary:${NC}"
echo "  Total queries:     $TOTAL"
echo "  Successful:        $SUCCESS ($(echo "scale=1; $SUCCESS * 100 / $TOTAL" | bc)%)"
echo "  Failed:            $FAILED ($(echo "scale=1; $FAILED * 100 / $TOTAL" | bc)%)"
echo "  Total duration:    ${TOTAL_DURATION}s"
echo "  Throughput:        $(echo "scale=2; $SUCCESS / $TOTAL_DURATION" | bc) queries/sec"
echo ""
echo -e "${BLUE}Latency (successful queries):${NC}"
echo "  Min:     ${MIN_LATENCY:-N/A}s"
echo "  Max:     ${MAX_LATENCY:-N/A}s"
echo "  Avg:     ${AVG_LATENCY:-N/A}s"
echo "  Median:  ${MEDIAN_LATENCY:-N/A}s"
echo "  P95:     ${P95_LATENCY:-N/A}s"
echo "  P99:     ${P99_LATENCY:-N/A}s"

# Save summary
cat > "$RESULTS_DIR/summary.txt" << EOF
TAVANA STRESS TEST SUMMARY
==========================
Date: $(date)
Host: $HOST:$PORT
Concurrent Queries: $CONCURRENT
Query: $QUERY

RESULTS
-------
Total:      $TOTAL
Successful: $SUCCESS ($(echo "scale=1; $SUCCESS * 100 / $TOTAL" | bc)%)
Failed:     $FAILED
Duration:   ${TOTAL_DURATION}s
Throughput: $(echo "scale=2; $SUCCESS / $TOTAL_DURATION" | bc) qps

LATENCY (seconds)
-----------------
Min:    ${MIN_LATENCY:-N/A}
Max:    ${MAX_LATENCY:-N/A}
Avg:    ${AVG_LATENCY:-N/A}
Median: ${MEDIAN_LATENCY:-N/A}
P95:    ${P95_LATENCY:-N/A}
P99:    ${P99_LATENCY:-N/A}
EOF

# Post-test metrics
echo ""
echo -e "${GREEN}▶ POST-TEST METRICS${NC}"
echo "===================="
get_pod_metrics | tee "$RESULTS_DIR/posttest-pods.txt"
echo ""
get_node_metrics | tee "$RESULTS_DIR/posttest-nodes.txt"
echo ""
get_vpa_status | tee "$RESULTS_DIR/posttest-vpa.txt"
echo ""
get_hpa_status | tee "$RESULTS_DIR/posttest-hpa.txt"
echo ""

# Count pods after
GATEWAY_PODS_AFTER=$(kubectl get pods -n "$NAMESPACE" -l app=gateway --no-headers 2>/dev/null | wc -l | tr -d ' ')
WORKER_PODS_AFTER=$(kubectl get pods -n "$NAMESPACE" -l app=worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo -e "${YELLOW}Pods after test: Gateway=$GATEWAY_PODS_AFTER, Worker=$WORKER_PODS_AFTER${NC}"

if [ "$GATEWAY_PODS_AFTER" -gt "$GATEWAY_PODS_BEFORE" ] || [ "$WORKER_PODS_AFTER" -gt "$WORKER_PODS_BEFORE" ]; then
    echo -e "${GREEN}✓ HPA scaled up pods during test!${NC}"
else
    echo -e "${YELLOW}→ No HPA scaling detected (may need more sustained load)${NC}"
fi

# Show any errors
if [ "$FAILED" -gt 0 ]; then
    echo ""
    echo -e "${RED}▶ SAMPLE ERRORS:${NC}"
    grep ",FAIL," "$RESULTS_DIR/query-results.csv" | head -5
fi

echo ""
echo -e "${GREEN}▶ RESULTS SAVED TO: $RESULTS_DIR${NC}"
echo "Files:"
ls -la "$RESULTS_DIR"

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  STRESS TEST COMPLETE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

