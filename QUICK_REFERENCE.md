# Tavana Quick Reference

## 🚀 One-Line Install

```bash
helm install tavana oci://ghcr.io/angelerator/charts/tavana --version 1.0.0
```

## 📦 Docker Images

```bash
# Docker Hub (Public)
docker pull tavana/gateway:v1.0.0
docker pull tavana/worker:v1.0.0

# GitHub Container Registry (Public)
docker pull ghcr.io/angelerator/tavana-gateway:v1.0.0
docker pull ghcr.io/angelerator/tavana-worker:v1.0.0
```

## 🔧 Quick Deploy on Azure

```bash
# 1. Clone repo
git clone https://github.com/Angelerator/Tavana.git
cd Tavana

# 2. Deploy infrastructure
cd terraform/azure/examples/quickstart
terraform init
terraform apply

# 3. Get kubectl credentials
az aks get-credentials --resource-group tavana-dev-rg --name tavana-dev-aks

# 4. Install Tavana
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  --version 1.0.0 \
  -n tavana \
  --create-namespace \
  -f ../../helm/tavana/values-azure.yaml

# 5. Test connection
kubectl port-forward svc/tavana-gateway -n tavana 5432:5432
psql -h localhost -p 5432 -U tavana
```

## 🔍 Monitor Status

```bash
# Check pods
kubectl get pods -n tavana

# Check logs
kubectl logs -n tavana -l app=tavana-gateway
kubectl logs -n tavana -l app=tavana-worker

# Check metrics
kubectl port-forward -n tavana svc/tavana-gateway 8080:8080
curl http://localhost:8080/metrics
```

## 📊 Key Metrics (Prometheus)

```
tavana_query_queue_depth                # Queries waiting
tavana_query_queue_wait_seconds         # Wait time
tavana_active_queries                   # Currently running
tavana_worker_memory_mb                 # Worker memory
tavana_hpa_scale_up_signal              # Scale-up triggers
tavana_resource_ceiling_mb              # Max available
tavana_operation_mode                   # Scaling vs Saturation
```

## 🛠️ Common Tasks

### Scale Workers Manually
```bash
kubectl scale deployment/tavana-worker -n tavana --replicas=5
```

### Update Configuration
```bash
helm upgrade tavana oci://ghcr.io/angelerator/charts/tavana \
  --version 1.0.0 \
  -n tavana \
  --set worker.resources.limits.memory=16Gi
```

### View HPA Status
```bash
kubectl get hpa -n tavana
kubectl describe hpa tavana-worker -n tavana
```

### Restart Components
```bash
kubectl rollout restart deployment/tavana-gateway -n tavana
kubectl rollout restart deployment/tavana-worker -n tavana
```

## 🔒 Security

### Verify Image Signatures
```bash
cosign verify tavana/gateway:v1.0.0
cosign verify tavana/worker:v1.0.0
```

### Scan for Vulnerabilities
```bash
trivy image tavana/gateway:v1.0.0
trivy image tavana/worker:v1.0.0
```

## 🐛 Troubleshooting

### Gateway Not Responding
```bash
# Check logs
kubectl logs -n tavana deployment/tavana-gateway --tail=100

# Check service
kubectl get svc -n tavana tavana-gateway

# Check endpoints
kubectl get endpoints -n tavana tavana-gateway
```

### Workers Not Scaling
```bash
# Check HPA
kubectl describe hpa -n tavana

# Check metrics server
kubectl top nodes
kubectl top pods -n tavana

# Check queue
kubectl exec -it -n tavana deployment/tavana-gateway -- \
  curl http://localhost:8080/metrics | grep queue_depth
```

### Query Failing
```bash
# Check worker logs
kubectl logs -n tavana -l app=tavana-worker --tail=100

# Check if workers can access storage
kubectl exec -it -n tavana deployment/tavana-worker -- \
  wget -O- http://your-storage/test.parquet
```

## 📝 Configuration Files

```
tavana/
├── helm/
│   └── tavana/
│       ├── values.yaml              # Default values
│       ├── values-azure.yaml        # Azure overrides
│       └── templates/               # K8s manifests
├── terraform/
│   └── azure/
│       ├── main.tf                  # Azure module
│       └── examples/
│           ├── quickstart/          # Minimal setup
│           └── enterprise/          # Production setup
└── .github/
    └── workflows/
        ├── ci.yaml                  # Lint, test, build
        ├── release.yaml             # Multi-arch images
        └── security.yaml            # Trivy, audit
```

## 🔗 Important Links

- **GitHub**: https://github.com/Angelerator/Tavana
- **Docker Hub**: https://hub.docker.com/r/tavana
- **Releases**: https://github.com/Angelerator/Tavana/releases
- **Issues**: https://github.com/Angelerator/Tavana/issues
- **Docs**: See README.md, DEPLOYMENT.md

## 📞 Support

- **Issues**: Open a GitHub issue
- **Contributing**: See CONTRIBUTING.md
- **Documentation**: See DEPLOYMENT.md

## 🏷️ Version Info

- **Current**: v1.0.0
- **Released**: December 23, 2025
- **License**: Apache 2.0
- **Language**: Rust
- **Min Kubernetes**: 1.28+

