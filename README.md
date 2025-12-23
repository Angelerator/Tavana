# Tavana

**Cloud-Agnostic Auto-Scaling DuckDB Query Platform**

Tavana is a production-ready Kubernetes application that hosts DuckDB instances for executing analytical queries on remote object storage data (S3, ADLS, GCS). It automatically scales resources based on query demand and workload complexity.

## ✨ Features

- **Cloud Agnostic**: Deploy on Azure, AWS, GCP, or any Kubernetes cluster
- **Auto-Scaling**: Intelligent HPA + VPA scaling based on queue depth and resource usage
- **PostgreSQL Compatible**: Connect with Tableau, PowerBI, DBeaver, psql
- **Smart Queuing**: FIFO queue with capacity-aware scheduling
- **GitOps Ready**: Terraform + ArgoCD deployment
- **Secure by Default**: Pod security, network policies, workload identity
- **Observable**: Prometheus metrics, Grafana dashboards

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                       │
│    Tableau / PowerBI / DBeaver / psql (PostgreSQL Wire Protocol)               │
└─────────────────────────────────────┬──────────────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                           KUBERNETES CLUSTER                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                           TAVANA NAMESPACE                                │  │
│  │                                                                           │  │
│  │   ┌─────────────────────────────────────────────────────────────────┐    │  │
│  │   │                        GATEWAY (2+ pods)                         │    │  │
│  │   │   • PostgreSQL Wire Protocol (port 5432)                        │    │  │
│  │   │   • Smart Query Queue (FIFO, capacity-aware)                    │    │  │
│  │   │   • Worker Pool Management                                       │    │  │
│  │   │   • Prometheus Metrics                                           │    │  │
│  │   └─────────────────────────────────┬───────────────────────────────┘    │  │
│  │                                     │ gRPC                               │  │
│  │                                     ▼                                    │  │
│  │   ┌─────────────────────────────────────────────────────────────────┐    │  │
│  │   │                      WORKERS (2-20 pods, HPA)                    │    │  │
│  │   │   • DuckDB Query Execution                                       │    │  │
│  │   │   • Streaming Results                                            │    │  │
│  │   │   • VPA Resource Resizing                                        │    │  │
│  │   │   • Pre-installed Parquet/HTTPFS Extensions                      │    │  │
│  │   └─────────────────────────────────┬───────────────────────────────┘    │  │
│  │                                     │                                    │  │
│  └─────────────────────────────────────┼────────────────────────────────────┘  │
│                                        │                                       │
└────────────────────────────────────────┼───────────────────────────────────────┘
                                         │
                                         ▼
                     ┌─────────────────────────────────────┐
                     │         OBJECT STORAGE              │
                     │     S3 / ADLS Gen2 / GCS            │
                     └─────────────────────────────────────┘
```

## 🚀 Quick Start

### One-Click Deployment (Azure)

```bash
./deploy.sh --subscription-id YOUR_SUBSCRIPTION_ID --env prod
```

### Manual Deployment

```bash
# 1. Deploy infrastructure with Terraform
cd terraform/azure/examples/quickstart
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
terraform init && terraform apply

# 2. Install with Helm
helm install tavana oci://ghcr.io/tavana/charts/tavana \
  --namespace tavana \
  --create-namespace

# 3. Connect
kubectl port-forward svc/gateway -n tavana 5432:5432
PGPASSWORD=tavana psql -h localhost -p 5432 -U tavana -d tavana
```

### Query Example

```sql
-- Query Parquet files from S3
SELECT * FROM read_parquet('s3://my-bucket/data/*.parquet') LIMIT 100;

-- Aggregation across millions of rows
SELECT 
    date_trunc('month', order_date) as month,
    SUM(total_amount) as revenue
FROM read_parquet('s3://my-bucket/orders/*.parquet')
GROUP BY 1
ORDER BY 1;
```

## 📦 Components

| Component | Description |
|-----------|-------------|
| `tavana-gateway` | Query entry point (PostgreSQL protocol), queue management, metrics |
| `tavana-worker` | DuckDB query execution with auto-scaling |
| `tavana-common` | Shared library (proto, auth, config) |

## 📁 Project Structure

```
tavana/
├── .github/workflows/      # CI/CD pipelines
│   ├── ci.yaml            # Build, test, lint
│   ├── release.yaml       # Docker & Helm publishing
│   └── security.yaml      # Container scanning
├── crates/
│   ├── tavana-gateway/    # Gateway service
│   ├── tavana-worker/     # Worker service
│   └── tavana-common/     # Shared library
├── terraform/
│   └── azure/             # Azure infrastructure module
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── examples/
│           ├── quickstart/
│           └── enterprise/
├── helm/
│   └── tavana/            # Helm chart
├── gitops-template/       # Customer GitOps config template
├── deploy.sh              # One-click deployment script
├── DEPLOYMENT.md          # Detailed deployment guide
├── Dockerfile.gateway
└── Dockerfile.worker
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log level (trace/debug/info/warn/error) | `info` |
| `PG_PORT` | PostgreSQL wire protocol port | `5432` |
| `WORKER_SERVICE_NAME` | Kubernetes worker service name | `worker` |

### Helm Values

See [helm/tavana/values.yaml](./helm/tavana/values.yaml) for all options.

Key settings:

```yaml
gateway:
  replicaCount: 2
  resources:
    limits:
      memory: "4Gi"

worker:
  minReplicas: 2
  maxReplicas: 20
  resources:
    limits:
      memory: "12Gi"
```

## 📊 Monitoring

Tavana exposes Prometheus metrics at `/metrics`:

| Metric | Description |
|--------|-------------|
| `tavana_query_queue_depth` | Queries waiting in queue |
| `tavana_query_queue_wait_seconds` | Time queries wait before execution |
| `tavana_query_duration_seconds` | Query execution time |
| `tavana_active_queries` | Currently executing queries |
| `tavana_worker_memory_bytes` | Worker memory usage |

Import dashboards from `k8s/monitoring/` into Grafana.

## 🔐 Security

- **Pod Security**: Non-root, read-only filesystem, dropped capabilities
- **Network Policies**: Deny by default, explicit allow rules
- **Workload Identity**: Azure/AWS/GCP native identity (no credentials)
- **TLS**: All internal communication encrypted

## 🗺️ Roadmap

- [ ] AWS EKS Terraform module
- [ ] GCP GKE Terraform module
- [ ] Query caching with Redis
- [ ] Multi-tenancy with namespaces
- [ ] Catalog integration (Unity Catalog, Iceberg)

## 📚 Documentation

- [Deployment Guide](./DEPLOYMENT.md)
- [Helm Chart](./helm/tavana/README.md)
- [Terraform Modules](./terraform/README.md)

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines.

## 📄 License

Apache License 2.0
