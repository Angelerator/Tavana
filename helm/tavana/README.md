# Tavana Helm Chart

Cloud-Agnostic Auto-Scaling DuckDB Query Platform.

## Quick Start

### Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- Container registry with Tavana images

### Installation

```bash
# Add your custom values
helm install tavana ./helm/tavana \
  --namespace tavana \
  --create-namespace \
  --set global.imageRegistry=your-registry.azurecr.io \
  --set ingress.hosts[0].host=tavana.example.com
```

### Azure Deployment

```bash
# Use Azure-specific values
helm install tavana ./helm/tavana \
  --namespace tavana \
  --create-namespace \
  -f ./helm/tavana/values-azure.yaml \
  --set global.imageRegistry=your-acr.azurecr.io \
  --set ingress.hosts[0].host=tavana.example.com \
  --set ingress.tls[0].hosts[0]=tavana.example.com
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global.imageRegistry` | Container registry URL | `ghcr.io/angelerator` |
| `global.imageTag` | Image tag | `latest` |
| `gateway.replicaCount` | Gateway replicas | `2` |
| `worker.replicaCount` | Worker replicas | `2` |
| `worker.maxReplicas` | Max workers (HPA) | `10` |
| `ingress.enabled` | Enable ingress | `false` |
| `networkPolicies.enabled` | Enable network policies | `true` |

## Cloud-Specific Values

- `values.yaml` - Default values (works with any registry)
- `values-azure.yaml` - Azure AKS with Application Gateway

## Security

This chart follows security best practices:

- Non-root containers
- Read-only root filesystem
- Dropped capabilities
- Network policies
- Pod security standards (restricted)

## Architecture

```
┌─────────────┐     ┌─────────────┐
│   Gateway   │────►│   Workers   │
│  (2+ pods)  │     │  (2-20 pods)│
└─────────────┘     └─────────────┘
      │                    │
      │   PostgreSQL       │   gRPC
      │   Wire Protocol    │
      ▼                    ▼
   Clients            Object Storage
   (psql, DBeaver)    (S3/ADLS/GCS)
```

