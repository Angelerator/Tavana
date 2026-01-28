# Tavana Helm Chart

Cloud-Agnostic Auto-Scaling DuckDB Query Platform

## TL;DR

```bash
# From Docker Hub
helm install tavana oci://ghcr.io/tavana/charts/tavana

# Or from local
helm install tavana ./helm/tavana
```

## Introduction

This chart bootstraps a Tavana deployment on a Kubernetes cluster using the Helm package manager.

Tavana consists of:
- **Gateway**: Accepts PostgreSQL wire protocol and Arrow Flight SQL (ADBC) connections, manages query queuing and routing
- **Workers**: Execute DuckDB queries against cloud storage (S3, ADLS, GCS)

## Client Connectivity

Tavana supports multiple connection protocols:

### PostgreSQL Wire Protocol (Port 5432)
Standard PostgreSQL-compatible clients:
```bash
psql -h tavana-gateway -p 5432 -U user -c "SELECT * FROM delta_scan('s3://bucket/table')"
```

### Arrow Flight SQL / ADBC (Port 9091)
High-performance Arrow-native connectivity for analytics applications:

**Python (ADBC)**:
```python
import adbc_driver_flightsql.dbapi

with adbc_driver_flightsql.dbapi.connect("grpc://tavana-gateway:9091") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM delta_scan('s3://bucket/table')")
        table = cur.fetch_arrow_table()  # Zero-copy Arrow data
```

**Python (pyarrow.flight)**:
```python
import pyarrow.flight as flight

client = flight.connect("grpc://tavana-gateway:9091")
info = client.get_flight_info(flight.FlightDescriptor.for_command(b"SELECT 1"))
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
```

**Go (ADBC)**:
```go
import "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"

driver := flightsql.NewDriver()
db, _ := driver.Open("grpc://tavana-gateway:9091")
```

**JDBC**:
```
jdbc:arrow-flight-sql://tavana-gateway:9091/?useEncryption=false
```

## Prerequisites

- Kubernetes 1.28+
- Helm 3.12+
- PV provisioner support (for monitoring stack)
- Metrics Server (for HPA)

## Installing the Chart

### From OCI Registry (Recommended)

```bash
helm install tavana oci://ghcr.io/tavana/charts/tavana \
  --namespace tavana \
  --create-namespace \
  --set global.imageRegistry=myacr.azurecr.io
```

### From Source

```bash
git clone https://github.com/tavana/tavana.git
helm install tavana ./helm/tavana -n tavana --create-namespace
```

## Uninstalling

```bash
helm uninstall tavana -n tavana
```

## Configuration

### Global Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global.namespace` | Kubernetes namespace | `tavana` |
| `global.imageRegistry` | Container registry | `""` (Docker Hub) |
| `global.imagePullPolicy` | Image pull policy | `IfNotPresent` |
| `global.imageTag` | Image tag | `latest` |

### Gateway

| Parameter | Description | Default |
|-----------|-------------|---------|
| `gateway.enabled` | Deploy gateway | `true` |
| `gateway.replicaCount` | Number of replicas | `2` |
| `gateway.resources.requests.memory` | Memory request | `1Gi` |
| `gateway.resources.limits.memory` | Memory limit | `4Gi` |
| `gateway.service.pgPort` | PostgreSQL port (internal) | `5432` |
| `gateway.flightSql.enabled` | Enable Arrow Flight SQL (ADBC) | `true` |
| `gateway.flightSql.port` | Flight SQL gRPC port | `9091` |
| `gateway.loadBalancer.enabled` | Enable external LoadBalancer | `false` |
| `gateway.loadBalancer.externalPort` | External PostgreSQL port | `5432` |
| `gateway.loadBalancer.exposeFlight` | Expose Flight SQL externally | `false` |
| `gateway.loadBalancer.flightExternalPort` | External Flight SQL port | `9091` |
| `gateway.loadBalancer.annotations` | LoadBalancer annotations | `{}` |

### Worker

| Parameter | Description | Default |
|-----------|-------------|---------|
| `worker.enabled` | Deploy workers | `true` |
| `worker.replicaCount` | Initial replicas | `2` |
| `worker.minReplicas` | HPA min replicas | `2` |
| `worker.maxReplicas` | HPA max replicas | `20` |
| `worker.resources.requests.memory` | Memory request | `2Gi` |
| `worker.resources.limits.memory` | Memory limit | `12Gi` |
| `worker.nodeSelector` | Node selector | `{}` |
| `worker.tolerations` | Tolerations | `[]` |

### HPA (Horizontal Pod Autoscaler)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `hpa.enabled` | Enable HPA | `true` |
| `hpa.cpuTargetUtilization` | CPU target % | `70` |
| `hpa.queueDepthTarget` | Queue depth target | `5` |
| `hpa.queueWaitTimeSecondsTarget` | Wait time target | `30s` |

### Object Storage

| Parameter | Description | Default |
|-----------|-------------|---------|
| `objectStorage.endpoint` | S3 endpoint | `""` |
| `objectStorage.bucket` | Bucket name | `""` |
| `objectStorage.region` | AWS region | `us-east-1` |
| `objectStorage.existingSecret` | Secret with credentials | `""` |

### Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create service account | `true` |
| `serviceAccount.name` | Service account name | `tavana` |
| `serviceAccount.annotations` | SA annotations | `{}` |
| `networkPolicies.enabled` | Enable network policies | `true` |
| `podSecurityContext.runAsNonRoot` | Run as non-root | `true` |

## Cloud-Specific Configuration

### Azure (AKS)

```yaml
# values-azure.yaml
global:
  imageRegistry: "myacr.azurecr.io"

serviceAccount:
  annotations:
    azure.workload.identity/client-id: "YOUR_CLIENT_ID"
    azure.workload.identity/tenant-id: "YOUR_TENANT_ID"

worker:
  nodeSelector:
    nodepool: tavana
  tolerations:
    - key: "workload"
      operator: "Equal"
      value: "tavana"
      effect: "NoSchedule"
```

### AWS (EKS)

```yaml
# values-aws.yaml
global:
  imageRegistry: "123456789.dkr.ecr.us-east-1.amazonaws.com"

serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789:role/tavana-role"

objectStorage:
  bucket: "my-data-bucket"
  region: "us-east-1"
```

### GCP (GKE)

```yaml
# values-gcp.yaml
global:
  imageRegistry: "gcr.io/my-project"

serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: "tavana@my-project.iam.gserviceaccount.com"

objectStorage:
  endpoint: "https://storage.googleapis.com"
  bucket: "my-data-bucket"
```

## Using with ArgoCD

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: tavana
  namespace: argocd
spec:
  source:
    repoURL: oci://ghcr.io/tavana/charts
    chart: tavana
    targetRevision: "1.0.0"
    helm:
      valueFiles:
        - values-azure.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: tavana
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

## Upgrading

### To 1.x

No breaking changes.

## Troubleshooting

### Pods stuck in Pending

Check node resources:
```bash
kubectl describe node
kubectl top nodes
```

### Image pull errors

Verify registry access:
```bash
kubectl create job test-pull --image=YOUR_REGISTRY/tavana/gateway:latest -- echo ok
kubectl logs job/test-pull
```

### Connection refused

Check services:
```bash
kubectl get svc -n tavana
kubectl port-forward svc/gateway -n tavana 5432:5432
```

## License

Apache 2.0
