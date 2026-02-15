# Tavana

Cloud-agnostic, auto-scaling SQL query engine for data lakes. Query petabytes of Delta Lake, Parquet, and Iceberg data using standard SQL through PostgreSQL-compatible clients or high-performance Arrow Flight SQL.

## Features

- **DuckDB-Powered**: Fast analytical queries with columnar execution
- **Auto-Scaling**: Horizontal (2-20 pods) and vertical (memory resizing) scaling based on query load
- **Multi-Protocol**: PostgreSQL wire protocol + Arrow Flight SQL (ADBC)
- **Cloud Storage**: S3, Azure ADLS Gen2, Google Cloud Storage
- **Data Formats**: Delta Lake, Parquet, Iceberg, CSV, JSON
- **Streaming Results**: Server-side cursors for memory-efficient large result sets

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│     Clients     │     │     Clients     │
│  (psql, Tableau │     │  (Python ADBC,  │
│   DBeaver, etc) │     │   Arrow Flight) │
└────────┬────────┘     └────────┬────────┘
         │ PostgreSQL            │ gRPC
         │ Wire Protocol         │ Flight SQL
         └──────────┬────────────┘
                    ▼
           ┌────────────────┐
           │    Gateway     │
           │  (Query Queue, │
           │   Routing, HPA)│
           └────────┬───────┘
                    │ gRPC
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ Worker  │ │ Worker  │ │ Worker  │
   │ (DuckDB)│ │ (DuckDB)│ │ (DuckDB)│
   └─────────┘ └─────────┘ └─────────┘
        │           │           │
        └───────────┼───────────┘
                    ▼
           ┌────────────────┐
           │  Cloud Storage │
           │ (S3/ADLS/GCS)  │
           └────────────────┘
```

## Quick Start

### Docker Desktop

**1. Pull images:**
```bash
docker pull angelerator/tavana-gateway:latest
docker pull angelerator/tavana-worker:latest
```

**2. Create network:**
```bash
docker network create tavana-network
```

**3. Start worker:**
```bash
docker run -d --name tavana-worker --network tavana-network \
  -p 50053:50053 \
  -e GRPC_PORT=50053 \
  -e MAX_MEMORY_GB=4 \
  angelerator/tavana-worker:latest
```

**4. Start gateway:**
```bash
docker run -d --name tavana-gateway --network tavana-network \
  -p 5432:15432 \
  -p 9091:443 \
  -p 8080:8080 \
  -e PG_PORT=15432 \
  -e FLIGHT_SQL_PORT=443 \
  -e WORKER_ADDR=http://tavana-worker:50053 \
  angelerator/tavana-gateway:latest
```

**5. Connect:**
```bash
psql -h localhost -p 5432 -U tavana -c "SELECT 1"
```

#### Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  worker:
    image: angelerator/tavana-worker:latest
    ports:
      - "50053:50053"
    environment:
      - GRPC_PORT=50053
      - MAX_MEMORY_GB=4

  gateway:
    image: angelerator/tavana-gateway:latest
    ports:
      - "5432:15432"
      - "9091:443"
      - "8080:8080"
    environment:
      - PG_PORT=15432
      - FLIGHT_SQL_PORT=443
      - WORKER_ADDR=http://worker:50053
    depends_on:
      - worker
```

```bash
docker-compose up -d
```

### Kubernetes (Helm)

**1. Install from OCI registry:**
```bash
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  --namespace tavana \
  --create-namespace
```

**2. Install from source:**
```bash
git clone https://github.com/Angelerator/Tavana.git
cd Tavana
helm install tavana ./helm/tavana -n tavana --create-namespace
```

**3. Access via port-forward:**
```bash
kubectl port-forward -n tavana svc/gateway 5432:5432 &
psql -h localhost -p 5432 -U tavana -c "SELECT 1"
```

**4. Enable LoadBalancer (production):**
```bash
helm upgrade tavana ./helm/tavana -n tavana \
  --set gateway.loadBalancer.enabled=true
```

#### Cloud-Specific Examples

**Azure (AKS + ADLS Gen2):**
```bash
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  -n tavana --create-namespace \
  -f helm/tavana/values-azure.yaml \
  --set global.imageRegistry=YOUR_ACR.azurecr.io \
  --set serviceAccount.annotations."azure\.workload\.identity/client-id"=YOUR_CLIENT_ID
```

**AWS (EKS + S3):**
```bash
helm install tavana oci://ghcr.io/angelerator/charts/tavana \
  -n tavana --create-namespace \
  --set global.imageRegistry=YOUR_ACCOUNT.dkr.ecr.REGION.amazonaws.com \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=YOUR_ROLE_ARN \
  --set objectStorage.bucket=your-bucket \
  --set objectStorage.region=us-east-1
```

## Connecting Clients

### PostgreSQL Protocol (Port 5432)

**psql:**
```bash
psql -h tavana.example.com -p 5432 -U user -c "SELECT * FROM delta_scan('s3://bucket/table')"
```

**Python (psycopg2):**
```python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="tavana", database="main")
cursor = conn.cursor()
cursor.execute("SELECT * FROM delta_scan('s3://bucket/table') LIMIT 100")
print(cursor.fetchall())
```

**DBeaver / Tableau:**
- Connection Type: PostgreSQL
- Host: `tavana.example.com`
- Port: `5432`
- See `clients/` folder for streaming configuration

### Arrow Flight SQL (Port 9091)

**Python (ADBC) - Recommended for large datasets:**
```python
import adbc_driver_flightsql.dbapi as adbc

with adbc.connect("grpc://localhost:9091") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM delta_scan('s3://bucket/table')")
        table = cur.fetch_arrow_table()  # Zero-copy Arrow data
```

**JDBC:**
```
jdbc:arrow-flight-sql://localhost:9091/?useEncryption=false
```

## Query Examples

```sql
-- Delta Lake
SELECT * FROM delta_scan('s3://bucket/delta-table/') LIMIT 100;
SELECT * FROM delta_scan('az://container/path/') WHERE date > '2024-01-01';

-- Delta Lake with time travel
SELECT * FROM delta_scan('s3://bucket/table/', version := 5);

-- Parquet files
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');

-- CSV files
SELECT * FROM read_csv('s3://bucket/data.csv');

-- Aggregations
SELECT category, SUM(amount) 
FROM delta_scan('az://data/sales/') 
GROUP BY category;
```

## Configuration

### Environment Variables

**Gateway:**
| Variable | Default | Description |
|----------|---------|-------------|
| `PG_PORT` | `15432` | PostgreSQL wire protocol port |
| `FLIGHT_SQL_PORT` | `443` | Arrow Flight SQL port |
| `WORKER_ADDR` | `http://localhost:50053` | Worker gRPC address |
| `TLS_ENABLED` | `false` | Enable TLS/SSL |
| `LOG_LEVEL` | `info` | Log level |

**Worker:**
| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50053` | gRPC server port |
| `MAX_MEMORY_GB` | `0` (auto) | Max memory for DuckDB |
| `THREADS` | auto | DuckDB threads |

### Helm Values

Key configuration options in `values.yaml`:

```yaml
global:
  imageRegistry: ""  # e.g., your-acr.azurecr.io
  imageTag: "latest"

gateway:
  replicaCount: 2
  loadBalancer:
    enabled: false
  flightSql:
    enabled: true

worker:
  replicaCount: 2
  minReplicas: 2
  maxReplicas: 20
  resources:
    requests:
      memory: "4Gi"
    limits:
      memory: "16Gi"

hpa:
  enabled: true
  cpuTargetUtilization: 70

objectStorage:
  endpoint: ""
  bucket: ""
  region: "us-east-1"
```

## Project Structure

```
tavana/
├── crates/
│   ├── tavana-gateway/     # Query routing, protocols, auto-scaling
│   ├── tavana-worker/      # DuckDB execution, streaming
│   └── tavana-common/      # Shared types, gRPC definitions
├── helm/tavana/            # Kubernetes Helm chart
├── clients/                # Client configuration guides
│   ├── dbeaver/
│   └── tableau/
├── proto/                  # gRPC protocol definitions
├── Dockerfile.gateway
└── Dockerfile.worker
```

## License

Apache 2.0
