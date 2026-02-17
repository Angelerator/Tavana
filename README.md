# Tavana

**The open-source alternative to MotherDuck** — because every duck needs a powerful father.

Cloud-agnostic, auto-scaling SQL query engine for data lakes. Query petabytes of Delta Lake, Parquet, and Iceberg data using standard SQL through PostgreSQL-compatible clients or high-performance Arrow Flight SQL. Deploy on your own infrastructure, keep full control of your data.

## Features

- **DuckDB-Powered**: Fast analytical queries with columnar execution
- **Auto-Scaling**: Horizontal (2-20 pods) and vertical (memory resizing) scaling based on query load
- **Multi-Protocol**: PostgreSQL wire protocol + Arrow Flight SQL (ADBC)
- **Cloud Storage**: S3, Azure ADLS Gen2, Google Cloud Storage
- **Data Formats**: Delta Lake, Parquet, Iceberg, CSV, JSON
- **Streaming Results**: Server-side cursors for memory-efficient large result sets
- **Flexible Credentials**: Infrastructure-level (env vars, Workload Identity) with per-session user overrides via `SET` / `CREATE SECRET`

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
  -p 50051:443 \
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
      - "50051:443"
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

### Arrow Flight SQL (Port 443 recommended)

For best network compatibility (especially on Azure/corporate networks), use port 443.

**Python (ADBC) - Recommended for large datasets:**
```python
import adbc_driver_flightsql.dbapi as adbc

# Use port 443 for best Azure/corporate network compatibility
with adbc.connect("grpc://tavana.example.com:443") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM delta_scan('s3://bucket/table')")
        table = cur.fetch_arrow_table()  # Zero-copy Arrow data
```

**JDBC:**
```
jdbc:arrow-flight-sql://tavana.example.com:443/?useEncryption=false
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

## Credential Management

Tavana supports two layers of credential configuration. User-provided credentials take priority over infrastructure-level defaults.

**Infrastructure-level** (environment variables, Workload Identity):
```bash
# Set via Helm values, Docker env, or Kubernetes secrets
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AZURE_STORAGE_ACCOUNT_NAME=...
```

**User-level overrides** (per-session, via any SQL client):
```sql
-- Override S3 credentials for this session
SET s3_access_key_id = 'AKIA...';
SET s3_secret_access_key = '...';
SET s3_region = 'eu-west-1';

-- Or use DuckDB secrets for more complex setups
CREATE SECRET my_azure (TYPE azure, PROVIDER access_token, ACCESS_TOKEN '...', ACCOUNT_NAME '...');

-- Then query as usual — credentials apply automatically
SELECT * FROM read_parquet('s3://my-private-bucket/data.parquet');

-- Remove a secret override (reverts to infra defaults)
DROP SECRET my_azure;
```

User credentials are session-scoped, forwarded securely to workers per-query, and cleaned up automatically so they never leak between users on shared connection pools.

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
| `TAVANA_GRPC_STREAM_WINDOW_MB` | `512` | HTTP/2 per-stream window size (MB) |
| `TAVANA_GRPC_CONN_WINDOW_MB` | `1024` | HTTP/2 connection window size (MB) |
| `TAVANA_GRPC_CHANNEL_BUFFER` | `256` | Internal streaming channel buffer |

**Gateway (Query Caching):**
| Variable | Default | Description |
|----------|---------|-------------|
| `TAVANA_CACHE_REDIS_URL` | - | Redis/Dragonfly URL (e.g., `redis://redis:6379`) |
| `TAVANA_CACHE_TTL_SECS` | `300` | Default cache TTL (5 minutes) |
| `TAVANA_CACHE_MAX_SIZE_MB` | `100` | Maximum cacheable result size |
| `TAVANA_CACHE_COMPRESSION` | `true` | Enable LZ4 compression |
| `TAVANA_CACHE_VERSION` | `1` | Cache version (increment to invalidate all) |
| `TAVANA_CACHE_L1_ENABLED` | `true` | Enable in-memory L1 cache |
| `TAVANA_CACHE_L1_SIZE` | `1000` | L1 cache entries |

**Worker:**
| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50053` | gRPC server port |
| `MAX_MEMORY_GB` | `0` (auto) | Max memory for DuckDB |
| `THREADS` | auto | DuckDB threads |

**Worker (Remote File Caching):**
| Variable | Default | Description |
|----------|---------|-------------|
| `TAVANA_CACHE_HTTPFS_ENABLED` | `true` | Enable cache_httpfs extension |
| `TAVANA_CACHE_HTTPFS_DIR` | `/tmp/duckdb_cache` | Cache directory |
| `TAVANA_CACHE_HTTPFS_SIZE_GB` | `50` | Maximum disk cache size |
| `TAVANA_CACHE_HTTPFS_BLOCK_SIZE_MB` | `8` | Block size for caching |
| `TAVANA_CACHE_HTTPFS_PARALLEL_IO` | `8` | Parallel I/O requests |
| `TAVANA_CACHE_HTTPFS_PROFILING` | - | Enable cache profiling |

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
  # gRPC performance tuning (high-throughput Arrow streaming)
  grpc:
    streamWindowMB: 512      # HTTP/2 per-stream window
    connectionWindowMB: 1024 # HTTP/2 connection window
    channelBuffer: 256       # Streaming buffer size

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

# Query Result Caching (Redis/Dragonfly)
# 25x faster with Dragonfly vs Redis
queryCache:
  enabled: false  # Requires Redis/Dragonfly deployment
  redisUrl: "redis://redis:6379"
  ttlSecs: 300
  maxResultSizeMB: 100

# Remote File Caching (cache_httpfs)
# 11.6x speedup on repeated queries
cacheHttpfs:
  enabled: true
  cacheDir: "/tmp/duckdb_cache"
  maxSizeGB: 50
  blockSizeMB: 8
  parallelIO: 8

# Network Performance Tuning (TCP BBR)
# 10x improvement on lossy networks
networkTuning:
  enabled: false  # Requires privileged DaemonSet
  tcpCongestionControl: "bbr"
  tcpRmemMaxMB: 64
  tcpWmemMaxMB: 64
```

## Performance Optimizations

Tavana includes several performance optimizations that can be enabled for production workloads:

### Query Result Caching (Redis/Dragonfly)

Cache query results in a distributed cache to avoid re-executing identical queries. Based on Redis caching best practices:

- **Two-tier caching**: L1 (in-memory, <1μs) + L2 (Redis/Dragonfly, <1ms)
- **Smart TTL**: Aggregate queries get 2x TTL, point queries get 0.5x TTL
- **LZ4 compression**: Reduces network and storage overhead for large results
- **Dragonfly recommended**: 25x faster than Redis, drop-in compatible

```yaml
# values.yaml
queryCache:
  enabled: true
  redisUrl: "redis://dragonfly:6379"  # Or redis://redis:6379
  ttlSecs: 300                         # 5 minute default TTL
```

### Remote File Caching (cache_httpfs)

The `cache_httpfs` DuckDB community extension caches remote HTTP/S3/Azure data blocks locally:

- **11.6x speedup** on repeated queries (documented benchmarks)
- **On-disk caching** persists across query executions
- **Parallel I/O** with configurable parallelism
- **Automatic** for all remote file access

```yaml
# values.yaml
cacheHttpfs:
  enabled: true      # Enabled by default
  maxSizeGB: 50      # Disk cache size
  parallelIO: 8      # Concurrent connections
```

### Network Performance Tuning (TCP BBR)

A DaemonSet that applies high-performance kernel network settings to all nodes:

- **TCP BBR**: 10x performance on paths with packet loss
- **Large buffers**: 64MB TCP buffers for 10G+ networks
- **Fast Open**: Reduces latency for new connections

```yaml
# values.yaml
networkTuning:
  enabled: true
  tcpCongestionControl: "bbr"
  tcpRmemMaxMB: 64
  tcpWmemMaxMB: 64
```

**Note**: Requires privileged containers. The DaemonSet runs once at startup to apply sysctl settings.

### gRPC/HTTP/2 Optimization

Already enabled by default for optimal Arrow streaming:

- **512MB stream window**: Eliminates flow control backpressure
- **1GB connection window**: Supports multiple concurrent streams
- **256 batch buffer**: Maximizes pipeline throughput

## Project Structure

```
tavana/
├── crates/
│   ├── tavana-gateway/     # Query routing, protocols, auto-scaling
│   ├── tavana-worker/      # DuckDB execution, streaming
│   └── tavana-common/      # Shared types, gRPC definitions
├── helm/tavana/            # Kubernetes Helm chart
├── proto/                  # gRPC protocol definitions
├── Dockerfile.gateway
└── Dockerfile.worker
```

## License

Apache 2.0
