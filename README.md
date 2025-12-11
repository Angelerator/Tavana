# Tavana

**Cloud-Agnostic Auto-Scaling DuckDB Query Platform**

Tavana is a cloud-agnostic Kubernetes application that hosts DuckDB instances for executing analytical queries on remote object storage data (S3, ADLS, GCS). It automatically estimates required resources per query and creates appropriately-sized pods for execution.

## Features

- **Cloud Agnostic**: Deploy on AWS, Azure, GCP, or any Kubernetes cluster
- **Auto-Scaling**: Intelligent per-query pod sizing based on data and query complexity
- **Multiple Protocols**: 
  - PostgreSQL wire protocol (for Tableau, PowerBI)
  - Arrow Flight SQL (for Python, Polars, DuckDB)
  - REST API (for management)
- **Unity Catalog Compatible**: Standard catalog API for integration with data ecosystem tools
- **Secure by Default**: TLS everywhere, API Key & OIDC authentication
- **Observable**: Full OpenTelemetry integration (traces, metrics, logs)
- **Fine-grained Billing**: Comprehensive usage metering for BYOC deployments

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                 │
├────────────────┬─────────────────────┬──────────────────────────────────-┤
│  Tableau/PBI   │   Python/Polars     │     Management UI                 │
│  (PostgreSQL)  │   (Flight SQL)      │     (REST API)                    │
└───────┬────────┴─────────┬───────────┴─────────────┬────────────────────-┘
        │                  │                         │
        └──────────────────┼─────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │    tavana-gateway      │
              │  (Query Entry Point)   │
              └───────────┬────────────┘
                          │ gRPC
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
┌─────────────────┐  ┌─────────────┐  ┌──────────────────┐
│ tavana-catalog  │  │  tavana-    │  │ tavana-metering  │
│ (Metadata)      │  │  operator   │  │ (Usage Tracking) │
└─────────────────┘  │ (K8s CRD)   │  └──────────────────┘
                     └──────┬──────┘
                            │ Creates Pods
                            ▼
              ┌─────────────────────────┐
              │    Worker Pods          │
              │  ┌─────┐ ┌─────┐       │
              │  │DuckDB│ │DuckDB│ ...  │
              │  └─────┘ └─────┘       │
              └──────────┬──────────────┘
                         │
                         ▼
              ┌─────────────────────────┐
              │   Object Storage        │
              │  S3 / ADLS / GCS        │
              └─────────────────────────┘
```

## Components

| Component | Description |
|-----------|-------------|
| `tavana-gateway` | Query entry point supporting PG wire, Flight SQL, REST |
| `tavana-operator` | Kubernetes operator managing DuckDBQuery CRDs and worker pods |
| `tavana-worker` | DuckDB execution pod for running queries |
| `tavana-catalog` | Metadata service with Unity Catalog compatible REST API |
| `tavana-metering` | Usage tracking and billing metrics collection |
| `tavana-common` | Shared library with types, auth, TLS, protobuf definitions |

## Quick Start

### Prerequisites

- Rust 1.75+
- Docker
- Kubernetes cluster (Docker Desktop, minikube, or cloud)
- PostgreSQL 15+ (for catalog and metering)

### Development Setup

```bash
# Clone the repository
git clone https://github.com/tavana/tavana.git
cd tavana

# Build all crates
cargo build

# Run tests
cargo test

# Generate proto code
cargo build -p tavana-common
```

### Running Locally

```bash
# Start PostgreSQL
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=tavana \
  -e POSTGRES_DB=tavana \
  -p 5432:5432 \
  postgres:15

# Start the gateway
cargo run -p tavana-gateway

# Start the catalog
DATABASE_URL=postgres://postgres:tavana@localhost:5432/tavana \
cargo run -p tavana-catalog
```

### Deploying to Kubernetes

```bash
# Apply CRDs
kubectl apply -f k8s/crds/

# Deploy using Helm
helm install tavana ./helm/tavana \
  --namespace tavana \
  --create-namespace
```

## Configuration

All services are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `TLS_ENABLED` | Enable TLS for all connections | `true` |
| `LOG_LEVEL` | Logging level (trace, debug, info, warn, error) | `info` |
| `DATABASE_URL` | PostgreSQL connection URL | - |
| `OTLP_ENDPOINT` | OpenTelemetry collector endpoint | - |

See individual service documentation for more configuration options.

## Connecting Clients

### Tableau / PowerBI

```
Host: gateway.tavana.example.com
Port: 5432
Database: tavana
Username: <your-user>
Password: <your-api-key>
SSL: Require
```

### Python with Polars

```python
import polars as pl

# Using Flight SQL
uri = "grpc+tls://gateway.tavana.example.com:8815"
df = pl.read_database(
    "SELECT * FROM catalog.schema.table LIMIT 1000",
    connection=uri,
)
```

### DuckDB

```sql
-- Install the flight extension
INSTALL flight;
LOAD flight;

-- Connect to Tavana
ATTACH 'grpc://gateway.tavana.example.com:8815' AS tavana (TYPE FLIGHT);

-- Query data
SELECT * FROM tavana.catalog.schema.table;
```

## Development

### Project Structure

```
tavana/
├── Cargo.toml              # Workspace definition
├── proto/                  # Protocol buffer definitions
│   └── tavana/v1/
│       ├── common.proto
│       ├── query.proto
│       └── catalog.proto
├── crates/
│   ├── tavana-common/      # Shared library
│   ├── tavana-gateway/     # Query gateway
│   ├── tavana-operator/    # K8s operator
│   ├── tavana-worker/      # Query worker
│   ├── tavana-catalog/     # Metadata service
│   └── tavana-metering/    # Usage tracking
├── k8s/                    # Kubernetes manifests
└── helm/                   # Helm charts
```

### Running Tests

```bash
# Unit tests
cargo test

# Integration tests (requires running services)
cargo test --features integration

# Specific crate
cargo test -p tavana-gateway
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please read our contributing guidelines first.

