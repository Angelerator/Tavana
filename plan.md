# Tavana MVP - Simplified End-to-End Plan

## MVP Architecture (Security-Hardened)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Clients                                         │
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐  │
│  │ Tableau      │    │ PowerBI      │    │ Python (Polars/DuckDB)       │  │
│  │ PostgreSQL   │    │ PostgreSQL   │    │ Arrow Flight SQL / ADBC      │  │
│  └──────┬───────┘    └──────┬───────┘    └──────────────┬───────────────┘  │
│         │ TLS               │ TLS                       │ TLS              │
└─────────┼───────────────────┼───────────────────────────┼──────────────────┘
          │ :5432             │ :5432                     │ :8815
          └───────────────────┴─────────────┬─────────────┘
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Kubernetes (Docker Desktop)                           │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    tavana-gateway (Deployment)                         │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐   │ │
│  │  │ pg-wire Server  │  │ Arrow Flight    │  │ Auth Middleware      │   │ │
│  │  │ :5432           │  │ SQL :8815       │  │ API Key + OIDC       │   │ │
│  │  └─────────────────┘  └─────────────────┘  └──────────────────────┘   │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐   │ │
│  │  │ Query Analyzer  │  │ Resource        │  │ OpenTelemetry        │   │ │
│  │  │ (sqlparser-rs)  │  │ Estimator       │  │ Exporter             │   │ │
│  │  └─────────────────┘  └─────────────────┘  └──────────────────────┘   │ │
│  └───────────────────────────────┬────────────────────────────────────────┘ │
│                                  │ gRPC (TLS)                               │
│                    ┌─────────────┴─────────────┐                            │
│                    ▼                           ▼                            │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐  │
│  │ tavana-catalog (Deployment) │  │ tavana-operator (Deployment)        │  │
│  │ ┌─────────────────────────┐ │  │ ┌─────────────────────────────────┐ │  │
│  │ │ Unity Catalog REST API  │ │  │ │ CRD: DuckDBQuery                │ │  │
│  │ │ /api/2.1/unity-catalog  │ │  │ │ Reconciler Loop                 │ │  │
│  │ └─────────────────────────┘ │  │ │ Pod Lifecycle Manager           │ │  │
│  │ ┌─────────────────────────┐ │  │ └─────────────────────────────────┘ │  │
│  │ │ Internal gRPC API       │ │  └──────────────────┬──────────────────┘  │
│  │ │ Table/Schema Lookup     │ │                     │ Creates pods        │
│  │ └─────────────────────────┘ │                     ▼                     │
│  │ ┌─────────────────────────┐ │  ┌─────────────────────────────────────┐  │
│  │ │ PostgreSQL              │ │  │ duckdb-worker-{id} (Pod)            │  │
│  │ │ (StatefulSet)           │ │  │ ┌─────────────────────────────────┐ │  │
│  │ └─────────────────────────┘ │  │ │ DuckDB + httpfs/s3/azure/gcs    │ │  │
│  └─────────────────────────────┘  │ │ Query Executor                  │ │  │
│                                    │ │ Result Streamer (gRPC)          │ │  │
│                                    │ └─────────────────────────────────┘ │  │
│                                    │ Resources: Estimated CPU/Memory     │  │
│                                    └─────────────────────────────────────┘  │
│                                                   │                         │
└───────────────────────────────────────────────────┼─────────────────────────┘
                                                    │ Cloud IAM
                                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Object Storage (Real Cloud)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ AWS S3          │  │ Azure ADLS      │  │ Google GCS      │             │
│  │ Parquet/Delta/  │  │ Parquet/Delta/  │  │ Parquet/Delta/  │             │
│  │ Iceberg/CSV     │  │ Iceberg/CSV     │  │ Iceberg/CSV     │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Security Architecture (Hardened from Day 1)

| Layer | Security Measure |

|-------|------------------|

| **External Traffic** | TLS 1.3, certificate validation |

| **Internal Traffic** | TLS between all services (cert-manager) |

| **Authentication** | API Keys (hashed, rotatable) + OIDC (JWT validation) |

| **Authorization** | Token-based, scopes for read/write/admin |

| **Secrets** | K8s Secrets + optional Vault integration |

| **Cloud Access** | Workload Identity (no stored credentials) |

| **Pod Security** | Non-root, read-only filesystem, security contexts |

| **Network** | NetworkPolicies restricting pod-to-pod traffic |

## Resource Estimation Engine

### Multi-Join Estimation with Statistics

```rust
pub async fn estimate_resources(
    query: &ParsedQuery,
    catalog: &CatalogClient,
    storage: &ObjectStore,
    history: &QueryHistoryClient,  // NEW: Learn from past queries
) -> Result<PodResources> {
    // 1. Check if similar query was run before (query fingerprinting)
    let query_fingerprint = compute_query_fingerprint(query);
    if let Some(historical) = history.get_similar_query(&query_fingerprint).await? {
        // Use actual historical execution metrics with small buffer
        return Ok(PodResources {
            memory_mb: (historical.actual_peak_memory_mb as f64 * 1.1) as u32,
            cpu_millicores: (historical.actual_cpu_used as f64 * 1.1) as u32,
        });
    }
    
    // 2. Gather table statistics
    let mut table_stats: HashMap<String, TableStats> = HashMap::new();
    for table in query.referenced_tables() {
        let meta = catalog.get_table(&table).await?;
        let stats = match meta.format {
            Format::Parquet => read_parquet_stats(storage, &meta.location).await?,
            Format::Delta => read_delta_stats(storage, &meta.location).await?,
            Format::Iceberg => read_iceberg_stats(storage, &meta.location).await?,
            Format::Csv | Format::Json => {
                // Check if we computed stats before
                if let Some(cached) = catalog.get_cached_stats(&table).await? {
                    cached
                } else {
                    // HEAD request + estimate
                    let size = storage.head(&meta.location).await?.size;
                    TableStats::estimated_from_size(size, meta.format)
                }
            }
        };
        table_stats.insert(table.clone(), stats);
    }
    
    // 3. Estimate join chain with cardinality estimation
    let joins = query.extract_joins();
    let mut peak_memory = 0u64;
    let mut running_rows = 0u64;
    
    for (i, join) in joins.iter().enumerate() {
        let left_stats = if i == 0 {
            table_stats.get(&join.left_table).unwrap()
        } else {
            &TableStats { row_count: running_rows, ..Default::default() }
        };
        let right_stats = table_stats.get(&join.right_table).unwrap();
        
        // Detect PK-FK relationships for accurate estimation
        let relationship = detect_join_relationship(
            left_stats.column_stats(&join.left_col),
            right_stats.column_stats(&join.right_col),
        );
        
        let (output_rows, join_memory) = match relationship {
            JoinRelationship::OneToMany { many_side, .. } => {
                let rows = if many_side == join.left_table { 
                    left_stats.row_count 
                } else { 
                    right_stats.row_count 
                };
                let mem = right_stats.size_bytes * 2; // Hash table
                (rows, mem)
            }
            JoinRelationship::ManyToMany { selectivity } => {
                let rows = (left_stats.row_count as f64 * right_stats.row_count as f64 * selectivity) as u64;
                let mem = left_stats.size_bytes.min(right_stats.size_bytes) * 3;
                (rows, mem)
            }
            _ => {
                let rows = left_stats.row_count.max(right_stats.row_count);
                let mem = (left_stats.size_bytes + right_stats.size_bytes) / 2;
                (rows, mem)
            }
        };
        
        peak_memory = peak_memory.max(join_memory);
        running_rows = output_rows;
    }
    
    // 4. Apply complexity factors
    let mut memory_mb = (peak_memory / 1_000_000) as u32;
    if query.has_aggregations() { memory_mb = (memory_mb as f64 * 1.3) as u32; }
    if query.has_order_by() { memory_mb = (memory_mb as f64 * 1.2) as u32; }
    if query.has_window_functions() { memory_mb = (memory_mb as f64 * 1.4) as u32; }
    if query.has_distinct() { memory_mb = (memory_mb as f64 * 1.2) as u32; }
    
    // 5. Apply learned correction factor from historical data
    let correction = history.get_estimation_correction_factor(
        &query.tables(),
        &query.operations(),
    ).await?.unwrap_or(1.0);
    memory_mb = (memory_mb as f64 * correction) as u32;
    
    let memory_mb = memory_mb.clamp(512, 65536);
    let cpu_millicores = (memory_mb / 4).clamp(500, 16000);
    
    Ok(PodResources { memory_mb, cpu_millicores })
}
```

## Query History & Learning System

### Data Model

```rust
// Logged after every query execution
#[derive(Serialize)]
pub struct QueryExecutionLog {
    // Identity
    pub query_id: Uuid,
    pub query_fingerprint: String,      // Normalized SQL hash for similarity
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    
    // Query details
    pub sql_text: String,
    pub sql_normalized: String,         // Parameters replaced with ?
    pub tables_accessed: Vec<String>,
    pub join_count: u32,
    pub has_aggregation: bool,
    pub has_window_function: bool,
    
    // Estimation vs Reality
    pub estimated_memory_mb: u32,
    pub estimated_cpu_millicores: u32,
    pub estimated_rows: u64,
    pub actual_peak_memory_mb: u32,     // From cgroup metrics
    pub actual_cpu_seconds: f64,        // From cgroup metrics
    pub actual_rows_returned: u64,
    
    // Table statistics at query time (for debugging estimation)
    pub table_stats_snapshot: HashMap<String, TableStatsSnapshot>,
    
    // Execution metrics
    pub queue_time_ms: u64,
    pub execution_time_ms: u64,
    pub result_size_bytes: u64,
    pub pod_name: String,
    pub worker_node: String,
    
    // Outcome
    pub status: QueryStatus,            // Success, Failed, Timeout, OOM
    pub error_message: Option<String>,
    
    // For billing
    pub cost_estimate_usd: f64,
}

#[derive(Serialize)]
pub struct TableStatsSnapshot {
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_distinct_counts: HashMap<String, u64>,
}
```

### Storage Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Query History Storage                             │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              Real-time (Hot) - PostgreSQL                    │    │
│  │  • Last 24 hours of queries                                  │    │
│  │  • Fast lookup for similar query matching                    │    │
│  │  • Indexed on query_fingerprint, tables_accessed             │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              │ Daily rollup job                      │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │           Historical (Cold) - Object Storage                 │    │
│  │  • Parquet files partitioned by date                         │    │
│  │  • s3://tavana-logs/query_history/year=2024/month=12/day=09/ │    │
│  │  • Queryable via DuckDB for analytics                        │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              │ Aggregation                           │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │           Aggregated Stats - PostgreSQL                      │    │
│  │  • Per-table estimation accuracy metrics                     │    │
│  │  • Per-query-pattern correction factors                      │    │
│  │  • Updated daily by analytics job                            │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Learning Loop

```rust
// Daily job to improve estimation accuracy
pub async fn update_estimation_model(history_store: &HistoryStore) -> Result<()> {
    // 1. Compute correction factors per table combination
    let correction_factors = history_store.query(r#"
        SELECT 
            tables_accessed,
            AVG(actual_peak_memory_mb::float / estimated_memory_mb) as memory_correction,
            AVG(actual_cpu_seconds / (estimated_cpu_millicores / 1000.0 * execution_time_ms / 1000.0)) as cpu_correction,
            COUNT(*) as sample_count,
            STDDEV(actual_peak_memory_mb::float / estimated_memory_mb) as memory_stddev
        FROM query_history
        WHERE status = 'Success'
          AND timestamp > NOW() - INTERVAL '30 days'
        GROUP BY tables_accessed
        HAVING COUNT(*) >= 10  -- Need enough samples
    "#).await?;
    
    // 2. Store correction factors
    for factor in correction_factors {
        history_store.upsert_correction_factor(CorrectionFactor {
            tables: factor.tables_accessed,
            memory_multiplier: factor.memory_correction.clamp(0.5, 2.0),
            cpu_multiplier: factor.cpu_correction.clamp(0.5, 2.0),
            confidence: 1.0 - (factor.memory_stddev / factor.memory_correction).min(1.0),
            sample_count: factor.sample_count,
            updated_at: Utc::now(),
        }).await?;
    }
    
    // 3. Detect systematic under/over estimation patterns
    let patterns = history_store.query(r#"
        SELECT 
            CASE 
                WHEN has_window_function THEN 'window_function'
                WHEN join_count > 3 THEN 'multi_join'
                WHEN has_aggregation THEN 'aggregation'
                ELSE 'simple'
            END as pattern,
            AVG(actual_peak_memory_mb::float / estimated_memory_mb) as correction
        FROM query_history
        WHERE status = 'Success'
        GROUP BY 1
    "#).await?;
    
    // 4. Update global complexity factors based on patterns
    for pattern in patterns {
        history_store.update_complexity_factor(&pattern.pattern, pattern.correction).await?;
    }
    
    Ok(())
}
```

### Query Fingerprinting (For Similarity)

```rust
// Normalize SQL to find similar queries
pub fn compute_query_fingerprint(sql: &str) -> String {
    let parsed = sqlparser::parse(sql).unwrap();
    
    // 1. Normalize: replace literals with placeholders
    let normalized = normalize_literals(&parsed);
    // SELECT * FROM users WHERE id = 123 → SELECT * FROM users WHERE id = ?
    
    // 2. Sort table names (order shouldn't matter for similarity)
    let tables = extract_tables_sorted(&normalized);
    
    // 3. Extract structural features
    let features = QueryFeatures {
        tables,
        join_types: extract_join_types(&normalized),
        aggregations: extract_aggregations(&normalized),
        has_subquery: has_subquery(&normalized),
        has_cte: has_cte(&normalized),
    };
    
    // 4. Hash for fast lookup
    let mut hasher = blake3::Hasher::new();
    hasher.update(features.canonical_string().as_bytes());
    hasher.finalize().to_hex().to_string()
}
```

### Analytics Queries (Self-Service)

Users can query their own history:

```sql
-- Via Tavana (querying the history as a table)
SELECT 
    DATE_TRUNC('day', timestamp) as day,
    COUNT(*) as query_count,
    SUM(actual_cpu_seconds) as total_cpu_seconds,
    SUM(result_size_bytes) / 1e9 as total_gb_returned,
    AVG(execution_time_ms) as avg_latency_ms,
    SUM(cost_estimate_usd) as estimated_cost
FROM tavana.system.query_history
WHERE user_id = current_user()
  AND timestamp > NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1;

-- Find slow queries
SELECT 
    sql_normalized,
    COUNT(*) as executions,
    AVG(execution_time_ms) as avg_ms,
    MAX(execution_time_ms) as max_ms
FROM tavana.system.query_history
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY avg_ms DESC
LIMIT 20;

-- Estimation accuracy report
SELECT 
    tables_accessed,
    COUNT(*) as queries,
    AVG(actual_peak_memory_mb::float / estimated_memory_mb) as memory_ratio,
    AVG(actual_rows_returned::float / NULLIF(estimated_rows, 0)) as rows_ratio
FROM tavana.system.query_history
WHERE status = 'Success'
GROUP BY 1
ORDER BY queries DESC;
```

## MVP Implementation Phases

### Phase 1: Foundation (Week 1-2)

**Goal**: Basic end-to-end query execution with TLS

1. Rust workspace with crate structure
2. tavana-common: Shared types, proto definitions, TLS utilities
3. tavana-worker: DuckDB executor with gRPC result streaming
4. Basic K8s manifests for Docker Desktop
5. TLS certificate generation (self-signed for dev)

**Validation**: Worker pod can execute hardcoded query, return results over TLS gRPC

### Phase 2: Gateway + Protocols (Week 3-4)

**Goal**: External clients can connect

1. tavana-gateway with pg-wire protocol (for Tableau/PowerBI)
2. Arrow Flight SQL server (for Python/Polars)
3. API Key authentication middleware
4. Query routing to worker pods
5. OpenTelemetry instrumentation

**Validation**:

- `psql` connects, runs query, gets results
- Python Arrow Flight client connects, runs query

### Phase 3: Operator + Auto-scaling (Week 5-6)

**Goal**: Intelligent pod creation per query

1. tavana-operator with DuckDBQuery CRD
2. Reconciler creates appropriately-sized pods
3. Resource estimation (parquet footer reading)
4. Pod lifecycle management (creation, monitoring, cleanup)
5. Bounded query queue

**Validation**: Different queries spawn different-sized pods

### Phase 4: Catalog + Full Estimation (Week 7-8)

**Goal**: Table management and smart resource sizing

1. tavana-catalog with PostgreSQL backend
2. Unity Catalog REST API endpoints
3. Internal gRPC API for gateway
4. Full resource estimation (Delta, Iceberg, CSV)
5. OIDC authentication integration

**Validation**:

- Register table via Unity API
- Query references table by name
- Correct pod size based on table metadata

### Phase 5: Production Hardening (Week 9-10)

**Goal**: Production-ready security and observability

1. Full TLS everywhere (cert-manager)
2. NetworkPolicies
3. Pod security contexts
4. Prometheus metrics + Grafana dashboards
5. Distributed tracing with Jaeger
6. Metering data export

**Validation**: Security scan passes, full observability in place

## Project Structure (Simplified)

```
tavana/
├── Cargo.toml                      # Workspace
├── crates/
│   ├── tavana-common/              # Shared types, proto, TLS
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── proto.rs            # Generated from .proto
│   │   │   ├── tls.rs              # TLS config utilities
│   │   │   ├── auth.rs             # Auth token types
│   │   │   └── error.rs
│   │   └── Cargo.toml
│   ├── tavana-gateway/             # Main entry point
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── pg_wire.rs          # PostgreSQL protocol
│   │   │   ├── flight.rs           # Arrow Flight SQL
│   │   │   ├── auth.rs             # API Key + OIDC
│   │   │   ├── query.rs            # Parser + estimator
│   │   │   └── telemetry.rs
│   │   └── Cargo.toml
│   ├── tavana-operator/
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── crd.rs
│   │   │   └── reconciler.rs
│   │   └── Cargo.toml
│   ├── tavana-worker/
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   └── executor.rs
│   │   └── Cargo.toml
│   └── tavana-catalog/
│       ├── src/
│       │   ├── main.rs
│       │   ├── unity_api.rs        # REST endpoints
│       │   ├── grpc_api.rs         # Internal API
│       │   └── db.rs               # PostgreSQL
│       └── Cargo.toml
├── proto/
│   └── tavana/v1/
│       ├── common.proto
│       ├── query.proto
│       └── catalog.proto
├── deploy/
│   ├── k8s/
│   │   ├── namespace.yaml
│   │   ├── gateway.yaml
│   │   ├── operator.yaml
│   │   ├── catalog.yaml
│   │   ├── postgres.yaml
│   │   ├── network-policies.yaml
│   │   └── certificates.yaml
│   └── docker/
│       ├── gateway.Dockerfile
│       ├── operator.Dockerfile
│       ├── worker.Dockerfile
│       └── catalog.Dockerfile
└── tests/
    └── e2e/
```

## Key Dependencies

```toml
[workspace.dependencies]
# Async runtime
tokio = { version = "1.35", features = ["full"] }

# Protocols
tonic = { version = "0.11", features = ["tls"] }
arrow = "53"
arrow-flight = { version = "53", features = ["flight-sql-experimental"] }
pgwire = "0.19"  # PostgreSQL wire protocol

# K8s
kube = { version = "0.88", features = ["runtime", "derive"] }
k8s-openapi = { version = "0.21", features = ["v1_29"] }

# Data formats
duckdb = "1.0"
object_store = { version = "0.9", features = ["aws", "azure", "gcp"] }
deltalake = "0.17"
iceberg = "0.2"
parquet = "53"

# Auth
openidconnect = "3.5"
jsonwebtoken = "9"

# Observability
tracing = "0.1"
tracing-opentelemetry = "0.23"
opentelemetry = { version = "0.22", features = ["metrics", "trace"] }
opentelemetry-otlp = "0.15"

# Security
rustls = "0.22"
rcgen = "0.12"  # Certificate generation
```

## MVP Decisions Summary

| Aspect | MVP Choice |

|--------|------------|

| BI Tool Protocol | PostgreSQL wire (pg-wire) |

| Python Protocol | Arrow Flight SQL |

| Data Formats | Parquet, Delta, Iceberg, CSV, JSON |

| Clouds | S3, ADLS, GCS |

| Auth | API Keys + OIDC (minimal) |

| TLS | Everywhere (internal + external) |

| Catalog Backend | PostgreSQL |

| Catalog API | Unity Catalog compatible |

| Resource Estimation | Smart (metadata reading + query analysis) |

| Observability | Full OpenTelemetry |

| Local K8s | Docker Desktop |