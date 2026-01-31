# True Streaming with Server-Side Cursors Architecture

## Executive Summary

This document analyzes Tavana's current streaming architecture against industry best
practices from PostgreSQL, Trino, Snowflake, CockroachDB, StarRocks, and DuckDB.
Based on deep codebase analysis, we identify what's already implemented, what gaps
remain, and propose targeted improvements.

---

## Current Tavana Implementation Analysis

### What's Already Implemented ✅

Based on deep codebase analysis, Tavana already has significant streaming infrastructure:

#### 1. True Streaming from Worker (grpc.rs)

```rust
// grpc.rs:166 - TRUE STREAMING with callback
let result = executor.execute_query_streaming(&sql, |batch| {
    // Each batch sent immediately as DuckDB produces it
    // Never buffered - TRUE STREAMING
    total_rows += batch.num_rows() as u64;
    // ... serialize and send via channel
});
```

- **Channel buffer**: 64 for query execution, 32 for cursor fetch
- **Comment confirms**: "TRUE STREAMING: batches are sent as they're produced, never buffered"
- **Cancellation support**: via `QueryCancellationToken`

#### 2. Server-Side Cursor Manager (cursor_manager.rs)

```rust
pub struct ActiveCursor {
    pub id: String,
    batches: Mutex<Vec<RecordBatch>>,  // Small, bounded buffer
    current_offset: AtomicUsize,        // LIMIT/OFFSET pagination
    exhausted: AtomicBool,
    sql: String,                        // For re-pagination
    schema: Arc<Schema>,
}
```

- **Max batch size**: 10,000 rows (configurable via `CURSOR_BATCH_SIZE`)
- **LIMIT/OFFSET pagination**: Prevents OOM for large tables
- **TTL cleanup**: Background task evicts idle cursors
- **Statistics**: Active cursor count, rows fetched, oldest cursor age

#### 3. Gateway Cursor Support (cursors.rs)

```rust
pub struct CursorState {
    pub query: String,
    pub columns: Vec<(String, String)>,
    pub current_offset: usize,
    pub exhausted: bool,
    pub worker_id: Option<String>,      // ✅ Stored!
    pub worker_addr: Option<String>,    // ✅ Stored but NOT USED!
    pub uses_true_streaming: bool,
}
```

- **True streaming flag**: Distinguishes worker-side cursors from LIMIT/OFFSET fallback
- **Worker ID stored**: From `DeclareCursorResponse`
- **FETCH implementation**: Uses worker's `FetchCursor` gRPC when true streaming enabled

#### 4. Backpressure System (backpressure.rs)

```rust
pub struct BackpressureConfig {
    pub flush_threshold_bytes: usize,   // 64KB (TCP window size)
    pub flush_threshold_rows: usize,    // 100 rows
    pub flush_timeout_secs: u64,        // 300 seconds (patient)
    pub write_buffer_size: usize,       // 32KB (small for early pressure)
}
```

**Multi-layer backpressure:**
1. **Bytes-based flushing**: 64KB threshold
2. **TCP-level backpressure**: 32KB BufWriter
3. **Bounded channels**: Size 4 between gRPC and socket writer
4. **Slow client detection**: >5s flush time logged
5. **Connection health checks**: Every 500 rows

#### 5. Portal State for Extended Query Protocol (server.rs)

```rust
struct PortalState {
    rows: Vec<Vec<String>>,  // Buffered rows
    offset: usize,           // For resumption
    column_count: usize,
}
```

- **Max buffer**: 50,000 rows (configurable via `TAVANA_MAX_PORTAL_BUFFER_ROWS`)
- **PortalSuspended**: Sent when max_rows reached
- **Resume support**: Subsequent Execute reads from buffer

#### 6. Delta PIN_SNAPSHOT Caching (executor.rs) - NEW ✅

```rust
pub struct DeltaTableCache {
    attached_tables: RwLock<HashMap<String, AttachedDeltaTable>>,
    max_tables: usize,  // Default: 50
    ttl_secs: u64,      // Default: 3600
}
```

- **Auto-attach**: Delta tables attached with `PIN_SNAPSHOT`
- **Query rewriting**: `delta_scan('path')` → `"cached_alias"`
- **LRU eviction**: When max tables exceeded
- **1.47× speedup**: Per DuckDB benchmarks

---

### Current Architecture Diagram

```
┌─────────┐     ┌────────────────────────────────────────────────────────────┐
│         │     │                      GATEWAY                               │
│         │     │                                                            │
│ DBeaver │     │  ┌────────────────┐    ┌─────────────────────────────┐    │
│ Tableau │────▶│  │ ConnectionState │    │ BackpressureWriter          │    │
│ JDBC    │     │  │ - cursors       │    │ - 64KB flush threshold      │    │
│         │     │  │ - portal_state  │    │ - 32KB write buffer         │    │
│         │◀────│  │ - schema_cache  │    │ - slow client detection     │    │
│         │     │  └────────────────┘    └─────────────────────────────┘    │
│         │     │                                     │                      │
│         │     │  ┌────────────────────────────────────────────────────┐   │
│         │     │  │ Extended Query Protocol Handler                    │   │
│         │     │  │ - Parse/Bind/Describe/Execute                      │   │
│         │     │  │ - Portal state (buffered, max 50K rows)            │   │
│         │     │  │ - PortalSuspended for pagination                   │   │
│         │     │  └────────────────────────────────────────────────────┘   │
│         │     │                                                            │
│         │     │  ┌────────────────────────────────────────────────────┐   │
│         │     │  │ DECLARE CURSOR Handler (cursors.rs)                │   │
│         │     │  │ - True streaming via worker cursors                │   │
│         │     │  │ - Fallback: LIMIT/OFFSET pagination                │   │
│         │     │  │ - Worker ID/addr stored (but not routed!)          │   │
│         │     │  └────────────────────────────────────────────────────┘   │
└─────────┘     └────────────────────────────────────────────────────────────┘
                                          │
                                          │ gRPC (bounded channel, size=4)
                                          ▼
                ┌────────────────────────────────────────────────────────────┐
                │                       WORKER                               │
                │                                                            │
                │  ┌─────────────────────────────────────────────────────┐  │
                │  │ QueryServiceImpl                                    │  │
                │  │ - execute_query (true streaming via callback)       │  │
                │  │ - declare_cursor / fetch_cursor / close_cursor      │  │
                │  │ - cancellation support                              │  │
                │  └─────────────────────────────────────────────────────┘  │
                │                                                            │
                │  ┌─────────────────────────────────────────────────────┐  │
                │  │ CursorManager                                       │  │
                │  │ - max_cursors: 100 (configurable)                   │  │
                │  │ - LIMIT/OFFSET pagination                           │  │
                │  │ - TTL cleanup: 300s (5 minutes)                     │  │
                │  │ - Bounded buffers: 10K rows per batch               │  │
                │  └─────────────────────────────────────────────────────┘  │
                │                                                            │
                │  ┌─────────────────────────────────────────────────────┐  │
                │  │ DeltaTableCache (NEW!)                              │  │
                │  │ - ATTACH with PIN_SNAPSHOT                          │  │
                │  │ - Auto-attach for delta_scan queries                │  │
                │  │ - 1.47× speedup via metadata caching                │  │
                │  └─────────────────────────────────────────────────────┘  │
                │                                                            │
                │  ┌─────────────────────────────────────────────────────┐  │
                │  │ DuckDbExecutor                                      │  │
                │  │ - Connection pool (default: 4)                      │  │
                │  │ - True streaming via Arrow iterator                 │  │
                │  │ - Azure token refresh (background thread)           │  │
                │  └─────────────────────────────────────────────────────┘  │
                └────────────────────────────────────────────────────────────┘
```

---

### Identified Gaps ⚠️

#### Gap 1: Cursor Affinity Routing NOT Implemented

**Code evidence** (cursors.rs:406):
```rust
// Uses worker_client passed in, NOT cursor.worker_addr!
match worker_client.fetch_cursor(&cursor_name, max_rows).await {
```

**Current behavior**:
- `worker_id` and `worker_addr` are stored in `CursorState`
- But FETCH always uses the default `WorkerClient`
- In multi-worker setup, FETCH goes to wrong worker → "cursor not found"

**Impact**: True streaming only works with single worker.

#### Gap 2: Extended Query Protocol Still Buffers

**Code evidence** (server.rs:1663-1665):
```rust
struct PortalState {
    rows: Vec<Vec<String>>,  // TODO: Replace with Stream for true streaming
    ...
}
```

**Current behavior**:
- Extended Query Protocol (JDBC) buffers all rows in `PortalState`
- Limited to 50,000 rows to prevent OOM
- Larger results rejected with "use DECLARE CURSOR"

**Impact**: DBeaver/JDBC queries buffer entire result before first row.

#### Gap 3: No Result Spooling for Very Large Results

**Current limits**:
- Portal buffer: 50,000 rows
- Cursor batch: 10,000 rows
- No disk/object storage spillover

**Impact**: Results exceeding memory limits fail.

---

## Comparison with Industry Best Practices

### PostgreSQL Extended Query Protocol

| Feature | PostgreSQL | Tavana | Gap? |
|---------|------------|--------|------|
| Portal support | ✅ Full | ✅ Partial | Buffered, not streaming |
| Execute(max_rows) | ✅ Streaming | ⚠️ Buffered first | Portal buffers all rows |
| DECLARE CURSOR | ✅ Server-side | ✅ Implemented | Works! |
| WITH HOLD cursors | ✅ Persist after txn | ❌ Not implemented | Low priority |

### Trino Spooling Protocol

| Feature | Trino | Tavana | Gap? |
|---------|-------|--------|------|
| Direct streaming | ✅ For small results | ✅ Simple Query only | Extended Query buffered |
| Object storage spool | ✅ For large results | ❌ Not implemented | Phase 4 candidate |
| Parallel writing | ✅ Workers → S3 | ❌ Not implemented | Complex |
| URL-based retrieval | ✅ Client gets URLs | ❌ Not implemented | Different model |

**Recommendation**: Trino's spooling is elegant but complex. For Tavana, simpler
approaches (streaming cursors) may be more appropriate.

### StarRocks Buffer Management

| Feature | StarRocks | Tavana | Gap? |
|---------|-----------|--------|------|
| Send buffer bounds | 256KB-2MB | ✅ 32KB-64KB | Good, smaller for early backpressure |
| Auto-flush | ✅ On buffer full | ✅ On threshold | Good |
| Blocking I/O | ✅ writeBlocking | ✅ async flush | Good (relies on tokio) |
| Packet splitting | ✅ >16MB | N/A | PostgreSQL max is 1GB |

**Conclusion**: Tavana's backpressure is well-implemented, similar to StarRocks.

### Snowflake Streaming

| Feature | Snowflake | Tavana | Gap? |
|---------|-----------|--------|------|
| Streaming ResultSet | ✅ streamResult=true | ⚠️ Simple Query only | Extended Query buffered |
| Backpressure | ✅ Driver 1.6.23+ | ✅ Multi-layer | Good |
| Batch streaming | ✅ streamRows(start,end) | ⚠️ Via cursors | Works but verbose |
| Distributed fetch | ✅ get_result_batches() | ❌ Not applicable | Single-worker model |

### CockroachDB Cursors

| Feature | CockroachDB | Tavana | Gap? |
|---------|-------------|--------|------|
| Cursor affinity | ✅ Routes to owner | ⚠️ Stored, not used | **Key gap** |
| Keyset pagination | ✅ Alternative | ❌ Not implemented | Could add |
| LIMIT/OFFSET fallback | ✅ But discouraged | ✅ Implemented | Good |

### DuckDB Arrow Streaming

| Feature | DuckDB | Tavana | Gap? |
|---------|--------|--------|------|
| Batch-at-a-time | ✅ Iterator API | ✅ Implemented | Good |
| Zero-copy | ✅ Arrow native | ✅ JSON fallback | Arrow IPC available |
| PIN_SNAPSHOT | ✅ ATTACH caching | ✅ NEW! Implemented | Good |

---

## Recommended Improvements (Prioritized)

### Priority 1: Fix Cursor Affinity Routing (1-2 days)

**Problem**: `worker_addr` stored but never used for routing.

**Solution**: Create WorkerClient per worker address.

```rust
// In cursors.rs, modify handle_fetch_cursor:

pub async fn handle_fetch_cursor(
    sql: &str,
    cursors: &mut ConnectionCursors,
    worker_clients: &WorkerClientPool,  // NEW: pool of clients
    user_id: &str,
) -> Option<CursorResult> {
    // ... existing code ...
    
    if cursor.uses_true_streaming {
        // Route to correct worker!
        let worker_client = if let Some(addr) = &cursor.worker_addr {
            worker_clients.get_or_create(addr).await
        } else {
            worker_clients.default()
        };
        
        match worker_client.fetch_cursor(&cursor_name, max_rows).await {
            // ...
        }
    }
}
```

**New component**:
```rust
pub struct WorkerClientPool {
    clients: DashMap<String, WorkerClient>,
    default_addr: String,
}

impl WorkerClientPool {
    pub async fn get_or_create(&self, addr: &str) -> Arc<WorkerClient> {
        if let Some(client) = self.clients.get(addr) {
            return client.clone();
        }
        let client = WorkerClient::connect(addr).await?;
        self.clients.insert(addr.to_string(), client.clone());
        client
    }
}
```

**Impact**: True streaming works in multi-worker deployments.

### Priority 2: True Streaming for Extended Query Protocol (1 week)

**Problem**: Portal buffers all rows before sending any.

**Solution**: Replace `Vec<Vec<String>>` with gRPC stream handle.

```rust
// Instead of buffering:
struct PortalState {
    rows: Vec<Vec<String>>,  // Current: buffers all
}

// Stream handle:
struct PortalState {
    stream: Option<tonic::Streaming<QueryResultBatch>>,  // NEW: holds stream
    schema: Arc<Schema>,
    rows_sent: u64,
}
```

**How it works**:
1. First Execute: Open gRPC stream, send first `max_rows`
2. PortalSuspended: Keep stream handle in `PortalState`
3. Next Execute: Continue reading from same stream
4. Close: Drop stream handle

**Complexity**: Medium - requires changing Execute handler flow.

### Priority 3: Schema Caching for Non-Parameterized Queries (2 days)

**Problem**: LIMIT 0 schema detection runs for each Describe.

**Solution**: Cache by Delta table path.

```rust
// In server.rs, extend schema_cache:
let mut schema_cache: HashMap<String, Vec<(String, String)>> = HashMap::new();
let mut delta_schema_cache: HashMap<String, Vec<(String, String)>> = HashMap::new();

// For delta_scan queries, extract path and cache:
if sql.contains("delta_scan") {
    if let Some(path) = extract_delta_path(sql) {
        if let Some(schema) = delta_schema_cache.get(&path) {
            return Ok(schema.clone());
        }
        // Execute LIMIT 0, cache result
        delta_schema_cache.insert(path, schema);
    }
}
```

**Impact**: Reduces Azure Blob metadata reads.

### Priority 4: Result Spooling (Phase 4, Optional, 2 weeks)

**Problem**: Results exceeding memory limits fail.

**Solution**: Spool to Azure Blob Storage (Trino-style).

```rust
pub struct ResultSpooler {
    storage: Arc<dyn ObjectStore>,
    config: SpoolConfig,
}

impl ResultSpooler {
    pub async fn should_spool(&self, estimated_size: usize) -> bool {
        estimated_size > self.config.memory_threshold
    }
    
    pub async fn spool(&self, cursor_id: &str, batches: impl Iterator<Item = RecordBatch>) 
        -> Result<SpoolSession>;
}
```

**When to spool**: When result exceeds `max_portal_buffer_rows` (50K).

---

## Configuration Reference (Current + Proposed)

```yaml
# CURRENT - Worker configuration
CURSOR_BATCH_SIZE: 10000              # Rows per cursor batch
MAX_CURSORS: 100                      # Max concurrent cursors
CURSOR_IDLE_TIMEOUT_SECS: 300         # TTL for idle cursors
CURSOR_CLEANUP_INTERVAL_SECS: 60      # Cleanup frequency

# CURRENT - Delta caching (NEW!)
DELTA_CACHE_MAX_TABLES: 50            # Max attached tables
DELTA_CACHE_TTL_SECS: 3600            # Metadata cache TTL

# CURRENT - Gateway backpressure
TAVANA_FLUSH_THRESHOLD_BYTES: 65536   # 64KB
TAVANA_FLUSH_THRESHOLD_ROWS: 100
TAVANA_FLUSH_TIMEOUT_SECS: 300        # Patient waiting
TAVANA_WRITE_BUFFER_SIZE: 32768       # 32KB (small for early pressure)
TAVANA_MAX_PORTAL_BUFFER_ROWS: 50000  # Portal OOM protection

# PROPOSED - Cursor routing
TAVANA_WORKER_CLIENT_POOL_SIZE: 10    # Max cached worker connections
TAVANA_WORKER_CLIENT_TIMEOUT_SECS: 30 # Connection timeout

# PROPOSED - Result spooling (Phase 4)
TAVANA_SPOOL_ENABLED: false
TAVANA_SPOOL_THRESHOLD_ROWS: 50000    # Start spooling after this
TAVANA_SPOOL_STORAGE_TYPE: azure_blob # azure_blob | s3
TAVANA_SPOOL_CONTAINER: tavana-spool
TAVANA_SPOOL_TTL_HOURS: 1
```

---

## Metrics Reference (Current + Proposed)

### Current Metrics

```rust
// Worker (via Prometheus endpoint)
cursor_active_count: Gauge,
cursor_rows_fetched_total: Counter,

// Gateway (via logs currently)
streaming_rows_sent: logged per query
slow_flush_count: logged per connection
```

### Proposed Metrics

```rust
// Worker
cursor_declared_total: Counter,
cursor_fetch_latency_seconds: Histogram,
cursor_ttl_evictions_total: Counter,
delta_cache_hits_total: Counter,
delta_cache_misses_total: Counter,

// Gateway
cursor_route_hits_total: Counter,      // Affinity routing success
cursor_route_misses_total: Counter,    // Fallback to default worker
portal_buffer_rows: Histogram,         // Distribution of portal sizes
portal_overflow_total: Counter,        // Results exceeding buffer limit
streaming_backpressure_events: Counter,
```

---

## Implementation Roadmap

### Phase 1: Cursor Affinity (Priority 1) - 2 days

| Task | Effort | Files |
|------|--------|-------|
| Create `WorkerClientPool` | 0.5 day | `worker_client.rs` |
| Update `handle_fetch_cursor` to use pool | 0.5 day | `cursors.rs` |
| Update `handle_close_cursor` to use pool | 0.5 day | `cursors.rs` |
| Test with multi-worker setup | 0.5 day | Integration tests |

### Phase 2: Extended Query Streaming (Priority 2) - 1 week

| Task | Effort | Files |
|------|--------|-------|
| Add stream handle to `PortalState` | 1 day | `server.rs` |
| Modify Execute handler for streaming | 2 days | `server.rs` |
| Handle Parse clearing stream | 0.5 day | `server.rs` |
| Handle Close clearing stream | 0.5 day | `server.rs` |
| Test with DBeaver scrolling | 1 day | Manual testing |

### Phase 3: Schema Caching (Priority 3) - 2 days

| Task | Effort | Files |
|------|--------|-------|
| Add `delta_schema_cache` | 0.5 day | `server.rs` |
| Extract Delta path from queries | 0.5 day | `pg_compat.rs` |
| Cache invalidation logic | 0.5 day | TTL-based |
| Test with repeated queries | 0.5 day | Manual testing |

### Phase 4: Result Spooling (Optional) - 2 weeks

| Task | Effort | Files |
|------|--------|-------|
| Create `ResultSpooler` component | 2 days | New file |
| Azure Blob integration | 2 days | Uses existing `object_store` |
| Integrate with Portal handling | 3 days | `server.rs` |
| Cleanup background task | 1 day | Background thread |
| Test with 1M+ row results | 2 days | Load testing |

---

## Expected Benefits

| Metric | Current | After Phase 1-3 | After Phase 4 |
|--------|---------|-----------------|---------------|
| Multi-worker cursor routing | ❌ Broken | ✅ Works | ✅ Works |
| Extended Query first-row latency | 30s (buffered) | <1s (streaming) | <1s |
| Gateway memory (1M rows) | OOM | ~50MB (streaming) | ~50MB |
| Max result size | 50K rows | 50K (cursor unlimited) | Unlimited |
| Delta metadata reads | Per Describe | Once per session | Once |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Stream handle lifetime management | Memory leaks | TTL + Close handling |
| Worker failure with active stream | Client error | Graceful retry with new cursor |
| Azure Blob latency for spooling | Slow large results | Only for >50K rows |
| Backward compatibility | Breaking changes | Feature flags for new behavior |

---

## Conclusion

Tavana's streaming infrastructure is **more mature than initially assessed**:

✅ **Already excellent**:
- True streaming from DuckDB (batch-at-a-time)
- Multi-layer backpressure (StarRocks-style)
- Server-side cursors with LIMIT/OFFSET fallback
- Delta PIN_SNAPSHOT caching (1.47× speedup)

⚠️ **Key gaps to fix**:
1. **Cursor affinity routing** (2 days) - Critical for multi-worker
2. **Extended Query streaming** (1 week) - Eliminates buffering
3. **Schema caching** (2 days) - Reduces metadata reads

📋 **Nice to have**:
4. **Result spooling** (2 weeks) - For very large results

The recommended approach prioritizes **small, high-impact changes** that leverage
existing infrastructure rather than wholesale architecture replacement.
