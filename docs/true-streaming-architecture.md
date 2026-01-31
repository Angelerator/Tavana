# True Streaming with Server-Side Cursors Architecture

## Executive Summary

This document analyzes Tavana's current streaming architecture against industry best
practices from PostgreSQL, Trino, Snowflake, CockroachDB, StarRocks, and DuckDB.
Based on deep codebase analysis, we identify what's already implemented, what gaps
remain, and propose targeted improvements with strong emphasis on **data privacy
and tenant isolation**.

---

## Data Privacy and Tenant Isolation

### Critical Security Principles

Tavana handles queries across multiple users accessing potentially sensitive data.
**Data isolation and privacy must be enforced at every layer.**

### DuckDB Native Security Features (Recommended)

DuckDB 1.4+ provides robust security controls that Tavana should leverage:

#### 1. Connection-Level Isolation (CRITICAL)

Each user connection MUST use an isolated DuckDB connection:

```rust
// In executor.rs - ENFORCE per-user connection isolation
pub struct IsolatedConnection {
    conn: Connection,
    user_id: String,
    allowed_paths: Vec<String>,
}

impl IsolatedConnection {
    pub fn new(user_id: &str, user_allowed_paths: &[String]) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        
        // Lock down security settings for this user's session
        conn.execute("SET enable_external_access = false", [])?;
        conn.execute("SET allow_community_extensions = false", [])?;
        
        // Only allow access to user's authorized paths
        let paths_json = serde_json::to_string(user_allowed_paths)?;
        conn.execute(&format!("SET allowed_directories = {}", paths_json), [])?;
        
        // Lock configuration to prevent tampering
        conn.execute("SET lock_configuration = true", [])?;
        
        Ok(Self { conn, user_id: user_id.to_string(), allowed_paths: user_allowed_paths.to_vec() })
    }
}
```

#### 2. File System Restrictions

```sql
-- Disable all external file access by default
SET enable_external_access = false;

-- Or restrict to specific directories per tenant
SET allowed_directories = ['/data/tenant_123/'];

-- Disable specific file systems
SET disabled_filesystems = 'LocalFileSystem';
```

#### 3. Extension Security

```sql
-- Disable community extensions (only allow core extensions)
SET allow_community_extensions = false;

-- Disable auto-loading of extensions
SET autoload_known_extensions = false;
SET autoinstall_known_extensions = false;
```

#### 4. Resource Limits per Tenant

```sql
-- Limit CPU threads per query
SET threads = 4;

-- Limit memory per connection
SET memory_limit = '2GB';

-- Limit temp storage
SET max_temp_directory_size = '10GB';
```

#### 5. Configuration Locking

```sql
-- After setting up security, lock all configuration
SET lock_configuration = true;
-- Any subsequent SET commands will fail
```

### Tavana-Level Isolation Requirements

| Layer | Isolation Requirement | Implementation |
|-------|----------------------|----------------|
| **Gateway** | Per-connection state | `ConnectionState` with `user_id` |
| **Gateway** | Cursor isolation | Cursors keyed by `(connection_id, cursor_name)` |
| **Gateway** | Schema cache isolation | Cache keyed by `(user_id, sql_hash)` |
| **Worker** | Per-user DuckDB connections | Pooled by user_id, not shared |
| **Worker** | Cursor isolation | Cursor IDs include user_id prefix |
| **Worker** | Delta cache isolation | Attach tables per-user or verify access |
| **Secrets** | Never log credentials | Redact in all logs |
| **Secrets** | Per-user Azure credentials | User-specific secrets in Secrets Manager |

### Multi-Tenant Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GATEWAY                                        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Connection Handler                                                   │   │
│  │ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │   │
│  │ │ User A Conn │  │ User B Conn │  │ User C Conn │   (isolated)      │   │
│  │ │ - cursors   │  │ - cursors   │  │ - cursors   │                   │   │
│  │ │ - schema $  │  │ - schema $  │  │ - schema $  │                   │   │
│  │ │ - portal    │  │ - portal    │  │ - portal    │                   │   │
│  │ └─────────────┘  └─────────────┘  └─────────────┘                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ gRPC with user_id in metadata
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WORKER                                         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Connection Pool (per-user isolation)                                 │   │
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │   │
│  │ │ User A Pool     │  │ User B Pool     │  │ User C Pool     │       │   │
│  │ │ - allowed_dirs  │  │ - allowed_dirs  │  │ - allowed_dirs  │       │   │
│  │ │ - memory_limit  │  │ - memory_limit  │  │ - memory_limit  │       │   │
│  │ │ - locked config │  │ - locked config │  │ - locked config │       │   │
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Cursor Manager (user-prefixed IDs)                                   │   │
│  │ - "userA_cursor_1" → User A only                                     │   │
│  │ - "userB_cursor_1" → User B only                                     │   │
│  │ - Verify user_id on every FETCH/CLOSE                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Security Checklist

- [ ] Each user has isolated DuckDB connection with locked config
- [ ] `allowed_directories` set per user based on their data access
- [ ] `enable_external_access = false` by default
- [ ] `allow_community_extensions = false` always
- [ ] Cursor IDs include user_id to prevent cross-user access
- [ ] Schema cache keyed by user_id to prevent information leakage
- [ ] Azure credentials per-user via Secrets Manager
- [ ] Resource limits (memory, threads) per user
- [ ] All security settings locked via `lock_configuration = true`
- [ ] Prepared statements used for all user-provided values

---

## Current Tavana Implementation Analysis

### What's Already Implemented

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
    pub worker_id: Option<String>,      // Stored!
    pub worker_addr: Option<String>,    // Stored but NOT USED!
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

#### 6. Delta PIN_SNAPSHOT Caching (executor.rs)

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

### Identified Gaps

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

## DuckDB Native Features to Leverage

### 1. Prepared Statement Caching (Native)

DuckDB's Rust API provides `prepare_cached` for automatic statement caching:

```rust
// Native DuckDB prepared statement cache
pub fn prepare_cached(&self, sql: &str) -> Result<CachedStatement<'_>>
```

**How to use for schema caching**:
```rust
// In executor.rs - use prepare_cached for all queries
pub fn get_schema(&self, sql: &str) -> Result<Schema> {
    // prepare_cached automatically caches the prepared statement
    // including its schema metadata
    let stmt = self.conn.prepare_cached(&format!(
        "SELECT * FROM ({}) AS _schema LIMIT 0", sql
    ))?;
    
    // Schema is cached along with the prepared statement
    Ok(stmt.column_count(), stmt.column_names(), stmt.column_types())
}
```

**Benefits**:
- Works for ALL queries, not just delta_scan
- Native DuckDB caching (no custom implementation)
- Automatic eviction (LRU-based)
- Thread-safe

**Configuration**:
```rust
// Set cache capacity per connection
conn.set_prepared_statement_cache_capacity(100);

// Flush cache when needed
conn.flush_prepared_statement_cache();
```

### 2. Query Interruption

```rust
// Native query cancellation
let interrupt_handle = conn.interrupt_handle();

// In another thread/task
interrupt_handle.interrupt();
```

### 3. Arrow Streaming

```rust
// Native Arrow batch streaming (already used)
let mut stmt = conn.prepare(sql)?;
let arrow_stream = stmt.query_arrow([])?;

// Iterate batches without buffering
for batch in arrow_stream {
    process_batch(batch)?;
}
```

---

## Comparison with Industry Best Practices

### PostgreSQL Extended Query Protocol

| Feature | PostgreSQL | Tavana | Gap? |
|---------|------------|--------|------|
| Portal support | Full | Partial | Buffered, not streaming |
| Execute(max_rows) | Streaming | Buffered first | Portal buffers all rows |
| DECLARE CURSOR | Server-side | Implemented | Works! |
| WITH HOLD cursors | Persist after txn | Not implemented | Low priority |

### StarRocks Buffer Management

| Feature | StarRocks | Tavana | Gap? |
|---------|-----------|--------|------|
| Send buffer bounds | 256KB-2MB | 32KB-64KB | Good, smaller for early backpressure |
| Auto-flush | On buffer full | On threshold | Good |
| Blocking I/O | writeBlocking | async flush | Good (relies on tokio) |
| Packet splitting | >16MB | N/A | PostgreSQL max is 1GB |

**Conclusion**: Tavana's backpressure is well-implemented, similar to StarRocks.

### CockroachDB Cursors

| Feature | CockroachDB | Tavana | Gap? |
|---------|-------------|--------|------|
| Cursor affinity | Routes to owner | Stored, not used | **Key gap** |
| Keyset pagination | Alternative | Not implemented | Could add |
| LIMIT/OFFSET fallback | But discouraged | Implemented | Good |

### DuckDB Features

| Feature | DuckDB | Tavana | Gap? |
|---------|--------|--------|------|
| Batch-at-a-time | Iterator API | Implemented | Good |
| prepare_cached | Native | **Not used** | Should use |
| lock_configuration | Native | **Not used** | Should use for security |
| allowed_directories | Native | **Not used** | Should use for isolation |
| PIN_SNAPSHOT | ATTACH caching | Implemented | Good |

---

## Recommended Improvements (Prioritized)

### Priority 1: Fix Cursor Affinity Routing (2 days)

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

### Priority 3: Use DuckDB `prepare_cached` for Schema Caching (1 day)

**Problem**: LIMIT 0 schema detection runs for each Describe.

**Solution**: Use DuckDB's native `prepare_cached`:

```rust
// In executor.rs - leverage native caching
impl DuckDbExecutor {
    pub fn get_schema_cached(&self, sql: &str, user_id: &str) -> Result<Schema> {
        let conn = self.get_user_connection(user_id)?;
        
        // prepare_cached handles caching automatically
        // Same SQL → same prepared statement → cached schema
        let stmt = conn.prepare_cached(&format!(
            "SELECT * FROM ({}) AS _schema LIMIT 0", sql
        ))?;
        
        // Extract schema from cached prepared statement
        let schema = Schema::new(
            (0..stmt.column_count())
                .map(|i| Field::new(stmt.column_name(i)?, stmt.column_type(i)?))
                .collect()
        );
        
        Ok(schema)
    }
}
```

**Benefits**:
- Works for ALL queries (not just Delta)
- Native DuckDB implementation
- Per-connection cache (user isolation)
- No custom cache code needed

### Priority 4: Implement Per-User Security Isolation (3 days)

**Problem**: Connections may not have proper isolation.

**Solution**: Apply DuckDB security settings per user:

```rust
pub fn create_user_connection(user_id: &str, user_config: &UserConfig) -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    
    // 1. Restrict file access to user's allowed paths
    let paths = serde_json::to_string(&user_config.allowed_paths)?;
    conn.execute(&format!("SET allowed_directories = {}", paths), [])?;
    
    // 2. Disable external access by default
    if !user_config.allow_external_access {
        conn.execute("SET enable_external_access = false", [])?;
    }
    
    // 3. Disable community extensions
    conn.execute("SET allow_community_extensions = false", [])?;
    
    // 4. Set resource limits
    conn.execute(&format!("SET threads = {}", user_config.max_threads), [])?;
    conn.execute(&format!("SET memory_limit = '{}'", user_config.memory_limit), [])?;
    
    // 5. Set prepared statement cache size
    conn.set_prepared_statement_cache_capacity(user_config.cache_capacity);
    
    // 6. LOCK configuration to prevent tampering
    conn.execute("SET lock_configuration = true", [])?;
    
    Ok(conn)
}
```

### Priority 5: Result Spooling (Optional, 2 weeks)

**Problem**: Results exceeding memory limits fail.

**Solution**: Spool to Azure Blob Storage (Trino-style).

**When to implement**: Only if users need results >50K rows via Extended Query Protocol.

---

## Configuration Reference

```yaml
# CURRENT - Worker configuration
CURSOR_BATCH_SIZE: 10000              # Rows per cursor batch
MAX_CURSORS: 100                      # Max concurrent cursors
CURSOR_IDLE_TIMEOUT_SECS: 300         # TTL for idle cursors
CURSOR_CLEANUP_INTERVAL_SECS: 60      # Cleanup frequency

# CURRENT - Delta caching
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

# PROPOSED - Per-user security (via user config, not env vars)
# Per-user configuration stored in database or config file:
# - allowed_directories: ["/data/user_123/"]
# - memory_limit: "2GB"
# - max_threads: 4
# - cache_capacity: 100
# - allow_external_access: false

# PROPOSED - Result spooling (Phase 5)
TAVANA_SPOOL_ENABLED: false
TAVANA_SPOOL_THRESHOLD_ROWS: 50000
TAVANA_SPOOL_STORAGE_TYPE: azure_blob
TAVANA_SPOOL_CONTAINER: tavana-spool
TAVANA_SPOOL_TTL_HOURS: 1
```

---

## Implementation Roadmap

### Phase 1: Cursor Affinity (2 days)

| Task | Effort | Files |
|------|--------|-------|
| Create `WorkerClientPool` | 0.5 day | `worker_client.rs` |
| Update `handle_fetch_cursor` to use pool | 0.5 day | `cursors.rs` |
| Update `handle_close_cursor` to use pool | 0.5 day | `cursors.rs` |
| Test with multi-worker setup | 0.5 day | Integration tests |

### Phase 2: Extended Query Streaming (1 week)

| Task | Effort | Files |
|------|--------|-------|
| Add stream handle to `PortalState` | 1 day | `server.rs` |
| Modify Execute handler for streaming | 2 days | `server.rs` |
| Handle Parse clearing stream | 0.5 day | `server.rs` |
| Handle Close clearing stream | 0.5 day | `server.rs` |
| Test with DBeaver scrolling | 1 day | Manual testing |

### Phase 3: Use Native `prepare_cached` (1 day)

| Task | Effort | Files |
|------|--------|-------|
| Replace schema detection with `prepare_cached` | 0.5 day | `executor.rs` |
| Set cache capacity per connection | 0.25 day | `executor.rs` |
| Test schema caching for various queries | 0.25 day | Tests |

### Phase 4: Per-User Security Isolation (3 days)

| Task | Effort | Files |
|------|--------|-------|
| Create `UserConnectionConfig` struct | 0.5 day | `executor.rs` |
| Apply DuckDB security settings per user | 1 day | `executor.rs` |
| Add `lock_configuration` after setup | 0.5 day | `executor.rs` |
| Add user_id to cursor IDs | 0.5 day | `cursor_manager.rs` |
| Test isolation between users | 0.5 day | Tests |

### Phase 5: Result Spooling (Optional, 2 weeks)

| Task | Effort | Files |
|------|--------|-------|
| Create `ResultSpooler` component | 2 days | New file |
| Azure Blob integration | 2 days | Uses existing `object_store` |
| Integrate with Portal handling | 3 days | `server.rs` |
| Cleanup background task | 1 day | Background thread |
| Test with 1M+ row results | 2 days | Load testing |

---

## Expected Benefits

| Metric | Current | After Phase 1-4 | After Phase 5 |
|--------|---------|-----------------|---------------|
| Multi-worker cursor routing | Broken | Works | Works |
| Extended Query first-row latency | 30s (buffered) | <1s (streaming) | <1s |
| Gateway memory (1M rows) | OOM | ~50MB (streaming) | ~50MB |
| Max result size | 50K rows | 50K (cursor unlimited) | Unlimited |
| Schema detection | Per Describe | Cached (all queries) | Cached |
| User isolation | Partial | Full (DuckDB native) | Full |
| Data privacy | Not enforced | Enforced (locked config) | Enforced |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Stream handle lifetime management | Memory leaks | TTL + Close handling |
| Worker failure with active stream | Client error | Graceful retry with new cursor |
| Azure Blob latency for spooling | Slow large results | Only for >50K rows |
| Backward compatibility | Breaking changes | Feature flags for new behavior |
| User escaping isolation | Data breach | `lock_configuration = true` prevents tampering |
| Cache pollution between users | Info leakage | Per-connection cache (DuckDB native) |

---

## Conclusion

Tavana's streaming infrastructure is **more mature than initially assessed**:

**Already excellent**:
- True streaming from DuckDB (batch-at-a-time)
- Multi-layer backpressure (StarRocks-style)
- Server-side cursors with LIMIT/OFFSET fallback
- Delta PIN_SNAPSHOT caching (1.47× speedup)

**Key gaps to fix**:
1. **Cursor affinity routing** (2 days) - Critical for multi-worker
2. **Extended Query streaming** (1 week) - Eliminates buffering
3. **Native prepare_cached** (1 day) - Schema caching for ALL queries
4. **Per-user security isolation** (3 days) - Data privacy with DuckDB native features

**Nice to have**:
5. **Result spooling** (2 weeks) - For very large results

The recommended approach prioritizes **small, high-impact changes** that leverage
DuckDB's native features for both performance (prepare_cached) and security
(lock_configuration, allowed_directories).
