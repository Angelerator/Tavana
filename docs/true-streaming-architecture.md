# True Streaming with Server-Side Cursors Architecture

## Executive Summary

This document proposes a comprehensive architecture for implementing true streaming query
results in Tavana, based on best practices from PostgreSQL, Trino, Snowflake, CockroachDB,
StarRocks, and DuckDB. The goal is to eliminate the current "buffer all rows then send"
approach that causes memory issues and proportional slowdowns with large result sets.

## Industry Best Practices Survey

### PostgreSQL Extended Query Protocol

PostgreSQL's wire protocol supports true streaming through:
- **Portals**: Created during Bind phase, represent a prepared statement ready for execution
- **Execute(max_rows)**: Controls how many rows are fetched per call
- **Server-side cursors**: Via `DECLARE CURSOR` for incremental fetching
- **WITH HOLD cursors**: Persist beyond transaction boundaries

**Key insight**: Each cursor requires holding open a dedicated read-only transaction,
operating on a database snapshot taken at cursor creation.

### Trino/Presto Spooling Protocol

Trino implements a sophisticated **spooling protocol** for large result sets:

1. **Object storage offload**: Results stored in S3/object storage (separate from coordinator)
2. **Parallel writing**: Coordinator and workers write data to storage in parallel
3. **URL-based retrieval**: Coordinator provides URLs to individual data segments
4. **Automatic cleanup**: Data removed from storage after client download
5. **Compression support**: Reduces network overhead

**Benefits**: Higher throughput for large queries, reduced coordinator CPU/I/O load.

**Fallback**: Automatically uses direct protocol for smaller queries or older clients.

### Snowflake Streaming Architecture

Snowflake implements multiple strategies:

1. **Streaming ResultSet**: `streamResult=true` returns Node.js Readable stream
2. **Backpressure**: Driver versions 1.6.23+ implement backpressure to prevent overwhelming clients
3. **Batch streaming**: `streamRows(start, end)` for consuming specific row ranges
4. **Distributed fetch**: `get_result_batches()` returns `ResultBatch` objects for parallel processing
5. **Arrow batches**: `fetch_arrow_batches()` iterator for memory-efficient processing

### CockroachDB Distributed Cursors

CockroachDB offers three pagination approaches:

1. **LIMIT/OFFSET**: Not recommended (slow for large offsets)
2. **Keyset pagination**: Fast, uses indexed columns for efficient paging
3. **Cursors**: Stateful iteration, requires dedicated read-only transaction

**Cursor affinity**: In distributed databases, directing cursor operations to specific
nodes supports consistent state management across cluster.

### StarRocks Buffer Management

StarRocks implements efficient buffer management:

1. **Send buffer bounds**: 256KB minimum, 2MB maximum per connection
2. **Auto-flush**: Automatically flushes when buffer is full
3. **Packet splitting**: Handles packets >16MB by splitting
4. **Blocking I/O**: Uses `Channels.writeBlocking()` for backpressure
5. **Fixed read buffers**: 16KB default to limit memory per connection

### DuckDB Arrow Streaming

DuckDB + Arrow enables zero-copy streaming:

1. **Batch-at-a-time processing**: Never loads entire dataset into memory
2. **Larger-than-memory analysis**: Processes data in streaming batches
3. **Pushdown optimization**: Filters and projections pushed into Arrow scans
4. **Columnar efficiency**: Native support for nested structures (lists, structs, maps)

### JDBC Streaming Best Practices

To prevent OOM with large ResultSets:

```java
// Enable server-side cursor streaming
stmt = conn.createStatement(
    ResultSet.TYPE_FORWARD_ONLY, 
    ResultSet.CONCUR_READ_ONLY
);

// Option 1: Row-by-row streaming (internally buffered)
stmt.setFetchSize(Integer.MIN_VALUE);

// Option 2: Balanced batch streaming (recommended)
stmt.setFetchSize(1000);  // Adjust based on row size
```

**Key insight**: `fetchSize` of 1000-10000 provides good balance between
round trips and memory consumption.

---

## Current Architecture Problems

### The Buffering Problem

```
┌─────────┐     ┌───────────┐     ┌────────────┐     ┌─────────────┐
│ DBeaver │────▶│  Gateway  │────▶│   Worker   │────▶│   DuckDB    │
└─────────┘     └───────────┘     └────────────┘     └─────────────┘
     │                │                  │                   │
     │  Parse/Bind/   │                  │                   │
     │  Describe      │   LIMIT 0 for    │                   │
     │  ────────────▶ │   schema detect  │                   │
     │                │   ─────────────▶ │                   │
     │                │   ◀───────────── │                   │
     │                │                  │                   │
     │  Execute       │                  │                   │
     │  max_rows=200  │   Execute FULL   │                   │
     │  ────────────▶ │   query (!)      │                   │
     │                │   ─────────────▶ │  Scan entire      │
     │                │                  │  Delta table      │
     │                │                  │  ◀─────────────── │
     │                │   BUFFER ALL     │                   │
     │                │   rows in memory │                   │
     │                │   ◀───────────── │                   │
     │  200 rows      │                  │                   │
     │  ◀──────────── │                  │                   │
     │                │                  │                   │
     │  [scroll]      │                  │                   │
     │  Execute       │   Resume from    │                   │
     │  ────────────▶ │   buffer (or     │                   │
     │                │   RE-EXECUTE!)   │                   │
```

**Key Issues:**
1. Gateway buffers entire result set before sending first row
2. High memory usage on Gateway (can OOM with large results)
3. DBeaver may re-execute entire query on scroll
4. LIMIT 0 schema detection still reads Delta metadata
5. No true cursor-based pagination

### Performance Impact

| Scenario | Current Behavior | Impact |
|----------|-----------------|--------|
| SELECT * FROM large_delta_table LIMIT 5000 | Buffer 5000 rows | High latency to first row, memory spike |
| DBeaver scroll to fetch next 200 rows | May re-execute query | Proportional slowdown |
| Schema detection | LIMIT 0 reads metadata | Repeated Azure Blob reads |

---

## Proposed Architecture: Multi-Tier Streaming

Based on industry best practices, we propose a **multi-tier streaming architecture**
that combines the best approaches from major players.

### Architecture Overview

```
┌─────────┐     ┌───────────────────────────────────────────┐     ┌────────────┐
│         │     │              GATEWAY                      │     │            │
│         │     │  ┌─────────────────────────────────────┐  │     │            │
│ DBeaver │────▶│  │  Cursor Affinity Router            │  │────▶│   Worker   │
│         │     │  │  - cursor_id → worker mapping      │  │     │            │
│         │     │  │  - TTL-based cleanup               │  │     │            │
│         │     │  └─────────────────────────────────────┘  │     │            │
│         │     │                                           │     │            │
│         │◀────│  ┌─────────────────────────────────────┐  │◀────│            │
│         │     │  │  Backpressure Manager              │  │     │            │
│         │     │  │  - Send buffer: 256KB-2MB          │  │     │            │
│         │     │  │  - Auto-flush on full              │  │     │            │
│         │     │  │  - Slow client detection           │  │     │            │
│         │     │  └─────────────────────────────────────┘  │     │            │
└─────────┘     └───────────────────────────────────────────┘     └────────────┘
                                                                         │
                                                                         ▼
                                                                  ┌─────────────┐
                                                                  │   DuckDB    │
                                                                  │             │
                                                                  │ PIN_SNAPSHOT│
                                                                  │ Delta Cache │
                                                                  └─────────────┘
```

### Tier 1: Worker-Side Streaming (DuckDB)

Leverage DuckDB's native Arrow streaming for zero-copy result generation.

```rust
/// Server-side cursor with DuckDB Arrow streaming
pub struct StreamingCursor {
    /// Unique cursor ID
    pub id: String,
    /// DuckDB connection (dedicated for this cursor)
    connection: Arc<PooledConnection>,
    /// Arrow batch iterator (lazy - doesn't buffer)
    arrow_iter: Option<Box<dyn Iterator<Item = RecordBatch> + Send>>,
    /// Schema (cached from first batch)
    schema: Arc<Schema>,
    /// Configuration
    config: CursorConfig,
    /// Statistics
    stats: CursorStats,
}

#[derive(Clone)]
pub struct CursorConfig {
    /// Batch size for Arrow streaming (rows per batch)
    /// Based on JDBC best practices: 1000-10000 is optimal
    pub batch_size: usize,
    /// TTL before cursor is evicted (PostgreSQL default: session lifetime)
    pub ttl: Duration,
    /// Memory limit per cursor (Impala-style spooling threshold)
    pub max_memory_bytes: usize,
    /// Enable Delta PIN_SNAPSHOT caching
    pub delta_cache_enabled: bool,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            batch_size: 2048,           // ~2K rows per batch
            ttl: Duration::from_secs(300), // 5 minute TTL
            max_memory_bytes: 100 * 1024 * 1024, // 100MB per cursor
            delta_cache_enabled: true,
        }
    }
}

pub struct CursorStats {
    /// Total rows fetched so far
    pub rows_fetched: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Last access time (for TTL)
    pub last_access: Instant,
    /// Whether cursor is exhausted
    pub exhausted: bool,
    /// Peak memory usage
    pub peak_memory_bytes: u64,
}

impl StreamingCursor {
    /// Create cursor with ATTACH PIN_SNAPSHOT optimization
    pub async fn declare(
        id: String,
        sql: &str,
        executor: &DuckDbExecutor,
        config: CursorConfig,
    ) -> Result<Self> {
        // Use PIN_SNAPSHOT for Delta tables (1.47× speedup)
        if config.delta_cache_enabled {
            executor.auto_attach_delta_tables(sql);
        }
        
        let connection = executor.get_connection();
        let conn = connection.connection.lock()?;
        
        // Prepare statement without executing
        let stmt = conn.prepare(sql)?;
        let arrow_iter = stmt.query_arrow(params![])?;
        let schema = arrow_iter.get_schema();
        
        Ok(Self {
            id,
            connection,
            arrow_iter: Some(Box::new(arrow_iter)),
            schema,
            config,
            stats: CursorStats {
                rows_fetched: 0,
                bytes_sent: 0,
                last_access: Instant::now(),
                exhausted: false,
                peak_memory_bytes: 0,
            },
        })
    }
    
    /// Fetch next batch - TRUE STREAMING (no full buffering)
    pub fn fetch(&mut self, max_rows: usize) -> Result<FetchResult> {
        self.stats.last_access = Instant::now();
        
        let iter = self.arrow_iter.as_mut()
            .ok_or_else(|| anyhow!("Cursor already closed"))?;
        
        let mut batches = Vec::new();
        let mut rows_collected = 0;
        
        // Collect batches until we have enough rows
        while rows_collected < max_rows {
            match iter.next() {
                Some(batch) => {
                    rows_collected += batch.num_rows();
                    self.stats.rows_fetched += batch.num_rows() as u64;
                    batches.push(batch);
                }
                None => {
                    self.stats.exhausted = true;
                    break;
                }
            }
        }
        
        Ok(FetchResult {
            batches,
            exhausted: self.stats.exhausted,
            rows_in_batch: rows_collected,
            total_rows_fetched: self.stats.rows_fetched,
        })
    }
}
```

### Tier 2: Gateway Streaming Pipeline

Based on StarRocks buffer management and Snowflake backpressure patterns.

```rust
/// Gateway streaming pipeline with backpressure
pub struct StreamingPipeline {
    /// Cursor affinity router (CockroachDB-style)
    cursor_router: Arc<CursorAffinityRouter>,
    /// Send buffer configuration (StarRocks-style)
    buffer_config: BufferConfig,
    /// Active streaming sessions
    active_sessions: DashMap<String, StreamingSession>,
}

#[derive(Clone)]
pub struct BufferConfig {
    /// Minimum send buffer size (StarRocks: 256KB)
    pub min_buffer_bytes: usize,
    /// Maximum send buffer size (StarRocks: 2MB)
    pub max_buffer_bytes: usize,
    /// Flush threshold (bytes before auto-flush)
    pub flush_threshold_bytes: usize,
    /// Flush threshold (rows before auto-flush)
    pub flush_threshold_rows: usize,
    /// Slow client detection timeout
    pub slow_client_timeout: Duration,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            min_buffer_bytes: 256 * 1024,      // 256KB (StarRocks min)
            max_buffer_bytes: 2 * 1024 * 1024, // 2MB (StarRocks max)
            flush_threshold_bytes: 64 * 1024,  // 64KB
            flush_threshold_rows: 100,
            slow_client_timeout: Duration::from_secs(30),
        }
    }
}

/// Cursor affinity router for distributed cursor management
pub struct CursorAffinityRouter {
    /// Map: cursor_id -> worker location
    cursor_locations: DashMap<String, CursorLocation>,
    /// Cursor metadata cache
    cursor_metadata: DashMap<String, CursorMetadata>,
    /// Configuration
    config: RouterConfig,
}

#[derive(Clone)]
pub struct CursorLocation {
    /// Worker address (gRPC endpoint)
    pub worker_addr: String,
    /// Worker ID for logging
    pub worker_id: String,
    /// Creation time
    pub created_at: Instant,
    /// Last access time
    pub last_access: Instant,
}

#[derive(Clone)]
pub struct CursorMetadata {
    /// Original SQL query (for re-execution on worker failure)
    pub sql: String,
    /// Schema (cached to avoid LIMIT 0 queries)
    pub schema: Arc<Schema>,
    /// User identity
    pub user_id: String,
    /// Total rows fetched so far
    pub rows_fetched: u64,
}

impl CursorAffinityRouter {
    /// Route cursor operation to correct worker
    pub fn route(&self, cursor_id: &str) -> Option<CursorLocation> {
        self.cursor_locations.get(cursor_id).map(|e| {
            // Update last access time
            let mut loc = e.value().clone();
            loc.last_access = Instant::now();
            loc
        })
    }
    
    /// Register cursor after DECLARE
    pub fn register(
        &self,
        cursor_id: &str,
        worker_addr: String,
        worker_id: String,
        metadata: CursorMetadata,
    ) {
        let location = CursorLocation {
            worker_addr,
            worker_id,
            created_at: Instant::now(),
            last_access: Instant::now(),
        };
        
        self.cursor_locations.insert(cursor_id.to_string(), location);
        self.cursor_metadata.insert(cursor_id.to_string(), metadata);
    }
    
    /// Handle worker failure - cursor migration
    pub async fn handle_worker_failure(
        &self,
        failed_worker: &str,
        available_workers: &[String],
    ) -> Vec<String> {
        let mut migrated = Vec::new();
        
        // Find cursors on failed worker
        let affected: Vec<_> = self.cursor_locations.iter()
            .filter(|e| e.value().worker_addr == failed_worker)
            .map(|e| e.key().clone())
            .collect();
        
        for cursor_id in affected {
            // Get metadata for re-creation
            if let Some(metadata) = self.cursor_metadata.get(&cursor_id) {
                // Remove old location - client will get "cursor not found"
                // and can retry with new cursor (PostgreSQL-compatible behavior)
                self.cursor_locations.remove(&cursor_id);
                migrated.push(cursor_id);
            }
        }
        
        migrated
    }
    
    /// Cleanup expired cursors (TTL-based)
    pub fn cleanup_expired(&self, ttl: Duration) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        
        self.cursor_locations.retain(|_, v| {
            let expired = now.duration_since(v.last_access) > ttl;
            if expired { removed += 1; }
            !expired
        });
        
        // Also cleanup metadata
        self.cursor_metadata.retain(|k, _| {
            self.cursor_locations.contains_key(k)
        });
        
        removed
    }
}
```

### Tier 3: Protocol Handler Integration

```rust
/// Extended Query Protocol handler with true streaming
impl ExtendedQueryHandler {
    /// Handle Execute message with streaming support
    async fn handle_execute_streaming(
        &mut self,
        portal_name: &str,
        max_rows: i32,
    ) -> Result<()> {
        // Check for active cursor (resume case)
        if let Some(cursor_id) = self.get_active_cursor(portal_name) {
            // Route to worker holding this cursor
            if let Some(location) = self.cursor_router.route(&cursor_id) {
                return self.fetch_from_cursor_streaming(
                    &cursor_id,
                    &location,
                    max_rows,
                ).await;
            } else {
                // Cursor not found - could be expired or worker failed
                // Fall back to re-execution (PostgreSQL-compatible)
                debug!("Cursor {} not found, re-executing query", cursor_id);
            }
        }
        
        // New query or cursor expired - declare new cursor
        let sql = self.get_prepared_query(portal_name)?;
        let cursor_id = self.declare_streaming_cursor(&sql).await?;
        self.set_active_cursor(portal_name, cursor_id.clone());
        
        // Fetch first batch
        let location = self.cursor_router.route(&cursor_id)
            .ok_or_else(|| anyhow!("Cursor just created but not found"))?;
        
        self.fetch_from_cursor_streaming(&cursor_id, &location, max_rows).await
    }
    
    /// Stream results from worker cursor to PostgreSQL client
    async fn fetch_from_cursor_streaming(
        &mut self,
        cursor_id: &str,
        location: &CursorLocation,
        max_rows: i32,
    ) -> Result<()> {
        // Create gRPC streaming connection to worker
        let mut client = self.get_worker_client(&location.worker_addr).await?;
        
        let request = proto::FetchCursorRequest {
            cursor_id: cursor_id.to_string(),
            max_rows: max_rows as u32,
        };
        
        let mut stream = client.fetch_cursor_streaming(request).await?.into_inner();
        
        // Stream directly to PostgreSQL client (zero-copy goal)
        let mut rows_sent = 0;
        let mut bytes_since_flush = 0;
        
        while let Some(batch_result) = stream.message().await? {
            match batch_result.result {
                Some(proto::query_result_batch::Result::RecordBatch(data)) => {
                    // Decode Arrow batch
                    let rows = self.decode_arrow_batch(&data.data)?;
                    
                    for row in rows {
                        // Build and send DataRow
                        let data_row = self.build_data_row(&row);
                        bytes_since_flush += data_row.len();
                        rows_sent += 1;
                        
                        self.socket.write_all(&data_row).await?;
                        
                        // Backpressure check (StarRocks-style)
                        if bytes_since_flush >= self.config.flush_threshold_bytes {
                            let flush_start = Instant::now();
                            self.socket.flush().await?;
                            
                            let flush_time = flush_start.elapsed();
                            if flush_time > self.config.slow_client_timeout {
                                warn!(
                                    "Slow client detected: flush took {:?}",
                                    flush_time
                                );
                            }
                            bytes_since_flush = 0;
                        }
                    }
                }
                
                Some(proto::query_result_batch::Result::Profile(profile)) => {
                    // Final flush
                    self.socket.flush().await?;
                    
                    if profile.exhausted {
                        // All rows sent - CommandComplete
                        let cmd_tag = format!("SELECT {}", rows_sent);
                        self.send_command_complete(&cmd_tag).await?;
                    } else {
                        // More rows available - PortalSuspended
                        self.send_portal_suspended().await?;
                    }
                }
                
                Some(proto::query_result_batch::Result::Error(err)) => {
                    self.send_error(&err.message).await?;
                    return Err(anyhow!("{}: {}", err.code, err.message));
                }
                
                _ => {}
            }
        }
        
        Ok(())
    }
}
```

---

## Alternative Architecture: Result Spooling (Trino-Style)

For extremely large result sets that exceed memory limits, implement
Trino-style result spooling to object storage.

```rust
/// Result spooling for very large result sets
pub struct ResultSpooler {
    /// Object storage client (Azure Blob, S3)
    storage: Arc<dyn ObjectStore>,
    /// Spool configuration
    config: SpoolConfig,
    /// Active spool sessions
    sessions: DashMap<String, SpoolSession>,
}

#[derive(Clone)]
pub struct SpoolConfig {
    /// Threshold to start spooling (Impala default: 100MB)
    pub memory_threshold_bytes: usize,
    /// Maximum disk/storage usage (Impala default: 1GB)
    pub max_spool_bytes: usize,
    /// Storage path prefix
    pub storage_prefix: String,
    /// TTL for spooled data
    pub data_ttl: Duration,
    /// Enable compression
    pub compress: bool,
}

impl ResultSpooler {
    /// Spool large result to object storage
    pub async fn spool(
        &self,
        cursor_id: &str,
        batches: impl Iterator<Item = RecordBatch>,
    ) -> Result<SpoolSession> {
        let session_id = format!("{}/{}", cursor_id, Uuid::new_v4());
        let mut segments = Vec::new();
        let mut segment_id = 0;
        let mut current_size = 0;
        
        for batch in batches {
            // Serialize batch to Parquet (compressed)
            let data = self.serialize_batch(&batch)?;
            
            // Upload segment to object storage
            let path = format!(
                "{}/{}/segment_{:06}.parquet",
                self.config.storage_prefix,
                session_id,
                segment_id
            );
            
            self.storage.put(&path.into(), data.clone()).await?;
            
            segments.push(SpoolSegment {
                path,
                row_count: batch.num_rows(),
                byte_size: data.len(),
            });
            
            current_size += data.len();
            segment_id += 1;
            
            // Check size limit
            if current_size > self.config.max_spool_bytes {
                return Err(anyhow!(
                    "Result exceeds maximum spool size: {} > {}",
                    current_size, self.config.max_spool_bytes
                ));
            }
        }
        
        let session = SpoolSession {
            id: session_id.clone(),
            segments,
            created_at: Instant::now(),
            current_segment: 0,
        };
        
        self.sessions.insert(session_id, session.clone());
        Ok(session)
    }
    
    /// Fetch segment from spool
    pub async fn fetch_segment(
        &self,
        session_id: &str,
        segment_index: usize,
    ) -> Result<Vec<RecordBatch>> {
        let session = self.sessions.get(session_id)
            .ok_or_else(|| anyhow!("Spool session not found"))?;
        
        let segment = session.segments.get(segment_index)
            .ok_or_else(|| anyhow!("Segment {} not found", segment_index))?;
        
        // Download from object storage
        let data = self.storage.get(&segment.path.clone().into()).await?.bytes().await?;
        
        // Deserialize Parquet
        self.deserialize_batches(&data)
    }
    
    /// Cleanup spooled data after client downloads
    pub async fn cleanup(&self, session_id: &str) -> Result<()> {
        if let Some((_, session)) = self.sessions.remove(session_id) {
            for segment in session.segments {
                let _ = self.storage.delete(&segment.path.into()).await;
            }
        }
        Ok(())
    }
}
```

---

## Implementation Phases

### Phase 1: Enhanced Worker Cursors (2 weeks)

**Objective**: True streaming from DuckDB to Gateway

1. **StreamingCursor implementation**
   - DuckDB Arrow iterator state preservation
   - Batch-at-a-time fetching (no full buffering)
   - Memory tracking per cursor
   - TTL-based eviction

2. **Delta PIN_SNAPSHOT integration**
   - Automatic ATTACH for delta_scan queries (DONE ✓)
   - Schema caching per cursor
   - Avoid LIMIT 0 queries for cached tables

3. **gRPC streaming service**
   - `FetchCursorStreaming` RPC with true gRPC streaming
   - Cursor statistics endpoint
   - Health checks for cursor state

**Deliverables**:
- Worker can hold 1000+ concurrent cursors
- First row latency < 100ms for cached Delta tables
- Memory usage bounded per cursor

### Phase 2: Gateway Cursor Router (1 week)

**Objective**: Reliable cursor affinity in distributed setup

1. **CursorAffinityRouter**
   - cursor_id → worker mapping
   - Last-access tracking for TTL
   - Metadata caching (schema, original SQL)

2. **Worker failure handling**
   - Detect worker unavailability
   - Return "cursor not found" to client
   - Client retries with new cursor (PostgreSQL-compatible)

3. **Cleanup background task**
   - TTL-based cursor eviction
   - Metrics for cursor lifecycle

**Deliverables**:
- Cursors correctly routed to owning worker
- Clean recovery from worker failures
- Metrics for cursor hit/miss rate

### Phase 3: Streaming Pipeline (2 weeks)

**Objective**: Zero-copy streaming to PostgreSQL clients

1. **Backpressure-aware streaming**
   - StarRocks-style buffer management (256KB-2MB)
   - Auto-flush on buffer full
   - Slow client detection

2. **Extended Query Protocol integration**
   - Transparent cursor management for JDBC clients
   - Proper PortalSuspended handling
   - Close cursor on new Parse

3. **Testing & optimization**
   - Load testing with 1M+ row result sets
   - Memory profiling under concurrent load
   - Latency measurements end-to-end

**Deliverables**:
- No Gateway OOM for any result size
- Constant memory usage regardless of result size
- Sub-second first-row latency

### Phase 4: Result Spooling (Optional, 2 weeks)

**Objective**: Handle result sets exceeding memory limits

1. **Object storage integration**
   - Azure Blob Storage / S3 support
   - Parquet serialization for segments
   - Compression (Zstd)

2. **Hybrid mode**
   - Memory-only for small results (< threshold)
   - Spool to storage for large results
   - Automatic cleanup after download

**Deliverables**:
- Results up to 10GB+ supported
- No coordinator/gateway memory exhaustion
- Configurable spooling thresholds

---

## Configuration

```yaml
# Worker configuration
streaming:
  # Cursor settings
  cursor:
    max_cursors: 1000              # Max concurrent cursors per worker
    default_ttl_seconds: 300       # 5 minute default TTL
    max_ttl_seconds: 3600          # 1 hour max TTL
    batch_size: 2048               # Rows per Arrow batch
    max_memory_per_cursor_mb: 100  # Memory limit per cursor
  
  # Delta caching (already implemented)
  delta_cache:
    enabled: true
    max_tables: 50
    ttl_seconds: 3600
    pin_snapshot: true

# Gateway configuration
cursor_routing:
  # Affinity router
  location_cache_ttl_seconds: 600  # 10 minute location cache
  cleanup_interval_seconds: 60     # Cleanup check interval
  retry_on_miss: true              # Re-execute on cursor not found
  
  # Backpressure (StarRocks-style)
  buffer:
    min_bytes: 262144              # 256KB
    max_bytes: 2097152             # 2MB
    flush_threshold_bytes: 65536   # 64KB
    flush_threshold_rows: 100
    slow_client_timeout_seconds: 30

# Result spooling (optional, Phase 4)
spooling:
  enabled: false                   # Enable for very large results
  memory_threshold_mb: 100         # Start spooling after this
  max_spool_gb: 1                  # Max storage per result
  storage_type: azure_blob         # azure_blob | s3
  storage_container: tavana-spool
  compress: true
  data_ttl_hours: 1
```

---

## Monitoring

### Worker Metrics

```rust
// Cursor metrics
cursor_active_count: Gauge,           // Current active cursors
cursor_declared_total: Counter,       // Total cursors declared
cursor_fetch_total: Counter,          // Total fetch operations
cursor_rows_streamed_total: Counter,  // Total rows streamed
cursor_fetch_latency: Histogram,      // Fetch latency distribution
cursor_ttl_evictions_total: Counter,  // Evicted due to TTL
cursor_memory_bytes: Gauge,           // Memory used by cursors

// Delta cache metrics (already implemented)
delta_cache_hits_total: Counter,
delta_cache_misses_total: Counter,
delta_tables_attached: Gauge,
```

### Gateway Metrics

```rust
// Routing metrics
cursor_route_hits_total: Counter,     // Successful routing
cursor_route_misses_total: Counter,   // Cursor not found
cursor_worker_failures_total: Counter, // Worker unavailability

// Streaming metrics
streaming_bytes_total: Counter,       // Total bytes streamed
streaming_rows_total: Counter,        // Total rows streamed
streaming_flush_total: Counter,       // Number of flushes
streaming_slow_clients_total: Counter, // Slow client detections
streaming_backpressure_events: Counter, // Backpressure applied

// Spooling metrics (Phase 4)
spool_sessions_total: Counter,
spool_bytes_written_total: Counter,
spool_segments_created_total: Counter,
```

### Alerting Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| cursor_active_count | > 800 (80%) | > 950 (95%) |
| cursor_memory_bytes | > 8GB | > 12GB |
| cursor_fetch_latency_p99 | > 500ms | > 2s |
| streaming_slow_clients_total rate | > 1/min | > 10/min |

---

## Expected Benefits

| Metric | Current | Phase 1-3 | Phase 4 |
|--------|---------|-----------|---------|
| First row latency (1M rows) | 30+ seconds | < 1 second | < 1 second |
| Gateway memory (1M rows) | ~500MB | ~10MB | ~10MB |
| Max result size | ~1M rows (OOM) | ~10M rows | Unlimited |
| Scroll latency | Variable | Constant | Constant |
| Delta metadata reads | Per scroll | Once | Once |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cursor state loss on worker restart | High | TTL + client retry with new cursor (PostgreSQL-compatible) |
| Worker memory exhaustion | High | Per-cursor memory limits + LRU eviction |
| Cursor affinity routing miss | Medium | Graceful degradation to re-execution |
| Long-running cursors blocking | Medium | Separate cursor connection pool |
| Object storage latency (Phase 4) | Low | Only for very large results |
| DBeaver not using cursors | Low | Fall back to current buffering |

---

## Conclusion

This architecture combines proven patterns from major database systems:

- **PostgreSQL**: Extended Query Protocol with portals and cursors
- **Trino**: Result spooling to object storage for large results
- **Snowflake**: Backpressure and distributed batch fetching
- **CockroachDB**: Cursor affinity in distributed systems
- **StarRocks**: Buffer management and auto-flush patterns
- **DuckDB**: Zero-copy Arrow streaming

The phased implementation approach allows incremental delivery of benefits:
1. Phase 1-3 (5 weeks): True streaming eliminates OOM and reduces latency
2. Phase 4 (optional, 2 weeks): Spooling enables unlimited result sizes

Combined with the Delta PIN_SNAPSHOT caching (already implemented), this
architecture will provide enterprise-grade query streaming performance for
DBeaver, Tableau, and other JDBC/ODBC clients.
