# True Streaming with Server-Side Cursors Architecture

## Executive Summary

This document proposes an architecture for implementing true streaming query results
in Tavana, eliminating the current "buffer all rows then send" approach that causes
memory issues and proportional slowdowns with large result sets.

## Current Architecture Problems

### The Buffering Problem

Currently, when a client like DBeaver executes a query with the PostgreSQL Extended
Query Protocol:

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
| Schema detection | LIMIT 0 reads metadata | Repeated metadata reads |

## Proposed Architecture: True Streaming

### Architecture Overview

```
┌─────────┐     ┌───────────┐          ┌────────────┐     ┌─────────────┐
│ DBeaver │────▶│  Gateway  │─────────▶│   Worker   │────▶│   DuckDB    │
└─────────┘     └───────────┘          └────────────┘     └─────────────┘
     │                │                       │                   │
     │  Parse/Bind/   │                       │                   │
     │  Describe      │   DECLARE CURSOR      │                   │
     │  ────────────▶ │   ───────────────────▶│                   │
     │                │                       │   Create cursor   │
     │                │                       │   ─────────────▶  │
     │                │   cursor_id           │   ◀───────────── │
     │  ◀──────────── │   ◀─────────────────── │                   │
     │                │                       │                   │
     │  Execute       │   FETCH cursor_id     │                   │
     │  max_rows=200  │   LIMIT 200           │                   │
     │  ────────────▶ │   ───────────────────▶│   Fetch next 200 │
     │                │                       │   ─────────────▶  │
     │                │   Stream 200 rows     │   ◀───────────── │
     │  200 rows      │   ◀─────────────────── │                   │
     │  ◀──────────── │   (no buffering!)     │                   │
     │                │                       │                   │
     │  [scroll]      │                       │                   │
     │  Execute       │   FETCH cursor_id     │                   │
     │  ────────────▶ │   LIMIT 200           │                   │
     │                │   ───────────────────▶│   Continue from   │
     │                │                       │   cursor position │
     │  200 rows      │   Stream 200 rows     │   ─────────────▶  │
     │  ◀──────────── │   ◀─────────────────── │   ◀───────────── │
```

### Key Components

#### 1. Cursor Affinity Router

The Gateway needs to route subsequent FETCH requests to the same Worker that
holds the cursor:

```rust
/// Cursor affinity routing ensures FETCH goes to the same worker
pub struct CursorAffinityRouter {
    /// Map: cursor_id -> worker_address
    cursor_locations: DashMap<String, WorkerLocation>,
    /// TTL for cursor location entries
    ttl: Duration,
}

impl CursorAffinityRouter {
    /// Route a cursor operation to the correct worker
    pub fn route_cursor(&self, cursor_id: &str) -> Option<WorkerLocation> {
        self.cursor_locations.get(cursor_id).map(|e| e.value().clone())
    }
    
    /// Register a cursor after DECLARE
    pub fn register_cursor(&self, cursor_id: &str, worker: WorkerLocation) {
        self.cursor_locations.insert(cursor_id.to_string(), worker);
    }
}
```

#### 2. Worker-Side Streaming Cursor

The Worker maintains cursors that can be fetched incrementally:

```rust
/// Server-side cursor with streaming support
pub struct StreamingCursor {
    /// Unique cursor ID
    pub id: String,
    /// DuckDB prepared statement (holds query state)
    statement: PreparedStatement,
    /// Arrow iterator for streaming results
    arrow_iter: Box<dyn Iterator<Item = RecordBatch>>,
    /// Schema (cached from first fetch)
    schema: Arc<Schema>,
    /// Rows fetched so far
    rows_fetched: u64,
    /// Whether cursor is exhausted
    exhausted: bool,
    /// Last access time (for TTL)
    last_access: Instant,
}

impl StreamingCursor {
    /// Fetch next N rows without buffering entire result
    pub fn fetch(&mut self, max_rows: usize) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut rows_collected = 0;
        
        while rows_collected < max_rows {
            match self.arrow_iter.next() {
                Some(batch) => {
                    rows_collected += batch.num_rows();
                    result.push(batch);
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }
        
        self.rows_fetched += rows_collected as u64;
        self.last_access = Instant::now();
        Ok(result)
    }
}
```

#### 3. gRPC Streaming Cursor Service

Add new gRPC methods for streaming cursors:

```protobuf
service QueryService {
    // Existing methods...
    
    // New streaming cursor methods
    rpc DeclareCursorStreaming(DeclareCursorRequest) returns (DeclareCursorResponse);
    rpc FetchCursorStreaming(FetchCursorRequest) returns (stream QueryResultBatch);
    rpc CloseCursor(CloseCursorRequest) returns (CloseCursorResponse);
}

message DeclareCursorRequest {
    string cursor_id = 1;
    string sql = 2;
    UserIdentity user = 3;
    // New: cursor options
    CursorOptions options = 4;
}

message CursorOptions {
    // Whether to use ATTACH PIN_SNAPSHOT for Delta tables
    bool cache_delta_metadata = 1;
    // Preferred batch size for streaming
    uint32 batch_size = 2;
    // Cursor TTL in seconds (0 = default)
    uint32 ttl_seconds = 3;
}
```

#### 4. Gateway Extended Query Protocol Handler

Update the Gateway to use streaming cursors:

```rust
/// Handle Extended Query Protocol with true streaming
async fn handle_execute_streaming(
    &mut self,
    sql: &str,
    max_rows: i32,
) -> Result<()> {
    // Check if we have an active cursor for this query
    if let Some(cursor_id) = self.active_cursor.as_ref() {
        // Resume from cursor (true streaming - no re-execution!)
        return self.fetch_from_cursor(cursor_id, max_rows).await;
    }
    
    // New query - declare cursor on worker
    let cursor_id = self.declare_streaming_cursor(sql).await?;
    self.active_cursor = Some(cursor_id.clone());
    
    // Fetch first batch
    self.fetch_from_cursor(&cursor_id, max_rows).await
}

async fn fetch_from_cursor(&mut self, cursor_id: &str, max_rows: i32) -> Result<()> {
    // Route to the worker holding this cursor
    let worker = self.cursor_router.route_cursor(cursor_id)
        .ok_or_else(|| anyhow!("Cursor not found"))?;
    
    // Create streaming gRPC call
    let mut stream = worker.fetch_cursor_streaming(FetchCursorRequest {
        cursor_id: cursor_id.to_string(),
        max_rows: max_rows as u32,
    }).await?;
    
    // Stream directly to PostgreSQL client (no buffering!)
    while let Some(batch) = stream.message().await? {
        match batch.result {
            Some(Result::RecordBatch(data)) => {
                // Send DataRow messages directly to client
                for row in decode_arrow_batch(&data) {
                    self.send_data_row(&row).await?;
                }
            }
            Some(Result::Profile(profile)) => {
                // More rows available?
                if !profile.exhausted {
                    self.send_portal_suspended().await?;
                } else {
                    self.send_command_complete(profile.rows_returned).await?;
                }
            }
            _ => {}
        }
    }
    
    Ok(())
}
```

### Implementation Phases

#### Phase 1: Worker Cursor Enhancement (1-2 weeks)

1. **Enhance StreamingCursor**
   - Store DuckDB iterator state across fetches
   - Implement proper resource cleanup on timeout
   - Add cursor statistics for monitoring

2. **Update gRPC Service**
   - Add `FetchCursorStreaming` RPC with true streaming
   - Implement cursor TTL and cleanup
   - Add cursor statistics endpoint

3. **Integration with Delta Cache**
   - Use ATTACH PIN_SNAPSHOT for cursor queries
   - Cache schema per cursor (avoid LIMIT 0)

#### Phase 2: Gateway Cursor Router (1 week)

1. **Implement CursorAffinityRouter**
   - Track cursor -> worker mapping
   - Handle worker failures (cursor migration)
   - TTL-based cleanup

2. **Update Extended Query Handler**
   - Detect cursor resume vs new query
   - Route FETCH to correct worker
   - Handle PortalSuspended properly

#### Phase 3: True Streaming Pipeline (2 weeks)

1. **Zero-Copy Streaming**
   - Stream Arrow batches directly from worker to client
   - No intermediate buffering on Gateway
   - Backpressure support

2. **DBeaver Compatibility**
   - Handle DBeaver's Parse/Bind/Execute pattern
   - Proper PortalSuspended handling
   - Close cursor on new Parse

3. **Testing & Optimization**
   - Load testing with large result sets
   - Memory profiling
   - Latency measurements

### Expected Benefits

| Metric | Current | With True Streaming | Improvement |
|--------|---------|---------------------|-------------|
| First row latency (1M rows) | 30+ seconds | < 1 second | 30x+ |
| Gateway memory (1M rows) | ~500MB | ~10MB | 50x |
| Scroll latency | Variable (re-execute) | Constant | Predictable |
| Delta metadata reads | Per scroll | Once per cursor | N× reduction |

### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Cursor state loss on worker restart | Cursor TTL + client retry with new cursor |
| Worker memory exhaustion from many cursors | Cursor limits + LRU eviction |
| Long-running cursors blocking connections | Separate cursor connection pool |
| DBeaver not using cursors properly | Fall back to current buffering behavior |

### Configuration

```yaml
# Worker configuration
streaming:
  # Maximum concurrent cursors per worker
  max_cursors: 1000
  # Default cursor TTL (seconds)
  cursor_ttl: 300
  # Batch size for streaming (rows)
  batch_size: 2048
  # Enable Delta PIN_SNAPSHOT for cursors
  delta_cache_enabled: true

# Gateway configuration
cursor_routing:
  # TTL for cursor location cache
  location_ttl: 600
  # Retry on cursor not found
  retry_on_miss: true
```

### Monitoring

New metrics to add:

```rust
// Worker metrics
cursor_active_count: Gauge,           // Current active cursors
cursor_rows_streamed: Counter,        // Total rows streamed via cursors
cursor_fetch_latency: Histogram,      // Latency per fetch
cursor_ttl_evictions: Counter,        // Cursors evicted due to TTL

// Gateway metrics
cursor_route_hits: Counter,           // Successful cursor routing
cursor_route_misses: Counter,         // Cursor not found (re-execution needed)
streaming_bytes_transferred: Counter, // Bytes streamed without buffering
```

## Conclusion

True streaming with server-side cursors will eliminate the memory and latency issues
caused by result buffering. Combined with the Delta PIN_SNAPSHOT caching (already
implemented), this will provide significant performance improvements for DBeaver and
other JDBC clients querying large Delta tables.

The implementation can be done incrementally, with each phase providing immediate
benefits while building toward the complete streaming solution.
