# DBeaver Configuration for Tavana

This guide explains how to configure DBeaver to connect to Tavana and stream large result sets without memory issues.

## Quick Start

### 1. Create a New PostgreSQL Connection

1. Open DBeaver
2. Database → New Database Connection → PostgreSQL
3. Enter connection details:
   - **Host**: `tavana.example.com`
   - **Port**: `5432`
   - **Database**: `main`
   - **Username**: Your email (e.g., `user@example.com`)
   - **Password**: Your password

### 2. Configure Streaming (CRITICAL - Prevents Crashes!)

#### Step A: Disable Auto-Commit

1. Right-click your connection → **Edit Connection**
2. Go to **Connection settings** → **Initialization** tab
3. **Uncheck "Auto-commit"**

Or use the toolbar: Click the **Auto-commit toggle button** to switch to "Manual commit" mode.

#### Step B: Set Fetch Size

1. Go to **Window → Preferences → Editors → Data Editor**
2. Set **"Result set fetch size"** to `5000`

#### Step C: Driver Properties (Optional but Recommended)

Go to **Driver properties** tab and set:

| Property | Value | Why |
|----------|-------|-----|
| `sslmode` | `require` | Enable SSL |
| `defaultRowFetchSize` | `5000` | Stream results in batches |

**⚠️ DO NOT SET `preferQueryMode=simple`** - This disables streaming!

### 3. Verify Streaming Works

Run a test query:
```sql
SELECT * FROM delta_scan('az://your-storage-container/dev/bronze/...') LIMIT 10000
```

You should see results appearing progressively without memory spikes.

## Understanding Why This Matters

### The Problem

By default, DBeaver loads **ALL rows into memory** before displaying. For large tables:
- 1M rows × 1KB/row = **1 GB of memory** → DBeaver crashes

### The Solution

With proper configuration:
- DBeaver fetches 5000 rows at a time
- Memory usage stays at **~8 MB** regardless of table size
- Results appear progressively as they stream

| Configuration | Memory for 1M rows | Behavior |
|--------------|-------------------|----------|
| Default | **~1 GB** | All rows buffered → crash |
| Streaming enabled | **~8 MB** | 5000 rows at a time |

## Bypassing Server-Side Limits

Tavana may enforce a result row limit (e.g., 100,000 rows) to protect clients from OOM.

### To Get Unlimited Results

Add the `TAVANA:UNLIMITED` hint to your query:

```sql
-- TAVANA:UNLIMITED
SELECT * FROM delta_scan('az://your-storage-container/dev/bronze/...');
```

Or as a block comment:
```sql
/*TAVANA:UNLIMITED*/ SELECT * FROM delta_scan('az://...');
```

**⚠️ Warning**: Only use this if you've configured streaming properly (Steps A & B above), otherwise DBeaver will crash!

## Troubleshooting

### Out of Memory / DBeaver Crashes

**Cause**: Auto-commit is enabled or fetch size is 0.

**Fix**: 
1. Disable auto-commit (Step A above)
2. Set fetch size to 5000 (Step B above)
3. Restart DBeaver

### Query Takes Forever to Start

**Cause**: Large result set without LIMIT.

**Fix**: Use LIMIT for exploratory queries:
```sql
SELECT * FROM delta_scan('az://...') LIMIT 1000
```

### "Transaction aborted" Errors

**Cause**: A previous query failed and the transaction is in error state.

**Fix**: Click "Rollback" in the toolbar, or run:
```sql
ROLLBACK
```

## Large Result Sets Best Practices

### Option 1: Pagination
```sql
-- First 10,000 rows
SELECT * FROM delta_scan('az://...') LIMIT 10000 OFFSET 0;

-- Next 10,000 rows
SELECT * FROM delta_scan('az://...') LIMIT 10000 OFFSET 10000;
```

### Option 2: Export to File
Right-click results → **Export Data** → CSV/Excel. This streams directly to file without loading into UI.

### Option 3: Server-Side Cursors
```sql
BEGIN;
DECLARE my_cursor CURSOR FOR SELECT * FROM delta_scan('az://...');
FETCH 1000 FROM my_cursor;
-- process results...
FETCH 1000 FROM my_cursor;
CLOSE my_cursor;
COMMIT;
```

## Reference

- [PostgreSQL JDBC Fetch Size](https://jdbc.postgresql.org/documentation/query/)
- [DBeaver Auto/Manual Commit](https://dbeaver.com/docs/dbeaver/Auto-and-Manual-Commit-Modes/)
