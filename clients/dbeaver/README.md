# DBeaver Configuration for Tavana

This guide explains how to configure DBeaver to connect to Tavana and stream large result sets without memory issues.

## Quick Start

### 1. Create a New PostgreSQL Connection

1. Open DBeaver
2. Database → New Database Connection → PostgreSQL
3. Enter connection details:
   - **Host**: `tavana-dev.int.nokia.com`
   - **Port**: `5432`
   - **Database**: `main`
   - **Username**: Your email (e.g., `user@nokia.com`)
   - **Password**: Your password

### 2. Configure Driver Properties (CRITICAL!)

Go to **Driver properties** tab and set these properties:

| Property | Value | Why |
|----------|-------|-----|
| `sslmode` | `prefer` | Enable SSL with fallback |
| `defaultRowFetchSize` | `5000` | **Stream results in batches** |
| `defaultAutoCommit` | `false` | **Enable server-side cursors** |

**⚠️ DO NOT SET `preferQueryMode=simple`** - This disables streaming and causes memory issues!

### 3. Verify Streaming Works

Run a test query:
```sql
SELECT * FROM delta_scan('az://dagster-data-pipelines/dev/bronze/...') LIMIT 10000
```

You should see results appearing progressively without memory spikes.

## Understanding the Configuration

### Why These Settings Matter

| Setting | Effect |
|---------|--------|
| `defaultRowFetchSize=5000` | JDBC driver fetches 5000 rows at a time instead of all rows |
| `defaultAutoCommit=false` | Enables PostgreSQL server-side cursors for streaming |

With these settings, the JDBC driver:
1. Sends `BEGIN` to start a transaction
2. Internally uses `DECLARE CURSOR` / `FETCH` for row batching
3. Only keeps ~5000 rows in memory at a time

### Memory Usage Comparison

| Configuration | Memory for 2M rows | Behavior |
|--------------|-------------------|----------|
| Default (fetchSize=0) | **454 MB** | All rows buffered |
| fetchSize=5000 | **~8 MB** | True streaming |

## Troubleshooting

### Out of Memory / DBeaver Crashes

**Cause**: You're using `preferQueryMode=simple` or missing the streaming settings.

**Fix**: 
1. Remove `preferQueryMode=simple` from driver properties
2. Add `defaultRowFetchSize=5000`
3. Add `defaultAutoCommit=false`

### Query Takes Forever to Start

**Cause**: Large result set without LIMIT.

**Fix**: Always use LIMIT for exploratory queries:
```sql
SELECT * FROM delta_scan('az://...') LIMIT 1000
```

### "Transaction aborted" Errors

**Cause**: Server-side cursor requires active transaction.

**Fix**: Ensure `defaultAutoCommit=false` is set.

## Large Result Sets

For queries returning millions of rows, use pagination:

```sql
-- First page
SELECT * FROM delta_scan('az://...') LIMIT 10000 OFFSET 0;

-- Second page
SELECT * FROM delta_scan('az://...') LIMIT 10000 OFFSET 10000;

-- And so on...
```

Or use server-side cursors directly:
```sql
BEGIN;
DECLARE my_cursor CURSOR FOR SELECT * FROM delta_scan('az://...');
FETCH 1000 FROM my_cursor;
-- ... process results ...
FETCH 1000 FROM my_cursor;
-- ... more processing ...
CLOSE my_cursor;
COMMIT;
```

## Reference

- [PostgreSQL JDBC Fetch Size Documentation](https://jdbc.postgresql.org/documentation/query/)
- [Understanding JDBC Fetch Size](https://shaneborden.com/2025/10/14/understanding-and-setting-postgresql-jdbc-fetch-size/)
