# Tableau Configuration for Tavana

This directory contains configuration files to connect Tableau to Tavana.

## Quick Start

### 1. Install the TDC File

Copy `tavana.tdc` to your Tableau Datasources folder:

**Windows:**
```powershell
copy tavana.tdc "%USERPROFILE%\Documents\My Tableau Repository\Datasources\"
```

**Mac:**
```bash
cp tavana.tdc ~/Documents/My\ Tableau\ Repository/Datasources/
```

**Tableau Server:**
```bash
cp tavana.tdc /var/opt/tableau/tableau_server/data/tabsvc/vizqlserver/Datasources/
```

### 2. Restart Tableau

Close and reopen Tableau Desktop, or restart Tableau Server.

### 3. Connect to Tavana

1. Open Tableau Desktop
2. Connect → PostgreSQL
3. Enter connection details:
   - **Server**: `tavana.example.com`
   - **Port**: `9090`
   - **Database**: `main` (or leave empty)
   - **Username**: Your Separ username (e.g., `user@example.com`)
   - **Password**: Your Separ password
   - **Require SSL**: ✅ Checked

### 4. Use Custom SQL

Since Tavana doesn't have traditional schemas/tables, use **Custom SQL**:

```sql
SELECT * FROM delta_scan('az://container/path/to/delta/') LIMIT 1000
```

## Why the TDC File?

Tavana uses a **stateless architecture** where queries are distributed across multiple workers. This means:

- Temporary tables created on Worker A don't exist on Worker B
- Session variables don't persist across queries
- Transaction state isn't maintained

The TDC file tells Tableau to avoid these session-dependent features:

| Capability | Setting | Reason |
|------------|---------|--------|
| `CAP_CREATE_TEMP_TABLES` | no | Temp tables don't persist across workers |
| `CAP_SELECT_INTO` | no | Creates tables (session-dependent) |
| `CAP_SET_QUERY_BANDING` | no | Session variables don't persist |
| `CAP_ISOLATION_LEVEL_*` | no | Transaction isolation is per-query |

## Troubleshooting

### "Bad Connection" Error

If you still get connection errors:

1. Verify the TDC file is in the correct location
2. Restart Tableau completely
3. Check that SSL is enabled
4. Verify your credentials with psql:
   ```bash
   PGPASSWORD='your-password' psql -h tavana.example.com -p 9090 -U 'user@example.com' -c "SELECT 1"
   ```

### No Tables Visible

Tavana is a **query engine**, not a database with stored tables. Use Custom SQL with `delta_scan()`:

```sql
-- Query Delta Lake tables
SELECT * FROM delta_scan('az://storage/path/') LIMIT 100

-- Query Parquet files
SELECT * FROM parquet_scan('az://storage/path/*.parquet') LIMIT 100
```

### Performance Tips

1. **Always use LIMIT** for initial exploration
2. **Use column selection** instead of `SELECT *`
3. **Filter early** with WHERE clauses
4. **Leverage partition pruning** by filtering on partition columns

## Reference

- [Tableau Connector Capabilities](https://help.tableau.com/current/pro/desktop/en-us/odbc_capabilities.htm)
- [Tableau TDC Documentation](https://tableau.github.io/connector-plugin-sdk/docs/capabilities)

