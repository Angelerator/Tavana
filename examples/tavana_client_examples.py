"""
Tavana High-Performance Client Examples
========================================

Tested approaches ranked by throughput (1M rows, 3 columns):

    1. ADBC (Arrow Flight SQL)     ~10M rows/s   (2.2x faster than psycopg2)
    2. psycopg2                    ~5.4M rows/s   (best PG wire client)
    3. psycopg3 + C extension      ~4.8M rows/s   (on par with psycopg2)
    4. psycopg3 pure Python         ~314K rows/s   (17x slower — avoid!)

Install dependencies:

    pip install psycopg2-binary "psycopg[c]" adbc-driver-flightsql pandas pyarrow

Usage:

    Replace TAVANA_HOST, TAVANA_USER, and TAVANA_PASSWORD with your actual values,
    or export them as environment variables before running.
"""

import os
import time

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or edit directly
# ---------------------------------------------------------------------------
TAVANA_HOST = os.environ.get("TAVANA_HOST", "localhost")
TAVANA_PG_PORT = int(os.environ.get("TAVANA_PG_PORT", "5432"))
TAVANA_FLIGHT_PORT = int(os.environ.get("TAVANA_FLIGHT_PORT", "9091"))
TAVANA_DB = os.environ.get("TAVANA_DB", "main")
TAVANA_USER = os.environ.get("TAVANA_USER", "tavana")
TAVANA_PASSWORD = os.environ.get("TAVANA_PASSWORD", "your-password")

SAMPLE_QUERY = """
    SELECT *
    FROM delta_scan('az://your-container/your/delta/table/path/')
"""


# ============================================================================
# 1. ADBC / Arrow Flight SQL  — FASTEST (~10M rows/s)
# ============================================================================
def example_adbc_flight_sql():
    """
    Arrow Flight SQL transfers data as native Apache Arrow batches over gRPC.
    Zero string-conversion overhead — ideal for analytical workloads and Pandas.
    """
    import adbc_driver_flightsql.dbapi as flight_sql

    conn = flight_sql.connect(
        uri=f"grpc://{TAVANA_HOST}:{TAVANA_FLIGHT_PORT}",
        db_kwargs={
            "username": TAVANA_USER,
            "password": TAVANA_PASSWORD,
        },
    )

    cur = conn.cursor()
    cur.execute(SAMPLE_QUERY)

    # Option A: get a PyArrow Table (zero-copy, best for analytics)
    arrow_table = cur.fetch_arrow_table()
    print(f"ADBC -> Arrow Table: {len(arrow_table):,} rows, {arrow_table.nbytes / 1e6:.1f} MB")

    # Option B: convert to Pandas DataFrame (single call, very fast)
    df = arrow_table.to_pandas()
    print(f"ADBC -> DataFrame: {len(df):,} rows")

    cur.close()
    conn.close()
    return df


# ============================================================================
# 2. ADBC with Arrow streaming (constant memory for huge tables)
# ============================================================================
def example_adbc_streaming():
    """
    For tables too large to fit in memory, use fetch_record_batch() to
    process data in chunks without ever materializing the full result.
    """
    import adbc_driver_flightsql.dbapi as flight_sql
    import pyarrow as pa

    conn = flight_sql.connect(
        uri=f"grpc://{TAVANA_HOST}:{TAVANA_FLIGHT_PORT}",
        db_kwargs={
            "username": TAVANA_USER,
            "password": TAVANA_PASSWORD,
        },
    )

    cur = conn.cursor()
    cur.execute(SAMPLE_QUERY)

    reader = cur.fetch_record_batch()
    total_rows = 0
    total_bytes = 0
    for batch in reader:
        total_rows += batch.num_rows
        total_bytes += batch.nbytes
        # process each batch here without holding the full dataset

    print(f"ADBC streaming: {total_rows:,} rows, {total_bytes / 1e6:.1f} MB processed")

    cur.close()
    conn.close()


# ============================================================================
# 3. psycopg2 — simple SELECT (best PG wire performance, ~5.4M rows/s)
# ============================================================================
def example_psycopg2_select():
    """
    psycopg2 is C-based and the fastest PostgreSQL wire protocol client.
    Best for small-to-medium result sets that fit in memory.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=TAVANA_HOST,
        port=TAVANA_PG_PORT,
        database=TAVANA_DB,
        user=TAVANA_USER,
        password=TAVANA_PASSWORD,
        sslmode="prefer",
    )
    conn.autocommit = True

    cur = conn.cursor()
    t0 = time.time()
    cur.execute(SAMPLE_QUERY)
    rows = cur.fetchall()
    elapsed = time.time() - t0

    columns = [desc[0] for desc in cur.description]
    print(f"psycopg2 SELECT: {len(rows):,} rows in {elapsed:.2f}s ({len(rows)/elapsed:,.0f} rows/s)")
    print(f"  Columns: {columns[:5]}{'...' if len(columns) > 5 else ''}")

    cur.close()
    conn.close()
    return rows


# ============================================================================
# 4. psycopg2 — server-side cursor (constant memory for large tables)
# ============================================================================
def example_psycopg2_cursor(batch_size: int = 50_000):
    """
    Server-side cursors stream data in batches via DECLARE/FETCH.
    Memory usage stays constant regardless of table size.

    Tavana materializes the cursor on first DECLARE, so subsequent
    FETCHes are fast local reads (no repeated cloud storage scans).
    """
    import psycopg2

    conn = psycopg2.connect(
        host=TAVANA_HOST,
        port=TAVANA_PG_PORT,
        database=TAVANA_DB,
        user=TAVANA_USER,
        password=TAVANA_PASSWORD,
        sslmode="prefer",
    )
    conn.autocommit = True

    cur = conn.cursor()
    t0 = time.time()

    cur.execute(f"DECLARE my_cursor CURSOR FOR {SAMPLE_QUERY}")

    total_rows = 0
    while True:
        cur.execute(f"FETCH {batch_size} FROM my_cursor")
        batch = cur.fetchall()
        if not batch:
            break
        total_rows += len(batch)
        # process each batch here

    cur.execute("CLOSE my_cursor")
    elapsed = time.time() - t0

    print(f"psycopg2 cursor: {total_rows:,} rows in {elapsed:.2f}s ({total_rows/elapsed:,.0f} rows/s)")

    cur.close()
    conn.close()


# ============================================================================
# 5. psycopg2 → Pandas DataFrame
# ============================================================================
def example_psycopg2_to_pandas():
    """
    Quick way to get a Pandas DataFrame via the PG wire protocol.
    For best performance with Pandas, prefer ADBC (example 1).
    """
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(
        host=TAVANA_HOST,
        port=TAVANA_PG_PORT,
        database=TAVANA_DB,
        user=TAVANA_USER,
        password=TAVANA_PASSWORD,
        sslmode="prefer",
    )
    conn.autocommit = True

    t0 = time.time()
    df = pd.read_sql(SAMPLE_QUERY, conn)
    elapsed = time.time() - t0

    print(f"psycopg2 -> Pandas: {len(df):,} rows in {elapsed:.2f}s")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
    print(f"  Dtypes:\n{df.dtypes}")

    conn.close()
    return df


# ============================================================================
# 6. psycopg3 + C extension  (~4.8M rows/s, modern async-capable API)
# ============================================================================
def example_psycopg3():
    """
    psycopg3 offers a modern API with async support, connection pooling,
    and pipeline mode. CRITICAL: install the C extension for performance!

        pip install "psycopg[c]"

    Without the C extension, psycopg3 is ~17x slower than psycopg2.
    """
    import psycopg

    conn = psycopg.connect(
        host=TAVANA_HOST,
        port=TAVANA_PG_PORT,
        dbname=TAVANA_DB,
        user=TAVANA_USER,
        password=TAVANA_PASSWORD,
        sslmode="prefer",
        autocommit=True,
    )

    t0 = time.time()
    cur = conn.execute(SAMPLE_QUERY)
    rows = cur.fetchall()
    elapsed = time.time() - t0

    print(f"psycopg3 SELECT: {len(rows):,} rows in {elapsed:.2f}s ({len(rows)/elapsed:,.0f} rows/s)")

    conn.close()
    return rows


# ============================================================================
# 7. SQLAlchemy (compatible with Tavana's PG wire protocol)
# ============================================================================
def example_sqlalchemy():
    """
    SQLAlchemy works with Tavana via the psycopg2 dialect.
    Useful for ORM-based applications and existing codebases.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    from urllib.parse import quote_plus

    user = quote_plus(TAVANA_USER)
    password = quote_plus(TAVANA_PASSWORD)
    url = f"postgresql+psycopg2://{user}:{password}@{TAVANA_HOST}:{TAVANA_PG_PORT}/{TAVANA_DB}"

    engine = create_engine(url, connect_args={"sslmode": "prefer"})

    t0 = time.time()
    with engine.connect() as conn:
        df = pd.read_sql(text(SAMPLE_QUERY), conn)
    elapsed = time.time() - t0

    print(f"SQLAlchemy -> Pandas: {len(df):,} rows in {elapsed:.2f}s")

    engine.dispose()
    return df


# ============================================================================
# 8. DuckDB client-side (attach Tavana as a remote PostgreSQL source)
# ============================================================================
def example_duckdb_attach():
    """
    Use local DuckDB to query Tavana by attaching it as a PostgreSQL source.
    Great for joining Tavana data with local files/parquet.
    """
    import duckdb
    from urllib.parse import quote_plus

    user = quote_plus(TAVANA_USER)
    password = quote_plus(TAVANA_PASSWORD)
    pg_url = f"postgresql://{user}:{password}@{TAVANA_HOST}:{TAVANA_PG_PORT}/{TAVANA_DB}"

    db = duckdb.connect()
    db.execute("INSTALL postgres; LOAD postgres;")
    db.execute(f"ATTACH '{pg_url}' AS tavana (TYPE postgres, READ_ONLY);")

    t0 = time.time()
    result = db.execute(f"SELECT * FROM tavana.main.({SAMPLE_QUERY})")
    arrow_table = result.fetch_arrow_table()
    elapsed = time.time() - t0

    print(f"DuckDB attach: {len(arrow_table):,} rows in {elapsed:.2f}s")

    db.close()
    return arrow_table


# ============================================================================
# Quick benchmark runner
# ============================================================================
def run_benchmark(query: str = "SELECT i, i*2 AS d, i*3 AS t FROM generate_series(1, 500000) t(i)"):
    """
    Run a quick throughput benchmark comparing available clients.
    Uses generate_series so no cloud credentials are needed.
    """
    import psycopg2

    print("=" * 70)
    print("Tavana Client Throughput Benchmark")
    print("=" * 70)

    # psycopg2
    conn = psycopg2.connect(
        host=TAVANA_HOST, port=TAVANA_PG_PORT, database=TAVANA_DB,
        user=TAVANA_USER, password=TAVANA_PASSWORD, sslmode="prefer",
    )
    conn.autocommit = True
    cur = conn.cursor()
    t0 = time.time()
    cur.execute(query)
    rows = cur.fetchall()
    elapsed = time.time() - t0
    print(f"  psycopg2 SELECT:   {len(rows):>10,} rows in {elapsed:.3f}s  ({len(rows)/elapsed:>12,.0f} rows/s)")
    cur.close()
    conn.close()

    # psycopg3
    try:
        import psycopg
        conn3 = psycopg.connect(
            host=TAVANA_HOST, port=TAVANA_PG_PORT, dbname=TAVANA_DB,
            user=TAVANA_USER, password=TAVANA_PASSWORD,
            sslmode="prefer", autocommit=True,
        )
        t0 = time.time()
        cur = conn3.execute(query)
        rows = cur.fetchall()
        elapsed = time.time() - t0
        has_c = False
        try:
            import psycopg_c  # noqa: F401
            has_c = True
        except ImportError:
            pass
        label = "psycopg3+C" if has_c else "psycopg3 (SLOW!)"
        print(f"  {label:20s} {len(rows):>10,} rows in {elapsed:.3f}s  ({len(rows)/elapsed:>12,.0f} rows/s)")
        conn3.close()
    except ImportError:
        print("  psycopg3:          (not installed)")

    # ADBC
    try:
        import adbc_driver_flightsql.dbapi as flight_sql
        conn_a = flight_sql.connect(
            uri=f"grpc://{TAVANA_HOST}:{TAVANA_FLIGHT_PORT}",
            db_kwargs={"username": TAVANA_USER, "password": TAVANA_PASSWORD},
        )
        cur = conn_a.cursor()
        t0 = time.time()
        cur.execute(query)
        tbl = cur.fetch_arrow_table()
        elapsed = time.time() - t0
        print(f"  ADBC Flight SQL:   {len(tbl):>10,} rows in {elapsed:.3f}s  ({len(tbl)/elapsed:>12,.0f} rows/s)")
        cur.close()
        conn_a.close()
    except Exception as e:
        print(f"  ADBC Flight SQL:   (unavailable: {e})")

    print("=" * 70)


if __name__ == "__main__":
    print(__doc__)
    print("Running benchmark with generate_series (no credentials needed)...\n")
    run_benchmark()
