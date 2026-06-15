---
title: "SQL and Databases - Real World"
topic: python
subtopic: sql-and-databases
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, sql, production, incident, connection-pool, sql-injection, pandas, snowflake]
---

# SQL and Databases — Real World

## Production Incident: Connection Pool Exhaustion Under Load

**Situation:** A nightly pipeline that normally ran in 30 minutes began timing out after 15 minutes. `pg_stat_activity` showed 100 connections — Postgres's maximum — all in state `idle in transaction`. The pipeline workers were queued waiting for a connection that would never be released.

**Root cause:** A bug introduced earlier that week left connections open inside a `try/except` block that swallowed the exception without reaching the `finally` clause.

```python
# ❌ THE BUG: exception is swallowed, conn.close() never runs
def load_records(records: list[dict]) -> int:
    conn = psycopg2.connect(DB_URL)  # Connection opened
    try:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, records)
        conn.commit()
        return len(records)
    except psycopg2.IntegrityError:
        conn.rollback()
        return 0
        # conn.close() is NEVER CALLED — connection leaked!
        # After 20 workers each leak 5 connections = 100 connections, pool exhausted
```

**Diagnosis using pg_stat_activity:**

```python
def diagnose_connection_pool(conn):
    """Check for leaked/stuck connections."""
    sql = """
        SELECT
            state,
            COUNT(*) as count,
            MAX(EXTRACT(EPOCH FROM (now() - query_start))) as max_age_seconds,
            LEFT(query, 100) as sample_query
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY state, LEFT(query, 100)
        ORDER BY count DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()

# Output showed:
# state="idle in transaction", count=87, max_age_seconds=847
# → 87 connections stuck in open transactions for 14 minutes
```

**The fix — always use context managers and finally blocks:**

```python
# ✅ FIX 1: Use SQLAlchemy engine (pool manages connections automatically)
from sqlalchemy import create_engine, text

engine = create_engine(DB_URL, pool_size=10, pool_pre_ping=True)

def load_records_safe(records: list[dict]) -> int:
    with engine.begin() as conn:  # engine.begin() = auto-commit on exit, auto-rollback on error
        conn.execute(
            text("INSERT INTO orders ..."),
            records
        )
    return len(records)
    # Connection ALWAYS returned to pool when `with` block exits, success or failure

# ✅ FIX 2: If using raw psycopg2, always close in finally
def load_records_psycopg2(records: list[dict]) -> int:
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, records)
        conn.commit()
        return len(records)
    except psycopg2.IntegrityError:
        conn.rollback()
        return 0
    finally:
        conn.close()  # Runs no matter what — success, IntegrityError, or any other exception

# ✅ FIX 3: Additional safeguard — pool_timeout to fail fast
engine = create_engine(
    DB_URL,
    pool_size=10,
    pool_timeout=5,  # Raise after 5 seconds if no connection available
                     # Fail loudly rather than hanging for 60 seconds
)
```

**Result:** After deploying the fix, connections dropped from 100 to 8 under the same load. The pipeline ran in its normal 28 minutes.

**Key lesson:** Every psycopg2 `connect()` call must have a guaranteed `close()` — in a `finally` block or as a context manager. SQLAlchemy's `engine.begin()` eliminates this risk by managing the connection lifecycle automatically.

---

## Production Incident: Parameterized Query Bypassed with String Formatting

**Situation:** A data analyst added a "filter by date range" feature to a pipeline's admin dashboard. They used Python's `.format()` to build the WHERE clause from a user-supplied date range. An engineer testing input validation entered `2024-01-01' OR '1'='1` as the start date.

```python
# ❌ THE VULNERABLE CODE (simplified)
def get_pipeline_runs(start_date: str, end_date: str) -> list[dict]:
    # "start_date" comes from a web form — user-controlled input
    query = f"""
        SELECT run_id, status, rows_loaded
        FROM pipeline_runs
        WHERE started_at BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY started_at DESC
    """
    # Input: start_date = "2024-01-01' OR '1'='1"
    # Resulting SQL:
    # WHERE started_at BETWEEN '2024-01-01' OR '1'='1' AND '...'
    # '1'='1' is always True → returns ALL rows in the table
    with engine.connect() as conn:
        return conn.execute(text(query)).mappings().all()
```

**The fix — parameterized queries everywhere, no exceptions:**

```python
# ✅ SAFE: Parameterized query — user input is always a value, never SQL
from sqlalchemy import text

def get_pipeline_runs_safe(start_date: str, end_date: str) -> list[dict]:
    # Validate types before executing (defense in depth)
    from datetime import date
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {start_date!r}, {end_date!r}")

    query = text("""
        SELECT run_id, status, rows_loaded
        FROM pipeline_runs
        WHERE started_at BETWEEN :start_date AND :end_date
        ORDER BY started_at DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
        return result.mappings().all()
    # SQLAlchemy sends: WHERE started_at BETWEEN $1 AND $2 with values ['2024-01-01', '2024-12-31']
    # The malicious input becomes a literal string parameter — not executable SQL
```

**What made this incident worse:** The `pipeline_runs` table had a `config_json` column containing API keys for 15 external services. SQL injection could have exfiltrated all of them.

**Prevention checklist:**
1. Linting rule: ban `f"...{user_input}..."` patterns in SQL files (use `sqlfluff` or custom AST check)
2. Parameterize ALL queries — even "safe" internal values (defense in depth)
3. DB user permissions: the ETL user should only have `SELECT/INSERT/UPDATE` on specific tables — not `DROP TABLE`, `COPY TO STDOUT`, or cross-schema access
4. Code review checklist item: "Does this PR build any SQL string dynamically?"

---

## Production Incident: pandas to_sql Creating Wrong Column Types

**Situation:** A pipeline used `df.to_sql("fact_orders", engine, if_exists="replace")` to rebuild a daily table. After migration to a new Python version, `amount` (which had been `NUMERIC`) started being created as `DOUBLE PRECISION`, causing downstream Snowflake comparisons to fail silently due to floating-point precision.

**Root cause:** `df.to_sql()` infers Postgres types from pandas dtypes. `float64` maps to `DOUBLE PRECISION`, not `NUMERIC`. On the new Python version, amount strings that were previously inferred as `object` were now inferred as `float64`.

```python
# ❌ FRAGILE: type inference from pandas dtypes
df.to_sql("fact_orders", engine, if_exists="replace", index=False)
# Creates: amount DOUBLE PRECISION (float arithmetic → precision loss on currency!)
```

**The fix — explicit dtype mapping:**

```python
from sqlalchemy import Integer, String, Numeric, DateTime, Boolean
from sqlalchemy import Column, Table, MetaData


# ✅ Option 1: Pass dtype dict to to_sql
df.to_sql(
    "fact_orders",
    engine,
    if_exists="append",  # Never "replace" in production — you lose your indexes!
    index=False,
    dtype={
        "order_id": String(64),
        "customer_id": String(64),
        "amount_cents": Integer(),       # Store as integer cents, never float
        "status": String(32),
        "created_at": DateTime(timezone=True),
    },
    chunksize=10_000,
    method="multi",
)

# ✅ Option 2: Create table explicitly before to_sql (best practice)
def ensure_table_exists(engine):
    """Create table with exact schema — never let pandas infer types."""
    meta = MetaData()
    Table(
        "fact_orders", meta,
        Column("order_id", String(64), primary_key=True),
        Column("customer_id", String(64), nullable=False),
        Column("amount_cents", Integer, nullable=False),
        Column("status", String(32), server_default="pending"),
        Column("created_at", DateTime(timezone=True)),
    )
    meta.create_all(engine, checkfirst=True)  # Only creates if table doesn't exist

ensure_table_exists(engine)
df.to_sql("fact_orders", engine, if_exists="append", index=False)

# Currency rule: NEVER store money as FLOAT/DOUBLE
# Float arithmetic: 0.1 + 0.2 = 0.30000000000000004
# Store as INTEGER cents (divide by 100 at display time) or NUMERIC(18,2)
```

---

## Production Incident: Snowflake Connector Token Expiry After 4 Hours

**Situation:** A Snowflake pipeline that processed a large historical backfill started failing after 4 hours with `snowflake.connector.errors.OperationalError: JWT token has expired`. The connection was kept open the entire time rather than refreshed.

**Root cause:** Snowflake's key-pair authentication JWT tokens expire after 60 minutes by default. The pipeline held one connection for the entire 4-hour run.

**The fix — reconnect per batch, or use connection factories:**

```python
import snowflake.connector
from contextlib import contextmanager
import os


def make_snowflake_connection():
    """Create a fresh Snowflake connection. Always use fresh connections for long jobs."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_KEY_PATH"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        # Token lifetime: set to max to reduce reconnect frequency
        # Default 60min; max 60min for JWT; use OAuth for longer-lived sessions
        session_parameters={"QUERY_TAG": "etl_pipeline"},
        login_timeout=30,
        network_timeout=300,
    )


@contextmanager
def snowflake_connection():
    """Context manager: fresh connection, always closed."""
    conn = make_snowflake_connection()
    try:
        yield conn
    finally:
        conn.close()


def load_batch_to_snowflake(batch: list[dict], table: str):
    """
    Open a fresh connection per batch.
    Eliminates token expiry on multi-hour pipelines.
    Overhead: ~200ms per batch open/close — acceptable for batches of 10K+.
    """
    with snowflake_connection() as conn:
        with conn.cursor() as cur:
            # Use %s placeholders (Snowflake connector is psycopg2-compatible)
            placeholders = ", ".join(["%s"] * len(batch[0]))
            cur.executemany(
                f"INSERT INTO {table} VALUES ({placeholders})",
                [tuple(r.values()) for r in batch]
            )
        conn.commit()


# For very long pipelines: reconnect every N batches
def load_large_dataset(records_iter, table: str, batch_size: int = 10_000,
                        reconnect_every: int = 20):
    """Reconnect every 20 batches (~200K rows) to prevent token expiry."""
    batch = []
    batch_count = 0
    conn = make_snowflake_connection()

    try:
        for record in records_iter:
            batch.append(record)
            if len(batch) >= batch_size:
                _insert_batch(conn, batch, table)
                batch_count += 1
                batch = []

                if batch_count % reconnect_every == 0:
                    conn.close()
                    conn = make_snowflake_connection()
                    logger.info(f"Reconnected to Snowflake after {batch_count} batches")

        if batch:
            _insert_batch(conn, batch, table)
    finally:
        conn.close()
```

---

## Best Practices Checklist for Python + Databases

| Risk | Prevention |
|---|---|
| SQL injection | Always use parameterized queries — never f-strings in SQL |
| Connection leak | Use context managers / SQLAlchemy engine — never `conn.close()` without `finally` |
| Pool exhaustion | Size pools using Little's Law; set `pool_timeout` to fail fast |
| OOM on large queries | Server-side cursors (named cursors in psycopg2) or `pd.read_sql(chunksize=N)` |
| Wrong column types | Explicit `dtype=` in `to_sql`; pre-create table schema |
| Float precision on money | Store as integer cents or `NUMERIC(18,2)`, never `FLOAT`/`DOUBLE` |
| Token expiry on long runs | Reconnect per batch or periodically; use `pool_pre_ping=True` |
| Slow bulk inserts | Use COPY > `execute_values` > `executemany` > row-by-row |
| Duplicate data on retry | Upserts with `ON CONFLICT DO UPDATE` or MERGE |
| Untested schema changes | Use Alembic migrations; test with testcontainers |

---

## Interview Tips

- The connection pool exhaustion story is extremely common in production — having a specific root cause (leaked connection in exception handler) and fix (context managers / `engine.begin()`) is compelling.
- SQL injection: the correct answer is always "parameterized queries, no exceptions" — but also mention defense-in-depth: minimal DB permissions, linting rules to catch dynamic SQL.
- `pool_pre_ping=True` is worth memorizing — it prevents cryptic "SSL connection has been closed unexpectedly" errors that only happen after a period of connection idle time.
- Snowflake-specific: JWT token expiry after 60 minutes is a well-known pitfall for long-running pipelines — mentioning it signals real Snowflake production experience.
