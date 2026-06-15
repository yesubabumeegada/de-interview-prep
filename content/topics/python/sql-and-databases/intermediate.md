---
title: "SQL and Databases - Intermediate"
topic: python
subtopic: sql-and-databases
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, sqlalchemy, connection-pooling, bulk-insert, upsert, transactions, asyncpg, snowflake, bigquery]
---

# SQL and Databases — Intermediate

## SQLAlchemy Core vs ORM: When to Use Each

SQLAlchemy has two distinct layers. For data engineering work, the choice matters.

```python
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import DeclarativeBase, Session


engine = create_engine("postgresql://user:pass@host:5432/db")


# ── SQLAlchemy CORE (preferred for DE) ────────────────────────────────────
# - SQL-first: you write SQL (or SQL expressions), SQLAlchemy executes it
# - Great for: bulk inserts, analytical queries, pipeline code
# - Lower overhead than ORM

meta = MetaData()
orders_table = Table(
    "orders", meta,
    Column("order_id", String, primary_key=True),
    Column("amount_cents", Integer),
    Column("status", String),
)

# Execute raw SQL via Core (cleanest for pipeline code)
with engine.connect() as conn:
    result = conn.execute(
        text("SELECT order_id, amount_cents FROM orders WHERE status = :status"),
        {"status": "completed"}
    )
    rows = result.mappings().all()   # List of dict-like RowMapping objects


# ── SQLAlchemy ORM (avoid for DE bulk work) ───────────────────────────────
# - Object-first: maps Python classes to tables
# - Great for: web apps with CRUD on individual objects
# - Problematic for DE: ORM overhead kills bulk insert performance;
#   ORM loads entire objects into memory

class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    order_id: str = Column(String, primary_key=True)
    amount_cents: int = Column(Integer)
    status: str = Column(String)

# ORM bulk insert (slow for DE — do NOT use for 1M+ rows)
with Session(engine) as session:
    session.add_all([Order(order_id="1", amount_cents=999, status="pending")])
    session.commit()
# Use Core's execute(insert(table).values(...)) instead for bulk work
```

**Rule for DE:** Use **Core** for pipeline code (bulk inserts, analytical queries). Reserve ORM for metadata tables (pipeline run logs, configs) where you're inserting a few rows.

---

## Connection Pooling

Creating a new DB connection takes 50-500ms (TCP handshake + auth + SSL). Connection pools reuse connections, reducing latency to near-zero.

```python
from sqlalchemy import create_engine, event

engine = create_engine(
    "postgresql://user:pass@host:5432/db",
    pool_size=10,          # Persistent connections always kept open
    max_overflow=5,        # Extra connections allowed when pool is exhausted
                           # Total max = pool_size + max_overflow = 15
    pool_timeout=30,       # Seconds to wait for a free connection before raising
    pool_recycle=3600,     # Recycle connections older than 1 hour (prevents stale)
    pool_pre_ping=True,    # Execute "SELECT 1" before each checkout to detect dead connections
                           # Prevents "server closed the connection unexpectedly" errors
)

# Event hooks for monitoring pool behavior
@event.listens_for(engine, "checkout")
def on_checkout(dbapi_conn, connection_record, connection_proxy):
    # Called every time a connection is checked out from the pool
    pass  # Add metrics here: pool_checkouts.inc()

@event.listens_for(engine, "checkin")
def on_checkin(dbapi_conn, connection_record):
    pass  # Connection returned to pool


# For scripts (not long-running services): use NullPool (no pooling, close immediately)
from sqlalchemy.pool import NullPool
script_engine = create_engine("postgresql://...", poolclass=NullPool)

# Check pool status
with engine.connect() as conn:
    pool = engine.pool
    print(f"Pool size: {pool.size()}")
    print(f"Checked out: {pool.checkedout()}")
    print(f"Overflow: {pool.overflow()}")
```

---

## Bulk Inserts: executemany vs COPY vs to_sql

Performance differs by **orders of magnitude**. Always choose the fastest method for your use case.

```python
import psycopg2
import psycopg2.extras
import io
import csv
import time


def benchmark_insert_methods(records: list[dict], conn_str: str):
    """Compare insert speeds for 100K records."""

    # Method 1: Row-by-row execute — NEVER use for bulk (1 round-trip per row)
    # 100K rows ≈ 60-120 seconds
    conn = psycopg2.connect(conn_str)
    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                "INSERT INTO test_table (id, amount) VALUES (%s, %s)",
                (record["id"], record["amount"])
            )
    conn.commit()

    # Method 2: executemany — batches records, but still one INSERT per row
    # 100K rows ≈ 5-15 seconds (better but still slow)
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO test_table (id, amount) VALUES (%s, %s)",
            [(r["id"], r["amount"]) for r in records]
        )
    conn.commit()

    # Method 3: execute_values (psycopg2) — builds one multi-row INSERT
    # INSERT INTO test_table VALUES (%s, %s), (%s, %s), ... (N rows per statement)
    # 100K rows ≈ 1-3 seconds
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO test_table (id, amount) VALUES %s",
            [(r["id"], r["amount"]) for r in records],
            page_size=1000,  # Rows per INSERT statement
        )
    conn.commit()

    # Method 4: COPY FROM — binary protocol, fastest possible
    # 100K rows ≈ 0.1-0.5 seconds (10-100x faster than executemany)
    def copy_from_records(conn, records: list[dict], table: str):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        for r in records:
            writer.writerow([r["id"], r["amount"]])
        buffer.seek(0)
        with conn.cursor() as cur:
            cur.copy_from(buffer, table, sep=",", columns=("id", "amount"))
        conn.commit()

    copy_from_records(conn, records, "test_table")
    conn.close()


# pandas to_sql with method='multi' (uses multi-row INSERT, not COPY)
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql://...")
df.to_sql(
    "test_table",
    engine,
    if_exists="append",
    index=False,
    method="multi",      # Multi-row INSERT (2-5x faster than default single-row)
    chunksize=5_000,
)

# For maximum pandas→Postgres speed: use method with COPY
from io import StringIO

def pg_copy_from_df(df: pd.DataFrame, table: str, conn):
    """Use COPY for orders-of-magnitude faster pandas→Postgres loads."""
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(buffer, table, sep=",", null="")
    conn.commit()
```

---

## Upsert Patterns

```python
# Postgres: ON CONFLICT DO UPDATE (UPSERT)
def upsert_postgres(conn, records: list[dict], table: str, conflict_cols: list[str]):
    """Insert or update on conflict. Idempotent."""
    cols = list(records[0].keys())
    update_cols = [c for c in cols if c not in conflict_cols]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES %s
        ON CONFLICT ({", ".join(conflict_cols)})
        DO UPDATE SET {update_clause}
    """
    import psycopg2.extras
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, sql, [tuple(r[c] for c in cols) for r in records]
        )
    conn.commit()


# Snowflake: MERGE statement
def upsert_snowflake(conn, records: list[dict], target: str, key_col: str):
    """Snowflake MERGE (upsert). Uses a staging table → MERGE pattern."""
    # Step 1: Load into a temp staging table
    stage_table = f"{target}_stage_{int(time.time())}"
    conn.execute(f"CREATE TEMPORARY TABLE {stage_table} LIKE {target}")

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO {stage_table} VALUES %s",
            [tuple(r.values()) for r in records],
        )

    # Step 2: MERGE staging into target
    cols = list(records[0].keys())
    update_set = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key_col)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    merge_sql = f"""
        MERGE INTO {target} t
        USING {stage_table} s ON t.{key_col} = s.{key_col}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    conn.execute(merge_sql)
    conn.execute(f"DROP TABLE IF EXISTS {stage_table}")
    conn.commit()
```

---

## Transaction Management

```python
import psycopg2

conn = psycopg2.connect("postgresql://...")

# Explicit transaction control (default behavior — autocommit is OFF)
with conn.cursor() as cur:
    try:
        cur.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
        cur.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
        conn.commit()   # Both updates committed atomically
    except Exception:
        conn.rollback() # Both updates rolled back — accounts unchanged
        raise

# Savepoints: partial rollback within a transaction
with conn.cursor() as cur:
    conn.autocommit = False
    cur.execute("INSERT INTO orders (id, amount) VALUES (1, 99.99)")
    cur.execute("SAVEPOINT order_inserted")

    try:
        cur.execute("INSERT INTO order_items (order_id, sku) VALUES (1, 'ABC')")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT order_inserted")
        # order row survives; order_items insert was rolled back

    conn.commit()  # Commits the order but not the failed item insert

# Autocommit mode (required for DDL, VACUUM in Postgres, CREATE DATABASE)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("VACUUM ANALYZE orders")  # Can't run inside a transaction
conn.autocommit = False
```

---

## Async Database Access

```python
import asyncio
import asyncpg   # pip install asyncpg — pure async Postgres driver


async def main():
    # asyncpg connection (not psycopg2 — different API)
    conn = await asyncpg.connect("postgresql://user:pass@host/db")

    # Parameterized: asyncpg uses $1, $2, ... not %s
    rows = await conn.fetch(
        "SELECT id, amount FROM orders WHERE status = $1", "completed"
    )
    for row in rows:
        print(dict(row))

    # Bulk insert with executemany
    await conn.executemany(
        "INSERT INTO processed (id, amount) VALUES ($1, $2)",
        [(r["id"], r["amount"]) for r in rows]
    )

    await conn.close()


# Connection pool for async (essential in production)
async def main_with_pool():
    pool = await asyncpg.create_pool(
        "postgresql://user:pass@host/db",
        min_size=5,
        max_size=20,
    )
    async with pool.acquire() as conn:
        result = await conn.fetchval("SELECT COUNT(*) FROM orders")
        print(f"Total orders: {result}")
    await pool.close()


asyncio.run(main_with_pool())
```

---

## Database-Specific Connectors

```python
# Snowflake
import snowflake.connector

conn = snowflake.connector.connect(
    account="xy12345.us-east-1",
    user="etl_user",
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse="COMPUTE_WH",
    database="ANALYTICS",
    schema="PUBLIC",
    role="TRANSFORMER",
)
cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
cur.close()
conn.close()

# BigQuery
from google.cloud import bigquery

client = bigquery.Client(project="my-project")  # Auth via GOOGLE_APPLICATION_CREDENTIALS
query = "SELECT * FROM `my-project.dataset.table` WHERE date = @date"
job_config = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("date", "STRING", "2024-01-15")]
)
df = client.query(query, job_config=job_config).to_dataframe()

# Redshift (psycopg2 — Redshift is Postgres-compatible)
import psycopg2
conn = psycopg2.connect(
    host="cluster.account.us-east-1.redshift.amazonaws.com",
    port=5439,
    dbname="warehouse",
    user="etl_user",
    password=os.environ["REDSHIFT_PASSWORD"],
    sslmode="require",
)
```

---

## Interview Tips

- **SQLAlchemy Core over ORM** for DE is a concrete opinion — saying "I use Core for pipeline code because ORM overhead matters at scale" shows production judgment.
- Know the bulk insert hierarchy: row-by-row → `executemany` → `execute_values` → `COPY`. Be able to state approximate speedups (COPY is 10-100x faster than `executemany`).
- `pool_pre_ping=True` is a must-mention for production SQLAlchemy engines — it prevents cryptic "connection was closed" errors on long-running pipelines.
- The difference between `pool_size` and `max_overflow`: pool_size connections are always open; max_overflow connections are created on demand and closed when returned.
