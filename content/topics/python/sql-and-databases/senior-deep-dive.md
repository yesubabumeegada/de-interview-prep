---
title: "SQL and Databases - Senior Deep Dive"
topic: python
subtopic: sql-and-databases
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, sql, connection-pool, server-side-cursor, locking, sqlalchemy-events, testcontainers, alembic]
---

# SQL and Databases — Senior Deep Dive

## Connection Pool Sizing: Little's Law

Guessing pool size leads to either under-utilization (too small → timeouts) or resource waste (too large → DB overwhelmed). Use **Little's Law** from queuing theory.

```
pool_size = throughput × average_latency
```

**Example:** 50 concurrent pipeline workers, each query takes 200ms average.

```python
# Little's Law: pool_size = concurrency × latency_in_seconds
concurrency = 50
avg_query_latency = 0.2  # 200ms
pool_size = concurrency * avg_query_latency  # = 10

# Add overhead factor for spikes (1.2-2x)
recommended_pool_size = int(pool_size * 1.5)  # = 15

from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://user:pass@host/db",
    pool_size=15,           # Based on Little's Law
    max_overflow=5,         # Burst capacity
    pool_timeout=10,        # Fail fast — don't queue forever
    pool_pre_ping=True,     # Detect dead connections
)

# Pool sizing anti-patterns:
# 1. pool_size = num_workers (20 workers × 5 queries each = 100 connections!)
#    Fix: pool_size should reflect CONCURRENT connections, not total workers
# 2. pool_size = postgres max_connections (usually 100)
#    Fix: leave headroom for direct queries, monitoring, pgAdmin connections
# 3. Setting pool_timeout too high — a 60-second timeout means pipelines
#    silently hang for a minute before failing

# Monitor pool health
import logging
logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)  # Very verbose but useful
```

**Postgres connection limit awareness:** Postgres `max_connections` defaults to 100. With 5 pipeline containers each using pool_size=20, you've consumed all 100 connections. Use PgBouncer (connection pooler) between your app and Postgres in production to multiplex thousands of app connections onto a smaller Postgres pool.

---

## Query Performance from Python: EXPLAIN via psycopg2

Identify slow queries without leaving Python:

```python
import psycopg2
import json


def explain_query(conn, query: str, params=None, analyze: bool = False) -> dict:
    """
    Run EXPLAIN (ANALYZE) on a query and return the plan.
    analyze=True actually executes the query — use only on small tables or replicas!
    """
    explain_sql = f"EXPLAIN (FORMAT JSON{', ANALYZE' if analyze else ''}) {query}"
    with conn.cursor() as cur:
        cur.execute(explain_sql, params)
        plan = cur.fetchone()[0][0]  # EXPLAIN FORMAT JSON returns nested list
    return plan


def find_slow_queries(conn, min_duration_ms: float = 1000) -> list[dict]:
    """Find queries running longer than min_duration_ms using pg_stat_activity."""
    sql = """
        SELECT
            pid,
            now() - pg_stat_activity.query_start AS duration,
            query,
            state
        FROM pg_stat_activity
        WHERE (now() - pg_stat_activity.query_start) > interval %(min_ms)s
          AND state = 'active'
        ORDER BY duration DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"min_ms": f"{min_duration_ms} milliseconds"})
        return cur.fetchall()


# Example usage
conn = psycopg2.connect("postgresql://...")
plan = explain_query(conn, "SELECT * FROM orders WHERE customer_id = %s", ("cust_001",))

# Check for sequential scans (red flag for large tables)
plan_text = json.dumps(plan)
if "Seq Scan" in plan_text:
    print("⚠️  Sequential scan detected — consider adding an index")
    print(f"Table: {plan[0]['Plan'].get('Relation Name', 'unknown')}")

# Kill a long-running query
def cancel_query(conn, pid: int):
    """Gracefully cancel a running query (allows transaction cleanup)."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
    conn.commit()
```

---

## Large Result Streaming: Server-Side Cursors

`fetchall()` loads the entire result into Python memory. For millions of rows, use a **server-side (named) cursor** — Postgres holds the result on disk and streams it to Python in batches.

```python
import psycopg2
from typing import Iterator


def stream_large_table(
    conn, query: str, params=None, batch_size: int = 10_000
) -> Iterator[list[dict]]:
    """
    Server-side cursor: Postgres streams results, Python uses O(batch_size) memory.
    Contrast with fetchall(): Postgres sends ALL rows before Python sees ANY.
    """
    # Named cursor = server-side cursor in psycopg2
    # Unnamed cursors (conn.cursor()) are client-side (fetchall loads everything)
    with conn.cursor("streaming_cursor", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = batch_size  # Rows per network round-trip
        cur.execute(query, params)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield list(rows)


# Usage: process 50M rows with constant memory
conn = psycopg2.connect("postgresql://...")
conn.autocommit = False  # Server-side cursors require a transaction

total = 0
for batch in stream_large_table(conn, "SELECT * FROM fact_events WHERE year = 2024"):
    process_batch(batch)
    total += len(batch)
    print(f"Processed {total:,} rows")

# ── Key distinction ────────────────────────────────────────────────────────
# Client-side (default): cur.execute() → DB runs query → sends ALL rows →
#   Python stores ALL rows in memory → cur.fetchmany() returns from local buffer
#   Memory: O(result_set_size) — dangerous for large tables
#
# Server-side (named): cur.execute() → DB runs query → holds result in temp file →
#   cur.fetchmany() → DB sends 10K rows → Python processes → cur.fetchmany() again
#   Memory: O(batch_size) regardless of result set size — safe for any table size
```

---

## Optimistic vs Pessimistic Locking in Python Apps

```python
import psycopg2
from datetime import datetime


# Pessimistic locking: SELECT FOR UPDATE locks the row
# Use when: high contention, can't afford to retry
def transfer_funds_pessimistic(conn, from_id: int, to_id: int, amount: float):
    """Lock both rows before reading — guaranteed no concurrent modifications."""
    with conn.cursor() as cur:
        # Lock both rows in consistent order (always smaller ID first to prevent deadlock)
        first_id, second_id = min(from_id, to_id), max(from_id, to_id)
        cur.execute(
            "SELECT id, balance FROM accounts WHERE id IN (%s, %s) "
            "ORDER BY id FOR UPDATE",  # Locks rows until COMMIT
            (first_id, second_id)
        )
        rows = {r[0]: r[1] for r in cur.fetchall()}

        if rows[from_id] < amount:
            raise ValueError("Insufficient funds")

        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (amount, from_id))
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (amount, to_id))
    conn.commit()


# Optimistic locking: no lock on read; check version on write; retry on conflict
# Use when: low contention, retries are cheap
def update_pipeline_config_optimistic(conn, config_id: int, new_value: dict):
    """Read-modify-write with version check. No locks held during processing."""
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value, version FROM pipeline_configs WHERE id = %s",
                (config_id,)
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Config {config_id} not found")

            current_value, version = row
            # ... apply business logic to current_value ...

            cur.execute(
                """UPDATE pipeline_configs
                   SET value = %s, version = version + 1, updated_at = NOW()
                   WHERE id = %s AND version = %s""",  # Only update if version matches
                (json.dumps(new_value), config_id, version)
            )
            rows_updated = cur.rowcount  # 0 = someone else updated it; 1 = success
        conn.commit()

        if rows_updated == 1:
            return  # Success
        # Version mismatch — retry from fresh read
        time.sleep(0.1 * (2 ** attempt))  # Backoff before retry

    raise RuntimeError(f"Could not update config {config_id} after {MAX_RETRIES} retries")
```

---

## SQLAlchemy Event Hooks for Logging and Metrics

```python
from sqlalchemy import event
from sqlalchemy.engine import Engine
import time
import logging

logger = logging.getLogger("db.queries")
query_count = 0
slow_query_threshold_ms = 500


@event.listens_for(Engine, "before_cursor_execute")
def before_execute(conn, cursor, statement, parameters, context, executemany):
    context._query_start_time = time.time()


@event.listens_for(Engine, "after_cursor_execute")
def after_execute(conn, cursor, statement, parameters, context, executemany):
    global query_count
    duration_ms = (time.time() - context._query_start_time) * 1000
    query_count += 1

    # Log slow queries
    if duration_ms > slow_query_threshold_ms:
        logger.warning(
            "Slow query",
            extra={
                "duration_ms": round(duration_ms, 2),
                "query": statement[:500],  # Truncate long queries
                "params": str(parameters)[:200],
            }
        )

    # Emit metrics (e.g., to Datadog, Prometheus)
    # statsd.histogram("db.query.duration", duration_ms)
    # statsd.increment("db.query.count")
```

---

## Testing Database Code: pytest-postgresql and testcontainers

```python
# Option 1: pytest-postgresql — spins up a real Postgres process
# pip install pytest-postgresql

import pytest
from pytest_postgresql import factories

postgresql_proc = factories.postgresql_proc(port=None)  # Random port
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
def db_conn(postgresql):
    """Real Postgres connection for tests."""
    conn = psycopg2.connect(**postgresql.info.dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                amount_cents INTEGER,
                status TEXT
            )
        """)
    conn.autocommit = False
    yield conn
    conn.close()


def test_upsert_orders(db_conn):
    records = [{"order_id": "o1", "amount_cents": 999, "status": "pending"}]
    upsert_postgres(db_conn, records, "orders", conflict_cols=["order_id"])

    with db_conn.cursor() as cur:
        cur.execute("SELECT amount_cents FROM orders WHERE order_id = 'o1'")
        assert cur.fetchone()[0] == 999

    # Re-run same data (idempotency test)
    upsert_postgres(db_conn, records, "orders", conflict_cols=["order_id"])
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 1  # Still only one row


# Option 2: testcontainers — Docker-based, works for any DB (Postgres, MySQL, Snowflake mock)
# pip install testcontainers

from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:15") as pg:
        yield pg


@pytest.fixture
def engine(postgres_container):
    from sqlalchemy import create_engine
    engine = create_engine(postgres_container.get_connection_url())
    yield engine
    engine.dispose()
```

---

## Schema Migration with Alembic

Alembic manages schema changes for pipeline metadata tables (run logs, configs, DLQ).

```python
# alembic/versions/001_create_pipeline_runs.py

from alembic import op
import sqlalchemy as sa


def upgrade():
    """Create the pipeline_runs table."""
    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("pipeline_name", sa.String(256), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("rows_loaded", sa.Integer, server_default="0"),
        sa.Column("error_message", sa.Text),
    )
    op.create_index("ix_pipeline_runs_name_status", "pipeline_runs",
                    ["pipeline_name", "status"])


def downgrade():
    """Undo the migration (for rollback)."""
    op.drop_table("pipeline_runs")
```

```bash
# Run migrations in CI/CD or on pipeline startup
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# Generate a new migration from model changes
alembic revision --autogenerate -m "add rows_dropped column"
```

---

## Interview Tips

- **Little's Law** for pool sizing is a killer senior answer — interviewers rarely hear it and it shows you can reason about systems quantitatively rather than guessing.
- **Server-side cursors** are the answer to "how do you query a 100M-row table from Python without running out of memory?" — named cursors in psycopg2 (`conn.cursor("name")`) are the specific mechanism.
- Distinguish **pessimistic locking** (lock on read, good for high-contention updates) from **optimistic locking** (check version on write, good for low-contention; retry on conflict). Pick the right one for the scenario.
- For testing: `testcontainers` is the modern approach (Docker-based, any DB version, no local Postgres required). `pytest-postgresql` is lighter for Postgres-only projects.
- Alembic is specifically for metadata tables in pipelines — not for managing the warehouse schema (that's dbt's job).
