---
title: "SQL and Databases - Scenario Questions"
topic: python
subtopic: sql-and-databases
content_type: scenario_question
tags: [python, sql, psycopg2, parameterized-queries, bulk-upsert, connection-pooling, snowflake]
---

# Scenario Questions — SQL and Databases

<article data-difficulty="junior">

## 🟢 Junior: Safe Parameterized Query Returning Dicts

**Scenario:** Write a Python function `query_orders(status: str, min_amount_cents: int) -> list[dict]` that queries a Postgres `orders` table and returns results as a list of dictionaries. The function must use parameterized queries (no f-strings in SQL), handle the case where no rows are returned, and close the connection cleanly even if an error occurs.

<details>
<summary>✅ Solution</summary>

```python
import psycopg2
import psycopg2.extras
import os
from typing import Any


DB_URL = os.environ["DATABASE_URL"]  # "postgresql://user:pass@host:5432/db"


def query_orders(status: str, min_amount_cents: int) -> list[dict]:
    """
    Safely query orders by status and minimum amount.

    Returns a list of dicts — e.g.:
    [{"order_id": "o1", "amount_cents": 999, "status": "completed"}, ...]

    Empty list if no rows match.
    """
    # ✅ Parameterized query — %s placeholders, values in separate tuple
    # psycopg2 escapes all values; user input can NEVER become SQL code
    sql = """
        SELECT
            order_id,
            customer_id,
            amount_cents,
            status,
            created_at
        FROM orders
        WHERE status = %s
          AND amount_cents >= %s
        ORDER BY created_at DESC
    """

    conn = None  # Initialize to None so finally block is safe
    try:
        conn = psycopg2.connect(DB_URL)
        # RealDictCursor: rows returned as real dicts (not tuples)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (status, min_amount_cents))  # Values as a tuple
            rows = cur.fetchall()
            # fetchall() returns [] if no rows match — not None
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        # Log the error (in production, use structured logging)
        print(f"Database error in query_orders: {e}")
        raise  # Re-raise — callers should know the query failed
    finally:
        if conn is not None:
            conn.close()  # ALWAYS runs: success, error, or exception


# Test the function
results = query_orders(status="completed", min_amount_cents=1000)
print(f"Found {len(results)} orders")
for order in results[:3]:
    print(order)
# [{"order_id": "o_001", "customer_id": "c_042", "amount_cents": 2599,
#   "status": "completed", "created_at": datetime(2024, 1, 15, ...)}]


# ── What NOT to do ────────────────────────────────────────────────────────

def query_orders_UNSAFE(status: str, min_amount_cents: int) -> list[dict]:
    """❌ NEVER do this — SQL injection vulnerability."""
    sql = f"""
        SELECT * FROM orders
        WHERE status = '{status}'  -- String interpolation in SQL!
          AND amount_cents >= {min_amount_cents}
    """
    # If status = "' OR '1'='1" the WHERE clause becomes always-true
    # Returns every row in the database
    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute(sql)  # ← DANGEROUS
        return cur.fetchall()
    # Also: conn is never closed if fetchall() raises an exception!


# ── Improved version using context managers ────────────────────────────────

from contextlib import contextmanager

@contextmanager
def db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()


def query_orders_clean(status: str, min_amount_cents: int) -> list[dict]:
    """Cleaner version using context manager — no explicit finally needed."""
    sql = """
        SELECT order_id, customer_id, amount_cents, status, created_at
        FROM orders
        WHERE status = %s AND amount_cents >= %s
        ORDER BY created_at DESC
    """
    with db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (status, min_amount_cents))
            return [dict(row) for row in cur.fetchall()]
```

**Why `RealDictCursor`?** The default cursor returns tuples like `(1, "Alice", 999, "completed")`. You'd need to manually zip with column names to get a dict. `RealDictCursor` returns `{"order_id": 1, "customer_id": "Alice", ...}` directly — safer and more readable.

**Why `conn = None` before `try`?** If `psycopg2.connect()` itself raises an exception (e.g., wrong host), `conn` was never assigned. Without the `None` initialization, the `finally` block would raise `NameError: name 'conn' is not defined`, hiding the original connection error.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Bulk Upsert for 1M Rows into Postgres

**Scenario:** You need to load 1 million order records into a Postgres `orders` table with columns `(order_id TEXT PRIMARY KEY, customer_id TEXT, amount_cents INTEGER, status TEXT, updated_at TIMESTAMP)`. The records may already exist (updates are common). Implement a bulk upsert function that: handles conflicts by updating existing rows, uses batched inserts for memory efficiency, and is measurably faster than row-by-row insertion. Include a basic benchmark.

<details>
<summary>✅ Solution</summary>

```python
import psycopg2
import psycopg2.extras
import time
import os
from typing import Iterator


DB_URL = os.environ["DATABASE_URL"]


def create_orders_table(conn):
    """Ensure the table exists with the right schema."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id    TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders(customer_id)")
    conn.commit()


def bulk_upsert_orders(records: list[dict], conn, batch_size: int = 10_000) -> int:
    """
    Upsert records into the orders table using ON CONFLICT DO UPDATE.
    Idempotent: running with the same records twice produces one row per order_id.

    Uses execute_values for multi-row INSERT — much faster than executemany.
    Returns total rows upserted.
    """
    if not records:
        return 0

    # ON CONFLICT clause: update all columns except the primary key on conflict
    upsert_sql = """
        INSERT INTO orders (order_id, customer_id, amount_cents, status, updated_at)
        VALUES %s
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id  = EXCLUDED.customer_id,
            amount_cents = EXCLUDED.amount_cents,
            status       = EXCLUDED.status,
            updated_at   = NOW()
    """

    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            values = [
                (r["order_id"], r["customer_id"], r["amount_cents"], r["status"])
                for r in batch
            ]
            psycopg2.extras.execute_values(
                cur,
                upsert_sql,
                values,
                template=None,
                page_size=batch_size,  # All values in one INSERT statement
            )
            total += len(batch)
    conn.commit()
    return total


# ── Benchmark: compare approaches ─────────────────────────────────────────

def generate_records(n: int) -> list[dict]:
    """Generate N synthetic order records."""
    import random
    statuses = ["pending", "completed", "cancelled"]
    return [
        {
            "order_id": f"order_{i:07d}",
            "customer_id": f"cust_{random.randint(1, 10000):05d}",
            "amount_cents": random.randint(100, 100_000),
            "status": random.choice(statuses),
        }
        for i in range(n)
    ]


def benchmark():
    N = 100_000  # 100K records for the benchmark
    records = generate_records(N)
    conn = psycopg2.connect(DB_URL)
    create_orders_table(conn)

    # Method 1: Row-by-row execute (baseline — the worst approach)
    conn.execute("TRUNCATE orders")
    start = time.time()
    with conn.cursor() as cur:
        for r in records[:10_000]:  # Only 10K — would take too long for 100K
            cur.execute(
                "INSERT INTO orders (order_id, customer_id, amount_cents, status) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status",
                (r["order_id"], r["customer_id"], r["amount_cents"], r["status"])
            )
    conn.commit()
    row_by_row_time = time.time() - start
    extrapolated_100k = row_by_row_time * 10
    print(f"Row-by-row (10K, extrapolated to 100K): {extrapolated_100k:.1f}s")

    # Method 2: execute_values in batches of 10K (the right approach)
    conn.execute("TRUNCATE orders")
    conn.commit()
    start = time.time()
    total = bulk_upsert_orders(records, conn, batch_size=10_000)
    execute_values_time = time.time() - start
    print(f"execute_values ({total:,} rows): {execute_values_time:.2f}s "
          f"({total/execute_values_time:,.0f} rows/sec)")

    # Typical results on a local Postgres:
    # Row-by-row (10K, extrapolated): ~90s for 100K
    # execute_values (100K):           ~1.2s for 100K  (~75x faster)

    conn.close()


# ── For 1M rows: stream in chunks from source ─────────────────────────────

def load_1m_orders(records_source: Iterator[dict]) -> int:
    """Stream 1M records in 10K-row batches. Memory: O(batch_size), not O(1M)."""
    conn = psycopg2.connect(DB_URL)
    create_orders_table(conn)
    total = 0
    batch = []

    for record in records_source:
        batch.append(record)
        if len(batch) >= 10_000:
            total += bulk_upsert_orders(batch, conn, batch_size=10_000)
            batch = []
            print(f"Loaded {total:,} records...")

    if batch:
        total += bulk_upsert_orders(batch, conn)

    conn.close()
    return total


if __name__ == "__main__":
    benchmark()
```

**Performance expectations for 1M rows:**
- Row-by-row: ~900 seconds (15 minutes)
- `executemany`: ~60-90 seconds
- `execute_values` (10K batches): ~12-15 seconds
- `COPY FROM`: ~2-4 seconds (for new inserts only — no conflict handling)

**When to use COPY instead:** If you're doing a clean initial load (no conflicts), use `COPY FROM STDIN` via `cur.copy_from()` — it's 5-10x faster than `execute_values`. For upserts with conflict handling, `execute_values` + `ON CONFLICT` is the best available.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Connection Management Layer for 50 Concurrent Snowflake Workers

**Scenario:** Your pipeline runs 50 concurrent workers, each executing queries against Snowflake. Design a connection management layer that handles: (1) connection pool exhaustion when all 50 workers need a connection simultaneously, (2) automatic token refresh when JWT tokens expire after 60 minutes, (3) query timeout enforcement so a single slow query can't block a worker indefinitely, and (4) graceful degradation when Snowflake is unavailable. Explain your architecture and implement the key components.

<details>
<summary>✅ Solution</summary>

```python
import threading
import time
import queue
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)


# ── Configuration ──────────────────────────────────────────────────────────

@dataclass
class SnowflakeConfig:
    account: str
    user: str
    private_key_path: str
    warehouse: str
    database: str
    schema: str
    pool_size: int = 10          # Max simultaneous connections
    acquire_timeout: float = 30.0  # Seconds to wait for a free connection
    query_timeout: int = 300       # Seconds before a query is killed
    token_ttl: float = 3000.0      # Refresh connection after 50 minutes (JWT expires at 60)


# ── Connection wrapper with health tracking ────────────────────────────────

@dataclass
class ManagedConnection:
    conn: object
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    query_count: int = 0

    def is_expired(self, ttl: float) -> bool:
        return (time.time() - self.created_at) >= ttl

    def is_healthy(self) -> bool:
        """Quick health check — run a 1ms query."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False


# ── Connection Pool ────────────────────────────────────────────────────────

class SnowflakeConnectionPool:
    """
    Thread-safe Snowflake connection pool with:
    - Bounded size (prevents overwhelming Snowflake credit consumption)
    - JWT token refresh (reconnect before token expires)
    - Health checking on checkout (prevent stale connection errors)
    - Acquire timeout (fail fast instead of blocking indefinitely)
    """

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._pool: queue.Queue = queue.Queue(maxsize=config.pool_size)
        self._lock = threading.Lock()
        self._total_connections = 0
        self._metrics = {
            "pool_hits": 0,         # Reused existing connection
            "new_connections": 0,   # Created a new connection
            "timeouts": 0,          # acquire_timeout exceeded
            "health_failures": 0,   # Connection failed health check
            "token_refreshes": 0,   # Connection replaced due to token expiry
        }
        # Pre-warm pool with initial connections
        self._warm_pool(min(3, config.pool_size))

    def _make_connection(self) -> ManagedConnection:
        """Create a fresh Snowflake connection."""
        conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            private_key_file=self.config.private_key_path,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            # Server-side query timeout — Snowflake cancels the query automatically
            network_timeout=self.config.query_timeout,
            session_parameters={
                "STATEMENT_TIMEOUT_IN_SECONDS": str(self.config.query_timeout),
                "QUERY_TAG": "pipeline_worker",
            },
            login_timeout=20,
        )
        with self._lock:
            self._total_connections += 1
            self._metrics["new_connections"] += 1
        logger.info(f"New Snowflake connection created (total: {self._total_connections})")
        return ManagedConnection(conn=conn)

    def _warm_pool(self, count: int):
        """Pre-create connections to avoid cold-start latency."""
        for _ in range(count):
            try:
                self._pool.put_nowait(self._make_connection())
            except Exception as e:
                logger.warning(f"Pool warm-up connection failed: {e}")

    @contextmanager
    def acquire(self):
        """
        Context manager: check out a connection, return it on exit.
        Handles: pool exhaustion, token expiry, health failures, query timeout.
        """
        managed_conn = None
        start = time.time()

        while True:
            # ── Try to get a connection from the pool ─────────────────────
            try:
                managed_conn = self._pool.get(timeout=1.0)
            except queue.Empty:
                # Pool empty — can we create a new one?
                with self._lock:
                    can_create = self._total_connections < self.config.pool_size

                if can_create:
                    try:
                        managed_conn = self._make_connection()
                    except Exception as e:
                        logger.error(f"Failed to create Snowflake connection: {e}")
                        if time.time() - start >= self.config.acquire_timeout:
                            self._metrics["timeouts"] += 1
                            raise TimeoutError(
                                f"Could not acquire Snowflake connection after "
                                f"{self.config.acquire_timeout}s (pool={self.config.pool_size}, "
                                f"active={self._total_connections})"
                            )
                        time.sleep(0.5)
                        continue
                else:
                    # At max connections — wait for one to be returned
                    if time.time() - start >= self.config.acquire_timeout:
                        self._metrics["timeouts"] += 1
                        raise TimeoutError(
                            f"All {self.config.pool_size} Snowflake connections busy "
                            f"after {self.config.acquire_timeout}s. "
                            f"Increase pool_size or reduce worker concurrency."
                        )
                    time.sleep(0.2)
                    continue

            # ── Validate the connection ────────────────────────────────────
            # Token expiry check: replace connection before JWT expires
            if managed_conn.is_expired(self.config.token_ttl):
                logger.info("Snowflake connection token near expiry — refreshing")
                try:
                    managed_conn.conn.close()
                except Exception:
                    pass
                with self._lock:
                    self._total_connections -= 1
                    self._metrics["token_refreshes"] += 1
                managed_conn = self._make_connection()

            # Health check: detect dead connections (network blip, warehouse suspended)
            if not managed_conn.is_healthy():
                logger.warning("Unhealthy Snowflake connection detected — replacing")
                try:
                    managed_conn.conn.close()
                except Exception:
                    pass
                with self._lock:
                    self._total_connections -= 1
                    self._metrics["health_failures"] += 1
                managed_conn = self._make_connection()

            # ── Connection is valid — yield it ────────────────────────────
            self._metrics["pool_hits"] += 1
            managed_conn.last_used_at = time.time()
            managed_conn.query_count += 1

            try:
                yield managed_conn.conn
            finally:
                # Always return to pool (or discard if connection is broken)
                try:
                    managed_conn.conn.rollback()  # Ensure clean state
                    self._pool.put_nowait(managed_conn)
                except Exception:
                    # Connection is broken — discard it
                    with self._lock:
                        self._total_connections -= 1
                    logger.warning("Discarded broken Snowflake connection")
            break  # Exit the while loop

    def get_metrics(self) -> dict:
        return {
            **self._metrics,
            "pool_size": self.config.pool_size,
            "total_connections": self._total_connections,
            "available": self._pool.qsize(),
        }

    def close_all(self):
        """Drain and close all connections (call on pipeline shutdown)."""
        while not self._pool.empty():
            try:
                managed = self._pool.get_nowait()
                managed.conn.close()
            except (queue.Empty, Exception):
                pass


# ── Circuit Breaker for Snowflake Unavailability ──────────────────────────

class SnowflakeCircuitBreaker:
    """Open circuit after N consecutive failures — fast-fail instead of hammering Snowflake."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._opened_at: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return False
            if time.time() - self._opened_at >= self.recovery_timeout:
                # Try half-open
                self._opened_at = None
                self._failures = 0
                return False
            return True

    def record_success(self):
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self):
        with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._opened_at = time.time()
                logger.error(f"Snowflake circuit OPENED after {self._failures} failures")


# ── High-Level Client used by Pipeline Workers ────────────────────────────

class SnowflakePipelineClient:
    """
    The interface pipeline workers use. Hides all connection management.
    Workers just call execute_query() — no connection lifecycle to manage.
    """

    def __init__(self, config: SnowflakeConfig):
        self.pool = SnowflakeConnectionPool(config)
        self.circuit = SnowflakeCircuitBreaker()
        self.config = config

    def execute_query(self, sql: str, params: dict = None) -> list[dict]:
        """Execute a parameterized query, return list of dicts."""
        if self.circuit.is_open:
            raise RuntimeError("Snowflake circuit breaker is OPEN — fast-failing")

        try:
            with self.pool.acquire() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute(sql, params or {})
                    result = cur.fetchall()
            self.circuit.record_success()
            return result
        except TimeoutError:
            # Pool exhausted — don't count as circuit failure (Snowflake is fine)
            raise
        except Exception as e:
            self.circuit.record_failure()
            logger.error(f"Snowflake query failed: {e}")
            raise

    def shutdown(self):
        self.pool.close_all()
        logger.info(f"Snowflake pool metrics: {self.pool.get_metrics()}")


# ── Worker usage ──────────────────────────────────────────────────────────

config = SnowflakeConfig(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_path=os.environ["SNOWFLAKE_KEY_PATH"],
    warehouse="COMPUTE_WH",
    database="ANALYTICS",
    schema="PUBLIC",
    pool_size=15,           # 50 workers / avg 3 queries in-flight per worker = ~15
    acquire_timeout=30.0,
    query_timeout=300,
    token_ttl=3000.0,       # Refresh at 50 min (before 60-min JWT expiry)
)

client = SnowflakePipelineClient(config)

# 50 workers all share the same client (and thus the same pool)
def worker_function(partition_key: str):
    results = client.execute_query(
        "SELECT * FROM fact_orders WHERE partition_key = %(pk)s",
        {"pk": partition_key}
    )
    process(results)

from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=50) as executor:
    executor.map(worker_function, partition_keys)

client.shutdown()
```

**Architecture decisions to explain in the interview:**

1. **Pool size = 15, not 50:** Applying Little's Law — if each worker's query takes ~300ms and there are 50 workers, average concurrent DB connections = 50 × 0.3 = 15. Over-provisioning to 50 wastes Snowflake credits and can trigger warehouse scaling.

2. **Token refresh at 50 minutes, not 60:** JWT tokens expire at 60 minutes. Refreshing at 50 minutes gives a 10-minute safety margin. Connecting fresh at the start of each batch (50K rows) amortizes the ~200ms reconnect cost.

3. **Circuit breaker on the client, not the pool:** Pool exhaustion is an application-side issue (fix: increase pool_size or reduce concurrency). Snowflake unavailability is a service-side issue — the circuit breaker handles it by fast-failing all 50 workers instead of having them queue and time out one by one.

4. **`STATEMENT_TIMEOUT_IN_SECONDS` on the Snowflake session:** This is a server-side timeout. Even if the Python thread dies or the network hangs, Snowflake will cancel the query server-side — preventing long-running queries from consuming Snowflake credits and blocking workers indefinitely.

5. **Graceful degradation:** `TimeoutError` from pool exhaustion is different from Snowflake being down. Workers can send timed-out batches to a DLQ for retry rather than crashing the entire pipeline.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `fetchall()`, `fetchone()`, and `fetchmany()` in Python's DB-API?**
A: `fetchall()` loads the entire result set into a Python list in memory — dangerous for large queries. `fetchone()` returns one row (or None) and advances the cursor pointer. `fetchmany(n)` returns the next N rows. For large result sets, iterate the cursor directly (which uses `fetchmany()` internally) or use a server-side named cursor to avoid loading the full result into Python memory.

**Q: What is `ON CONFLICT DO UPDATE` and when do you use it?**
A: It is Postgres's upsert syntax (INSERT + UPDATE on conflict). When you try to insert a row whose primary key or unique constraint already exists, instead of raising an error, Postgres updates the conflicting row with the new values. You use it for idempotent loads — running the same pipeline twice must not create duplicate rows. The pattern is essential in ETL when pipelines retry on failure.

**Q: Why is `pool_pre_ping=True` important in SQLAlchemy?**
A: Database servers close idle connections after a timeout (Postgres: 10 minutes by default, RDS: 8 hours). Without `pool_pre_ping`, SQLAlchemy may hand your code a connection that was closed on the server side, causing a cryptic error mid-query. With `pool_pre_ping=True`, SQLAlchemy executes `SELECT 1` before each checkout — if it fails, it discards the dead connection and creates a fresh one transparently.

**Q: What is a server-side cursor and when do you need it?**
A: A server-side (named) cursor keeps the query result on the database server and streams rows to Python in configurable batches. You need it when querying tables too large to fit in memory — for example, `SELECT * FROM fact_events` with 100M rows would OOM with a client-side cursor (`fetchall()`), but with a named cursor you process 10K rows at a time with constant memory. In psycopg2: `conn.cursor("named_cursor")` creates a server-side cursor.
