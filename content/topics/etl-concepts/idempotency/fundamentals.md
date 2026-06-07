---
title: "Idempotency - Fundamentals"
topic: etl-concepts
subtopic: idempotency
content_type: study_material
difficulty_level: beginner
layer: fundamentals
tags: [etl, idempotency, upsert, deduplication, exactly-once]
---

# Idempotency — Fundamentals

## What Is Idempotency?

An operation is **idempotent** if applying it multiple times produces the same result as applying it once.

```
f(f(x)) = f(x)
```

In data pipelines, idempotency means: **if the same pipeline run executes multiple times (due to retries or replays), the data in the target should be identical to running it once**.

```
Non-idempotent:  Run 1 → 100 rows. Run 2 (retry) → 200 rows (duplicates!).
Idempotent:      Run 1 → 100 rows. Run 2 (retry) → 100 rows (same result).
```

---

## Why Idempotency Matters

Without idempotency:
- **Retries cause duplicates**: Airflow retries a failed task → rows are inserted twice
- **Backfills corrupt data**: Re-running historical pipelines multiplies records
- **At-least-once delivery breaks**: Streaming systems may re-deliver messages → double-counting

With idempotency:
- Retries are safe — operators can click "retry" without data consequences
- Backfills work correctly
- On-call runbooks are simpler: "just retry the task"

---

## Idempotent Write Patterns

### Pattern 1: UPSERT (INSERT ... ON CONFLICT)

```sql
-- PostgreSQL: Insert or update on conflict
INSERT INTO orders (order_id, customer_id, status, total_usd, updated_at)
VALUES ('ORD-001', 'CUST-100', 'shipped', 99.99, '2024-01-15 10:00:00')
ON CONFLICT (order_id)
DO UPDATE SET
    status     = EXCLUDED.status,
    total_usd  = EXCLUDED.total_usd,
    updated_at = EXCLUDED.updated_at;
-- Running this 10 times produces exactly 1 row — idempotent!
```

```python
import pandas as pd
import sqlalchemy as sa

def upsert_dataframe(df: pd.DataFrame, table: str, conflict_col: str, engine):
    """Idempotent upsert using PostgreSQL ON CONFLICT."""
    # Build parameterized upsert
    cols       = list(df.columns)
    update_cols = [c for c in cols if c != conflict_col]

    placeholders = ", ".join(f":{c}" for c in cols)
    updates      = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col})
        DO UPDATE SET {updates}
    """
    with engine.begin() as conn:
        conn.execute(sa.text(sql), df.to_dict("records"))
```

### Pattern 2: DELETE + INSERT (Partition Replacement)

For batch loads, delete the partition being re-loaded and re-insert.

```python
def idempotent_partition_load(
    df: pd.DataFrame,
    table: str,
    partition_col: str,
    partition_value: str,
    engine
):
    """
    Atomically replace a partition. Safe to run multiple times.
    """
    with engine.begin() as conn:
        # Delete the partition (idempotent — same delete result every time)
        conn.execute(sa.text(f"""
            DELETE FROM {table}
            WHERE {partition_col} = :partition
        """), {"partition": partition_value})

        # Re-insert the data
        df[partition_col] = partition_value
        df.to_sql(table, conn, if_exists="append", index=False)
```

### Pattern 3: CREATE OR REPLACE (Warehouse-Level)

```sql
-- BigQuery / Snowflake: Replace an entire table or partition atomically
CREATE OR REPLACE TABLE `project.dataset.orders_2024_01_15`
AS
SELECT *
FROM `project.raw.orders`
WHERE DATE(created_at) = '2024-01-15';
-- Running twice = same result. The second run replaces the first.
```

### Pattern 4: MERGE Statement

```sql
-- Snowflake MERGE: handles insert, update, and delete idempotently
MERGE INTO target_orders AS tgt
USING staging_orders AS src
    ON tgt.order_id = src.order_id
WHEN MATCHED THEN
    UPDATE SET
        status    = src.status,
        total_usd = src.total_usd
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, status, total_usd)
    VALUES (src.order_id, src.customer_id, src.status, src.total_usd)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;   -- Remove rows not in source (optional)
```

---

## Deterministic IDs

A common source of non-idempotency is using auto-increment IDs or UUID4 for surrogate keys — each run generates new IDs for the same logical record.

```python
import hashlib

def generate_deterministic_id(*args) -> str:
    """
    Generate a deterministic surrogate key from business key components.
    Same business key → same ID, every time.
    """
    key = "|".join(str(a) for a in args)
    return hashlib.md5(key.encode()).hexdigest()

# Example: surrogate key for an order
order_id = generate_deterministic_id("ORD-12345", "2024-01-15", "cust-456")
# Always produces "8f14e45f..." for these inputs

# Contrast with:
import uuid
random_id = str(uuid.uuid4())  # Different every time! NOT idempotent!
```

---

## Idempotent Airflow DAGs

Airflow DAGs should be idempotent: re-running a DAG for a specific `execution_date` should produce the same result.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_daily_orders(ds: str, **context):
    """
    Idempotent: uses 'ds' (execution date) to scope the operation.
    Re-running for the same 'ds' produces the same data.
    """
    # DELETE the partition for this date
    engine.execute(f"DELETE FROM orders WHERE order_date = '{ds}'")

    # Re-extract and re-load
    df = extract_orders_for_date(ds)
    df.to_sql("orders", engine, if_exists="append", index=False)

with DAG(
    "daily_orders_load",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,   # Catchup is safe because the task is idempotent
) as dag:

    load_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_daily_orders,
        provide_context=True,
    )
```

**Key principle**: Use `{{ ds }}` (the execution date, not `NOW()`) as the scope for all operations. `NOW()` produces different results on every run; `{{ ds }}` is stable.

---

## Common Idempotency Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `INSERT INTO ... SELECT` without dedup | Duplicates on retry | Use UPSERT or DELETE+INSERT |
| `uuid.uuid4()` as surrogate key | New ID every run | Use deterministic hash of business key |
| `WHERE updated_at > NOW() - 1 hour` | Moving window changes each run | Use `WHERE updated_at > :fixed_hwm` |
| `CURRENT_TIMESTAMP` in partition | New partition each retry | Use execution date from scheduler |
| Append to existing partition | Grows on retry | Replace partition atomically |

---

## Interview Tips

> **Tip 1:** Idempotency is one of the most important properties in pipeline design. Frame your answer around retries: "If Airflow retries this task at 3 AM, will we get duplicates?" If yes, the design is flawed.

> **Tip 2:** The three main idempotent write patterns are UPSERT, partition replacement (DELETE + INSERT), and CREATE OR REPLACE. Know when to use each based on data volume and target system capabilities.

> **Tip 3:** Deterministic IDs (hash of business key) are essential for idempotency when using surrogate keys. Auto-increment and UUID4 both break idempotency.

> **Tip 4:** In Airflow, always scope operations to the `execution_date` (`{{ ds }}`), never to `NOW()`. The execution date is stable across retries; NOW() is not.

> **Tip 5:** Partition replacement (DELETE + INSERT in a transaction) is often simpler and faster than MERGE for large daily partitions. It avoids the complexity of MERGE while being fully idempotent.
