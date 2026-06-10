---
title: "Idempotency - Senior Deep Dive"
topic: etl-concepts
subtopic: idempotency
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [etl, idempotency, exactly-once, distributed, delta-lake, two-phase-commit]
---

# Idempotency — Senior Deep Dive

## Exactly-Once Processing in Distributed Systems

True exactly-once is impossible without distributed coordination. In practice, we achieve it through **idempotent writes + at-least-once delivery**.

### The Fallacy of "Exactly-Once"

```
Claim: "Our message queue delivers exactly-once"
Reality: Under failures (network partitions, crashes), the queue delivers at-least-once.
         Exactly-once semantics are achieved by making the consumer idempotent.
```

The correct framing: **at-least-once delivery + idempotent consumer = exactly-once observable effect**.

### Distributed Idempotency with Optimistic Locking

```python
from sqlalchemy import Column, String, Integer, DateTime, JSON
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key          = Column(String, primary_key=True)
    pipeline     = Column(String, nullable=False)
    status       = Column(String, nullable=False)  # "processing", "completed", "failed"
    result       = Column(JSON)
    created_at   = Column(DateTime)
    completed_at = Column(DateTime)

class DistributedIdempotencyGuard:
    """
    Prevents concurrent processing of the same logical operation
    across multiple workers using a database-backed lock.
    """
    def __init__(self, session: Session):
        self.session = session

    def execute_once(self, key: str, pipeline: str, fn, *args, **kwargs):
        """
        Execute fn exactly once for the given key.
        Concurrent calls with the same key block until the first completes.
        """
        # Try to claim the key (INSERT fails if already exists)
        try:
            idem_key = IdempotencyKey(
                key=key,
                pipeline=pipeline,
                status="processing",
                created_at=datetime.utcnow()
            )
            self.session.add(idem_key)
            self.session.flush()  # Raises IntegrityError if key exists
        except IntegrityError:
            self.session.rollback()
            # Key exists — check if already completed
            existing = self.session.query(IdempotencyKey).filter_by(key=key).one()

            if existing.status == "completed":
                return existing.result  # Return cached result
            elif existing.status == "processing":
                raise RuntimeError(f"Key {key} is being processed by another worker")
            else:
                raise RuntimeError(f"Key {key} previously failed: {existing.result}")

        # We own the key — execute the function
        try:
            result = fn(*args, **kwargs)
            idem_key.status       = "completed"
            idem_key.result       = result
            idem_key.completed_at = datetime.utcnow()
            self.session.commit()
            return result
        except Exception as e:
            idem_key.status = "failed"
            idem_key.result = {"error": str(e)}
            self.session.commit()
            raise
```

---

## Idempotent Delta Lake Operations

Delta Lake provides ACID transactions and native support for idempotent writes.

### Application-Level Transaction IDs

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

def idempotent_delta_write(
    df: DataFrame,
    table_path: str,
    application_id: str,    # Unique per logical operation
    user_metadata: str = ""
):
    """
    Write to Delta Lake with application-level idempotency.
    If the same application_id is used twice, the second write is a no-op.
    """
    # Check if this application_id was already committed
    history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
    already_committed = (
        history
        .filter(f"operationParameters.appId = '{application_id}'")
        .count() > 0
    )

    if already_committed:
        print(f"Application ID {application_id} already committed. Skipping.")
        return

    # Write with application ID in operation metadata
    (
        df.write
        .format("delta")
        .mode("append")
        .option("appId", application_id)
        .option("userMetadata", user_metadata)
        .save(table_path)
    )
    print(f"Committed with appId: {application_id}")
```

### Delta MERGE Idempotency

```python
def idempotent_delta_merge(
    spark,
    new_data: DataFrame,
    target_path: str,
    merge_key: str,
    transaction_id: str
):
    """
    Idempotent Delta MERGE using transaction IDs to prevent double-execution.
    """
    target = DeltaTable.forPath(spark, target_path)

    # Check if this transaction was already applied
    history = target.history()
    if history.filter(f"userMetadata = '{transaction_id}'").count() > 0:
        print(f"Transaction {transaction_id} already applied. Idempotent skip.")
        return 0

    (
        target.alias("tgt")
        .merge(new_data.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Record transaction ID in history (via operation metadata)
    spark.sql(f"""
        ALTER TABLE delta.`{target_path}`
        SET TBLPROPERTIES ('lastTransactionId' = '{transaction_id}')
    """)
```

---

## Idempotency in Airflow at Scale

### Run-ID Based Scoping

```python
from airflow.decorators import task, dag
from airflow.models import Variable
from datetime import datetime

@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Safe because tasks are idempotent
    max_active_runs=1,
)
def orders_pipeline():

    @task
    def extract(ds: str, run_id: str) -> str:
        """Extract scoped to execution date — idempotent."""
        # Use ds (execution date) not NOW() for scoping
        output_path = f"s3://staging/{ds}/orders_{run_id}.parquet"

        if s3_exists(output_path):
            print(f"Extract already done for {ds}. Using cached file.")
            return output_path

        df = extract_orders_for_date(ds)
        df.to_parquet(output_path)
        return output_path

    @task
    def load(staging_path: str, ds: str):
        """Load idempotently via partition replacement."""
        df = pd.read_parquet(staging_path)

        with engine.begin() as conn:
            # Delete this partition (idempotent — same result every run)
            conn.execute(sa.text(
                "DELETE FROM orders WHERE order_date = :d"
            ), {"d": ds})
            # Re-insert
            df.to_sql("orders", conn, if_exists="append", index=False)

        print(f"Loaded {len(df)} rows for {ds}")

    path = extract()
    load(path)

dag_instance = orders_pipeline()
```

---

## Idempotency Testing

Testing idempotency is as important as implementing it.

```python
import pytest
import pandas as pd
from sqlalchemy import create_engine

@pytest.fixture
def test_engine():
    engine = create_engine("sqlite:///:memory:")
    # Create test tables
    engine.execute("""
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            status   TEXT,
            amount   REAL
        )
    """)
    return engine

def test_upsert_idempotency(test_engine):
    """Verify that running upsert N times produces the same result as running once."""
    df = pd.DataFrame([
        {"order_id": "ORD-001", "status": "pending", "amount": 99.99},
        {"order_id": "ORD-002", "status": "shipped", "amount": 49.99},
    ])

    # Run the upsert 3 times
    for _ in range(3):
        upsert_dataframe(df, "orders", "order_id", test_engine)

    # Verify exactly 2 rows (not 6)
    result = pd.read_sql("SELECT * FROM orders", test_engine)
    assert len(result) == 2, f"Expected 2 rows, got {len(result)} — idempotency violated!"
    assert set(result["order_id"]) == {"ORD-001", "ORD-002"}

def test_partition_replace_idempotency(test_engine):
    """Verify partition replacement is idempotent."""
    date = "2024-01-15"
    df   = pd.DataFrame([
        {"order_id": "ORD-001", "order_date": date, "amount": 99.99},
    ])

    # Run 5 times
    for _ in range(5):
        idempotent_partition_load(df, "orders", "order_date", date, test_engine)

    result = pd.read_sql(
        f"SELECT * FROM orders WHERE order_date = '{date}'", test_engine
    )
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"

def test_seen_events_prevents_duplicates(test_engine):
    """Verify the seen-events store prevents double-processing."""
    seen_store = SeenEventsStore(test_engine)
    event      = {"id": "evt-abc123", "order_id": "ORD-001", "amount": 99.99}

    processed_count = 0

    def process(event):
        nonlocal processed_count
        if not process_event_idempotently(event, seen_store, test_engine):
            return  # Duplicate — skipped
        processed_count += 1

    # "Deliver" same event 5 times (simulates at-least-once delivery)
    for _ in range(5):
        process(event)

    assert processed_count == 1, f"Expected 1 processing, got {processed_count}"
```

---

## Idempotency Matrix: Design Guide

| Pipeline Type | Write Strategy | Scope Key | Dedup Mechanism |
|---|---|---|---|
| Daily batch to DW | Partition replace | `execution_date` | DELETE + INSERT in transaction |
| Streaming to PostgreSQL | UPSERT | Event business key | ON CONFLICT DO UPDATE |
| Streaming to Delta Lake | Delta MERGE | Entity PK | MERGE whenMatchedUpdateAll |
| Event log (append-only) | Append + ON CONFLICT DO NOTHING | event_id | Skip insert on duplicate |
| API response caching | CREATE OR REPLACE | Request hash | Table replace |
| Multi-step pipeline | Idempotency key table | run_id + step | Check before execute |

---

## Interview Tips

> **Tip 1:** Challenge the claim "our system delivers exactly-once." Explain that distributed systems can only achieve at-least-once delivery under failures, and that exactly-once effects require idempotent consumers — the two combined approximate exactly-once observable behavior.

> **Tip 2:** Delta Lake's application ID feature (`appId` write option) enables idempotency at the storage layer by recording which logical operations have been committed. This is the cleanest idempotency mechanism for Spark pipelines.

> **Tip 3:** Testing idempotency explicitly (running the same operation N times and asserting the result equals running it once) is rare but highly valued in interviews. It shows rigorous engineering thinking.

> **Tip 4:** The distributed idempotency guard pattern (using a database-backed lock table with "processing"/"completed"/"failed" states) is the production-grade approach for multi-worker pipelines where the same task might be dispatched to multiple executors.

> **Tip 5:** Always distinguish between **producer idempotency** (Kafka `enable.idempotence`) and **consumer idempotency** (UPSERT + seen-events). Both are needed for end-to-end exactly-once; one without the other is insufficient.

## ⚡ Cheat Sheet

**ETL vs ELT**
```
ETL: transform before loading → good for strict schema targets (DW)
ELT: load raw then transform → good for data lakes (Spark/dbt on raw data)
Modern default: ELT (storage cheap; compute on demand; raw data preserved)
```

**Idempotency patterns**
```python
# Write-if-not-exists (partition-level)
if not partition_exists(output_path, date=run_date):
    write_partition(data, output_path, date=run_date)

# Overwrite idempotent partition (Delta)
df.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"dt = '{run_date}'").save(path)

# Watermark-based incremental load
SELECT * FROM source WHERE updated_at > (SELECT MAX(updated_at) FROM target)
```

**CDC (Change Data Capture) patterns**
```
Log-based CDC: reads DB transaction log (Debezium → Kafka → Lakehouse)
  + Low impact on source DB
  + Captures deletes + updates
Query-based:   polls source table for new/changed rows (watermark)
  - Misses deletes; higher DB load
  
Debezium event fields: op (c=create, u=update, d=delete, r=read/snapshot)
                        before, after, source metadata
```

**Backfill strategy**
```python
# Generate backfill date range
from datetime import date, timedelta
backfill_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

# Run in parallel (limit concurrency to avoid source DB overload)
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as pool:
    pool.map(run_etl_for_date, backfill_dates)
```

**SCD2 (dbt snapshot)**
```yaml
# snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['name', 'city', 'email'],
        invalidate_hard_deletes=True,
    )
}}
SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```

**Batch vs Streaming**
| Dimension | Batch | Streaming |
|---|---|---|
| Latency | Minutes to hours | Sub-second to minutes |
| Throughput | High (bulk) | Lower per event |
| Complexity | Lower | Higher |
| Use case | Daily reports, DW loads | Fraud detection, live dashboards |

**Pipeline design patterns**
```
Fan-out:    one source → multiple downstream consumers
Fan-in:     multiple sources → one joined output
Watermark:  track max processed timestamp; resume from watermark
Dead letter: failed records → separate queue for inspection/retry
Circuit breaker: stop pipeline on DQ failure; alert + wait for fix
```

**Error handling**
```python
try:
    process(record)
except ValidationError as e:
    dead_letter_queue.append({"record": record, "error": str(e), "ts": now()})
    metrics.increment("dead_letter_count")
except RetryableError as e:
    retry_queue.append({"record": record, "retry_count": retry_count + 1})
except Exception as e:
    alert_oncall(f"Unexpected error: {e}"); raise
```

**Data reconciliation**
```sql
-- Row count comparison
SELECT 'source' AS src, COUNT(*) FROM source.orders WHERE date = '2024-01-15'
UNION ALL
SELECT 'target', COUNT(*) FROM gold.orders WHERE dt = '2024-01-15';

-- Sum comparison
SELECT ABS(s.total - t.total) AS discrepancy
FROM (SELECT SUM(amount) AS total FROM source.orders WHERE date = '2024-01-15') s
CROSS JOIN (SELECT SUM(amount) AS total FROM gold.orders WHERE dt = '2024-01-15') t;
```
