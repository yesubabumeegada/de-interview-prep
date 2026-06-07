---
title: "Idempotency - Real World"
topic: etl-concepts
subtopic: idempotency
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, idempotency, production, case-study, airflow, delta-lake]
---

# Idempotency — Real World

## Case Study 1: Double-Charging Bug from Non-Idempotent Payment Pipeline

### Problem

A fintech company's payment processing pipeline used `INSERT INTO payments SELECT ...` without any deduplication. When Airflow retried a failed task (network timeout), the same payments were inserted twice. Some customers were double-charged.

### Root Cause Analysis

```python
# OLD CODE — NOT idempotent
def load_payments(date: str, engine):
    df = extract_payments(date)
    # This APPENDS to the table on every run!
    df.to_sql("payments", engine, if_exists="append", index=False)
```

When Airflow retried the task, `extract_payments` returned the same 50,000 rows, and all 50,000 were appended again → 100,000 rows for one day.

### Fix: Idempotent Partition Replacement

```python
def load_payments_idempotent(date: str, engine):
    """
    Idempotent: replace the partition for this date atomically.
    Safe to retry any number of times.
    """
    df = extract_payments(date)

    with engine.begin() as conn:
        # 1. Delete existing partition for this date (idempotent)
        deleted = conn.execute(sa.text(
            "DELETE FROM payments WHERE payment_date = :d"
        ), {"d": date}).rowcount

        if deleted > 0:
            print(f"Replaced {deleted} existing rows for {date}")

        # 2. Insert fresh data
        df["payment_date"] = date
        df.to_sql("payments", conn, if_exists="append", index=False)

    print(f"Loaded {len(df)} payments for {date}")
```

### Preventive Measure: Idempotency Tests in CI

```python
# tests/test_pipeline_idempotency.py
import pytest

def test_payment_load_is_idempotent(test_engine, sample_payments_df):
    """Ensure loading payments twice produces the same result as loading once."""
    date = "2024-01-15"

    # Run twice (simulates retry)
    load_payments_idempotent(date, test_engine)
    load_payments_idempotent(date, test_engine)

    count = pd.read_sql(
        f"SELECT COUNT(*) FROM payments WHERE payment_date = '{date}'",
        test_engine
    ).iloc[0, 0]

    assert count == len(sample_payments_df), \
        f"Idempotency violation: expected {len(sample_payments_df)} rows, got {count}"
```

---

## Case Study 2: Airflow Backfill with Non-Idempotent DAG

### Problem

An analytics team needed to backfill 6 months of data after a bug was fixed. Running `airflow dags backfill` on the existing DAG multiplied all historical data by 6 (one run per re-execution attempt during the backfill).

### Original DAG (Non-Idempotent)

```python
# BROKEN: Uses NOW() instead of execution_date
def load_daily_events(**context):
    yesterday = datetime.utcnow() - timedelta(days=1)  # NOW() — changes on each retry!
    df = extract_events(yesterday)
    df.to_sql("events", engine, if_exists="append", index=False)
```

### Fixed DAG (Idempotent)

```python
def load_daily_events(ds: str, **context):
    """
    Uses 'ds' (execution_date) as the scope — stable across retries and backfills.
    """
    # ds is "2024-01-15", "2024-01-16", etc. — fixed per DAG run
    df = extract_events_for_date(ds)

    with engine.begin() as conn:
        conn.execute(sa.text(
            "DELETE FROM events WHERE event_date = :d"
        ), {"d": ds})
        df.to_sql("events", conn, if_exists="append", index=False)

    print(f"[{ds}] Loaded {len(df)} events")

with DAG(
    "daily_events",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Now safe to backfill!
) as dag:
    PythonOperator(
        task_id="load",
        python_callable=load_daily_events,
        provide_context=True,
    )
```

### Safe Backfill Command

```bash
# After fixing the DAG, safe to backfill 6 months
airflow dags backfill \
    --start-date 2023-07-01 \
    --end-date 2024-01-01 \
    --reset-dagruns \
    daily_events
```

---

## Case Study 3: Streaming Deduplication at Scale

### Problem

A real-time order processing pipeline consumed from Kafka. During a Kafka broker restart, some messages were re-delivered. The consumer (which used `INSERT` statements) wrote duplicates, inflating the revenue dashboard by $400K.

### Solution: Multi-Layer Idempotency

```python
import hashlib
from confluent_kafka import Consumer, KafkaError
import sqlalchemy as sa

class IdempotentOrderConsumer:
    def __init__(self, kafka_config: dict, db_engine):
        self.consumer = Consumer({
            **kafka_config,
            "enable.auto.commit": False,  # Manual commit for safety
            "isolation.level": "read_committed",  # Only read committed messages
        })
        self.engine = db_engine

    def _derive_event_id(self, msg) -> str:
        """
        Derive a stable event ID from Kafka position.
        Even if the application crashes and re-reads the message,
        the event_id is identical.
        """
        return hashlib.sha256(
            f"{msg.topic()}:{msg.partition()}:{msg.offset()}".encode()
        ).hexdigest()

    def process_batch(self, messages: list) -> dict:
        """Process a batch of messages idempotently."""
        results = {"processed": 0, "skipped_duplicates": 0, "errors": 0}

        for msg in messages:
            if msg.error():
                results["errors"] += 1
                continue

            event_id = self._derive_event_id(msg)
            event    = json.loads(msg.value())

            try:
                processed = self._upsert_order(event_id, event)
                if processed:
                    results["processed"] += 1
                else:
                    results["skipped_duplicates"] += 1
            except Exception as e:
                print(f"Failed to process event {event_id}: {e}")
                results["errors"] += 1

        return results

    def _upsert_order(self, event_id: str, event: dict) -> bool:
        """
        Upsert order using deterministic event_id.
        Returns False if this event_id was already processed (duplicate).
        """
        order = event.get("after") or event  # Handle CDC envelope or raw event

        with self.engine.begin() as conn:
            # Try to insert event record (UNIQUE constraint on event_id)
            result = conn.execute(sa.text("""
                INSERT INTO processed_events (event_id, processed_at)
                VALUES (:eid, NOW())
                ON CONFLICT (event_id) DO NOTHING
            """), {"eid": event_id})

            if result.rowcount == 0:
                return False  # Already processed

            # Upsert the actual order data
            conn.execute(sa.text("""
                INSERT INTO orders (order_id, customer_id, status, amount_usd, event_id)
                VALUES (:oid, :cid, :status, :amount, :eid)
                ON CONFLICT (order_id)
                DO UPDATE SET
                    status     = EXCLUDED.status,
                    amount_usd = EXCLUDED.amount_usd,
                    event_id   = EXCLUDED.event_id
                WHERE orders.updated_at < NOW()
            """), {
                "oid":    order["order_id"],
                "cid":    order["customer_id"],
                "status": order["status"],
                "amount": order["amount_usd"],
                "eid":    event_id,
            })

        return True
```

---

## Production Checklist: Idempotency

```markdown
Before deploying any data pipeline, verify:

[ ] Extract step uses fixed scope (execution_date) not NOW()
[ ] Load step uses UPSERT, MERGE, or partition replacement — never raw INSERT
[ ] Surrogate keys are deterministic hashes, not UUID4 or auto-increment
[ ] Airflow tasks use {{ ds }} for date scoping
[ ] Retry policy tested: run task 3x and verify row count equals 1x
[ ] Backfill tested: run historical dates and verify no duplicates
[ ] Streaming consumers use event_id for deduplication
[ ] Consumer offset commits after successful write, not before
[ ] Integration tests include idempotency assertion
[ ] Runbook includes "safe to retry" confirmation
```

---

## Interview Tips

> **Tip 1:** The double-charging story is a powerful anecdote. Show that idempotency isn't an academic concern — it directly prevents financial incidents. Tie it to real business impact.

> **Tip 2:** For Airflow, the key principle is: always use `{{ ds }}` (execution date) for scoping, never `datetime.utcnow()`. The execution date is stable across retries; `utcnow()` is not.

> **Tip 3:** Deriving `event_id` from Kafka topic + partition + offset is a robust pattern. The position in Kafka is immutable — the same message always has the same position, making this a perfect stable key for deduplication.

> **Tip 4:** Include idempotency tests in CI. A test that runs the pipeline twice and asserts the result equals one run is inexpensive but catches regressions immediately.

> **Tip 5:** When reviewing someone else's pipeline code in an interview, always ask: "What happens when this task is retried?" If the answer is "duplicates," the design needs idempotency work.
