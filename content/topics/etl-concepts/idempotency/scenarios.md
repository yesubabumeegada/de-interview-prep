---
title: "Idempotency - Scenario Questions"
topic: etl-concepts
subtopic: idempotency
content_type: scenario_question
tags: [etl, idempotency, airflow, streaming, interview, scenarios]
---

# Scenario Questions — Idempotency

<article data-difficulty="junior">

## 🟢 Junior: Fix a Non-Idempotent Batch Pipeline

**Scenario:** A daily pipeline loads sales data with this code: `df.to_sql("sales", engine, if_exists="append")`. When Airflow retried the task last Monday due to a database timeout, it loaded the same day's data twice. The sales dashboard showed double the actual revenue. How do you fix it?

<details>
<summary>💡 Hint</summary>
The problem is that "append" mode adds rows every time the function runs. Think about how to make the load operation produce the same result regardless of how many times it runs.
</details>

<details>
<summary>✅ Solution</summary>

**Root cause:** `if_exists="append"` adds rows unconditionally. Every retry appends the same data again.

**Fix: Use partition replacement (DELETE + INSERT in a transaction):**

```python
def load_daily_sales_idempotent(date: str, df: pd.DataFrame, engine):
    """
    Idempotent daily sales load.
    Running this 10 times for '2024-01-15' produces the same result as running once.
    """
    with engine.begin() as conn:
        # Step 1: Remove any existing data for this date
        deleted = conn.execute(sa.text("""
            DELETE FROM sales WHERE sale_date = :d
        """), {"d": date}).rowcount

        if deleted > 0:
            print(f"Removed {deleted} existing rows for {date} (idempotent reset)")

        # Step 2: Insert the data
        df["sale_date"] = date
        df.to_sql("sales", conn, if_exists="append", index=False)

    print(f"Loaded {len(df)} sales rows for {date}")

# In Airflow — use ds (execution date), not datetime.now()
def airflow_task(ds: str, **context):
    df = extract_sales_from_source(date=ds)
    load_daily_sales_idempotent(date=ds, df=df, engine=engine)
```

**Why this works:**
- DELETE removes whatever exists for that date (0 rows on first run, N rows on retry)
- INSERT loads fresh data
- Result: exactly N rows for that date, regardless of how many times the task ran

**Alternative: UPSERT if you have a unique key:**

```sql
-- If sales have a unique sale_id:
INSERT INTO sales (sale_id, sale_date, amount, customer_id)
VALUES (:sale_id, :sale_date, :amount, :customer_id)
ON CONFLICT (sale_id)
DO UPDATE SET
    amount      = EXCLUDED.amount,
    customer_id = EXCLUDED.customer_id,
    sale_date   = EXCLUDED.sale_date;
```

**Validation test:**

```python
def test_idempotency():
    df = get_test_data("2024-01-15")

    for _ in range(3):
        load_daily_sales_idempotent("2024-01-15", df, engine)

    count = engine.execute("SELECT COUNT(*) FROM sales WHERE sale_date = '2024-01-15'").scalar()
    assert count == len(df), f"Got {count} rows, expected {len(df)}"
    print("Idempotency test passed!")
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Idempotent Streaming Consumer Design

**Scenario:** You're building a Kafka consumer that reads order events and writes to PostgreSQL. The consumer is part of a critical payment reconciliation system. Requirements: (1) no duplicate orders in the database, (2) safe to redeploy mid-stream (the consumer will re-read messages from last committed offset), (3) if the DB write succeeds but Kafka offset commit fails, the message will be re-delivered — handle this gracefully.

<details>
<summary>💡 Hint</summary>
Think about when to commit the Kafka offset relative to the DB write. Consider what happens in each failure scenario: DB fails, offset commit fails, both fail.
</details>

<details>
<summary>✅ Solution</summary>

**The key challenge:** Kafka offset commit and DB write are two separate operations. They can't be done atomically unless you store offsets in the same DB as your business data.

**Solution: Store Kafka offsets in the same PostgreSQL database:**

```python
from confluent_kafka import Consumer
import sqlalchemy as sa
import json

class IdempotentKafkaConsumer:
    def __init__(self, kafka_config: dict, pg_engine):
        self.consumer = Consumer({
            **kafka_config,
            "enable.auto.commit": False,     # We manage commits manually
            "auto.offset.reset": "earliest",
        })
        self.engine = pg_engine
        self._init_offset_table()

    def _init_offset_table(self):
        """Store Kafka offsets in PostgreSQL for exactly-once semantics."""
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS kafka_offsets (
                    consumer_group TEXT,
                    topic          TEXT,
                    partition      INT,
                    offset_val     BIGINT,
                    PRIMARY KEY (consumer_group, topic, partition)
                )
            """))

    def get_stored_offset(self, group: str, topic: str, partition: int) -> int:
        """Get the last committed offset from PostgreSQL."""
        with self.engine.connect() as conn:
            row = conn.execute(sa.text("""
                SELECT offset_val FROM kafka_offsets
                WHERE consumer_group = :g AND topic = :t AND partition = :p
            """), {"g": group, "t": topic, "p": partition}).fetchone()
        return row[0] if row else -1

    def process_message(self, msg) -> bool:
        """
        Process a single message idempotently.
        Returns True if processed (new), False if already seen (duplicate).
        """
        topic     = msg.topic()
        partition = msg.partition()
        offset    = msg.offset()
        group     = "order-reconciliation-consumer"

        # Check if we've already processed this offset
        stored_offset = self.get_stored_offset(group, topic, partition)
        if offset <= stored_offset:
            print(f"Offset {offset} already processed (stored: {stored_offset}). Skipping.")
            return False

        event = json.loads(msg.value())
        order = event.get("after", event)

        # Write order + advance offset in the SAME transaction
        with self.engine.begin() as conn:
            # Upsert the order (idempotent even if order_id seen before)
            conn.execute(sa.text("""
                INSERT INTO orders (order_id, customer_id, amount_usd, status)
                VALUES (:oid, :cid, :amount, :status)
                ON CONFLICT (order_id) DO UPDATE SET
                    status     = EXCLUDED.status,
                    amount_usd = EXCLUDED.amount_usd
            """), {
                "oid":    order["order_id"],
                "cid":    order["customer_id"],
                "amount": order["amount_usd"],
                "status": order["status"],
            })

            # Advance the offset record in the SAME transaction
            conn.execute(sa.text("""
                INSERT INTO kafka_offsets (consumer_group, topic, partition, offset_val)
                VALUES (:g, :t, :p, :o)
                ON CONFLICT (consumer_group, topic, partition)
                DO UPDATE SET offset_val = EXCLUDED.offset_val
            """), {"g": group, "t": topic, "p": partition, "o": offset})

        return True

    def run(self, topic: str, group: str):
        self.consumer.subscribe([topic])

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            self.process_message(msg)
            # No need to commit Kafka offset — it's tracked in PostgreSQL!
```

**Failure scenarios handled:**
- **DB write fails:** Transaction rolls back; offset not advanced; message re-delivered → safe retry
- **Offset commit fails:** (Not relevant — we don't use Kafka offset commit)
- **App crashes after DB write:** Offset was advanced in same transaction; re-delivery is detected as duplicate → skip
- **Redeploy mid-stream:** Consumer reads from PostgreSQL-stored offset; no double-processing

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Idempotent Multi-Step Pipeline with Distributed Workers

**Scenario:** You have a 5-step data pipeline (Extract → Validate → Transform → Enrich → Load) that runs on 20 distributed workers. Steps 3 and 4 are CPU-intensive and run in parallel across workers. The pipeline runs hourly. Requirements: (1) any step can be retried independently without corrupting data, (2) two workers must never execute the same step for the same logical run concurrently, (3) the pipeline must be resumable from the last successful step if interrupted, and (4) all executions must be auditable.

<details>
<summary>💡 Hint</summary>
Think about a pipeline execution table that tracks step status with database-level locking. Consider how to scope each step to a pipeline_run_id rather than wall-clock time.
</details>

<details>
<summary>✅ Solution</summary>

**Core design: Run-scoped idempotency with distributed step locking**

```python
import enum
import uuid
from datetime import datetime
import sqlalchemy as sa

class StepStatus(enum.Enum):
    PENDING    = "pending"
    RUNNING    = "running"
    COMPLETED  = "completed"
    FAILED     = "failed"
    SKIPPED    = "skipped"

class PipelineOrchestrator:
    """
    Manages idempotent, resumable, distributed pipeline execution.
    Uses PostgreSQL advisory locks to prevent concurrent step execution.
    """
    def __init__(self, engine):
        self.engine = engine
        self._init_tables()

    def _init_tables(self):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    pipeline_name   TEXT NOT NULL,
                    logical_date    DATE NOT NULL,
                    started_at      TIMESTAMPTZ DEFAULT NOW(),
                    completed_at    TIMESTAMPTZ,
                    status          TEXT DEFAULT 'running',
                    UNIQUE (pipeline_name, logical_date)
                );

                CREATE TABLE IF NOT EXISTS pipeline_steps (
                    run_id      UUID REFERENCES pipeline_runs(run_id),
                    step_name   TEXT NOT NULL,
                    status      TEXT DEFAULT 'pending',
                    started_at  TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    worker_id   TEXT,
                    output_path TEXT,
                    error       TEXT,
                    PRIMARY KEY (run_id, step_name)
                );
            """))

    def get_or_create_run(self, pipeline: str, logical_date: str) -> str:
        """Get existing run or create new one. Returns run_id."""
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO pipeline_runs (pipeline_name, logical_date)
                VALUES (:p, :d)
                ON CONFLICT (pipeline_name, logical_date) DO NOTHING
            """), {"p": pipeline, "d": logical_date})

            row = conn.execute(sa.text("""
                SELECT run_id, status FROM pipeline_runs
                WHERE pipeline_name = :p AND logical_date = :d
            """), {"p": pipeline, "d": logical_date}).fetchone()

        return str(row.run_id)

    def claim_step(self, run_id: str, step_name: str, worker_id: str) -> bool:
        """
        Attempt to claim a step for execution.
        Uses FOR UPDATE SKIP LOCKED to prevent two workers from claiming the same step.
        Returns True if claimed, False if already taken or completed.
        """
        with self.engine.begin() as conn:
            # Ensure step row exists
            conn.execute(sa.text("""
                INSERT INTO pipeline_steps (run_id, step_name, status)
                VALUES (:run, :step, 'pending')
                ON CONFLICT (run_id, step_name) DO NOTHING
            """), {"run": run_id, "step": step_name})

            # Try to claim: only claim if PENDING (not RUNNING or COMPLETED)
            result = conn.execute(sa.text("""
                UPDATE pipeline_steps
                SET status = 'running', started_at = NOW(), worker_id = :worker
                WHERE run_id   = :run
                  AND step_name = :step
                  AND status    = 'pending'
            """), {"run": run_id, "step": step_name, "worker": worker_id})

            return result.rowcount == 1  # 1 = claimed, 0 = already taken

    def complete_step(self, run_id: str, step_name: str, output_path: str = None):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE pipeline_steps
                SET status = 'completed', completed_at = NOW(), output_path = :path
                WHERE run_id = :run AND step_name = :step
            """), {"run": run_id, "step": step_name, "path": output_path})

    def fail_step(self, run_id: str, step_name: str, error: str):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE pipeline_steps
                SET status = 'failed', completed_at = NOW(), error = :error,
                    -- Reset to pending for retry
                    status = 'pending'
                WHERE run_id = :run AND step_name = :step
            """), {"run": run_id, "step": step_name, "error": error})

    def is_step_complete(self, run_id: str, step_name: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(sa.text("""
                SELECT status FROM pipeline_steps
                WHERE run_id = :run AND step_name = :step
            """), {"run": run_id, "step": step_name}).fetchone()
        return row and row.status == "completed"


class PipelineWorker:
    def __init__(self, worker_id: str, orchestrator: PipelineOrchestrator):
        self.worker_id    = worker_id
        self.orchestrator = orchestrator

    def execute_step(self, run_id: str, step_name: str, step_fn, *args, **kwargs):
        """
        Execute a single step idempotently with distributed locking.
        """
        orch = self.orchestrator

        # Skip if already completed (resumability)
        if orch.is_step_complete(run_id, step_name):
            print(f"[{self.worker_id}] Step {step_name} already completed. Skipping.")
            return True

        # Try to claim (distributed locking)
        claimed = orch.claim_step(run_id, step_name, self.worker_id)
        if not claimed:
            print(f"[{self.worker_id}] Step {step_name} claimed by another worker. Skipping.")
            return False

        print(f"[{self.worker_id}] Executing step {step_name} for run {run_id}")

        try:
            output = step_fn(run_id, *args, **kwargs)
            orch.complete_step(run_id, step_name, output_path=str(output) if output else None)
            print(f"[{self.worker_id}] Step {step_name} completed successfully")
            return True
        except Exception as e:
            orch.fail_step(run_id, step_name, error=str(e))
            print(f"[{self.worker_id}] Step {step_name} failed: {e}")
            raise


# Usage
def run_pipeline(logical_date: str, worker_id: str):
    engine = create_engine("postgresql://...")
    orch   = PipelineOrchestrator(engine)
    worker = PipelineWorker(worker_id, orch)

    run_id = orch.get_or_create_run("orders_pipeline", logical_date)

    # Each step is idempotent, claimable, and resumable
    steps = [
        ("extract",   lambda run_id: extract_data(logical_date)),
        ("validate",  lambda run_id: validate_data(run_id)),
        ("transform", lambda run_id: transform_data(run_id)),
        ("enrich",    lambda run_id: enrich_data(run_id)),
        ("load",      lambda run_id: load_to_warehouse(run_id)),
    ]

    for step_name, step_fn in steps:
        worker.execute_step(run_id, step_name, step_fn)

# Audit query
AUDIT_QUERY = """
SELECT
    r.logical_date,
    r.pipeline_name,
    s.step_name,
    s.status,
    s.worker_id,
    s.started_at,
    s.completed_at,
    EXTRACT(EPOCH FROM (s.completed_at - s.started_at)) AS duration_seconds
FROM pipeline_runs r
JOIN pipeline_steps s ON r.run_id = s.run_id
WHERE r.logical_date >= CURRENT_DATE - 7
ORDER BY r.logical_date DESC, s.started_at;
"""
```

**Properties achieved:**
- **Idempotent:** Re-running the same `(pipeline, logical_date)` is safe — completed steps are skipped
- **Distributed locking:** `UPDATE ... WHERE status = 'pending'` with row-level lock prevents two workers running the same step
- **Resumable:** Failed pipelines restart from the last incomplete step
- **Auditable:** Full history of who ran what, when, with durations

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What does idempotency mean in data engineering?**
A: An idempotent operation produces the same result regardless of how many times it is executed. In ETL, this means running a pipeline twice for the same input data yields the same output state — no duplicates, no data loss.

**Q: Why is idempotency critical for ETL pipelines?**
A: Network failures, scheduler retries, and operator re-runs are inevitable in production. Without idempotency, retries create duplicate records or corrupt state. With it, any run can be safely retried without manual cleanup.

**Q: How do you make a data load idempotent?**
A: Use INSERT OVERWRITE or MERGE (upsert) semantics instead of plain INSERT. Partition data by a natural key (e.g., date), so re-running the load for a partition replaces the previous output atomically rather than appending to it.

**Q: What is the difference between idempotency and exactly-once semantics?**
A: Idempotency is a property of the operation (re-running produces the same state). Exactly-once is a delivery guarantee (each message is processed precisely once). An idempotent sink can tolerate at-least-once delivery and still produce a correct final state.

**Q: How do you achieve idempotency when writing to a relational database?**
A: Use UPSERT (INSERT ... ON CONFLICT DO UPDATE in PostgreSQL, MERGE in SQL Server/Snowflake) keyed on a unique business key or surrogate ID. This ensures re-processing the same record updates rather than duplicates it.

**Q: How do you handle idempotency when calling external APIs in a pipeline?**
A: Use idempotency keys (a unique request ID sent with each call) supported by the API to ensure the server deduplicates repeat requests. Store successful call records locally so you can skip already-processed items on retry without re-calling the API.

**Q: What is a natural idempotency key?**
A: A natural idempotency key is a business-meaningful unique identifier inherent in the data — such as order_id, event_id, or a composite of (source_system, record_id, date). Using natural keys avoids the need for synthetic deduplication logic.

**Q: How does Airflow support idempotent DAG design?**
A: Airflow passes `execution_date` (the logical run time) to each task, allowing tasks to scope their reads and writes to a specific partition. Designing tasks to process only their `execution_date` partition and overwrite it makes each run naturally idempotent.

---

## 💼 Interview Tips

- Lead with the "why" before the "how" — explain that retries and failures are inevitable, so idempotency is a correctness requirement, not an optimization. This frames you as a systems thinker.
- Distinguish idempotency from deduplication — deduplication removes existing duplicates reactively; idempotency prevents them proactively. Interviewers appreciate the nuance.
- When discussing sink writes, name the specific SQL/API mechanism (MERGE, UPSERT, ON CONFLICT) rather than just saying "use upsert" — specificity signals hands-on experience.
- Be ready for the follow-up: "What if your idempotency key source has gaps?" — discuss surrogate key generation strategies and how to handle missing natural keys gracefully.
- Mention the testing angle: idempotency should be verified by running the same pipeline twice in a test environment and asserting row counts and values are identical.
- For streaming pipelines, explain that idempotency at the sink is your primary defense against at-least-once delivery — showing you understand delivery semantics in context impresses senior interviewers.
