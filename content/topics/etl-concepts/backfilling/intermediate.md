---
title: "Backfilling - Intermediate"
topic: etl-concepts
subtopic: backfilling
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, backfilling, airflow, partition, streaming, dbt]
---

# Backfilling — Intermediate

## Backfill Strategy Selection

| Scenario | Recommended Strategy |
|---|---|
| New pipeline, no existing data | Full backfill with date range |
| Bug fix in transformation | Incremental backfill (affected dates only) |
| New column added to model | dbt full-refresh or column-scoped backfill |
| Late-arriving source data | Rolling window re-run (last N days always) |
| Streaming pipeline, missed window | Replay from Kafka topic offset |
| Large table, only recent change | Partition-targeted backfill |

---

## Backfill Metadata Tracking

Track backfill progress to support resumability and auditing:

```python
import sqlalchemy as sa
from datetime import date, timedelta
from typing import Optional

class BackfillManager:
    """
    Tracks backfill progress in a metadata table.
    Enables resumable backfills that can be interrupted and continued.
    """
    def __init__(self, engine):
        self.engine = engine
        self._init_table()

    def _init_table(self):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS backfill_jobs (
                    job_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    pipeline     TEXT NOT NULL,
                    start_date   DATE NOT NULL,
                    end_date     DATE NOT NULL,
                    status       TEXT DEFAULT 'pending',
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    requested_by TEXT
                );

                CREATE TABLE IF NOT EXISTS backfill_date_status (
                    job_id       UUID REFERENCES backfill_jobs(job_id),
                    run_date     DATE NOT NULL,
                    status       TEXT DEFAULT 'pending',
                    rows_loaded  INT,
                    started_at   TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    error        TEXT,
                    PRIMARY KEY  (job_id, run_date)
                );
            """))

    def create_job(
        self, pipeline: str,
        start_date: date, end_date: date,
        requested_by: str = None
    ) -> str:
        """Create a backfill job and populate date rows."""
        with self.engine.begin() as conn:
            row = conn.execute(sa.text("""
                INSERT INTO backfill_jobs (pipeline, start_date, end_date, requested_by)
                VALUES (:p, :s, :e, :by)
                RETURNING job_id
            """), {"p": pipeline, "s": start_date, "e": end_date, "by": requested_by}).fetchone()
            job_id = str(row[0])

            # Create date rows
            current = start_date
            while current <= end_date:
                conn.execute(sa.text("""
                    INSERT INTO backfill_date_status (job_id, run_date)
                    VALUES (:job, :d)
                """), {"job": job_id, "d": current})
                current += timedelta(days=1)

        return job_id

    def get_pending_dates(self, job_id: str) -> list[date]:
        """Get dates that haven't been processed yet."""
        sql = """
            SELECT run_date FROM backfill_date_status
            WHERE job_id = :job
              AND status IN ('pending', 'failed')
            ORDER BY run_date
        """
        with self.engine.connect() as conn:
            rows = conn.execute(sa.text(sql), {"job": job_id}).fetchall()
        return [r[0] for r in rows]

    def mark_date_running(self, job_id: str, run_date: date):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE backfill_date_status
                SET status = 'running', started_at = NOW()
                WHERE job_id = :job AND run_date = :d
            """), {"job": job_id, "d": run_date})

    def mark_date_complete(self, job_id: str, run_date: date, rows: int):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE backfill_date_status
                SET status = 'completed', completed_at = NOW(), rows_loaded = :rows
                WHERE job_id = :job AND run_date = :d
            """), {"job": job_id, "d": run_date, "rows": rows})

    def mark_date_failed(self, job_id: str, run_date: date, error: str):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE backfill_date_status
                SET status = 'failed', error = :err, completed_at = NOW()
                WHERE job_id = :job AND run_date = :d
            """), {"job": job_id, "d": run_date, "err": error[:2000]})

    def run_backfill(
        self,
        job_id: str,
        pipeline_fn,
        max_concurrent: int = 1,
        continue_on_error: bool = True
    ) -> dict:
        """Execute backfill job with progress tracking and resumability."""
        import concurrent.futures
        pending = self.get_pending_dates(job_id)
        results = {"success": 0, "failed": 0, "errors": []}

        def process_date(run_date: date):
            self.mark_date_running(job_id, run_date)
            try:
                rows = pipeline_fn(str(run_date))
                self.mark_date_complete(job_id, run_date, rows)
                return ("success", run_date, rows)
            except Exception as e:
                self.mark_date_failed(job_id, run_date, str(e))
                return ("failed", run_date, str(e))

        if max_concurrent == 1:
            for d in pending:
                outcome, run_date, detail = process_date(d)
                if outcome == "success":
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    results["errors"].append({"date": str(run_date), "error": detail})
                    if not continue_on_error:
                        break
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = {executor.submit(process_date, d): d for d in pending}
                for future in concurrent.futures.as_completed(futures):
                    outcome, run_date, detail = future.result()
                    if outcome == "success":
                        results["success"] += 1
                    else:
                        results["failed"] += 1

        return results
```

---

## dbt Backfill Patterns

### Full Refresh for Schema Changes

```bash
# When a new column is added to a model, full-refresh all affected models
dbt run --full-refresh --select orders_daily+

# Selective: only recompute revenue model (plus its dependencies)
dbt run --full-refresh --select +daily_revenue
```

### dbt Date Range Backfill

For incremental models, you can override the incremental filter via variables:

```sql
-- models/orders_daily.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT
    order_id, customer_id, total_usd, order_date, created_at
FROM {{ source('raw', 'orders') }}

{% if is_incremental() and not var('backfill_mode', false) %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% elif var('backfill_start_date', none) is not none %}
WHERE DATE(created_at) BETWEEN '{{ var("backfill_start_date") }}'
                             AND '{{ var("backfill_end_date", modules.datetime.date.today().isoformat()) }}'
{% endif %}
```

```bash
# Run backfill for a specific date range
dbt run \
  --select orders_daily \
  --vars '{"backfill_mode": true, "backfill_start_date": "2024-01-01", "backfill_end_date": "2024-03-31"}'
```

---

## Streaming Backfill: Replaying Kafka

For streaming pipelines, backfilling means replaying messages from an earlier Kafka offset.

```python
from confluent_kafka import Consumer, TopicPartition, KafkaError
from datetime import datetime

def find_offset_for_timestamp(
    consumer: Consumer,
    topic: str,
    partition: int,
    target_timestamp: datetime
) -> int:
    """Find the Kafka offset corresponding to a specific timestamp."""
    ts_ms = int(target_timestamp.timestamp() * 1000)
    tp    = TopicPartition(topic, partition, ts_ms)

    offsets = consumer.offsets_for_times([tp])
    result  = offsets[0]

    if result.offset == -1:
        print(f"No messages after {target_timestamp} in {topic}:{partition}")
        return None

    return result.offset

def replay_from_timestamp(
    topic: str,
    kafka_config: dict,
    start_time: datetime,
    end_time: datetime,
    processor_fn
):
    """
    Replay Kafka messages in a time range for streaming backfill.
    """
    consumer = Consumer({
        **kafka_config,
        "group.id": f"backfill-consumer-{topic}-{int(start_time.timestamp())}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

    # Get partitions
    metadata    = consumer.list_topics(topic)
    partitions  = [
        TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions.keys()
    ]

    # Find start offsets for each partition
    start_ts_ms = int(start_time.timestamp() * 1000)
    start_tps   = [TopicPartition(topic, tp.partition, start_ts_ms) for tp in partitions]
    start_offsets = consumer.offsets_for_times(start_tps)

    # Assign and seek
    consumer.assign(start_offsets)
    for tp in start_offsets:
        if tp.offset >= 0:
            consumer.seek(tp)

    end_ts_ms = int(end_time.timestamp() * 1000)
    processed = 0

    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            break  # No more messages in this window
        if msg.error():
            continue
        if msg.timestamp()[1] > end_ts_ms:
            break  # Past the end time window

        processor_fn(json.loads(msg.value()))
        processed += 1

    consumer.close()
    print(f"Replayed {processed} messages from {start_time} to {end_time}")
    return processed
```

---

## Protecting Production During Backfill

### Rate Limiting

```python
from dataclasses import dataclass
import time

@dataclass
class RateLimiter:
    rows_per_second: int
    _last_check: float = 0
    _rows_since_check: int = 0

    def throttle(self, rows_processed: int):
        """Call after each batch; sleeps if processing too fast."""
        self._rows_since_check += rows_processed
        elapsed = time.time() - self._last_check

        if elapsed > 1.0:  # Check every second
            rps = self._rows_since_check / elapsed
            if rps > self.rows_per_second:
                # Sleep proportionally to how much faster we are than limit
                sleep_time = (rps / self.rows_per_second - 1) * elapsed
                time.sleep(sleep_time)

            self._last_check       = time.time()
            self._rows_since_check = 0

# Usage: Limit backfill to 50K rows/second to protect source
limiter = RateLimiter(rows_per_second=50_000)
for batch in get_batches():
    rows = process_batch(batch)
    limiter.throttle(rows)
```

### Backfill Priority Queuing

```python
import heapq
from dataclasses import dataclass, field

@dataclass(order=True)
class BackfillTask:
    priority: int           # Lower = higher priority
    date:     str = field(compare=False)
    pipeline: str = field(compare=False)

class PriorityBackfillQueue:
    """
    Prioritize backfill tasks so recent dates are processed first.
    Recent data is usually more impactful for dashboards.
    """
    def __init__(self):
        self._queue = []

    def add_date_range(self, pipeline: str, start_date: date, end_date: date):
        current = start_date
        while current <= end_date:
            # Newer dates get lower priority number (higher priority)
            days_from_today = (date.today() - current).days
            task = BackfillTask(
                priority=days_from_today,
                date=str(current),
                pipeline=pipeline
            )
            heapq.heappush(self._queue, task)
            current += timedelta(days=1)

    def pop(self) -> BackfillTask:
        return heapq.heappop(self._queue)

    def is_empty(self) -> bool:
        return len(self._queue) == 0
```

---

## Interview Tips

> **Tip 1:** When asked about backfilling a 2-year history, break it into phases: estimate data volume, assess source system capacity, plan a throttled rate, and run dates from most-recent backwards (to restore current dashboards first).

> **Tip 2:** The backfill metadata tracking pattern (job + date-level status table) enables resumable backfills. If the job is interrupted at day 180/365, you can resume from day 181 without re-doing the first 180 days.

> **Tip 3:** dbt's `--vars` mechanism for date-scoped backfill overrides is a clean, production-grade approach. Show you know the `is_incremental()` + variable-controlled WHERE clause pattern.

> **Tip 4:** For streaming systems, backfill means Kafka replay from a specific offset or timestamp. Know how to find the offset for a timestamp using `offsets_for_times()`.

> **Tip 5:** Always ask: "Who knows the backfill is happening?" Downstream consumers (dashboards, reports) may see temporarily inconsistent data during a large backfill. Communicate the window to stakeholders.
