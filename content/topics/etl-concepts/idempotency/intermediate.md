---
title: "Idempotency - Intermediate"
topic: etl-concepts
subtopic: idempotency
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, idempotency, deduplication, airflow, streaming, kafka]
---

# Idempotency — Intermediate

## Deduplication Strategies

### Row-Level Deduplication with ROW_NUMBER

```sql
-- Remove duplicates, keeping the most recently updated version
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id           -- Business key
            ORDER BY updated_at DESC        -- Keep latest
        ) AS rn
    FROM orders_staging
)
INSERT INTO orders_clean
SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1;
```

### Event Deduplication with Seen-IDs Table

For streaming pipelines, a "seen events" table provides idempotency at the event level:

```python
import sqlalchemy as sa
from typing import Optional

class SeenEventsStore:
    """
    Track processed event IDs to prevent duplicate processing.
    Backed by a database table with TTL-based cleanup.
    """
    def __init__(self, engine, ttl_days: int = 7):
        self.engine   = engine
        self.ttl_days = ttl_days
        self._init_table()

    def _init_table(self):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS seen_events (
                    event_id     TEXT PRIMARY KEY,
                    topic        TEXT NOT NULL,
                    processed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_seen_events_processed
                ON seen_events (processed_at)
            """))

    def is_seen(self, event_id: str) -> bool:
        sql = "SELECT 1 FROM seen_events WHERE event_id = :id"
        with self.engine.connect() as conn:
            return conn.execute(sa.text(sql), {"id": event_id}).fetchone() is not None

    def mark_seen(self, conn, event_id: str, topic: str):
        """Call within the same transaction as the business write."""
        conn.execute(sa.text("""
            INSERT INTO seen_events (event_id, topic)
            VALUES (:id, :topic)
            ON CONFLICT (event_id) DO NOTHING
        """), {"id": event_id, "topic": topic})

    def cleanup_old_events(self):
        """Periodic cleanup of old seen-event records."""
        with self.engine.begin() as conn:
            result = conn.execute(sa.text("""
                DELETE FROM seen_events
                WHERE processed_at < NOW() - INTERVAL ':days days'
            """), {"days": self.ttl_days})
            print(f"Cleaned up {result.rowcount} old event records")
```

### Atomic Idempotent Processing

```python
def process_event_idempotently(
    event: dict,
    seen_store: SeenEventsStore,
    target_engine
) -> bool:
    """
    Process an event exactly once using the seen-events store.
    Returns True if processed, False if duplicate (skipped).
    """
    event_id = event.get("id") or event.get("event_id")
    if not event_id:
        raise ValueError("Event missing ID — cannot guarantee idempotency")

    # Check outside transaction (fast path for known duplicates)
    if seen_store.is_seen(event_id):
        return False  # Already processed

    # Process + mark seen in the same transaction
    with target_engine.begin() as conn:
        # Re-check inside transaction to handle race conditions
        row = conn.execute(sa.text(
            "SELECT 1 FROM seen_events WHERE event_id = :id"
        ), {"id": event_id}).fetchone()

        if row:
            return False  # Race condition: another worker got there first

        # Apply the business logic
        _apply_event(conn, event)

        # Mark as seen in the same transaction
        seen_store.mark_seen(conn, event_id, event.get("topic", "unknown"))

    return True
```

---

## Idempotent Streaming with Kafka

### Kafka Producer Idempotency

```python
from confluent_kafka import Producer

# Enable idempotent producer (exactly-once at Kafka level)
producer = Producer({
    "bootstrap.servers": "kafka:9092",
    "enable.idempotence": True,          # Prevents duplicate Kafka messages
    "acks": "all",                       # Required for idempotency
    "max.in.flight.requests.per.connection": 5,  # Max for idempotent producer
    "retries": 2147483647,               # Max retries
})

def produce_idempotent(topic: str, key: str, value: dict):
    """Idempotent Kafka produce — no duplicates even on retry."""
    producer.produce(
        topic=topic,
        key=key.encode(),
        value=json.dumps(value).encode(),
        on_delivery=lambda err, msg: print(f"Delivered: {msg.offset()}" if not err else f"Error: {err}")
    )
    producer.flush()
```

### Transactional Kafka Producer

For exactly-once end-to-end (consume → process → produce):

```python
producer = Producer({
    "bootstrap.servers": "kafka:9092",
    "transactional.id": "my-pipeline-txn-001",  # Unique per producer instance
    "enable.idempotence": True,
})

producer.init_transactions()

def process_message_transactionally(consumer, msg, processed_result: dict):
    """
    Consume → Process → Produce in a single transaction.
    On failure, the transaction aborts and the consumer offset is not committed.
    """
    producer.begin_transaction()
    try:
        # Produce result
        producer.produce(
            topic="processed.orders",
            key=msg.key(),
            value=json.dumps(processed_result).encode()
        )

        # Commit consumer offsets as part of the same transaction
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata()
        )

        producer.commit_transaction()
    except Exception as e:
        producer.abort_transaction()
        raise
```

---

## Idempotency in dbt

dbt models are idempotent by design when using table or incremental materializations.

### Incremental Model Idempotency

```sql
-- models/orders_daily.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

SELECT
    order_id,
    customer_id,
    status,
    total_usd,
    DATE(created_at)  AS order_date,
    created_at,
    updated_at
FROM {{ source('raw', 'orders') }}

{% if is_incremental() %}
-- On incremental runs, only process rows newer than existing max
WHERE updated_at > (
    SELECT COALESCE(MAX(updated_at), '2000-01-01')
    FROM {{ this }}
)
{% endif %}
```

**Key: `unique_key='order_id'` + `incremental_strategy='merge'`** makes this idempotent. Running the same incremental run twice produces the same result because the MERGE operation is idempotent.

### Full Refresh Idempotency

```bash
# dbt full refresh is always idempotent — drops and recreates the table
dbt run --full-refresh --select orders_daily
```

---

## Insert-Only (Append-Only) Patterns

Some systems use append-only writes + deduplication at query time. This separates the concerns of storage (append-only) and deduplication (query-time).

```sql
-- Every event is appended; no UPDATE or UPSERT ever
CREATE TABLE order_events (
    event_id    UUID DEFAULT gen_random_uuid(),
    order_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,   -- 'status_change', 'payment', etc.
    payload     JSONB,
    occurred_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Query: get current order state (latest event per order)
SELECT DISTINCT ON (order_id)
    order_id,
    event_type,
    payload->>'status' AS current_status,
    occurred_at
FROM order_events
ORDER BY order_id, occurred_at DESC;
```

**Benefits of insert-only:**
- No locking — inserts never block each other
- Full audit history preserved
- Idempotent by nature (re-inserting the same event is detectable by `event_id`)

```python
def append_event_idempotently(event: dict, engine):
    """
    Append an event to the insert-only log.
    Uses event_id to skip duplicates (ON CONFLICT DO NOTHING).
    """
    sql = """
        INSERT INTO order_events (event_id, order_id, event_type, payload, occurred_at)
        VALUES (:event_id, :order_id, :event_type, :payload::jsonb, :occurred_at)
        ON CONFLICT (event_id) DO NOTHING
    """
    with engine.begin() as conn:
        conn.execute(sa.text(sql), {
            "event_id":   event["id"],
            "order_id":   event["order_id"],
            "event_type": event["type"],
            "payload":    json.dumps(event.get("payload", {})),
            "occurred_at": event["occurred_at"],
        })
```

---

## Comparison: Idempotency Strategies

| Strategy | Best For | Complexity | Performance |
|---|---|---|---|
| UPSERT | Low-volume mutable tables | Low | Good |
| Partition replace | Daily/hourly batch loads | Low | Excellent |
| MERGE statement | Full upsert with deletes | Medium | Good (warehouse-specific) |
| Seen-events table | Streaming event processing | Medium | Good (needs cleanup) |
| Kafka idempotent producer | Kafka pipelines | Low | Excellent (built-in) |
| Insert-only + dedup view | Audit logs, event sourcing | Medium | Query-time cost |

---

## Interview Tips

> **Tip 1:** When asked "how do you ensure idempotency in streaming?" describe the three-step approach: (1) deduplicate by event ID, (2) use idempotent writes (UPSERT), (3) commit consumer offset and business write atomically.

> **Tip 2:** The seen-events store pattern trades storage (keeping a table of processed IDs) for safety. Mention the TTL cleanup to show operational awareness.

> **Tip 3:** dbt incremental models with `unique_key` + `incremental_strategy='merge'` are idempotent by design. This is a key reason teams prefer dbt for transformation — retries just work.

> **Tip 4:** Kafka's `enable.idempotence=true` prevents duplicates at the Kafka level (producer retries). But it doesn't make the consumer idempotent — you still need UPSERT or seen-events at the sink.

> **Tip 5:** Insert-only (append-only) is the simplest idempotency strategy: you never update records, so there's nothing to go wrong. The cost is query-time deduplication. It's ideal for audit logs, event sourcing, and CDC sinks.
