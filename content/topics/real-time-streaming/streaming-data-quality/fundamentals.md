---
title: "Streaming Data Quality - Fundamentals"
topic: real-time-streaming
subtopic: streaming-data-quality
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, data-quality, schema-validation, late-data, deduplication, watermarks]
---

# Streaming Data Quality — Fundamentals

## 🎯 Analogy

Think of streaming DQ like a quality inspector on a factory assembly line. Unlike a batch inspector who can examine the entire finished batch at the end of the day, the streaming inspector must make real-time pass/fail decisions on each item as it moves by — with incomplete information about what's already passed and what's still coming.

---

## Why Streaming DQ Is Harder Than Batch

```
Batch DQ:
  - Full dataset available → count nulls, check uniqueness across all rows
  - Deterministic: same input → same output
  - Can re-run if DQ fails (idempotent)
  - Late data doesn't exist — everything is in the file

Streaming DQ:
  - Only a window of data available at any time
  - Late data arrives after the window closes
  - Non-deterministic: ordering varies by run
  - Schema drift: producers change formats without notice
  - Volume spikes: sudden bursts look like DQ issues but are legitimate
  - State management: tracking seen IDs requires unbounded memory without pruning
```

### The Core Streaming DQ Challenges

```
Challenge 1 — Completeness without full dataset:
  Batch: SELECT COUNT(*) WHERE field IS NULL / total_count
  Stream: Track null rate in a sliding window. Can't know "total" for unbounded stream.

Challenge 2 — Uniqueness across time:
  Batch: SELECT COUNT(*) vs COUNT(DISTINCT id)
  Stream: Must maintain a "seen" set in state. State grows unboundedly without TTL.

Challenge 3 — Timeliness:
  Batch: File arrived late? Fail the DAG.
  Stream: Late records are normal. How late is too late? → Watermarks.

Challenge 4 — Schema drift:
  Batch: Schema mismatch at load time → clear error.
  Stream: Producer changes field type → silent data corruption (JSON) or deserialization error.
```

---

## Schema Validation in Streams

### Schema Registry Enforcement

```
Without schema registry:
  Producer A sends: {"order_id": "123", "amount": 99.99}
  Producer B sends: {"order_id": 456, "amount": "hundred"}  ← type drift, silent corruption

With schema registry (Avro/Protobuf):
  1. Producer registers schema before writing
  2. Schema registry enforces backward/forward compatibility rules
  3. Incompatible schema → producer write fails immediately
  4. Consumer deserializes using schema ID in message → type-safe
```

```python
# Kafka producer with schema registry enforcement
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

schema_str = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "customer_id", "type": ["null", "string"], "default": null}
  ]
}
"""

producer = AvroProducer(
    {
        'bootstrap.servers': 'kafka:9092',
        'schema.registry.url': 'http://schema-registry:8081',
    },
    default_value_schema=avro.loads(schema_str)
)

# This will be validated against the schema before writing
producer.produce(
    topic='orders',
    value={"order_id": "o-123", "amount": 99.99, "customer_id": None}
)
```

### Avro Schema Evolution Rules

```
Compatible changes (no pipeline disruption):
  ✓ Add optional field with default value
    {"name": "discount", "type": ["null", "double"], "default": null}
  ✓ Remove field that has a default value (old consumers use default)

Incompatible changes (break consumers):
  ✗ Remove required field (no default)
  ✗ Change field type (int → string)
  ✗ Rename field (consumers see it as deleted + added)

Schema evolution strategy:
  BACKWARD: new schema can read old data  (add fields with defaults)
  FORWARD: old schema can read new data   (remove fields with defaults)
  FULL: both backward and forward         (safest for streaming)
```

---

## Late-Arriving Data: Watermarks

In streaming, events arrive out of order. Watermarks tell Flink "all events with timestamp ≤ watermark have arrived."

```
Event-time timeline:

  Events arriving:  [t=10] [t=8] [t=11] [t=6] [t=12] [t=9] ...
  Watermark:                                    W=9 (means: t ≤ 9 all arrived)

  Window [0, 10):
    - Closes when watermark passes t=10 (watermark ≥ 10)
    - Events arriving after window closes: LATE DATA
    - t=6 and t=8 arrived before window closed: included
    - If t=6 arrives when watermark=11: it's late (window already closed)
```

```python
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration

# Allow up to 5 seconds of late arrivals
strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(lambda event, _: event["event_time_ms"])
)
```

### Late Data: Three Handling Options

```
Option 1 — Discard (default):
  Late records arriving after window closes are dropped.
  Use when: late data has no business value (real-time dashboards).

Option 2 — Allowed lateness:
  Extend window to accept late records for a defined period.
  Window re-fires with updated results.
  .allowed_lateness(Time.minutes(1))

Option 3 — Side output:
  Route late records to a separate stream for later reprocessing.
  Use when: late data is important but needs separate handling.
```

```python
from pyflink.datastream import OutputTag

late_tag = OutputTag("late-orders", Types.MAP(Types.STRING(), Types.STRING()))

windowed = (
    stream
    .key_by(lambda e: e["product_id"])
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowed_lateness(Time.minutes(1))
    .side_output_late_data(late_tag)
    .sum("amount")
)

# Main output: on-time records
on_time = windowed

# Late records: send to DLQ or reprocessing pipeline
late = windowed.get_side_output(late_tag)
late.add_sink(KafkaSink_to_late_topic)
```

---

## Dead Letter Queues (DLQ)

A DLQ is a dedicated Kafka topic for records that fail validation. This prevents one bad record from stopping the entire pipeline.

```
DLQ pattern:

  Kafka (raw) → Flink (validate) → Kafka (valid events)
                                 ↘ Kafka (DLQ) → Alert → Human review

  DLQ record format:
  {
    "original_message": "<raw bytes as base64>",
    "error_type": "SCHEMA_VALIDATION_FAILED",
    "error_message": "Field 'amount' expected DOUBLE, got STRING",
    "source_topic": "orders",
    "source_partition": 3,
    "source_offset": 4821,
    "failed_at": "2024-01-15T10:30:00Z",
    "pipeline": "order-validator"
  }
```

```python
from pyflink.datastream.functions import FlatMapFunction

class OrderValidator(FlatMapFunction):
    def flat_map(self, record: dict, collector):
        errors = []

        if not record.get("order_id"):
            errors.append("Missing required field: order_id")
        if not isinstance(record.get("amount"), (int, float)) or record["amount"] < 0:
            errors.append(f"Invalid amount: {record.get('amount')}")
        if record.get("customer_id") is None:
            errors.append("Null customer_id")  # completeness check

        if errors:
            dlq_record = {
                "original": record,
                "errors": errors,
                "failed_at": time.time()
            }
            # Use side output for DLQ
            collector.collect_side_output(dlq_tag, dlq_record)
        else:
            collector.collect(record)
```

---

## Key DQ Metrics in Streaming

```
Metric               What it measures           Alert threshold
──────────────────────────────────────────────────────────────
null_rate            % of records with nulls    > 5% for required fields
duplicate_rate       % duplicate records         > 0.1%
late_record_rate     % records arriving late     > 10% (may indicate source issue)
schema_error_rate    % schema validation fails   > 0% (zero tolerance)
dlq_volume           Records in DLQ per minute  > 100/min
record_count_drop    Sudden drop in volume       < 50% of rolling average
```

---

## Key Terms

| Term | Definition |
|------|-----------|
| Watermark | Timestamp threshold below which all events are assumed to have arrived |
| Late data | Events arriving with event-time earlier than the current watermark |
| DLQ | Dead letter queue — sink for invalid records that fail validation |
| Schema registry | Central store for Avro/Protobuf schemas; enforces schema compatibility |
| Side output | Flink mechanism to route records to a secondary output stream |
| Allowed lateness | Grace period after window close during which late records are still accepted |
