---
title: "Streaming Data Quality - Real World"
topic: real-time-streaming
subtopic: streaming-data-quality
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [streaming, data-quality, production, dlq, monitoring, schema-drift, data-contracts]
---

# Streaming Data Quality — Real World

## Case Study: Schema Drift Causing Silent Revenue Loss

### Problem

An e-commerce company's checkout service deployed a new version that changed the `amount` field from a number to a string (e.g., `"amount": "99.99"` instead of `"amount": 99.99`). The Flink pipeline used JSON deserialization without schema validation. The string value was silently ignored by the aggregation function (it cast to 0.0), resulting in revenue dashboards showing $0 for all orders for 4 hours before anyone noticed.

### Root Cause Analysis

```
Timeline:
  14:00 - Checkout service v2 deployed (amount field changed to string type)
  14:00 - Kafka receives orders with amount="99.99" (string)
  14:01 - Flink deserializes record, casts "99.99" to Double → returns 0.0 (no exception!)
  14:05 - Revenue dashboard drops to $0/minute (was $50K/minute)
  18:00 - On-call engineer notices — 4 hours of incorrect revenue data
  18:05 - Incident declared, checkout service rolled back
  18:30 - Historical events replayed from Kafka → data corrected in Delta Lake

  Cost: 4 hours of incorrect dashboards, 2 hours of incident response
  Prevention cost: <1 week to implement schema registry
```

### Solution Implemented

```python
# 1. Enforce Avro schema with schema registry
# New order schema — amount must be DOUBLE
avro_schema = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},    ← REQUIRED to be double
    {"name": "customer_id", "type": ["null", "string"], "default": null},
    {"name": "event_time_ms", "type": "long"}
  ]
}
"""

# Checkout service must now register and use this schema
# If it tries to produce amount as a string → schema registry rejects the write
# Incident cannot happen silently

# 2. Runtime type validation as defense-in-depth
def validate_amount(record: dict) -> list[str]:
    errors = []
    amount = record.get("amount")
    if amount is None:
        errors.append("amount is null")
    elif not isinstance(amount, (int, float)):
        errors.append(f"amount has wrong type: {type(amount).__name__} (expected number)")
    elif amount < 0:
        errors.append(f"amount is negative: {amount}")
    elif amount > 100_000:
        errors.append(f"amount suspiciously large: {amount}")
    return errors

# 3. Volume anomaly alert that would have caught this faster
# Revenue per minute should not drop more than 50% vs rolling average
# Alert would fire within 5 minutes instead of 4 hours
```

---

## Case Study: Late Data Causing Double-Counting in Dashboards

### Problem

A ride-sharing company's Flink pipeline computes completed ride counts per 5-minute window. Mobile clients have intermittent connectivity, causing ride completion events to arrive 10-60 minutes late. The pipeline used `outputMode("complete")` in Spark, causing all historical windows to be recomputed and "updated" values to be appended to the Delta Lake table — resulting in duplicate rows for the same window.

### Architecture Fix

```python
# WRONG: complete mode with append writes → duplicates
query = (
    rides
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "city"
    )
    .count()
    .writeStream
    .outputMode("complete")    ← Emits all windows every trigger
    .format("delta")
    .start("/data/delta/ride_counts")  ← Appends all windows each time → DUPLICATES
)

# CORRECT: use update mode + MERGE for idempotent upsert
def upsert_ride_counts(batch_df, batch_id):
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, "/data/delta/ride_counts")
    (
        delta_table.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.window_start = s.window.start AND t.city = s.city"
        )
        .whenMatchedUpdate(set={"count": "s.count", "updated_at": "current_timestamp()"})
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    rides
    .withWatermark("event_time", "60 minutes")  # allow up to 60 min late arrivals
    .groupBy(
        window("event_time", "5 minutes"),
        "city"
    )
    .count()
    .writeStream
    .outputMode("update")           ← Only emit changed windows
    .foreachBatch(upsert_ride_counts)  ← Idempotent upsert
    .option("checkpointLocation", "/checkpoints/ride-counts")
    .trigger(processingTime="30 seconds")
    .start()
)
```

### Late Data Side Output for Audit

```python
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import ProcessWindowFunction

late_rides_tag = OutputTag("late-rides")

class RideCountWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements, collector):
        count = sum(1 for _ in elements)
        window_end = context.window().end
        collector.collect({"city": key, "window_end": window_end, "count": count})

# Route late data to separate Kafka topic for audit + potential reprocessing
stream \
    .key_by(lambda r: r["city"]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .allowed_lateness(Time.minutes(60)) \
    .side_output_late_data(late_rides_tag) \
    .process(RideCountWindowFunction())

late_stream = ...get_side_output(late_rides_tag)
late_stream.sink_to(late_rides_kafka_sink)  # for audit

# Monitor late data rate to detect mobile connectivity issues
# Alert if > 5% of rides arrive > 15 minutes late (indicates app bug, not normal behavior)
```

---

## Case Study: DLQ-Driven Self-Healing Pipeline

### Problem

A data pipeline occasionally receives malformed records from a third-party vendor who doesn't always follow the agreed schema. Instead of failing or silently dropping records, the team built a self-healing DLQ process.

```
Self-healing DLQ architecture:

  Kafka (raw) → Flink (validator) → Kafka (valid) → Delta Lake
                                  → Kafka (DLQ) → DLQ Monitor
                                                       ↓
                                                 Classify error type
                                                       ↓
                                  FIXABLE?    YES → Transform & republish to raw topic
                                              NO  → Alert on-call + update runbook

  Fixable errors (automated):
    - Extra whitespace in string fields → trim and republish
    - Unix timestamp in seconds instead of milliseconds → multiply by 1000 and republish
    - Incorrect field name (common typo: "ammount" → "amount") → rename and republish

  Non-fixable errors (human review):
    - Missing required fields with no derivation possible
    - Referential integrity violation (unknown customer_id)
    - Business rule violation (amount < 0)
```

```python
# DLQ consumer: classify and potentially auto-fix records
from confluent_kafka import Consumer, Producer
import json

class DLQHealer:
    FIXABLE_TRANSFORMATIONS = [
        lambda r: {**r, "amount": r["ammount"], "ammount": None}  # typo fix
        if "ammount" in r and "amount" not in r else r,

        lambda r: {**r, "event_time_ms": r["event_time_ms"] * 1000}  # seconds to ms
        if r.get("event_time_ms", 0) < 1_000_000_000_000 else r,  # < year 2001 in ms

        lambda r: {k: v.strip() if isinstance(v, str) else v for k, v in r.items()},  # trim
    ]

    def attempt_heal(self, dlq_record: dict) -> tuple[bool, dict]:
        """Try to fix a DLQ record. Returns (fixed, corrected_record)."""
        original = dlq_record.get("original", {})

        healed = original
        for transform in self.FIXABLE_TRANSFORMATIONS:
            healed = transform(healed)

        # Re-validate after transformation
        errors = validate_order(healed)

        if not errors:
            return True, healed
        return False, healed
```

---

## Production DQ Configuration Reference

```yaml
# streaming_dq_config.yaml
pipeline: order-processor

schema:
  format: avro
  registry_url: http://schema-registry:8081
  subject: orders-value
  compatibility: FULL  # backward + forward compatible

checks:
  completeness:
    required_fields: [order_id, customer_id, amount, event_time_ms]
    severity: CRITICAL

  validity:
    rules:
      - field: amount
        type: range
        min: 0.01
        max: 100000
        severity: CRITICAL
      - field: order_id
        type: regex
        pattern: "^ORD-[0-9]{10}$"
        severity: WARNING
      - field: event_time_ms
        type: range
        min_relative: "-1d"   # not more than 1 day old
        max_relative: "+1h"   # not more than 1 hour in future
        severity: WARNING

  uniqueness:
    key: order_id
    window: 2h
    severity: CRITICAL

  volume:
    window: 5m
    expected_rate: 500   # events per minute
    min_ratio: 0.5       # alert if < 50% of expected
    max_ratio: 3.0       # alert if > 3x expected
    severity: WARNING

dlq:
  topic: orders-dlq
  retention_hours: 168   # 7 days — must investigate within 1 week
  auto_heal: true
  alert_threshold_per_minute: 50  # alert if > 50 DLQ records/min

monitoring:
  metrics_topic: dq-metrics
  emit_interval_seconds: 10
  late_data_threshold_minutes: 15  # alert if late data rate > 5%
```

---

## Common DQ Anti-Patterns

```
Anti-pattern 1: try/except that swallows all exceptions
  Bad:  except Exception: pass  ← silent data loss
  Good: except Exception as e: route_to_dlq(record, str(e))

Anti-pattern 2: Using dropDuplicates() without watermark
  Bad:  df.dropDuplicates(["order_id"])  ← state grows forever
  Good: df.withWatermark("t", "1h").dropDuplicatesWithinWatermark(["order_id"])

Anti-pattern 3: Alerting on every DQ failure
  Bad:  Alert fires for every single null field → alert fatigue
  Good: Alert on rate (null_rate > 5% sustained for 5 minutes)

Anti-pattern 4: No DLQ monitoring → DLQ fills up silently
  Bad:  DLQ topic fills with 500K records over a week, nobody notices
  Good: Alert on DLQ volume growth rate, DLQ record age (stale unprocessed records)

Anti-pattern 5: Blocking external lookups for referential integrity
  Bad:  For each record, synchronously HTTP call to customers API → 50ms per record
  Good: Broadcast state from CDC stream, or async I/O with connection pooling

Anti-pattern 6: Schema validation in only one place
  Bad:  Only validate at the sink (Flink operator near end of pipeline)
  Good: Validate at multiple layers: schema registry (producer), Flink source
        deserialization, business rule check in processing, sink constraint check
```
