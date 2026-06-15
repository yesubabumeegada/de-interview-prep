---
title: "Streaming Data Quality - Senior Deep Dive"
topic: real-time-streaming
subtopic: streaming-data-quality
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, data-quality, great-expectations, data-contracts, stateful-dq, monitoring, production]
---

# Streaming Data Quality — Senior Deep Dive

## Designing a Production Streaming DQ Framework

A production streaming DQ framework must handle all dimensions of data quality continuously, without blocking the main pipeline, and with observable metrics.

```
Streaming DQ framework architecture:

  Kafka (raw) → Flink (DQ validator)
                ├── valid records → Kafka (clean topic) → downstream processors
                ├── DLQ records → Kafka (dlq topic) → alerting + manual review
                └── DQ metrics → Kafka (dq-metrics topic) → Prometheus → Grafana

  DQ validator layers:
    1. Schema validation (fast, Avro/schema registry)
    2. Field-level checks (null, range, format)
    3. Cross-field business rules (amount > 0 if status == "completed")
    4. Cross-event rules (stateful: order total = sum of items)
    5. Volume anomaly detection (window-level)
    6. Late data tracking and side output

  Key design principles:
    - DQ checks must not block or slow the main pipeline
    - Failed records go to DLQ, not to exception handlers that halt processing
    - DQ metrics are emitted asynchronously
    - All DQ rules are configurable (data contract driven), not hardcoded
```

---

## Great Expectations on Spark Structured Streaming

Great Expectations (GX) is typically batch-oriented, but can be applied to streaming via `foreachBatch`.

```python
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Initialize GX data context (once at driver startup)
context = gx.get_context()

def validate_batch_with_gx(batch_df, batch_id: int):
    """Apply GX validation to each Spark Structured Streaming micro-batch."""

    # Convert to pandas for GX validation (for small-medium batch sizes)
    # For large batches: sample or use Spark-native GX validator
    pdf = batch_df.toPandas()

    batch_request = RuntimeBatchRequest(
        datasource_name="orders_streaming",
        data_connector_name="runtime_data_connector",
        data_asset_name="orders_micro_batch",
        runtime_parameters={"batch_data": pdf},
        batch_identifiers={"batch_id": str(batch_id)},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="orders_streaming_suite",
    )

    # Run expectations
    results = validator.validate()

    if not results["success"]:
        failed = [r for r in results["results"] if not r["success"]]
        for failure in failed:
            print(f"BATCH {batch_id} DQ FAILURE: {failure['expectation_config']['expectation_type']}")
            # Emit metric to Prometheus
            dq_failure_counter.labels(
                expectation=failure['expectation_config']['expectation_type'],
                batch_id=batch_id
            ).inc()

    # Write valid records (all records — GX validates but doesn't filter here)
    # For filtering, add a separate validation step before this
    batch_df.write.format("delta").mode("append").save("/data/delta/orders")

# GX Expectation Suite (defined once, used per batch)
suite_config = """
{
  "expectation_suite_name": "orders_streaming_suite",
  "expectations": [
    {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "order_id"}},
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "amount"}},
    {"expectation_type": "expect_column_values_to_be_between",
     "kwargs": {"column": "amount", "min_value": 0, "max_value": 1000000}},
    {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
    {"expectation_type": "expect_table_row_count_to_be_between",
     "kwargs": {"min_value": 1, "max_value": 100000}}
  ]
}
"""
```

---

## Advanced Stateful DQ: Referential Integrity in Streams

Validating that streaming events reference valid entities (e.g., customer IDs exist in a reference table).

```python
from pyflink.datastream.functions import RichFlatMapFunction
import redis

class ReferentialIntegrityChecker(RichFlatMapFunction):
    """
    Check that order's customer_id exists in the customer reference set.
    Reference set is maintained in Redis (populated by CDC from customers DB).
    """

    def open(self, config):
        self.redis = redis.Redis(host='redis', port=6379, decode_responses=True)
        self.unknown_customer_counter = (
            self.get_runtime_context()
            .get_metrics_group()
            .counter("unknown_customer_id")
        )

    def flat_map(self, order: dict, collector):
        customer_id = order.get("customer_id")

        if customer_id:
            # Check Redis (populated via CDC from customers table)
            exists = self.redis.sismember("valid_customer_ids", customer_id)
            if not exists:
                self.unknown_customer_counter.inc()
                # Send to DLQ with violation details
                collector.collect_side_output(dlq_tag, {
                    "original": order,
                    "violation": "REFERENTIAL_INTEGRITY",
                    "detail": f"customer_id {customer_id} not found in customers table"
                })
                return

        collector.collect(order)
```

---

## Schema Evolution Handling in Production

```python
# Flink: handle schema evolution gracefully with multi-version support
import json
from typing import Optional

class SchemaEvolutionHandler:
    """
    Handle multiple schema versions in the same Kafka topic.
    Producers may be at different versions during rolling deployments.
    """

    SCHEMA_VERSIONS = {
        "v1": ["order_id", "amount", "customer_id"],
        "v2": ["order_id", "amount", "customer_id", "discount", "promo_code"],
        "v3": ["order_id", "amount", "customer_id", "discount", "promo_code", "geo_region"],
    }

    def parse(self, raw_message: str) -> Optional[dict]:
        try:
            record = json.loads(raw_message)
        except json.JSONDecodeError as e:
            return self._dlq(raw_message, f"JSON parse error: {e}")

        # Detect version by presence of version field or field set
        version = record.get("schema_version", self._infer_version(record))

        # Normalize to latest schema (v3) with defaults for missing fields
        normalized = {
            "order_id": record.get("order_id"),
            "amount": record.get("amount"),
            "customer_id": record.get("customer_id"),
            "discount": record.get("discount", 0.0),      # v2+ field, default 0
            "promo_code": record.get("promo_code"),        # v2+ field, nullable
            "geo_region": record.get("geo_region", "UNKNOWN"),  # v3+ field, default UNKNOWN
            "_schema_version": version,
        }

        return normalized

    def _infer_version(self, record: dict) -> str:
        if "geo_region" in record:
            return "v3"
        elif "discount" in record:
            return "v2"
        return "v1"

    def _dlq(self, raw: str, error: str) -> None:
        # Route to DLQ — return None to signal invalid record
        # Actual DLQ routing handled by the outer ProcessFunction
        print(f"DLQ: {error} | raw: {raw[:200]}")
        return None
```

---

## Streaming DQ Observability Platform

```python
# DQ metrics streamed to a dedicated Kafka topic → consumed by Grafana/monitoring

from dataclasses import dataclass, asdict
from typing import Optional
import json
import time

@dataclass
class DQEvent:
    pipeline_id: str
    topic: str
    partition: int
    offset: int
    check_name: str
    passed: bool
    severity: str  # INFO, WARNING, CRITICAL
    violation_detail: Optional[str]
    record_count: int
    event_time_ms: int
    processing_time_ms: int

class DQMetricsEmitter:
    def __init__(self, producer):
        self.producer = producer
        self.dq_topic = "dq-metrics"

    def emit(self, event: DQEvent):
        self.producer.produce(
            self.dq_topic,
            key=f"{event.pipeline_id}:{event.check_name}",
            value=json.dumps(asdict(event))
        )

# Grafana dashboard panels (Prometheus queries):
panels = {
    "DQ Pass Rate by Check": """
        sum(rate(dq_events_total{passed="true"}[5m])) by (check_name)
        /
        sum(rate(dq_events_total[5m])) by (check_name)
    """,

    "DQ Failure Volume": """
        sum(rate(dq_events_total{passed="false"}[1m])) by (check_name, severity)
    """,

    "Null Rate Trend": """
        rate(dq_null_field_count[5m]) / rate(dq_total_record_count[5m])
    """,

    "Late Record Rate": """
        rate(dq_late_records_total[5m]) / rate(dq_total_record_count[5m])
    """,

    "Volume Anomaly Events": """
        increase(dq_volume_anomaly_total[10m])
    """
}

# Alert routing:
"""
Severity mapping:
  CRITICAL: null rate > 10% on required field, schema validation failure rate > 1%
            → PagerDuty, immediate team alert
  WARNING:  null rate 1-10%, late record rate > 20%, volume drop > 30%
            → Slack #data-platform channel
  INFO:     single late record, minor schema version mismatch
            → DQ dashboard only, no alert
"""
```

---

## Handling Poison Pills

A **poison pill** is a record that causes a consumer to crash repeatedly, halting the pipeline.

```
Poison pill patterns:

  1. Malformed JSON → JSON parser throws exception → Flink task fails → restart → same message → loop
  2. Null pointer in business logic → NPE → restart loop
  3. Extremely large message → OOM → restart loop
  4. Timestamp overflow → arithmetic exception → restart loop

Poison pill handling strategy:

  Option A — Try/catch at record level (safest):
    try:
        process(record)
    except Exception as e:
        emit_to_dlq(record, str(e))
        continue  # don't let one record stop the task

  Option B — Flink's fault tolerance restart:
    - Set restart strategy with max failures
    - If same offset fails N times → manual intervention needed
    - Risk: upstream records blocked until resolved

  Option C — Dead letter offset tracking:
    - Track which Kafka offsets failed
    - Skip known-bad offsets on restart
    - Risky: must ensure you're skipping the right message
```

```python
# Safe record processing with DLQ routing
class SafeOrderProcessor(FlatMapFunction):
    MAX_RECORD_SIZE_BYTES = 1_000_000  # 1MB — reject oversized records

    def flat_map(self, raw_bytes: bytes, collector):
        # Size check first
        if len(raw_bytes) > self.MAX_RECORD_SIZE_BYTES:
            collector.collect_side_output(dlq_tag, {
                "error": "RECORD_TOO_LARGE",
                "size_bytes": len(raw_bytes),
                "preview": raw_bytes[:100].decode("utf-8", errors="replace")
            })
            return

        try:
            record = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            collector.collect_side_output(dlq_tag, {
                "error": "PARSE_ERROR",
                "detail": str(e),
                "raw_preview": raw_bytes[:200].decode("utf-8", errors="replace")
            })
            return

        try:
            processed = self.process_order(record)
            collector.collect(processed)
        except Exception as e:
            # Business logic error — don't crash the task
            collector.collect_side_output(dlq_tag, {
                "error": "PROCESSING_ERROR",
                "detail": str(e),
                "record": record
            })

    def process_order(self, record: dict) -> dict:
        # Business logic here
        return {**record, "processed": True}
```

---

## Senior Interview Questions

**Q: How would you design a streaming DQ system that validates referential integrity (e.g., order must have a valid product_id) without blocking the main pipeline?**
A: Use a two-stream join approach. Maintain a broadcast state of the reference table (products), broadcast to all order processing operators. Each operator checks the incoming order's product_id against the broadcast state. The broadcast state is refreshed periodically via a CDC stream from the products database. For missing product_ids, route to DLQ with a "REFERENTIAL_INTEGRITY" violation type. This avoids external calls (no Redis round-trip) and doesn't block the main pipeline.

**Q: Describe the tradeoffs between schema registry enforcement vs. flexible JSON with runtime validation.**
A: Schema registry gives compile-time enforcement — invalid schemas are rejected at the producer before entering Kafka. The cost is deployment coupling: producers must coordinate schema changes with the registry. JSON with runtime validation is more flexible for rapid iteration but shifts failure to runtime — a schema change in production can corrupt downstream consumers before the team notices. For mature, production pipelines: use schema registry. For exploratory pipelines or microservices with many independent teams: use JSON + schema validation at the consumer with strict alerting on schema_error_rate.

**Q: What is the "validator tax" in streaming DQ and how do you minimize it?**
A: The validator tax is the latency and throughput overhead added by DQ checks in the critical path. Minimize it by: (1) running DQ checks in parallel side chains rather than inline, (2) using Flink's async I/O for external lookups, (3) batching metric emissions rather than per-record Prometheus updates, (4) sampling-based validation for high-volume streams (validate 10% of records for volume anomaly detection, 100% for schema), and (5) pushing schema validation to the Kafka consumer deserialization layer (free, done anyway for Avro).
