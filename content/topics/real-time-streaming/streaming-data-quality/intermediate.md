---
title: "Streaming Data Quality - Intermediate"
topic: real-time-streaming
subtopic: streaming-data-quality
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, data-quality, flink, spark, deduplication, volume-anomaly, stateful-dq]
---

# Streaming Data Quality — Intermediate

## Deduplication in Streams

### Flink Deduplication with Keyed State

```java
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

public class EventDeduplicator extends KeyedProcessFunction<String, Event, Event> {

    private ValueState<Long> firstSeenState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // TTL: clean up after 2 hours (dedup window)
        StateTtlConfig ttl = StateTtlConfig
            .newBuilder(Time.hours(2))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(1000)  // clean during RocksDB compaction
            .build();

        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("seen_at", Long.class);
        desc.enableTimeToLive(ttl);
        firstSeenState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        if (firstSeenState.value() == null) {
            firstSeenState.update(ctx.timestamp());
            out.collect(event);
        }
        // Duplicate — silently discard
    }
}

// Usage:
stream
    .keyBy(Event::getEventId)  // dedup key
    .process(new EventDeduplicator())
    .name("Deduplication");
```

### Spark dropDuplicatesWithinWatermark

```python
from pyspark.sql.functions import col, to_timestamp, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

schema = (
    StructType()
    .add("order_id", StringType())
    .add("amount", DoubleType())
    .add("customer_id", StringType())
    .add("event_time", TimestampType())
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
)

deduped = (
    df
    .withWatermark("event_time", "1 hour")
    .dropDuplicatesWithinWatermark(["order_id"])
    # State is automatically pruned outside the watermark window
    # Unlike dropDuplicates() which keeps all state forever
)

query = (
    deduped.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders-dedup")
    .outputMode("append")
    .start("/data/delta/orders")
)
```

**Key difference:**
- `dropDuplicates(["order_id"])` — keeps ALL order_ids in state forever (memory leak)
- `dropDuplicatesWithinWatermark(["order_id"])` — prunes state outside watermark window (production-safe)

---

## Null/Completeness Checks with ProcessFunction

```python
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream import OutputTag
from pyflink.common.typeinfo import Types

incomplete_tag = OutputTag("incomplete-records", Types.STRING())

class CompletenessChecker(ProcessFunction):
    """Check required fields and route incomplete records to DLQ."""

    REQUIRED_FIELDS = ["order_id", "customer_id", "amount", "event_time"]

    def process_element(self, record: dict, ctx, collector):
        missing = [f for f in self.REQUIRED_FIELDS if record.get(f) is None]

        if missing:
            error_record = {
                "original": record,
                "missing_fields": missing,
                "check": "completeness",
                "detected_at": ctx.timestamp()
            }
            collector.collect_side_output(incomplete_tag, str(error_record))
        else:
            collector.collect(record)

# Wire it up
validated_stream = stream.process(CompletenessChecker())
dlq_stream = validated_stream.get_side_output(incomplete_tag)
dlq_stream.sink_to(dlq_kafka_sink)
```

---

## Volume Anomaly Detection

Sudden drops or spikes in record volume often indicate upstream issues (producer outage, traffic spike, bot activity).

### Z-Score Based Volume Alert (Flink)

```python
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
import statistics

class VolumeAnomalyDetector(ProcessWindowFunction):
    """
    Compare current window record count to rolling average.
    Alert if count is more than 3 standard deviations from mean.
    """

    def __init__(self, history_size: int = 10):
        self.history_size = history_size
        self.count_history = []

    def process(self, key, context, elements, collector):
        current_count = sum(1 for _ in elements)

        if len(self.count_history) >= 2:
            mean = statistics.mean(self.count_history)
            std = statistics.stdev(self.count_history)

            if std > 0:
                z_score = abs(current_count - mean) / std
                if z_score > 3.0:
                    collector.collect({
                        "type": "VOLUME_ANOMALY",
                        "window_end": context.window().end,
                        "current_count": current_count,
                        "expected_mean": mean,
                        "z_score": z_score,
                        "severity": "HIGH" if z_score > 5 else "MEDIUM"
                    })

        # Update history (sliding window)
        self.count_history.append(current_count)
        if len(self.count_history) > self.history_size:
            self.count_history.pop(0)

        # Always emit the count metric (for dashboards)
        collector.collect({"type": "VOLUME_METRIC", "count": current_count,
                           "window_end": context.window().end})

# Usage
stream \
    .window_all(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .process(VolumeAnomalyDetector(history_size=10)) \
    .filter(lambda e: e["type"] == "VOLUME_ANOMALY") \
    .sink_to(alert_sink)
```

---

## Stateful DQ: Cross-Event Validation

Some DQ rules require checking relationships across multiple events. Example: validate that an `order_total` event matches the sum of its `order_line_item` events.

```java
// Flink: validate order total = sum of line items using CoProcessFunction
public class OrderTotalValidator
    extends KeyedCoProcessFunction<String, OrderTotal, LineItem, DQResult> {

    // State: accumulate line items per order
    private MapState<String, List<LineItem>> lineItemsState;
    private ValueState<Double> totalState;
    private ValueState<Long> deadlineState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lineItemsState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("lineItems", String.class, List.class));
        totalState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("total", Double.class));
        deadlineState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("deadline", Long.class));
    }

    @Override
    public void processElement1(OrderTotal total, Context ctx, Collector<DQResult> out)
        throws Exception {
        totalState.update(total.getAmount());
        // Set timer to validate after 30s (expecting all line items by then)
        long deadline = ctx.timestamp() + 30_000L;
        deadlineState.update(deadline);
        ctx.timerService().registerEventTimeTimer(deadline);
    }

    @Override
    public void processElement2(LineItem item, Context ctx, Collector<DQResult> out)
        throws Exception {
        // Accumulate line items
        List<LineItem> items = lineItemsState.get(item.getOrderId());
        if (items == null) items = new ArrayList<>();
        items.add(item);
        lineItemsState.put(item.getOrderId(), items);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DQResult> out)
        throws Exception {
        Double expectedTotal = totalState.value();
        List<LineItem> items = lineItemsState.get(ctx.getCurrentKey());

        if (expectedTotal != null && items != null) {
            double actualSum = items.stream().mapToDouble(LineItem::getAmount).sum();
            boolean valid = Math.abs(expectedTotal - actualSum) < 0.01;  // $0.01 tolerance

            out.collect(DQResult.builder()
                .orderId(ctx.getCurrentKey())
                .passed(valid)
                .expectedTotal(expectedTotal)
                .actualSum(actualSum)
                .build());
        } else if (expectedTotal != null) {
            // Total received but no line items — DQ failure
            out.collect(DQResult.failed(ctx.getCurrentKey(), "No line items received"));
        }

        // Clean up state
        totalState.clear();
        lineItemsState.remove(ctx.getCurrentKey());
        deadlineState.clear();
    }
}
```

---

## Streaming Data Contracts

A **data contract** is a formal agreement between data producers and consumers defining schema, semantics, SLAs, and quality expectations.

```yaml
# data_contract.yaml (example format)
id: orders-v2
version: 2.1.0
producer: checkout-service
consumers: [analytics-pipeline, fraud-detection, inventory-service]

schema:
  format: avro
  registry: http://schema-registry:8081
  subject: orders-value

quality:
  completeness:
    required_fields: [order_id, customer_id, amount, event_time]
    null_tolerance: 0%

  timeliness:
    max_producer_lag_seconds: 30    # events must be produced within 30s of occurring
    max_pipeline_lag_seconds: 120   # events must be processed within 2 min

  volume:
    expected_events_per_minute: 500
    acceptable_deviation_pct: 20%

  uniqueness:
    dedup_key: order_id
    dedup_window: 1h

sla:
  availability: 99.9%
  support_contact: checkout-team@company.com
  change_notification_days: 14  # must notify 14 days before breaking changes
```

```python
# Automated contract validation in Flink pipeline
class ContractValidator:
    def __init__(self, contract: dict):
        self.required_fields = contract["quality"]["completeness"]["required_fields"]
        self.max_lag_ms = contract["quality"]["timeliness"]["max_producer_lag_seconds"] * 1000

    def validate(self, record: dict, processing_time_ms: int) -> list[str]:
        violations = []

        # Completeness
        for field in self.required_fields:
            if record.get(field) is None:
                violations.append(f"CONTRACT_VIOLATION: null {field}")

        # Timeliness (producer lag)
        event_time = record.get("event_time_ms", 0)
        producer_lag = processing_time_ms - event_time
        if producer_lag > self.max_lag_ms:
            violations.append(
                f"CONTRACT_VIOLATION: producer lag {producer_lag}ms > {self.max_lag_ms}ms"
            )

        return violations
```

---

## DQ Metrics to Monitoring Dashboard

```python
# Flink job: emit DQ metrics to Prometheus via Kafka
from pyflink.datastream.functions import RichMapFunction

class DQMetricsEmitter(RichMapFunction):
    def open(self, config):
        # Register Flink metrics
        self.null_count = self.get_runtime_context() \
            .get_metrics_group() \
            .counter("null_field_count")
        self.total_count = self.get_runtime_context() \
            .get_metrics_group() \
            .counter("total_record_count")
        self.dlq_count = self.get_runtime_context() \
            .get_metrics_group() \
            .counter("dlq_record_count")

    def map(self, record):
        self.total_count.inc()
        if any(v is None for v in record.values()):
            self.null_count.inc()
        return record
```

---

## Integration with dbt for Downstream Batch Validation

```sql
-- dbt model: orders_dq_audit (runs daily)
-- Validates streaming data quality in Delta Lake after the fact

WITH daily_orders AS (
  SELECT * FROM {{ source('delta', 'orders') }}
  WHERE DATE(event_time) = '{{ var("run_date") }}'
),

dq_checks AS (
  SELECT
    COUNT(*)                                        AS total_records,
    COUNT(DISTINCT order_id)                        AS unique_orders,
    COUNT(*) - COUNT(DISTINCT order_id)             AS duplicate_count,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_count,
    SUM(CASE WHEN amount <= 0 THEN 1 END)           AS invalid_amount_count,
    MIN(event_time)                                  AS earliest_event,
    MAX(event_time)                                  AS latest_event
  FROM daily_orders
)

SELECT
  *,
  ROUND(duplicate_count * 100.0 / total_records, 4)    AS duplicate_rate_pct,
  ROUND(null_customer_count * 100.0 / total_records, 4) AS null_rate_pct,
  -- DQ pass/fail flags
  duplicate_count = 0                                   AS uniqueness_passed,
  null_customer_count = 0                               AS completeness_passed,
  invalid_amount_count = 0                              AS validity_passed
FROM dq_checks
```

```yaml
# dbt test for streaming DQ (schema.yml)
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique  # catches any missed stream deduplication
      - name: amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id
```
