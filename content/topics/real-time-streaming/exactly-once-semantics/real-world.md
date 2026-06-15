---
title: "Exactly-Once Semantics - Real World"
topic: real-time-streaming
subtopic: exactly-once-semantics
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [streaming, exactly-once, production, kafka, flink, delta-lake, deduplication]
---

# Exactly-Once Semantics — Real World

## Case Study: Payment Processing Pipeline

### Problem
A fintech company processes 500K payment events/day via Kafka. The downstream Delta Lake ledger table was showing occasional duplicate records (0.01% rate), causing incorrect account balances. The root cause: the Flink sink was configured with `AT_LEAST_ONCE` delivery, and idempotent sink logic was missing.

### Solution Architecture

```
Before (broken):
  Kafka → Flink (AT_LEAST_ONCE sink) → Delta (plain INSERT) → Duplicates!

After (fixed):
  Kafka (read_committed)
    → Flink (EXACTLY_ONCE + TwoPhaseCommit)
      → Delta (MERGE on payment_id)
        → Reconciliation job (daily batch check)
```

```python
# Flink job: payment deduplication to Delta
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.datastream.connectors.kafka import DeliveryGuarantee, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import json

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(30_000)  # 30s checkpoints
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)
env.get_checkpoint_config().set_checkpoint_timeout(60_000)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("payments")
    .set_group_id("payment-processor")
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Use foreachBatch to Delta with MERGE for idempotency
# (actual Delta write via Flink-Delta connector or via Spark in foreachBatch)
```

```python
# Spark Structured Streaming: exactly-once MERGE to Delta
def write_payments_to_delta(batch_df, batch_id):
    """
    Called once per micro-batch. Idempotent: same batch_id re-run produces same result.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.functions import current_timestamp

    if batch_df.isEmpty():
        return

    # Deduplicate within the batch first (source may have duplicates)
    deduped = batch_df.dropDuplicates(["payment_id"])

    delta_table = DeltaTable.forPath(spark, "/data/delta/payments")

    (
        delta_table.alias("t")
        .merge(
            deduped.alias("s"),
            "t.payment_id = s.payment_id"
        )
        .whenMatchedUpdate(
            condition="s.event_time > t.event_time",  # only update if newer
            set={
                "status": "s.status",
                "updated_at": "current_timestamp()",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "payments")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .selectExpr("from_json(json, 'payment_id STRING, amount DOUBLE, status STRING, event_time TIMESTAMP') as d")
    .select("d.*")
    .writeStream
    .option("checkpointLocation", "/checkpoints/payments")
    .foreachBatch(write_payments_to_delta)
    .trigger(processingTime="10 seconds")
    .start()
)
```

---

## Case Study: E-Commerce Order Deduplication

### Problem
Mobile clients retry failed API calls, resulting in duplicate `order_created` events in Kafka. The order database received duplicate orders ~0.5% of the time, causing double-charging.

### Multi-Layer Defense Strategy

```
Layer 1 — API Gateway:
  - Client includes idempotency-key header (UUID per order attempt)
  - API gateway stores key in Redis (TTL 24h)
  - Duplicate request → return cached response, no Kafka write

Layer 2 — Kafka Producer:
  - enable.idempotence=true (PID + sequence)
  - Prevents transport-level duplicates

Layer 3 — Stream Processor (Flink):
  - Deduplicate on order_id using KeyedProcessFunction
  - State TTL of 2 hours (beyond API gateway window)

Layer 4 — Sink (PostgreSQL):
  - INSERT ... ON CONFLICT (order_id) DO NOTHING
  - Final safety net
```

```java
// Flink: KeyedProcessFunction for order deduplication
public class OrderDeduplicationFunction
    extends KeyedProcessFunction<String, OrderEvent, OrderEvent> {

    private ValueState<Long> firstSeenTimestamp;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(2))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>("firstSeen", Long.class);
        descriptor.enableTimeToLive(ttlConfig);  // auto-expire state after 2h

        firstSeenTimestamp = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(OrderEvent event, Context ctx, Collector<OrderEvent> out)
        throws Exception {
        if (firstSeenTimestamp.value() == null) {
            firstSeenTimestamp.update(ctx.timestamp());
            out.collect(event);  // first time seeing this order_id → emit
        }
        // else: duplicate — silently drop
    }
}
```

---

## Case Study: Kafka → Iceberg Exactly-Once

### Problem
Writing Kafka events to Apache Iceberg in a streaming fashion while guaranteeing exactly-once. Iceberg's atomic commit mechanism enables this.

```python
# Using Flink + Iceberg connector (exactly-once via Iceberg's atomic commits)
from pyflink.datastream import StreamExecutionEnvironment
from pyiceberg.catalog import load_catalog

# Flink Iceberg sink uses Iceberg's snapshot-based atomic commit
# Each Flink checkpoint → one Iceberg snapshot (atomic)
# On recovery: uncommitted snapshots are ignored, last committed snapshot is the truth

# SQL equivalent (Flink SQL):
sql = """
CREATE TABLE iceberg_orders (
  order_id STRING,
  amount DOUBLE,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'hive_catalog',
  'catalog-database' = 'prod',
  'catalog-table' = 'orders',
  'warehouse' = 's3://my-bucket/warehouse'
);

INSERT INTO iceberg_orders
SELECT order_id, amount, TO_TIMESTAMP(FROM_UNIXTIME(event_time / 1000))
FROM kafka_orders_source;
"""

# Iceberg exactly-once guarantee:
# - Flink checkpoints align with Iceberg snapshot commits
# - If Flink restarts, it re-reads from last Kafka offset (stored in checkpoint)
# - Any partial Iceberg files from the failed attempt are orphaned (not in snapshot)
# - Flink re-processes and creates new files in a new snapshot
```

---

## Monitoring Exactly-Once Health

```python
# Prometheus metrics for exactly-once pipeline health
from prometheus_client import Counter, Gauge, Histogram

# Key metrics to track
duplicate_events_total = Counter(
    'duplicate_events_total',
    'Number of duplicate events detected and dropped',
    ['topic', 'processor']
)

checkpoint_duration_seconds = Histogram(
    'flink_checkpoint_duration_seconds',
    'Flink checkpoint duration',
    buckets=[1, 5, 10, 30, 60, 120]
)

kafka_consumer_lag = Gauge(
    'kafka_consumer_lag_records',
    'Kafka consumer lag in records',
    ['topic', 'partition', 'consumer_group']
)

# Alert rules (Prometheus/Grafana):
"""
ALERT FlinkCheckpointFailing
  IF flink_checkpoint_failed_total > 0
  FOR 5m
  LABELS { severity = "critical" }
  ANNOTATIONS {
    summary = "Flink checkpoint failing — pipeline may degrade to at-least-once"
  }

ALERT KafkaConsumerLagHigh
  IF kafka_consumer_lag_records > 100000
  FOR 10m
  LABELS { severity = "warning" }

ALERT DuplicateRateHigh
  IF rate(duplicate_events_total[5m]) > 100
  LABELS { severity = "warning" }
  ANNOTATIONS {
    summary = "High duplicate rate — check upstream idempotency"
  }
"""
```

---

## Common Production Pitfalls

```
Pitfall 1: Reusing transactional.id across parallel instances
  Problem: Two Flink tasks with same transactional.id fence each other
  Fix: Use KafkaSink which auto-appends subtask index to prefix
       transactional.id = "prefix-{subtask_index}-{checkpoint_id}"

Pitfall 2: Checkpoint timeout shorter than transaction timeout
  Problem: Kafka transaction expires before checkpoint completes
  Fix: transaction.timeout.ms > checkpoint timeout + network slack

Pitfall 3: Not cleaning up orphaned Kafka transactions
  Problem: Aborted transactions accumulate, consuming broker memory
  Fix: Set transactional.id.expiration.ms on broker (default 7 days)

Pitfall 4: Delta MERGE without proper dedup key
  Problem: Using composite key with mutable fields causes missed dedup
  Fix: Use immutable natural key (order_id, payment_id) as merge condition

Pitfall 5: State explosion in Flink deduplication
  Problem: Seen-set grows unboundedly without TTL
  Fix: Always configure StateTtlConfig with appropriate TTL
       TTL should be > max expected duplicate window (e.g., 2x API retry timeout)
```

---

## Real Numbers and SLAs

```
Production benchmarks (payment processing pipeline, 2023):

  Volume:           500K events/day, peaks at 50K events/hour
  Duplicate rate:   0.001% (5 events/day) — all caught by dedup layer
  Checkpoint freq:  30 seconds
  Checkpoint size:  ~200MB (Flink state: dedup keys in RocksDB)
  Recovery time:    ~90 seconds (restore from S3 + Kafka replay)
  Throughput loss:  ~12% vs at-least-once baseline
  Latency overhead: ~25ms P99 (2PC with Kafka transactions)

  Cost decision:
    12% throughput loss × infrastructure cost = ~$800/month
    One duplicate transaction = potential $10K+ loss + compliance risk
    → Exactly-once is worth it for payments
    → Not worth it for click-stream analytics (at-least-once fine)
```
