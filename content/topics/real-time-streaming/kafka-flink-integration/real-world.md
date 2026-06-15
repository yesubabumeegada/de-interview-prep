---
title: "Kafka-Flink Integration - Real World"
topic: real-time-streaming
subtopic: kafka-flink-integration
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [kafka, flink, streaming, production, performance, monitoring, lakehouse]
---

# Kafka-Flink Integration — Real World

## Case Study: Real-Time Fraud Detection Pipeline

### Architecture

A payments company processes 2M transactions/day. Fraud signals must be computed and acted on within 200ms of the transaction event.

```
Architecture:

  POS Terminals / Apps
       ↓
  Kafka: transactions (16 partitions, RF=3, acks=all)
       ↓
  Flink Job: fraud-scorer
  ├── Source: KafkaSource (16 parallelism, exactly matches partitions)
  ├── keyBy(merchant_id)
  ├── RichFlatMapFunction: rule engine (velocity checks, amount anomalies)
  ├── keyBy(card_id)
  ├── AsyncFunction: real-time ML score from feature store (Redis)
  └── Sink: KafkaSink → fraud-alerts topic (AT_LEAST_ONCE)
             KafkaSink → transactions-enriched topic (EXACTLY_ONCE → Delta Lake)
       ↓
  Kafka: fraud-alerts (consumed by card authorization service, P99 < 50ms)
  Kafka: transactions-enriched (consumed by Spark job → Delta Lake ledger)
```

### Flink Job Configuration

```python
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(16)  # match Kafka partitions
env.enable_checkpointing(30_000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_timeout(60_000)
env.get_checkpoint_config().set_min_pause_between_checkpoints(15_000)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("transactions")
    .set_group_id("fraud-scorer-v3")
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
    .set_value_only_deserializer(SimpleStringSchema())
    .set_property("isolation.level", "read_committed")
    .set_property("fetch.min.bytes", "65536")
    .set_property("fetch.max.wait.ms", "50")  # low latency: don't wait long
    .build()
)

stream = env.from_source(
    source,
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_millis(500))
        .with_idleness(Duration.of_seconds(10)),
    "Transactions Source"
)
```

### Velocity Check with Flink State

```python
from pyflink.datastream.functions import RichFlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

class VelocityCheckFunction(RichFlatMapFunction):
    """Flag cards with > 5 transactions in 60 seconds."""

    def open(self, config):
        # Count transactions per card in the last 60s (sliding window via state)
        self.count_state = self.get_runtime_context().get_map_state(
            MapStateDescriptor("tx_timestamps", Types.LONG(), Types.BOOLEAN())
        )

    def flat_map(self, transaction, collector):
        import time
        now_ms = int(time.time() * 1000)
        window_start = now_ms - 60_000  # 60-second window

        # Add current transaction timestamp
        self.count_state.put(now_ms, True)

        # Evict old entries outside window
        old_keys = [k for k in self.count_state.keys() if k < window_start]
        for k in old_keys:
            self.count_state.remove(k)

        # Count recent transactions
        count = sum(1 for _ in self.count_state.keys())

        transaction["velocity_count"] = count
        transaction["velocity_flag"] = count > 5

        collector.collect(transaction)
```

---

## Case Study: Kafka → Flink → Delta Lake Streaming Lakehouse

### Problem

A retail company wants sub-minute data freshness in their analytics Delta Lake tables. Previously, Spark batch jobs ran every 15 minutes, causing stale dashboards.

### Solution

```
Before: Kafka → S3 (raw JSON) → Spark batch (15 min) → Delta Lake
After:  Kafka → Flink (transform) → Delta Lake (streaming, ~30s lag)
```

```python
# Flink SQL: streaming Kafka to Delta (via Flink-Delta connector)
table_env = StreamTableEnvironment.create(env)

# Kafka source
table_env.execute_sql("""
    CREATE TABLE kafka_orders (
        order_id     STRING,
        customer_id  STRING,
        product_id   STRING,
        quantity     INT,
        unit_price   DOUBLE,
        event_time   TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = 'orders',
        'properties.bootstrap.servers'  = 'kafka:9092',
        'properties.group.id'           = 'flink-delta-ingest',
        'scan.startup.mode'             = 'latest-offset',
        'format'                        = 'avro-confluent',
        'avro-confluent.url'            = 'http://schema-registry:8081'
    )
""")

# Derived table: compute revenue per order
table_env.execute_sql("""
    CREATE VIEW order_revenue AS
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity * unit_price AS revenue,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_date,
        event_time
    FROM kafka_orders
""")

# Sink: Delta Lake (via filesystem connector + Delta commit listener)
table_env.execute_sql("""
    CREATE TABLE delta_orders (
        order_id    STRING,
        customer_id STRING,
        product_id  STRING,
        revenue     DOUBLE,
        event_date  STRING,
        event_time  TIMESTAMP(3)
    ) PARTITIONED BY (event_date)
    WITH (
        'connector'  = 'delta',
        'table-path' = 's3://my-bucket/delta/orders'
    )
""")

table_env.execute_sql("INSERT INTO delta_orders SELECT * FROM order_revenue")
```

### Monitoring the Streaming Lakehouse

```python
# Grafana dashboard metrics (via Prometheus)
metrics = {
    # Kafka lag — should stay < 1000 records (30s lag at 33 records/sec)
    "kafka_consumer_lag": "kafka_consumer_lag_sum{topic='orders', group='flink-delta-ingest'}",

    # Flink checkpoint health
    "checkpoint_duration_p99": "histogram_quantile(0.99, flink_checkpoint_duration_seconds_bucket)",
    "checkpoint_failures": "increase(flink_checkpoint_failed_total[5m])",

    # Delta table freshness (custom metric from Delta log)
    "delta_last_commit_age_seconds": "time() - delta_last_commit_timestamp",

    # Throughput
    "records_per_second": "rate(flink_taskmanager_job_task_numRecordsOut[1m])",
}

# Alert: data freshness > 2 minutes (10x normal lag)
alert_rule = """
ALERT DeltaFreshnessLag
  IF delta_last_commit_age_seconds > 120
  FOR 2m
  LABELS { severity = "critical", team = "data-platform" }
  ANNOTATIONS {
    summary = "Delta Lake orders table is stale",
    runbook = "https://wiki/runbooks/flink-delta-lag"
  }
"""
```

---

## Production Configuration Reference

```yaml
# flink-conf.yaml (production settings for Kafka-Flink integration)

# Checkpointing
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 120000
execution.checkpointing.min-pause: 30000
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# State backend
state.backend: rocksdb
state.backend.rocksdb.memory.managed: true
state.backend.incremental: true
state.checkpoints.dir: s3://my-bucket/flink-checkpoints
state.savepoints.dir: s3://my-bucket/flink-savepoints

# Network buffer tuning
taskmanager.network.memory.fraction: 0.15
taskmanager.network.memory.min: 128mb
taskmanager.network.memory.max: 1gb

# Restart strategy
restart-strategy: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 5
restart-strategy.failure-rate.failure-rate-interval: 5min
restart-strategy.failure-rate.delay: 30s

# Kafka consumer properties (set in KafkaSource builder)
# properties.fetch.min.bytes=65536
# properties.fetch.max.wait.ms=500
# properties.max.poll.records=500
# properties.session.timeout.ms=30000
# properties.heartbeat.interval.ms=10000
```

---

## Common Production Decisions

```
Decision: KafkaSink delivery guarantee for analytics vs transactional pipelines

  Analytics (click streams, page views):
    DeliveryGuarantee.AT_LEAST_ONCE
    + Higher throughput, simpler config
    + Downstream analytics can tolerate rare duplicates
    - Small chance of duplicates (acceptable for count/sum metrics)

  Transactional (payments, inventory):
    DeliveryGuarantee.EXACTLY_ONCE
    + Zero duplicates, regulatory compliance
    - ~10-15% throughput overhead
    - Requires transaction.timeout.ms tuning
    - Downstream consumers need isolation.level=read_committed

Decision: Flink checkpoint interval

  Low-latency pipelines (P99 < 500ms): 10-30s checkpoint interval
    - Faster recovery (shorter replay window)
    - Higher S3 cost (more frequent checkpoints)

  High-throughput pipelines (> 100K events/sec): 60-120s interval
    - Less S3 pressure
    - Larger replay window on recovery (more duplicate work)
    - Use incremental checkpoints to reduce checkpoint size

Decision: Schema evolution handling

  Avro with Schema Registry: backward/forward compatible evolution
    - Add fields with defaults → consumers continue working
    - Remove fields → producers stop sending, consumers ignore missing field
    - Change field type → incompatible, requires new schema version + topic migration

  JSON (no schema): flexible but dangerous
    - New fields silently ignored (may cause bugs)
    - Type changes silently misinterpreted
    - Use JSON Schema validation at the Kafka producer or in Flink source operator
```
