---
title: "Streaming Architecture Patterns — Real World"
topic: real-time-streaming
subtopic: streaming-architecture
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [streaming, architecture, kafka, flink, delta-lake, production, monitoring, multi-region]
---

# Streaming Architecture Patterns — Real World

## Pattern 1: Production Event-Driven E-Commerce Pipeline

```python
"""
Full production streaming architecture for an e-commerce platform:

Architecture:
  Web/App → Kafka (events) → Flink (process) → Delta Lake (storage) → Serving
  MySQL → Debezium → Kafka (CDC) → Flink → Delta Lake

Components:
  - Kafka: 9 brokers, 300 partitions, 7-day retention
  - Flink: 32 tasks, RocksDB state, 60-second checkpoints
  - Delta Lake: Bronze / Silver / Gold on S3
  - Redis: real-time serving cache (< 5ms)
  - Grafana: monitoring dashboard

This pattern shows the Kafka producer (application side) and the Flink consumer (processing side).
"""

# ── Producer (application side) ────────────────────────────────────────────────

from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import uuid, time

# Avro schema for order events
ORDER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.ecommerce",
  "fields": [
    {"name": "event_id",   "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "order_id",   "type": "long"},
    {"name": "user_id",    "type": "long"},
    {"name": "amount",     "type": "double"},
    {"name": "status",     "type": "string"},
    {"name": "event_time", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
"""

schema_registry = SchemaRegistryClient({"url": "http://schema-registry:8081"})
avro_serializer = AvroSerializer(schema_registry, ORDER_EVENT_SCHEMA)

producer = Producer({
    "bootstrap.servers": "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    "acks": "all",                   # wait for all ISR replicas
    "enable.idempotence": True,      # exactly-once at-most-once producer guarantee
    "retries": 10,
    "max.in.flight.requests.per.connection": 5,
    "compression.type": "snappy",    # ~50% smaller messages
    "linger.ms": 10,                 # batch events for 10ms for throughput
    "batch.size": 65536              # 64 KB batch
})

def publish_order_event(order_id: int, user_id: int, event_type: str,
                        amount: float, status: str) -> None:
    """Publish order lifecycle event to Kafka with Avro schema."""
    event = {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "order_id":   order_id,
        "user_id":    user_id,
        "amount":     amount,
        "status":     status,
        "event_time": int(time.time() * 1000)
    }

    serialized = avro_serializer(
        event,
        SerializationContext("prod.ecommerce.orders.v1", MessageField.VALUE)
    )

    producer.produce(
        topic="prod.ecommerce.orders.v1",
        key=str(order_id).encode(),  # partition by order_id → ordering per order
        value=serialized,
        on_delivery=lambda err, msg: print(f"Delivered to {msg.partition()}" if not err
                                           else f"Delivery failed: {err}")
    )
    producer.poll(0)  # trigger callbacks without blocking

# Usage:
publish_order_event(order_id=12345, user_id=67890,
                    event_type="ORDER_PLACED", amount=99.99, status="pending")
publish_order_event(order_id=12345, user_id=67890,
                    event_type="ORDER_SHIPPED", amount=99.99, status="shipped")
```

---

## Pattern 2: Multi-Stage Flink Pipeline (Bronze → Silver → Gold)

```java
// Flink job: consumes raw Kafka events → validates → enriches → writes to Delta
// Bronze job: raw events → Bronze Delta Lake (no transformation)
// Silver job: Bronze Kafka → validate schema, enrich from Redis → Silver Delta
// Gold job:   Silver Kafka → windowed aggregations → Gold Delta for dashboards

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.time.Duration;

public class SilverPipelineJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // State backend: RocksDB (for large state, incremental checkpointing)
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // incremental
        env.getCheckpointConfig().setCheckpointInterval(60_000);    // 60 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setParallelism(32);
        
        // Source: Bronze Kafka topic (raw events after initial ingestion)
        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("prod.ecommerce.orders.bronze")
            .setGroupId("silver-pipeline")
            .setValueOnlyDeserializer(new OrderEventAvroDeserializer())
            .build();

        DataStream<OrderEvent> rawStream = env.fromSource(
            source,
            WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                             .withTimestampAssigner((e, ts) -> e.getEventTime()),
            "KafkaSource"
        );

        // Silver transformation: validate + enrich
        DataStream<EnrichedOrderEvent> silverStream = rawStream
            // Filter: drop events with invalid schema
            .filter(e -> e.getOrderId() > 0 && e.getUserId() > 0 && e.getAmount() > 0)
            // Enrich: look up user data from Redis (async, non-blocking)
            .flatMap(new AsyncRedisEnrichmentFunction())
            // Standardize: convert status codes to canonical values
            .map(e -> {
                e.setStatus(normalizeStatus(e.getStatus()));
                return e;
            });

        // Sink 1: Silver Kafka topic (for downstream Gold job)
        KafkaSink<EnrichedOrderEvent> kafkaSink = KafkaSink.<EnrichedOrderEvent>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(new OrderEventKafkaSerializer("prod.ecommerce.orders.silver"))
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)  // 2PC with Kafka
            .build();
        silverStream.sinkTo(kafkaSink);

        // Sink 2: Silver Delta Lake (Iceberg format, partitioned by date + category)
        silverStream
            .keyBy(e -> e.getCategory())
            .process(new DeltaLakeBatchSink("s3://bucket/delta/silver/orders/"))
            .name("DeltaLakeSilverSink");

        env.execute("Silver Pipeline Job");
    }
}

// Gold job: tumbling window aggregations for dashboard
public class GoldAggregationJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        
        // Read Silver stream
        DataStream<EnrichedOrderEvent> silverStream = /* ... Kafka source for silver topic ... */;

        // Gold: revenue per category per 5-minute window
        silverStream
            .keyBy(EnrichedOrderEvent::getCategory)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(
                new RevenueAggregator(),   // accumulates sum(amount), count, max(amount)
                new RevenueWindowResult()  // adds window_start, window_end
            )
            // Write to Delta Lake Gold table (ACID, partitioned by date+hour+category)
            .addSink(new DeltaLakeSink("s3://bucket/delta/gold/revenue_by_category/"))
            .name("GoldRevenueSink");

        // Gold: DLQ for late events (watermark too old)
        // Events arriving > 30 minutes late go to DLQ for manual review
        OutputTag<EnrichedOrderEvent> lateTag = new OutputTag<>("late-events"){};
        // ... configure allowedLateness(Time.minutes(30)) + side output ...

        env.execute("Gold Aggregation Job");
    }
}
```

---

## Pattern 3: Real-Time Monitoring and Alerting

```python
"""
Pattern: streaming monitoring system
- Ingest metrics from all services via Kafka
- Compute rolling stats with Flink SQL
- Alert via SNS when thresholds exceeded
- Write to InfluxDB/Prometheus for dashboards
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(8)
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# DDL: Source — Kafka metrics topic
t_env.execute_sql("""
CREATE TABLE service_metrics (
    service_name    STRING,
    metric_name     STRING,
    metric_value    DOUBLE,
    host            STRING,
    event_time      TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'prod.infra.metrics',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'monitoring-job',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
""")

# DDL: Sink — alert Kafka topic (alerts → Lambda → SNS)
t_env.execute_sql("""
CREATE TABLE metric_alerts (
    service_name    STRING,
    metric_name     STRING,
    avg_value       DOUBLE,
    max_value       DOUBLE,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    alert_level     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'prod.infra.alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
)
""")

# Flink SQL: 5-minute tumbling window with threshold alerting
t_env.execute_sql("""
INSERT INTO metric_alerts
SELECT
    service_name,
    metric_name,
    AVG(metric_value)   AS avg_value,
    MAX(metric_value)   AS max_value,
    window_start,
    window_end,
    CASE
        WHEN metric_name = 'error_rate'  AND AVG(metric_value) > 0.05  THEN 'P1'
        WHEN metric_name = 'latency_p99' AND AVG(metric_value) > 2000  THEN 'P1'
        WHEN metric_name = 'cpu_percent' AND AVG(metric_value) > 90    THEN 'P2'
        ELSE 'INFO'
    END AS alert_level
FROM TABLE(
    TUMBLE(TABLE service_metrics, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
WHERE metric_name IN ('error_rate', 'latency_p99', 'cpu_percent')
GROUP BY service_name, metric_name, window_start, window_end
HAVING
    (metric_name = 'error_rate'  AND AVG(metric_value) > 0.05)
    OR (metric_name = 'latency_p99' AND AVG(metric_value) > 2000)
    OR (metric_name = 'cpu_percent' AND AVG(metric_value) > 90)
""")

# Downstream: Lambda reads from metric_alerts Kafka topic → SNS notification
# (Lambda code not shown — triggers PagerDuty for P1, Slack for P2)
```

---

## Pattern 4: Multi-Region Active-Active Streaming

```
Architecture: two regions (us-east-1, eu-west-1) both serving traffic
Challenge: events from both regions must be merged without duplication

                us-east-1                          eu-west-1
                ─────────                          ──────────
  Users → Kafka (primary) ──── MirrorMaker2 ────→ Kafka (replica)
                │                                        │
                Flink job                           Flink job
                │                                        │
                Delta Lake (us-east)               Delta Lake (eu-west)
                                │                        │
                                └──── Delta Sharing ─────┘
                                           │
                               Global Gold tables (read-only)

MirrorMaker2 configuration (bidirectional replication):
  us-east → eu-west: topics = prod.ecommerce.*
  eu-west → us-east: topics = prod.ecommerce.*
  
  Topic renaming: us-east topic → eu-west as us-east.prod.ecommerce.orders
                  eu-west topic → us-east as eu-west.prod.ecommerce.orders
  
  Prevents loops: messages from us-east replicated to eu-west
                  are NOT replicated back (loop detection via __mm2 headers)

Deduplication in Gold:
  Each event has: event_id (UUID), region (us-east/eu-west), event_time
  When merging both regions in Gold:
    MERGE INTO global_orders USING (
      SELECT * FROM delta.`s3://us-east-bucket/silver/orders/`
      UNION ALL
      SELECT * FROM delta.`s3://eu-west-bucket/silver/orders/`
    ) ON global_orders.event_id = source.event_id
    WHEN NOT MATCHED THEN INSERT *
  Idempotent: event_id dedup ensures no duplicates even if same event arrives from both regions

Latency:
  Local events: us-east users see us-east data (< 20ms)
  Cross-region: eu-west users see eu-west data (< 20ms)
  Global merge: Gold table updated every 5 minutes (eventually consistent)
  
  Trade-off: global Gold table is eventually consistent (not strongly consistent)
  Acceptable for: analytics dashboards, reporting
  Not acceptable for: inventory (use DynamoDB Global Tables with strong consistency)
```

---

## Interview Tips

> **Tip 1:** "How do you ensure message ordering in a streaming pipeline?" — Kafka guarantees ordering within a partition. To maintain ordering: always partition by the business key (e.g., `order_id` as the Kafka message key). All events for `order_id=12345` land in the same partition, in order. Flink reads each partition sequentially, so events per key are processed in order. Problems arise when: (a) consumer parallelism > partition count (impossible — Kafka caps consumers at partition count); (b) two jobs write to the same key concurrently (coordinate via Kafka transactions or distributed lock); (c) reprocessing from checkpoint while new events arrive (Kafka's ordering guarantee means the checkpoint offset and subsequent offsets are all in partition order — safe). Never rely on processing time for ordering; use event time + watermarks.

> **Tip 2:** "Walk me through how you would migrate a batch ETL pipeline to streaming without downtime." — Five-phase approach: (1) Deploy streaming pipeline reading from Kafka (new events only) writing to a shadow table; (2) Run both batch and streaming in parallel for 1-2 weeks — compare outputs, validate streaming correctness; (3) Historical backfill: replay Kafka topic from earliest offset (if retention is sufficient) OR run batch job to backfill historical data into the new streaming Delta table; (4) Switch reads to streaming table (dashboards point to new table); (5) Decommission batch ETL. Key safeguards: keep batch ETL running for 1 week after cutover (quick rollback). Monitor: row counts match, aggregates match, no schema differences. Never hard cut-over — always parallel-run first.

> **Tip 3:** "What is the role of Schema Registry in a streaming architecture?" — Schema Registry (Confluent or AWS Glue) serves as a central contract for event schemas. It: (a) enforces compatibility rules — FORWARD compatibility means consumers can read older events with new schema (new fields must have defaults); BACKWARD means producers can write old schema (useful when rolling out new producers); (b) decouples producers from consumers — consumers read the schema ID embedded in each Avro message, fetch schema from Registry, deserialize safely; (c) prevents breaking changes automatically — Registry rejects a schema registration if it violates the configured compatibility mode; (d) enables schema evolution without coordination — add optional fields at any time without redeploying consumers. Without Schema Registry: a schema change in one service can silently corrupt another service's deserialization and cause subtle data corruption bugs.
