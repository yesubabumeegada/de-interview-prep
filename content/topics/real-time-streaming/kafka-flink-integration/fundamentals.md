---
title: "Kafka-Flink Integration - Fundamentals"
topic: real-time-streaming
subtopic: kafka-flink-integration
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [kafka, flink, streaming, integration, source, sink, offsets, consumer-group]
---

# Kafka-Flink Integration — Fundamentals

## 🎯 Analogy

Think of Kafka as an endless conveyor belt of packages (events), and Flink as a smart sorting facility that processes each package. The integration defines how packages move from the belt to the facility, how the facility keeps track of where it left off (offsets), and how processed packages get loaded onto an outbound belt (sink).

---

## Why Kafka + Flink Together?

Kafka and Flink are the most common pairing in modern real-time data pipelines:

```
Kafka strengths:
  - Durable, replayable event log
  - High throughput (millions of events/sec)
  - Decouples producers from consumers
  - Replay capability for reprocessing

Flink strengths:
  - Stateful stream processing
  - Event-time semantics (handles late data)
  - Exactly-once guarantees
  - Rich windowing and join operations

Together:
  Kafka = durable transport + source of truth
  Flink = stateful computation engine
  → Real-time analytics, stream transformations, complex event processing
```

---

## Kafka as a Flink Source

### Old API: FlinkKafkaConsumer (deprecated in Flink 1.15+)

```java
// Legacy — FlinkKafkaConsumer (still seen in older codebases)
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
    "my-topic",
    new SimpleStringSchema(),
    kafkaProperties
);
consumer.setStartFromEarliest();
DataStream<String> stream = env.addSource(consumer);
```

### New API: KafkaSource (Flink 1.14+, preferred)

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders")
    .setGroupId("flink-orders-consumer")
    .setStartingOffsets(OffsetsInitializer.committedOffsets())  // resume from last commit
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> stream = env.fromSource(
    source,
    WatermarkStrategy.forMonotonousTimestamps(),
    "Kafka Orders Source"
);
```

```python
# Python equivalent (PyFlink)
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("orders")
    .set_group_id("flink-orders-consumer")
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
```

---

## Starting Offset Strategies

```
Offset initialization options:

  OffsetsInitializer.earliest()
    → Read from the very beginning of the topic (useful for initial backfill)

  OffsetsInitializer.latest()
    → Start from the tip of the log (miss existing messages, process only new)

  OffsetsInitializer.committedOffsets()
    → Resume from last committed offset (default for resuming jobs)
    → Falls back to "latest" if no committed offsets exist for the group

  OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
    → Resume from committed; fall back to earliest (for new consumer groups)

  OffsetsInitializer.offsets(Map<TopicPartition, Long> specificOffsets)
    → Start from specific offsets per partition (for manual recovery)

  OffsetsInitializer.timestamp(long timestamp)
    → Start from first offset at or after a timestamp (useful for reprocessing)
```

---

## How Flink Tracks Kafka Offsets

```
Offset management with checkpointing:

  Without checkpointing:
    Flink commits offsets to Kafka's __consumer_offsets periodically
    → Risk: offsets committed before processing complete → data loss on restart

  With checkpointing (recommended):
    Flink stores offsets IN the checkpoint (not in Kafka's __consumer_offsets)
    On restart → Flink reads checkpoint → seeks Kafka to checkpointed offset
    Kafka __consumer_offsets also updated (for visibility via kafka-consumer-groups)

  Key config:
    env.enable_checkpointing(60_000)  # 60-second checkpoints
    # In KafkaSource, do NOT set enable.auto.commit=true — Flink manages offsets
```

---

## Kafka as a Flink Sink

### KafkaSink (New API)

```python
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import DeliveryGuarantee

sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("processed-orders")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)  # or EXACTLY_ONCE
    .build()
)

stream.sink_to(sink)
```

### Delivery Guarantees for KafkaSink

```
DeliveryGuarantee.NONE:
  Fire-and-forget. Fastest, no retries.

DeliveryGuarantee.AT_LEAST_ONCE:
  Retries on failure. Possible duplicates if producer retries.
  Requires: checkpointing enabled.

DeliveryGuarantee.EXACTLY_ONCE:
  Kafka transactions. No duplicates.
  Requires: checkpointing + transactional.id.prefix configured.
  Consumers must use isolation.level=read_committed.
```

---

## Flink SQL Kafka Connector

Flink SQL provides a declarative way to read from and write to Kafka:

```sql
-- Create Kafka source table
CREATE TABLE orders_kafka (
  order_id    STRING,
  customer_id STRING,
  amount      DOUBLE,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'            = 'kafka',
  'topic'                = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-orders',
  'scan.startup.mode'   = 'latest-offset',
  'format'              = 'json',
  'json.fail-on-missing-field' = 'false'
);

-- Create Kafka sink table
CREATE TABLE processed_orders_kafka (
  order_id    STRING,
  total       DOUBLE,
  window_end  TIMESTAMP(3)
) WITH (
  'connector'            = 'kafka',
  'topic'                = 'processed-orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format'              = 'json'
);

-- Query: aggregate and write to sink
INSERT INTO processed_orders_kafka
SELECT
  customer_id,
  SUM(amount) AS total,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end
FROM orders_kafka
GROUP BY customer_id, TUMBLE(event_time, INTERVAL '1' MINUTE);
```

---

## Key Terms

| Term | Definition |
|------|-----------|
| KafkaSource | New Flink source API (Flink 1.14+), replaces FlinkKafkaConsumer |
| KafkaSink | New Flink sink API, supports AT_LEAST_ONCE and EXACTLY_ONCE |
| consumer group.id | Identifies the Flink job's consumer group in Kafka |
| Committed offset | The offset stored in Kafka's `__consumer_offsets` topic |
| Checkpointed offset | Offset stored inside the Flink checkpoint (authoritative for recovery) |
| DeliveryGuarantee | Enum controlling KafkaSink delivery semantics |
