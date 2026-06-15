---
title: "Kafka-Flink Integration - Intermediate"
topic: real-time-streaming
subtopic: kafka-flink-integration
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [kafka, flink, streaming, schema-registry, avro, watermarks, parallelism, backpressure]
---

# Kafka-Flink Integration — Intermediate

## Partitioning and Parallelism Alignment

One of the most important performance considerations is aligning Kafka partition count with Flink parallelism.

```
Kafka partitions → Flink source subtasks:

  Kafka topic: 8 partitions
  Flink source parallelism: 8
  → Each source subtask reads exactly 1 partition (ideal)

  Kafka topic: 8 partitions
  Flink source parallelism: 4
  → Each source subtask reads 2 partitions (acceptable)

  Kafka topic: 8 partitions
  Flink source parallelism: 12
  → 4 source subtasks are idle (wasted resources!)
  → Cannot have more source parallelism than partitions

  Rule: set Flink source parallelism = Kafka partition count
        or a divisor of it (e.g., 4 for 8 partitions)
```

```python
# Set parallelism at the source level
source = KafkaSource.builder()...build()

stream = env.from_source(
    source,
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
    "Kafka Source"
)
stream = stream.set_parallelism(8)  # match Kafka partition count
```

---

## Schema Registry Integration

Using Avro or Protobuf schemas with the Confluent Schema Registry prevents schema drift and enables schema evolution.

### Avro Deserialization in Flink

```java
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

// Flink connector: ConfluentRegistryAvroDeserializationSchema
KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders-avro")
    .setGroupId("flink-avro-consumer")
    .setStartingOffsets(OffsetsInitializer.committedOffsets())
    .setDeserializer(
        KafkaRecordDeserializationSchema.valueOnly(
            ConfluentRegistryAvroDeserializationSchema.forGeneric(
                schema,  // Avro Schema object
                "http://schema-registry:8081"
            )
        )
    )
    .build();
```

```python
# PyFlink: custom Avro deserializer
from pyflink.common.serialization import DeserializationSchema
import fastavro
import io
import requests
import struct

class ConfluentAvroDeserializer(DeserializationSchema):
    def __init__(self, schema_registry_url: str, topic: str):
        self.registry_url = schema_registry_url
        self.topic = topic
        self._schema_cache = {}

    def deserialize(self, message: bytes) -> dict:
        # Confluent wire format: magic byte (1) + schema_id (4) + avro payload
        magic, schema_id = struct.unpack('>bI', message[:5])
        avro_bytes = message[5:]

        if schema_id not in self._schema_cache:
            resp = requests.get(f"{self.registry_url}/schemas/ids/{schema_id}")
            self._schema_cache[schema_id] = fastavro.parse_schema(resp.json()["schema"])

        schema = self._schema_cache[schema_id]
        return fastavro.schemaless_reader(io.BytesIO(avro_bytes), schema)

    def is_end_of_stream(self, record) -> bool:
        return False
```

---

## Watermark Strategies for Kafka

### Event-Time vs. Ingestion-Time

```
Event-time (preferred for correctness):
  - Timestamps embedded in the message payload
  - Flink uses these for windowing and joins
  - Handles late data correctly (with watermarks)
  - Requires timestamps in the data

Ingestion-time (simpler, less accurate):
  - Flink uses the time the record enters the Flink job
  - No late data handling (stream is always "on time")
  - Use when: event timestamps are unreliable or missing
```

### Watermark Configuration

```java
// Bounded out-of-orderness: tolerate up to 5s late arrivals
WatermarkStrategy<Order> watermarkStrategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((order, recordTimestamp) -> order.getEventTime());

DataStream<Order> stream = env.fromSource(
    kafkaSource,
    watermarkStrategy,
    "Kafka Orders"
);
```

```java
// Per-partition watermarks (important for Kafka):
// Each Kafka partition may have different lag.
// Flink generates watermarks per partition and takes the minimum.
// This prevents one slow partition from blocking the entire pipeline.

// By default, KafkaSource uses per-partition watermarks.
// If one partition is idle (no messages), configure:
WatermarkStrategy<Order> strategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner(...)
    .withIdleness(Duration.ofSeconds(30));  // mark partition idle after 30s of no messages
```

---

## Kafka → Flink → Iceberg/Delta Streaming Lakehouse

```
Streaming lakehouse pattern:

  Kafka (raw events) → Flink (transform + enrich) → Iceberg/Delta (analytical store)
                                                          ↓
                                                    dbt / Spark SQL (batch queries)

  Benefits:
  - Real-time data available for analytics within seconds
  - ACID transactions in the lakehouse (no partial writes)
  - Time travel for debugging and reprocessing
  - Single storage layer for streaming and batch
```

```sql
-- Flink SQL: Kafka → Iceberg streaming write
CREATE CATALOG iceberg_catalog WITH (
  'type'            = 'iceberg',
  'catalog-type'    = 'hive',
  'uri'             = 'thrift://hive-metastore:9083',
  'warehouse'       = 's3://my-bucket/warehouse'
);

CREATE TABLE iceberg_catalog.prod.orders (
  order_id    STRING,
  customer_id STRING,
  amount      DOUBLE,
  event_time  TIMESTAMP(3)
) PARTITIONED BY (DATE_FORMAT(event_time, 'yyyy-MM-dd'));

-- Stream from Kafka to Iceberg
INSERT INTO iceberg_catalog.prod.orders
SELECT
  order_id,
  customer_id,
  amount,
  TO_TIMESTAMP(FROM_UNIXTIME(event_time_ms / 1000)) AS event_time
FROM orders_kafka;
```

---

## Backpressure: Kafka to Flink

Backpressure occurs when Flink cannot process records as fast as Kafka delivers them.

```
Backpressure flow:

  Kafka → KafkaSource operator (buffer fills up)
        → Source slows poll rate automatically
        → Kafka consumer lag increases
        → Alert: consumer lag > threshold

  Flink backpressure indicators (Flink Web UI):
    - "Back Pressured" badge on operators
    - High input buffer usage
    - Low output buffer usage

  Common causes:
    1. Expensive operator (slow UDF, external call without async I/O)
    2. Data skew (one key has disproportionate load)
    3. Insufficient parallelism
    4. Slow sink (downstream system overloaded)
```

```python
# Fix: use AsyncFunction for external lookups instead of blocking calls
from pyflink.datastream import AsyncDataStream
from pyflink.datastream.functions import AsyncFunction, ResultFuture

class AsyncRedisLookup(AsyncFunction):
    def open(self, config):
        import aioredis
        self.redis = aioredis.from_url("redis://redis:6379")

    async def async_invoke(self, order: dict, result_future: ResultFuture):
        account_info = await self.redis.hgetall(f"account:{order['account_id']}")
        enriched = {**order, "account_name": account_info.get(b"name", b"").decode()}
        result_future.complete([enriched])

enriched_stream = AsyncDataStream.unordered_wait(
    stream,
    AsyncRedisLookup(),
    timeout=5,
    time_unit=TimeUnit.SECONDS,
    capacity=100  # max 100 inflight async requests
)
```

---

## Kafka MirrorMaker 2 for Geo-Replication

```
MirrorMaker 2 (MM2) — active-passive replication:

  Source cluster (us-east-1)     Target cluster (eu-west-1)
  ─────────────────────────────────────────────────────────
  Topic: orders         →  MM2  →  Topic: us-east-1.orders (prefixed)
  Consumer groups       →  MM2  →  Offset translation (checkpoints)

  Flink job consuming from target cluster:
  - Reads from us-east-1.orders on eu-west-1 cluster
  - Uses RemoteClusterHighWatermark or translated offsets

  Key MM2 configuration:
  clusters = us-east-1, eu-west-1
  us-east-1.bootstrap.servers = kafka-us:9092
  eu-west-1.bootstrap.servers = kafka-eu:9092
  us-east-1->eu-west-1.enabled = true
  us-east-1->eu-west-1.topics = orders, payments
  replication.factor = 3
  sync.topic.acls.enabled = false
```

---

## Monitoring: Key Metrics

```
Flink Web UI metrics:
  - Records per second in/out per operator
  - Backpressure ratio (0 = no backpressure, 1 = fully backpressured)
  - Checkpoint duration and size
  - Task manager memory and GC metrics

Kafka metrics to watch:
  - consumer_lag (records behind latest offset)
    Alert threshold: > 10K records sustained for > 5 minutes
  - fetch_latency_avg (slow broker or network issues)
  - records_consumed_rate (should match throughput expectations)

Prometheus scrape config for Flink:
  - Flink exposes metrics via JMX or Prometheus reporter
  - Key metric: flink_jobmanager_job_lastCheckpointDuration (ms)
  - Alert if > 60_000 (1 minute — indicates state too large or slow S3)

Dashboard panels:
  1. Kafka consumer lag over time (per topic/partition)
  2. Flink checkpoint success rate (should be 100%)
  3. End-to-end latency (event_time to sink_write_time)
  4. Records processed per second per operator
```

---

## Interview Q&A

**Q: What happens if Kafka has 16 partitions but Flink source parallelism is 4?**
A: Each of the 4 source subtasks will read from 4 Kafka partitions. This is valid — each subtask handles multiple partitions round-robin. Performance may be limited vs. 16-way parallelism. You cannot set source parallelism higher than partition count.

**Q: Why use per-partition watermarks in Flink-Kafka integration?**
A: Different Kafka partitions may have different event-time distributions. If one partition has no messages (idle), its watermark would stall the global watermark, blocking all windows from triggering. Per-partition watermarks with idleness timeout prevent this.

**Q: How does the Schema Registry prevent schema drift in a Flink pipeline?**
A: The Schema Registry enforces a contract: producers register schemas before writing, and schemas can only evolve in compatible ways (e.g., adding optional fields). Flink deserializes using the schema ID embedded in each message, fetched from the registry. Incompatible changes are rejected at the producer, preventing malformed records from reaching Flink.
