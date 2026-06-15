---
title: "Exactly-Once Semantics - Intermediate"
topic: real-time-streaming
subtopic: exactly-once-semantics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, exactly-once, kafka, flink, spark, checkpointing, two-phase-commit]
---

# Exactly-Once Semantics — Intermediate

## Flink Exactly-Once: Checkpointing + Two-Phase Commit

Flink achieves exactly-once by combining **distributed snapshots (checkpoints)** with a **two-phase commit (2PC) protocol** for external sinks.

```
Flink checkpoint + sink commit flow:

  Source (Kafka)   Flink Operators   Sink (Kafka / DB)
  ──────────────   ───────────────   ─────────────────
  Read offset 100  Process events    Pre-commit (barrier)
       ↓                ↓                    ↓
  Checkpoint barrier propagates through all operators
       ↓                ↓                    ↓
  State snapshots  State snapshots   notifyCheckpointComplete()
  saved to S3/GCS  saved to S3/GCS   → commitTransaction()

  On recovery:
    Flink reloads state from last successful checkpoint
    Kafka source resets to checkpointed offset
    Sink re-commits or rolls back based on checkpoint state
```

### Flink Kafka Source (New API)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60_000)  # checkpoint every 60 seconds
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("orders")
    .set_group_id("flink-orders-consumer")
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    source,
    WatermarkStrategy.for_monotonous_timestamps(),
    "Kafka Orders Source"
)
```

### Flink Kafka Sink with Exactly-Once

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
    .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)  # uses Kafka transactions
    .set_transactional_id_prefix("flink-orders-sink")        # unique prefix
    .build()
)

stream.sink_to(sink)
env.execute("Exactly-Once Orders Pipeline")
```

**Key requirement**: The Kafka sink's `transactional.id` prefix must be unique per Flink job. Flink appends the subtask index and checkpoint ID automatically.

---

## Spark Structured Streaming Exactly-Once

Spark uses a different approach:

```
Spark exactly-once mechanisms:

  1. Write-Ahead Log (WAL):
     - Received data logged to fault-tolerant storage before processing
     - On restart, replay from WAL

  2. Idempotent sinks:
     - Spark generates unique batch IDs
     - Sink uses batch ID to deduplicate re-runs

  3. Checkpoint location:
     - Tracks offsets read from source
     - Tracks watermarks and state
     - Re-uses same offsets on restart → no data loss or re-processing
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType

spark = SparkSession.builder.appName("ExactlyOnceOrders").getOrCreate()

schema = StructType() \
    .add("order_id", StringType()) \
    .add("amount", LongType()) \
    .add("event_time", LongType())

# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write with exactly-once using foreachBatch + idempotent upsert
def upsert_to_delta(batch_df, batch_id):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, "/data/delta/orders")
    (
        delta_table.alias("target")
        .merge(batch_df.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    parsed.writeStream
    .option("checkpointLocation", "/checkpoints/orders")  # required for exactly-once
    .foreachBatch(upsert_to_delta)
    .trigger(processingTime="30 seconds")
    .start()
)
```

**Why `foreachBatch` + MERGE gives exactly-once**: if a batch re-runs (Spark restarts), the same batch_id is replayed. Since MERGE is idempotent (upsert by order_id), re-running produces the same result.

---

## Deduplication Patterns

### Redis Bloom Filter

```python
import redis
from bloomfilter import BloomFilter  # redis-py-bloom

r = redis.Redis(host='redis', port=6379)

def is_duplicate(event_id: str) -> bool:
    # Bloom filter: fast probabilistic check (false positives possible, no false negatives)
    bf_key = "processed_events_bf"
    if r.bf().exists(bf_key, event_id):
        return True  # Probably a duplicate (check exact set for confirmation)
    r.bf().add(bf_key, event_id)
    return False

# More accurate: use a Redis Set with TTL
def is_duplicate_exact(event_id: str, ttl_seconds: int = 86400) -> bool:
    key = f"seen:{event_id}"
    added = r.set(key, 1, nx=True, ex=ttl_seconds)  # nx=True: only set if not exists
    return added is None  # None means key already existed → duplicate
```

### Flink Stream-Stream Deduplication

```java
// Java: Flink deduplication using KeyedProcessFunction
public class DeduplicationFunction extends KeyedProcessFunction<String, Order, Order> {
    private ValueState<Boolean> seenState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor =
            new ValueStateDescriptor<>("seen", Boolean.class);
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<Order> out) throws Exception {
        if (seenState.value() == null) {
            seenState.update(true);
            // Register timer to clean up state after 1 hour
            ctx.timerService().registerEventTimeTimer(
                ctx.timestamp() + 3_600_000L
            );
            out.collect(order);
        }
        // else: duplicate — discard
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Order> out) {
        seenState.clear();  // Clean up state to avoid unbounded growth
    }
}
```

### Delta Lake MERGE Deduplication

```sql
-- Upsert into Delta table, deduplicating on order_id
MERGE INTO delta.`/data/delta/orders` AS target
USING (
  SELECT order_id, amount, event_time,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC) AS rn
  FROM new_orders_staging
) AS source
ON target.order_id = source.order_id AND source.rn = 1
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## Exactly-Once to External Systems

```
External system support matrix:

  System              Exactly-Once Method           Notes
  ──────────────────────────────────────────────────────────────────
  Kafka               Transactions (2PC)             Native support
  Delta Lake          ACID MERGE + checkpoint        Idempotent writes
  PostgreSQL          ON CONFLICT DO UPDATE          Upsert by dedup key
  Elasticsearch       _id-based upsert (PUT)         Retry-safe
  HDFS/S3             Atomic rename + checkpoint     WAL approach
  DynamoDB            Conditional writes             Put if version matches
  BigQuery            Load job IDs (idempotent)      Retry same job ID

  No native support (hard):
  - Arbitrary REST APIs without idempotency keys
  - Non-ACID databases
  - S3 prefix writes without atomic rename
```

---

## The End-to-End Exactly-Once Illusion

True exactly-once across heterogeneous systems is often **emulated** rather than native:

```
End-to-end exactly-once stack:

  Kafka (source)
    → enable.idempotence + read_committed consumer
  Flink (processing)
    → checkpointing + TwoPhaseCommitSinkFunction
  Delta Lake (sink)
    → ACID transactions + MERGE with dedup key

  Each layer handles its own guarantee:
  - Kafka: no duplicate messages delivered to Flink
  - Flink: no duplicate processing (state restored from checkpoint)
  - Delta: no duplicate rows (MERGE is idempotent)

  Break any link → lose the guarantee
```

---

## Interview Q&A

**Q: What's the difference between idempotent producer and Kafka transactions?**
A: Idempotent producer prevents duplicates on a single partition within one session. Kafka transactions extend this to atomic writes across multiple partitions/topics and survive producer restarts via `transactional.id`.

**Q: How does Flink implement exactly-once with a Kafka sink?**
A: Flink uses `TwoPhaseCommitSinkFunction`. At checkpoint barrier, the sink pre-commits (opens a Kafka transaction). When checkpoint completes, `notifyCheckpointComplete()` commits the transaction. On failure, uncommitted transactions are aborted on restart.

**Q: Can Spark Structured Streaming guarantee exactly-once to any sink?**
A: No. Spark guarantees exactly-once only with sinks that support idempotent writes (Delta Lake MERGE, database upserts). For arbitrary sinks, `foreachBatch` with idempotent logic is the pattern. Non-idempotent sinks (plain INSERT) give at-least-once.
