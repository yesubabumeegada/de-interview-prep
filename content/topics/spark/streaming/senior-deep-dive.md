---
title: "Spark Streaming — Senior Deep Dive"
topic: spark
subtopic: streaming
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, structured-streaming, offset-management, state-store, rocksdb, exactly-once, continuous-processing]
---

# Spark Streaming — Senior Deep Dive

## Offset Management Internals

Structured Streaming tracks source offsets in the checkpoint directory:

```
checkpoint/
├── commits/         ← completed batches (0, 1, 2, ...)
├── offsets/         ← planned offsets per batch
├── metadata         ← query ID, schema
└── state/           ← aggregation state (RocksDB or HDFS)
```

The two-phase commit protocol:
```
1. Driver reads source → writes planned offsets to offsets/{batchId}
2. Tasks process data and write to sink
3. Sink confirms write (Delta: transaction commit, Kafka: producer commit)
4. Driver writes batchId to commits/{batchId}

On restart:
  - If batchId in offsets/ but NOT commits/ → replay batch (sink must be idempotent)
  - If batchId in both commits/ → batch done, advance to next
```

This guarantees exactly-once processing with idempotent sinks (Delta Lake, Kafka transactions, JDBC upsert).

---

## State Store: In-Memory vs. RocksDB

Spark 3.2 introduced the **RocksDB State Store** as an alternative to the default HDFS-backed state:

| | Default (HDFS) State Store | RocksDB State Store |
|--|---|---|
| **Storage** | Executor in-memory + HDFS checkpoint | RocksDB on executor local disk |
| **State size limit** | Executor heap (GC pressure for large state) | Multi-TB (disk-backed, off-heap) |
| **Checkpointing** | Full snapshot to HDFS each batch | Incremental — only changed keys |
| **Recovery** | Load full snapshot from HDFS | Load from last incremental checkpoint |
| **Latency** | Low for small state | Low for all sizes; slightly more overhead for tiny state |

```python
# Enable RocksDB state store
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# Tuning:
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "false")
```

Use RocksDB when:
- State per key is large (ML features, session history)
- State has many keys (millions of users)
- Default store causes GC pauses or OOM

---

## Continuous Processing Mode

Continuous processing (experimental, Spark 2.3+) achieves millisecond latency by running a continuous query instead of micro-batches:

```python
query = stream.writeStream \
    .trigger(continuous="100 milliseconds") \  # checkpoint every 100ms
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "output") \
    .start()
```

**Constraints:**
- Only stateless operations (filter, project, map)
- No aggregations, joins, or user-defined functions
- Supported sources: Kafka, Rate; Supported sinks: Kafka, Console, Memory

**When to use:** Requires < 10ms end-to-end latency. For most DE use cases, micro-batch with 1-second triggers achieves 5-10 second latency — sufficient and much more reliable.

---

## Structured Streaming Internals: MicroBatch Execution

```python
# What happens each trigger interval:
# 1. IncrementalExecution.executedPlan() runs planning
# 2. Source.getOffset() → new maximum offset available
# 3. Source.getBatch(start, end) → DataFrame for this batch
# 4. Physical plan executed (DAGScheduler → tasks)
# 5. Sink writes result
# 6. Source.commit(offset) called
# 7. Checkpoint updated (commits/N)

# Monitor batch timing:
query.lastProgress["durationMs"]
# {"triggerExecution": 3200, "getBatch": 100, "queryPlanning": 40,
#  "walCommit": 15, "commitOffsets": 20, "latestOffset": 50}

# Key metric: triggerExecution >> processingTime interval → falling behind!
# If 30s trigger but batchDuration = 45s: you're falling behind by 15s/batch
```

---

## Backpressure and Rate Limiting

```python
# Control how much data is read per trigger
# Kafka: limit max records per partition per trigger
spark.conf.set("maxOffsetsPerTrigger", "50000")  # total across all partitions

# Or per-partition:
stream = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", "50000") \
    .load()

# Rate source: built-in
spark.conf.set("spark.streaming.backpressure.enabled", "true")  # DStream only

# For Structured Streaming: use maxOffsetsPerTrigger + monitor lag
# Lag = (latest offset - committed offset) per partition
# Available via Kafka AdminClient or streaming UI
```

---

## State Store Operational Challenges

```python
# State row count growing unboundedly = logic bug
# Check: query.lastProgress["stateOperators"][0]["numRowsTotal"]
# Should plateau or slowly increase; monotonic growth = leak

# Common causes of state leaks:
# 1. Missing watermark → Spark never drops old state
df.withWatermark("event_time", "2 hours")   # REQUIRED for stateful ops

# 2. Timeout not set in mapGroupsWithState
state.setTimeoutDuration("1 hour")   # REQUIRED; else state lives forever

# 3. State key cardinality explosion
# If groupBy("user_id", "session_id") and session_id is UUID, state grows forever
# Use meaningful sessionization, not random IDs as keys

# Debugging state:
import requests
state_info = requests.get("http://driver:4040/api/v1/applications/{appId}/streaming/statistics")
```

---

## Exactly-Once Sink Patterns

```python
# Pattern 1: Delta Lake — transactional sink (recommended)
def write_delta(batch_df, batch_id):
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save("s3://bucket/delta/events/")
# Delta's transaction log ensures each batch is committed atomically

# Pattern 2: Idempotent upsert for databases
def write_postgres_upsert(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("dbtable", "events_staging") \
        .mode("overwrite") \
        .save()
    spark.sql(f"""
        INSERT INTO events
        SELECT * FROM events_staging
        ON CONFLICT (event_id) DO UPDATE
        SET amount = EXCLUDED.amount, updated_at = EXCLUDED.updated_at
    """)

# Pattern 3: Idempotent by batch_id
def write_with_batch_id(batch_df, batch_id):
    with_id = batch_df.withColumn("_batch_id", F.lit(batch_id))
    # Delete old batch if exists (replay scenario)
    spark.sql(f"DELETE FROM events WHERE _batch_id = {batch_id}")
    with_id.write.format("delta").mode("append").save(path)
```

---

## Interview Tips

> **Tip 1:** "How does Structured Streaming achieve exactly-once semantics?" — Two-phase commit: Spark writes planned offsets before processing, then writes a commit record after the sink confirms. On recovery, if offsets exist but no commit, Spark replays the batch — so the sink must be idempotent. Delta Lake handles this via atomic transactions. For custom sinks, use upsert or batch_id deduplication. The source (Kafka) provides exactly-once reads via offset tracking in the checkpoint.

> **Tip 2:** "When would you use the RocksDB state store?" — When state per executor is large (tens of GB, millions of keys) or when the default HDFS state store is causing JVM GC pressure. Default HDFS state keeps everything in executor heap — GC pauses scale with state size. RocksDB stores state on local disk off-heap, checkpoints incrementally (much faster than full snapshots), and can handle multi-TB state. Required for any production-scale sessionization or feature store use case.

> **Tip 3:** "What causes a streaming job to fall behind and how do you fix it?" — "Falling behind" means each batch takes longer to process than the trigger interval. Root causes: (1) Input rate spike — use `maxOffsetsPerTrigger` to limit reads per batch. (2) State explosion — state store becoming huge; add or tighten watermarks, fix state timeout logic. (3) Slow sink — sink write taking too long; switch to Delta or buffer writes. (4) Data skew — some partitions getting much more data; repartition by balanced key. Monitor `triggerExecution` in `lastProgress` — if it exceeds trigger interval, you're falling behind.
