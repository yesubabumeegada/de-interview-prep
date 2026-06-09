---
title: "Spark Structured Streaming — Senior Deep Dive"
topic: real-time-streaming
subtopic: spark-structured-streaming
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, structured-streaming, continuous-processing, state-store, rocksdb, production, tuning]
---

# Spark Structured Streaming — Senior Deep Dive

## Streaming Execution Model

```
Micro-batch execution (default):
  1. Trigger fires (e.g., every 30 seconds)
  2. Driver determines new data offsets (Kafka: check latest offsets)
  3. Driver schedules tasks across executors
  4. Executors process the batch:
     a. Read from source (Kafka, S3, Delta)
     b. Apply transformations
     c. Update state store (for stateful operations)
     d. Write to sink
  5. Driver commits: offsets written to checkpoint
  6. Repeat

Continuous processing mode (Spark 3+, experimental):
  - Epoch-based: ~1ms latency instead of ~100ms
  - Limitation: no stateful operations, no aggregations
  - Only for simple stateless transforms (filter, map, select)
  - Not widely adopted in production

State Store:
  Default: HDFS-based state store (write-ahead log to checkpoint dir)
  RocksDB state store (Spark 3.2+, Databricks):
    - In-memory + on-disk (same as Flink's RocksDB backend)
    - Supports TB-scale state
    - Enable:
      spark.conf.set("spark.sql.streaming.stateStore.providerClass",
          "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
      spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
  
  State size monitoring:
    query.lastProgress["stateOperators"][0]["memoryUsedBytes"]
    query.lastProgress["stateOperators"][0]["numRowsTotal"]
```

---

## Advanced Watermark and Late Data Strategies

```python
from pyspark.sql.functions import *

# Multiple watermarks in one query
# When joining two streams with different watermarks:
# Global watermark = min(watermark1, watermark2)
# → the slower stream controls how fast state is cleaned up

orders_wm = orders.withWatermark("event_time", "10 minutes")
payments_wm = payments.withWatermark("payment_time", "20 minutes")

# Global watermark = min(current_max_order_time - 10min, current_max_payment_time - 20min)
# If payments are slow (20 min delay), state is kept longer → more memory

# Strategy for asymmetric lateness:
# Put tighter watermark on the stream with less latency
# Use larger watermark on the stream with more latency

# Stateful operations with TTL (mapGroupsWithState / flatMapGroupsWithState)
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql import functions as F

def update_session_state(user_id, events, state: GroupState):
    """
    Session tracking: update user session state per micro-batch.
    State expires after 30 minutes of inactivity.
    """
    if state.hasTimedOut:
        # Session expired (30 min inactivity) — emit final session record
        session = state.get
        state.remove()
        yield Session(user_id, session["start"], state.getCurrentWatermarkMs(), 
                      session["page_count"])
        return
    
    # Update state with new events
    new_events = list(events)
    if state.exists:
        current = state.get
        updated = {
            "start": current["start"],
            "page_count": current["page_count"] + len(new_events),
            "last_activity": max(e.event_time for e in new_events)
        }
    else:
        updated = {
            "start": min(e.event_time for e in new_events),
            "page_count": len(new_events),
            "last_activity": max(e.event_time for e in new_events)
        }
    
    state.update(updated)
    state.setTimeoutDuration("30 minutes")  # reset timeout on activity

# Apply stateful function
sessions = events \
    .withWatermark("event_time", "10 minutes") \
    .groupBy("user_id") \
    .applyInPandasWithState(
        update_session_state,
        output_schema="user_id string, start timestamp, end timestamp, pages int",
        state_schema="start timestamp, page_count int, last_activity timestamp",
        output_mode="append",
        timeout_conf=GroupStateTimeout.EventTimeTimeout
    )
```

---

## Performance Tuning

```python
# Production tuning checklist

# 1. Partition optimization
spark.conf.set("spark.sql.shuffle.partitions", "200")  # for joins/aggregations
# Rule: 2-4 partitions per CPU core; adjust for batch size

# 2. Kafka source rate limiting (back-pressure)
spark.readStream.format("kafka") \
    .option("maxOffsetsPerTrigger", 500_000) \  # limit records per batch
    .option("minPartitions", 32)                 # repartition if < 32 Kafka partitions

# 3. Trigger interval tuning
# Goal: trigger interval > batch processing time (avoid queuing)
# Monitor: query.lastProgress["triggerExecution"]["batchId"] gaps
# If processing time > trigger interval → increase trigger interval or parallelism

# 4. State store tuning (RocksDB)
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB", "256")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB", "64")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.maxWriteBufferNumber", "3")

# 5. Checkpoint optimization
# Checkpoint to fast storage (Azure ADLS Gen2, S3 with acceleration)
# Avoid NFS or slow HDFS for checkpoint
# Checkpoint frequency: every 5-10 minutes for stable jobs (reduces checkpoint overhead)

# 6. Memory tuning
# --executor-memory 8g
# --conf spark.executor.memoryOverhead=2048  # for off-heap (state store, native libs)
# --conf spark.memory.fraction=0.6            # fraction for execution+storage
# --conf spark.memory.storageFraction=0.5     # of memory.fraction for caching

# 7. Monitoring key metrics
import json

def log_stream_progress(query):
    progress = query.lastProgress
    print(f"""
    Batch ID:         {progress['batchId']}
    Processing time:  {progress['triggerExecution']['batchTriggerDeltaMs']}ms
    Input rows/sec:   {progress['inputRowsPerSecond']:.0f}
    Process rows/sec: {progress['processedRowsPerSecond']:.0f}
    State rows:       {sum(s.get('numRowsTotal',0) for s in progress.get('stateOperators',[]))}
    State mem (MB):   {sum(s.get('memoryUsedBytes',0) for s in progress.get('stateOperators',[])) / 1024**2:.1f}
    Watermark:        {progress.get('eventTime', {}).get('watermark', 'N/A')}
    """)
```

---

## Exactly-Once Semantics Deep Dive

```python
"""
Exactly-once in Spark Structured Streaming:

Source side (idempotent reads):
  Kafka: offsets stored in checkpoint → restart reads from committed offsets
  Delta: version-based reads → restart reads from saved version
  
Processing side:
  Stateful: state stored in checkpoint → restored on restart
  Stateless: deterministic transforms re-run on replayed data

Sink side:
  Requires idempotent or transactional sink:
  
  1. Delta Lake (recommended):
     Structured Streaming writes Delta transaction log entries
     Each micro-batch = one Delta commit (atomic)
     Retry: Spark checks Delta log for batch ID → skips if already committed
     Enable idempotent writes:
       .option("txnAppId", "my-streaming-job")    # unique job identifier
       .option("txnVersion", str(batch_id))        # monotonic batch ID
     
  2. Kafka (transactional producer):
     .option("kafka.transactional.id", "flink-tx-")
     .option("kafka.isolation.level", "read_committed")
     
  3. JDBC (custom with foreachBatch):
     Use batch_id as idempotency key:
     MERGE INTO target USING (SELECT * FROM source WHERE batch_id = ?) 
     ON target.batch_id = source.batch_id WHEN NOT MATCHED THEN INSERT ALL
     
  4. REST API:
     Include X-Idempotency-Key header = SHA256(job_id + batch_id + record_id)
     API server: check key in Redis → if seen, return 200 without re-processing

Gotcha: foreachBatch can be called more than once for the same batch_id on failure
  → your function MUST be idempotent (safe to run twice with same data)
  → never use INSERT without conflict handling in foreachBatch
"""

# Production-grade exactly-once Delta write in foreachBatch
def exactly_once_delta_write(batch_df, batch_id):
    """
    Write to Delta with idempotency.
    Spark may call this function twice for the same batch_id on failure.
    Delta's txnAppId + txnVersion makes it exactly-once.
    """
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .option("txnAppId", "order-streaming-job-v1") \
        .option("txnVersion", str(batch_id)) \
        .save("s3://bucket/delta/orders/")

stream.writeStream \
    .foreachBatch(exactly_once_delta_write) \
    .option("checkpointLocation", "s3://bucket/checkpoints/orders/") \
    .start()
```

---

## Interview Tips

> **Tip 1:** "How do you debug a Spark Structured Streaming job that is falling behind (processing slower than incoming rate)?" — Check `query.lastProgress["processedRowsPerSecond"]` vs `inputRowsPerSecond`. If input > processed: job is falling behind. Diagnose via Spark UI → Streaming tab → check which stage takes the longest. Common causes: (a) state store too large — RocksDB thrashing on disk; (b) shuffle partitions too high/low; (c) sink is the bottleneck (JDBC write too slow); (d) skewed data — one key has 90% of events. Fixes: increase executor count, tune `maxOffsetsPerTrigger` to reduce batch size, optimize sink (batch writes), add RocksDB state store for large state.

> **Tip 2:** "What is the difference between `mapGroupsWithState` and `flatMapGroupsWithState`?" — Both enable arbitrary stateful operations with custom state per key. `mapGroupsWithState`: returns exactly one output row per group per batch (must always emit). `flatMapGroupsWithState`: returns zero or more output rows per group per batch (can emit nothing, or multiple rows). Use `flatMapGroupsWithState` for session windowing (emit session record only when session expires), complex event detection (emit alert only when pattern matches), or accumulating batches (emit once threshold reached). Use `mapGroupsWithState` for running aggregations where you always want current state output (e.g., latest metric per device).

> **Tip 3:** "How does Spark Structured Streaming handle schema evolution in Delta Lake sinks?" — Enable schema evolution on the Delta table: `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`. With this, if the streaming DataFrame adds new columns, Delta merges the schema on write. Without it, schema mismatches cause job failure. Best practice: (a) in Bronze, always write raw strings (never enforce schema on write); (b) in Silver foreachBatch, validate schema explicitly and route bad rows to DLQ; (c) for Gold, schema changes should go through a migration process (add column → backfill → update downstream). Auto-merge is convenient but hides schema drift — only use with monitoring on the schema change event log.
