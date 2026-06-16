---
title: "Spark Streaming Interview Scenarios"
description: "Scenarios covering Structured Streaming, watermarks, triggers, and stateful processing"
content_type: scenario_question
topic: spark
subtopic: streaming
tags: [spark, structured-streaming, kafka, watermark, stateful, micro-batch, continuous]
---

<article data-difficulty="junior">

## Scenario: Structured Streaming vs DStreams — Why Did Spark Deprecate DStreams?

An interviewer asks: "What is the difference between Spark Structured Streaming and the original Spark Streaming (DStreams)? Why did Spark move away from DStreams?"

<details>
<summary>✅ Solution</summary>

### What Were DStreams (Spark Streaming)?

**DStreams (Discretized Streams)** were Spark's original streaming API, introduced in Spark 1.0 (2013). The model:

1. Divide the continuous stream into fixed-size time batches (e.g., 2-second windows)
2. Each batch creates an RDD
3. Process each RDD with the standard RDD API
4. Results written at the end of each batch

```python
# DStream API (deprecated)
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, batchDuration=2)   # 2-second micro-batches
lines = ssc.socketTextStream("localhost", 9999)
word_counts = lines.flatMap(lambda x: x.split()) \
                   .map(lambda w: (w, 1)) \
                   .reduceByKey(lambda a, b: a + b)
word_counts.print()
ssc.start()
ssc.awaitTermination()
```

### What is Structured Streaming?

**Structured Streaming** (Spark 2.0+, 2016) treats the stream as an **unbounded table** that grows over time. You write the same DataFrame/SQL API you use for batch jobs:

```python
# Structured Streaming API (current)
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

spark = SparkSession.builder.getOrCreate()

# Read stream as if it's a table
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input-topic") \
    .load()

words = lines.select(
    explode(split(col("value").cast("string"), " ")).alias("word")
)
word_counts = words.groupBy("word").count()

# Write continuously
query = word_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
```

### Side-by-Side Comparison

| Feature | DStreams (Deprecated) | Structured Streaming |
|---|---|---|
| Abstraction | RDD (low-level) | DataFrame / Dataset / SQL (high-level) |
| API | Streaming-specific methods | Same as batch API |
| Catalyst optimizer | No | Yes — query optimization automatic |
| Fault tolerance | Receiver checkpointing | Write-ahead log + offset tracking |
| Exactly-once semantics | Difficult, manual | Built-in (with idempotent sinks) |
| Event time processing | Workarounds needed | First-class watermark support |
| Late data handling | Very limited | Watermarks handle late data gracefully |
| Python support | Limited, slower | Full parity with Scala/Java |
| SQL support | None | Full Spark SQL |
| Stream-stream joins | Not supported | Supported |
| Continuous processing | No (micro-batch only) | Yes (experimental, ~1ms latency) |

### Why DStreams Were Deprecated

**1. No Catalyst optimization**: DStream code couldn't be optimized by Catalyst. Every `map`, `filter`, `reduceByKey` was user-defined code Spark couldn't inspect or improve.

**2. Inconsistent API**: Engineers needed two separate mental models — batch (DataFrame) and streaming (DStream) — even for identical logic.

**3. Event time was an afterthought**: DStreams processed data in processing time (when it arrived), not event time (when it happened). Handling late data required complex custom logic.

**4. No exactly-once guarantees out of the box**: DStreams relied on receiver-based input that was harder to make fault-tolerant. Exactly-once required careful manual setup.

**5. Python was second-class**: Python DStream code crossed the JVM boundary frequently, causing significant overhead.

### Structured Streaming Core Model

```
Input stream → Trigger (micro-batch or continuous)
                    ↓
         Incremental query execution
                    ↓
         Output mode: append / update / complete
                    ↓
              Sink (Kafka, Delta, console, etc.)

Checkpoints track:
  - Source offsets (which Kafka offsets have been processed)
  - State store (for stateful aggregations)
  - Metadata (query progress)
```

### Triggers in Structured Streaming

```python
# Process in fixed intervals (most common)
.trigger(processingTime="30 seconds")

# Process one micro-batch then stop (useful for backfill)
.trigger(once=True)

# Process all available data then stop (Spark 3.3+)
.trigger(availableNow=True)

# Continuous processing — lowest latency, limited operations
.trigger(continuous="1 second")
```

</details>

</article>

<article data-difficulty="mid">

## Scenario: 10-Minute Window Aggregate, 2-Minute Watermark, Data Arrives 5 Minutes Late

You have a Structured Streaming job:
- Reads from Kafka
- Computes a 10-minute tumbling window aggregate
- Watermark is set to **2 minutes**
- Late data arrives **5 minutes after the window closes**

What happens to the late data? Is it included or dropped? Explain the watermark mechanics.

<details>
<summary>✅ Solution</summary>

### The Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, current_timestamp

spark = SparkSession.builder.getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

events = df.select(
    col("value").cast("string").alias("event_data"),
    col("timestamp").alias("event_time")   # event time from Kafka message
)

windowed = events \
    .withWatermark("event_time", "2 minutes") \     # watermark threshold
    .groupBy(
        window(col("event_time"), "10 minutes")     # 10-minute tumbling window
    ).count()

query = windowed.writeStream \
    .outputMode("append") \    # append = only emit results for closed windows
    .format("console") \
    .start()
```

### What is a Watermark?

The watermark is Spark's estimate of "how far behind the real-time clock can event data arrive?"

```
Watermark = max(event_time seen so far) − watermark_delay
           = max(event_time seen so far) − 2 minutes
```

Spark will **discard state** for windows that ended before the watermark. Any late data for those windows is dropped.

### Timeline Analysis

Let's trace through the scenario concretely:

```
Event time:    10:00          10:10          10:20
Window:        [10:00-10:10]  [10:10-10:20]

Events arrive normally up to 10:10 (window closes)
```

**At 10:10**: The 10-minute window [10:00, 10:10) closes.

**Question**: When does Spark actually emit results for this window and drop its state?

```
Watermark = max(event_time_seen) − 2 minutes
```

The window [10:00, 10:10) ends at 10:10.  
Spark will close and emit this window when: `watermark > 10:10`  
Which means: `max(event_time_seen) − 2 min > 10:10`  
Which means: `max(event_time_seen) > 10:12`

**So Spark waits until it sees an event with event_time > 10:12 before closing the 10:00–10:10 window.**

### The Late Data Scenario: 5 Minutes Late

```
Window [10:00, 10:10) closes at event_time 10:10
Watermark delay = 2 minutes
Window state dropped when watermark crosses 10:10
Watermark crosses 10:10 when Spark sees event_time > 10:12

A late event arrives with event_time = 10:05 
but arrives at processing time 10:15 (5 minutes after window close)

By 10:15, Spark has already seen events up to at least 10:17 (real traffic)
Watermark = 10:17 - 2 min = 10:15 > 10:10  ← window state already evicted!
```

**Result: The late event is DROPPED.**

With a 2-minute watermark and data arriving 5 minutes late, the late event is always dropped because:
- The watermark only tolerates **2 minutes** of lateness
- The data is **5 minutes** late → beyond the watermark threshold

### Visual Representation

```
event_time →  10:00   10:02   10:04   10:06   10:08   10:10   10:12   10:14
                                                              ↑
                                                       Window closes

Watermark advances as max(event_time) increases:
At event_time 10:06: watermark = 10:04
At event_time 10:10: watermark = 10:08
At event_time 10:12: watermark = 10:10  ← window state EVICTED at this point
At event_time 10:14: watermark = 10:12

Late event: event_time=10:05, arrives when current watermark=10:12
10:05 < 10:12 (window for 10:05 is already evicted) → DROPPED ✗
```

### How to Fix: Increase Watermark to 10 Minutes

If late data can arrive up to 10 minutes after its event time, set the watermark to at least 10 minutes:

```python
events.withWatermark("event_time", "10 minutes") \   # now tolerates 10 min late
      .groupBy(window(col("event_time"), "10 minutes")) \
      .count()
```

Now the state for [10:00, 10:10) is retained until watermark crosses 10:10,  
which requires seeing event_time > 10:20. Data arriving at event_time 10:05  
with a 5-minute delay (arriving at 10:15) would be **included** ✓.

### Trade-off: Watermark Size vs State Size vs Latency

```
Larger watermark:
  ✓ More late data included
  ✗ More state stored in memory/disk (longer history)
  ✗ Results emitted later (higher latency in append mode)

Smaller watermark:
  ✓ Less state, lower memory pressure
  ✓ Faster result emission
  ✗ More late data dropped
```

### Output Modes and Watermarks

```
append mode:  Only emit window results AFTER the watermark passes the window end
              → requires watermark, introduces latency
              → state cleaned up after emission

update mode:  Emit partial results every micro-batch as they change
              → no watermark required, but state grows forever without watermark
              → with watermark: state cleaned after watermark passes

complete mode: Re-emit ALL results every micro-batch
              → no state cleanup → only for small result sets
```

</details>

</article>

<article data-difficulty="senior">

## Scenario: Design a Fraud Detection Pipeline with Structured Streaming

Design a **real-time fraud detection** pipeline using Spark Structured Streaming with these requirements:

- **Source**: Kafka topic `transactions` with 100K events/sec
- **Detection rule**: Flag any user with more than 5 transactions in a **1-minute sliding window**
- **Reliability**: recover from failures without data loss
- **Latency**: end-to-end < 10 seconds
- **Scale**: handle traffic spikes up to 3× normal volume

Cover: stream source configuration, state management, output sink, checkpointing, and scaling strategy.

<details>
<summary>✅ Solution</summary>

### Architecture Overview

```
Kafka (transactions topic)
        ↓
Structured Streaming Job
  ├── Kafka source (multiple partitions)
  ├── Deserialization + validation
  ├── Watermark (handle late arrivals)
  ├── Sliding window aggregation (1-min window, 30-sec slide)
  ├── Fraud rule evaluation (count > 5)
  └── Stateful operator with checkpointing
        ↓
Dual output:
  ├── Kafka (fraud-alerts topic) → downstream alerting systems
  └── Delta Lake (fraud_events table) → analytics + audit trail
```

### Step 1: Kafka Source Configuration

100K events/sec requires careful Kafka consumer tuning:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import (
    from_json, col, window, count, current_timestamp,
    to_json, struct, lit
)

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    .getOrCreate()

# Transaction schema
tx_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# Kafka source — tuned for 100K events/sec
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 300000) \     # 100K/sec × 3s trigger = 300K
    .option("kafka.max.poll.records", "50000") \
    .option("failOnDataLoss", "false") \          # continue if Kafka offsets unavailable
    .load()

# Parse JSON payload
transactions = raw_stream \
    .select(from_json(col("value").cast("string"), tx_schema).alias("data")) \
    .select("data.*") \
    .filter(col("event_time").isNotNull()) \      # drop malformed records
    .filter(col("user_id").isNotNull())
```

### Step 2: State Management — Sliding Window Aggregation

A **sliding window** (1-min window, 30-sec slide) means each event participates in 2 windows. This doubles state compared to tumbling windows:

```python
# Watermark: tolerate events up to 30 seconds late
# (keeps latency < 10s while handling minor network delays)
flagged = transactions \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        col("user_id"),
        window(col("event_time"), "1 minute", "30 seconds")  # sliding window
    ) \
    .agg(
        count("*").alias("tx_count")
    ) \
    .filter(col("tx_count") > 5) \    # fraud rule: > 5 txns in 1 min
    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("tx_count"),
        lit("HIGH_VELOCITY").alias("fraud_type"),
        current_timestamp().alias("detected_at")
    )
```

**Why sliding over tumbling?**

With a tumbling window, a user could make 5 transactions in the last 30 seconds of window 1 and 5 transactions in the first 30 seconds of window 2 — 10 transactions in 1 minute but never caught. Sliding windows detect this.

### Step 3: Dual Output Sinks with Exactly-Once Semantics

```python
CHECKPOINT_BASE = "s3://bucket/fraud-detection/checkpoints/"

# Output 1: Kafka for real-time alerting
kafka_fraud_query = flagged \
    .select(
        col("user_id").alias("key"),
        to_json(struct("user_id", "window_start", "window_end",
                       "tx_count", "fraud_type", "detected_at")).alias("value")
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/kafka-sink/") \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

# Output 2: Delta Lake for audit trail
delta_fraud_query = flagged \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/delta-sink/") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .partitionBy("fraud_type") \
    .start("s3://bucket/fraud_events/")
```

**Exactly-once guarantee chain:**

```
Kafka source → Structured Streaming (checkpointed offsets) → Delta Lake sink
                                                           ↓
                                                    Delta transaction log
                                                    ensures idempotent writes
                                                    (each micro-batch is a transaction)
```

For Kafka sink, exactly-once requires Kafka transactions:
```python
.option("kafka.transactional.id", "fraud-detection-producer") \
.option("kafka.enable.idempotence", "true")
```

### Step 4: Checkpointing Design

```
s3://bucket/fraud-detection/checkpoints/
├── kafka-source/
│   ├── offsets/              ← Kafka offset tracking per micro-batch
│   └── commits/              ← which batches were committed
├── kafka-sink/
│   └── ...
└── delta-sink/
    └── ...

State store (RocksDB recommended for large state):
  spark.conf.set("spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

State location (part of checkpoint):
  checkpointLocation/state/0/   ← per-operator state
```

RocksDB state store vs default HashMapStateStore:

| | HashMapStateStore | RocksDBStateStore |
|---|---|---|
| State storage | In JVM heap | On-disk + memory cache |
| GC pressure | High for large state | Low |
| Recovery speed | Fast (in-memory) | Moderate (disk read) |
| State size limit | ~10GB per executor | 100s of GB |
| Recommended for | Small state | Large state (fraud detection) |

```python
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
```

### Step 5: Scaling for 100K Events/Sec + 3× Spikes

**Kafka partitioning**: Spark parallelism ≤ number of Kafka partitions.

```
100K events/sec × 3 spike = 300K events/sec peak
Each Spark task processes ~1 Kafka partition
Each task can handle ~5K-10K events/sec comfortably

Kafka partitions needed: 300K / 7.5K = 40 partitions minimum
Use: 48 partitions (multiples of executor count)
```

**Spark cluster sizing:**

```
Trigger interval: 3 seconds
Events per trigger at 3× peak: 300K × 3s = 900K events

Executor cores: 48 (matches Kafka partitions)
Per-executor: 5 cores, 32GB RAM
Total executors: 48/5 ≈ 10 executors

With Dynamic Allocation:
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")   # handles 3× spike
spark.conf.set("spark.shuffle.service.enabled", "true")
```

### Step 6: Monitoring and SLA Validation

```python
import time

def monitor_streaming_query(query, sla_seconds=10):
    """Monitor query progress and alert on SLA breach."""
    while query.isActive:
        progress = query.lastProgress
        if progress:
            # Latency from event creation to processing
            processing_time = progress.get("durationMs", {}).get("triggerExecution", 0)
            input_rate = progress.get("inputRowsPerSecond", 0)
            
            print(f"Trigger duration: {processing_time}ms | "
                  f"Input rate: {input_rate:.0f} rows/sec | "
                  f"Batch: {progress.get('batchId')}")
            
            if processing_time > (sla_seconds * 1000):
                print(f"WARNING: Processing time {processing_time}ms exceeds SLA!")
        time.sleep(10)

# Run in separate thread
import threading
monitor_thread = threading.Thread(
    target=monitor_streaming_query,
    args=(kafka_fraud_query,),
    daemon=True
)
monitor_thread.start()

spark.streams.awaitAnyTermination()
```

### Latency Budget Analysis

```
End-to-end latency target: < 10 seconds

Component breakdown:
  Kafka ingestion lag:     ~100ms (producer → broker)
  Watermark delay:         500ms  (we set 30s but most events arrive in time)
  Trigger interval:        3s     (we wait up to 3s to batch events)
  Processing (aggregate):  500ms  (in-memory sliding window)
  Kafka write (sink):      200ms  (async producer)
  ─────────────────────────────
  Total typical:           ~4-5s  ✓ well within 10s SLA

Worst case (late events, GC pause):
  Watermark delay:         30s    ← only for 30-second-late events
  For on-time events: ~5-6s      ✓ still within SLA
```

### Recovery Behavior

On job restart after failure:
1. Spark reads checkpoint offsets → resumes from last committed Kafka offsets
2. State store (RocksDB) reloads from checkpoint
3. No data loss: events between failure and restart are reprocessed from Kafka
4. Delta Lake idempotence ensures no duplicate writes

```bash
# Restart the job — checkpoint handles state recovery automatically
spark-submit \
  --class com.company.FraudDetection \
  --conf spark.sql.streaming.checkpointLocation=s3://bucket/fraud-detection/checkpoints/ \
  fraud-detection.jar
```

</details>

</article>
