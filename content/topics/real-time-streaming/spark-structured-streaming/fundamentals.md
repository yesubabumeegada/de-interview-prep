---
title: "Spark Structured Streaming — Fundamentals"
topic: real-time-streaming
subtopic: spark-structured-streaming
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, structured-streaming, streaming, kafka, delta-lake, micro-batch]
---

# Spark Structured Streaming — Fundamentals

## What Is Spark Structured Streaming?

Spark Structured Streaming is a **scalable, fault-tolerant stream processing engine** built on the Spark SQL engine. It treats a live data stream as an unbounded table — new data continuously appended, queries run continuously.

```
Processing model:

  Streaming DataFrame = continuous unbounded table
  ┌──────────────────────────────────────────────────┐
  │  Batch 0  │  Batch 1  │  Batch 2  │  Batch 3  │ ...
  └──────────────────────────────────────────────────┘
  
  Each "batch" (trigger interval) = SQL query on new data
  
  Key properties:
  - Write the same Spark SQL/DataFrame code as batch
  - Engine handles: state management, fault tolerance, late data
  - Output modes: Append (new rows only), Update (changed rows), Complete (full result)
  
Flink vs Spark Structured Streaming:
  Flink:   true record-by-record streaming, ~10ms latency, rich state
  Spark SS: micro-batch (100ms to seconds), ~100ms-1s latency, simpler ops
  
  Use Spark SS when:
    - Team already uses Spark/Databricks
    - Integration with Delta Lake, MLlib, Spark SQL needed
    - Batch + streaming on same codebase
  Use Flink when:
    - Sub-100ms latency required
    - Complex event-time windowing with late data
    - Rich stateful operations (custom timers, complex state)
```

---

## Core Concepts

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("StreamingExample") \
    .config("spark.sql.streaming.checkpointLocation", "s3://bucket/checkpoints/") \
    .getOrCreate()

# Read from Kafka (returns streaming DataFrame)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \   # or "earliest" for replay
    .option("maxOffsetsPerTrigger", 100000) \ # back-pressure: limit batch size
    .load()

# Kafka gives: key, value, topic, partition, offset, timestamp, timestampType
# value is binary — parse it:
order_schema = StructType([
    StructField("order_id",   StringType()),
    StructField("user_id",    StringType()),
    StructField("amount",     DoubleType()),
    StructField("category",   StringType()),
    StructField("event_time", TimestampType())
])

orders = raw_stream \
    .select(from_json(col("value").cast("string"), order_schema).alias("data")) \
    .select("data.*")

# Apply transformations (same as batch DataFrame API)
filtered = orders.filter(col("amount") > 0)
enriched = filtered.withColumn("processing_time", current_timestamp())

# Write to Delta Lake (append mode)
query = enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/orders/") \
    .start("s3://bucket/delta/orders/")

query.awaitTermination()
```

---

## Triggers and Output Modes

```python
# TRIGGERS: control when Spark processes each micro-batch

# 1. Default (unspecified): process each micro-batch as fast as possible
query = df.writeStream.format("delta").start(path)

# 2. Fixed interval: process every N seconds (batch completes before next starts)
query = df.writeStream \
    .trigger(processingTime="30 seconds") \
    .format("delta").start(path)

# 3. Once: process all available data, then stop (for scheduled batch-like streaming)
query = df.writeStream \
    .trigger(once=True) \
    .format("delta").start(path)

# 4. AvailableNow (Spark 3.3+): process all pending data in multiple batches, then stop
#    Better than Once for large backlogs (doesn't put all data in one batch)
query = df.writeStream \
    .trigger(availableNow=True) \
    .format("delta").start(path)
query.awaitTermination()

# OUTPUT MODES:
# Append:   only write new rows to sink (stateless or append-only aggregations)
# Update:   only write changed rows (aggregations that change over time)
# Complete: rewrite entire result table every batch (small aggregations only)

# Example: windowed count (uses Update or Complete mode)
from pyspark.sql.functions import window

windowed = orders \
    .withWatermark("event_time", "5 minutes") \   # tolerate 5 min late data
    .groupBy(
        window("event_time", "1 minute"),          # 1-minute tumbling window
        "category"
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_amount")
    )

query = windowed.writeStream \
    .outputMode("update") \   # emit when window values change
    .format("console") \
    .start()
```

---

## Watermarks for Late Data

```python
# Watermark: tells Spark how late data can arrive
# Spark won't emit/finalize a window until watermark passes window end

# Without watermark:
#   Spark accumulates state forever (memory grows unboundedly)
#   Windows never "complete" — state kept indefinitely

# With watermark:
#   Spark drops state for windows older than: max(event_time) - watermark_threshold
#   Late data that falls before the watermark is DROPPED (not counted)

orders_with_wm = orders \
    .withWatermark("event_time", "10 minutes")  # accept events up to 10 min late

# Window aggregation with watermark
result = orders_with_wm \
    .groupBy(
        window("event_time", "1 hour", "30 minutes"),  # 1-hour window, slide every 30 min
        "category"
    ) \
    .count()

# State cleanup:
#   Watermark = max(event_time seen) - 10 minutes
#   Window [10:00, 11:00] emitted when watermark > 11:00 (i.e., event_time > 11:10)
#   After emission: state for that window is deleted

# Append mode with watermark:
#   Row emitted only when it can no longer be updated (watermark passes window end)
#   No partial results — each row emitted exactly once
result.writeStream.outputMode("append").format("delta").start(path)
```

---

## Interview Tips

> **Tip 1:** "What is the checkpoint in Spark Structured Streaming?" — The checkpoint stores the streaming query's progress: which Kafka offsets have been processed, and the state of any stateful operations (aggregations, joins). On failure and restart, Spark reads the checkpoint to resume from the last committed offset — no data is processed twice. The checkpoint is stored in a durable location (HDFS, S3, ADLS). Without a checkpoint, restarting the query would start from scratch (`startingOffsets=latest`) or replay everything (`earliest`). Checkpoint location is set per query and must be unique per streaming query.

> **Tip 2:** "What's the difference between `trigger(once=True)` and `trigger(availableNow=True)`?" — `once=True`: processes all available data in a single micro-batch, then stops. Problem: for large backlogs, puts all data in one giant batch (OOM risk, slow). `availableNow=True` (Spark 3.3+): processes all available data in multiple optimally-sized batches, then stops. Better for large backlogs — data flows through multiple batches at normal throughput, then the query stops automatically. Use `availableNow` for scheduled batch-style streaming (e.g., triggered every 15 minutes via Databricks Workflows).

> **Tip 3:** "What are the output modes and when can you use each?" — Append mode: only new rows written to sink. Supported for queries without aggregations, or aggregations with watermark where output is final. Complete mode: full result table rewritten every batch. Only for small aggregations (entire result must fit in driver memory for console sink). Update mode: only rows that changed since last batch. Supported for aggregations. Limitation: Update mode is NOT supported for Delta Lake append sink (use Append with watermark, or merge separately). Most production pipelines use Append mode with watermarks for correctness and sink compatibility.
