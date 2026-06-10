---
title: "Structured Streaming on Databricks - Scenario Questions"
topic: databricks
subtopic: structured-streaming
content_type: scenario_question
tags: [databricks, structured-streaming, interview, scenarios]
---

# Scenario Questions — Structured Streaming on Databricks

<article data-difficulty="junior">

## 🟢 Junior: Basic Streaming Pipeline

**Scenario:** JSON files land in S3 every minute. Write a streaming pipeline that ingests them into a Delta table with Auto Loader, adding ingestion timestamp metadata.

<details>
<summary>💡 Hint</summary>
Use `readStream` with `cloudFiles` format. Add `current_timestamp()` for metadata. Write with `availableNow=True` for scheduled batch-style or `processingTime` for continuous.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import current_timestamp, input_file_name

raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .load("s3://lake/landing/events/")
)

enriched = (raw
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

query = (enriched.writeStream
    .option("checkpointLocation", "/checkpoints/events/")
    .trigger(availableNow=True)
    .toTable("production.bronze.events")
)

query.awaitTermination()
```

**Key Points:**
- `cloudFiles` = Auto Loader (incremental file discovery)
- `availableNow=True`: processes all available files then stops (schedule via Workflow)
- Checkpoint ensures exactly-once (same file never processed twice)
- `_ingested_at` and `_source_file` are standard metadata columns for traceability

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Trigger Modes

**Scenario:** Your pipeline must process data with <2 minute latency during business hours but can tolerate 30-minute latency overnight. What trigger strategy do you use?

<details>
<summary>💡 Hint</summary>
Two approaches: continuous trigger during business hours, or a single continuous stream with an appropriate trigger interval that meets the tighter SLA.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Option A: Single stream with 1-minute trigger (meets both SLAs)
(df.writeStream
    .trigger(processingTime="1 minute")  # Micro-batch every 60 seconds
    .option("checkpointLocation", "/checkpoints/stream/")
    .toTable("production.silver.events")
)
# Latency: ~1 minute (meets <2 min SLA at all times)
# Cost: cluster runs 24/7

# Option B: Scheduled batch (more cost-efficient)
# Workflow runs every 2 minutes during business hours (8 AM - 6 PM)
# Workflow runs every 30 minutes overnight
(df.writeStream
    .trigger(availableNow=True)  # Process available, then stop
    .option("checkpointLocation", "/checkpoints/stream/")
    .toTable("production.silver.events")
)
# Two workflows with different cron schedules:
# Business hours: */2 8-17 * * 1-5 (every 2 min)
# Overnight: */30 0-7,18-23 * * * (every 30 min)
# Cost: cluster only runs during processing (not 24/7)

# RECOMMENDATION: Option B (saves ~60% cost vs always-on)
# SLA met: <2 min during business hours, <30 min overnight
# Cost: pay for ~5 min compute per 2-min cycle (cluster start + process + stop)
```

**Key Points:**
- `processingTime="1 minute"`: continuous, low latency, higher cost (24/7 cluster)
- `availableNow=True` + scheduling: batch-style, lower cost, latency = schedule interval
- For <2 min latency: need either continuous trigger OR very frequent scheduled runs
- Cost trade-off: continuous cluster ($4K/month) vs scheduled ($1.5K/month)
- Same checkpoint works across both modes (can switch between them!)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Deduplication in Streaming

**Scenario:** Your Kafka source delivers at-least-once (same event_id may arrive multiple times). Implement streaming deduplication that guarantees each event_id is written to the silver table exactly once.

<details>
<summary>💡 Hint</summary>
Use `dropDuplicatesWithinWatermark` with a watermark that defines how long to remember seen event_ids. The watermark bounds memory usage.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import col, from_json, to_timestamp

# Read from Kafka
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "user-events")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_ts")))
)

# Deduplicate: keep first occurrence of each event_id within watermark window
deduped = (kafka_df
    .withWatermark("event_time", "2 hours")  # Remember event_ids for 2 hours
    .dropDuplicatesWithinWatermark(["event_id"])
)

(deduped.writeStream
    .option("checkpointLocation", "/checkpoints/deduped-events/")
    .trigger(processingTime="30 seconds")
    .toTable("production.silver.events")
)

# How it works:
# 1. Event arrives with event_id = "abc123" at time 10:00
# 2. Spark checks: have we seen "abc123" in the last 2 hours? No → pass through
# 3. Same event arrives again at 10:01 (duplicate delivery)
# 4. Spark checks: "abc123" seen within watermark window? Yes → DROP
# 5. At 12:01: watermark passes 10:00 → state for "abc123" is cleaned up

# Trade-off: watermark duration
# Longer (4 hours): catches more delayed duplicates, uses more memory
# Shorter (30 min): uses less memory, but late duplicates after 30 min slip through
# Choose based on: how delayed can duplicate deliveries be from Kafka?
```

**Key Points:**
- `dropDuplicatesWithinWatermark`: exactly-once dedup within the watermark window
- Watermark bounds state (memory): only tracks event_ids for the specified duration
- Duplicates arriving AFTER the watermark window will NOT be caught (trade-off)
- For Kafka at-least-once: 1-2 hour watermark typically sufficient (redeliveries are fast)
- State uses RocksDB in production (handles millions of unique event_ids)
- Combined with Delta's transactional writes: end-to-end exactly-once

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: foreachBatch MERGE

**Scenario:** Your CDC stream needs to MERGE into a silver table (upsert: insert new rows, update existing rows). Standard append mode won't work because you need UPDATE semantics. Implement using foreachBatch.

<details>
<summary>💡 Hint</summary>
Use `foreachBatch` to get each micro-batch as a regular DataFrame, then execute a MERGE statement against the target Delta table.
</details>

<details>
<summary>✅ Solution</summary>

```python
def merge_to_silver(batch_df, batch_id):
    """MERGE each micro-batch into the silver table (upsert pattern)."""
    if batch_df.isEmpty():
        return
    
    batch_df.createOrReplaceTempView("batch_updates")
    
    spark.sql("""
        MERGE INTO production.silver.customers AS target
        USING batch_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN
            UPDATE SET 
                target.name = source.name,
                target.email = source.email,
                target.region = source.region,
                target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, region, updated_at)
            VALUES (source.customer_id, source.name, source.email, source.region, source.updated_at)
    """)

# Apply MERGE via foreachBatch
cdc_stream = (spark.readStream
    .format("kafka")
    .option("subscribe", "cdc.customers")
    .load()
    .select(from_json(col("value").cast("string"), cdc_schema).alias("data"))
    .select("data.after.*")  # Debezium "after" image
    .filter(col("customer_id").isNotNull())
)

(cdc_stream.writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", "/checkpoints/customers-merge/")
    .trigger(processingTime="30 seconds")
    .start()
    .awaitTermination()
)

# Why foreachBatch for MERGE:
# - Standard writeStream only supports append/complete/update output modes
# - MERGE needs the full DataFrame API (not available in pure streaming)
# - foreachBatch gives you a complete DataFrame each micro-batch
# - The checkpoint ensures exactly-once: if batch fails, re-processes same data
# - MERGE is idempotent: re-running with same data → same result (no duplicates)
```

**Key Points:**
- `foreachBatch`: unlocks full DataFrame/SQL API for each micro-batch
- MERGE is idempotent: safe to retry (checkpoint + Delta transactions = exactly-once)
- `source.updated_at > target.updated_at` prevents old data from overwriting new
- Check `batch_df.isEmpty()` to avoid errors on empty batches
- Use `createOrReplaceTempView` to reference batch in SQL MERGE statement
- This is the standard pattern for CDC → Delta upsert on Databricks

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Windowed Aggregation

**Scenario:** Build a real-time dashboard showing event counts per event_type in 5-minute tumbling windows. Handle late data arriving up to 10 minutes after the window closes.

<details>
<summary>💡 Hint</summary>
Use `window()` function with watermark. The watermark allows late data to update past windows for up to 10 minutes. Use `update` output mode so only changed windows are written.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import window, col, count, sum as spark_sum

events = (spark.readStream
    .table("production.silver.events")
    .withColumn("event_time", col("event_timestamp"))
)

# Windowed aggregation with late data handling
windowed = (events
    .withWatermark("event_time", "10 minutes")  # Accept data up to 10 min late
    .groupBy(
        window("event_time", "5 minutes"),  # 5-minute tumbling windows
        "event_type"
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("amount").alias("total_amount"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type",
        "event_count",
        "total_amount",
    )
)

# Write with UPDATE mode (only changed windows emitted)
(windowed.writeStream
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/event-windows/")
    .trigger(processingTime="30 seconds")
    .toTable("production.gold.event_metrics_5min")
)

# How late data works:
# Window 10:00-10:05 receives most events during 10:00-10:05
# Late event arrives at 10:12 with event_time=10:03 (7 min late)
# Since 7 min < 10 min watermark → window 10:00-10:05 is UPDATED with this event
# Late event arrives at 10:20 with event_time=10:02 (18 min late)
# Since 18 min > 10 min watermark → event is DROPPED (window already finalized)

# Dashboard query:
# SELECT * FROM production.gold.event_metrics_5min 
# WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
# ORDER BY window_start DESC;
```

**Key Points:**
- Watermark "10 minutes": accepts data arriving up to 10 min after the window ends
- `window("event_time", "5 minutes")`: tumbling (non-overlapping) 5-min windows
- `outputMode("update")`: only writes windows that changed (efficient for Delta)
- Late data: updates previously written windows (Delta supports this via MERGE internally)
- After watermark passes: window state is cleaned up (bounded memory)
- For dashboard: query the gold table for the latest windows (always up-to-date)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Stream Production Architecture

**Scenario:** Design a streaming platform processing: 3 Kafka topics (orders, events, clicks), each needing bronze/silver layers, with a gold layer joining all three. Requirements: <1 min latency, exactly-once, fault-tolerant, and cost under $5K/month.

<details>
<summary>💡 Hint</summary>
Separate streams per source (isolated failure), shared cluster (cost-efficient). Each source: Kafka → bronze → silver. Gold reads from all three silver tables via separate stream.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE: 7 streaming queries on 2 clusters

# Cluster 1: Ingestion (bronze) — 4 workers, continuous
INGESTION_STREAMS = {
    "orders_bronze": {
        "source": "kafka:orders-topic",
        "target": "production.bronze.orders",
        "trigger": "10 seconds",
    },
    "events_bronze": {
        "source": "kafka:events-topic",
        "target": "production.bronze.events",
        "trigger": "10 seconds",
    },
    "clicks_bronze": {
        "source": "kafka:clicks-topic",
        "target": "production.bronze.clicks",
        "trigger": "10 seconds",
    },
}

# Cluster 2: Transformation (silver + gold) — 8 workers, continuous
TRANSFORM_STREAMS = {
    "orders_silver": {
        "source": "readStream from bronze.orders",
        "target": "production.silver.orders",
        "trigger": "30 seconds",
        "logic": "parse, validate, dedup",
    },
    "events_silver": {
        "source": "readStream from bronze.events",
        "target": "production.silver.events",
        "trigger": "30 seconds",
        "logic": "parse, validate, dedup",
    },
    "clicks_silver": {
        "source": "readStream from bronze.clicks",
        "target": "production.silver.clicks",
        "trigger": "30 seconds",
        "logic": "parse, sessionize",
    },
    "gold_metrics": {
        "source": "readStream from silver.events",
        "target": "production.gold.realtime_metrics",
        "trigger": "1 minute",
        "logic": "windowed aggregation (5-min windows)",
    },
}

# COST ESTIMATE:
# Cluster 1: 4 × m5.xlarge (spot) × 24/7 = ~$1,200/month
# Cluster 2: 8 × i3.xlarge (spot) × 24/7 = ~$3,000/month
# Kafka (MSK): ~$500/month (existing, shared)
# TOTAL: ~$4,700/month ✓ (under $5K)

# LATENCY:
# Kafka → Bronze: ~10 seconds (trigger interval)
# Bronze → Silver: ~30 seconds
# Silver → Gold: ~1 minute
# End-to-end: ~1.5 minutes worst case (within <2 min SLA)

# FAULT TOLERANCE:
# Each stream has independent checkpoint
# If one stream fails: others continue unaffected
# Workflow monitors stream health → auto-restart on failure
# Exactly-once: checkpoint + Delta transactions per stream
```

**Key Points:**
- Separate cluster for ingestion vs transformation (different scaling needs)
- Multiple streams per cluster (cost-efficient, resource sharing)
- Independent checkpoints: one stream failure doesn't affect others
- 7 streams total: manageable to monitor (one dashboard for all)
- Spot instances safe: streaming queries handle worker reclamation gracefully
- End-to-end latency: sum of trigger intervals across layers (~1.5 min)
- Cost: $4.7K/month for full real-time platform (reasonable for mid-size company)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Handling Backpressure

**Scenario:** Your Kafka topic receives 500K events/minute during peak hours (10 AM - 2 PM) but only 50K/minute off-peak. Your streaming pipeline processes 200K/minute max. During peak, consumer lag grows to 5M messages. Design the solution.

<details>
<summary>💡 Hint</summary>
Options: autoscale the cluster during peak, increase maxOffsetsPerTrigger with larger cluster, or accept the lag and process it (catch up during off-peak).
</details>

<details>
<summary>✅ Solution</summary>

```python
# PROBLEM: Peak (500K/min) > Processing capacity (200K/min) = growing lag

# SOLUTION 1: Autoscale cluster during peak
{
    "autoscale": {
        "min_workers": 4,   # Off-peak: handles 200K/min easily
        "max_workers": 16,  # Peak: handles 500K/min+
    },
    "spark_conf": {
        "spark.databricks.streaming.autoScaling.enabled": "true",
    }
}
# Cluster detects growing lag → adds workers → throughput increases
# Workers scale down during off-peak (cost-efficient)

# SOLUTION 2: Increase parallelism (if Kafka has enough partitions)
# Kafka topic: increase from 6 partitions → 24 partitions
# More partitions = more parallel readers = higher throughput
# Each partition maps to one Spark task → more tasks = more parallelism

# SOLUTION 3: Accept lag, process backlog during off-peak
(spark.readStream.format("kafka")
    .option("maxOffsetsPerTrigger", "500000")  # Process up to 500K per batch
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "high-volume-topic")
    .load()
    .writeStream
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/checkpoints/high-volume/")
    .toTable("production.bronze.events")
)
# Peak hours: processes 200K/min, lag grows to ~5M over 4 hours
# Off-peak: processes 200K/min on 50K/min input → lag drains in ~25 minutes
# Acceptable if latency SLA during peak is "within 30 minutes"

# SOLUTION 4: Optimize processing to increase throughput
# - Photon engine: 2-3x faster parsing and writing
# - Avoid UDFs: use native Spark functions
# - Broadcast small joins: eliminate shuffle
# - Increase shuffle partitions: "spark.sql.shuffle.partitions": "auto"
# Optimization can increase 200K/min → 400K/min on same cluster size

# RECOMMENDED: Combination of Solution 1 (autoscale) + Solution 4 (optimize)
# Autoscale handles spikes, optimization maximizes throughput per worker
# Lag stays near-zero even during peak
```

**Key Points:**
- Backpressure = input rate > processing rate (lag grows)
- First: optimize processing (Photon, native functions, broadcast) — free throughput gains
- Second: autoscale cluster (more workers during peak) — costs more but handles any spike
- Third: increase Kafka partitions (more parallelism) — one-time change
- maxOffsetsPerTrigger caps batch size (prevents OOM) but doesn't solve throughput
- If SLA allows: accepting temporary lag is cheapest (process backlog during off-peak)
- Monitor: consumer lag metric, input rate vs processing rate, batch duration

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: State Store Tuning

**Scenario:** Your streaming deduplication query tracks 50M unique event_ids in state (2-hour watermark, high-cardinality key). The state uses 8 GB of memory and GC pauses are causing 30-second batch delays. Optimize.

<details>
<summary>💡 Hint</summary>
Switch from default HashMap state backend to RocksDB (off-heap, SSD-based). RocksDB handles large state without GC pressure.
</details>

<details>
<summary>✅ Solution</summary>

```python
# PROBLEM: 50M keys × ~160 bytes each = 8 GB in JVM heap
# JVM GC pauses every few minutes → batches take 30s instead of 5s

# SOLUTION: RocksDB state backend (off-heap, SSD-based)
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

# Additional RocksDB tuning:
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "false")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB", "256")

# USE i3 INSTANCE TYPE (has local NVMe SSD for RocksDB storage)
# i3.xlarge: 950 GB NVMe SSD → RocksDB state stored on fast local disk
# m5.xlarge: no local disk → RocksDB uses EBS (slower, but still better than heap)

# RESULT:
# Before (HashMap): 8 GB heap, GC pauses 30s, batch duration 30s
# After (RocksDB): 0.5 GB heap (only cache), state on SSD, batch duration 3s

# ALTERNATIVE: Reduce state size
# Option A: Shorter watermark (2 hours → 30 minutes)
# Fewer keys tracked (only last 30 min), but late duplicates after 30 min slip through
.withWatermark("event_time", "30 minutes")  # 4x less state

# Option B: Bloom filter pre-dedup (reduce keys entering state)
# Hash event_id to a bloom filter (approximate dedup) before Spark dedup
# Catches 99% of duplicates before they hit state → state stays small

# MONITORING state health:
# Spark UI → Structured Streaming → "State Operator"
# Metrics: numKeysTotal (should be bounded), memoryUsedBytes, commitLatencyMs
# Alert if numKeysTotal grows linearly (watermark not cleaning up!)
```

**Key Points:**
- Default HashMap: state in JVM heap → GC pauses for large state (>2 GB)
- RocksDB: state on local SSD, off-heap → no GC, handles 100M+ keys
- i3 instances recommended: fast local NVMe for RocksDB state storage
- GC time in batch duration: classic symptom of in-memory state pressure
- Reducing watermark reduces state size (trade-off: less late data tolerance)
- Monitor: numKeysTotal should plateau (bounded by watermark), not grow indefinitely
- RocksDB is the standard for ANY production stateful streaming on Databricks

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Structured Streaming in Spark and how does it differ from DStreams?**
A: Structured Streaming is Spark's declarative streaming API that treats a live data stream as an unbounded DataFrame. It replaced the older DStreams (RDD-based) API by providing the full DataFrame/SQL API, end-to-end exactly-once semantics, and event-time processing — all with a much simpler programming model.

**Q: What are the three output modes in Structured Streaming?**
A: Append mode writes only new rows to the output sink. Complete mode writes the entire result table on each trigger — only supported for aggregations. Update mode writes only rows that changed since the last trigger. Choice depends on the query type and sink capabilities.

**Q: What is a watermark in Structured Streaming and why is it needed?**
A: A watermark is a threshold that tells Structured Streaming how late data can arrive and still be processed in the correct time window. Without a watermark, the engine would need to retain state forever to handle arbitrarily late data — the watermark bounds the state and enables state cleanup.

**Q: How does Structured Streaming achieve exactly-once semantics?**
A: By combining idempotent sources (Kafka offset tracking), a write-ahead checkpoint log (storing progress and state), and idempotent sinks (Delta Lake with transactional writes). On restart, the engine recovers from the checkpoint and reprocesses from the last committed offset without duplication.

**Q: What are the trigger types in Structured Streaming?**
A: Default (as-fast-as-possible, continuous micro-batches), Fixed interval (`Trigger.ProcessingTime("1 minute")` — process once per interval), Once (`Trigger.Once()` — process all available data in one batch then stop), and AvailableNow (`Trigger.AvailableNow()` — like Once but with multiple micro-batches for large backfills).

**Q: What is state management in Structured Streaming and what operations require it?**
A: State management tracks information across micro-batches (e.g., running aggregations, stream-stream join buffers, deduplication keys). Stateful operations include windowed aggregations, `dropDuplicates`, stream-stream joins, and `mapGroupsWithState`. State is stored in the checkpoint location and can grow large for long-running streams.

**Q: How do you handle schema evolution in a Structured Streaming pipeline reading from Kafka?**
A: Parse the message payload in the Spark job itself (e.g., from Avro or JSON), and use schema registry integration (Confluent Schema Registry with `from_avro`) to handle schema changes. Alternatively, use Auto Loader with `cloudFiles.schemaEvolutionMode` for file-based sources.

**Q: What is foreachBatch and when would you use it?**
A: `foreachBatch` is a sink that passes each micro-batch as a DataFrame to a user-defined function, enabling arbitrary batch operations (multi-table writes, MERGE operations, external API calls) as the streaming sink. It is the escape hatch when built-in sinks don't support your required write pattern.

---

## 💼 Interview Tips

- Understand watermarks deeply — late data handling is a common interview deep-dive and many candidates cannot explain the tradeoff between watermark delay and completeness.
- Know all trigger types and when to use AvailableNow vs. Once — this shows practical knowledge of running streaming jobs in cost-efficient batch mode.
- Be ready to debug a streaming job from first principles: check the Spark UI streaming tab for input rate, processing rate, batch duration, and state size.
- Senior interviewers will probe state management: how does state grow over time, what happens if you run out of state store memory, and how do you tune state store configuration?
- Common mistake: ignoring checkpoint management — explain how to safely move or reset a checkpoint and the implications for reprocessing and deduplication.
- Connect Structured Streaming to Delta Lake MERGE for CDC pipelines — using `foreachBatch` with a MERGE is a production-grade pattern that demonstrates real-world implementation knowledge.
