---
title: "Structured Streaming on Databricks - Intermediate"
topic: databricks
subtopic: structured-streaming
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, structured-streaming, watermark, stateful, joins, windowing]
---

# Structured Streaming on Databricks — Intermediate

## Watermarks and Late Data

Watermarks define how long to wait for late-arriving data before finalizing results:

```python
from pyspark.sql.functions import window, col

# Without watermark: Spark keeps ALL state forever (memory grows unbounded!)
# With watermark: Spark drops state for data older than the threshold

events = spark.readStream.table("production.bronze.events")

# Watermark: allow data up to 10 minutes late
windowed_counts = (events
    .withWatermark("event_time", "10 minutes")  # Data >10 min late is dropped
    .groupBy(
        window("event_time", "5 minutes"),  # 5-minute tumbling windows
        "event_type"
    )
    .count()
)

(windowed_counts.writeStream
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/windowed/")
    .trigger(processingTime="30 seconds")
    .toTable("production.gold.event_counts_5min")
)

# How watermark works:
# Max event_time seen so far: 10:25
# Watermark = 10:25 - 10 minutes = 10:15
# Events with event_time < 10:15 are DROPPED (too late)
# Windows ending before 10:15 are FINALIZED (no more updates)
# State for finalized windows is cleaned up (memory freed!)
```

---

## Stream-Stream Joins

Join two streams together (e.g., match clicks with purchases):

```python
# Join clicks stream with purchases stream
# Match: same user, purchase within 30 minutes of click

clicks = (spark.readStream.table("bronze.clicks")
    .withWatermark("click_time", "30 minutes")
)

purchases = (spark.readStream.table("bronze.purchases")
    .withWatermark("purchase_time", "30 minutes")
)

# Inner join with time constraint
click_to_purchase = (clicks.join(
    purchases,
    (clicks.user_id == purchases.user_id) &
    (purchases.purchase_time >= clicks.click_time) &
    (purchases.purchase_time <= clicks.click_time + expr("INTERVAL 30 MINUTES")),
    "inner"
))

(click_to_purchase.writeStream
    .option("checkpointLocation", "/checkpoints/click-purchase-join/")
    .trigger(processingTime="1 minute")
    .toTable("production.silver.attributed_purchases")
)
```

---

## Stream-Static Joins (Enrichment)

Join streaming data with a static dimension table:

```python
# Streaming events enriched with static customer dimension
events_stream = spark.readStream.table("production.bronze.events")

# Static (batch) read of dimension — refreshed each micro-batch
customers = spark.table("production.silver.customers")  # NOT readStream!

enriched = events_stream.join(
    customers,
    events_stream.user_id == customers.customer_id,
    "left"
)

(enriched.writeStream
    .option("checkpointLocation", "/checkpoints/enriched-events/")
    .trigger(processingTime="30 seconds")
    .toTable("production.silver.enriched_events")
)

# Note: the static table is re-read each micro-batch
# If customers table updates hourly, enrichment picks up changes within 1 batch
# For large dimensions: broadcast hint helps performance
```

---

## Deduplication in Streams

```python
# Deduplicate events within a time window (watermark-based)
deduped = (events
    .withWatermark("event_time", "1 hour")  # Track dedup state for 1 hour
    .dropDuplicatesWithinWatermark(["event_id"])  # Unique by event_id
)

# How it works:
# Spark keeps a set of seen event_ids for the watermark window (1 hour)
# If the same event_id arrives again within 1 hour → dropped
# After 1 hour: state for that event_id is cleaned up (memory bounded)
# Trade-off: duplicates arriving >1 hour late will NOT be caught

(deduped.writeStream
    .option("checkpointLocation", "/checkpoints/deduped/")
    .trigger(processingTime="30 seconds")
    .toTable("production.silver.events_deduped")
)
```

---

## foreachBatch (Custom Write Logic)

For complex write patterns (MERGE, multi-table writes):

```python
def upsert_to_delta(batch_df, batch_id):
    """Custom write logic: MERGE (upsert) each micro-batch into target."""
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO production.silver.customers t
        USING updates s ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN
            UPDATE SET t.name = s.name, t.email = s.email, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, updated_at) 
            VALUES (s.customer_id, s.name, s.email, s.updated_at)
    """)

# Use foreachBatch to apply custom logic per micro-batch
(customer_updates_stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/customer-upsert/")
    .trigger(processingTime="1 minute")
    .start()
)

# foreachBatch advantages:
# - Full DataFrame API available (MERGE, multiple writes, external calls)
# - Each batch is a complete DataFrame (not row-by-row)
# - Combine with exactly-once via idempotent MERGE + checkpoint
```

---

## Monitoring Streaming Queries

```python
# Get streaming query metrics
query = df.writeStream.trigger(processingTime="30 seconds").toTable("target").start()

# Check progress
progress = query.lastProgress
print(f"Input rows: {progress['numInputRows']}")
print(f"Processing time: {progress['batchDuration']}ms")
print(f"Source: {progress['sources']}")

# For always-on streams: monitor via StreamingQueryListener
from pyspark.sql.streaming import StreamingQueryListener

class MetricsListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        p = event.progress
        # Emit to Prometheus/CloudWatch
        emit_metric("stream_input_rows", p.numInputRows)
        emit_metric("stream_batch_duration_ms", p.batchDuration)
        
        # Alert if falling behind (input rate > processing rate)
        if p.numInputRows > 0 and p.batchDuration > 30000:
            alert("Stream falling behind: batch took >30s")

spark.streams.addListener(MetricsListener())
```

---

## State Store Management

```python
# Stateful operations (aggregations, dedup, joins) store state on disk
# State grows with: unique keys × watermark duration

# Monitor state size:
# Spark UI → Structured Streaming tab → State Store Size

# If state grows too large:
# 1. Reduce watermark duration (less late data tolerance = less state)
# 2. Reduce grouping key cardinality (fewer unique groups = less state)
# 3. Use RocksDB state store (more efficient than default HashMap)

# Enable RocksDB state backend (better for large state):
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# RocksDB advantages:
# - Stores state on local SSD (not heap memory) → avoids GC pressure
# - Handles millions of keys efficiently
# - Better recovery (incremental checkpointing)
```

---

## Interview Tips

> **Tip 1:** "What is a watermark?" — A threshold defining how late data can arrive and still be processed. `withWatermark("event_time", "10 minutes")` means: data arriving more than 10 minutes after the latest seen event time will be dropped. It bounds state (memory), enables window finalization, and handles late data gracefully.

> **Tip 2:** "How do you do MERGE (upsert) in streaming?" — Use `foreachBatch`: each micro-batch becomes a regular DataFrame, and you run a MERGE statement against the target Delta table. The checkpoint ensures exactly-once: if a batch fails and retries, the MERGE is idempotent (matched rows → UPDATE, unmatched → INSERT).

> **Tip 3:** "Stream-stream vs stream-static join?" — Stream-stream: join two infinite streams with a time constraint (clicks ↔ purchases within 30 min). Requires watermarks on both sides. Stream-static: enrich streaming data with a dimension table (re-read each batch). Static join is simpler — no watermark needed for the static side.
