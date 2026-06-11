---
title: "Stream Processing Patterns — Fundamentals"
topic: real-time-streaming
subtopic: stream-processing-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, patterns, lambda-architecture, kappa-architecture, fan-out, enrichment]
---

# Stream Processing Patterns — Fundamentals


## 🎯 Analogy

Think of stream processing patterns like recipes for common real-time problems: filtering is sieving, enrichment is marinating (adding context from a lookup), aggregation is reducing a sauce, and joining two streams is combining two ingredients at the right moment.

---
## Core Streaming Patterns Overview

```
Fundamental patterns in stream processing:

1. Filter + Transform (stateless)
   Input stream → filter bad records → transform format → output stream
   Example: parse JSON, drop nulls, rename fields
   
2. Aggregation (stateful)
   Input stream → group by key → aggregate per window → output counts/sums
   Example: order counts per category per minute

3. Enrichment (stream-table join)
   Stream + lookup table → enriched stream
   Example: join clickstream with user profile table

4. Fan-out (one to many)
   Single stream → multiple consumers/outputs
   Example: order events → (analytics sink) + (fraud check) + (email service)

5. Merge (many to one)
   Multiple streams → unified stream
   Example: events from web + mobile + API → single event stream

6. Stream-Stream Join
   Two streams matched on key + time condition
   Example: orders + payments → matched order-payments

7. CDC (Change Data Capture)
   Database change log → stream of insert/update/delete events
   Example: Debezium reads MySQL binlog → Kafka topic → downstream systems

8. Event Sourcing
   Store all changes as events (immutable log), derive state by replaying
   Example: bank account balance = replay all transaction events
```

---

## Lambda Architecture

```
Lambda Architecture: dual-path processing (batch + speed)

  Data Source
      │
      ├─────────────────────────────┐
      │                             │
      ▼                             ▼
  Batch Layer                  Speed Layer
  (Spark/Hadoop)              (Flink/Kafka Streams)
  Process ALL data            Process RECENT data
  High accuracy               Low latency
  High latency (hours)        Approximate (no reprocessing)
      │                             │
      ▼                             ▼
  Serving Layer (merged view)
  Historical batch view + Recent stream view
  Query: UNION or time-based routing

Example: Ad impression analytics
  Speed layer: count impressions per ad in last 5 minutes (approximate, fast)
  Batch layer: recount all impressions for the day (accurate, used for billing)
  Serving: dashboard shows speed layer for real-time, batch layer for reports

Problems with Lambda Architecture:
  1. Two codebases: same logic implemented twice (batch + streaming)
  2. Merging results is complex (how to handle overlapping time ranges?)
  3. Operational burden: maintain two separate systems
  4. Debugging: harder to trace issues across two paths
```

---

## Kappa Architecture

```
Kappa Architecture: streaming-only (eliminate batch path)

  Data Source
      │
      ▼
  Stream Storage (Kafka, Kinesis, Delta Lake)
  [Immutable log with long retention]
      │
      ├─────────────────────────────┐
      │                             │
      ▼                             ▼
  Stream Processor v1          Stream Processor v2
  (current production)         (new version, replay)
                                    │
                                    ▼
                                New Output Table
                                (when ready → swap)

Key insight: replay old events through new streaming job = same as batch reprocessing
  
  How reprocessing works:
    New streaming job reads from beginning of Kafka topic (earliest offset)
    Processes historical + real-time data in same code path
    Writes to new output table (doesn't affect production yet)
    When new table is up-to-date → atomic swap (point queries to new table)
    Decommission old streaming job

Requirements for Kappa:
  Long event retention: Kafka with 30-day+ retention (or ADLS/S3 backing)
  Idempotent sinks: replay may produce duplicates → sink must handle upserts
  Fast replay: enough processing capacity to catch up before cutover

Lambda vs Kappa:
  Lambda:   accurate results, complex operations, two codebases
  Kappa:    single codebase, simpler ops, requires long retention + fast replay
  Modern choice: Kappa preferred with Delta Lake/Iceberg (immutable storage + streaming)
```

---

## Fan-Out Pattern

```python
# Fan-out: one stream → multiple independent consumers

# Kafka: all consumer groups read from same topic independently
# Event: order event consumed by:
#   1. analytics-consumer:  writes to data warehouse
#   2. fraud-consumer:      checks for fraud patterns
#   3. notification-consumer: sends order confirmation email
#   4. inventory-consumer:  decrements inventory

# Each consumer group has independent offset tracking
# One consumer falling behind doesn't affect others

# Implementation (Kafka):
from confluent_kafka import Consumer

# Analytics consumer
analytics_consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'analytics-consumer',  # unique group ID
    'auto.offset.reset': 'earliest'
})
analytics_consumer.subscribe(['orders'])

# Fraud consumer (separate group, independent offset)
fraud_consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'fraud-consumer',
    'auto.offset.reset': 'latest'
})
fraud_consumer.subscribe(['orders'])

# Both consumers receive ALL events from 'orders' topic
# They don't interfere with each other

# Fan-out with transformation (Flink):
DataStream<Order> orders = env.addSource(orderSource);

// Analytics path
orders
    .filter(o -> o.getStatus().equals("completed"))
    .addSink(analyticsSink);

// Fraud path
orders
    .process(new FraudScorer())
    .filter(a -> a.getScore() > 0.8)
    .addSink(fraudAlertSink);

// Notification path
orders
    .filter(o -> o.getStatus().equals("confirmed"))
    .addSink(emailSink);

// All three run in parallel from the same orders stream
```

---

## Enrichment Pattern

```python
# Enrichment: join stream with reference data to add context

from pyspark.sql.functions import *

# Streaming events (clicks)
clicks = spark.readStream.format("kafka") \
    .option("subscribe", "clicks").load() \
    .select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# Static reference (user profiles — changes slowly)
user_profiles = spark.read.format("delta") \
    .load("s3://bucket/delta/user-profiles/")  # batch read at job start

# Option 1: broadcast join (small reference table)
enriched = clicks.join(
    broadcast(user_profiles),  # broadcast = replicate to all executors
    "user_id",
    "left"
)
# Best for: reference table < 2 GB, rarely changes

# Option 2: foreachBatch with periodic refresh (large or frequently changing table)
def enrich_batch(batch_df, batch_id):
    profiles = spark.read.format("delta").load("s3://bucket/delta/user-profiles/")
    result = batch_df.join(broadcast(profiles), "user_id", "left")
    result.write.format("delta").mode("append").save("s3://bucket/delta/enriched-clicks/")

clicks.writeStream.foreachBatch(enrich_batch) \
    .option("checkpointLocation", "s3://bucket/checkpoints/enriched/").start()
# Best for: reference table changes frequently (refresh each batch)

# Option 3: async I/O (Flink) — lookup database per record in parallel
DataStream<EnrichedClick> enriched = AsyncDataStream.unorderedWait(
    clicks,
    new RedisUserProfileLookup(),  // async Redis lookup
    timeout=500, TimeUnit.MILLISECONDS,
    capacity=200  // max 200 concurrent Redis requests
);
# Best for: external key-value store, low latency lookups
```

---


## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.master("local[*]").appName("patterns").getOrCreate()

# Pattern 1: Filter (drop events that don't match criteria)
# stream.filter(col("amount") > 0)

# Pattern 2: Enrich (join stream with static lookup table)
regions = spark.createDataFrame([("US","North America"),("EU","Europe")], ["code","name"])

# In streaming: broadcast the small lookup table
# enriched = stream.join(broadcast(regions), stream.region_code == regions.code)

# Pattern 3: Deduplication (exactly-once with watermark)
# deduped = stream.withWatermark("ts", "10 minutes").dropDuplicates(["order_id"])

# Pattern 4: Stream-stream join (orders + payments within 30 min window)
# joined = orders.join(payments,
#   expr("order_id = payment_order_id AND payments.ts BETWEEN orders.ts AND orders.ts + INTERVAL 30 MINUTES"))

print("Core patterns: filter → enrich → aggregate → deduplicate → join")
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is the fundamental difference between Lambda and Kappa architecture?" — Lambda uses two separate processing paths: a batch layer for accuracy and a speed (streaming) layer for low latency. The serving layer merges both. This means the same business logic must be implemented twice, creating operational complexity. Kappa eliminates the batch path: all processing is streaming, and historical reprocessing is done by replaying the event log from the beginning through the same streaming job. Kappa is simpler but requires long event retention (Kafka with weeks/months of retention, or cloud object storage) and idempotent sinks (for safe replay).

> **Tip 2:** "When would you still use Lambda architecture in 2024?" — Kappa is preferred for most modern architectures. Lambda still makes sense when: (a) the streaming engine can't produce exactly-accurate results for complex analytics (e.g., exact distinct count requires HyperLogLog approximation in streaming); (b) batch reprocessing of large historical data is much cheaper than streaming replay (e.g., 5 years of data in Spark batch vs. streaming); (c) the organization already has a mature batch platform and streaming is being added incrementally. With Delta Lake and Iceberg enabling both streaming writes and batch reads on the same table, many organizations have moved to a unified Kappa-like approach.

> **Tip 3:** "What's the difference between fan-out and broadcast in streaming?" — Fan-out: multiple independent consumers reading the same stream (each consumer gets ALL records). Kafka: multiple consumer groups, each reads from offset 0 independently. Use case: analytics + fraud + notifications all processing the same event. Broadcast (Flink/Spark): sending data to ALL parallel instances of a downstream operator — used in broadcast joins (e.g., send small lookup table to all partitions). Not the same as fan-out: broadcast is an internal operator exchange pattern, while fan-out is a multi-consumer architecture pattern.
