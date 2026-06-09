---
title: "Spark Structured Streaming — Scenarios"
topic: real-time-streaming
subtopic: spark-structured-streaming
content_type: scenario_question
tags: [spark, structured-streaming, interview, scenarios, delta-lake, kafka, debugging]
---

# Spark Structured Streaming — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Near-Real-Time Analytics Platform

**Scenario:** An e-commerce company wants to show sellers their sales metrics updated every 5 minutes: total orders, revenue, top products, and conversion rate. Data comes from Kafka. Design the system using Spark Structured Streaming and Delta Lake.

<details>
<summary>💡 Hint</summary>
Design a Bronze → Silver → Gold medallion pipeline. Think about: trigger(processingTime='5 minutes'), foreachBatch for Delta MERGE at Silver, tumbling window aggregations at Gold. Serving layer reads from Delta.
</details>

<details>
<summary>✅ Solution</summary>

```
Architecture:
  Kafka (order-events, page-view-events) → Databricks Streaming → Delta Lake → Power BI / API

Pipeline:

1. Bronze (raw ingest):
   Two streaming queries:
   - orders-bronze: Kafka order-events → Delta (abfss://bronze/orders/)
   - views-bronze:  Kafka page-view-events → Delta (abfss://bronze/views/)
   Trigger: processingTime="30 seconds"
   Schema: raw JSON, no validation

2. Silver (parse + deduplicate):
   ForeachBatch:
   - Parse JSON schema
   - Validate required fields
   - dropDuplicates(["order_id"]) / dropDuplicates(["view_id"])
   - Delta merge (upsert) on primary key
   Trigger: processingTime="1 minute"

3. Gold (seller metrics, 5-minute windows):
   
   Order metrics per seller:
   silver_orders
     .withWatermark("order_time", "10 minutes")
     .groupBy(
       window("order_time", "5 minutes"),
       "seller_id"
     )
     .agg(
       count("order_id").alias("order_count"),
       sum("revenue").alias("total_revenue"),
       avg("revenue").alias("avg_order_value")
     )
   → Delta: abfss://gold/seller-metrics/
   
   Top products per seller (foreachBatch):
   - Per batch: rank products by order_count using window functions
   - Write top-10 per seller to Delta
   
   Conversion rate:
   - Join order events and page view events (stream-table join)
   - conversion = orders / views (per product, per 5-min window)
   - Write to Delta

4. Serving layer:
   - Power BI: DirectQuery on Delta gold tables (real-time with 5-min refresh)
   - REST API: read from Delta gold tables on-demand
   - Delta OPTIMIZE + Z-ORDER by seller_id (run every hour via batch job)
     for fast single-seller queries

Trigger design:
  Bronze: 30 seconds (fast ingest, no transformation overhead)
  Silver: 1 minute (transformation cost justified; deduplicate across bronze)
  Gold:   5 minutes (aligned with business reporting cadence; watermark 10 min)

Checkpoints: ADLS Gen2 (abfss://checkpoints/)
State backend: RocksDB (seller count may be 100K+)

Estimated latency:
  Event → Bronze: ~30 seconds
  Bronze → Silver: ~90 seconds (30s bronze + 60s silver trigger)
  Silver → Gold: ~8 minutes (90s silver + 5-min gold window + 2-min watermark)
  Total: ~10 minutes end-to-end (acceptable for 5-min dashboard refresh)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Streaming Job Processing Latency Growing Over Time

**Scenario:** Your Spark Structured Streaming job started processing in 30 seconds per batch. After 3 days, each batch takes 8+ minutes, causing your 5-minute trigger interval to queue up. The Spark UI shows the job is stuck on a shuffle stage. Diagnose and fix.

<details>
<summary>💡 Hint</summary>
Growing batch duration usually means growing state. Check stream-stream joins (unbounded state without watermark), window aggregations with stale data, or accumulating deduplication state. Add watermarks and state TTL.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Check query.lastProgress for trends
  print(query.lastProgress)
  →  "triggerExecution": {"batchDuration": 480000}   (8 minutes!)
  →  "stateOperators": [{"numRowsTotal": 450000000, "memoryUsedBytes": 85000000000}]
                                                       (85 GB state — 85× initial!)

Root cause identified: state explosion
  Job has stream-stream join without watermark or time bound
  State accumulates all records from both streams since job start
  State buffer: 450 million rows (3 days × arrival rate)

Step 2: Check shuffle plan
  Spark UI → Stage → SQL plan → find "ShuffleExchange" on join
  Input: 450M rows from state (full state shuffled every batch)
  This is the bottleneck: 450M rows × 100 bytes = 45 GB shuffle per batch

Fix 1: Add watermarks to both streams and time constraint to join
  orders_wm   = orders.withWatermark("order_time", "1 hour")
  payments_wm = payments.withWatermark("payment_time", "2 hours")
  
  joined = orders_wm.join(payments_wm,
    expr("""orders.order_id = payments.order_id
            AND payment_time BETWEEN order_time AND order_time + INTERVAL 6 HOURS"""),
    "leftOuter")
  
  Effect: state bounded to 6 hours of data instead of 3 days

Fix 2: Switch to RocksDB state store (if not already)
  spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
  Effect: 85 GB state moves from heap to disk → no GC pressure

Fix 3: Increase shuffle partitions for large state
  spark.conf.set("spark.sql.shuffle.partitions", "400")  # was 200
  Effect: each partition smaller → less memory per task

Fix 4: Stop and restart from latest checkpoint with fixes applied
  query.stop()
  # State is now invalid (watermark removes old data on restart)
  # Restart with new code → state rebuilds with bounded growth

After fix:
  State size: stabilizes at 5 GB (6 hours × arrival rate)
  Batch duration: drops to 45 seconds
  Job stable for 7+ days

Lesson: always add watermarks and time bounds to stream-stream joins.
        Monitor stateOperators.numRowsTotal weekly.
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Migrate Batch Job to Streaming

**Scenario:** You have a batch Spark job that runs every hour reading from S3 (new Parquet files dropped hourly) and writing aggregated results to a PostgreSQL database. Business wants results every 5 minutes. How do you migrate to Spark Structured Streaming?

<details>
<summary>💡 Hint</summary>
Map the batch job components to streaming equivalents: S3 file listing → Auto Loader (cloudFiles), hourly batch read → trigger(processingTime='5 minutes'), aggregate write to Postgres → foreachBatch with JDBC UPSERT.
</details>

<details>
<summary>✅ Solution</summary>

```
Current batch architecture:
  Cron (every hour) → Spark batch job → read new Parquet from S3 → aggregate → JDBC write to PostgreSQL

New streaming architecture:
  Continuous → Spark Structured Streaming (S3 file source) → aggregate → foreachBatch → JDBC write

Step 1: Replace batch source with streaming source
  # Batch:
  df = spark.read.parquet("s3://bucket/data/")
  
  # Streaming (Auto Loader — monitors S3 for new files via SQS/SNS notifications):
  df = spark.readStream.format("cloudFiles") \   # Databricks Auto Loader
      .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "s3://bucket/schema/") \
      .load("s3://bucket/data/")
  
  # Or standard file streaming (polls S3 for new files):
  df = spark.readStream.format("parquet") \
      .option("path", "s3://bucket/data/") \
      .load()
  
  # Trigger: process all available files, stop (scheduled by Databricks Workflows)
  .trigger(availableNow=True)

Step 2: Wrap existing aggregation logic in foreachBatch
  # Existing batch aggregation (unchanged):
  def aggregate_and_write(batch_df, batch_id):
      result = batch_df \
          .groupBy("category", "date") \
          .agg(sum("amount").alias("total"), count("*").alias("count"))
      
      # Write to PostgreSQL (existing JDBC code, unchanged)
      result.write.format("jdbc") \
          .option("url", "jdbc:postgresql://pg:5432/analytics") \
          .option("dbtable", "category_aggregates") \
          .option("user", "user").option("password", "pass") \
          .mode("append").save()
      
      print(f"Batch {batch_id}: wrote {result.count()} aggregated rows")

Step 3: Configure streaming query
  query = df.writeStream \
      .foreachBatch(aggregate_and_write) \
      .option("checkpointLocation", "s3://bucket/checkpoints/category-agg/") \
      .trigger(availableNow=True) \
      .start()
  query.awaitTermination()

Step 4: Schedule with Databricks Workflows (every 5 minutes)
  Task type: Notebook / Python file
  Schedule: 0/5 * * * * (every 5 minutes)
  Cluster: job cluster (cost-efficient, spins up fresh)

Step 5: Make JDBC write idempotent
  # Problem: if job fails and restarts, same data may be written again
  # Fix: use UPSERT in PostgreSQL
  def aggregate_and_upsert(batch_df, batch_id):
      result = batch_df.groupBy("category", "date") \
          .agg(sum("amount").alias("total"), count("*").alias("count"))
      
      # Upsert via MERGE (PostgreSQL: INSERT ... ON CONFLICT DO UPDATE)
      result.write.format("jdbc") \
          .option("url", JDBC_URL) \
          .option("dbtable", "category_aggregates") \
          .option("driver", "org.postgresql.Driver") \
          .mode("overwrite")  # for small result sets, overwrite is simplest
          .save()             # or use execute("INSERT ... ON CONFLICT ...")

Migration timeline: 1 day (mostly testing)
Outcome:
  Before: 60-minute latency (batch), job runs on large cluster
  After:  5-minute latency, smaller job cluster (less data per run)
  Cost:   Similar (small cluster × 12 runs/hour vs large cluster × 1 run/hour)
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "What is Auto Loader (cloudFiles) and why is it better than the built-in file streaming source?" — Auto Loader is a Databricks-native file discovery mechanism. It uses AWS SQS/SNS event notifications (or ADLS Event Grid) to detect new files instantly instead of listing all files in S3. The built-in file streaming source lists the entire S3 prefix every batch to find new files — with millions of files, listing takes minutes and becomes the bottleneck. Auto Loader: O(1) notification per new file (regardless of total file count). It also handles schema inference and evolution automatically via a schema location. For any production Databricks streaming job reading from cloud storage, use Auto Loader.

> **Tip 2:** "How do you handle exactly-once writes to PostgreSQL from Spark Structured Streaming?" — PostgreSQL doesn't have a native streaming connector with 2PC support, so use foreachBatch with idempotent writes. Approach 1: UPSERT — `INSERT INTO ... ON CONFLICT (primary_key) DO UPDATE SET ...`. Re-running the same batch produces the same result (upsert is idempotent). Approach 2: Delete-then-insert per batch ID — `DELETE FROM table WHERE batch_id = N; INSERT INTO table ...`. Approach 3: Use a staging table — insert all, then merge from staging to final. Include `batch_id` in the table for audit/replay detection. The key insight: exactly-once at the sink requires idempotent writes, not distributed transactions.

> **Tip 3:** "How would you handle a sudden spike in Kafka data (10× normal volume) in your streaming job?" — The `maxOffsetsPerTrigger` option limits how much data is consumed per batch (provides back-pressure). With 10× spike: Spark processes at its max rate but consumer lag builds temporarily. The lag resolves as the spike subsides. Ensure: (a) the downstream sink can handle burst writes (connection pooling, Delta table write throughput); (b) state store has enough memory for the burst (RocksDB handles spikes better than heap); (c) monitoring alerts on consumer lag > 10 minutes; (d) if sustained 10× growth, increase Databricks cluster size (add workers) or increase Kafka partition count to allow higher Spark parallelism.

