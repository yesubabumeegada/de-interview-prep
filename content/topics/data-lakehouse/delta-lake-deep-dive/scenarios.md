---
title: "Delta Lake Deep Dive — Scenarios"
topic: data-lakehouse
subtopic: delta-lake-deep-dive
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [delta-lake, scenarios, interview, optimization, debugging]
---

# Delta Lake Deep Dive — Interview Scenarios

## Scenario 1: Delta Table Scan Is Slow Despite Partitioning

**Question:** A Delta table `orders` is partitioned by `order_date` and has 1B rows (3 years of data). A query `SELECT * FROM orders WHERE customer_id = 12345` takes 8 minutes. Partitioning by order_date doesn't help here because no date filter is applied. How do you fix this?

**Answer:**

```
Root cause: query filters on customer_id, but table is only partitioned by order_date
  → Delta scans ALL partitions (1,095 days × ~N files per day)
  → Even with column stats, customer_id is scattered across all files

Option 1: ZORDER by customer_id (quick win)
  OPTIMIZE orders ZORDER BY (customer_id);
  -- Z-order co-locates records with same customer_id within each date partition
  -- column stats: minValues.customer_id / maxValues.customer_id per file
  -- Delta skips files that don't contain customer_id 12345
  Expected: 8 min → ~30 sec (most files skipped via data skipping)
  
  Limitation: Z-order helps within partitions, but still scans all date partitions
  If data is 3 years, that's 1,095 partitions to check stats on

Option 2: Add Bloom filter index on customer_id (targeted point lookups)
  CREATE BLOOMFILTER INDEX ON TABLE orders
  FOR COLUMNS (customer_id OPTIONS (fpp=0.01, numItems=10000000));
  -- Bloom filter per file: can quickly rule out "this customer is NOT in this file"
  -- fpp=0.01 = 1% false positive rate
  Expected: very fast file pruning for equality lookups

Option 3: Re-partition table (if customer-centric queries dominate)
  -- Add customer_id as additional partition key (with bucketing to avoid too many partitions)
  -- Not recommended if cardinality of customer_id is very high (millions of customers)
  -- Alternative: partition by (order_date, customer_id_bucket) where bucket = customer_id % 100

Final recommendation:
  Step 1: ZORDER BY (customer_id) → immediate improvement, no config change
  Step 2: Bloom filter for exact equality lookups (WHERE customer_id = X)
  Step 3: Monitor: does query hit <10% of files? If yes, solution is sufficient
```

---

## Scenario 2: Delta Log Is Growing Too Large

**Question:** A high-frequency streaming table receives 500 Spark micro-batches per hour (one every ~7 seconds). After 6 months, the `_delta_log/` directory has 2.6M JSON files. Listing the log directory takes 30+ seconds. Operations like DESCRIBE HISTORY are hanging. Fix this.

**Answer:**

```
Root cause:
  500 commits/hour × 24 hours × 180 days = 2,160,000 commit JSON files
  Delta checkpoint every 10 commits = 216,000 checkpoint files
  Total: 2.3M+ files in _delta_log/
  S3 LIST on 2M+ files: slow and expensive

Immediate fixes:

1. Force checkpoint creation
   spark.sql("ALTER TABLE streaming_table SET TBLPROPERTIES ('delta.checkpointInterval' = '1')")
   -- Every commit creates a checkpoint (reduces JSON reads on load)
   -- After one write, a fresh checkpoint exists that captures full state
   -- Revert to normal interval after fix: checkpointInterval = '10'

2. Archive old log entries (Delta log retention)
   spark.sql("""
     ALTER TABLE streaming_table
     SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 7 days')
   """)
   -- Delta will clean up log files older than 7 days on next VACUUM
   spark.sql("VACUUM streaming_table RETAIN 168 HOURS")
   -- After VACUUM: _delta_log/ shrinks dramatically

3. Reduce commit frequency (root cause fix)
   -- 500 commits/hour is too high; each Spark trigger = 1 commit
   -- Current trigger: processingTime="7 seconds"
   -- Change to: processingTime="5 minutes" → 12 commits/hour (97% reduction)
   -- Or: availableNow=True (batch-triggered, not continuous)
   
   Tradeoff: latency increases from 7 seconds to 5 minutes
   Evaluate: is 5-minute latency acceptable for this use case?
   If yes: change trigger. If no: keep 7-sec trigger but monitor log growth.

4. Long-term: Enable Delta liquid clustering + compact
   OPTIMIZE streaming_table
   -- Reduces data file count; doesn't directly fix log file count
   -- But fewer data files = smaller checkpoint files = faster reads

Expected result: _delta_log/ from 2.3M files → ~2,000 files after VACUUM
```

---

## Scenario 3: Build a Delta-Based CDC Pipeline with Exactly-Once

**Question:** You're building a CDC pipeline: Kafka (order updates) → Spark Structured Streaming → Delta Silver table. The pipeline must guarantee exactly-once delivery (no duplicates, no missing records). How do you design this?

**Answer:**

```
Design:

Component 1: Kafka Source
  Read from Kafka with maxOffsetsPerTrigger to control batch size
  kafka_stream = spark.readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "order-updates") \
      .option("startingOffsets", "latest") \
      .option("maxOffsetsPerTrigger", 50000) \
      .option("failOnDataLoss", "false")  \  # handle Kafka retention gaps
      .load()

Component 2: Checkpoint (Kafka offset tracking)
  Spark checkpoint location tracks: which Kafka offsets have been committed
  On restart: Spark reads checkpoint, resumes from last committed offset
  .option("checkpointLocation", "s3://bucket/checkpoints/silver_orders")
  
  This ensures: each Kafka message is processed exactly once
  (At-least-once at Kafka level + Delta idempotent MERGE = exactly-once)

Component 3: Delta MERGE (idempotent write)
  def process_batch(batch_df, batch_id):
      # Deduplicate within batch (same order_id might appear twice in one batch)
      deduped = batch_df.dropDuplicates(["order_id"])
      
      # MERGE into Delta (idempotent: re-running same batch = same result)
      DeltaTable.forPath(spark, silver_path).alias("t") \
          .merge(deduped.alias("s"), "t.order_id = s.order_id") \
          .whenMatchedUpdate(
              condition="s.updated_at > t.updated_at",  # only update if newer
              set={"status": "s.status", "updated_at": "s.updated_at"}
          ) \
          .whenNotMatchedInsertAll() \
          .execute()
  
  kafka_stream.writeStream \
      .foreachBatch(process_batch) \
      .option("checkpointLocation", "s3://bucket/checkpoints/silver_orders") \
      .trigger(processingTime="1 minute") \
      .start()

Why this is exactly-once:
  1. Kafka offset tracked in checkpoint → no messages skipped or re-processed across restarts
  2. MERGE with condition "s.updated_at > t.updated_at" → re-processing older messages
     doesn't overwrite newer state
  3. dropDuplicates within batch → handles same key appearing multiple times in micro-batch
  4. Delta transaction log → partial writes are never visible (atomicity)

Test for exactly-once: inject failure mid-batch, restart, verify no duplicates in Silver
```
