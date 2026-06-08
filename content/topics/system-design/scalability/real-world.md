---
title: "Scalability & Partitioning — Real World"
topic: system-design
subtopic: scalability
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, scalability, performance, partitioning, production]
---

# Scalability & Partitioning — Real World

## Pattern: Scaling a Snowflake DW from Slow to Fast

**Situation:** A company's Snowflake data warehouse has a 500GB `orders` table. BI queries used to take 5 seconds; now they take 3 minutes after 2 years of data growth.

```sql
-- Step 1: Diagnose with query profile
-- Snowflake UI → Query History → worst queries → Query Profile
-- Look for: TableScan node consuming >80% of time

-- Step 2: Check partition pruning
SELECT query_text,
       partitions_scanned,
       partitions_total,
       ROUND(partitions_scanned / partitions_total * 100, 1) as pct_scanned
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%orders%'
  AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY total_elapsed_time DESC;
-- If pct_scanned > 50%: add clustering key to reduce scan

-- Step 3: Add clustering key
ALTER TABLE orders CLUSTER BY (TO_DATE(order_date), region);
-- Snowflake auto-clustering service will recluster incrementally

-- Step 4: Check clustering effectiveness (after 24 hours)
SELECT SYSTEM$CLUSTERING_INFORMATION('orders',
    '(TO_DATE(order_date), region)');
-- "average_depth": 1.12 ← excellent (< 1.5 is good)
-- "total_partition_count": 4200
-- "partitions_not_contributing_to_clustering": 150 ← few unclustered partitions

-- Step 5: Scale warehouse for BI queries
-- Separate warehouses for ETL (large, short burst) vs BI (medium, auto-suspend)
ALTER WAREHOUSE bi_warehouse SET
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MAX_CLUSTER_COUNT = 3;  -- multi-cluster: scale out during high concurrency
```

---

## Pattern: Handling Black Friday Traffic Spike

**Situation:** 10× normal event volume during Black Friday. Kafka pipeline falls behind.

```python
# Pre-Black Friday preparation:
# 1. Pre-scale Kafka: double partition count on orders topic
# 2. Pre-scale consumer group: deploy 2× consumer instances
# 3. Set rate limit to prevent OOM during catch-up

# Spark Structured Streaming with rate limiting:
query = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "orders")
        .option("maxOffsetsPerTrigger", "500000")   # cap at 500K/batch (normal: 50K)
        .load()
    .writeStream
        .format("delta")
        .option("checkpointLocation", "s3://bucket/checkpoints/orders_bf")
        .trigger(processingTime="30 seconds")    # increase from 60s to 30s during peak
        .start("s3://bucket/delta/orders")
)

# During event: monitor lag
# kafka-consumer-groups.sh --describe --group orders-consumer

# Post-event: consolidate small files from high-frequency writes
spark.sql("OPTIMIZE delta.`s3://bucket/delta/orders` WHERE order_date = '2024-11-29'")
```

---

## Common Scalability Issues and Fixes

| Symptom | Root Cause | Fix |
|---|---|---|
| Query time grows linearly with data | No partitioning; full table scan | Add partition key; check pruning is happening |
| One Spark task takes 10× longer than others | Data skew on join/group key | Salting, broadcast join, or AQE skew join |
| Kafka consumer lag growing | Too few consumer instances | Scale consumer instances up to partition count |
| Pipeline OOM errors | Partition too large in memory | Repartition to smaller sizes; filter earlier |
| S3 listing is slow | Too many small files | Delta OPTIMIZE; coarser partitioning |
| Snowflake query scans 100% partitions | Missing or wrong cluster key | Add cluster key matching WHERE clause columns |
| dbt model runs 2 hours | Full table scan in dbt model | Add `WHERE event_date = current_date` incremental config |

---

## Interview Tips

> **Tip 1:** "How do you choose partition size in Spark?" — Target 128MB–1GB per partition. Too small (< 10MB): overhead from scheduling thousands of tiny tasks. Too large (> 2GB): risk of OOM, especially during joins. Check: `df.rdd.getNumPartitions()` and `df.count() / df.rdd.getNumPartitions()` to estimate rows per partition. Rule of thumb: 200 partitions for 200GB data = 1GB/partition. Tune with `spark.sql.shuffle.partitions` (default 200; reduce for small data, increase for large).

> **Tip 2:** "How does Snowflake's micro-partitioning differ from traditional partitioning?" — Traditional partitioning: user-defined partition key, data physically stored in separate files per partition value. Snowflake micro-partitions: automatic (no user definition), each micro-partition is 50–500MB compressed, and Snowflake stores min/max stats for every column in every micro-partition. Queries use these stats to skip entire micro-partitions. Clustering keys tell Snowflake how to physically arrange data so similar values land in the same micro-partitions — improving skip efficiency.

> **Tip 3:** "A Spark job runs fine locally but OOMs in production on a 1TB dataset. What do you check?" — 1. Shuffle partition count: if `spark.sql.shuffle.partitions=200`, each partition handles 5GB during a shuffle on 1TB → OOM. Increase to 2000. 2. Data skew: one partition with 200GB, rest with < 1GB. 3. Broadcast join with a table that's actually large (300MB+ shouldn't be broadcast). 4. Caching a large DataFrame in memory. 5. `collect()` or `toPandas()` pulling all data to driver. Use `explain()` to see the full plan before running.
