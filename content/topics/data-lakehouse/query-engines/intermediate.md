---
title: "Query Engines (Trino, Spark, Flink) — Intermediate"
topic: data-lakehouse
subtopic: query-engines
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [trino, spark, flink, optimization, execution-plans]
---

# Query Engines — Intermediate

## Trino Query Optimization

```sql
-- 1. Understand the execution plan
EXPLAIN SELECT customer_id, SUM(amount) 
FROM iceberg.silver.orders 
WHERE order_date >= DATE '2024-01-01'
GROUP BY customer_id;
-- Look for: TableScan filter pushdown, PartitionedOutput, HashAggregation

-- 2. Partition pruning (critical for S3 query cost)
-- GOOD: uses partition column in WHERE clause
SELECT * FROM iceberg.silver.orders WHERE order_date = DATE '2024-01-15';
-- BAD: no partition filter → full scan
SELECT * FROM iceberg.silver.orders WHERE YEAR(order_date) = 2024;
-- Trino can't push YEAR() function to partition pruning in all cases

-- 3. Small table broadcast joins
-- Trino auto-broadcasts small tables (< 100MB by default)
-- Force broadcast for a medium table:
SELECT /*+ USE_BROADCAST(d) */ f.*, d.product_name
FROM iceberg.gold.fact_orders f
JOIN iceberg.gold.dim_products d ON f.product_id = d.product_id;
-- join.broadcast-max-rows = 100000000
-- join.max-broadcast-table-size = 100MB

-- 4. Dynamic filtering (Trino 354+)
-- Trino can push filter from a small dim table to a large fact table
-- Requires: SemiJoin or Join where filter side is small
SELECT * FROM iceberg.silver.orders o
JOIN iceberg.silver.customers c ON o.customer_id = c.customer_id
WHERE c.country = 'US';
-- Dynamic filter: "customer_id IN (set of US customer_ids)" pushed to orders scan

-- 5. Statistics collection (required for join ordering)
ANALYZE iceberg.silver.orders;
-- Collects: row count, column NDV (number of distinct values), null fraction
-- Query planner uses these to order joins (smaller result first)
```

---

## Spark Execution Tuning

```python
# 1. Adaptive Query Execution (AQE) — enabled by default in Spark 3.2+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # merge small partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")            # auto-handle data skew
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# 2. Partition tuning
# Too few partitions: underutilized executors
# Too many partitions: scheduling overhead, small tasks
# Rule of thumb: 2-4 partitions per CPU core in your cluster

# Check current partition count
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Repartition for even distribution
df = df.repartition(200)  # explicit count
df = df.repartition(col("customer_id"))  # repartition by key (for JOIN/GROUP BY)

# Coalesce (reduce without full shuffle)
df = df.coalesce(10)  # only shrinks, no full reshuffle

# 3. Caching strategy
# Cache ONLY if DataFrame is used 3+ times in the same job
df_expensive = spark.sql("SELECT ... complex join ...")
df_expensive.cache()
df_expensive.count()  # trigger materialization

# Unpersist when done (release memory)
df_expensive.unpersist()

# 4. Broadcast joins (key optimization)
from pyspark.sql.functions import broadcast

# Small table (< 10MB): broadcast to all executors (avoid shuffle)
result = large_df.join(broadcast(small_dim_df), "product_id")

# Configure threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # auto-broadcast under this size

# 5. Avoid wide transformations where possible
# Wide: groupBy, join, distinct (cause shuffle)
# Narrow: filter, select, map (no shuffle)
# Rule: filter aggressively BEFORE any wide transformation
filtered = df.filter(col("order_date") >= "2024-01-01")  # filter first
result = filtered.groupBy("customer_id").agg(sum("amount"))  # then aggregate
```

---

## Flink Windowing and Watermarks

```python
# Flink SQL: windowing functions for streaming aggregation

-- Tumbling window: non-overlapping, fixed size
INSERT INTO gold.hourly_revenue
SELECT
  TUMBLE_START(event_ts, INTERVAL '1' HOUR) AS window_start,
  TUMBLE_END(event_ts, INTERVAL '1' HOUR) AS window_end,
  region,
  SUM(amount) AS total_revenue,
  COUNT(*) AS order_count
FROM silver.orders
GROUP BY TUMBLE(event_ts, INTERVAL '1' HOUR), region;

-- Sliding window: overlapping windows
SELECT
  HOP_START(event_ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_start,
  SUM(amount) AS revenue_last_hour
FROM silver.orders
GROUP BY HOP(event_ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR), region;

-- Session window: gap-based (closes N seconds after last event)
SELECT
  SESSION_START(event_ts, INTERVAL '30' MINUTE) AS session_start,
  SESSION_END(event_ts, INTERVAL '30' MINUTE) AS session_end,
  user_id,
  COUNT(*) AS events_in_session
FROM clickstream
GROUP BY SESSION(event_ts, INTERVAL '30' MINUTE), user_id;

-- Watermarks: handle late data
-- Table DDL with watermark definition:
CREATE TABLE orders_stream (
  order_id     BIGINT,
  amount       DOUBLE,
  event_ts     TIMESTAMP(3),
  -- Watermark: tolerate up to 5 minutes of late data
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka', ...);

-- Events arriving up to 5 minutes late are included in their correct window
-- Events arriving > 5 minutes late are DROPPED (past the watermark)
```

---

## Query Federation with Trino

```sql
-- Trino connects to multiple data sources in one SQL query
-- Each source = a "catalog" in Trino config

-- Available catalogs (example setup):
-- iceberg  → S3 + Glue (Iceberg tables)
-- postgresql → RDS Postgres (OLTP)
-- redis → Redis (feature store / cache)
-- elasticsearch → ES index (search)

-- Join across systems in one query:
SELECT
  o.order_id,
  o.amount,
  c.email,           -- from Postgres (OLTP source of truth)
  f.clv_score,       -- from Redis feature store
  o.status
FROM iceberg.silver.orders o
JOIN postgresql.public.customers c ON o.customer_id = c.id
LEFT JOIN redis.features.customer_clv f ON o.customer_id = f.customer_id
WHERE o.order_date = DATE '2024-01-15'
  AND c.country = 'US';

-- Performance note: Trino pushes filters to each source
-- postgres filter: WHERE id IN (US customer IDs from orders)
-- Dynamic filtering reduces data fetched from each source

-- ETL use case: migrate data across systems without intermediate files
INSERT INTO iceberg.silver.migrated_orders
SELECT id, customer_id, amount, status, created_at
FROM postgresql.legacy.orders
WHERE created_at >= DATE '2024-01-01';
-- No Spark job needed for simple migrations
```

---

## Interview Tips

> **Tip 1:** "How does Trino achieve low latency when Spark takes 30+ seconds?" — Three reasons: (1) No JVM startup — Trino coordinators and workers are persistent long-running processes; (2) Pipelined execution — Trino starts returning data as soon as the first splits are ready, rather than materializing all results first; (3) In-memory processing — Trino keeps intermediate data in memory (not disk). The trade-off: Trino can OOM for very large aggregations; Spark spills to disk and can handle arbitrarily large data with enough time.

> **Tip 2:** "What are Flink checkpoints and why are they critical?" — Checkpoints are snapshots of the entire Flink job state (all operator states, including Kafka offsets) written to durable storage (S3/HDFS). If a Flink job fails, it restarts from the last checkpoint — no data is reprocessed from before the checkpoint. Without checkpoints: job restart = start from Kafka offset 0 (replay everything). Checkpoint interval = max recovery time: checkpoint every 60 seconds → worst case 60 seconds of re-processing on restart.

> **Tip 3:** "What's the difference between Spark Structured Streaming and Flink for a 1-minute latency requirement?" — Both can achieve 1-minute latency. Spark Structured Streaming: simpler Python API, Delta Lake native integration, better tooling (Databricks monitoring). Flink: more efficient for large-scale stateful processing, true event-at-a-time within each checkpoint interval, better for complex event patterns. For 1-minute latency: Spark Streaming with `processingTime="1 minute"` trigger is easier to build and operate. Choose Flink only when you need sub-second latency or very complex stateful logic.
