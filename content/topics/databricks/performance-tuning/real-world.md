---
title: "Performance Tuning - Real-World Examples"
topic: databricks
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, spark, performance, production, skew, optimization, incident]
---

# Performance Tuning — Real-World Production Examples

## Production Pattern: Fixing a Skewed Aggregation

A clickstream pipeline was taking 4+ hours every night. Root cause: 0.1% of users drove 60% of all events (bot traffic):

```python
# Diagnosis: task duration distribution exposed extreme skew
# Min: 2s, Median: 45s, Max: 14,400s (4 hours!)
# One partition had 800M rows (bot traffic), others had <100K

# Solution 1: Filter bots before aggregation
enriched = (
    raw_events
    .filter("is_bot = false")          # remove known bots
    .filter("session_count_24h < 500")  # heuristic bot filter
)

# Solution 2: Enable AQE skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")  # lower threshold

# Solution 3: Salt the aggregation key for remaining high-traffic users
N_SALT = 20

per_user = (
    enriched
    .withColumn("salt", (F.hash(F.col("user_id")) % N_SALT).cast("int"))
    .groupBy("user_id", "salt", F.window("event_time", "1 hour"))
    .agg(F.count("*").alias("events_salt"))
)

# Merge salted aggregates (second pass — cheap, already reduced)
final = (
    per_user
    .groupBy("user_id", F.col("window.start").alias("hour"))
    .agg(F.sum("events_salt").alias("events_per_hour"))
)

# Result: 4h → 22 minutes (11x improvement)
```

---

## Production Pattern: Read Amplification Fix

A reporting job read 2TB of data to produce a 50MB report. Z-ordering dropped it to 8GB:

```sql
-- Before: full table scan every night
SELECT
    customer_id,
    SUM(amount) AS monthly_revenue,
    COUNT(*) AS order_count
FROM prod.sales.orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'us-east'
GROUP BY customer_id;

-- EXPLAIN showed: FileScan reading 2TB (all files, no pruning)
-- Root cause: table was partitioned by `year` only, not `region`
-- Z-ordering by order_date and region was never run

-- Fix: add Z-ordering on the filter columns
OPTIMIZE prod.sales.orders ZORDER BY (order_date, region);
-- Runtime: 45 minutes (one-time cost)

-- After optimization: same query reads 8GB (97% data skipped)
-- Query time: 3min → 18 seconds
```

**Why it worked:** Delta's data skipping reads file statistics (min/max per column per file). After Z-ordering, all January + us-east rows are co-located → most files are completely out of range and skipped.

---

## Production Pattern: Broadcast Join Regression

A pipeline slowed from 8 minutes to 45 minutes after a data volume increase. Root cause: a broadcast join started OOMing executors:

```python
# Before fix: implicit broadcast
customer_dim = spark.table("prod.dimensions.customers")  # was 80MB, now 320MB

orders = spark.table("prod.sales.orders")

# Databricks auto-broadcast threshold was 200MB
# When dim table grew to 320MB, broadcast stopped → fell back to sort-merge join
# Sort-merge join on 2B row table → 45 minute shuffle
result = orders.join(customer_dim, "customer_id")  # no longer broadcasting!

# Fix: explicitly raise threshold OR don't rely on auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")  # raise threshold

# OR: use broadcast hint (doesn't rely on threshold)
result = orders.join(broadcast(customer_dim), "customer_id")

# Better: monitor dimension table size in CI/CD
dim_size_mb = customer_dim.cache().count()  # trigger caching
df_size = spark._jvm.org.apache.spark.util.SizeEstimator.estimate(customer_dim._jdf) / 1e6
assert df_size < 300, f"customer_dim is {df_size:.0f}MB — consider removing broadcast hint"
```

---

## Production Pattern: Small Files Problem in Streaming Pipeline

A Structured Streaming job writing to Delta every 30 seconds produced 2,880 tiny files/day per table. After 30 days: 86,400 files. Query performance degraded 15x:

```python
# Problem: streaming micro-batches write one file per trigger per partition

# Solution 1: Auto Optimize (Databricks specific)
spark.sql("""
    ALTER TABLE prod.streaming.events
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',  -- merge small writes
        'delta.autoOptimize.autoCompact' = 'true'     -- compact in background
    )
""")

# Solution 2: Reduce streaming checkpoint frequency
query = (
    events_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 minutes")   # was "30 seconds" → reduces file count 10x
    .toTable("prod.streaming.events")
)

# Solution 3: Scheduled OPTIMIZE in a separate job (runs daily off-peak)
spark.sql("OPTIMIZE prod.streaming.events ZORDER BY (user_id, event_date)")

# Result: 86,400 files → 240 files after daily OPTIMIZE
# Query performance restored to baseline
```

---

## Cluster Right-Sizing Playbook

```python
# Step 1: Run baseline benchmark on current cluster
baseline = benchmark("current", lambda: run_etl_job(), iterations=3)
print(f"Baseline: {baseline['avg_s']:.1f}s")

# Step 2: Check utilization (are we over-provisioned?)
# In Databricks: Cluster → Ganglia metrics → CPU/Memory utilization
# If CPU < 50% at peak: over-provisioned
# If Memory < 60% at peak: over-provisioned

# Step 3: Test scale-down
# If avg CPU is 30%, try half the workers
# If avg CPU is 80%, cluster is well-utilized — don't scale down

# Step 4: Test instance types
# Memory-intensive (shuffles, caches): r5.2xlarge (memory optimized)
# Compute-intensive (ML, joins): c5.4xlarge (compute optimized)
# General ETL: i3.xlarge (good balance, local NVMe disk)

# Step 5: DBU efficiency score
configs_tested = {
    "8x i3.xlarge":  {"time_s": 1200, "dbu_rate": 8.0},
    "4x i3.2xlarge": {"time_s": 900,  "dbu_rate": 8.0},
    "4x r5.2xlarge": {"time_s": 750,  "dbu_rate": 8.0},
}

for config, metrics in configs_tested.items():
    cost = (metrics["time_s"] / 3600) * metrics["dbu_rate"] * 0.22
    print(f"{config}: {metrics['time_s']}s, ${cost:.2f}")

# Output:
# 8x i3.xlarge:  1200s, $0.49
# 4x i3.2xlarge: 900s,  $0.44  ← same cost, faster
# 4x r5.2xlarge: 750s,  $0.37  ← cheapest AND fastest (memory-bound job)
```
