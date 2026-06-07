---
title: "PySpark Memory Management - Scenario Questions"
topic: pyspark
subtopic: memory-management
content_type: scenario_question
tags: [pyspark, memory, interview, scenarios, oom]
---

# Scenario Questions — PySpark Memory Management

<article data-difficulty="junior">

## 🟢 Junior: Driver OOM

**Scenario:** Your Spark job fails with `java.lang.OutOfMemoryError: Java heap space` on the DRIVER (not executor). The job does: read 50 GB from S3, join with a 2 GB table, then calls `df.collect()` to return results. Driver memory is 4 GB. What's wrong?

<details>
<summary>✅ Solution</summary>

**Problem:** `df.collect()` pulls the ENTIRE result set to the driver's JVM memory. If the result is larger than driver memory → OOM.

**Fix:** Never collect large results. Choose an alternative:

```python
# BAD
results = big_df.collect()  # Pulls all 50 GB to driver → OOM!

# FIX 1: Write to storage instead of collecting
big_df.write.parquet("s3://output/results/")

# FIX 2: If you need a summary, aggregate first
summary = big_df.groupBy("region").agg(count("*"), sum("amount"))
small_result = summary.collect()  # Only a few rows — safe!

# FIX 3: If you need a sample
sample = big_df.limit(1000).collect()  # 1000 rows fits easily

# FIX 4: If you truly need large data in Python, use toPandas on aggregated result
pdf = summary.toPandas()  # Small DataFrame → safe
```

**Key rule:** Driver memory should only hold: job coordination, broadcast variables, and small results. Never use collect() on production data.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Executor OOM During collect_list

**Scenario:** This code crashes executors (8 GB memory) on the groupBy stage:

```python
result = events.groupBy("user_id").agg(collect_list("event_data").alias("all_events"))
```

Some users have 5M+ events. How do you fix it without just adding more memory?

<details>
<summary>✅ Solution</summary>

**Root cause:** `collect_list` accumulates ALL events for a user into one array. A user with 5M events × 200 bytes = 1 GB for ONE group. Multiple such groups hit the same executor → OOM.

**Fix 1: Cap the collection (keep only recent N events)**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, collect_list

# Limit to last 100 events per user
w = Window.partitionBy("user_id").orderBy(col("event_time").desc())
limited = events.withColumn("rn", row_number().over(w)).filter("rn <= 100").drop("rn")

# Now safe: max 100 elements per group
result = limited.groupBy("user_id").agg(collect_list("event_data").alias("recent_events"))
```

**Fix 2: Don't collect at all — use aggregations instead**
```python
# Instead of collecting all events, compute summary metrics
result = events.groupBy("user_id").agg(
    count("*").alias("total_events"),
    countDistinct("event_type").alias("unique_types"),
    max("event_time").alias("last_active"),
    avg("duration").alias("avg_duration"),
)
# Aggregations use O(1) memory per group regardless of group size
```

**Fix 3: More partitions (reduce groups per executor)**
```python
spark.conf.set("spark.sql.shuffle.partitions", "2000")
# More partitions → fewer user groups per executor → less memory pressure
# But doesn't fix the fundamental problem if ONE group is 1 GB
```

**Fix 4: Use struct + window for recent events (no groupBy)**
```python
# Create struct of last N events using window function
w = Window.partitionBy("user_id").orderBy(col("event_time").desc()).rowsBetween(0, 99)
# This doesn't work directly with collect_list in window, but demonstrates the concept:
# Process per-partition with mapInPandas for full control
```

**Best answer:** Fix 1 (cap) + Fix 2 (aggregate instead of collect where possible). The key insight: `collect_list` has UNBOUNDED memory — always cap it in production.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Job Spilling 500 GB — Diagnose and Fix

**Scenario:** Your daily ETL joins two large tables (2B rows × 500M rows) and groups by customer_id. The job completes but takes 4 hours (SLA: 1 hour). Spark UI shows 500 GB of shuffle spill to disk. Executor config: 16 GB memory, 5 cores, 200 shuffle partitions. Fix without increasing cluster size.

<details>
<summary>✅ Solution</summary>

**Diagnosis from Spark UI:**
- Shuffle write: 800 GB (both tables shuffled for sort-merge join)
- Shuffle spill (disk): 500 GB across all executors
- 200 shuffle partitions → average 4 GB per partition
- 16 GB executor, 5 cores, memory.fraction=0.6: execution memory = 9.6 GB total
- Per-task execution memory = 9.6 GB / 5 cores = 1.9 GB per task
- Partition size (4 GB) >> per-task memory (1.9 GB) → SPILL!

**Root cause:** Partitions (4 GB) are much larger than per-task execution memory (1.9 GB).

**Fix 1: Increase shuffle partitions (most impactful, free)**
```python
# Current: 800 GB / 200 partitions = 4 GB each (too large!)
# Target: 128 MB per partition
# New partitions: 800 GB / 128 MB = 6250 partitions
spark.conf.set("spark.sql.shuffle.partitions", "6000")
# Now: 800 GB / 6000 = 133 MB per partition (fits easily in 1.9 GB per task)
```

**Fix 2: Broadcast the smaller table (if feasible)**
```python
# 500M rows — how large is it after filter/projection?
# If the join only uses a few columns from the 500M table:
small_side = dim_table.select("key", "needed_column")  # Maybe only 50 GB
# If < 1 GB after projection: broadcast!
result = large_table.join(broadcast(small_side), "key")
# Eliminates 800 GB shuffle entirely → 0 spill
```

**Fix 3: Enable AQE (automatic partition coalescing + skew handling)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
# AQE auto-sizes partitions at runtime based on actual data
# Even with shuffle.partitions=200, AQE can split large partitions
```

**Fix 4: Reduce cores per executor (more memory per task)**
```python
spark.conf.set("spark.executor.cores", "3")  # Was 5
# Now per-task memory = 9.6 GB / 3 = 3.2 GB (still not enough for 4 GB partition)
# Must combine with Fix 1 (more partitions)
```

**Optimal combined fix:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "4000")  # Smaller partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE handles the rest: coalesces where partitions are tiny, splits where skewed
# Expected: 4 hours → 45 minutes (zero spill)
```

</details>

</article>
