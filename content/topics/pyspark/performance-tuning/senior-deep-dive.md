---
title: "PySpark Performance Tuning - Senior Deep Dive"
topic: pyspark
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, performance, skew, bucketing, speculation, dynamic-allocation, cluster-sizing]
---

# PySpark Performance Tuning — Senior-Level Deep Dive

## Data Skew — The Silent Killer

### Detecting Skew

```python
from pyspark.sql.functions import spark_partition_id, count

# Method 1: Check partition sizes
df.groupBy(spark_partition_id().alias("partition")) \
    .count() \
    .orderBy(col("count").desc()) \
    .show(20)
# If max partition is 100x larger than median: severe skew

# Method 2: Check join key distribution
df.groupBy("join_key").count() \
    .orderBy(col("count").desc()) \
    .show(10)
# If top key has 10M rows and average is 1K: skew on that key

# Method 3: Spark UI
# Look at: Stages → Stage N → Tasks → Duration
# If one task takes 30 minutes and others take 30 seconds: skew
```

### Skew Mitigation Techniques

**Technique 1: Broadcast Join (eliminates the problem entirely)**

```python
# If the skewed side is the SMALL table: broadcast it
# No partition-based join at all → no skew possible
result = large_skewed.join(broadcast(small_table), "key")
```

**Technique 2: Salting (for large × large joins with known hot keys)**

```python
from pyspark.sql.functions import floor, rand, concat, lit, explode, array

SALT_RANGE = 20  # Split hot key across 20 partitions

# Identify hot keys (keys with >1M rows)
hot_keys = large_df.groupBy("key").count() \
    .filter("count > 1000000") \
    .select("key").rdd.flatMap(lambda x: x).collect()

# Salt the large table: add random suffix to hot keys
large_salted = large_df.withColumn("salted_key",
    when(col("key").isin(hot_keys),
         concat(col("key"), lit("_"), floor(rand() * SALT_RANGE).cast("string")))
    .otherwise(col("key"))
)

# Replicate small table rows for hot keys
small_exploded = small_df.withColumn("salted_key",
    when(col("key").isin(hot_keys),
         explode(array([concat(col("key"), lit(f"_{i}")) for i in range(SALT_RANGE)])))
    .otherwise(col("key"))
)

# Join on salted key — hot key spread across 20 partitions
result = large_salted.join(small_exploded, "salted_key") \
    .drop("salted_key")
```

**Technique 3: Isolate and process hot keys separately**

```python
# Split into hot (broadcast-joinable) and cold (normal join) paths
hot_key_list = ["NULL", "UNKNOWN", "DEFAULT"]

# Hot path: small dimension filtered to hot keys → broadcast
large_hot = large_df.filter(col("key").isin(hot_key_list))
small_hot = small_df.filter(col("key").isin(hot_key_list))
result_hot = large_hot.join(broadcast(small_hot), "key")

# Cold path: normal sort-merge join (balanced, no skew)
large_cold = large_df.filter(~col("key").isin(hot_key_list))
result_cold = large_cold.join(small_df, "key")

# Combine
result = result_hot.unionByName(result_cold)
```

**Technique 4: AQE Skew Join (automatic in Spark 3.0+)**

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
# Spark automatically detects and splits oversized partitions at runtime
```

---

## Bucketing — Pre-Shuffled Storage

Bucketing writes data pre-partitioned by a specific column. Subsequent joins on that column skip the shuffle entirely.

```python
# Write bucketed (pre-shuffled by customer_id into 100 buckets)
orders.write \
    .bucketBy(100, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("orders_bucketed")

customers.write \
    .bucketBy(100, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("customers_bucketed")

# Join on bucketed tables: ZERO SHUFFLE
result = spark.table("orders_bucketed") \
    .join(spark.table("customers_bucketed"), "customer_id")
# Plan shows: no Exchange (shuffle) operator! Data already co-located.
```

**When bucketing helps:**
- Same join pattern executed repeatedly (daily ETL joining same tables)
- Tables are large enough that shuffle cost is significant (>10 GB)
- The bucket count matches between joined tables (must be the same number)

**When NOT to bucket:**
- Ad-hoc exploratory queries (join patterns change)
- One-time processing (shuffle once is fine)
- Tables that change frequently (re-bucketing is expensive)

---

## Dynamic Allocation — Auto-Scaling Executors

```python
# Let Spark add/remove executors based on workload
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "5s")

# How it works:
# - Tasks queuing > 5 seconds → request more executors (scale up)
# - Executor idle > 60 seconds → release it (scale down)
# - Keeps between 2-100 executors
```

**When to use dynamic allocation:**
- Variable workload intensity within a job (heavy shuffle stage → light map stage)
- Shared clusters with multiple jobs
- Cost optimization (don't hold idle resources)

**When to use fixed allocation:**
- Predictable, steady workloads
- Strict SLA (can't wait for executors to spin up)
- Streaming jobs (consistent load)

---

## Speculation — Handling Slow Tasks

```python
# If one task is much slower than others (not skew, but slow node/hardware):
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")  # Launch speculative if 1.5x median
spark.conf.set("spark.speculation.quantile", "0.9")    # After 90% of tasks done

# How it works:
# - If 90% of tasks are done and one task takes >1.5x the median time
# - Spark launches a DUPLICATE task on another executor
# - Whichever finishes first wins, the other is killed
```

> **Caution with speculation:** Only safe for idempotent tasks. If your task writes to an external system (database, API), speculative duplicate can cause double-writes. Disable for non-idempotent operations.

---

## Cluster Sizing Guide

### How to Size Executors

```
Rule of thumb (YARN/K8s):
- Executor cores: 4-5 (more = GC issues, less = underutilized)
- Executor memory: 4-8 GB per core
- Number of executors: total_cores_needed / cores_per_executor

Example: Process 500 GB of data
- Target partition size: 128 MB
- Partitions needed: 500,000 MB / 128 MB = ~4000
- Cores needed for parallelism: 4000 / 3 waves = ~1300 cores (if we want 3 waves)
- With 5 cores/executor: 1300 / 5 = 260 executors
- Memory/executor: 5 cores × 6 GB/core = 30 GB
- Plus overhead: 30 GB + 4 GB overhead = 34 GB total

Configuration:
spark.executor.cores = 5
spark.executor.memory = 30g
spark.executor.memoryOverhead = 4g
spark.executor.instances = 260  (or use dynamic allocation)
```

### Memory per Core Trade-offs

| Memory/Core | Good For | Risk |
|-------------|----------|------|
| 2-3 GB | Simple transforms, filters | OOM on large joins/sorts |
| 4-6 GB | Most workloads (joins, aggregations) | Balance of cost and capability |
| 8-12 GB | Large joins, ML training, heavy UDFs | Longer GC pauses |
| 16+ GB | Extremely large aggregations | Serious GC issues, wasteful |

---

## Advanced Configuration Patterns

### Pattern 1: Heavy Join Workload

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")
```

### Pattern 2: Heavy Aggregation Workload

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.memory.fraction", "0.8")  # More memory for execution
spark.conf.set("spark.executor.memory", "12g")
spark.conf.set("spark.executor.cores", "4")
```

### Pattern 3: I/O-Heavy (Read Large Files from S3)

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")  # Read partition size
spark.conf.set("spark.sql.files.openCostInBytes", "4MB")      # Small file overhead
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "200")
spark.conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
# More executor cores: I/O bound tasks benefit from concurrency
spark.conf.set("spark.executor.cores", "8")
```

---

## Performance Tuning Methodology

```
1. MEASURE: Identify the slow stage in Spark UI
   └── Which stage? What operation? How long?

2. DIAGNOSE: What's the bottleneck?
   ├── Shuffle too large? → Broadcast or pre-partition
   ├── Data skew? → Salt or separate hot keys
   ├── Spilling to disk? → More memory or smaller partitions
   ├── Too many small tasks? → Enable AQE coalescing
   ├── UDF overhead? → Replace with native functions
   └── I/O bound? → Check file sizes, increase parallelism

3. FIX: Apply one optimization at a time
   └── Measure again to confirm improvement

4. REPEAT: Until meeting SLA
```

---

## Interview Tips

> **Tip 1:** "How do you handle data skew in a join?" — "First I detect it: check join key distribution and look for outlier counts. Then: (1) If one side is small, broadcast it (eliminates the problem). (2) If both sides are large, salt the hot keys and replicate the corresponding dimension rows. (3) In Spark 3.0+, enable AQE skew join handling (automatic). (4) As a last resort, isolate hot keys into a separate broadcast-join path."

> **Tip 2:** "What's bucketing and when do you use it?" — "Bucketing pre-shuffles data at write time into a fixed number of hash-based files. When two tables are bucketed on the same column with the same bucket count, joins on that column skip the shuffle entirely. Use it for frequently-joined tables in recurring pipelines. Don't use for ad-hoc exploration or frequently-changing tables."

> **Tip 3:** "How do you size a Spark cluster?" — "Three factors: (1) Data volume → determines partition count (target 128 MB each). (2) Parallelism needed → total cores = partitions / desired waves. (3) Memory per task → 4-6 GB/core for most workloads, more for heavy joins. I typically use 5 cores and 30 GB per executor. Dynamic allocation auto-scales the executor count."

---

## ⚡ Cheat Sheet

### Spark Configuration Knobs

| Config Key | Default | Recommended (large jobs) | What it controls |
|---|---|---|---|
| `spark.executor.memory` | 1g | 16–30g | Heap memory per executor |
| `spark.driver.memory` | 1g | 4–8g | Driver heap (collect/broadcast) |
| `spark.memory.fraction` | 0.6 | 0.7–0.75 | Fraction of heap for execution+storage |
| `spark.memory.storageFraction` | 0.5 | 0.3–0.4 | Fraction of memory.fraction reserved for cache |
| `spark.sql.shuffle.partitions` | 200 | 500–2000 | Post-shuffle partition count |
| `spark.shuffle.file.buffer` | 32k | 64k–128k | Buffer size for shuffle write |
| `spark.reducer.maxSizeInFlight` | 48m | 96m | Max data fetched per reduce task at once |
| `spark.sql.adaptive.enabled` | true (Spark 3+) | true | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | true | AQE: merge small post-shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | true | true | AQE: auto-split skewed partitions |
| `spark.sql.autoBroadcastJoinThreshold` | 10m | 50m–200m | Max table size for auto-broadcast join |
| `spark.default.parallelism` | 2× cores | 2–3× total cores | Default RDD partition count |
| `spark.sql.files.maxPartitionBytes` | 128m | 128m–256m | Max bytes per file-scan partition |

### Signs of Misconfiguration → Fix

| Symptom | Likely Cause | Fix |
|---|---|---|
| Executor OOM / GC overhead | Heap too small or too many partitions in memory | Increase `spark.executor.memory`; reduce `spark.sql.shuffle.partitions` |
| Driver OOM | Collecting large dataset or large broadcast | Increase `spark.driver.memory`; avoid `collect()` on large DFs |
| Spill to disk (high I/O) | Insufficient execution memory | Increase `spark.memory.fraction`; add more memory per executor |
| Stage stuck at 199/200 tasks | Skewed partition | Enable AQE skew join; salt keys manually |
| Too many small shuffle files | `spark.sql.shuffle.partitions` too high | Lower shuffle partitions or enable AQE partition coalescing |
| Join defaulting to SortMerge | Small table above broadcast threshold | Raise `spark.sql.autoBroadcastJoinThreshold` or use `broadcast()` hint |
| Low parallelism / idle cores | `spark.default.parallelism` too low | Set to 2–3× total executor cores |
| Slow file reads | Partition size too small (many tiny tasks) | Increase `spark.sql.files.maxPartitionBytes` |
