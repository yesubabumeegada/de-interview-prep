---
title: "Spark Performance & Tuning — Fundamentals"
topic: spark
subtopic: performance-and-tuning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, performance, shuffle, caching, partition, broadcast, explain, aqe]
---

# Spark Performance & Tuning — Fundamentals

## 🎯 Analogy

Tuning Spark is like organizing a warehouse operation. The fundamental levers are: how many workers you have (executors/cores), how big each work pile is (partition size), how often workers have to pass items to each other (shuffles), and whether common items are pre-staged near each worker (caching and broadcast).

---

## The Performance Hierarchy

Fix in this order — lower numbers have much higher impact:

```
1. Data volume reduction     (read less data)
   ↓ predicate pushdown, partition pruning, column pruning

2. Shuffle reduction         (move less data)
   ↓ broadcast joins, pre-partitioning, AQE

3. Skew elimination          (balance work)
   ↓ salting, AQE skew join, repartition

4. Memory tuning             (avoid spill)
   ↓ executor memory, storage fractions, GC settings

5. Parallelism tuning        (use all cores)
   ↓ partition count, executor count, core count
```

---

## Read Less Data: Predicate Pushdown and Column Pruning

```python
from pyspark.sql import functions as F

# Column pruning: select only needed columns EARLY
# BAD:
df = spark.read.parquet("s3://bucket/events/")
result = df.groupBy("region").sum("amount")
# Spark still reads ALL columns from Parquet

# GOOD: Catalyst does this automatically for DataFrame/SQL
# BUT: explicitly select first when reading via JDBC or complex sources
df = spark.read.parquet("s3://bucket/events/").select("region", "amount")

# Partition pruning: filter on partition columns
# BAD (no pruning):
df.filter(F.year(F.col("event_date")) == 2024)  # function disables pruning!

# GOOD (pruning):
df.filter(F.col("year") == 2024)   # direct column filter

# Verify with explain():
df.filter(F.col("year") == 2024).explain()
# Look for: PartitionFilters: [year = 2024]  (good!)
# vs ReadSchema showing all columns (bad if you don't need them)
```

---

## Shuffle: The Main Cost Center

Every shuffle operation involves:
1. Write partition data to local disk (map side)
2. Network transfer to target executors (reduce side)
3. Read and merge on reduce side

```python
# Operations that cause shuffles:
df.groupBy("region").sum("amount")          # shuffle by region
df.join(other, "key")                       # shuffle both sides by key
df.repartition(100)                         # full shuffle
df.orderBy("timestamp")                     # sort shuffle
df.distinct()                               # shuffle to deduplicate

# Operations that DON'T shuffle:
df.filter(...)
df.select(...)
df.withColumn(...)
df.limit(100)
df.coalesce(4)   # only if decreasing partitions
```

```python
# Count shuffles in your plan:
df.explain()
# Every "Exchange" node in the physical plan = one shuffle
# Minimize Exchange nodes!
```

---

## Broadcast Joins: Eliminate Shuffles Entirely

```python
from pyspark.sql.functions import broadcast

# Default: shuffle both sides (SortMergeJoin)
# Large orders (10GB) joined with dim_region (1KB) → still shuffles orders!
result = orders.join(dim_region, "region_id")

# With broadcast: no shuffle
result = orders.join(broadcast(dim_region), "region_id")
# dim_region sent once to each executor → 1KB × n_executors << 10GB shuffle

# Auto-broadcast threshold (default 10MB):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# Verify broadcast is being used:
result.explain()
# Look for: BroadcastHashJoin  (good!)
# vs SortMergeJoin             (involves shuffle)
```

---

## Partition Count: Getting It Right

```python
# Rule of thumb: 2–4 tasks per CPU core, targeting 100-256 MB per partition

# Check current partition count:
df.rdd.getNumPartitions()

# After reading from files: partitions = number of input files (or HDFS blocks)
# After a shuffle: default = spark.sql.shuffle.partitions (200)

# 200 is wrong for most jobs:
# Small data (1GB) with 200 partitions → 5MB per task → 95% scheduling overhead
# Large data (1TB) with 200 partitions → 5GB per task → OOM, spill

# Fix 1: Change the default
spark.conf.set("spark.sql.shuffle.partitions", "40")   # for ~40 cores

# Fix 2: Use AQE (auto-coalesce after shuffle)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Fix 3: Explicit repartition (when you know the data size)
df.repartition(40)
```

---

## Caching: When and What

```python
# Cache when: the same DataFrame is accessed multiple actions
# Don't cache: one-time use, or data that changes between reads

# Example: training pipeline uses same preprocessed data 3 times
preprocessed = (raw_df
    .filter("is_valid = true")
    .withColumn("features", compute_features_udf("data"))
).cache()   # ← expensive step, reused below

train_df = preprocessed.filter("split = 'train'")
val_df = preprocessed.filter("split = 'val'")
model = train(train_df)            # action 1: fills cache
evaluate(model, val_df)            # action 2: hits cache
save_features(preprocessed)       # action 3: hits cache

preprocessed.unpersist()           # free memory!
```

---

## AQE: Let Spark Tune Itself

Adaptive Query Execution (Spark 3.0+, on by default in 3.2+) handles three problems automatically:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 1. Too many small shuffle partitions → coalesced
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Data skew in joins → split skewed partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Oversized broadcast threshold estimates → switch to broadcast at runtime
# AQE measures actual shuffle output sizes and switches SortMergeJoin → BroadcastHashJoin
```

---

## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[4]").appName("perf-demo").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Generate data
orders = spark.range(100000).withColumn("region", (F.col("id") % 5).cast("string")) \
    .withColumn("amount", F.rand() * 1000)
dim_region = spark.createDataFrame([
    ("0", "US"), ("1", "EU"), ("2", "APAC"), ("3", "LATAM"), ("4", "MEA")
], ["region", "region_name"])

# Without broadcast
no_bcast = orders.join(dim_region, "region").groupBy("region_name").sum("amount")
no_bcast.explain()  # Shows Exchange (shuffle)

# With broadcast
with_bcast = orders.join(broadcast(dim_region), "region").groupBy("region_name").sum("amount")
with_bcast.explain()  # Shows BroadcastHashJoin (no shuffle!)
```

> **Run it:** Works with `local[4]` — no cluster needed.

---

## Interview Tips

> **Tip 1:** "What is the single most impactful Spark performance optimization?" — Broadcast joins: replacing a sort-merge join (shuffles both sides) with a broadcast join (no shuffle) can reduce job time by 10-100× for large-fact/small-dimension joins. The next most impactful is predicate pushdown / partition pruning — reading 1% of the data is better than reading 100% and filtering in memory.

> **Tip 2:** "What is the right number of shuffle partitions?" — The default (200) is rarely correct. Target 100-256 MB per partition. Estimate: `total_shuffle_data_size / 200MB`. For a 40-core cluster with 4 cores/executor, target 2-4× total cores = 80-160 partitions. Enable AQE (`spark.sql.adaptive.coalescePartitions.enabled=true`) and Spark will auto-coalesce after each shuffle — then you can set shuffle.partitions higher than needed and let AQE merge small partitions.

> **Tip 3:** "How do you identify the bottleneck in a slow Spark job?" — Open the Spark UI (port 4040): (1) Jobs tab — which job is slow? (2) Stages tab — which stage, and does it have task time variance (skew)? (3) Executors tab — high GC time? Spill to disk? (4) SQL tab — which operator takes the most time? Which plan nodes have unexpected row counts? Then check `.explain("formatted")` for missing optimizations (missing pushdown, unexpected nested-loop join).
