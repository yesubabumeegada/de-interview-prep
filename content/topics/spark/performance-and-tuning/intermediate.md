---
title: "Spark Performance & Tuning — Intermediate"
topic: spark
subtopic: performance-and-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, performance, skew, salting, spill, kryo, vectorized-reader, file-formats]
---

# Spark Performance & Tuning — Intermediate

## Data Skew: Detection and Fixes

Data skew occurs when some partitions have far more data than others — one task runs for hours while others finish in seconds.

```python
# Detect skew: check partition sizes
from pyspark.sql import functions as F

# Method 1: count per partition
df.groupBy(F.spark_partition_id()).count() \
    .orderBy(F.desc("count")).show(10)

# Method 2: check the join key distribution
df.groupBy("key_column").count() \
    .orderBy(F.desc("count")).show(20)
# If top 10 keys have 90% of rows → skew!

# Spark UI: Stage detail → Task duration timeline
# One bar 10× longer than others = skew
```

**Fix 1: AQE Skew Join (automatic, Spark 3.0+)**

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # 5× median
# AQE splits skewed partitions and duplicates the matching non-skewed data
```

**Fix 2: Salting (manual, for extreme skew or Spark < 3.0)**

```python
import random

# Add a random salt to distribute hot keys
n_salts = 10

# Explode the small side (create n_salts copies of each row)
from pyspark.sql.functions import explode, array, lit
small_df_salted = small_df.withColumn("_salt",
    explode(array([lit(i) for i in range(n_salts)])))

# Random salt on large side
large_df_salted = large_df.withColumn("_salt",
    (F.rand() * n_salts).cast("int"))

# Join on original key + salt
result = large_df_salted.join(
    small_df_salted,
    (large_df_salted.key == small_df_salted.key) &
    (large_df_salted._salt == small_df_salted._salt)
).drop("_salt")
```

**Fix 3: Null skew (common: many NULL keys)**

```python
# NULLs all go to one partition in a join
# Fix: filter NULLs, process separately
non_null = df.filter(F.col("key").isNotNull())
null_rows = df.filter(F.col("key").isNull())

result_non_null = non_null.join(other, "key")
result = result_non_null.unionAll(null_rows.withColumn(...))
```

---

## Spill to Disk

Spill happens when execution memory is exhausted during shuffle or aggregation. It dramatically slows jobs (disk I/O).

```python
# Detect spill in Spark UI: Stages tab
# "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)" columns
# Any non-zero spill = problem

# Causes:
# 1. Shuffle partitions too few → large partitions don't fit in memory
# 2. Executor memory too small
# 3. Aggregation with high-cardinality groupBy and many distinct values

# Fix 1: Increase partitions (smaller per-partition data)
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Fix 2: Increase executor memory
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.memory.fraction", "0.75")  # give more to execution

# Fix 3: Off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "8g")

# Fix 4: Replace groupByKey with reduceByKey/aggregateByKey (pre-agg on map side)
# groupByKey: ALL values shuffled → then aggregated (often causes spill)
# reduceByKey: partial aggregation on each partition first → then shuffle
```

---

## File Format Impact on Performance

```python
# Format comparison for Spark:

# Parquet (columnar, compressed) — best for analytics
df.write.parquet("output/")              # 10× smaller than CSV
df.read.parquet("input/")               # reads only needed columns (column pruning)

# ORC (columnar, compressed) — similar to Parquet, better for Hive
df.write.orc("output/")

# CSV (row, uncompressed) — worst for analytics, good for exchange
df.write.option("header", "true").csv("output/")

# JSON — flexible schema, verbose
df.write.json("output/")

# Delta Lake (Parquet + transaction log) — best for ACID + streaming
df.write.format("delta").save("output/")

# Performance numbers (approximate, 1 GB data):
# CSV read: 120 seconds
# Parquet read (all columns): 15 seconds
# Parquet read (3 of 50 columns): 2 seconds  ← column pruning!
# Delta read (with data skipping): 0.5 seconds
```

---

## Kryo Serialization

Java's default serialization is slow and verbose. Kryo is faster and more compact:

```python
spark = SparkSession.builder \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrationRequired", "false") \   # don't require registration
    .getOrCreate()

# For maximum speed: register classes explicitly
spark.conf.set("spark.kryo.classesToRegister",
    "com.example.Order,com.example.Customer")

# Kryo impact:
# Shuffle data size: ~40% smaller than Java serialization
# Shuffle time: ~30% faster
# Task launch time: faster (smaller closure serialization)
```

---

## Vectorized Parquet/ORC Reader

```python
# Vectorized reading: process columnar data in batches, not row-by-row
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")   # default true
spark.conf.set("spark.sql.orc.enableVectorizedReader", "true")       # default true

# Batch size: how many rows per batch
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")  # default 4096

# Disable for complex nested types that don't support vectorized reading:
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
# (arrays, maps, structs may not benefit or may be unsupported)
```

---

## Sort-Merge Join vs. Broadcast Join vs. Shuffle-Hash Join

```python
# 1. BroadcastHashJoin (fastest)
# When: one side < autoBroadcastJoinThreshold (default 10MB)
# No shuffle; small side broadcast to all executors
orders.join(broadcast(dim), "key")

# 2. SortMergeJoin (default for large joins)
# When: both sides large (or broadcast threshold exceeded)
# Shuffles and sorts both sides by join key; merge sorted streams
# Requires sort — expensive but memory-efficient (streams both sides)

# 3. ShuffleHashJoin
# When: one side is moderate size
# Shuffles both sides, builds hash table from smaller side
# Memory-intensive: hash table must fit in executor memory
orders.hint("shuffle_hash").join(customers, "key")

# Choose:
# small × large → BroadcastHashJoin (always)
# large × large, equi-join → SortMergeJoin (default, safe)
# medium × large, medium fits in memory → ShuffleHashJoin (faster than SMJ if fits)
# inequality join → BroadcastNestedLoopJoin (avoid for large inputs!)
```

---

## Interview Tips

> **Tip 1:** "How do you fix data skew in Spark?" — In Spark 3.0+, enable AQE skew join handling — it detects and splits skewed partitions automatically. For pre-3.0 or extreme skew: salting. Salting adds a random integer (0..N) to the join key on the large side, then explodes the small side N times with each salt value. This distributes the hot key across N partitions. The cost: N× more data on the small side — acceptable for a small dimension table.

> **Tip 2:** "What causes spill to disk and how do you fix it?" — Spill happens when execution memory (shuffle buffers, sort, hash aggregation) exceeds available executor heap. Fixes in order: (1) increase partition count so less data per partition; (2) increase executor memory; (3) enable off-heap memory; (4) use pre-aggregation (reduceByKey over groupByKey). Check Spark UI Stages tab — "Shuffle Spill (Disk)" column shows the problem directly.

> **Tip 3:** "When would Spark choose SortMergeJoin over BroadcastHashJoin?" — When both sides exceed `autoBroadcastJoinThreshold` (default 10MB). SortMergeJoin sorts both sides by join key (triggering shuffles on both sides), then merges the sorted streams. It's memory-efficient (streaming merge) but expensive (two shuffles + two sorts). For tables that are "medium" sized (hundreds of MB), raising the broadcast threshold or using shuffle-hash join hint can be faster.
