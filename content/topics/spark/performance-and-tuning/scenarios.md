---
title: "Spark Performance Tuning Interview Scenarios"
description: "Scenarios covering shuffle optimization, skew handling, caching, and configuration tuning"
content_type: scenario_question
topic: spark
subtopic: performance-and-tuning
tags: [spark, performance, tuning, shuffle, skew, caching, partitioning, spill]
---

<article data-difficulty="junior">

## Scenario: Diagnosing Slow Tasks in a Spark Job

Your Spark job has 200 tasks but 190 of them finish in 2 minutes while 10 tasks take 45 minutes. What is this problem called and what are the common causes?

<details>
<summary>✅ Solution</summary>

**This problem is called Data Skew** — a condition where data is unevenly distributed across partitions, causing some tasks to process significantly more data than others.

### Why It Matters

Spark processes one task per partition in parallel. If one partition has 100x more data than the others, that task becomes the bottleneck regardless of how many executors you have. The entire stage cannot complete until the last task finishes.

### Common Causes of Data Skew

**1. Skewed join keys**
A small number of key values appear far more often than others. Example: joining on `country_code` where 80% of rows have `country_code = 'US'`.

**2. NULL values aggregated into one partition**
In a `groupBy` or join, all NULLs hash to the same partition.

**3. Hot keys in aggregations**
`groupBy("user_id").count()` when one user_id has millions of events.

**4. Poor custom partitioning**
A custom partitioner that doesn't distribute keys evenly.

### How to Confirm Skew

1. Open Spark UI → Stages tab → click the slow stage
2. Look at the "Tasks" table — sort by "Duration"
3. If max task duration >> median task duration, you have skew
4. Check "Shuffle Read Size" per task — skewed tasks will show much larger values

### Quick Detection in Code

```python
# Check partition sizes to identify skew
df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show(20)
```

### Key Terminology to Know

| Term | Definition |
|------|-----------|
| Skew | Uneven data distribution across partitions |
| Straggler task | A single slow task that delays the entire stage |
| Hot key | A key value that appears disproportionately often |
| Partition | The unit of parallelism in Spark |

</details>

</article>

<article data-difficulty="mid">

## Scenario: Optimizing a Large-to-Small Table Join

A Spark join between a 2TB fact table and a 500MB dimension table is causing excessive shuffle. How would you optimize it? Show the code for your solution.

<details>
<summary>✅ Solution</summary>

**The solution is a Broadcast Hash Join** — broadcast the smaller table to every executor so no shuffle is needed for the large table.

### Why the Default Join Causes Shuffle

By default, Spark uses a Sort-Merge Join for large tables. Both sides must be sorted and shuffled by the join key, which involves:
- Writing shuffle data to disk
- Network transfer of 2TB+ of data
- Re-sorting on both sides

This is expensive. Shuffling 2TB across the network is the primary bottleneck.

### Solution 1: Broadcast Join (Best Option)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

fact_table = spark.table("orders")          # 2TB
dimension_table = spark.table("products")   # 500MB

# Explicitly broadcast the smaller table
result = fact_table.join(
    broadcast(dimension_table),
    on="product_id",
    how="inner"
)

result.write.parquet("/output/enriched_orders")
```

**What happens internally:**
1. Driver sends the 500MB dimension table to ALL executors
2. Each executor holds the full dimension table in memory
3. The 2TB fact table is processed locally on each executor — no shuffle needed
4. Join is performed as a hash lookup (O(1) per row)

### Solution 2: Auto-Broadcast Configuration

```python
# Raise the broadcast threshold (default is 10MB — often too low)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 600 * 1024 * 1024)  # 600MB

# Spark will now automatically broadcast tables under 600MB
result = fact_table.join(dimension_table, on="product_id")
```

### Solution 3: Check if Broadcast is Actually Happening

```python
# Inspect the physical plan
result.explain(mode="formatted")
```

Look for `BroadcastHashJoin` in the plan. If you see `SortMergeJoin`, the broadcast is not being applied.

### When Broadcast Won't Work

| Situation | Alternative |
|-----------|-------------|
| Dimension table > 2GB | Bucketing (pre-partition both tables on join key) |
| Both tables are large | Salting to handle skew in sort-merge join |
| Streaming join | Watermarking + state store |

### Bucketing as an Alternative (Pre-shuffle)

```python
# Write tables with the same bucketing — eliminates shuffle at query time
fact_table.write \
    .bucketBy(200, "product_id") \
    .sortBy("product_id") \
    .saveAsTable("orders_bucketed")

dimension_table.write \
    .bucketBy(200, "product_id") \
    .sortBy("product_id") \
    .saveAsTable("products_bucketed")

# Now the join has no shuffle
spark.table("orders_bucketed").join(
    spark.table("products_bucketed"),
    on="product_id"
).explain()  # Should show BucketedHashAggregation
```

### Performance Impact

| Approach | Shuffle Data | Join Type | Best For |
|----------|-------------|-----------|----------|
| Default SortMerge | ~2TB | SortMergeJoin | Both tables large |
| Broadcast | 0 (small table only) | BroadcastHashJoin | One table < ~2GB |
| Bucketing | 0 (pre-computed) | BucketedHashJoin | Repeated joins on same key |

</details>

</article>

<article data-difficulty="senior">

## Scenario: Diagnosing a Nightly Job Blowing Its SLA

A nightly Spark job processing 1TB of e-commerce orders has been running for 6 hours (SLA: 2 hours). Using Spark UI observations: stage 3 has a single task running 5.5 hours, GC time is 40% of task time, shuffle read is 800GB. Provide a systematic performance diagnosis and remediation plan.

<details>
<summary>✅ Solution</summary>

### Reading the Signals

| Observation | Diagnosis |
|-------------|-----------|
| Single task running 5.5h in stage 3 | Severe data skew — one partition has a disproportionate amount of data |
| GC time = 40% of task time | Memory pressure — executor heap is undersized or too much data is held in memory |
| Shuffle read = 800GB (input was 1TB) | Explosive fanout — likely a cross-join, window function over large groups, or poor partitioning |

### Step 1: Diagnose the Skew

```python
# Identify the skewed key
df.groupBy("order_status")  # or whatever the groupBy/join key is
  .count()
  .orderBy("count", ascending=False)
  .show(20)
```

If one value dominates (e.g., `status = 'COMPLETED'` has 800M of 1B rows), that's your hot key.

**Check partition size distribution:**
```python
from pyspark.sql.functions import spark_partition_id

df.groupBy(spark_partition_id()).count() \
  .orderBy("count", ascending=False) \
  .show(20)
```

### Step 2: Fix the Skew with Salting

For a `groupBy` aggregation with a hot key:

```python
from pyspark.sql.functions import col, concat, lit, floor, rand, sum as spark_sum

SALT_FACTOR = 50  # Number of salt buckets

# Phase 1: Add salt to distribute hot keys
salted = df.withColumn(
    "salted_key",
    concat(col("order_status"), lit("_"), (rand() * SALT_FACTOR).cast("int").cast("string"))
)

# Phase 2: Partial aggregation across salted partitions
partial_agg = salted.groupBy("salted_key", "order_status") \
    .agg(spark_sum("amount").alias("partial_sum"),
         spark_sum("quantity").alias("partial_qty"))

# Phase 3: Final aggregation — recombine salted buckets
final_agg = partial_agg.groupBy("order_status") \
    .agg(spark_sum("partial_sum").alias("total_amount"),
         spark_sum("partial_qty").alias("total_quantity"))
```

For a **join with a skewed key**, use Spark's built-in skew hint (Spark 3.0+):

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# AQE will automatically split skewed partitions
result = orders.join(customers, "customer_id")
```

### Step 3: Fix the GC Pressure

40% GC time means the executor JVM is spending almost half its time collecting garbage. Root causes and fixes:

**A. Increase executor memory:**
```bash
spark-submit \
  --executor-memory 16g \
  --executor-cores 4 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  ...
```

**B. Switch to G1GC (better for large heaps):**
```bash
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+PrintGCDetails"
```

**C. Reduce object creation — use DataFrames over RDDs:**
DataFrames use Tungsten's off-heap binary format. RDDs create many JVM objects. If any part of the pipeline uses `rdd.map(lambda row: ...)`, convert it.

**D. Avoid caching large DataFrames in MEMORY_ONLY:**
```python
# Bad: stores as JVM objects, causes GC pressure
df.cache()  # defaults to MEMORY_AND_DISK with deserialized format

# Better: use Kryo serialization or MEMORY_AND_DISK_SER
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Step 4: Investigate the 800GB Shuffle

800GB shuffle read on 1TB input is a 0.8x amplification — suspicious. Could be:

1. **Window functions without PARTITION BY bounds** — can cause each row to read all preceding rows
2. **A groupBy on a low-cardinality column** — all data funnels into few partitions
3. **An accidental cross-join** — check the query plan

```python
# Check for cross-joins in the plan
df.explain(mode="extended")  # Look for CartesianProduct in plan
```

**Increase shuffle partitions to spread the load:**
```python
# Default is 200, which means 800GB / 200 = 4GB per partition — too large
spark.conf.set("spark.sql.shuffle.partitions", "2000")
# Target: ~128MB-256MB per partition
# 800GB / 256MB = ~3200 partitions
spark.conf.set("spark.sql.shuffle.partitions", "3200")
```

**Enable Adaptive Query Execution (AQE) to auto-tune:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
```

### Step 5: Remediation Summary and Expected Impact

| Fix | Problem Solved | Expected Gain |
|-----|---------------|---------------|
| Salting hot key | Stage 3 skew | Stage 3: 5.5h → ~15 min |
| AQE skew join | Adaptive skew handling | Automatic for future runs |
| G1GC + larger heap | GC pressure (40%) | 30-40% task time savings |
| Increase shuffle.partitions | 800GB in too few partitions | Reduces spill/OOM risk |
| AQE coalesce partitions | Over/under-partitioning | Better overall balance |

**Expected outcome:** Job runtime drops from 6 hours to under 90 minutes, well within the 2-hour SLA.

### Configuration Block (Production-Ready)

```python
spark = SparkSession.builder \
    .appName("orders-nightly") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m") \
    .config("spark.sql.shuffle.partitions", "3200") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.cores", "4") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:G1HeapRegionSize=32m") \
    .getOrCreate()
```

</details>

</article>
