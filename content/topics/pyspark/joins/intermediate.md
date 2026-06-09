---
title: "PySpark Joins — Intermediate"
topic: pyspark
subtopic: joins
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, broadcast-join, sort-merge-join, skewed-join, salting, performance]
---

# PySpark Joins — Intermediate

Once you know the join types, the next challenge is performance. Most join-related production incidents — OOMs, multi-hour stages, skewed tasks — trace back to a handful of root causes covered here.

---

## How Spark Physically Executes Joins

Spark has two main physical join strategies:

### 1. Broadcast Hash Join (BHJ)
- The smaller table is broadcast (sent in full) to every executor.
- The larger table is scanned locally and matched against the in-memory hash map.
- **No shuffle required** — fastest possible join.
- Triggers automatically when the smaller side is under `spark.sql.autoBroadcastJoinThreshold` (default: 10 MB).

### 2. Sort-Merge Join (SMJ)
- Both sides are **shuffled** by the join key, then **sorted**, then **merged**.
- Required when neither side fits in memory for broadcasting.
- The shuffle is expensive: it moves data across the network and writes shuffle files to disk.
- Default strategy when tables are both large.

```
BHJ:  [large table] ─── scan ──→ [local hash lookup] ──→ output
                              ↑
                   broadcast copy of small table

SMJ:  [table A] ─→ shuffle by key ─→ sort ─→ ┐
                                               ├─→ merge ─→ output
      [table B] ─→ shuffle by key ─→ sort ─→ ┘
```

---

## Broadcast Join — When and How

### Automatic Broadcast

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("joins-intermediate").getOrCreate()

# Check the current threshold
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# 10485760  (10 MB)

# If your dimension table is 8 MB on disk, Spark broadcasts it automatically
# Verify in the query plan:
fact.join(dim, on="product_id", how="inner").explain()
# Look for "BroadcastHashJoin" in the plan
```

### Manual Broadcast Hint

When Spark underestimates a table's size (common with compressed formats like Parquet), force it:

```python
from pyspark.sql.functions import broadcast

# Scenario: product dimension table is 50 MB, above the 10 MB threshold
# but small enough to broadcast comfortably

product_dim = spark.read.parquet("s3://warehouse/dims/products/")
sales_fact = spark.read.parquet("s3://warehouse/facts/sales/")

# Force broadcast on the small side
result = sales_fact.join(
    broadcast(product_dim),
    on="product_id",
    how="inner"
)
result.explain()
# Plan shows: BroadcastHashJoin, BuildRight
```

### Raising the Threshold Globally

```python
# Raise threshold to 100 MB for a session with larger dimension tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)

# Disable auto-broadcast entirely (force SMJ for debugging)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

**Production rule of thumb:**
- < 100 MB → broadcast always
- 100 MB – 2 GB → broadcast if executors have sufficient memory (test with `.explain()`)
- > 2 GB → SMJ (or bucketed join — see senior-deep-dive)

---

## Sort-Merge Join Internals

Understanding SMJ helps you diagnose slow joins.

### The Three Phases

1. **Map phase:** Each partition of both tables is mapped through a hash partitioner on the join key. This produces shuffle files.
2. **Shuffle phase:** Network transfer — all rows with the same key end up on the same executor.
3. **Reduce phase:** Each executor sorts its partition and merges the two sorted streams.

### What Makes SMJ Slow

```python
# Example: joining a 500GB fact table with a 5GB dimension
# The 5GB dimension gets shuffled even though it could be broadcast
# with a higher threshold

# Check the query plan for shuffle indicators:
sales_fact.join(region_dim, on="region_id", how="inner").explain(extended=True)
# Look for: Exchange hashpartitioning(region_id#123, 200)
#                  ^^^^ This is the shuffle — expensive!

# Fix: either raise threshold or use broadcast hint
```

### Tuning Shuffle Partitions

```python
# Default: 200 shuffle partitions (often wrong for your data size)
# Rule: aim for 100-200 MB per partition after shuffle

# For a 100 GB join output:
# 100 GB / 150 MB per partition ≈ 667 partitions
spark.conf.set("spark.sql.shuffle.partitions", 700)

# Adaptive Query Execution (Spark 3+) handles this automatically:
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE coalesces small partitions and splits large ones post-shuffle
```

---

## Data Skew — Detection and the Salting Technique

Skew is the #1 cause of join performance problems in production. It occurs when join key values are unevenly distributed — one key (e.g., `user_id = NULL` or a mega-retailer's `account_id`) makes up 40% of your data.

### Detecting Skew

```python
from pyspark.sql.functions import col, count, desc

# Check distribution of your join key
sales_fact.groupBy("user_id") \
    .agg(count("*").alias("row_count")) \
    .orderBy(desc("row_count")) \
    .limit(20) \
    .show()

# If top values have 10x-100x more rows than median → you have skew
# Also check Spark UI: Stage → Tasks → Max Task Duration vs Median
# If max >> median, you have stragglers caused by skew
```

### Salting: The Standard Fix

Salting artificially increases the cardinality of the skewed key by appending a random suffix, distributing the heavy keys across multiple partitions.

```python
from pyspark.sql.functions import col, lit, concat, rand, floor, explode, array

SALT_FACTOR = 10  # Distribute into 10 sub-partitions

# Step 1: Salt the LARGE table (fact side)
# Append a random salt 0-9 to the join key
sales_salted = sales_fact.withColumn(
    "salted_key",
    concat(col("user_id").cast("string"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Step 2: EXPLODE the SMALL table (dim side)
# Each dim row must be replicated SALT_FACTOR times with each salt suffix
from pyspark.sql.functions import array, explode, lit

# Create array of salts [0, 1, 2, ..., 9]
salt_array = array([lit(i) for i in range(SALT_FACTOR)])

users_salted = users_dim.withColumn("salt", explode(salt_array)) \
    .withColumn(
        "salted_key",
        concat(col("user_id").cast("string"), lit("_"), col("salt").cast("string"))
    ).drop("salt")

# Step 3: Join on salted_key instead of user_id
result = sales_salted.join(users_salted, on="salted_key", how="inner") \
    .drop("salted_key")

result.show()
```

**Why this works:** instead of all rows with `user_id=999999` going to one partition, they're spread across 10 partitions (one per salt value). The dimension row is replicated 10 times to match each salt.

**Trade-offs:**
- Increases dimension table size by `SALT_FACTOR`
- Only practical when dim table is small enough to replicate
- If both tables are large and skewed → combine salting with AQE skew join

---

## AQE Skew Join Optimization (Spark 3+)

Adaptive Query Execution handles skew automatically — no manual salting needed in many cases.

```python
# Enable AQE (should be enabled in production by default)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Tuning parameters:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# A partition is skewed if it's > 5x the median partition size

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(256 * 1024 * 1024))
# And it's > 256 MB

# AQE will automatically split skewed partitions and replicate the other side
# Check the plan for "AQEShuffleRead" with "skewedPartitionSpec" annotations
```

**When AQE isn't enough:** when the skew is extreme (one key = 80% of data), AQE may still struggle. In those cases combine AQE + salting or filter out the extreme key and handle it separately.

---

## Diagnosing Join Performance: A Checklist

```python
# 1. Check the physical plan — what join strategy is being used?
df.explain(mode="formatted")
# Look for: BroadcastHashJoin vs SortMergeJoin vs BroadcastNestedLoopJoin

# 2. Check shuffle partition count
spark.conf.get("spark.sql.shuffle.partitions")
# Check if it matches your data volume

# 3. Check for skew in Spark UI
# Stages → click the join stage → Task Metrics → Duration distribution

# 4. Verify statistics are up to date (affects CBO and auto-broadcast)
spark.sql("ANALYZE TABLE my_catalog.dim_products COMPUTE STATISTICS FOR ALL COLUMNS")
# Check stats:
spark.sql("DESCRIBE EXTENDED my_catalog.dim_products").show(100, truncate=False)

# 5. Force AQE and observe
spark.conf.set("spark.sql.adaptive.enabled", "true")
df.explain(mode="formatted")  # Re-check plan after AQE
```

---

## Real Example: Diagnosing a Slow Join Pipeline

```python
# Before optimization: SMJ on a 50GB fact with a 500MB dimension
# Runtime: 45 minutes

# Diagnosis:
# 1. .explain() showed SortMergeJoin (not broadcast)
# 2. dim table is 500 MB — above 10 MB threshold
# 3. No skew detected in the join key

# Fix 1: Raise broadcast threshold to 600 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 600 * 1024 * 1024)
# Runtime after fix: 8 minutes ✓

# Fix 2: Alternatively, use broadcast hint (more explicit, preferred in code)
result = fact_df.join(broadcast(dim_df), on="product_id", how="inner")
# Runtime: 7 minutes ✓

# Additional optimization: coalesce output to avoid tiny files
result.coalesce(50).write.parquet("s3://output/enriched_sales/")
```

---

## Key Takeaways

1. **BHJ = no shuffle = fast.** Always check if the smaller side can be broadcast.
2. **SMJ = shuffle both sides.** Expensive but necessary for large-large joins.
3. **Skew = stragglers.** Detect with key distribution analysis + Spark UI task duration.
4. **Salting distributes skew** by artificially expanding key cardinality.
5. **AQE handles skew automatically** in Spark 3+ — enable it in production.
6. **`shuffle.partitions = 200` is rarely correct** — tune based on data size.
