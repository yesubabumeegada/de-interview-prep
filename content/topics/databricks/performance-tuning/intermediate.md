---
title: "Performance Tuning - Intermediate"
topic: databricks
subtopic: performance-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, spark, skew, spill, shuffle, liquid-clustering, photon]
---

# Performance Tuning — Intermediate Concepts

## Diagnosing Data Skew

Skew is when one or a few partitions have far more data than others — causing one slow task that blocks the entire stage:

```python
# Detect skew in Spark UI
# Stage detail → Task table → sort by Duration → if max is 10x median, you have skew

# Detect in code
from pyspark.sql import functions as F

# Check key distribution before a join
df.groupBy("customer_id").count() \
  .orderBy(F.desc("count")) \
  .show(20)
# If top 5 customers have 100K rows and average is 50 → severe skew

# Also check null distribution
df.select(
    F.count("*").alias("total"),
    F.count(F.col("customer_id")).alias("non_null"),
    F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("null_count")
).show()
# NULL keys always go to same partition → worst skew case
```

---

## Fixing Data Skew

**Technique 1: Salting (for group-by or join skew)**

```python
import random

# Problem: one customer_id has 5M rows, everyone else < 1K
# Solution: split the large key across N partitions using a salt

N = 10  # number of salt values

# Step 1: Add salt to the large table
large_df = orders.withColumn(
    "salt",
    F.when(F.col("customer_id") == "MEGA_CORP", (F.rand() * N).cast("int"))
     .otherwise(F.lit(0))
)

# Step 2: Explode small table to match all salt values for the skewed key
small_df_expanded = customers \
    .withColumn("salt_range", F.array([F.lit(i) for i in range(N)])) \
    .withColumn("salt", F.explode("salt_range")) \
    .filter(
        (F.col("customer_id") == "MEGA_CORP") |
        (F.col("salt") == 0)
    )

# Step 3: Join on both customer_id and salt
result = large_df.join(
    small_df_expanded,
    on=["customer_id", "salt"],
    how="left"
).drop("salt", "salt_range")
```

**Technique 2: AQE skew join handling (automatic, no code)**

```python
# AQE automatically splits skewed partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")   # 5x median = skewed
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

**Technique 3: Filter then process**

```python
# Handle the skewed key separately
normal_orders = orders.filter("customer_id != 'MEGA_CORP'")
mega_corp_orders = orders.filter("customer_id == 'MEGA_CORP'")

result_normal = normal_orders.join(customers, "customer_id")
result_mega = mega_corp_orders.join(customers.filter("customer_id == 'MEGA_CORP'"), "customer_id")

result = result_normal.union(result_mega)
```

---

## Controlling Shuffle Partitions

```python
# Default: 200 shuffle partitions (often wrong for large/small data)
print(spark.conf.get("spark.sql.shuffle.partitions"))  # "200"

# For small data (< 1GB): reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "20")

# For large data (> 100GB): increase
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# Best practice: let AQE tune it automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
# AQE will coalesce 2000 tiny partitions into fewer larger ones at runtime
```

---

## Detecting and Fixing Spill

Spill occurs when data doesn't fit in executor memory during shuffle:

```python
# Detect in Spark UI: Stage detail → Summary Metrics
# "Shuffle spill (memory)" and "Shuffle spill (disk)" columns
# If these are non-zero: you're spilling

# Fix 1: Reduce partition size (more, smaller partitions)
spark.conf.set("spark.sql.shuffle.partitions", "1000")  # was 200

# Fix 2: Increase memory per executor
# In cluster config:
# spark.executor.memory = 16g  (was 8g)

# Fix 3: Reduce working set (push filters earlier)
# Bad: read all, then filter
result = spark.table("big_table").join(other, "id").filter("status = 'active'")

# Good: filter first, then join (pushdown predicates)
result = spark.table("big_table").filter("status = 'active'").join(other, "id")
```

---

## Liquid Clustering (Modern Z-ordering)

Liquid clustering replaces Z-ordering and static Hive partitioning — better for evolving data:

```sql
-- Create a table with liquid clustering
CREATE TABLE prod.sales.orders
    CLUSTER BY (customer_id, order_date)
AS SELECT * FROM source_orders;

-- Or add to existing table
ALTER TABLE prod.sales.orders
    CLUSTER BY (customer_id, order_date);

-- Trigger clustering (or set up automatic clustering)
OPTIMIZE prod.sales.orders;

-- Liquid clustering advantages over ZORDER:
-- 1. Incremental — only re-clusters new/changed data
-- 2. No full table rewrite on clustering key change
-- 3. Works well with streaming writes
-- 4. Better with high cardinality columns
```

**Liquid vs Z-order vs Hive partitioning:**

| | Hive Partition | Z-Order | Liquid Clustering |
|--|--|--|--|
| **Column cardinality** | Low (< 100 values) | Medium-High | High |
| **Query filter types** | = only | Range, = | Range, = |
| **Write amplification** | None | High (OPTIMIZE) | Low (incremental) |
| **Changing clustering key** | Drop and recreate | DROP ZORDER, re-OPTIMIZE | `ALTER TABLE CLUSTER BY` |

---

## Photon Engine

Databricks-native vectorized execution engine — faster than standard Spark for SQL workloads:

```python
# Enable Photon (cluster configuration)
# In cluster settings: Enable Photon Acceleration

# Photon accelerates:
# - SQL queries (SELECT, aggregations, joins)
# - Delta read/write
# - Window functions
# - Sort operations

# Check if Photon is being used
df.explain("formatted")
# Look for: PhotonResultStage, PhotonProject, PhotonAggregate in the plan

# NOT accelerated by Photon:
# - Python UDFs (drop to JVM layer)
# - Custom RDD operations
# - Pandas UDFs (partially)
```

**Typical speedups with Photon:**
- Simple aggregations: 2-5x
- Complex joins: 2-3x
- String operations: 3-8x
- Overall ETL pipelines: 2-4x

---

## Interview Tips

> **Tip 1:** "How do you detect data skew and what do you do about it?" — "Detect: in the Spark UI stage detail, sort tasks by duration — if max is 10x+ the median, that's skew. Also check key distribution with `groupBy().count()`. Fix: (1) Enable AQE skew join handling (automatic, no code). (2) Salt: add a random prefix to the hot key and replicate the small side of the join. (3) Separate hot keys: process them individually with a union."

> **Tip 2:** "What causes spill to disk in Spark?" — "Spill happens when a shuffle operation (sort, aggregation, join) doesn't fit in executor memory. The overflow goes to disk — 5-10x slower than memory. Fix: increase `spark.executor.memory`, reduce `spark.sql.shuffle.partitions` (fewer but larger), or push filters earlier to reduce data volume before the shuffle."

> **Tip 3:** "What is Liquid Clustering and how is it different from Z-ordering?" — "Both co-locate similar data for better file skipping. Z-ordering requires a full table OPTIMIZE every time — expensive on large tables. Liquid clustering is incremental: only newly written data is clustered, and you can change the clustering keys without rewriting the whole table. Liquid clustering also handles high-cardinality keys better and works well with continuous streaming writes."
