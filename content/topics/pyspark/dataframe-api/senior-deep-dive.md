---
title: "PySpark DataFrame API - Senior Deep Dive"
topic: pyspark
subtopic: dataframe-api
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, dataframe, optimization, execution-plan, catalyst, shuffle, partitioning]
---

# PySpark DataFrame API — Senior-Level Deep Dive

## Understanding the Execution Plan

Every DataFrame operation builds a logical plan. The Catalyst optimizer transforms it into a physical plan. Learn to read these plans.

```python
# View the execution plan
df.explain()          # Physical plan only
df.explain(True)      # All plans: parsed → analyzed → optimized → physical
df.explain("cost")    # With cost estimates (Spark 3.0+)
df.explain("formatted")  # Human-readable format

# Example output
# == Physical Plan ==
# *(2) HashAggregate(keys=[department], functions=[avg(salary)])
# +- Exchange hashpartitioning(department, 200)    ← SHUFFLE
#    +- *(1) HashAggregate(keys=[department], functions=[partial_avg(salary)])
#       +- *(1) FileScan parquet [department,salary]
#          PushedFilters: [IsNotNull(salary)]      ← PREDICATE PUSHDOWN
#          ReadSchema: struct<department:string,salary:double>  ← COLUMN PRUNING
```

### What to Look For in Plans

| Pattern | Meaning | Concern? |
|---------|---------|----------|
| `Exchange` | Shuffle (data redistribution) | ⚠️ Expensive |
| `BroadcastExchange` | Small table broadcast | ✅ Good |
| `Sort` | Sorting (for merge join or order) | ⚠️ If unexpected |
| `Filter` near `FileScan` | Predicate pushdown | ✅ Good |
| `Project` with few columns | Column pruning | ✅ Good |
| `SortMergeJoin` | Both tables large | Check if broadcast possible |
| `BroadcastHashJoin` | Small table broadcast | ✅ Optimal for dim joins |

## Controlling Shuffles

Shuffles are the #1 performance killer in Spark. Each shuffle:
1. Serializes data
2. Writes to disk
3. Transfers across network
4. Deserializes on receiving node

```python
# Check current shuffle partition count
spark.conf.get("spark.sql.shuffle.partitions")  # Default: 200

# Set appropriate shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # AQE (Spark 3.0+)
# Or manually based on data size:
# Rule of thumb: target 128MB per partition
# 100GB data → 100*1024/128 ≈ 800 partitions

# Operations that cause shuffles:
# - groupBy/agg
# - join (unless broadcast or co-partitioned)
# - distinct/dropDuplicates
# - orderBy/sort (global sort)
# - repartition()

# Operations that DON'T shuffle:
# - filter, select, withColumn
# - coalesce (only reduces partitions)
# - map-side aggregation (partial agg before shuffle)
```

### Eliminating Unnecessary Shuffles

```python
# ANTI-PATTERN: Two shuffles from two groupBy on same key
result1 = df.groupBy("customer_id").agg(sum("amount"))
result2 = df.groupBy("customer_id").agg(count("*"))
final = result1.join(result2, "customer_id")  # Third shuffle for join!

# OPTIMIZED: Single shuffle
final = df.groupBy("customer_id").agg(
    sum("amount").alias("total_amount"),
    count("*").alias("order_count")
)

# Pre-partition for multiple operations on same key
df_partitioned = df.repartition(200, "customer_id")
df_partitioned.cache()  # Cache the repartitioned version

# Now these use the same partitioning (no additional shuffle)
agg1 = df_partitioned.groupBy("customer_id").agg(...)
agg2 = df_partitioned.groupBy("customer_id").agg(...)
```

## Broadcast Join Optimization

```python
from pyspark.sql.functions import broadcast

# Force broadcast (override auto-broadcast threshold)
result = large_df.join(broadcast(small_df), "join_key")

# Configure auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB

# Disable broadcast entirely (for testing)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Check actual join type in plan
result.explain()
# Look for BroadcastHashJoin vs SortMergeJoin
```

**Broadcast join rules:**
- Table must fit in driver memory AND each executor memory
- Broadcast side is collected to driver then sent to all executors
- Maximum practical size: ~1-2GB (depends on cluster memory)
- Don't broadcast if the "small" table is actually large → OOM

## Adaptive Query Execution (AQE)

AQE (Spark 3.0+) re-optimizes the plan at runtime based on actual data statistics.

```python
# Enable AQE (often enabled by default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE features:
# 1. Auto-coalesce shuffle partitions (reduces small files)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# 2. Convert sort-merge join to broadcast at runtime
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "30MB")

# 3. Handle skewed joins automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

## Data Skew Mitigation

```python
# Detect skew: check partition sizes
df.groupBy(spark_partition_id()).count().orderBy(desc("count")).show()

# Method 1: Salting the skewed key
from pyspark.sql.functions import rand, floor, concat, lit

SALT_BUCKETS = 10

# Salt the large (skewed) table
large_salted = large_df.withColumn(
    "salt", floor(rand() * SALT_BUCKETS).cast("string")
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Replicate the small table with all salt values
from pyspark.sql.functions import explode, array

small_replicated = small_df.crossJoin(
    spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
).withColumn(
    "salt", col("salt").cast("string")
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Join on salted key — distributes hot key across 10 partitions
result = large_salted.join(small_replicated, "salted_key")

# Method 2: Isolate and broadcast the hot key
hot_keys = ["NULL", "UNKNOWN", "DEFAULT"]

# Split into hot and cold paths
large_hot = large_df.filter(col("join_key").isin(hot_keys))
large_cold = large_df.filter(~col("join_key").isin(hot_keys))
small_hot = small_df.filter(col("join_key").isin(hot_keys))

# Hot path: broadcast (small after filtering)
result_hot = large_hot.join(broadcast(small_hot), "join_key")
# Cold path: normal sort-merge
result_cold = large_cold.join(small_df, "join_key")
# Combine
result = result_hot.union(result_cold)
```

## Custom Partitioning Strategies

```python
# Repartition by column (hash partitioning)
df = df.repartition(100, "customer_id")

# Repartition by multiple columns
df = df.repartition(100, "customer_id", "event_date")

# Range partitioning (for ordered writes)
df = df.repartitionByRange(100, col("event_date"))

# Coalesce (reduce partitions WITHOUT shuffle — only merges)
df = df.coalesce(10)  # Merge 200 partitions → 10 (no shuffle, unbalanced)

# Get current partition count
df.rdd.getNumPartitions()

# Inspect partition sizes
from pyspark.sql.functions import spark_partition_id
df.groupBy(spark_partition_id().alias("partition_id")) \
    .count() \
    .orderBy("count") \
    .show()
```

## Predicate Pushdown and Column Pruning

```python
# Spark automatically pushes predicates to the data source
# This means less data is read from disk/S3

# GOOD: Filter pushed to Parquet scan
df = spark.read.parquet("s3://data/events/")
result = df.filter(col("event_date") == "2024-01-15") \
           .select("user_id", "action")
# Plan shows: PushedFilters: [EqualTo(event_date, 2024-01-15)]
# ReadSchema: struct<user_id:string, action:string>  (only 2 columns read)

# BAD: UDF prevents pushdown
from pyspark.sql.functions import udf
my_filter = udf(lambda x: x == "2024-01-15")
result = df.filter(my_filter(col("event_date")))
# Plan shows: NO pushed filters — full table scan!

# TIP: Always filter with built-in functions, not UDFs
```

## Handling Late Data and Watermarks (Batch Context)

```python
# In batch processing: detect and handle late-arriving records
# Records that arrive after their partition has been processed

# Strategy: Two-pass write
# Pass 1: Write new data to the correct partition
new_data = spark.read.parquet("s3://staging/today/")

# Pass 2: Identify records that belong to already-closed partitions
late_records = new_data.filter(col("event_date") < "2024-01-01")
current_records = new_data.filter(col("event_date") >= "2024-01-01")

# Write current records normally (append to active partitions)
current_records.write.mode("append") \
    .partitionBy("event_date") \
    .parquet("s3://warehouse/events/")

# Write late records to a separate location for reconciliation
late_records.write.mode("append") \
    .parquet("s3://warehouse/late_arrivals/")
```

## Interview Tip 💡

> Senior-level PySpark questions almost always involve performance. Structure your answer around: (1) "How many shuffles does this require?" (2) "Can we reduce shuffles by repartitioning, broadcasting, or combining aggregations?" (3) "What's the data skew risk?" If you can read `explain()` output and identify optimization opportunities, you'll stand out. Practice reading physical plans until the patterns are automatic.
