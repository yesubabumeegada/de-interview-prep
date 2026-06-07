---
title: "PySpark Partitioning and Bucketing - Intermediate"
topic: pyspark
subtopic: partitioning-and-bucketing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, partitioning, bucketing, hash-partitioning, range-partitioning, bucketBy, partition-pruning]
---

# PySpark Partitioning and Bucketing — Intermediate

## Hash vs Range Partitioning

### Hash Partitioning

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("PartitionTypes").getOrCreate()

# Hash partitioning: partition = hash(key) % num_partitions
# Distributes data evenly (assuming uniform key distribution)
df_hash = df.repartition(100, "user_id")

# All rows with same user_id go to same partition
# Good for: joins (co-locate join keys), groupBy operations
# Risk: skew if some keys are very popular
```

### Range Partitioning

```python
# Range partitioning: partition based on value ranges
# Sorts data globally and divides into equal-sized ranges
df_range = df.repartitionByRange(100, "timestamp")

# Partition 0: timestamps 00:00 - 02:24
# Partition 1: timestamps 02:24 - 04:48
# ...

# Good for: range queries, time-series data, ordered output
# Requires sampling to determine range boundaries

# Combine range partitioning with sorting
df_sorted = df.repartitionByRange(100, F.col("event_date"), F.col("user_id"))
```

### Comparison

| Aspect | Hash Partitioning | Range Partitioning |
|--------|------------------|-------------------|
| Distribution | Even (uniform keys) | Even (by value range) |
| Co-location | Same key → same partition | Adjacent values → same partition |
| Best for | Joins, groupBy | Range queries, time-series |
| Skew risk | Popular keys cause skew | Temporal hot spots |
| Ordering | No guaranteed order | Sorted within/across partitions |

---

## Custom Partitioners (RDD Level)

```python
from pyspark import Partitioner

class TimeBasedPartitioner(Partitioner):
    """Partition by hour-of-day to co-locate temporal data."""
    
    def __init__(self):
        self._num_partitions = 24  # One partition per hour
    
    def numPartitions(self):
        return self._num_partitions
    
    def partitionFunc(self, key):
        # key is a timestamp string like "2024-01-15T14:30:00"
        hour = int(key[11:13])
        return hour

# Apply to pair RDD
events_rdd = df.rdd.map(lambda row: (row.timestamp_str, row))
partitioned = events_rdd.partitionBy(24, lambda key: int(key[11:13]))
```

---

## Bucketing (bucketBy + sortBy)

Bucketing pre-hashes data into a fixed number of files, enabling **shuffle-free joins**:

```python
# Write bucketed table
(orders_df
    .write
    .bucketBy(256, "customer_id")   # 256 buckets, hashed on customer_id
    .sortBy("customer_id")          # Sort within each bucket
    .mode("overwrite")
    .saveAsTable("orders_bucketed"))  # Must save as table (Hive metastore)

# Write matching bucketed table
(customers_df
    .write
    .bucketBy(256, "customer_id")
    .sortBy("customer_id")
    .mode("overwrite")
    .saveAsTable("customers_bucketed"))

# Join WITHOUT shuffle — both tables already co-partitioned!
spark.conf.set("spark.sql.autoBucketedScan.enabled", "true")
orders_b = spark.table("orders_bucketed")
customers_b = spark.table("customers_bucketed")

result = orders_b.join(customers_b, "customer_id")
result.explain()
# Shows SortMergeJoin WITHOUT Exchange nodes — no shuffle!
```

### Bucketing Requirements

| Requirement | Details |
|-------------|---------|
| Same bucket count | Both tables must have identical bucket count |
| Same bucket column | Join key must be the bucket column |
| Saved as table | Must use `saveAsTable` (needs Hive metastore) |
| Sort column matches | `sortBy` column should match bucket column |

---

## Partition Pruning on Read

```python
# Static partition pruning — filter on partition column
df = spark.read.parquet("s3://data/events/")

# GOOD: Direct filter on partition column → prunes partitions
df.filter(F.col("event_date") == "2024-01-15").explain()
# PartitionFilters: [event_date = 2024-01-15]

# BAD: Function on partition column → NO pruning
df.filter(F.year(F.col("event_date")) == 2024).explain()
# No PartitionFilters — reads ALL data!

# Fix: Use direct comparison
df.filter(
    (F.col("event_date") >= "2024-01-01") &
    (F.col("event_date") < "2025-01-01")
).explain()
# PartitionFilters: [event_date >= 2024-01-01, event_date < 2025-01-01]

# Dynamic Partition Pruning (Spark 3.0+)
# Spark pushes dimension filter to fact table partitions at runtime
spark.sql("""
    SELECT f.*
    FROM fact_table f
    JOIN dim_table d ON f.region = d.region
    WHERE d.country = 'US'
""").explain()
# DynamicPruningExpression — prunes fact partitions based on dim filter
```

---

## Combining Spark Partitions with Storage Partitions

```python
# Best practice: align Spark partitions with storage partitions before write

# Step 1: Repartition by storage partition column
# Step 2: Control number of files per partition

# Approach 1: repartition + coalesce per partition
(df
    .repartition("event_date")          # Group data by partition column
    .sortWithinPartitions("timestamp")  # Optional: sort within each file
    .write
    .partitionBy("event_date")
    .parquet("s3://output/events/"))

# Approach 2: Explicit file count control
target_files_per_partition = 5

# Calculate optimal repartition count
num_dates = df.select("event_date").distinct().count()
total_partitions = num_dates * target_files_per_partition

(df
    .repartition(total_partitions, "event_date")
    .write
    .partitionBy("event_date")
    .parquet("s3://output/events/"))
# Each date directory gets approximately 5 files
```

---

## Inspecting Partition Distribution

```python
# Check data distribution across Spark partitions
partition_counts = (df.rdd
    .mapPartitions(lambda it: [sum(1 for _ in it)])
    .collect())

print(f"Partitions: {len(partition_counts)}")
print(f"Min rows: {min(partition_counts)}")
print(f"Max rows: {max(partition_counts)}")
print(f"Avg rows: {sum(partition_counts) / len(partition_counts):.0f}")
print(f"Skew ratio: {max(partition_counts) / (sum(partition_counts) / len(partition_counts)):.2f}")

# Check storage partition sizes
import subprocess
# hdfs dfs -ls s3://data/events/event_date=2024-01-15/
# Shows file sizes per storage partition

# Programmatic check via DataFrame
partition_sizes = (spark.read.parquet("s3://data/events/")
    .groupBy("event_date")
    .count()
    .orderBy("count"))

partition_sizes.show()
# Check for skew: some dates might have 100x more data than others
```

---

## Bucketing vs Partitioning — When to Use Which

| Use Case | Partitioning | Bucketing |
|----------|-------------|-----------|
| Time-series filtering | Yes (by date) | Not needed |
| Frequent equi-joins | Not helpful | Yes (by join key) |
| High-cardinality join column | Bad (too many dirs) | Good (fixed bucket count) |
| Query filtering by column | Yes (partition pruning) | No pruning benefit |
| Repeated joins on same key | Partial benefit | Full benefit (zero shuffle) |

```python
# Combined strategy: partition BY date, bucket BY join key
(df.write
    .partitionBy("event_date")        # For time-based filtering
    .bucketBy(100, "customer_id")     # For join optimization
    .sortBy("customer_id")
    .saveAsTable("events_optimized"))
```

---

## Interview Tips

> **Tip 1:** "Explain hash vs range partitioning." — "Hash partitioning applies a hash function to the key and mods by partition count — it gives even distribution but no ordering. Range partitioning divides data into equal-sized ranges based on value — it preserves order and is good for range queries. Hash for joins and groupBy; range for time-series and sorted output."

> **Tip 2:** "What is bucketing and when would you use it?" — "Bucketing pre-hashes data into a fixed number of files using a hash on a specified column. When two tables are bucketed on the same column with the same bucket count, Spark can join them without any shuffle — the data is already co-partitioned. Use it for dimension tables that are joined frequently. The tradeoff: upfront write cost and Hive metastore dependency."

> **Tip 3:** "How do you ensure partition pruning happens?" — "Three rules: filter directly on the partition column (not a function of it), use simple comparison operators (=, >, <, IN), and check with explain() that PartitionFilters appear in the scan. Common mistake: `YEAR(date_col) = 2024` prevents pruning but `date_col >= '2024-01-01'` enables it. For dynamic pruning (Spark 3.0+), ensure the dimension filter can be pushed to the fact table scan."
