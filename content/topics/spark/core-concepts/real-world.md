---
title: "Spark Core Concepts — Real World"
topic: spark
subtopic: core-concepts
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, production, skew, caching-strategy, accumulator-pitfalls, real-world]
---

# Spark Core Concepts — Real World

## War Story: The Accumulator That Lied

**Scenario:** A data quality pipeline used an accumulator to count invalid records. The count was consistently about 30% higher than the actual invalid record count in downstream tables.

**Root cause:**
```python
# Original code:
invalid_count = sc.accumulator(0)

def validate(record):
    if not is_valid(record):
        invalid_count.add(1)   # BUG: transformations can re-execute!
        return None
    return record

clean_rdd = raw_rdd.map(validate)   # transformation — lazy
clean_rdd.cache()
clean_rdd.count()    # action 1: triggers execution → accumulators count correctly

# Later in the script:
clean_rdd.write.parquet("output/")   # action 2: hits cache, no re-execution → OK
```

The bug was subtle: between action 1 and the cache fill, a speculative task re-ran for a slow executor. The speculative copy also incremented the accumulator — but the task output was deduplicated. Accumulator double-counted; data didn't.

**Fix:**
```python
# 1. Use actions instead of transformations for counting
invalid_count = clean_rdd.filter(lambda r: r is None).count()

# 2. Or use DataFrame aggregate (recommended):
df = df.withColumn("is_invalid", ~is_valid_udf(F.col("data")))
invalid_count = df.filter("is_invalid").count()

# 3. If you must use accumulators: count in foreachPartition (action context)
def count_in_partition(partition):
    count = 0
    for record in partition:
        if not is_valid(record):
            count += 1
    invalid_acc.add(count)

clean_rdd.foreachPartition(count_in_partition)  # inside action → safe
```

---

## War Story: Cache Eviction Causing 3× Slowdown

**Scenario:** A pipeline cached three DataFrames — `dim_customers`, `dim_products`, `fact_orders`. Jobs ran fast in the morning but degraded 3× by afternoon as traffic data grew.

**Root cause:**
```
Morning:
  dim_customers: 2 GB cached
  dim_products:  1 GB cached
  fact_orders:   8 GB cached
  Total: 11 GB  (executor storage: 13 GB)  → all fits

Afternoon (fact_orders grown):
  dim_customers: 2 GB cached
  dim_products:  1 GB cached
  fact_orders:   15 GB cached
  Total: 18 GB  (executor storage: 13 GB)  → EVICTION!

fact_orders partitions evicted → recomputed from S3 on every action
Cache hit rate: 42%  (seen in Spark UI Storage tab)
```

**Fix:**
```python
# 1. Monitor cache hit rate in Spark UI → Storage tab
# 2. Only cache what's actually hot:
dim_customers.cache()   # 2GB — used in every job
dim_products.cache()    # 1GB — used in every job
# DON'T cache fact_orders — too large, read once per job from fast Parquet

# 3. Use MEMORY_AND_DISK to gracefully handle eviction
dim_customers.persist(StorageLevel.MEMORY_AND_DISK)

# 4. Unpersist after use:
result.write.parquet("output/")
dim_customers.unpersist()
dim_products.unpersist()
```

---

## Coalesce vs. Repartition: When to Use Which

```python
# Problem: writing 200 small files (default shuffle partitions)
# Each file = a few MB → S3/HDFS listing overhead, slow downstream reads

# repartition(N): full shuffle — use when INCREASING partitions
df.repartition(400)    # 200 → 400 partitions: shuffle required

# coalesce(N): merge local partitions — use when DECREASING
df.coalesce(20)        # 200 → 20 partitions: NO shuffle, just merges

# Before writing: coalesce to control output file size
target_size_mb = 256
total_mb = df.count() * avg_row_bytes / (1024 * 1024)
optimal_partitions = max(1, int(total_mb / target_size_mb))
df.coalesce(optimal_partitions).write.parquet("output/")
```

```python
# Caveat: coalesce can create uneven partitions (some executors idle)
# For final writes with slight imbalance, coalesce is fine
# For mid-pipeline use, repartition gives better parallelism

# Rule of thumb:
# After wide transform (result is small): coalesce before write
# Adding parallelism in middle of pipeline: repartition
```

---

## Partition Sizing Guide

```python
# Target partition size: 100-256 MB for batch, 10-50 MB for interactive

# Check actual partition sizes:
import sys
sizes = df.rdd.mapPartitions(lambda it: [sum(sys.getsizeof(r) for r in it)]).collect()
avg_mb = sum(sizes) / len(sizes) / (1024**2)
print(f"Avg partition: {avg_mb:.1f} MB")
print(f"Max partition: {max(sizes) / (1024**2):.1f} MB")  # outlier = skew

# Diagnose skew:
from pyspark.sql import functions as F
df.groupBy(F.spark_partition_id()).count().orderBy(F.desc("count")).show()
```

---

## Interview Tips

> **Tip 1:** "Have you encountered accumulator issues in production?" — Yes — accumulators can double-count when tasks are retried (speculative execution or failure recovery). Transformation-context accumulators are unsafe; action-context (foreachPartition) is safe. For reliable counting, use DataFrame aggregates instead — they're transactional and idempotent.

> **Tip 2:** "How do you decide partition count for a Spark job?" — Start with data size: target 100-256 MB per partition for batch. For shuffle operations, `spark.sql.shuffle.partitions` defaults to 200, which is wrong for most jobs (too many for small data, too few for large). With AQE enabled, Spark auto-coalesces after shuffles. Without AQE, set manually: `total_shuffle_bytes / 200MB`. For output files, coalesce to target 256 MB Parquet files for good downstream read performance.
