---
title: "PySpark Performance Tuning - Intermediate"
topic: pyspark
subtopic: performance-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, performance, aqe, serialization, join-strategies, memory-management]
---

# PySpark Performance Tuning — Intermediate Concepts

## Adaptive Query Execution (AQE) — Spark 3.0+

AQE re-optimizes the query plan **at runtime** based on actual data statistics collected after each shuffle stage. This is the single most impactful optimization in modern Spark.

```python
# Enable AQE (often default in Spark 3.2+/Databricks)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE Feature 1: Auto-coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
# Merges tiny post-shuffle partitions into ~128 MB chunks automatically

# AQE Feature 2: Auto-convert SortMergeJoin to BroadcastHashJoin
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")
# If one side of a join is discovered to be small at runtime → broadcast it

# AQE Feature 3: Handle skewed joins automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
# Splits oversized partitions into smaller chunks automatically
```

**What AQE solves without you doing anything:**

| Problem | Without AQE | With AQE |
|---------|------------|----------|
| 200 shuffle partitions but data is small | Tiny tasks, overhead | Auto-coalesces to fewer partitions |
| Join with unknown small table | Sort-merge (expensive) | Detects small side → broadcasts |
| Skewed join key | One task runs 100x longer | Splits skewed partition into sub-tasks |
| Post-filter reduces data dramatically | Plan uses pre-filter estimates | Re-optimizes based on actual row counts |

> **Rule:** Always enable AQE in production. It fixes the most common performance problems automatically.

---

## Join Strategy Selection

Spark chooses between three physical join strategies:

### BroadcastHashJoin (Best for Small + Large)

```python
# Automatically chosen when one side < autoBroadcastJoinThreshold (10 MB default)
# Force it explicitly:
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

**Requirements:** One side must fit in driver memory AND executor memory.
**Performance:** O(n) — probe the hash table for each row in the large side.
**No shuffle of the large table.**

### SortMergeJoin (Default for Large + Large)

```python
# Chosen when both sides are too large to broadcast
result = large_df_a.join(large_df_b, "key")
# Both sides shuffled by key, sorted, then merged
```

**Requires:** Shuffle + sort of BOTH tables.
**Performance:** O(n log n + m log m) for sorting + O(n + m) for merge.
**Two full shuffles** — expensive but handles any size.

### ShuffleHashJoin (Middle Ground)

```python
# One side fits in memory (per partition, not total)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
# Forces hash join when one side's per-partition data fits in memory
```

**Requires:** Shuffle of both sides, but no sort (builds hash table per partition).
**When:** One side is moderately small (too large for broadcast, but fits in executor memory per partition).

### Choosing the Right Strategy

| Scenario | Best Strategy | How to Get It |
|----------|--------------|---------------|
| One side < 100 MB | BroadcastHashJoin | `broadcast()` or increase threshold |
| Both sides large, equi-join | SortMergeJoin | Default (let optimizer decide) |
| Both large, one side moderate per partition | ShuffleHashJoin | Disable preferSortMergeJoin |
| Inequality join (range, BETWEEN) | BroadcastNestedLoopJoin | Broadcast the small side |

---

## Avoiding UDF Performance Traps

Python UDFs are 10-100x slower than native Spark functions because they serialize data between JVM and Python:

```python
# BAD: Python UDF (serializes every row to Python, processes, serializes back)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_email(email):
    return email.strip().lower() if email else None

df.withColumn("email_clean", clean_email(col("email")))
# 10-100x slower than native!

# GOOD: Use native Spark functions (runs in JVM, no serialization)
from pyspark.sql.functions import trim, lower

df.withColumn("email_clean", lower(trim(col("email"))))
# 10-100x faster!
```

**When you MUST use UDFs (complex logic), use Pandas UDFs:**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def complex_calculation(series: pd.Series) -> pd.Series:
    """Processes entire column as a Pandas Series — vectorized, much faster than row UDF."""
    return series.apply(lambda x: some_complex_math(x))

df.withColumn("result", complex_calculation(col("input_column")))
# 5-10x faster than regular Python UDF (vectorized, fewer serialization calls)
```

**UDF performance hierarchy:**

| Type | Relative Speed | When to Use |
|------|:---:|------|
| Native Spark functions | 1x (fastest) | Always prefer |
| Pandas UDF (vectorized) | 3-10x slower | Complex Python logic on columns |
| Python UDF (row-by-row) | 10-100x slower | Last resort only |

---

## Memory Management

### Executor Memory Layout

```
Total Executor Memory (e.g., 8 GB)
├── Execution Memory (50% of Spark pool): shuffles, joins, sorts, aggregations
├── Storage Memory (50% of Spark pool): cached DataFrames, broadcast variables
└── Overhead (off-heap): JVM overhead, Python processes
```

```python
# Key memory settings
spark.conf.set("spark.executor.memory", "8g")          # Total JVM heap
spark.conf.set("spark.executor.memoryOverhead", "2g")  # Off-heap (Python UDFs, overhead)
spark.conf.set("spark.memory.fraction", "0.6")         # % of heap for execution+storage
spark.conf.set("spark.memory.storageFraction", "0.5")  # Starting split (dynamic)
```

### Diagnosing Memory Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `java.lang.OutOfMemoryError: Java heap space` | Data too large for executor | Increase `executor.memory` or add more partitions |
| Spill to disk (Spark UI shows spill bytes) | Sort/join exceeds execution memory | Increase memory or reduce partition size |
| GC time >10% in Spark UI | Too many objects in heap | Reduce cached data, use columnar (Parquet) |
| Driver OOM | `collect()` or large broadcast | Never `collect()` large data; reduce broadcast size |

---

## Write Optimization

### File Size Control

```python
# Problem: too many small files after shuffle (200 partitions × 10 MB = 200 tiny files)
# Fix 1: coalesce before write
df.coalesce(10).write.parquet("s3://output/")  # 10 files, ~200 MB each

# Fix 2: maxRecordsPerFile (Spark 2.4+)
df.write.option("maxRecordsPerFile", 1000000).parquet("s3://output/")
# Each file gets at most 1M records — predictable file sizes

# Fix 3: AQE auto-coalesces partitions (Spark 3.0+)
# With AQE enabled, tiny partitions are automatically merged
```

### Partitioned Write Optimization

```python
# Problem: writing with partitionBy creates too many small files
# (10 departments × 200 shuffle partitions = 2000 files!)
df.write.partitionBy("department").parquet("s3://output/")  # 2000 files!

# Fix: repartition by the partition column before writing
df.repartition("department") \
    .write.partitionBy("department") \
    .parquet("s3://output/")  # 10 files (one per department, properly sized)

# For multiple partition columns with controlled file count:
df.repartition(50, "year", "month") \
    .write.partitionBy("year", "month") \
    .parquet("s3://output/")
```

---

## Serialization

### Kryo Serialization (Faster than Java Default)

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")
# Kryo is 2-10x faster than Java serialization for shuffle data
```

### Columnar Batch Processing

```python
# Enable Arrow-based columnar data transfer (for Pandas UDFs)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
# 10-100x faster for toPandas() and Pandas UDFs
```

---

## Configuration Checklist

| Setting | Default | Production Recommendation | Why |
|---------|---------|--------------------------|-----|
| `spark.sql.adaptive.enabled` | false (Spark <3.2) | `true` | Auto-optimizes at runtime |
| `spark.sql.shuffle.partitions` | 200 | `auto` (AQE) or data-based | Match to data volume |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | `50-100 MB` | Broadcast more joins |
| `spark.serializer` | Java | Kryo | Faster serialization |
| `spark.sql.execution.arrow.pyspark.enabled` | false | `true` | Faster Pandas interop |
| `spark.executor.memory` | 1g | 4-16g | Enough for tasks |
| `spark.executor.cores` | 1 | 4-5 | Parallelism per executor |
| `spark.default.parallelism` | Total cores | 2-3x total cores | Good parallelism |

---

## Interview Tips

> **Tip 1:** "What's the first thing you check for a slow Spark job?" — "The Spark UI. I look at: (1) Stage that's taking longest — what operation is it? (2) Task duration skew within that stage — is one task 10x longer (data skew)? (3) Shuffle read/write size — is it avoidable? (4) Spill metrics — do I need more memory?"

> **Tip 2:** "How does AQE help?" — "Three ways: (1) Coalesces tiny partitions after shuffle (fewer tasks, less overhead). (2) Converts SortMergeJoin to Broadcast at runtime if one side turns out small. (3) Splits skewed partitions automatically. It does all this based on actual data statistics collected between stages."

> **Tip 3:** "Why are Python UDFs slow?" — "Data serialization overhead. Each row is serialized from JVM to Python (pickle/Arrow), processed in Python, serialized back to JVM. This per-row overhead makes UDFs 10-100x slower than native Spark functions that run entirely in the JVM. Use Pandas UDFs (vectorized) when native functions can't express the logic."
