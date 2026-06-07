---
title: "PySpark Memory Management - Real-World Production Examples"
topic: pyspark
subtopic: memory-management
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, memory, production, oom-debugging, tuning]
---

# PySpark Memory Management — Real-World Production Examples

## Case Study 1: Driver OOM from collect()

**Problem:** Data science team calls `df.toPandas()` on a 20 GB DataFrame. Driver has 4 GB. Crashes with `java.lang.OutOfMemoryError: Java heap space` on the driver.

**Fix:**
```python
# BAD: pulling all data to driver
pdf = big_df.toPandas()  # 20 GB → driver OOM

# FIX 1: Aggregate first, then collect (only summary data)
summary = big_df.groupBy("category").agg(
    count("*").alias("n"), avg("amount").alias("avg")
)
pdf = summary.toPandas()  # Only a few KB — safe!

# FIX 2: Sample if they need raw records
sample_pdf = big_df.sample(fraction=0.001).toPandas()  # 20 MB sample

# FIX 3: If they truly need full data in Pandas, write to Parquet and read locally
big_df.write.parquet("/tmp/full_export/")
pdf = pd.read_parquet("/tmp/full_export/")  # Reads chunk by chunk outside Spark
```

---

## Case Study 2: Executor OOM During Large Join

**Problem:** Joining 500M-row fact table with 50M-row dimension. Executors (8 GB each) crash during the join stage.

**Diagnosis:**
```
Spark UI → Stages → Join stage → Tasks tab:
- Task 145: OOM (attempted to allocate 6 GB for hash table)
- Partition 145 has 50M rows (skewed — others have 500K)
- The hash join build side for this partition exceeds executor memory
```

**Fix (multiple options):**
```python
# Fix 1: More partitions (reduce per-partition data)
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # Was 200
# Each partition: 500M / 2000 = 250K rows (manageable)

# Fix 2: Broadcast the dimension (50M rows × ~200 bytes = 10 GB — too large!)
# NOT feasible for 50M rows. Skip this.

# Fix 3: Increase executor memory
spark.conf.set("spark.executor.memory", "24g")
spark.conf.set("spark.executor.cores", "4")  # 6 GB per core

# Fix 4: Enable AQE skew handling (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE detects the 50M-row partition and splits it automatically

# Fix 5 (best for this case): Filter the skew
# If partition 145 is "user_id = NULL" → handle separately
null_facts = facts.filter("user_id IS NULL")
non_null_facts = facts.filter("user_id IS NOT NULL")
result_nn = non_null_facts.join(dim, "user_id")  # Balanced, no skew
result_null = null_facts.crossJoin(broadcast(unknown_user_dim))  # Tiny broadcast
result = result_nn.unionByName(result_null)
```

---

## Case Study 3: GC Overhead Killing Throughput

**Problem:** Job runs at 50% of expected throughput. Spark UI shows 35% of task time in GC.

**Diagnosis:**
```
Executors tab:
- GC Time: 35% of total executor time
- Executor memory: 32g with G1GC
- 8 cores per executor (too many objects created concurrently)
```

**Fix:**
```python
# Fix 1: Reduce cores (fewer concurrent tasks = fewer objects at once)
spark.conf.set("spark.executor.cores", "4")  # Was 8
# Same total parallelism, just more executors with fewer cores each

# Fix 2: Use off-heap (execution memory bypasses GC)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "8g")
# Shuffles/joins happen off-heap → less JVM object creation

# Fix 3: Tune G1GC for large heaps
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:G1HeapRegionSize=16m "        # Larger regions for 32 GB heap
    "-XX:InitiatingHeapOccupancyPercent=35 "  # Start GC earlier (less full GC)
    "-XX:ConcGCThreads=4"              # More concurrent GC threads
)

# Fix 4: Switch to DataFrames if using RDDs
# RDD operations create Java objects per row → massive GC pressure
# DataFrame/Tungsten uses binary format → minimal GC
```

**Result:** GC time dropped from 35% to 8%, throughput doubled.

---

## Case Study 4: Python Worker OOM from Pandas UDF

**Problem:** Pandas UDF processes customer features. Some customers have 5M records. Python worker OOMs when loading 5M rows into a Pandas DataFrame.

```python
# The problematic UDF
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def compute_features(pdf: pd.DataFrame) -> pd.DataFrame:
    # pdf can be 5M rows for one customer → 4 GB in memory!
    # Python worker has default 384 MB overhead → OOM
    features = heavy_computation(pdf)
    return features

result = df.groupBy("customer_id").apply(compute_features)
```

**Fix:**
```python
# Fix 1: Increase Python worker memory
spark.conf.set("spark.executor.memoryOverhead", "6g")  # Was 384 MB
spark.conf.set("spark.executor.pyspark.memory", "4g")  # Spark 3.0+ explicit

# Fix 2: Limit data per group before the UDF
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Keep only last 10K records per customer
w = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
limited = df.withColumn("rn", row_number().over(w)).filter("rn <= 10000").drop("rn")
result = limited.groupBy("customer_id").apply(compute_features)

# Fix 3: Use applyInPandas with iterator (streaming, bounded memory)
def compute_features_streaming(key, pdf_iterator):
    for pdf_chunk in pdf_iterator:
        yield process_chunk(pdf_chunk)

result = df.groupBy("customer_id").applyInPandas(
    compute_features_streaming, schema=output_schema
)
# Processes in chunks instead of loading entire group into memory
```

---

## Production Memory Configuration Checklist

| Check | How | Red Flag |
|-------|-----|----------|
| Shuffle spill | Spark UI → Stages → Spill (Disk) | Any value > 0 |
| GC time | Spark UI → Executors → GC Time | > 10% of total time |
| Peak execution memory | Spark UI → Stage → Summary | At or near limit |
| Driver memory usage | Driver logs or metrics | > 80% of driver.memory |
| Python worker memory | Executor logs for "Cannot allocate memory" | Python process killed |
| Broadcast size vs executor memory | Compare broadcast size to memory.fraction × executor.memory | Broadcast > 30% of Spark memory |
| Cache eviction | Spark UI → Storage tab | Cached RDD marked as "partially cached" |

---

## Interview Tips

> **Tip 1:** "Your Spark job is spilling 200 GB to disk — how do you fix it?" — "First: identify WHICH stage/operator is spilling (Spark UI). If it's a join: increase partitions (more but smaller partitions fit in memory) or broadcast the small side. If it's a sort: same — more partitions. If it's groupBy with collect_list: cap the collection. As a last resort: increase executor memory. The goal is always zero spill."

> **Tip 2:** "How do you handle memory for PySpark UDFs?" — "UDFs run in Python workers outside the JVM. Their memory comes from memoryOverhead (and pyspark.memory in Spark 3.0+). Default 384 MB is insufficient for Pandas UDFs. I set memoryOverhead to 4-6 GB. Also: limit data per group before the UDF (cap with row_number), or use applyInPandas with an iterator pattern for streaming processing."

> **Tip 3:** "How do you choose between increasing memory vs increasing partitions?" — "Partitions first: it's free (just a config change) and improves parallelism. More memory costs money (larger instances). Rule: if average partition size > 128 MB, increase partitions. If partition sizes are already reasonable (50-128 MB) and still spilling, THEN increase memory. Also check for skew — one huge partition may need AQE or manual salting, not more global memory."
