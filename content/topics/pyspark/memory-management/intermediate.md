---
title: "PySpark Memory Management - Intermediate"
topic: pyspark
subtopic: memory-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, memory, unified-memory, spill, gc, caching]
---

# PySpark Memory Management — Intermediate Concepts

## Unified Memory Management

Since Spark 1.6, execution and storage memory share a unified pool with a dynamic boundary:

```
Spark Memory Pool (spark.memory.fraction × (heap - 300MB)):
┌─────────────────────────────────────────────────────┐
│  Execution Memory  ←→  Storage Memory               │
│  (shuffles, joins)      (cache, broadcast)          │
│                                                      │
│  ←── dynamic boundary moves based on demand ──→     │
└─────────────────────────────────────────────────────┘

Rules:
1. Execution can evict storage (cached data) when it needs more
2. Storage CANNOT evict execution (active computation must complete)
3. Storage fills from its side; Execution fills from its side
4. When one side is idle, the other can use the full pool
```

**Practical implication:** If you cache a large DataFrame and then run a heavy join, Spark may evict parts of your cached data to make room for the join. The join won't fail — it gets priority over cached data.

---

## Spilling — When Memory Isn't Enough

When a task's data exceeds available execution memory, Spark "spills" data to disk:

```
Spill process:
1. Sort/shuffle/join data exceeds memory budget
2. Spark writes overflow data to local disk (temp files)
3. When needed again: reads back from disk
4. Result: job succeeds but is 10-100x slower for that task

In Spark UI: "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)"
- Memory spill: data serialized but still in JVM
- Disk spill: data written to local SSD/HDD (much slower)
```

**Diagnosing spill:**
```python
# Check in Spark UI → Stages → Click on stage → Summary metrics
# If "Shuffle Spill (Disk)" > 0: you're spilling

# Common causes:
# 1. Partition too large for available execution memory
# 2. Too many concurrent tasks competing for memory
# 3. Large broadcast + heavy computation at same time
```

**Fixing spill:**

| Fix | How | Trade-off |
|-----|-----|-----------|
| More memory | `spark.executor.memory = "16g"` | Higher cost |
| More partitions | `spark.sql.shuffle.partitions = 800` | More scheduling overhead |
| Fewer cores | `spark.executor.cores = 3` (from 5) | Less parallelism, more memory per task |
| Higher memory fraction | `spark.memory.fraction = 0.8` | Less user memory for variables |

> **Rule:** Target is ZERO spill for all tasks. Any spill indicates the job is slower than it could be.

---

## GC (Garbage Collection) Tuning

Heavy GC pauses cause tasks to hang (10+ second pauses):

```python
# Check GC impact in Spark UI → Executors tab
# "GC Time" column: should be < 10% of total task time
# If > 10%: GC is a problem

# Solution 1: Reduce object creation (use DataFrames not RDDs)
# DataFrames use Tungsten binary format (off-heap-like, less GC pressure)
# RDDs create Java objects for every row → massive GC pressure

# Solution 2: Use G1GC (better for large heaps)
spark.conf.set("spark.executor.extraJavaOptions", 
    "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")

# Solution 3: Use off-heap memory (bypasses GC entirely)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
# Execution memory stored off-heap (not subject to GC)
```

---

## Caching Strategy

### Storage Levels

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)          # Fast, evicted if not enough memory
df.persist(StorageLevel.MEMORY_AND_DISK)       # Spills to disk if memory full (default for .cache())
df.persist(StorageLevel.DISK_ONLY)             # Only on disk (saves memory for computation)
df.persist(StorageLevel.MEMORY_ONLY_SER)       # Serialized in memory (less space, more CPU)
df.persist(StorageLevel.OFF_HEAP)              # Off-heap (no GC, requires config)
```

### When to Cache (Decision Framework)

| Cache when... | Don't cache when... |
|--------------|-------------------|
| DataFrame reused 2+ times | Used only once |
| After expensive computation (join/agg) | Simple filter/select (cheap to recompute) |
| Data fits in cluster memory | Data much larger than total cluster memory |
| Between multiple actions | Only one action follows |

```python
# GOOD: cache before multiple downstream uses
expensive_df = df.join(big_table, "key").groupBy("category").agg(...)
expensive_df.cache()
expensive_df.count()  # Triggers materialization

result_a = expensive_df.filter("category = 'A'").agg(...)  # Reads from cache
result_b = expensive_df.filter("category = 'B'").agg(...)  # Reads from cache

expensive_df.unpersist()  # Free memory when done!
```

### Cache Memory Sizing

```python
# Estimate cache size BEFORE caching:
df.persist(StorageLevel.MEMORY_ONLY)
df.count()  # Trigger caching

# Check actual cache size in Spark UI → Storage tab
# Or programmatically:
spark.sparkContext._jsc.sc().getRDDStorageInfo()

# If cached size > available storage memory:
# - Oldest cached partitions get evicted (LRU)
# - Job still works but recomputes evicted partitions
# - Monitor: "Block evicted" messages in logs
```

---

## PySpark-Specific Memory Concerns

### Python Worker Memory (UDFs)

```python
# Python UDFs run in separate Python processes (not JVM)
# Their memory comes from spark.executor.memoryOverhead (not executor.memory!)

# BAD: Pandas UDF loading 2 GB per partition into Python memory
@pandas_udf("double")
def heavy_transform(series: pd.Series) -> pd.Series:
    # This runs in Python — memory from memoryOverhead!
    return some_large_computation(series)

# If memoryOverhead is only 384 MB (default): Python worker OOM!

# FIX: Increase overhead for PySpark UDF workloads
spark.conf.set("spark.executor.memoryOverhead", "4g")
# AND reduce partition sizes so less data hits each Python worker
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

### Arrow/Pandas Conversion Memory

```python
# toPandas() and createDataFrame(pandas_df) use Arrow for transfer
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Arrow buffers consume DRIVER memory during toPandas()
# If DataFrame is 5 GB and driver has 4 GB → OOM
# Always aggregate/filter before toPandas()!
small_result = df.groupBy("region").count()  # Tiny result
pdf = small_result.toPandas()  # Safe: few KB
```

---

## Memory Configuration Template

```python
# Production configuration for 500 GB ETL workload
{
    # Executor sizing (target: 128 MB per partition, 5 cores per executor)
    "spark.executor.memory": "24g",
    "spark.executor.memoryOverhead": "4g",      # For Python workers
    "spark.executor.cores": "5",
    "spark.memory.fraction": "0.7",             # 70% for Spark (more than default 60%)
    
    # Driver (needs memory for broadcast + job coordination)
    "spark.driver.memory": "8g",
    "spark.driver.memoryOverhead": "2g",
    
    # Partition sizing
    "spark.sql.shuffle.partitions": "auto",     # AQE handles it
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    
    # GC optimization
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:G1HeapRegionSize=16m",
    
    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}
```

---

## Interview Tips

> **Tip 1:** "Explain Spark's unified memory model" — "Execution (shuffles, joins) and storage (cache, broadcast) share a single memory pool. The boundary is dynamic: execution can evict cached data when needed, but storage can't evict active computation. This means caching won't cause OOM — Spark just evicts cached data to make room for computation."

> **Tip 2:** "How do you fix spilling?" — "Three options in priority order: (1) Increase spark.sql.shuffle.partitions (more partitions = less data per task = fits in memory). (2) Increase spark.executor.memory. (3) Reduce spark.executor.cores (fewer concurrent tasks = more memory per task). Target: zero spill in all tasks."

> **Tip 3:** "PySpark UDF OOMs differently — why?" — "Python UDFs run in separate Python worker processes outside the JVM. Their memory comes from spark.executor.memoryOverhead, NOT executor.memory. Default overhead is only 384 MB — insufficient for Pandas UDFs processing large partitions. Fix: increase memoryOverhead to 2-4 GB and reduce partition sizes."
