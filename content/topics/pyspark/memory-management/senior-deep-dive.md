---
title: "PySpark Memory Management - Senior Deep Dive"
topic: pyspark
subtopic: memory-management
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, memory, tungsten, off-heap, optimization, debugging]
---

# PySpark Memory Management — Senior-Level Deep Dive

## Project Tungsten — Binary Memory Management

Tungsten is Spark's internal memory management layer that bypasses Java object overhead:

**Traditional Java objects:** Each row as a Java object has ~16 bytes header + field references + padding = massive overhead for billions of rows.

**Tungsten binary format:** Rows stored as raw bytes in a contiguous memory region. No Java object header, no GC pressure, cache-friendly access patterns.

```
Java Object (per row):        Tungsten (per row):
┌──────────────────────┐     ┌──────────────┐
│ Object header (16B)  │     │ Raw bytes:   │
│ Field refs (8B each) │     │ [4B int]     │
│ Actual data          │     │ [8B double]  │
│ Padding (alignment)  │     │ [var string] │
│ Total: ~80 bytes     │     │ Total: ~20B  │
└──────────────────────┘     └──────────────┘
```

**Impact:** 4x less memory usage, 10x less GC pressure, 2-3x faster processing (CPU cache locality).

**This is why DataFrame/Dataset is faster than RDD:** DataFrames use Tungsten. RDDs use Java objects.

---

## Off-Heap Memory Configuration

Off-heap memory lives outside the JVM heap — not subject to GC:

```python
# Enable off-heap for execution memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "8g")

# Total memory per executor becomes:
# On-heap: spark.executor.memory (managed by JVM GC)
# Off-heap: spark.memory.offHeap.size (managed by Spark directly, no GC)
# Overhead: spark.executor.memoryOverhead (OS + Python workers)

# Example: 8g on-heap + 8g off-heap + 2g overhead = 18g per executor
# Execution memory can use off-heap for shuffles/joins (zero GC impact)
```

**When to use off-heap:**
- Jobs with >16 GB executor memory (large heaps = long GC pauses)
- Frequent full GC causing task timeouts
- Jobs with many concurrent tasks generating lots of intermediate objects

---

## Memory Accounting Per Operator

Each Spark operator has different memory demands:

| Operator | Memory Pattern | Peak Usage |
|----------|---------------|-----------|
| `HashAggregate` | Hash table for groups | proportional to distinct groups |
| `SortMergeJoin` | Sort buffers for both sides | proportional to partition size |
| `BroadcastHashJoin` | Full broadcast in every executor | broadcast table size |
| `Window` | Sort buffer + window frame | partition size |
| `collect_list/set` | Accumulates into array | unbounded (entire group!) |
| `Sort` | Sort buffer | partition size |
| `HashJoin (build side)` | Hash table of build input | build-side partition size |

**The dangerous operators:** `collect_list`, `collect_set`, and `pivot` can use unbounded memory per group. One user with 10M events → 10M-element array in memory.

---

## Diagnosing Memory Issues via Spark UI

### Peak Execution Memory

```
Spark UI → Stages → Click stage → Summary Metrics:
- "Peak Execution Memory": highest memory used by any task
  If this equals spark.executor.memory × spark.memory.fraction: at limit!
  
- "Shuffle Spill (Memory)": bytes serialized to memory before disk
- "Shuffle Spill (Disk)": bytes actually written to disk
  Any disk spill = performance degradation
```

### Per-Task Memory Breakdown

```
Stages → Tasks tab:
- Duration: task time (if one is 10x longer = skew/memory issue)
- GC Time: time spent in garbage collection
  If GC > 20% of duration: memory pressure
  
- Shuffle Read: bytes this task read from previous stage
  If one task reads 100x more than others: skew
```

### Memory Tab (Spark 3.0+)

```
Executors → Memory tab:
- Storage Memory Used: cached DataFrames
- Storage Memory Available: remaining cache capacity
- If Storage > 70% of total: caching may be evicting execution needs
```

---

## Broadcast Variable Memory Impact

```python
# Each broadcast variable exists:
# 1. On the driver (full copy) — uses DRIVER memory
# 2. On each executor (full copy) — uses EXECUTOR storage memory

# 100 MB broadcast × 50 executors = 100 MB driver + 100 MB per executor = 5.1 GB total cluster memory

# Multiple broadcasts compound:
broadcast_a = broadcast(table_a)  # 100 MB
broadcast_b = broadcast(table_b)  # 200 MB  
broadcast_c = broadcast(table_c)  # 150 MB
# Each executor holds: 100 + 200 + 150 = 450 MB in storage memory!
# If executor.memory = 8g and memory.fraction = 0.6:
# Spark memory = 4.62 GB, Storage gets ~2.3 GB
# Broadcasts use 450 MB of that 2.3 GB (leaving 1.85 GB for cache)

# FIX: unpersist broadcasts when no longer needed
spark.sparkContext.broadcast(table_a).unpersist()
```

---

## Memory Sizing for Common Workloads

### Light ETL (filter, select, write)

```python
{
    "spark.executor.memory": "4g",
    "spark.executor.cores": "4",
    "spark.executor.memoryOverhead": "1g",
    # 1 GB per core — sufficient for map-only operations
}
```

### Medium ETL (joins, aggregations)

```python
{
    "spark.executor.memory": "16g",
    "spark.executor.cores": "5",
    "spark.executor.memoryOverhead": "2g",
    "spark.memory.fraction": "0.7",
    # ~3 GB per core — handles most join/agg patterns
}
```

### Heavy ETL (large joins, collect_list, pivots)

```python
{
    "spark.executor.memory": "32g",
    "spark.executor.cores": "4",
    "spark.executor.memoryOverhead": "4g",
    "spark.memory.fraction": "0.8",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "8g",
    # 8+ GB per core + off-heap — for memory-intensive operations
}
```

### PySpark with Pandas UDFs

```python
{
    "spark.executor.memory": "12g",
    "spark.executor.cores": "4",
    "spark.executor.memoryOverhead": "6g",  # Extra for Python workers!
    "spark.executor.pyspark.memory": "4g",  # Spark 3.0+ explicit Python budget
    # Python workers get 6 GB overhead + 4 GB pyspark.memory = 10 GB total
}
```

---

## Advanced: Memory Profiling

```python
# Method 1: Spark metrics (collected automatically)
spark.conf.set("spark.executor.processTreeMetrics.enabled", "true")
# Reports RSS (Resident Set Size) per executor in metrics

# Method 2: Custom memory tracking in UDFs
import os, psutil

def memory_intensive_udf(partition_iter):
    process = psutil.Process(os.getpid())
    before = process.memory_info().rss / 1024 / 1024  # MB
    
    results = []
    for row in partition_iter:
        results.append(transform(row))
    
    after = process.memory_info().rss / 1024 / 1024
    print(f"Partition used {after - before:.0f} MB")
    return iter(results)

df.mapInPandas(memory_intensive_udf, schema=output_schema)

# Method 3: JVM profiling (advanced)
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heapdump")
# Generates heap dump on OOM for analysis with tools like Eclipse MAT
```

---

## Interview Tips

> **Tip 1:** "Explain Tungsten and why DataFrames are faster than RDDs" — "Tungsten stores data as raw bytes in contiguous memory instead of Java objects. This eliminates object header overhead (16 bytes per row), reduces GC pressure (fewer objects to track), and improves CPU cache hit rate (sequential access). DataFrames use Tungsten automatically; RDDs use Java objects. Result: DataFrame operations are 2-10x faster and use 4x less memory."

> **Tip 2:** "How do you debug an executor OOM?" — "Step 1: Check Spark UI for which stage/task fails. Step 2: Look at shuffle spill (if spilling then OOM, partition is too large). Step 3: Check if broadcast variables are consuming storage memory. Step 4: Look for collect_list/collect_set (unbounded memory per group). Step 5: Check GC time (if >30%, memory is thrashing). Fix: increase partitions (smaller data per task), increase memory, or cap unbounded operations."

> **Tip 3:** "What's the relationship between executor cores and memory?" — "Each core runs one task concurrently. Memory is shared across all concurrent tasks. With 5 cores and 20 GB: each task gets ~4 GB on average. But if one task needs 15 GB (large partition): it steals from others. Reducing cores gives each task more memory but reduces parallelism. The sweet spot: 4-5 cores with 4-6 GB per core."

## ⚡ Cheat Sheet

**Memory Regions (Unified Memory Model)**
- Total executor memory = `spark.executor.memory` + `spark.executor.memoryOverhead`
- `spark.memory.fraction` (default 0.6) = fraction of heap for Spark managed memory
- Spark managed memory split: `spark.memory.storageFraction` (default 0.5) storage vs execution
- Reserved memory: 300MB fixed for internal Spark objects
- Formula: usable heap = (executor_memory − 300MB) × 0.6; execution can borrow from storage

**Tungsten Off-Heap**
```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")  # per executor
# Bypasses JVM GC; critical for GC-heavy workloads (long GC pauses in Spark UI = signal)
```

**GC Tuning Signals**
- GC time > 10% of task time in Spark UI → memory pressure
- G1GC recommended: `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`
- Reduce `spark.memory.fraction` to 0.5 to give more to user memory (caching, UDFs)

**OOM Taxonomy**
| OOM Type | Location | Common Cause | Fix |
|----------|----------|--------------|-----|
| Executor heap OOM | Executor | Large shuffle, big broadcasts | Increase executor memory or reduce parallelism |
| Driver OOM | Driver | collect(), broadcast large table | Increase driver memory; avoid collect |
| memoryOverhead OOM | Executor | Python UDFs, off-heap native libs | Increase `spark.executor.memoryOverhead` |
| Container OOM (K8s) | Container kill | overhead not accounted | Set overhead = 10-20% of executor memory |

**Cache Storage Levels**
```python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # spills to disk if memory full
df.persist(StorageLevel.MEMORY_ONLY_SER)  # serialized in memory (less space, more CPU)
df.persist(StorageLevel.DISK_ONLY)        # for large DFs used multiple times
df.unpersist()  # always clean up
```

**Interview Traps**
- `spark.executor.memory` ≠ total container memory; add `memoryOverhead` for container limit
- Caching a DF does NOT prevent re-computation if cache is evicted (LRU eviction)
- Python UDFs run in separate Python process — memory is outside executor JVM heap
