---
title: "Performance Tuning - Senior Deep Dive"
topic: databricks
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, spark, performance, cost-optimization, benchmarking, memory-tuning]
---

# Performance Tuning — Senior Deep Dive

## Reading Spark UI Like a Senior

The Spark UI is the primary diagnostic tool. Know exactly what to look for:

**Stages tab:**
```
Stage 5: 1000 tasks | Input: 200GB | Output: 50GB | Duration: 45min
  Task metrics:
    Duration: min=2s, median=180s, max=3600s  ← SKEW (3600s vs 180s median)
    GC Time: avg=45s                           ← HIGH GC (25% of task time in GC)
    Shuffle Read: avg=400MB                    ← large shuffle
    Spill (disk): avg=1.2GB                   ← SPILL (memory pressure)
```

**What each metric tells you:**

| Metric | Problem if... | Action |
|--------|--------------|--------|
| Max task >> Median task | Skew | Salt, AQE skew join |
| GC Time > 10% of task time | Memory pressure | Increase executor memory, fewer partitions |
| Shuffle spill > 0 | Executor OOM on shuffle | More shuffle partitions, more memory |
| Input >> Output | Heavy filtering happening late | Push filters earlier |
| Many tiny tasks (< 1s each) | Over-partitioned | AQE coalesce, reduce shuffle partitions |

---

## Memory Architecture Deep Dive

```
Executor JVM Memory:
┌─────────────────────────────────────┐
│  spark.executor.memory = 16g        │
│  ┌─────────────────────────────┐   │
│  │  Execution Memory (60%)     │   │
│  │  - Shuffle buffers          │   │
│  │  - Sort buffers             │   │
│  │  - Aggregation hash maps    │   │
│  ├─────────────────────────────┤   │
│  │  Storage Memory (40%)       │   │
│  │  - Cached RDDs/DataFrames   │   │
│  │  - Broadcast variables      │   │
│  └─────────────────────────────┘   │
│  spark.executor.memoryOverhead=2g  │
│  (Python worker, off-heap buffers) │
└─────────────────────────────────────┘
```

```python
# Tune memory fractions
spark.conf.set("spark.memory.fraction", "0.8")          # 80% to Spark (was 75%)
spark.conf.set("spark.memory.storageFraction", "0.3")   # 30% of above to storage cache

# Increase overhead for Python UDFs (PySpark generates more off-heap memory pressure)
# Set in cluster config:
# spark.executor.memoryOverhead = 4g

# Off-heap memory for Arrow-based operations
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```

---

## Join Strategy Selection

```python
# Spark chooses join strategy automatically, but you can force one:

# 1. Broadcast Hash Join (BHJ) — best when one side is small
df1.join(broadcast(df2), "key")           # manual hint
# Or: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")

# 2. Sort Merge Join (SMJ) — default for large tables, requires shuffle + sort
df1.hint("merge").join(df2, "key")        # force sort merge

# 3. Shuffle Hash Join — uses hash instead of sort (faster for some shapes)
df1.hint("shuffle_hash").join(df2, "key")

# 4. Broadcast Nested Loop Join — for non-equijoins (no key match)
# Very slow — avoid, rewrite as equijoin where possible

# Decision tree:
# One side < broadcast threshold → BHJ (no shuffle)
# Both large, equijoin → SMJ (shuffle both sides)
# Both large, non-equijoin → BNL (avoid — full cartesian)
```

---

## Advanced Shuffle Optimization

```python
# Push-based shuffle: reduces map output pressure on shuffle write
# Enabled by default in Databricks Runtime 12+

# Shuffle service tuning for large jobs
spark.conf.set("spark.shuffle.file.buffer", "1mb")        # write buffer (default 32kb)
spark.conf.set("spark.reducer.maxSizeInFlight", "128mb")  # reduce fetch buffer

# Avoid wide transformations where possible
# Bad: sort on non-partitioned column forces full sort (expensive)
df.orderBy("customer_id")  # full sort if not partitioned by customer_id

# Good: if already partitioned, sortWithinPartitions is local sort only
df.repartition("customer_id").sortWithinPartitions("customer_id", "order_date")

# Minimize shuffles by combining operations
# Bad: two shuffles
df.groupBy("date").agg(F.sum("revenue")).repartition("date")

# Better: one operation
df.repartition("date").groupBy("date").agg(F.sum("revenue"))
```

---

## Benchmarking Framework

Never guess — always measure before and after:

```python
import time
from typing import Callable
from pyspark.sql import DataFrame

def benchmark(name: str, fn: Callable[[], DataFrame], iterations: int = 3) -> dict:
    """Run a query N times and report timing statistics."""
    times = []
    for i in range(iterations):
        # Clear Delta cache between runs for fair comparison
        spark.sql("CLEAR CACHE")
        start = time.time()
        result = fn()
        count = result.count()  # materialize
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Run {i+1}: {elapsed:.2f}s, {count:,} rows")

    return {
        "name": name,
        "min_s": min(times),
        "max_s": max(times),
        "avg_s": sum(times) / len(times),
        "p50_s": sorted(times)[len(times)//2]
    }

# Compare before and after optimization
before = benchmark("Before optimization", lambda: run_query_v1())
after  = benchmark("After optimization",  lambda: run_query_v2())

speedup = before["avg_s"] / after["avg_s"]
print(f"\nSpeedup: {speedup:.1f}x ({before['avg_s']:.1f}s → {after['avg_s']:.1f}s)")
```

---

## Cost-Aware Optimization

Performance optimization must consider cost — a 2x faster job on a 4x bigger cluster is not a win:

```python
# Compute efficiency score: rows processed per dollar
def compute_efficiency(rows: int, duration_s: float, cluster_dbu_rate: float) -> float:
    """DBUs consumed: duration_hours * cluster_dbu_rate"""
    dbus = (duration_s / 3600) * cluster_dbu_rate
    cost_usd = dbus * 0.22  # example DBU price
    return rows / cost_usd  # rows per dollar

# Right-sizing: benchmark on different cluster sizes
configs = [
    {"workers": 4, "instance": "i3.xlarge", "dbu_rate": 4.0},
    {"workers": 8, "instance": "i3.xlarge", "dbu_rate": 8.0},
    {"workers": 4, "instance": "i3.2xlarge", "dbu_rate": 8.0},
]
# Find the configuration with best efficiency score, not just lowest time
```

---

## Interview Tips

> **Tip 1:** "Walk me through how you'd debug a slow Spark job." — "Open Spark UI → Jobs → find the slow job → click into Stages. Sort stages by duration — find the bottleneck stage. In stage detail, check: (1) Is max task >> median? That's skew. (2) Is GC time > 10%? Memory pressure — increase executor memory. (3) Is there shuffle spill? Reduce partition size or add memory. (4) Click on a slow task → look at the task timeline to see what phase is slow (compute vs I/O vs GC)."

> **Tip 2:** "When should you use repartition by column vs Z-ordering?" — "Repartition by column is a Spark operation — it shuffles data in memory so that all rows with the same key land in the same partition during the current job. Good for downstream joins or group-bys in the same job. Z-ordering is a Delta Lake file layout optimization — it sorts data within Delta files on disk so future queries that filter on that column can skip more files. Use repartition for in-job efficiency, Z-order for query performance on persisted Delta tables."

> **Tip 3:** "What's the difference between `spark.executor.memory` and `spark.executor.memoryOverhead`?" — "`executor.memory` is the JVM heap — what Spark uses for task execution, shuffle buffers, and cached data. `memoryOverhead` is off-heap memory for Python workers (PySpark), Arrow buffers, OS, and network I/O. Python UDFs are especially hungry for overhead memory — if you see executor OOM on Python jobs, the fix is usually increasing `memoryOverhead`, not `executor.memory`."
