---
title: "Spark Architecture — Real World"
topic: spark
subtopic: spark-architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, architecture, production, sizing, troubleshooting, war-stories]
---

# Spark Architecture — Real World

## Cluster Sizing Formula

**Rule of thumb for executor sizing:**

```
Executor cores:  3–5 cores per executor (sweet spot)
  • Too few cores → not enough parallelism per executor
  • Too many cores → HDFS contention (>5 concurrent reads per executor causes throttling)
  • 4 cores/executor is the de facto standard

Executor memory:  4–8 GB per core
  • Include overhead: total container = executor memory + spark.executor.memoryOverhead (default 10%)
  • For memory-heavy joins: 6–8 GB/core; for simple transforms: 4 GB/core

Example: 20-node cluster, each node has 32 cores, 128 GB RAM
  • Leave 1 core + 1 GB per node for OS/YARN daemons
  • Available: 31 cores, 127 GB per node
  • 4 cores/executor, 16 GB/executor → 7 executors/node (4×7=28 cores, 16×7=112 GB)
  • Total: 7 × 20 = 140 executors
```

```python
spark = SparkSession.builder \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.executor.instances", "140") \
    .getOrCreate()
```

---

## War Story: The Heartbeat Cascade

**Scenario:** Nightly ETL job was failing intermittently around 2 AM with `ExecutorLostFailure` errors. The job processed ~500 GB of data and ran fine in dev.

**Root cause investigation:**
```
Spark UI → Executors tab:
  GC time: 35% (healthy is <5%)
  
JVM heap usage: 95% before each GC pause
GC pauses: 8–12 seconds
network.timeout: 120s (default)
heartbeatInterval: 10s

After 12× missed heartbeats (120s), Driver marked executor dead.
```

**What was happening:** The job joined two large DataFrames, causing execution memory pressure. The JVM triggered full GC (stop-the-world), taking 10+ seconds. During GC, the executor couldn't send heartbeats. After 120 seconds of misses, the Driver assumed the executor was dead, killed it, and resubmitted the stage — which triggered the same GC cycle on another executor.

**Fix:**
```python
# 1. Reduce GC pressure: increase memory and tune GC
spark.conf.set("spark.executor.memory", "24g")      # was 12g
spark.conf.set("spark.executor.memoryOverhead", "3g")
spark.conf.set("spark.executor.extraJavaOptions", 
    "-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:MaxGCPauseMillis=500")

# 2. Increase heartbeat tolerance
spark.conf.set("spark.network.timeout", "600s")
spark.conf.set("spark.executor.heartbeatInterval", "60s")

# 3. Enable AQE to reduce shuffle size
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
Job has run clean for 18 months since.

---

## War Story: Shuffle Fetch Timeout on S3

**Scenario:** Spark job on EMR reading from S3 passed Stage 1 fine but failed Stage 2 with `FetchFailedException` after ~30 minutes.

**Root cause:** By default, Spark uses `spark.reducer.maxRetriesOnNetworkErrors=3` with exponential backoff. S3 has eventual consistency windows and request throttling. The shuffle fetch HTTP calls to other executors hit S3 presigned URLs that had already expired.

**Fix:**
```python
# Increase fetch timeout and retries
spark.conf.set("spark.shuffle.io.maxRetries", "10")
spark.conf.set("spark.shuffle.io.retryWait", "60s")

# On EMR specifically: use EMR's optimized Shuffle
# spark.shuffle.service.enabled=true  (EMR sets this automatically)

# Alternatively: use Glue/EMR with S3-backed shuffle (no executor-to-executor)
spark.conf.set("spark.shuffle.manager", 
    "com.amazonaws.emr.spark.shuffle.S3ShuffleManager")
```

---

## Production Configuration Checklist

```python
production_config = {
    # Executor sizing
    "spark.executor.cores": "4",
    "spark.executor.memory": "16g",
    "spark.executor.memoryOverhead": "2g",

    # Adaptive Query Execution (Spark 3.0+, default on in 3.2+)
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",

    # Shuffle tuning
    "spark.sql.shuffle.partitions": "auto",   # AQE manages this
    "spark.shuffle.io.maxRetries": "10",
    "spark.shuffle.io.retryWait": "30s",

    # Fault tolerance
    "spark.task.maxFailures": "4",
    "spark.network.timeout": "300s",
    "spark.executor.heartbeatInterval": "30s",

    # Serialization (Kryo is faster than Java serialization)
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired": "false",

    # GC tuning
    "spark.executor.extraJavaOptions":
        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35",
}
```

---

## Common Architecture Anti-Patterns

| Anti-Pattern | Symptom | Fix |
|---|---|---|
| Driver collecting too much | OOM on Driver, `collect()` on large DF | Use `.write()` instead of `.collect()` |
| Too many small files → too many tasks | Stage takes 10× longer than expected, each task <1s | Coalesce before writing, increase partition size |
| Too few partitions | 1–2 executors doing 100% of work | Repartition to 2–4× executor count |
| Broadcasting a huge table | `SparkOutOfMemoryError` on executors | Check size before broadcast; use SMJ for large tables |
| Caching everything | Jobs slower than uncached | Cache only what's reused; uncache when done |
| UDFs killing optimization | No predicate pushdown in plan | Replace UDFs with built-in functions where possible |

---

## Interview Tips

> **Tip 1:** "How would you size a Spark cluster for a 1 TB daily batch job?" — Start with 4 cores / 16 GB per executor, leaving 1 core + 2 GB per node for OS. Estimate needed parallelism: 1 TB at 128 MB partitions = ~8000 tasks. With 10-executor nodes having 7 executors each, 10 nodes = 70 executors × 4 cores = 280 parallel tasks — 8000 tasks / 280 slots ≈ 29 waves. Add 20% headroom for skew and retries. Enable AQE and dynamic allocation to auto-tune.

> **Tip 2:** "How do you debug a Spark job that's slower than expected?" — Start with the Spark UI: check Stage detail for skewed tasks (one task 10× longer than median), check Executors for high GC time (>5%), check SQL tab for missing predicate pushdown or unexpected sort-merge joins. Then check application logs for spill warnings. Finally review the physical plan with `.explain("formatted")` to confirm expected optimizations are applied.
