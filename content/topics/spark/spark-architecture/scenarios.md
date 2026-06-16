---
title: "Spark Architecture Interview Scenarios"
description: "Hands-on scenarios covering Spark driver, executors, DAG, and cluster modes"
content_type: scenario_question
topic: spark
subtopic: spark-architecture
tags: [spark, architecture, driver, executor, dag, cluster-manager]
---

<article data-difficulty="junior">

## Scenario: Driver OutOfMemoryError — What's Going Wrong?

A junior data engineer comes to you with this error:

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.scheduler.DAGScheduler...
```

Their job reads a 10GB CSV file, runs a few filters, and calls `.collect()` at the end to print results. The cluster has 10 executors with 8GB each. They ask: "Why is it failing? The executors have plenty of memory."

Walk through the driver's memory roles and explain what causes this OOM.

<details>
<summary>✅ Solution</summary>

### Why the Driver Has a Separate Memory Budget

The Spark **driver** is a single JVM process that runs on the machine where you submit the job (or on a cluster node in cluster mode). It is NOT one of the executors — it has its own heap, configured independently:

```
spark.driver.memory        # default: 1g
spark.driver.memoryOverhead  # off-heap for native libs, NIO buffers
```

Executor memory (`spark.executor.memory`) is irrelevant to driver OOM.

### What the Driver Stores in Memory

| Responsibility | Memory Impact |
|---|---|
| SparkContext / SparkSession objects | Small but persistent |
| DAG (Directed Acyclic Graph) | Grows with query complexity |
| Task result collection | **Huge** if `.collect()` is used |
| Broadcast variables | Each broadcast copy lives in driver + all executors |
| Accumulator state | Small per accumulator |
| RDD lineage graph | Grows with long chains of transformations |

### The Root Cause: `.collect()`

`.collect()` pulls **all data from all executors back to the driver** into a single in-memory list:

```python
# BAD — pulls 10GB into driver memory
results = df.collect()
for row in results:
    print(row)

# GOOD — process row-by-row on executors, print sample
df.show(20)          # displays top 20 rows
df.limit(1000).show(False)  # show 1000 rows

# GOOD — write to storage instead of collecting
df.write.parquet("s3://bucket/output/")
```

When 10GB of data flows back to a driver configured with (default) 1GB heap, you get OOM.

### How to Fix It

**Option 1: Avoid `.collect()` entirely**
```python
# Instead of collect → print, use show() or write()
df.filter(df.status == "active").show(50)
df.write.mode("overwrite").parquet("s3://output/")
```

**Option 2: Increase driver memory if you must collect**
```bash
spark-submit \
  --driver-memory 16g \
  --conf spark.driver.memoryOverhead=2g \
  my_job.py
```

**Option 3: Sample before collecting**
```python
# Collect a small sample for inspection
sample = df.sample(fraction=0.001).collect()
```

### Other Common Causes of Driver OOM

1. **Large broadcast joins**: `spark.sql.autoBroadcastJoinThreshold` defaults to 10MB. If you manually broadcast a large table:
   ```python
   from pyspark.sql.functions import broadcast
   df.join(broadcast(large_ref_df), "id")  # large_ref_df goes to driver first
   ```

2. **Long RDD lineage**: Hundreds of chained transformations build up DAG objects in driver heap. Fix: `df.cache()` or `df.checkpoint()` to truncate lineage.

3. **Driver-side aggregation**: Some operations like `countByValue()` return Python dicts to the driver:
   ```python
   # BAD — returns all counts to driver
   rdd.countByValue()
   
   # GOOD — stays distributed
   df.groupBy("col").count()
   ```

### Driver Memory Configuration Summary

```bash
spark-submit \
  --driver-memory 4g \                    # JVM heap for driver
  --conf spark.driver.memoryOverhead=1g \ # off-heap (native + NIO)
  --conf spark.driver.maxResultSize=2g \  # cap on .collect() result size
  my_job.py
```

`spark.driver.maxResultSize` is a safety net — it throws a controlled exception rather than letting the driver crash with OOM.

</details>

</article>

<article data-difficulty="mid">

## Scenario: Size the Executor Configuration for 500GB/hr on EMR r5.4xlarge

Your team needs to run a Spark job on Amazon EMR that processes **500GB of data per hour**. The cluster uses **r5.4xlarge** nodes (16 vCPU, 128GB RAM). You need to determine:

1. How many executors per node?
2. How many cores per executor?
3. How much memory per executor?
4. How many nodes do you need?

Show your arithmetic and explain the reasoning behind each decision.

<details>
<summary>✅ Solution</summary>

### Step 1: Subtract System Overhead Per Node

You cannot give Spark all 16 vCPU and 128GB — the OS, YARN NodeManager, and HDFS DataNode need headroom:

| Resource | Reserved | Available |
|---|---|---|
| vCPU | 1 (OS + NM) | **15 vCPU** |
| RAM | 8GB (OS + NM + overhead) | **120GB** |

### Step 2: Choose Cores Per Executor

**Rule of thumb: 4–5 cores per executor.**

- Too few (1–2): HDFS throughput is poor; each executor makes fewer parallel I/O calls.
- Too many (all 15): One executor per node → GC pauses halt all tasks; no CPU isolation.
- Sweet spot: **5 cores** per executor — well-established empirically for HDFS workloads.

### Step 3: Calculate Executors Per Node

```
15 vCPU available / 5 cores per executor = 3 executors per node
```

### Step 4: Calculate Memory Per Executor

```
120GB available / 3 executors = 40GB per executor
```

But we must also account for executor memory overhead (off-heap, NIO buffers):

```
spark.executor.memoryOverhead = max(384MB, 0.1 × executor.memory)
                               = max(384MB, 0.1 × 40GB) = 4GB
```

So the actual JVM heap per executor:

```
executor.memory = 40GB - 4GB overhead = 36GB
```

Final per-executor configuration:

```bash
--executor-cores 5
--executor-memory 36g
--conf spark.executor.memoryOverhead=4g
```

### Step 5: Determine Node Count for Throughput

Processing 500GB/hr = ~139MB/sec sustained read throughput.

r5.4xlarge with enhanced networking can sustain ~600MB/s EBS throughput and ~10Gbps network.

With **3 executors × 5 cores = 15 parallel tasks per node**, each task reading ~10MB partitions:

- Tasks complete ~every 2–5 seconds per stage
- A 10-node cluster = 150 concurrent tasks

For 500GB/hr (sustained), **8–10 nodes** is typically sufficient. Start with **8 nodes** and scale with EMR auto-scaling:

```bash
# EMR cluster sizing
aws emr create-cluster \
  --instance-type r5.4xlarge \
  --instance-count 9 \          # 1 master + 8 core nodes
  --configurations '[
    {"Classification":"spark-defaults","Properties":{
      "spark.executor.cores":"5",
      "spark.executor.memory":"36g",
      "spark.executor.memoryOverhead":"4g",
      "spark.executor.instances":"24",
      "spark.default.parallelism":"240",
      "spark.sql.shuffle.partitions":"240"
    }}
  ]'
```

### Step 6: Set Parallelism

```
Total executor slots = 8 nodes × 3 executors × 5 cores = 120 slots
spark.default.parallelism = 120 × 2 = 240    (2× slots is a good starting point)
spark.sql.shuffle.partitions = 240
```

### Summary

| Parameter | Value | Reasoning |
|---|---|---|
| `spark.executor.cores` | 5 | HDFS throughput sweet spot |
| `spark.executor.memory` | 36g | 40GB / node / 3 executors − overhead |
| `spark.executor.memoryOverhead` | 4g | 10% of 40GB |
| Executors per node | 3 | 15 cores / 5 |
| Node count | 8 core + 1 master | ~150 concurrent tasks for 500GB/hr |
| `spark.sql.shuffle.partitions` | 240 | 2× total slots |

One **driver** also runs on the master node — allocate 4–8GB separately:

```bash
--driver-memory 8g
--driver-cores 4
```

</details>

</article>

<article data-difficulty="senior">

## Scenario: Kafka → Stateful Aggregation → Delta Lake Job Crashes After 3 Days

A Spark Structured Streaming job has been running reliably for 3 days. It reads from Kafka, performs stateful windowed aggregation, and writes results to Delta Lake. Then it starts throwing `Lost task` errors and eventually fails. The job restarts from checkpoint but crashes again within hours.

Walk through your **systematic diagnosis**: DAG analysis, stage failure patterns, shuffle behavior, and GC pressure.

<details>
<summary>✅ Solution</summary>

### Phase 1: Gather Evidence Before Touching Anything

```bash
# 1. Capture the full stack trace — "Lost task" is vague
# Look for the underlying cause in the executor stderr logs:
# - FetchFailedException → shuffle fetch failed
# - ExecutorLostFailure → executor process died
# - TaskKilledException → speculative execution killed task
# - java.lang.OutOfMemoryError → heap or off-heap exhaustion

# In EMR/YARN — pull executor logs
yarn logs -applicationId application_XXX -containerId container_YYY > executor.log

# In Kubernetes
kubectl logs spark-app-XXX-exec-1 -n spark-namespace
```

### Phase 2: DAG and Stage Analysis via Spark UI

Even after a job fails, the Spark History Server retains the event log.

```
Spark UI → Stages tab → look for:
  - Which stage is failing? (note the stage ID)
  - Is it always the same stage? → points to data skew or bad state
  - What's the shuffle read/write size over time?
  - Are tasks in the failed stage disproportionately slow?
```

For a Kafka → stateful agg → Delta job, the DAG looks like:

```
Micro-batch N:
  Stage 1: Kafka source → deserialize → repartition by key
      ↓ [shuffle]
  Stage 2: StateStore read → window aggregation → StateStore write
      ↓ [no shuffle — each partition writes independently]
  Stage 3: Delta Lake sink (file commit)
```

**Key insight**: If Stage 2 is failing and it's been stable for 3 days, the most likely culprit is **state store growth**.

### Phase 3: Diagnose State Store Growth

Stateful aggregations keep state per key across micro-batches. With no watermark or an overly permissive watermark, state **grows unboundedly**:

```python
# PROBLEM: no watermark → state for every key kept forever
df.groupBy(
    window("event_time", "10 minutes"),
    "user_id"
).agg(count("*").alias("event_count"))

# FIX: add watermark to bound state
df.withWatermark("event_time", "30 minutes") \
  .groupBy(
      window("event_time", "10 minutes"),
      "user_id"
  ).agg(count("*").alias("event_count"))
```

After 3 days of accumulation, state files fill the executor local disk:

```bash
# Check state directory size
ls -lh /mnt/spark/state/0/default/
du -sh /mnt/spark/state/

# In Spark conf, locate state store path
spark.sql.streaming.checkpointLocation   # checkpoint root
# State files are at: <checkpoint>/state/
```

### Phase 4: Diagnose GC Pressure

Long-running streaming jobs accumulate object pressure. Signs in GC logs:

```bash
# Enable GC logging in spark-submit
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log"

# Symptoms of GC-related task loss:
# - Executor heartbeat timeout (spark.executor.heartbeatInterval default 10s)
# - "Executor heartbeat timed out after XXX ms" in driver logs
# - Tasks running >2x the median duration (GC pauses)
```

Recommended GC tuning for long-running streaming:

```bash
--conf "spark.executor.extraJavaOptions=
  -XX:+UseG1GC
  -XX:InitiatingHeapOccupancyPercent=35
  -XX:G1HeapRegionSize=16m
  -XX:MaxGCPauseMillis=500
  -XX:+ParallelRefProcEnabled"
```

### Phase 5: Diagnose Shuffle Fetch Failures

If Stage 2 shows `FetchFailedException`, a shuffle block from Stage 1 is unreachable:

```
org.apache.spark.shuffle.FetchFailedException: Failed to connect to /10.0.1.5:7337
```

Causes after 3 days of continuous running:
1. **Executor evicted** (spot instance, YARN container preemption, OOM kill)
2. **Local disk full** — shuffle files accumulate if not cleaned promptly
3. **Network instability** — transient but repeated

Fix — enable External Shuffle Service so shuffle data survives executor death:

```bash
# On YARN
spark.shuffle.service.enabled=true
spark.dynamicAllocation.enabled=true    # pair with ESS

# Tune shuffle file cleanup
spark.cleaner.referenceTracking.cleanCheckpoints=true
spark.sql.streaming.minBatchesToRetain=2   # keep last 2 batch checkpoints only
```

### Phase 6: Memory Pressure from Delta Lake Writes

Delta Lake uses optimistic concurrency and writes Parquet files, then updates the transaction log. Over time:

```python
# Transaction log (_delta_log/) grows — each batch adds 1 JSON file
# After 10 commits → checkpoint file created, old JSONs can be vacuumed

# Check transaction log size
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta_table/")
dt.history(10).show()

# Run VACUUM to remove old files (default 7-day retention)
dt.vacuum(168)   # 168 hours = 7 days

# OPTIMIZE to compact small files (run periodically, not in streaming job)
dt.optimize().executeCompaction()
```

### Phase 7: Full Resolution Checklist

```python
# 1. Add watermark to bound state size
streaming_df = (
    kafka_df
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        window("event_time", "10 minutes"),
        "user_id"
    )
    .agg(count("*").alias("event_count"))
)

# 2. Set checkpoint with minimal retention
query = (
    streaming_df.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://bucket/checkpoints/job1/")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .start("s3://bucket/output/")
)

# 3. Key Spark configs for stability
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "3")
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.executor.heartbeatInterval", "20s")
spark.conf.set("spark.network.timeout", "300s")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Summary Diagnosis Tree

```
"Lost task" after 3 days
├── Always same stage? → Data skew or unbounded state
│   └── Check: state store size, watermark config
├── FetchFailedException? → Shuffle data lost
│   └── Fix: External Shuffle Service, disk monitoring
├── Executor heartbeat timeout? → GC pressure or OOM
│   └── Fix: G1GC tuning, increase executor memory
└── Delta write failures? → Transaction log bloat
    └── Fix: VACUUM, OPTIMIZE, log cleanup
```

</details>

</article>
