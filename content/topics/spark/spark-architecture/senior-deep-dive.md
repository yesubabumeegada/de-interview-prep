---
title: "Spark Architecture — Senior Deep Dive"
topic: spark
subtopic: spark-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, architecture, task-scheduler, barrier-mode, shuffle-service, external-shuffle, columnar, arrow]
---

# Spark Architecture — Senior Deep Dive

## The Full Scheduling Stack

Spark has three scheduling layers — understanding all three is essential for diagnosing production issues:

```
Application Layer
  SparkSession / SparkContext
      ↓
DAGScheduler
  • Builds DAG from RDD/DataFrame lineage
  • Identifies stage boundaries (wide deps → shuffle)
  • Submits stage TaskSets
      ↓
TaskScheduler  (TaskSchedulerImpl)
  • Assigns tasks to executor slots using delay scheduling
  • Handles retries (maxTaskFailures attempts)
  • Reports task status back to DAGScheduler
      ↓
SchedulerBackend  (CoarseGrainedSchedulerBackend)
  • Talks to cluster manager (YARN, K8s, Standalone)
  • Manages executor registration and heartbeats
  • Serializes/deserializes tasks over RPC
```

```python
# Delay scheduling: Spark prefers data-local tasks
# Locality levels, tried in order:
# PROCESS_LOCAL  → data in executor's JVM memory (cache hit)
# NODE_LOCAL     → data on same physical node (local disk or same container)
# RACK_LOCAL     → same network rack (faster than cross-rack)
# ANY            → any executor (cross-rack shuffle)

# Tune locality wait — how long to wait for a better-locality slot
spark.conf.set("spark.locality.wait", "3s")         # per level
spark.conf.set("spark.locality.wait.node", "1s")    # wait for node-local
spark.conf.set("spark.locality.wait.rack", "2s")    # wait for rack-local
```

---

## External Shuffle Service

By default, shuffle files live in executor JVM processes. When executors die during dynamic allocation, their shuffle data is lost and stages must restart.

**External Shuffle Service (ESS)** solves this by storing shuffle files in a separate long-lived process on each worker:

```
Without ESS:                    With ESS:
Executor → shuffle file         Executor → shuffle file
[Executor dies]                 [Executor dies — ESS still running]
← stage restart needed →        Stage 2 fetches from ESS ← no restart
```

```bash
# Enable in spark-defaults.conf
spark.shuffle.service.enabled=true
spark.shuffle.service.port=7337
spark.dynamicAllocation.enabled=true  # ESS is required for dynamic allocation
```

On Kubernetes, ESS is replaced by **shuffle file consolidation** or **Remote Shuffle Services** (RSS) like Uniffle or Celeborn — covered in the deployment section.

---

## Barrier Mode Execution

Normal Spark tasks can run at different times — a stage's tasks start as slots become available. Some ML workloads (distributed training, gang-scheduled MPI-like jobs) require **all tasks to start simultaneously**:

```python
from pyspark import BarrierTaskContext

def distributed_train(records):
    ctx = BarrierTaskContext.get()
    # All tasks synchronize here — none proceed until all have arrived
    ctx.barrier()
    rank = ctx.partitionId()
    # Do distributed training with rank coordination
    ...

rdd.barrier().mapPartitions(distributed_train)
```

Barrier mode guarantees:
- All tasks in a stage start at the same time
- If any task fails, all tasks in the stage restart together
- Tasks can exchange info via `allGather()` without a shuffle

---

## Columnar Execution and Apache Arrow

Spark 2.x processed data row-by-row in the JVM. Spark 3.x introduced **columnar vectors** and **Apache Arrow** for in-memory format:

```python
# Columnar batch processing via Pandas UDFs (Arrow-backed)
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def normalize(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std()

# Arrow transfers entire column batches — 10-100× faster than row-by-row UDFs
df.withColumn("norm_revenue", normalize(df.revenue))
```

```python
# Enable Arrow optimization for toPandas() / createDataFrame()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

# Measured speedup: toPandas() 10-50× faster with Arrow enabled
pdf = df.toPandas()
```

**Columnar storage advantages:**
- Better CPU cache utilization (access one column at a time)
- SIMD vectorization — CPU processes 8-16 values in one instruction
- Better compression (same-type values compress better)

---

## Task Serialization and Closure Capture

The Driver serializes tasks (your lambda + closure) and sends them to executors. A common source of bugs:

```python
# BAD: closure captures the entire SparkSession (not serializable)
def transform(row):
    return spark.sql(...)   # SparkSession captured in closure!

# ALSO BAD: closure captures a large Python object
model = load_large_model()  # 500MB
df.map(lambda row: model.predict(row))  # 500MB sent to EVERY executor!

# GOOD: broadcast large objects explicitly
model_broadcast = spark.sparkContext.broadcast(load_large_model())
def predict(row):
    return model_broadcast.value.predict(row)   # fetched once per executor
```

```python
# Check closure size — large closures slow down scheduling
import pickle
closure = your_function.__closure__
if closure:
    size = sum(len(pickle.dumps(c.cell_contents)) for c in closure)
    print(f"Closure size: {size / 1024:.1f} KB")
```

---

## Fault Tolerance Mechanisms

| Failure Type | Recovery Mechanism | Cost |
|---|---|---|
| Task failure | Retry on another executor (up to `maxTaskFailures=4`) | Re-run single partition |
| Executor failure | Stage retry (lost shuffle data), executor replaced | Re-run shuffle stage |
| Driver failure | Job fails (unless checkpointing or Spark on K8s) | Full restart |
| Fetch failure | Stage resubmit (shuffle data lost) | Re-run upstream stage |

```python
# Increase task retry tolerance for unreliable clusters
spark.conf.set("spark.task.maxFailures", "4")    # default
spark.conf.set("spark.stage.maxConsecutiveAttempts", "4")  # abort stage after N failures

# Checkpoint to HDFS to truncate lineage for long-running streaming
ssc.checkpoint("hdfs:///checkpoint/")
```

---

## RPC and Heartbeat Architecture

All Spark inter-process communication goes over **Netty-based RPC** (replaced Akka in Spark 2.0):

```
Driver ←→ Executor: task launch, task status, metrics
Driver ←→ ClusterManager: resource requests, executor registration
Executor ←→ Executor: shuffle fetch (HTTP, not RPC)

Heartbeat: Executor → Driver every spark.executor.heartbeatInterval (default 10s)
  If heartbeat missed: executor assumed dead after spark.network.timeout (default 120s)
```

Common production issue: long GC pauses on executor cause missed heartbeats → executor marked dead → stage resubmitted → GC-induced cascade failures.

```python
# Tune to tolerate longer GC pauses
spark.conf.set("spark.network.timeout", "300s")
spark.conf.set("spark.executor.heartbeatInterval", "20s")
```

---

## Interview Tips

> **Tip 1:** "Walk me through what happens when you call .show() on a DataFrame." — The Driver's DAGScheduler analyzes the RDD lineage, splits it into stages at shuffle boundaries, serializes tasks, and submits them to the TaskScheduler. TaskScheduler finds executor slots (preferring data-local placement), sends tasks via Netty RPC. Executors deserialize tasks, execute against their partition data, and return results. The Driver collects final rows from the last stage and formats the table.

> **Tip 2:** "How does Spark handle executor failure?" — If a task fails, it's retried up to `maxTaskFailures` times on other executors. If shuffle data produced by a failed executor is needed, the upstream stage is re-submitted. The Driver continuously tracks stage dependencies and can rebuild any lost output by re-executing lineage — no data is permanently lost as long as the source is available.

> **Tip 3:** "What is the External Shuffle Service and when do you need it?" — ESS decouples shuffle file storage from executor JVM processes. It's required for dynamic allocation — without ESS, scaling down an executor destroys its shuffle files, forcing stage restarts. On YARN/Standalone, ESS is a sidecar daemon on each worker. On Kubernetes, you typically use a Remote Shuffle Service (Celeborn, Uniffle) instead, since ESS requires a node-level process that conflicts with K8s pod model.
