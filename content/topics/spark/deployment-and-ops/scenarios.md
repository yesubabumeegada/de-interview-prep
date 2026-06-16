---
title: "Spark Deployment and Operations Interview Scenarios"
description: "Scenarios covering cluster deployment, resource management, monitoring, and production operations"
content_type: scenario_question
topic: spark
subtopic: deployment-and-ops
tags: [spark, yarn, kubernetes, emr, databricks, monitoring, ops, cluster-management]
---

<article data-difficulty="junior">

## Scenario: Client Mode vs Cluster Mode

What are the differences between Spark's client mode and cluster mode? When would you use each?

<details>
<summary>✅ Solution</summary>

### The Key Difference: Where the Driver Runs

In every Spark application, there is one **driver** (the JVM process that runs your `main()` function, creates the SparkContext, and coordinates tasks) and many **executors** (the JVM processes that run tasks).

The deployment mode controls **where the driver runs**:

| Mode | Driver Location | Executor Location |
|------|----------------|-------------------|
| **Client mode** | On the machine that submitted the job (your laptop, edge node, notebook server) | On cluster worker nodes |
| **Cluster mode** | On a worker node inside the cluster (managed by YARN/K8s/Mesos) | On cluster worker nodes |

### Client Mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \        # ← driver runs HERE on the submitting machine
  --executor-memory 8g \
  --num-executors 10 \
  my_job.py
```

**What happens:**
1. You run `spark-submit` on your edge node or laptop
2. The driver process starts on YOUR machine
3. The driver negotiates with the cluster manager to launch executors on worker nodes
4. All driver logs appear in your terminal in real time
5. If your machine disconnects, the job dies

**Use client mode when:**
- Developing and debugging interactively (Jupyter notebooks, `pyspark` shell)
- You need to see driver logs immediately in your terminal
- The job is short-lived and you'll stay connected
- You are using a Spark interactive shell (`spark-shell`, `pyspark`)

### Cluster Mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \       # ← driver runs on a worker node inside the cluster
  --executor-memory 8g \
  --num-executors 10 \
  my_job.py
```

**What happens:**
1. You run `spark-submit` — it submits the job and exits immediately
2. The cluster manager (YARN/K8s) picks a worker node and launches the driver there
3. Driver negotiates for executors on other worker nodes
4. You disconnect from your terminal — the job continues running
5. Logs are collected in the cluster's log aggregation system (YARN logs, S3, etc.)

**Use cluster mode when:**
- Running production/scheduled jobs (Airflow, cron, etc.)
- You cannot keep your terminal open for hours
- Running on a remote cluster (EMR, Dataproc, AKS)
- You want the driver close to the executors (reduces network latency for driver-executor communication)

### Network Implication

In **client mode**, the driver communicates with executors across whatever network sits between your machine and the cluster. If the driver is on your laptop and executors are in AWS, every shuffle result that is returned to the driver must travel over the internet — slow and unreliable.

In **cluster mode**, driver and executors are co-located in the same datacenter/VPC — fast, low-latency communication.

### Quick Decision Guide

```
Is this interactive? (notebook, debugging, shell)
  → Client mode

Is this a scheduled/production job?
  → Cluster mode

Will you disconnect before the job finishes?
  → Cluster mode (client mode job would die)

Do you need to see logs immediately?
  → Client mode (or set up log streaming from cluster)
```

</details>

</article>

<article data-difficulty="mid">

## Scenario: Deploying a Fault-Tolerant Spark App on Kubernetes with Spot Instances

You need to deploy a Spark application on Kubernetes that processes 10TB of daily data. The job should: use Spot/Preemptible instances for cost savings, checkpoint to S3 for fault tolerance, and auto-scale based on queue depth. Describe your architecture and key configuration.

<details>
<summary>✅ Solution</summary>

### Architecture Overview

```
[Airflow / Argo Workflow]
        ↓ triggers
[spark-submit → K8s API]
        ↓ creates
[Driver Pod] ─────────────────────────────────────┐
        ↓ requests                                 │
[K8s Cluster Autoscaler]                          │
        ↓ provisions                               │
[Spot Instance Node Pool]                         │
  ├── [Executor Pod 1]  ─── reads/writes ──→ [S3 / HDFS]
  ├── [Executor Pod 2]                            │
  └── [Executor Pod N]  ←──────────────────────────┘
```

### Part 1: Spot Instance Configuration

Spot/Preemptible instances can be reclaimed with 2 minutes notice. The key is to ensure **executors run on Spot** while the **driver runs on On-Demand** (driver loss = job loss).

**Node pool setup (AWS EKS example):**
```yaml
# On-demand node group — for the driver
nodeGroups:
  - name: spark-driver-od
    instanceType: m5.2xlarge
    labels:
      spark-role: driver
      lifecycle: on-demand
    taints:
      - key: spark-driver
        effect: NoSchedule

  # Spot node group — for executors
  - name: spark-executor-spot
    instanceTypes: [m5.4xlarge, m5a.4xlarge, m4.4xlarge]  # multiple types = more capacity
    spot: true
    labels:
      spark-role: executor
      lifecycle: spot
```

**spark-submit configuration:**
```bash
spark-submit \
  --master k8s://https://K8S_API_ENDPOINT \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-ecr/spark:3.4.0 \
  \
  # Driver on on-demand nodes
  --conf spark.kubernetes.driver.label.spark-role=driver \
  --conf spark.kubernetes.driver.node.selector.lifecycle=on-demand \
  --conf spark.kubernetes.driver.tolerations.key=spark-driver \
  --conf spark.kubernetes.driver.tolerations.effect=NoSchedule \
  \
  # Executors on Spot nodes
  --conf spark.kubernetes.executor.label.spark-role=executor \
  --conf spark.kubernetes.executor.node.selector.lifecycle=spot \
  \
  # Graceful handling of Spot interruptions
  --conf spark.task.maxFailures=8 \
  --conf spark.stage.maxConsecutiveAttempts=10 \
  my_job.py
```

### Part 2: Checkpointing to S3 for Fault Tolerance

When an executor is evicted (Spot interruption), Spark re-runs failed tasks from the last successful shuffle write. For long-running stages, configure checkpointing to persist intermediate results.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("daily-10tb-processor") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Set checkpoint directory on S3
spark.sparkContext.setCheckpointDir("s3://my-bucket/spark-checkpoints/")

# For RDD lineage that gets too long (risk of re-computation chain being huge):
rdd = spark.sparkContext.parallelize(data)
for i in range(100):
    rdd = rdd.map(some_expensive_transform)
    if i % 10 == 0:
        rdd.checkpoint()  # Materialize and save to S3
        rdd.count()        # Force checkpointing (action required)

# For DataFrames — use write + re-read pattern (more reliable than RDD checkpoint):
def checkpoint_df(df, path):
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)

stage1_result = checkpoint_df(
    df.filter(...).join(...),
    "s3://my-bucket/checkpoints/stage1/"
)
```

**S3 configuration for performance:**
```bash
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.multipart.size=128M \
--conf spark.hadoop.fs.s3a.connection.maximum=500 \
```

### Part 3: Auto-Scaling Based on Queue Depth

Spark on Kubernetes supports **Dynamic Resource Allocation (DRA)** — executors are added/removed based on pending tasks.

```bash
# Enable dynamic allocation
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \  # Required on K8s (no external shuffle service by default)
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.dynamicAllocation.initialExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=120s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=30s \
```

**How it works:**
- If tasks are queued for > `schedulerBacklogTimeout` (30s), Spark requests more executors from K8s
- K8s Cluster Autoscaler provisions new Spot nodes to accommodate the new executor pods
- If executors are idle for > `executorIdleTimeout` (120s), they are released and Spot nodes scale down

### Part 4: Handling Spot Interruptions Gracefully

AWS sends a 2-minute warning before reclaiming Spot instances. Configure the node termination handler:

```yaml
# Install AWS Node Termination Handler via Helm
helm install aws-node-termination-handler \
  eks/aws-node-termination-handler \
  --set nodeSelector."lifecycle"=spot \
  --set enableSpotInterruptionDraining=true \
  --set enableScheduledEventDraining=true
```

When a node is drained:
1. Running executor pods are evicted
2. Their tasks fail and are rescheduled on other executors
3. Spark retries the tasks (`spark.task.maxFailures=8`)
4. K8s Autoscaler provisions replacement nodes

### Complete Configuration Summary

```python
spark = SparkSession.builder \
    .appName("daily-10tb-spot") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "200") \
    .config("spark.task.maxFailures", "8") \
    .config("spark.stage.maxConsecutiveAttempts", "10") \
    .config("spark.kubernetes.executor.deleteOnTermination", "false") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("s3://my-bucket/spark-checkpoints/")
```

**Estimated cost savings:** Spot instances are typically 60-90% cheaper than On-Demand. For a 200-executor job at $0.50/executor-hour running 3 hours: On-Demand = $300, Spot ≈ $60-120.

</details>

</article>

<article data-difficulty="senior">

## Scenario: Investigating Random ExecutorLostFailure on YARN

A production Spark job on YARN runs successfully 90% of the time but randomly fails with "ExecutorLostFailure" on the remaining 10%. Failures occur at different stages each time. Design a comprehensive investigation and hardening plan covering: resource contention, network issues, Spot interruptions, GC tuning, and application-level retry logic.

<details>
<summary>✅ Solution</summary>

### Triage: Understanding ExecutorLostFailure

`ExecutorLostFailure` means the driver lost contact with an executor. The root cause is almost never Spark itself — it's the **environment** that killed or disconnected the executor process. The randomness (different stages, 10% failure rate) is the critical clue: deterministic bugs fail at the same place every time; environmental issues are random.

### Investigation Framework

#### Step 1: Collect Evidence First

Before making any changes, gather data across multiple runs:

```bash
# On YARN — collect executor exit reasons from the NodeManager logs
yarn logs -applicationId application_XXXX_XXXX | grep -E "ExecutorLostFailure|KILLED|EXIT|OOM|GC overhead"

# Check YARN Resource Manager for preemption events
yarn application -status application_XXXX_XXXX | grep -i "preempt\|kill"

# On the NodeManager that hosted the failed executor:
grep -E "killed|OOM|memory exceeded" /var/log/hadoop-yarn/yarn-yarn-nodemanager-*.log
```

```python
# In your Spark application — log executor metrics before failure
spark.sparkContext.addSparkListener(...)  # Custom listener for executor events

# Or check SparkUI History Server:
# /history/application_XXXX/executors — look at "Exit Reason" for each executor
```

#### Step 2: Root Cause Analysis

**Hypothesis 1: YARN Memory Limits (Most Common)**

YARN has its own memory limit per container (`yarn.nodemanager.resource.memory-mb`). If the executor JVM exceeds its allocation, YARN kills it with `KILLED` status — which appears as `ExecutorLostFailure` in Spark.

```bash
# Check if YARN overhead is set high enough
# Default: max(384MB, 10% of executor-memory)
# For memory-intensive jobs, increase it:

spark-submit \
  --executor-memory 8g \
  --conf spark.executor.memoryOverhead=2g \  # JVM non-heap: off-heap, thread stacks, NIO buffers
  ...
```

Signs this is the cause: failures during shuffle-heavy stages, `KILLED` in YARN logs (not `FAILED`).

**Hypothesis 2: GC Pauses Causing Heartbeat Timeouts**

If an executor's GC pause exceeds the heartbeat timeout, the driver marks the executor as lost.

```bash
# Check GC time in Spark UI → Executors tab
# If GC time > 5% of total task time, GC is a problem

# Check heartbeat timeout (default 120s)
spark.conf.set("spark.network.timeout", "600s")          # Driver-executor network timeout
spark.conf.set("spark.executor.heartbeatInterval", "30s") # Executor heartbeat frequency

# Fix GC — switch to G1GC with explicit region sizing
--conf "spark.executor.extraJavaOptions=\
  -XX:+UseG1GC \
  -XX:G1HeapRegionSize=32m \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+PrintGCDetails \
  -XX:+PrintGCDateStamps \
  -Xloggc:/tmp/gc.log"
```

Signs this is the cause: GC time spikes in Spark UI, executor timeout errors in driver logs.

**Hypothesis 3: Network Issues / Shuffle Fetch Failures**

Executors communicate over the network for shuffle reads. Transient network issues (packet loss, congestion, NIC errors) cause shuffle fetch failures that eventually trigger executor loss if retry limits are exceeded.

```bash
# Check network errors on nodes
netstat -s | grep -E "retransmit|error|failed"
dmesg | grep -E "eth0|NIC|timeout"

# Increase shuffle fetch retry tolerance
--conf spark.shuffle.io.maxRetries=10 \
--conf spark.shuffle.io.retryWait=10s \
--conf spark.shuffle.io.connectionTimeout=300s \
```

**Hypothesis 4: YARN Preemption**

If your cluster has capacity scheduling and a high-priority queue preempts your job's containers, executors are killed — appearing as `ExecutorLostFailure`.

```bash
# Check for preemption in RM logs
grep "preempt" /var/log/hadoop-yarn/yarn-yarn-resourcemanager-*.log | grep application_XXXX

# Fix: submit to a non-preemptible queue, or set preemption thresholds
# In yarn-site.xml:
# yarn.resourcemanager.scheduler.monitor.policies=
#   org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy
```

**Hypothesis 5: Spot Instance Interruptions**

If running on EC2 Spot instances with YARN, the node can disappear with 2-minute warning. YARN will try to reschedule, but if the driver also catches the eviction, it logs `ExecutorLostFailure`.

```bash
# Check instance termination notices:
curl http://169.254.169.254/latest/meta-data/spot/termination-time 2>/dev/null
# Returns timestamp if termination is imminent
```

### Step 3: Hardening Plan

**A. Application-Level Resilience**

```python
# Increase task retry limits
spark.conf.set("spark.task.maxFailures", "8")       # default 4
spark.conf.set("spark.stage.maxConsecutiveAttempts", "10")

# Speculative execution — runs a duplicate of slow tasks
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")  # task 1.5x slower than median → speculate
spark.conf.set("spark.speculation.quantile", "0.9")    # 90% of tasks must finish first

# Checkpoint long lineage to avoid full recomputation on failure
spark.sparkContext.setCheckpointDir("s3://my-bucket/checkpoints/")

# Persist DataFrames that are reused across stages
expensive_df = compute_expensive_transform(raw_df)
expensive_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
expensive_df.count()  # Force materialization
```

**B. Resource Configuration Hardening**

```bash
spark-submit \
  --executor-memory 16g \
  --conf spark.executor.memoryOverhead=4g \        # Generous overhead for NIO, off-heap
  --conf spark.memory.fraction=0.8 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.shuffle.io.retryWait=10s \
  --conf spark.blacklist.enabled=true \             # Blacklist consistently failing nodes
  --conf spark.blacklist.task.maxTaskAttemptsPerNode=4 \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:InitiatingHeapOccupancyPercent=35"
```

**C. YARN-Level Hardening**

```xml
<!-- yarn-site.xml — increase NM memory check interval -->
<property>
  <name>yarn.nodemanager.pmem-check-enabled</name>
  <value>false</value>  <!-- Physical memory check — can cause false kills on JVM -->
</property>
<property>
  <name>yarn.nodemanager.vmem-check-enabled</name>
  <value>false</value>  <!-- Virtual memory check — almost always too aggressive -->
</property>
```

**D. Monitoring and Alerting**

```python
# Add a custom SparkListener to track executor loss events
from pyspark import SparkContext
from pyspark.java_gateway import ensure_callback_server_started

# Use spark.conf to log metrics to CloudWatch / Datadog
spark.conf.set("spark.metrics.conf.driver.sink.csv.class",
               "org.apache.spark.metrics.sink.CsvSink")

# In production: use Spark History Server + Grafana dashboards
# Key metrics to alert on:
# - spark_executor_lost_total (per job)
# - spark_task_retries_total
# - spark_executor_gc_time_ms
```

### Investigation Decision Tree

```
ExecutorLostFailure observed
        ↓
Check YARN logs for exit reason
    ├── KILLED by NM → Memory limit exceeded → Increase memoryOverhead
    ├── PREEMPTED → Queue preemption → Switch queue or disable preemption
    └── FAILED → JVM crashed
              ↓
        Check GC logs
            ├── Long GC pauses > heartbeat interval → G1GC + increase timeouts
            └── OOM → Reduce data per task (more partitions) or increase heap
        Check network logs
            └── NIC errors / retransmits → Investigate network, increase retry settings
        Check cloud metadata
            └── Spot interruption → Handle with node termination handler + task retry
```

### Summary of Actions by Priority

| Priority | Action | Addresses |
|----------|--------|-----------|
| P0 | Increase `spark.task.maxFailures=8` | Immediate resilience |
| P0 | Add `spark.executor.memoryOverhead=4g` | YARN kill prevention |
| P1 | Enable G1GC with logging | GC-induced timeouts |
| P1 | Increase network/heartbeat timeouts | Transient network issues |
| P1 | Enable node blacklisting | Consistently bad nodes |
| P2 | Add checkpointing | Reduce recomputation cost |
| P2 | Enable speculative execution | Straggler/slow node detection |
| P3 | CloudWatch/Grafana dashboards | Ongoing visibility |

</details>

</article>
