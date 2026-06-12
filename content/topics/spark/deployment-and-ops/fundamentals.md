---
title: "Spark Deployment & Ops — Fundamentals"
topic: spark
subtopic: deployment-and-ops
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, deployment, spark-submit, yarn, kubernetes, standalone, spark-ui, logs]
---

# Spark Deployment & Ops — Fundamentals

## 🎯 Analogy

Deploying Spark is like running a food delivery operation. spark-submit is the order dispatch system. The cluster manager (YARN/K8s) is the logistics coordinator that assigns delivery drivers (executors) to orders (jobs). The Spark UI is your operations dashboard showing real-time delivery status.

---

## spark-submit: The Deployment Command

```bash
spark-submit \
  --master yarn \                          # cluster manager
  --deploy-mode cluster \                  # run driver on cluster (prod)
  --name "Daily ETL Pipeline" \
  --num-executors 20 \                     # total executors
  --executor-cores 4 \                     # cores per executor
  --executor-memory 16g \                  # heap per executor
  --driver-memory 8g \                     # driver heap
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --py-files dependencies.zip \            # Python dependencies
  --jars extra-lib.jar \                   # extra JARs
  main_job.py \                            # your script
  --input s3://bucket/raw/ \              # script arguments
  --output s3://bucket/processed/
```

---

## Cluster Managers

**Standalone (simple clusters):**
```bash
# Start master
$SPARK_HOME/sbin/start-master.sh
# Start workers
$SPARK_HOME/sbin/start-worker.sh spark://master:7077

# Submit
spark-submit --master spark://master:7077 job.py
```

**YARN (Hadoop clusters):**
```bash
# Deploy modes:
spark-submit --master yarn --deploy-mode client  job.py  # driver on edge node
spark-submit --master yarn --deploy-mode cluster job.py  # driver on YARN container

# YARN allocates containers from ResourceManager
# Each executor runs in its own YARN container
```

**Kubernetes:**
```bash
spark-submit \
  --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-registry/spark:3.5 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  local:///opt/spark/jobs/main.py
```

---

## spark-defaults.conf: Persistent Configuration

```bash
# $SPARK_HOME/conf/spark-defaults.conf
spark.master                      yarn
spark.executor.memory             8g
spark.executor.cores              4
spark.driver.memory               4g
spark.sql.adaptive.enabled        true
spark.serializer                  org.apache.spark.serializer.KryoSerializer
spark.sql.shuffle.partitions      200
spark.eventLog.enabled            true
spark.eventLog.dir                hdfs:///spark-logs/
```

---

## Spark UI: Your Debugging Dashboard

The Spark UI runs on port 4040 of the Driver (client mode) or accessible via cluster proxy (cluster mode):

```
Tab Overview:
├── Jobs         — List of all jobs, status, duration
│   └── Click a job → see its stages
├── Stages       — All stages, per-stage metrics
│   ├── Task duration timeline (spot skew visually)
│   ├── GC time, shuffle read/write, spill
│   └── Click a stage → per-task breakdown
├── Storage      — Cached RDDs/DataFrames, hit rate
├── Environment  — All config values (great for debugging config issues)
├── Executors    — Per-executor: cores, memory, GC%, tasks, spill
├── SQL          — Physical plans, per-operator row counts and timing
└── JDBC/ODBC    — (if Thrift server running)
```

```python
# Programmatic access:
sc = spark.sparkContext
print(f"Spark UI: {sc.uiWebUrl}")   # e.g., http://localhost:4040

# History Server: access completed job UIs
# Configure: spark.eventLog.enabled=true, spark.eventLog.dir=hdfs:///spark-logs/
# Start: $SPARK_HOME/sbin/start-history-server.sh
# Access: http://history-server:18080
```

---

## Reading Logs

```bash
# Application logs location:
# YARN: yarn logs -applicationId application_TIMESTAMP_ID
# K8s: kubectl logs spark-job-driver -n spark-jobs
# Standalone: worker logs in $SPARK_HOME/logs/

# Executor logs:
yarn logs -applicationId application_1234567890_0042 -containerId container_xxx

# Grep for common error patterns:
grep "ERROR\|Exception\|WARN\|Killed" application.log | grep -v "SparkUI"

# Key error types:
# OutOfMemoryError: Java heap space  → executor memory too small
# ExecutorLostFailure                 → executor crashed (check GC time)
# FetchFailedException                → shuffle fetch failed (network/executor death)
# FileNotFoundException               → input path wrong or not accessible
# AnalysisException                   → SQL/schema error (caught early)
```

---

## Common Deployment Mistakes

| Mistake | Symptom | Fix |
|---|---|---|
| Too many executor cores | Poor HDFS throughput, contention | Use 4-5 cores/executor |
| No `--deploy-mode cluster` in prod | Job fails when client disconnects | Always use cluster mode for prod |
| Small driver memory | Driver OOM on collect/broadcast | Scale driver memory with join sizes |
| Missing `memoryOverhead` | Container killed by YARN | Add 10-15% overhead for native/Python |
| executor.instances too high | YARN queue starved | Use dynamic allocation instead |

---

## ▶️ Try It Yourself

```bash
# Run locally with all available cores
spark-submit \
  --master local[*] \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.enabled=true \
  my_job.py

# Open http://localhost:4040 while running to see the Spark UI
```

```python
# Check configuration at runtime:
for key, val in spark.sparkContext.getConf().getAll():
    print(f"{key} = {val}")
```

> **Run it:** Works locally — open http://localhost:4040 after submitting to see UI.

---

## Interview Tips

> **Tip 1:** "What is the difference between client and cluster deploy mode in spark-submit?" — Client mode runs the Driver process on the machine running spark-submit. Output goes to the local terminal, easy for development. If the machine disconnects, the job fails. Cluster mode runs the Driver inside the cluster (YARN container or K8s pod) — resilient to the submitting machine dying, logs are on the cluster. Always use cluster mode for production scheduled jobs.

> **Tip 2:** "How do you monitor a Spark job in production?" — Spark UI (port 4040 or History Server) is the primary tool: check job/stage/task timing, GC%, spill, and shuffle bytes. For automated monitoring: enable `spark.eventLog.enabled`, ship events to the History Server, and use Spark's metrics system to push to Prometheus/Grafana. Key metrics to alert on: job failure, stage duration > SLA, executor GC time > 10%, and Kafka consumer lag for streaming.

> **Tip 3:** "What is memoryOverhead and when do you need to set it?" — `memoryOverhead` is additional memory allocated per executor container outside the JVM heap. It covers: Python worker processes (PySpark), native code (Snappy/LZ4 decompression), off-heap memory, and container OS overhead. Default is 10% of executor memory (minimum 384MB). If containers are being killed by YARN/K8s with "Container killed by YARN for exceeding memory limits" — increase memoryOverhead by 1-2 GB.
