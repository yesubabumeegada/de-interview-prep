---
title: "Spark Deployment & Ops — Intermediate"
topic: spark
subtopic: deployment-and-ops
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, kubernetes, docker, helm, dynamic-allocation, resource-queues, monitoring, prometheus]
---

# Spark Deployment & Ops — Intermediate

## Spark on Kubernetes Deep Dive

Kubernetes is the modern standard for Spark deployment in cloud-native environments:

```bash
# spark-submit with K8s:
spark-submit \
  --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --name spark-etl \
  --conf spark.kubernetes.container.image=spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.executor.instances=10 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=16g \
  --conf spark.kubernetes.executor.request.cores=2 \  # request < limit for burstability
  --conf spark.kubernetes.executor.limit.cores=4 \
  --conf spark.kubernetes.driver.request.cores=1 \
  --conf spark.kubernetes.driver.limit.cores=4 \
  local:///opt/spark/examples/jars/spark-examples.jar 1000
```

**K8s resource model:**
```yaml
# Spark executor pod template:
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: spark-executor
    resources:
      requests:
        cpu: "2"
        memory: "18Gi"    # executor.memory (16g) + overhead (2g)
      limits:
        cpu: "4"
        memory: "18Gi"   # OOM kill if exceeded!
```

---

## Shuffle Service on Kubernetes

On YARN, External Shuffle Service is a daemon on each node. On K8s, node daemons are impractical — use a Remote Shuffle Service:

**Option 1: Spark's built-in shuffle file consolidation (simple)**
```python
# Disable dynamic allocation (no ESS on K8s by default)
spark.conf.set("spark.dynamicAllocation.enabled", "false")
# Use fixed executor count instead
```

**Option 2: Remote Shuffle Service (production scale)**
```bash
# Popular options:
# - Apache Celeborn (formerly RSS from ByteDance) — open source
# - Uniffle (from Tencent) — open source
# - Uber's Cosco — internal
# - Google Cloud Dataproc Shuffle — managed

# Celeborn configuration:
spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
spark.celeborn.master.endpoints=celeborn-master:9097
spark.celeborn.shuffle.chunk.size=8m
```

---

## YARN Queue Management

Production YARN clusters use queue hierarchies to share resources fairly:

```bash
# Fair Scheduler with multiple queues
# capacity-scheduler.xml or fair-scheduler.xml

# Submit to a specific queue:
spark-submit \
  --master yarn \
  --conf spark.yarn.queue=data-engineering \
  job.py

# YARN queue properties:
# minimum-capacity: guaranteed capacity even under load
# maximum-capacity: can use up to this when others don't need it
# user-limit-factor: max fraction of queue one user can use
```

```python
# Check if job is queued vs running:
import subprocess
result = subprocess.run(
    ["yarn", "application", "-status", "application_123_001"],
    capture_output=True, text=True)
# Look for: Application-State, Progress, Tracking-URL
```

---

## Dynamic Allocation Best Practices

```python
# Proper dynamic allocation config:
spark = SparkSession.builder \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "120s") \   # scale down after 2 min idle
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "5s") \  # scale up if backlog > 5s
    .config("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "5s") \
    .config("spark.shuffle.service.enabled", "true") \   # required on YARN!
    .getOrCreate()
```

**When to disable dynamic allocation:**
- Structured Streaming (needs stable executor count for consistent latency)
- Very short jobs (< 30 seconds — allocation overhead dominates)
- Jobs with large shuffles already cached by executors

---

## Prometheus + Grafana Monitoring

```python
# Enable Spark metrics to Prometheus:
spark = SparkSession.builder \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.class",
            "org.apache.spark.metrics.sink.PrometheusServlet") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.path",
            "/metrics/prometheus") \
    .getOrCreate()

# Metrics available at: http://driver:4040/metrics/prometheus
# Key metrics:
# spark_executor_cpuTime_count     — CPU time (not wall time)
# spark_executor_runTime_count     — task run time
# spark_executor_gcTime_count      — GC time
# spark_executor_memoryUsed_bytes  — memory in use
# spark_executor_diskBytesSpilled  — spill
# spark_streaming_lastCompletedBatch_processingDelay  — streaming lag
```

```yaml
# Grafana dashboard queries (PromQL):
# GC time ratio:
rate(spark_executor_gcTime_count[5m]) / rate(spark_executor_runTime_count[5m])

# Shuffle spill rate:
rate(spark_executor_diskBytesSpilled[5m])

# Streaming processing delay:
spark_streaming_lastCompletedBatch_processingDelay
```

---

## Docker Image for Spark

```dockerfile
# Spark Docker image with Python dependencies:
FROM apache/spark:3.5.0-python3

USER root
# Install Python dependencies
COPY requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

# Add your application
COPY jobs/ /opt/spark/jobs/

# Security: run as non-root
USER spark
```

```bash
# Build and push:
docker build -t my-registry/spark-etl:v1.2.3 .
docker push my-registry/spark-etl:v1.2.3

# Use in spark-submit:
spark-submit \
  --conf spark.kubernetes.container.image=my-registry/spark-etl:v1.2.3 \
  ...
```

---

## Spark Operator for Kubernetes (Kubeflow/SparkOperator)

Declarative Spark job management via Kubernetes CRDs:

```yaml
# SparkApplication CRD
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-etl
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: my-registry/spark-etl:v1.2.3
  mainApplicationFile: local:///opt/spark/jobs/daily_etl.py
  sparkVersion: "3.5.0"
  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark
  executor:
    cores: 4
    instances: 10
    memory: "16g"
    memoryOverhead: "2g"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
```

```bash
kubectl apply -f spark-job.yaml
kubectl get sparkapplications -n spark-jobs
kubectl describe sparkapplication daily-etl -n spark-jobs
```

---

## Interview Tips

> **Tip 1:** "How is Spark on Kubernetes different from Spark on YARN?" — On YARN: Executors are YARN containers managed by ResourceManager; External Shuffle Service is a node daemon that survives executor termination; dynamic allocation works out of the box. On K8s: Executors are Kubernetes Pods; no built-in shuffle service (need Remote Shuffle Service like Celeborn for dynamic allocation); Driver is also a Pod with a service for executor registration. K8s offers better isolation (container images), easier cloud integration, and GitOps-friendly config, but requires more setup for shuffle and dynamic allocation.

> **Tip 2:** "What is the Spark Operator and why would you use it?" — The Spark Operator is a Kubernetes operator that manages Spark jobs as Kubernetes custom resources (CRDs). Instead of imperative `spark-submit` calls, you declare the job as YAML and apply it with `kubectl`. Benefits: GitOps workflow (job configs in version control), automatic retry on failure, job history and status via `kubectl`, integration with K8s RBAC for access control, and easier integration with Argo/Airflow for orchestration.

> **Tip 3:** "Why does Spark need memoryOverhead on containers?" — The JVM heap (executor.memory) is not the only memory a container uses. Python worker processes (PySpark), native code for compression (Snappy/LZ4/Zstd), direct ByteBuffers, off-heap memory, and OS overhead all sit outside the JVM heap. YARN/K8s enforce container memory limits strictly — if total memory (heap + overhead) exceeds the container limit, the container is killed with no useful error. Set `executor.memoryOverhead` to at least 10% of executor memory, more for Python-heavy workloads (add 1-2 GB for PySpark).
