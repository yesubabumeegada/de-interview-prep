---
title: "Spark Deployment & Ops — Senior Deep Dive"
topic: spark
subtopic: deployment-and-ops
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, deployment, spot-instances, graceful-decommission, cost-optimization, multi-tenant, security]
---

# Spark Deployment & Ops — Senior Deep Dive

## Spot/Preemptible Instance Strategies

Spot instances (AWS) / Preemptible VMs (GCP) can reduce EMR/Dataproc costs by 60-80%:

```python
# Strategy: On-Demand for Driver + core executors; Spot for task nodes
# AWS EMR instance groups:
# - MASTER: 1× r5.2xlarge On-Demand (driver)
# - CORE: 4× r5.4xlarge On-Demand (stable, store HDFS data)
# - TASK: 50× r5.4xlarge Spot (cheap, no HDFS data, replaceable)

# Spark configuration for Spot tolerance:
spark.conf.set("spark.task.maxFailures", "10")           # more retries
spark.conf.set("spark.stage.maxConsecutiveAttempts", "8")
spark.conf.set("spark.blacklist.enabled", "true")         # avoid failed hosts
spark.conf.set("spark.blacklist.task.maxTaskAttemptsPerNode", "4")
```

**Graceful decommissioning:**
```python
# Spark (3.1+) can decommission executors gracefully:
# - Finish running tasks
# - Migrate shuffle blocks to surviving executors
# - Then terminate

spark.conf.set("spark.decommission.enabled", "true")
spark.conf.set("spark.storage.decommission.enabled", "true")
spark.conf.set("spark.storage.decommission.shuffleBlocks.enabled", "true")
spark.conf.set("spark.storage.decommission.replicationReattemptInterval", "30s")

# On AWS EMR: use instance refresh / scale-in protection on Core nodes
# On GCP Dataproc: graceful decommission built in
```

---

## Multi-Tenant Cluster Management

Running multiple teams' Spark jobs on shared infrastructure:

```python
# Namespace-level resource quotas (Kubernetes):
# kubectl create namespace team-a
# kubectl apply -f:
"""
apiVersion: v1
kind: ResourceQuota
metadata:
  name: team-a-quota
  namespace: team-a
spec:
  hard:
    requests.cpu: "200"
    requests.memory: 800Gi
    limits.cpu: "400"
    limits.memory: 1600Gi
    count/pods: "200"
"""

# Fair scheduler pools (YARN):
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "team-a-high")

# Priority classes (K8s):
spark.conf.set("spark.kubernetes.driver.podTemplateFile", "priority-pod-template.yaml")
# priority-pod-template.yaml sets priorityClassName: high-priority
```

---

## Security: Encryption and Authentication

```python
# 1. In-transit encryption (shuffle data encrypted)
spark.conf.set("spark.authenticate", "true")
spark.conf.set("spark.authenticate.secret", "changeme123")
spark.conf.set("spark.network.crypto.enabled", "true")
spark.conf.set("spark.network.crypto.keyLength", "256")

# 2. At-rest encryption (local temp/shuffle files)
spark.conf.set("spark.io.encryption.enabled", "true")
spark.conf.set("spark.io.encryption.keySizeBits", "256")

# 3. SSL for Spark UI and REST API
spark.conf.set("spark.ssl.enabled", "true")
spark.conf.set("spark.ssl.keyStore", "/path/to/keystore.jks")
spark.conf.set("spark.ssl.keyStorePassword", "${KEY_STORE_PASS}")

# 4. Kerberos authentication (HDFS/Hive on-prem)
spark.conf.set("spark.yarn.principal", "spark@REALM.COM")
spark.conf.set("spark.yarn.keytab", "/etc/security/keytabs/spark.keytab")

# 5. Credential management (avoid passwords in configs)
# Use AWS Secrets Manager / HashiCorp Vault:
import boto3
secret = boto3.client("secretsmanager").get_secret_value(
    SecretId="prod/spark/jdbc-creds")
spark.conf.set("spark.sql.jdbc.password", secret["SecretString"])
```

---

## Cost Optimization Framework

```python
# Estimate job cost before running:
# On AWS: instance_cost = (executor_count × instance_cost/hr) × (duration_hrs)

# Optimization levers by impact:
"""
Tier 1: Spot instances         → 60-80% cost reduction
Tier 2: Right-sizing executors → 20-40% (match memory to actual usage)
Tier 3: Efficient file formats → 10-30% (Parquet vs CSV)
Tier 4: Partition pruning      → 10-50% (read less data)
Tier 5: Caching hot datasets   → 5-20% (avoid re-read)
"""

# Track actual cost per job with tags:
spark.conf.set("spark.yarn.tags", "team=data-eng,project=revenue,env=prod")
# AWS: use EMR tagging, Cost Explorer to drill down

# Autoscale-aware shuffle:
# With Celeborn RSS: executors can terminate after stage (shuffle not lost)
# → Can use more aggressive scale-down policies
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "30s")  # aggressive scale-down
```

---

## Logging Best Practices

```python
# Structured logging from Spark applications:
import logging
import json

class SparkJobLogger:
    def __init__(self, job_name, run_id):
        self.job_name = job_name
        self.run_id = run_id
        self.logger = logging.getLogger(job_name)

    def log_stage_complete(self, stage_name, rows, duration_ms):
        self.logger.info(json.dumps({
            "event": "stage_complete",
            "job": self.job_name,
            "run_id": self.run_id,
            "stage": stage_name,
            "rows_processed": rows,
            "duration_ms": duration_ms,
            "timestamp": datetime.utcnow().isoformat()
        }))

# Configure log4j for Spark:
# log4j2.properties:
"""
rootLogger.level = WARN
logger.SparkContext.name = org.apache.spark.SparkContext
logger.SparkContext.level = INFO
appender.console.type = Console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{ISO8601} %-5p %c{1}: %m%n
"""
```

---

## Interview Tips

> **Tip 1:** "How do you safely use Spot instances for Spark?" — Run the Driver and HDFS/shuffle-data core nodes on On-Demand; run task nodes on Spot. Enable Spark's graceful decommissioning so Spot-interrupted executors migrate their shuffle blocks before terminating. Increase task retry limits. Use External Shuffle Service on YARN (or Remote Shuffle Service on K8s) so executor termination doesn't invalidate shuffle data. Monitor spot interruption signals via instance metadata and pre-migrate where possible.

> **Tip 2:** "How do you handle Spark security in a production multi-tenant cluster?" — Enable cluster-level authentication (`spark.authenticate`) with per-application secrets. Encrypt shuffle data in transit (`spark.network.crypto.enabled`). Use RBAC at the cluster manager level (YARN queues or K8s namespaces with resource quotas). Never hardcode credentials in spark-submit or config files — fetch from secrets management (AWS Secrets Manager, Vault) at runtime. Enable Kerberos for on-prem HDFS access.

> **Tip 3:** "How do you right-size executors for a production job?" — Start with 4 cores / 16 GB memory per executor. Run the job, then check Spark UI Executors tab: if `Memory Used / Memory Available` is consistently < 50%, reduce memory; if > 80%, increase it. Check GC time: > 5% means memory pressure. Check peak shuffle spill: any spill means memory is too small for the data being processed. Adjust executor memory in 2 GB increments until GC < 2% and zero spill.
