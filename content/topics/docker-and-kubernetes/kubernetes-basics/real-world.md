---
title: "Kubernetes Basics - Real World"
topic: docker-and-kubernetes
subtopic: kubernetes-basics
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [kubernetes, k8s, real-world, data-engineering, airflow]
---

# Kubernetes Basics — Real World

## Case Study: Migrating Airflow to KubernetesExecutor

### Background

A fintech company ran Airflow on a single large EC2 instance (16 CPU, 64 GB RAM). With 150 daily DAG runs and tasks requiring different resources (some needing 16GB for Spark, others needing only 512MB for API calls), the single machine was either overloaded during peaks or idle during off-hours.

### The Problem

- Monday morning: 45 tasks queue behind each other on 16 CPUs — 3-hour delays
- Off-peak hours: 90% of the 64 GB machine sits idle
- Resource contention: a memory-hungry Spark task OOM-killed lighter tasks
- Dependency conflicts: tasks needing pandas 1.5 conflicted with tasks needing pandas 2.0

### The KubernetesExecutor Solution

```yaml
# Airflow Helm values (abbreviated)
# values.yaml
executor: KubernetesExecutor

workers:
  # Each task gets its own pod — no shared worker queue
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 1Gi

# Per-task resource overrides in DAG:
```

```python
# DAG: task-level resource requests
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

spark_task = KubernetesPodOperator(
    task_id="run_spark_aggregation",
    image="registry/spark-job:abc1234",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "4", "memory": "16Gi"},
        limits={"cpu": "8", "memory": "24Gi"},
    ),
)

api_task = KubernetesPodOperator(
    task_id="call_external_api",
    image="registry/api-caller:def5678",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "256Mi"},
    ),
)
```

### Results After 3 Months

| Metric | EC2 Single VM | Kubernetes |
|---|---|---|
| Monday peak delay | 3 hours | 0 (parallel pods) |
| Cost (normalized) | $2,100/mo | $900/mo (scale to zero) |
| Dependency conflicts | Monthly | None (isolated pods) |
| Task isolation | None | Full (pod per task) |
| Resource utilization | 15% average | 70% average |

**The unexpected win:** Resource utilization jumped from 15% to 70% because K8s bin-packs tasks across nodes. The EC2 instance was always reserved whether or not tasks ran.

**Cost reduction:** Scale-to-zero for overnight periods when no DAGs run. With EC2, the VM ran 24/7.
