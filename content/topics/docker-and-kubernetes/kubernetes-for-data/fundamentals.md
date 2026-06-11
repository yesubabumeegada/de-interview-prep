---
title: "Kubernetes for Data - Fundamentals"
topic: docker-and-kubernetes
subtopic: kubernetes-for-data
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker, kubernetes, kubernetes-for-data]
---

# Kubernetes for Data — Fundamentals

## The Elastic Data Center Analogy

Running Spark on Kubernetes is like having a data center that instantly spawns servers when a job arrives and deletes them when done — you pay only for what you use. Before K8s, running Spark required a static cluster (YARN/EMR) that ran 24/7 even when idle. K8s-native Spark spins up driver + executor pods for a job and terminates them on completion — cost and resource waste drop dramatically.

---

## Spark on Kubernetes

```bash
# Submit Spark job to K8s
spark-submit   --master k8s://https://k8s-api-endpoint:6443   --deploy-mode cluster   --name revenue-spark-job   --conf spark.executor.instances=5   --conf spark.executor.memory=8g   --conf spark.executor.cores=4   --conf spark.kubernetes.container.image=registry/spark:3.5.0   --conf spark.kubernetes.namespace=data-platform   --conf spark.kubernetes.driver.serviceAccount=spark-sa   local:///app/revenue_job.py
```

---

## Airflow KubernetesExecutor

```python
# Each task runs in its own pod — full isolation
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

spark_task = KubernetesPodOperator(
    task_id="run_spark_aggregation",
    image="registry/spark-job:abc1234",
    namespace="data-platform",
    service_account_name="spark-sa",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "2", "memory": "8Gi"},
        limits={"cpu": "4", "memory": "16Gi"},
    ),
    env_vars=[k8s.V1EnvVar(name="ENV", value="production")],
    is_delete_operator_pod=True,  # cleanup after completion
)
```

---

## Flink on Kubernetes

```yaml
# FlinkDeployment (Flink Kubernetes Operator)
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: revenue-streaming-job
spec:
  image: registry/flink-job:v1.18
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
  job:
    jarURI: local:///opt/flink/usrlib/revenue-streaming.jar
    parallelism: 8
    upgradeMode: stateless
```
