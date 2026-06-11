---
title: "Kubernetes for Data - Intermediate"
topic: docker-and-kubernetes
subtopic: kubernetes-for-data
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker, kubernetes, kubernetes-for-data]
---

# Kubernetes for Data — Intermediate

## Spark Resource Tuning on K8s

```python
# Dynamic resource allocation: executors scale with data
spark = SparkSession.builder     .config("spark.dynamicAllocation.enabled", "true")     .config("spark.dynamicAllocation.minExecutors", "1")     .config("spark.dynamicAllocation.maxExecutors", "20")     .config("spark.dynamicAllocation.executorIdleTimeout", "60s")     .config("spark.kubernetes.executor.deleteOnTermination", "true")     .getOrCreate()
```

## Persistent Storage for Stateful Jobs

```yaml
# PVC for Spark shuffle data (faster than S3 for large shuffles)
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: spark-shuffle-pvc
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: gp3
  resources:
    requests:
      storage: 500Gi
```

## Node Affinity for Data Workloads

```yaml
# Spark executors on memory-optimized nodes
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node.kubernetes.io/instance-type
                operator: In
                values: [r5.4xlarge, r5.8xlarge]  # memory-optimized
  tolerations:
    - key: "workload"
      value: "spark"
      effect: "NoSchedule"
```

## Monitoring Spark Jobs on K8s

```bash
# View Spark UI for running job
kubectl port-forward <driver-pod> 4040:4040 -n data-platform
# Open http://localhost:4040

# Check executor pods
kubectl get pods -l spark-role=executor -n data-platform

# Watch logs
kubectl logs <driver-pod> -f -n data-platform

# Spark history server
kubectl port-forward svc/spark-history 18080:18080
```
