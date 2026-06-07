---
title: "Spark on Kubernetes - Intermediate"
topic: pyspark
subtopic: spark-kubernetes
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, kubernetes, spark-operator, pod-templates, volumes]
---

# Spark on Kubernetes — Intermediate

## Spark Operator (SparkApplication CRD)

The Spark Kubernetes Operator lets you define Spark jobs as Kubernetes-native YAML manifests.

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-sales-etl
  namespace: spark-production
spec:
  type: Python
  mode: cluster
  image: my-registry/spark-etl:v1.2
  mainApplicationFile: s3a://bucket/jobs/etl_job.py
  sparkVersion: "3.5.0"
  arguments: ["--date", "2024-01-15"]
  sparkConf:
    spark.sql.adaptive.enabled: "true"
    spark.sql.shuffle.partitions: "200"
  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark-driver-sa
  executor:
    cores: 2
    memory: "4g"
    instances: 5
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 60
```

| Feature | spark-submit | Spark Operator |
|---------|-------------|----------------|
| Job definition | CLI command | YAML manifest (declarative) |
| Retry handling | Manual | Built-in (restartPolicy) |
| Scheduling | External (Airflow) | ScheduledSparkApplication CRD |
| GitOps | Difficult | Natural (YAML in git) |

---

## Pod Templates

Pod templates control node selection, tolerations, affinity, and resources beyond what spark-submit flags offer.

```yaml
# executor-pod-template.yaml
apiVersion: v1
kind: Pod
spec:
  nodeSelector:
    node-type: spot
  tolerations:
    - key: "spot-instance"
      operator: "Exists"
      effect: "NoSchedule"
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchLabels:
                app: spark-executor
            topologyKey: kubernetes.io/hostname
  containers:
    - name: spark-executor
      resources:
        requests: { memory: "8Gi", cpu: "4" }
        limits: { memory: "12Gi", cpu: "4" }
```

Apply with:
```bash
--conf spark.kubernetes.executor.podTemplateFile=s3a://configs/executor-template.yaml
```

---

## Volume Mounts for Shuffle Storage

### Local NVMe SSD (Best Performance)

```yaml
spec:
  executor:
    volumeMounts:
      - name: nvme-ssd
        mountPath: /mnt/nvme
    volumes:
      - name: nvme-ssd
        hostPath: { path: /mnt/local-ssd, type: Directory }
  sparkConf:
    spark.local.dir: /mnt/nvme
```

### emptyDir (Default — Uses Node Disk)

```yaml
spec:
  executor:
    volumeMounts:
      - name: spark-local
        mountPath: /tmp/spark
    volumes:
      - name: spark-local
        emptyDir: { sizeLimit: "50Gi" }
```

| Storage | IOPS | Survives Pod Death | Best For |
|---------|------|--------------------|----------|
| Local NVMe | 500K+ | No | Shuffle-heavy jobs |
| emptyDir | 50-100K | No | General workloads |
| PVC (gp3) | 16K | Yes | Shared/persistent |

---

## Service Accounts and RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-driver-sa
  namespace: spark-production
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/spark-s3-access
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver-role
  namespace: spark-production
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["services", "configmaps"]
    verbs: ["create", "get", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver-binding
  namespace: spark-production
subjects:
  - kind: ServiceAccount
    name: spark-driver-sa
roleRef:
  kind: Role
  name: spark-driver-role
  apiGroup: rbac.authorization.k8s.io
```

---

## Executor Pod Garbage Collection

```yaml
# Spark Operator handles cleanup automatically
spec:
  timeToLiveSeconds: 3600  # Remove completed pods after 1 hour
  sparkConf:
    spark.kubernetes.executor.deleteOnTermination: "true"
```

---

## Volcano Scheduler (Gang Scheduling)

Without gang scheduling, K8s may schedule only some executors, leaving the job partially running. Volcano ensures all-or-nothing scheduling.

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: spark-job-gang
spec:
  minMember: 6    # 1 driver + 5 executors must all be schedulable
  minResources:
    cpu: "12"
    memory: "24Gi"
```

**Why it matters:** Without gang scheduling, a job might start with 2 of 20 requested executors, running extremely slowly. With Volcano, either all 20 get scheduled or the job waits (avoiding resource waste).

---

## Image Pull Secrets

```bash
spark-submit \
    --conf spark.kubernetes.container.image.pullSecrets=registry-creds \
    --conf spark.kubernetes.container.image=private-registry.com/spark:3.5.0 \
    ...
```

---

## Interview Tips

> **Tip 1:** "What is the Spark Operator?" — "A K8s controller that manages Spark jobs via a SparkApplication CRD. It provides declarative YAML config, built-in retry policies, scheduled applications, and native status reporting. It replaces raw spark-submit with GitOps-friendly manifests."

> **Tip 2:** "How do you handle shuffle storage on K8s?" — "Three options by performance: Local NVMe (hostPath) at 500K+ IOPS, emptyDir on node disk at 50-100K IOPS, or PVC with network-attached storage at 16K IOPS. Set spark.local.dir to the mount path. Enable shuffleTracking for dynamic allocation."

> **Tip 3:** "How do you secure Spark on K8s?" — "ServiceAccount with least-privilege RBAC (pod create/delete only in its namespace), IRSA for cloud access without static credentials, namespace isolation with ResourceQuotas, and Network Policies restricting pod-to-pod traffic."
