---
title: "Data Platform Infrastructure - Intermediate"
topic: docker-and-kubernetes
subtopic: data-platform-infrastructure
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker-and-kubernetes, kubernetes, data-platform, persistent-volumes, node-pools, karpenter, monitoring, prometheus, grafana]
---

# Data Platform Infrastructure — Intermediate

## PersistentVolume Strategies for Stateful Data Workloads

Stateful data workloads on Kubernetes need persistent storage — and the wrong choice of storage type causes performance problems, cost issues, or data loss. The key decisions are: ReadWriteOnce vs ReadWriteMany access mode, and block vs file vs object storage.

### Storage Class Comparison for Data Workloads

| Storage Type | AWS | Access Mode | Use Case | Throughput | Cost |
|---|---|---|---|---|---|
| Block (gp3) | EBS gp3 | RWO only | Single-pod DBs, Kafka, high-IOPS | High | Medium |
| File (EFS) | Amazon EFS | RWX | Shared logs, Jupyter notebooks, DAG files | Medium | High |
| Local NVMe | EC2 Instance Store | Node-local | Spark shuffle, temp data | Very High | Free (included) |
| Object | S3 | N/A (via SDK) | Checkpoints, artifacts, ML models | High (parallel) | Very Low |

### EFS for Shared Logs and DAG Distribution

EFS (Elastic File System) is the go-to for workloads that need `ReadWriteMany` — multiple pods reading and writing the same volume simultaneously.

```yaml
# EFS StorageClass — requires EFS CSI Driver installed on cluster
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-shared
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: fs-0123456789abcdef
  directoryPerms: "700"
  gidRangeStart: "1000"
  gidRangeEnd: "2000"

---
# PVC for shared Airflow DAGs (used by scheduler + all task pods)
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-dags-shared
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany    # CRITICAL: EFS supports this; EBS does NOT
  storageClassName: efs-shared
  resources:
    requests:
      storage: 10Gi   # EFS auto-expands; this is just a minimum declaration
```

### Local NVMe for Spark Shuffle

Spark shuffle writes are extremely I/O intensive. Writing shuffle data to EFS or even EBS causes 2-5x slowdown. The solution is using the node's local NVMe SSD (instance store) for shuffle.

```yaml
# SparkApplication: configure local NVMe for shuffle
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: heavy-aggregation
  namespace: analytics-de
spec:
  # ...
  sparkConf:
    "spark.local.dir": "/mnt/spark-shuffle"  # local NVMe mount point
    "spark.shuffle.file.buffer": "1m"
    "spark.shuffle.unsafe.file.output.buffer": "5m"
  driver:
    nodeSelector:
      node.kubernetes.io/instance-type: "i3.4xlarge"  # NVMe instance
    volumeMounts:
      - name: spark-shuffle
        mountPath: /mnt/spark-shuffle
  executor:
    nodeSelector:
      node.kubernetes.io/instance-type: "i3.4xlarge"
    volumeMounts:
      - name: spark-shuffle
        mountPath: /mnt/spark-shuffle
  volumes:
    - name: spark-shuffle
      hostPath:                     # use node's local storage
        path: /mnt/nvme/spark       # pre-formatted by node bootstrap script
        type: DirectoryOrCreate
```

**Warning:** Local NVMe is ephemeral — if the node is terminated (spot interruption), the shuffle data is gone and Spark must re-run the failed stage from the last checkpoint. This is acceptable because Spark handles it automatically via stage retry.

### gp3 for Stateful Single-Pod Workloads

For workloads where one pod owns the data (MLflow tracking DB, single Flink job manager), gp3 EBS volumes provide high IOPS at low cost:

```yaml
# gp3 StorageClass with IOPS configuration
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3-high-iops
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  iops: "6000"        # default 3000; max 16000
  throughput: "250"   # MB/s; default 125
volumeBindingMode: WaitForFirstConsumer  # wait to know which AZ the pod lands in
allowVolumeExpansion: true

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flink-rocksdb-state
  namespace: streaming-de
spec:
  accessModes:
    - ReadWriteOnce  # EBS: only one pod at a time
  storageClassName: gp3-high-iops
  resources:
    requests:
      storage: 500Gi
```

---

## Node Pools and Node Selectors

Running mixed workloads (Spark, Flink, Jupyter notebooks, ML training) on the same cluster efficiently requires different node types for different workloads. Node pools (EKS managed node groups or Karpenter NodePools) combined with nodeSelectors achieve this.

### Node Pool Architecture

```
Cluster node pools:
┌──────────────────────────────────────────────────────┐
│ system pool: m5.2xlarge (on-demand, 3 nodes)        │
│  - Airflow scheduler, webserver, operators, ArgoCD   │
│  - Taint: CriticalAddonsOnly=true:NoSchedule         │
├──────────────────────────────────────────────────────┤
│ spark pool: r5.4xlarge (spot, 0-100 nodes, Karpenter)│
│  - Spark drivers and executors                        │
│  - 122Gi RAM: fits multiple Spark executors per node  │
│  - Taint: workload=spark:NoSchedule                  │
├──────────────────────────────────────────────────────┤
│ gpu pool: g5.2xlarge (spot, 0-20 nodes, Karpenter)  │
│  - ML training jobs, PyTorch/TensorFlow workloads    │
│  - 1x A10G GPU per node                             │
│  - Taint: nvidia.com/gpu:NoSchedule                  │
├──────────────────────────────────────────────────────┤
│ general pool: m5.2xlarge (spot, 0-200 nodes)         │
│  - Airflow task pods, dbt jobs, lightweight pipelines │
│  - No taints — default landing zone for all workloads│
└──────────────────────────────────────────────────────┘
```

### Karpenter NodePool Configuration

Karpenter replaces the Cluster Autoscaler with a more flexible, just-in-time provisioner that provisions individual nodes in response to unschedulable pods:

```yaml
# Karpenter NodePool for Spark workloads
apiVersion: karpenter.sh/v1beta1
kind: NodePool
metadata:
  name: spark-workloads
spec:
  template:
    metadata:
      labels:
        node-type: spark
    spec:
      nodeClassRef:
        apiVersion: karpenter.k8s.aws/v1beta1
        kind: EC2NodeClass
        name: spark-nodeclass
      requirements:
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot", "on-demand"]  # prefer spot; fallback on-demand
        - key: node.kubernetes.io/instance-type
          operator: In
          values:
            - r5.4xlarge   # 128 GB RAM
            - r5.8xlarge   # 256 GB RAM
            - r5a.4xlarge  # AMD alternative
            - r5a.8xlarge
        - key: topology.kubernetes.io/zone
          operator: In
          values: ["us-east-1a", "us-east-1b", "us-east-1c"]
      taints:
        - key: workload
          value: spark
          effect: NoSchedule
  limits:
    cpu: 2000        # max 2000 cores total in this pool
    memory: 8000Gi
  disruption:
    consolidationPolicy: WhenEmpty   # only remove nodes with no pods
    expireAfter: 720h                # force-replace nodes after 30 days

---
# EC2NodeClass: AWS-specific configuration
apiVersion: karpenter.k8s.aws/v1beta1
kind: EC2NodeClass
metadata:
  name: spark-nodeclass
spec:
  amiFamily: AL2
  role: "KarpenterNodeRole-your-cluster"
  subnetSelectorTerms:
    - tags:
        karpenter.sh/discovery: "your-cluster"
  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: "your-cluster"
  blockDeviceMappings:
    - deviceName: /dev/xvda
      ebs:
        volumeSize: 200Gi
        volumeType: gp3
        iops: 3000
```

### Using nodeSelector and Tolerations in Workloads

```yaml
# SparkApplication targeting the spark pool
spec:
  driver:
    nodeSelector:
      node-type: spark              # must match node label
    tolerations:
      - key: workload
        operator: Equal
        value: spark
        effect: NoSchedule          # must tolerate the pool taint
  executor:
    nodeSelector:
      node-type: spark
    tolerations:
      - key: workload
        operator: Equal
        value: spark
        effect: NoSchedule
```

```python
# Airflow: GPU task targets GPU pool
gpu_training_task = KubernetesPodOperator(
    task_id="train_recommendation_model",
    image="your-registry/ml-trainer:1.4",
    node_selector={"node-type": "gpu"},
    tolerations=[
        k8s.V1Toleration(
            key="nvidia.com/gpu",
            operator="Exists",
            effect="NoSchedule",
        )
    ],
    resources=k8s.V1ResourceRequirements(
        limits={"nvidia.com/gpu": "1"},  # request 1 GPU
        requests={"memory": "16Gi", "cpu": "4"},
    ),
)
```

---

## Monitoring with Prometheus + Grafana

A complete data platform monitoring stack collects metrics from Kubernetes, JVM-based tools (Spark, Flink), and application-level metrics. The standard stack is Prometheus (collection) + Grafana (visualization) + Alertmanager (alerting).

### Installing the Monitoring Stack

```bash
# Install kube-prometheus-stack (includes Prometheus, Grafana, Alertmanager)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  -f monitoring-values.yaml
```

```yaml
# monitoring-values.yaml (key settings)
prometheus:
  prometheusSpec:
    retention: 30d
    storageSpec:
      volumeClaimTemplate:
        spec:
          storageClassName: gp3
          resources:
            requests:
              storage: 500Gi
    # Scrape all namespaces (including data team namespaces)
    podMonitorNamespaceSelector: {}
    serviceMonitorNamespaceSelector: {}

grafana:
  adminPassword: "set-via-secret"
  persistence:
    enabled: true
    storageClassName: gp3
    size: 20Gi
  # Pre-load dashboards
  dashboardProviders:
    dashboardproviders.yaml:
      apiVersion: 1
      providers:
        - name: 'data-platform'
          folder: 'Data Platform'
          type: file
          options:
            path: /var/lib/grafana/dashboards/data-platform
```

### Spark Metrics with Prometheus

Spark exposes JMX metrics. Configure Spark to emit to Prometheus via the Prometheus JMX exporter:

```yaml
# SparkApplication: configure Prometheus metrics export
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
spec:
  sparkConf:
    "spark.metrics.conf.driver.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet"
    "spark.metrics.conf.executor.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet"
    "spark.metrics.conf.driver.sink.prometheusServlet.path": "/metrics/prometheus"
    "spark.ui.prometheus.enabled": "true"

---
# PodMonitor: tells Prometheus to scrape Spark driver/executor pods
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: spark-applications
  namespace: monitoring
spec:
  namespaceSelector:
    matchNames: [finance-de, marketing-de, ml-platform]  # scrape these namespaces
  selector:
    matchLabels:
      spark-role: driver
  podMetricsEndpoints:
    - port: "4040"  # Spark UI / metrics port
      path: /metrics/prometheus
```

### Key Metrics to Monitor

```yaml
# Alertmanager rules for data platform
groups:
  - name: spark
    rules:
      # Spark job running too long (possible hung job)
      - alert: SparkJobRunningTooLong
        expr: |
          (time() - sparkapplication_start_time_seconds) > 7200  # 2 hours
          and on(name, namespace) sparkapplication_running == 1
        severity: warning

      # Spark executor OOM rate high
      - alert: SparkExecutorOOMHigh
        expr: |
          rate(spark_executor_jvmGCTime_ms_total[5m]) > 10000
        severity: warning

  - name: kubernetes_resources
    rules:
      # Namespace approaching quota limit
      - alert: NamespaceQuotaAlmostFull
        expr: |
          kube_resourcequota{resource="requests.memory",type="used"} /
          kube_resourcequota{resource="requests.memory",type="hard"} > 0.85
        severity: warning
        labels:
          team: "{{ $labels.namespace }}"

      # Node CPU saturation (affects all workloads on the node)
      - alert: NodeCPUSaturation
        expr: |
          100 - (avg by(node) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100) > 90
        for: 10m
        severity: critical
```

### Grafana Dashboard Panels for Data Platform

```python
# Key panels to include in a data platform Grafana dashboard:

# 1. Active Spark jobs by namespace (count)
# PromQL: count by(namespace) (sparkapplication_running == 1)

# 2. Spark executor pod count over time
# PromQL: count by(spark_app_name) (kube_pod_labels{label_spark_role="executor"})

# 3. Namespace memory utilization vs quota
# PromQL: kube_resourcequota{resource="requests.memory",type="used"}
#          / kube_resourcequota{resource="requests.memory",type="hard"}

# 4. Node count by pool over time (for cost tracking)
# PromQL: count by(label_node_type) (kube_node_labels)

# 5. Spot vs on-demand instance distribution
# PromQL: count by(label_karpenter_sh_capacity_type) (kube_node_labels)

# 6. Flink checkpoint lag (for streaming freshness)
# PromQL: flink_taskmanager_job_lastCheckpointDuration
```

---

## Cost Optimization with Karpenter Consolidation

Karpenter's consolidation feature actively removes underutilized nodes, unlike the Cluster Autoscaler which only scales down empty nodes:

```yaml
# Enable aggressive consolidation for batch workload pools
spec:
  disruption:
    consolidationPolicy: WhenUnderutilized  # actively consolidate, not just when empty
    consolidateAfter: 30s                   # try to consolidate 30s after last change
    budgets:
      - nodes: "10%"   # never consolidate more than 10% of nodes at once
        reasons: ["Underutilized"]
```

**Cost impact example:**
```
Without consolidation (Cluster Autoscaler, scale-down only when empty):
  - 30 nodes running at 20% average utilization
  - Cost: 30 × $0.40/hr = $12/hr = $8,640/month

With Karpenter WhenUnderutilized consolidation:
  - Pods bin-packed onto 8 nodes at 75% utilization
  - Cost: 8 × $0.40/hr = $3.20/hr = $2,304/month
  - Savings: ~73% reduction (~$6,336/month)
```

---
