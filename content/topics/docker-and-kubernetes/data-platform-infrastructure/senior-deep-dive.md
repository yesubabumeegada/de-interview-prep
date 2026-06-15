---
title: "Data Platform Infrastructure - Senior Deep Dive"
topic: docker-and-kubernetes
subtopic: data-platform-infrastructure
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [docker-and-kubernetes, kubernetes, data-platform, gitops, argocd, rbac, multi-tenancy, production-architecture]
---

# Data Platform Infrastructure — Senior Deep Dive

## GitOps with ArgoCD: The Production Deployment Model

Manual `kubectl apply` is not a production deployment model. In mature data platforms, all Kubernetes manifests live in git, and ArgoCD continuously reconciles the cluster state to match the git state. This is GitOps.

### Why GitOps for Data Platforms

1. **Audit trail:** Every change to infrastructure is a git commit with author, timestamp, and diff
2. **Rollback:** `git revert` is your rollback mechanism — declarative and safe
3. **Self-healing:** ArgoCD detects manual changes (drift) and reverts them automatically
4. **Multi-team self-service:** Teams manage their own manifests via PRs; platform team reviews and merges
5. **Disaster recovery:** If the cluster is destroyed, `argocd app sync` recreates everything from git in minutes

### Repository Structure

```
data-platform-infra/          (platform team repo — infrastructure)
├── cluster/
│   ├── namespaces/
│   │   ├── finance-de.yaml
│   │   ├── marketing-de.yaml
│   │   └── ml-platform.yaml
│   ├── rbac/
│   │   ├── finance-de-sa.yaml
│   │   └── marketing-de-sa.yaml
│   ├── quotas/
│   │   ├── finance-de-quota.yaml
│   │   └── marketing-de-quota.yaml
│   └── node-pools/
│       └── karpenter-nodepools.yaml
├── apps/                      (ArgoCD Application manifests)
│   ├── airflow.yaml
│   ├── jupyterhub.yaml
│   ├── mlflow.yaml
│   └── monitoring.yaml
└── argocd/
    └── argocd-values.yaml

finance-de-pipelines/         (finance team repo — team-owned)
├── spark-jobs/
│   ├── revenue-aggregation.yaml
│   └── cost-center-analysis.yaml
├── dbt-jobs/
│   └── dbt-finance-daily.yaml
└── secrets/
    └── external-secrets.yaml
```

### ArgoCD Application for the Monitoring Stack

```yaml
# apps/monitoring.yaml — ArgoCD manages the monitoring stack
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: monitoring
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"  # deploy before data workloads
spec:
  project: platform
  source:
    repoURL: https://prometheus-community.github.io/helm-charts
    chart: kube-prometheus-stack
    targetRevision: 58.2.2
    helm:
      valueFiles:
        - values/monitoring-prod.yaml
      valuesObject:
        prometheus:
          prometheusSpec:
            retention: 30d
  destination:
    server: https://kubernetes.default.svc
    namespace: monitoring
  syncPolicy:
    automated:
      prune: true     # delete resources removed from Helm chart
      selfHeal: true  # revert manual changes
    syncOptions:
      - CreateNamespace=true
      - ServerSideApply=true
```

### ArgoCD AppProject for Team Isolation

```yaml
# Each team gets an ArgoCD Project that restricts what they can deploy
# and where — prevents a team from accidentally deploying to another team's namespace
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: finance-de
  namespace: argocd
spec:
  description: "Finance Data Engineering team"
  sourceRepos:
    - "https://github.com/your-org/finance-de-pipelines"  # only their repo
  destinations:
    - namespace: finance-de    # only their namespace
      server: https://kubernetes.default.svc
  clusterResourceWhitelist: []  # no cluster-level resource creation
  namespaceResourceWhitelist:
    - group: "sparkoperator.k8s.io"
      kind: SparkApplication
    - group: "batch"
      kind: Job
    - group: ""
      kind: ConfigMap

---
# Finance team's ArgoCD Application
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: finance-spark-jobs
  namespace: argocd
spec:
  project: finance-de  # restricted to AppProject above
  source:
    repoURL: https://github.com/your-org/finance-de-pipelines
    targetRevision: main
    path: spark-jobs/
  destination:
    server: https://kubernetes.default.svc
    namespace: finance-de
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

---

## Advanced RBAC Patterns for Multi-Tenant Data Platforms

RBAC at scale requires more than just ServiceAccounts per team. A mature model separates concerns into three tiers: platform operators, team leads, and pipeline automation.

### Three-Tier RBAC Model

```yaml
# Tier 1: Platform operators — ClusterRole (cluster-wide)
# Can: manage all namespaces, node pools, RBAC, quotas
# Who: infrastructure/platform team (2-5 people)

apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: data-platform-operator
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["*"]  # full access — restricted by org policy/MFA

---
# Tier 2: Team leads — Role per namespace (namespace-scoped)
# Can: manage spark jobs, view pods/logs, manage configmaps
# Who: tech leads per data team (one per team)

apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: team-lead-role
  namespace: finance-de
rules:
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["*"]
  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["*"]
  - apiGroups: [""]
    resources: ["pods", "pods/log", "pods/exec"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "update"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list"]  # read only — cannot create secrets

---
# Tier 3: Pipeline ServiceAccounts — minimal permissions
# Can: only what the pipeline needs (create pods, read secrets it owns)
# Who: automated pipelines (Airflow tasks, CI/CD jobs)

apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: finance-pipeline-sa-role
  namespace: finance-de
rules:
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["create", "get", "watch", "delete"]
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["snowflake-creds", "s3-credentials"]  # only specific secrets
    verbs: ["get"]
```

### Auditing RBAC: Finding Overpermissioned ServiceAccounts

```bash
# Find all ServiceAccounts with ClusterAdmin binding (should be near zero)
kubectl get clusterrolebindings -o json | \
  jq '.items[] | select(.roleRef.name == "cluster-admin") | .subjects[]'

# Find what permissions a ServiceAccount has
kubectl auth can-i --list \
  --as=system:serviceaccount:finance-de:finance-pipeline-sa \
  -n finance-de

# Check if a ServiceAccount can read secrets in another namespace (shouldn't!)
kubectl auth can-i get secrets \
  --as=system:serviceaccount:finance-de:finance-pipeline-sa \
  -n marketing-de
# Should return: "no"

# Use rbac-lookup tool for cleaner output
# https://github.com/reactiveops/rbac-lookup
kubectl rbac-lookup finance-pipeline-sa -k serviceaccount -o wide
```

---

## Spark on Kubernetes: Advanced Configuration

### Dynamic Resource Allocation (DRA) with Kubernetes

Spark's Dynamic Resource Allocation lets executors scale up and down during a single job based on actual work:

```yaml
# SparkApplication with DRA enabled
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
spec:
  sparkConf:
    # Dynamic allocation
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.shuffleTracking.enabled": "true"  # required for K8s (no external shuffle service)
    "spark.dynamicAllocation.minExecutors": "2"
    "spark.dynamicAllocation.maxExecutors": "50"
    "spark.dynamicAllocation.initialExecutors": "5"
    "spark.dynamicAllocation.executorIdleTimeout": "60s"

    # Adaptive Query Execution (AQE) — works with DRA
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
    "spark.sql.adaptive.skewJoin.enabled": "true"

    # Memory tuning
    "spark.executor.memoryOverhead": "1024"  # off-heap for JVM overhead
    "spark.memory.fraction": "0.8"           # 80% of heap for execution+storage
    "spark.memory.storageFraction": "0.3"    # 30% of that for caching

  executor:
    instances: 5     # initial count; DRA adjusts this
    cores: 4
    memory: "8192m"
```

### Spark Checkpointing to S3 for Fault Tolerance

```python
# PySpark job with S3 checkpointing
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("revenue-aggregation") \
    .config("spark.checkpoint.dir", "s3a://your-bucket/spark-checkpoints/revenue-agg") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \  # IRSA
    .getOrCreate()

# Enable checkpointing for streaming or iterative jobs
sc = spark.sparkContext
sc.setCheckpointDir("s3a://your-bucket/spark-checkpoints/revenue-agg")

# For streaming jobs: checkpoint enables fault recovery
query = df_stream \
    .writeStream \
    .trigger(processingTime="5 minutes") \
    .option("checkpointLocation", "s3a://your-bucket/flink-checkpoints/revenue") \
    .start()
```

---

## Namespace ResourceQuota: Preventing Cost Explosions

At scale, a single misconfigured SparkApplication can request 1000 executors and exhaust the entire cluster. ResourceQuotas are the safety net.

### Tiered Quota Model

```yaml
# Different quota tiers for different team sizes/needs

# Small team quota (< 5 engineers, lightweight pipelines)
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: small-team-quota
  namespace: analytics-de
spec:
  hard:
    requests.cpu: "20"
    requests.memory: "80Gi"
    limits.memory: "160Gi"
    count/pods: "50"
    count/sparkapplications.sparkoperator.k8s.io: "2"

# Large team quota (10+ engineers, heavy Spark users)
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: large-team-quota
  namespace: finance-de
spec:
  hard:
    requests.cpu: "200"
    requests.memory: "800Gi"
    limits.memory: "1600Gi"
    count/pods: "500"
    count/sparkapplications.sparkoperator.k8s.io: "20"
```

### Monitoring Quota Usage

```bash
# Check all quotas across all namespaces — find who's close to limit
kubectl get resourcequota -A -o json | jq -r '
  .items[] | 
  {
    namespace: .metadata.namespace,
    cpu_used: .status.used["requests.cpu"],
    cpu_hard: .status.hard["requests.cpu"],
    mem_used: .status.used["requests.memory"],
    mem_hard: .status.hard["requests.memory"]
  } | 
  "\(.namespace): CPU \(.cpu_used)/\(.cpu_hard), Mem \(.mem_used)/\(.mem_hard)"
'

# Prometheus alert: namespace approaching quota
# expr: kube_resourcequota{type="used"} / kube_resourcequota{type="hard"} > 0.85
```

---

## Flink Stateful Streaming on Kubernetes: Production Concerns

Flink's stateful operations (windowing, joins, aggregations) require careful management of checkpoint storage to enable restarts without data loss.

```yaml
# FlinkDeployment: production-grade configuration
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: revenue-stream-processor
  namespace: streaming-de
spec:
  image: your-registry/flink:1.18.0
  flinkVersion: v1_18
  flinkConfiguration:
    # RocksDB state backend — handles state larger than memory
    state.backend: rocksdb
    state.backend.rocksdb.localdir: /mnt/rocksdb
    state.checkpoints.dir: s3://your-bucket/flink-checkpoints/revenue
    state.savepoints.dir: s3://your-bucket/flink-savepoints/revenue

    # Checkpoint frequency and timeout
    execution.checkpointing.interval: "60000"    # every 60 seconds
    execution.checkpointing.timeout: "300000"    # 5 minute timeout
    execution.checkpointing.min-pause: "10000"   # 10s min between checkpoints
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

    # HA — required for production (job manager HA via K8s)
    high-availability.type: kubernetes
    high-availability.storageDir: s3://your-bucket/flink-ha/revenue
    kubernetes.cluster-id: revenue-stream-processor

    # Parallelism
    parallelism.default: "32"
    taskmanager.numberOfTaskSlots: "4"

  jobManager:
    resource: {memory: "2048m", cpu: 1}
    replicas: 2  # HA: 2 job managers, one is standby

  taskManager:
    resource: {memory: "8192m", cpu: 4}
    replicas: 8  # 8 TMs × 4 slots = 32 total parallelism

  job:
    jarURI: s3://your-bucket/flink-jars/revenue-processor-2.1.0.jar
    parallelism: 32
    upgradeMode: savepoint  # take savepoint before upgrade, restore after
```

### Rolling Upgrades Without Data Loss

```bash
# Flink savepoint-based upgrade (zero data loss)
# Step 1: Trigger a savepoint before stopping
kubectl apply -f - <<EOF
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: revenue-stream-processor
  namespace: streaming-de
spec:
  # ... same spec
  job:
    upgradeMode: savepoint    # Flink Operator takes savepoint, then upgrades
    jarURI: s3://your-bucket/flink-jars/revenue-processor-2.2.0.jar  # new version
EOF

# Flink Operator handles: savepoint → stop job → update → restart from savepoint

# Monitor the upgrade
kubectl describe flinkdeployment revenue-stream-processor -n streaming-de
# Status: UPGRADING → RUNNING
```

---

## Production Data Platform: Lessons Learned

### Lesson 1: Quota Enforcement Breaks in-Progress Jobs

**Problem:** A team hits their ResourceQuota mid-Spark-job. Executors can't be added because the quota is full, so the job stalls.

**Fix:** Configure Spark DRA with `spark.dynamicAllocation.maxExecutors` below the team's quota limit. Add a Prometheus alert at 80% quota usage. Implement a "burst" mechanism where teams can temporarily increase quota via a PR to the platform repo.

### Lesson 2: Karpenter Consolidation Kills Flink Jobs

**Problem:** Karpenter consolidates a task manager node mid-checkpoint. Flink's checkpoint fails, triggering state recovery from the previous checkpoint. The 60-second checkpoint means up to 60 seconds of reprocessing. At high throughput, this causes 2-3 minute consumer lag spikes.

**Fix:** Add PodDisruptionBudget for Flink TaskManagers to ensure only one TaskManager is disrupted at a time, giving Flink time to complete in-progress checkpoints.

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: flink-taskmanager-pdb
  namespace: streaming-de
spec:
  minAvailable: "75%"  # keep at least 75% of TMs running during disruption
  selector:
    matchLabels:
      component: taskmanager
      app: revenue-stream-processor
```

### Lesson 3: Spark Driver OOM When Collecting Results

**Problem:** Data engineers call `df.collect()` or `df.toPandas()` on large DataFrames, sending all data to the Spark driver. The driver OOMs. K8s kills the driver pod, which kills the entire job.

**Fix:** Enforce through code review + custom Spark listener that logs a warning when `collect()` is called on DataFrames above a threshold. Set `spark.driver.maxResultSize` to prevent catastrophic collection.

```yaml
sparkConf:
  "spark.driver.maxResultSize": "4g"  # fail fast instead of OOM
  "spark.sql.execution.arrow.maxRecordsPerBatch": "10000"  # toPandas() limit
```

### Lesson 4: Image Pull Rate Limits on DockerHub

**Problem:** During a large-scale Spark job with 100 executors, all nodes try to pull the Spark image simultaneously from DockerHub. DockerHub's rate limit triggers 429 errors. Executors fail to start.

**Fix:** Mirror all public images to ECR. Never pull from DockerHub in production.

```bash
# Mirror public Spark image to ECR
docker pull apache/spark:3.5.0
docker tag apache/spark:3.5.0 $AWS_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/spark:3.5.0
docker push $AWS_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/spark:3.5.0

# Add ECR mirror to all SparkApplications
# image: $AWS_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/spark:3.5.0
```

---

## Senior Interview Talking Points

1. **GitOps is not optional at scale** — with 50 teams making infrastructure changes, you need an immutable audit trail and an automated reconciliation loop. `kubectl apply` from a developer's laptop is not auditable and creates configuration drift.

2. **ResourceQuotas protect the platform; LimitRanges protect the cluster** — quotas prevent a team from consuming all cluster resources; LimitRanges prevent individual pods from not setting requests/limits (which breaks the scheduler's bin-packing algorithm).

3. **Karpenter outperforms Cluster Autoscaler for DE workloads** — CA can only scale down when nodes are completely empty; batch jobs with bursty demand often leave "stranded" pods on underutilized nodes. Karpenter's bin-packing consolidation actively solves this, yielding 50-70% cost reduction in batch-heavy clusters.

4. **Stateful workload storage requires a decision matrix** — EBS for RWO (high-IOPS, single-pod stateful), EFS for RWX (shared logs, DAG files), instance store for ephemeral high-throughput (Spark shuffle), S3 for durable state (checkpoints, artifacts). Using EFS for everything is slow and expensive; using EBS for shared logs is impossible (RWO).

5. **The hardest problem in multi-tenant data platforms is cross-team dependency** — when Team A's Spark job causes a node to be scheduled in a way that blocks Team B's urgent pipeline. Solving this requires combination of: ResourceQuotas (hard limits), priority classes (some jobs get scheduled first), and node pool isolation (separate node pools for critical vs batch workloads).

---
