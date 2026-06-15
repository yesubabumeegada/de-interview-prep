---
title: "Data Platform Infrastructure - Fundamentals"
topic: docker-and-kubernetes
subtopic: data-platform-infrastructure
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker-and-kubernetes, kubernetes, data-platform, spark-operator, flink, jupyterhub, rbac, infrastructure]
---

# Data Platform Infrastructure — Fundamentals

## 🎯 Analogy

Think of a Kubernetes-based data platform like a shared office building. Different data teams (finance, marketing, ML) are tenants on different floors (namespaces). The building manager (platform team) sets rules: each floor has a power/water budget (ResourceQuota), the fire doors prevent you from wandering into other floors (NetworkPolicy), and each tenant has a keycard that only opens their floor (RBAC). The building handles shared utilities — plumbing (networking), electricity (compute nodes), and the lobby (shared services) — while tenants manage their own office layout.

---

## Why Run Data Workloads on Kubernetes?

The data engineering ecosystem is converging on Kubernetes as the infrastructure layer for data platforms. Major frameworks — Spark, Flink, Airflow, dbt, JupyterHub, MLflow — all have official Kubernetes operators or native K8s support. Running them on Kubernetes provides:

- **Unified infrastructure:** One control plane for all data workloads instead of separate clusters per tool
- **Elastic scaling:** Compute scales to zero when not in use (Karpenter/Cluster Autoscaler)
- **Reproducible environments:** Docker images ensure identical environments across dev, staging, prod
- **Cost efficiency:** Spot instances for batch workloads; autoscaling prevents over-provisioning
- **Multi-tenancy:** Namespace isolation, RBAC, and ResourceQuotas support dozens of teams on one cluster

Understanding data platform infrastructure on Kubernetes is increasingly a core competency for senior DE and data platform engineer roles.

---

## Common Data Workloads on Kubernetes

### Spark on Kubernetes (via spark-operator)

The Spark Operator (from Google, now maintained by the community) lets you run Spark jobs by submitting a `SparkApplication` custom resource — no need to manage Spark standalone mode or YARN.

```yaml
# SparkApplication custom resource — submit a Spark job
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: revenue-aggregation
  namespace: finance-de
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "your-registry/spark-python:3.5.0"
  imagePullPolicy: Always
  mainApplicationFile: "s3a://your-bucket/jobs/revenue_aggregation.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 2
    memory: "4096m"
    serviceAccount: spark-driver-sa
    nodeSelector:
      node-type: spark
  executor:
    cores: 4
    memory: "8192m"
    instances: 5
    nodeSelector:
      node-type: spark
```

```bash
# Submit via kubectl (operator watches for SparkApplication CRs)
kubectl apply -f revenue-aggregation.yaml -n finance-de

# Check status
kubectl get sparkapplication revenue-aggregation -n finance-de
kubectl describe sparkapplication revenue-aggregation -n finance-de

# View driver logs (the driver pod has the main application log)
kubectl logs revenue-aggregation-driver -n finance-de -f
```

### Flink on Kubernetes (via Flink Operator)

Apache Flink runs streaming jobs. The Flink Kubernetes Operator manages `FlinkDeployment` resources:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: clickstream-processor
  namespace: streaming-de
spec:
  image: your-registry/flink:1.18.0
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.checkpoints.dir: s3://your-bucket/flink-checkpoints
  serviceAccount: flink-sa
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 4
  job:
    jarURI: s3://your-bucket/flink-jobs/clickstream-processor-1.2.0.jar
    parallelism: 16
    upgradeMode: stateless
```

### JupyterHub for Data Science Notebooks

JupyterHub on Kubernetes (often deployed via the `jupyterhub` Helm chart) gives each user their own Jupyter server as a K8s pod:

```bash
# Deploy JupyterHub
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/
helm install jhub jupyterhub/jupyterhub \
  --namespace notebooks \
  --create-namespace \
  -f jupyterhub-values.yaml
```

```yaml
# jupyterhub-values.yaml (key sections)
singleuser:
  image:
    name: your-registry/datascience-notebook
    tag: "v2.1"
  memory:
    limit: 8G
    guarantee: 1G
  cpu:
    limit: 4
    guarantee: 0.5
  storage:
    capacity: 20Gi
    storageClassName: gp3

hub:
  config:
    Authenticator:
      admin_users: ["platform-team@company.com"]
    OAuthenticator:
      client_id: "..."
      oauth_callback_url: "https://jupyter.company.com/hub/oauth_callback"
```

### MLflow Tracking Server

MLflow tracks ML experiments (parameters, metrics, artifacts):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mlflow-tracking
  namespace: ml-platform
spec:
  replicas: 2
  selector:
    matchLabels:
      app: mlflow-tracking
  template:
    metadata:
      labels:
        app: mlflow-tracking
    spec:
      serviceAccountName: mlflow-sa
      containers:
        - name: mlflow
          image: your-registry/mlflow:2.12.0
          command:
            - mlflow
            - server
            - --host=0.0.0.0
            - --backend-store-uri=postgresql://mlflow:$(DB_PASS)@rds-endpoint:5432/mlflow
            - --default-artifact-root=s3://your-bucket/mlflow-artifacts
          resources:
            requests: {memory: "1Gi", cpu: "500m"}
            limits: {memory: "2Gi", cpu: "1"}
          env:
            - name: DB_PASS
              valueFrom:
                secretKeyRef:
                  name: mlflow-db-secret
                  key: password
```

### dbt Runner Pods

For running dbt jobs in Kubernetes (often triggered by Airflow):

```yaml
# dbt Job manifest — runs dbt models for the finance team
apiVersion: batch/v1
kind: Job
metadata:
  name: dbt-finance-daily-2024-01-15
  namespace: finance-de
spec:
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: finance-de-sa
      containers:
        - name: dbt
          image: your-registry/dbt-snowflake:1.8.0
          command: ["dbt", "run", "--select", "tag:finance", "--target", "prod"]
          env:
            - name: DBT_SNOWFLAKE_ACCOUNT
              valueFrom:
                secretKeyRef:
                  name: snowflake-creds
                  key: account
            - name: DBT_SNOWFLAKE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: snowflake-creds
                  key: password
          resources:
            requests: {memory: "512Mi", cpu: "250m"}
            limits: {memory: "2Gi", cpu: "1"}
```

---

## Namespace Isolation for Data Teams

Namespaces are the primary unit of isolation in Kubernetes. Every production data platform uses a namespace-per-team (or namespace-per-environment) model.

```bash
# Create namespaces for data teams
kubectl create namespace finance-de
kubectl create namespace marketing-de
kubectl create namespace ml-platform
kubectl create namespace streaming-de
kubectl create namespace notebooks

# Add team labels for cost tracking
kubectl label namespace finance-de team=finance cost-center=fin-001
kubectl label namespace marketing-de team=marketing cost-center=mkt-002
```

### Why Namespaces Matter for DE Teams

1. **Blast radius containment:** A misconfigured SparkApplication in `finance-de` that consumes all cluster CPU stays within the `finance-de` namespace's ResourceQuota — marketing's pipelines are unaffected.
2. **Access control:** RBAC policies scoped to a namespace mean the marketing team cannot see or modify finance pipelines.
3. **Resource quotas:** Set different budgets per team — the ML platform team gets 500 CPU cores; the small analytics team gets 20.
4. **Cost attribution:** Billing tools (Kubecost, OpenCost) aggregate costs by namespace → each team sees their monthly spend.

---

## RBAC Basics for Data Platforms

RBAC (Role-Based Access Control) in Kubernetes controls who can do what in which namespace. For data platforms, the key pattern is: one ServiceAccount per team/pipeline, with only the permissions they need.

```yaml
# ServiceAccount for finance team's pipelines
apiVersion: v1
kind: ServiceAccount
metadata:
  name: finance-pipeline-sa
  namespace: finance-de

---
# Role: what this SA can do in finance-de namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: finance-pipeline-role
  namespace: finance-de
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get"]  # read secrets only; cannot create/delete

---
# RoleBinding: bind the Role to the ServiceAccount
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: finance-pipeline-binding
  namespace: finance-de
subjects:
  - kind: ServiceAccount
    name: finance-pipeline-sa
    namespace: finance-de
roleRef:
  kind: Role
  name: finance-pipeline-role
  apiGroup: rbac.authorization.k8s.io
```

---

## Resource Quotas and LimitRanges

### ResourceQuota: Team-Level Budget

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: finance-de-quota
  namespace: finance-de
spec:
  hard:
    requests.cpu: "50"           # 50 cores max requested across all pods
    requests.memory: "200Gi"     # 200Gi memory max requested
    limits.cpu: "100"
    limits.memory: "400Gi"
    count/pods: "100"            # max 100 pods simultaneously
    count/sparkapplications.sparkoperator.k8s.io: "5"  # max 5 concurrent Spark jobs
```

### LimitRange: Per-Pod Defaults and Minimums

```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: finance-de-limits
  namespace: finance-de
spec:
  limits:
    - type: Container
      default:          # applied if container doesn't specify limits
        memory: "1Gi"
        cpu: "500m"
      defaultRequest:   # applied if container doesn't specify requests
        memory: "256Mi"
        cpu: "100m"
      max:              # no single container can exceed this
        memory: "32Gi"
        cpu: "16"
      min:              # no container can request less than this
        memory: "64Mi"
        cpu: "10m"
```

**Why this matters:** Without LimitRanges, a developer can submit a SparkApplication with no resource specifications. Kubernetes schedules it with no limits — it can consume all cluster resources. LimitRange provides a safety net.

---

## Key kubectl Commands for Data Platform Operations

```bash
# Overview of all data workloads across namespaces
kubectl get pods -A | grep -E "spark|flink|jupyter|mlflow|dbt"

# Check resource usage by namespace
kubectl top pods -n finance-de
kubectl top nodes

# List all SparkApplications
kubectl get sparkapplications -A

# Check ResourceQuota usage
kubectl describe resourcequota -n finance-de

# Describe a Flink deployment
kubectl describe flinkdeployment clickstream-processor -n streaming-de

# Get logs from Spark executor
kubectl logs <executor-pod-name> -n finance-de

# Check events (recent issues in a namespace)
kubectl get events -n finance-de --sort-by='.lastTimestamp' | tail -20
```

---
