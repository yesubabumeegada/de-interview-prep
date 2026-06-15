---
title: "Data Platform Infrastructure - Real World"
topic: docker-and-kubernetes
subtopic: data-platform-infrastructure
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [docker-and-kubernetes, kubernetes, data-platform, production, spark, flink, cost-optimization, observability]
---

# Data Platform Infrastructure — Real World

## What a Production Data Platform on Kubernetes Actually Looks Like

After running a multi-team data platform on Kubernetes for 2+ years, the architecture that emerges through operational pressure is significantly more opinionated than what the documentation suggests. Here's what production looks like and why.

### Reference Architecture (Multi-Team, AWS/EKS)

```
EKS Cluster: data-platform-prod
├── Node Pools (Karpenter-managed)
│   ├── system (on-demand, m5.2xlarge, 3 nodes): operators, ArgoCD, monitoring
│   ├── general-spot (spot, m5/m5a/m5n): Airflow tasks, dbt jobs, lightweight work
│   ├── spark-spot (spot, r5/r5a): Spark driver + executors, memory-optimized
│   ├── streaming (on-demand, m5.2xlarge): Flink TMs (avoid preemption for state)
│   └── gpu (spot, g5.2xlarge): ML training, inference serving
│
├── Namespaces
│   ├── argocd: GitOps controller
│   ├── monitoring: Prometheus + Grafana + Alertmanager
│   ├── airflow: Scheduler + Webserver (tasks go to team namespaces)
│   ├── jupyterhub: Shared notebook service
│   ├── ml-platform: MLflow tracking, feature store
│   ├── streaming-de: Flink jobs (ops team)
│   ├── finance-de: Finance team pipelines
│   ├── marketing-de: Marketing team pipelines
│   └── [12 more team namespaces]
│
├── Shared Infrastructure
│   ├── RDS Aurora (Postgres): Airflow metadata, MLflow tracking DB
│   ├── ElastiCache Redis: Celery broker (if using CeleryExecutor)
│   ├── S3: Spark checkpoints, Flink savepoints, Airflow logs, artifacts
│   ├── ECR: All container images (no DockerHub in prod)
│   └── AWS Secrets Manager + ESO: All credentials
│
└── Networking
    ├── AWS Load Balancer Controller: Ingress for Airflow, Grafana, JupyterHub
    ├── VPC CNI: Pod networking with VPC-native IPs (allows SG per pod)
    └── NetworkPolicies: Team namespace isolation
```

---

## Real Incident: Karpenter Consolidation Causing Cascading Flink Failures

**The situation:** A production Flink job processing clickstream events (10M events/sec) started failing checkpoints intermittently, causing consumer lag to spike from 5 seconds to 45 minutes. The on-call alert fired at 2 AM.

**Initial investigation:**

```bash
# Check Flink pod status
kubectl get pods -n streaming-de -l component=taskmanager
# UNEXPECTED: 3 of 8 task manager pods are in Terminating state

# Check what's happening to the nodes
kubectl get nodes --sort-by='.metadata.creationTimestamp'
# Two nodes that had task managers are in "Ready,SchedulingDisabled" state

# Check Karpenter controller logs
kubectl logs -n karpenter deployment/karpenter | grep -i "consolidat" | tail -20
# "Consolidating node ip-10-0-1-42: 2 pods, 15% utilization — moving to ip-10-0-1-55"
# "Consolidating node ip-10-0-1-78: 3 pods, 18% utilization"
```

**Root cause:** Karpenter's `WhenUnderutilized` consolidation was active on the streaming node pool. Between 2-4 AM (low traffic period), Flink TaskManagers used only 15-20% of node capacity. Karpenter consolidated TMs from two nodes, triggering graceful termination. But "graceful termination" for Flink means: evict the pod → Flink detects TM lost → trigger checkpoint → checkpoint fails (TM writing to checkpoint is gone) → failover → recover from previous checkpoint → 45 minutes of lag as processing catches up.

**Fix:**

```yaml
# 1. Disable consolidation for streaming node pool (use WhenEmpty instead)
apiVersion: karpenter.sh/v1beta1
kind: NodePool
metadata:
  name: streaming-nodes
spec:
  disruption:
    consolidationPolicy: WhenEmpty  # NOT WhenUnderutilized for stateful streaming
    expireAfter: 168h               # replace nodes weekly for security patches

# 2. Add PodDisruptionBudget to prevent multiple concurrent TM evictions
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: flink-taskmanager-pdb
  namespace: streaming-de
spec:
  minAvailable: "90%"  # at most 10% of TMs can be disrupted at once
  selector:
    matchLabels:
      component: taskmanager

# 3. Increase checkpoint frequency to reduce recovery time
# flinkConfiguration:
#   execution.checkpointing.interval: "30000"  # 30s instead of 60s
#   → worst case lag on recovery: 30s → ~5 minute catchup vs 45 minutes
```

**Lesson:** Node pool consolidation policy must match workload characteristics. Stateful streaming workloads need `WhenEmpty`; batch/stateless workloads benefit from `WhenUnderutilized`. Using the same policy for all workloads is the mistake.

---

## Real Pattern: Spark Cost Attribution and Chargeback

A $500K/month Spark compute bill without cost attribution by team is a management problem waiting to happen. The solution is tagging every SparkApplication with team/cost-center labels and querying Kubecost or OpenCost for breakdowns.

```yaml
# SparkApplication: mandatory cost attribution labels
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: revenue-weekly-aggregation
  namespace: finance-de
  labels:
    team: finance                    # for Kubecost team filter
    cost-center: fin-data-001        # for chargeback
    pipeline: revenue-aggregation    # for per-pipeline cost tracking
    environment: production
spec:
  # ...
  driver:
    labels:
      team: finance
      cost-center: fin-data-001
      pipeline: revenue-aggregation
  executor:
    labels:
      team: finance
      cost-center: fin-data-001
      pipeline: revenue-aggregation
```

```python
# Monthly cost report script (queries OpenCost API)
import requests
from datetime import datetime, timedelta

def get_team_costs(month: str) -> dict:
    """Fetch Kubecost allocation data aggregated by team label."""
    start = f"{month}-01T00:00:00Z"
    # last day of month
    end = (datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=32)).replace(day=1)
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    resp = requests.get(
        "http://kubecost.monitoring.svc.cluster.local:9090/model/allocation",
        params={
            "window": f"{start},{end_str}",
            "aggregate": "label:team",
            "accumulate": "true",
        }
    )
    data = resp.json()
    
    costs = {}
    for allocation_name, allocation in data["data"][0].items():
        if allocation_name == "__unallocated__":
            continue
        costs[allocation_name] = {
            "total_cost": round(allocation["totalCost"], 2),
            "cpu_cost": round(allocation["cpuCost"], 2),
            "memory_cost": round(allocation["ramCost"], 2),
            "storage_cost": round(allocation["pvCost"], 2),
        }
    return costs

# Output example:
# {
#   "finance": {"total_cost": 18420.50, "cpu_cost": 12100.00, ...},
#   "marketing": {"total_cost": 9850.25, ...},
#   "ml-platform": {"total_cost": 35200.00, ...},
# }
```

**Process:** Send monthly cost reports to each team's manager. Teams over-budget by >20% must file a justification. Teams consistently under-budget have their quotas reduced. This creates natural incentive for teams to right-size their Spark jobs.

---

## Real Pattern: Progressive Delivery for Spark Jobs via ArgoCD

Deploying new versions of Spark jobs (new JAR files) without testing them on production data first is risky. The pattern for safe Spark job upgrades:

```yaml
# ArgoCD Rollouts for Spark job versioning (using Argo Rollouts)
# Step 1: Deploy new version to "canary" SparkApplication
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: revenue-aggregation-canary
  namespace: finance-de
  labels:
    track: canary
spec:
  mainApplicationFile: "s3a://your-bucket/jobs/revenue_aggregation-v2.1.0.jar"
  sparkConf:
    # Run on 10% of data (one partition out of 10)
    "spark.sql.files.maxPartitionBytes": "134217728"  # 128MB max per partition
  driver:
    labels:
      track: canary
  executor:
    instances: 2  # minimal executors for canary
    labels:
      track: canary

---
# Validate canary output against production baseline
# If validation passes → update stable SparkApplication to v2.1.0
# If validation fails → delete canary, rollback blocked
```

```python
# Canary validation script (runs as a K8s Job after canary completes)
import boto3
import pandas as pd

def validate_canary_output(
    canary_path: str,
    baseline_path: str,
    tolerance_pct: float = 0.01  # 1% tolerance
) -> bool:
    """Compare canary Spark output to previous day's baseline."""
    s3 = boto3.client("s3")
    
    canary = pd.read_parquet(canary_path)
    baseline = pd.read_parquet(baseline_path)
    
    checks = {
        "row_count_match": abs(len(canary) - len(baseline)) / len(baseline) < tolerance_pct,
        "total_revenue_match": abs(
            canary["revenue"].sum() - baseline["revenue"].sum()
        ) / baseline["revenue"].sum() < tolerance_pct,
        "no_null_keys": canary["order_id"].isna().sum() == 0,
        "date_range_valid": (
            canary["date"].min() == baseline["date"].min() and
            canary["date"].max() == baseline["date"].max()
        ),
    }
    
    all_passed = all(checks.values())
    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"[CANARY_VALIDATION] {check_name}: {status}")
    
    return all_passed
```

---

## JupyterHub at Scale: Keeping Notebooks From Consuming the Cluster

JupyterHub's convenience is a double-edged sword at scale: data scientists leave their notebooks idle for hours, consuming reserved CPU and memory. This is the #1 cost waste on shared notebook platforms.

```yaml
# JupyterHub culling: automatically stop idle notebooks
# config/jupyterhub_config.py

c.JupyterHub.services = [{
    "name": "cull-idle",
    "admin": True,
    "command": [
        "python", "-m", "jupyterhub_idle_culler",
        "--timeout=3600",      # cull after 1 hour idle
        "--cull-every=300",    # check every 5 minutes
        "--max-age=86400",     # cull servers older than 24 hours regardless
        "--remove-named-servers",
    ],
}]

# Profile-based resource tiers (let users self-select based on need)
c.KubeSpawner.profile_list = [
    {
        "display_name": "Small (1 CPU, 2GB RAM) — for exploration",
        "default": True,
        "kubespawner_override": {
            "cpu_limit": 1,
            "mem_limit": "2G",
            "cpu_guarantee": 0.1,
            "mem_guarantee": "512M",
        }
    },
    {
        "display_name": "Medium (4 CPU, 16GB RAM) — for data processing",
        "kubespawner_override": {
            "cpu_limit": 4,
            "mem_limit": "16G",
            "cpu_guarantee": 1,
            "mem_guarantee": "4G",
        }
    },
    {
        "display_name": "Large (8 CPU, 32GB RAM) — requires manager approval",
        "kubespawner_override": {
            "cpu_limit": 8,
            "mem_limit": "32G",
            "cpu_guarantee": 4,
            "mem_guarantee": "16G",
            "extra_annotations": {"requires-approval": "true"},
        }
    },
]
```

**Impact of notebook culling:** On a 100-user JupyterHub platform without culling, ~60% of servers are idle at any time. With 1-hour culling, idle servers drop to ~5%. At $0.20/hr per small server, this saves $10,000-$15,000/month for a 100-user platform.

---

## Data Platform SRE: The Runbook Library

Mature data platform teams maintain runbooks for every common incident. Sample runbook excerpts:

### Runbook: Spark Job Hanging (No Progress)

```bash
# Check if all executors are running
kubectl get pods -n <team-namespace> -l spark-role=executor | wc -l
# Expected: matches spec.executor.instances

# Check Spark UI for stuck stage
kubectl port-forward <driver-pod-name> 4040:4040 -n <team-namespace>
# Open http://localhost:4040/stages — look for stage with 0 active tasks

# Common causes:
# 1. Data skew: one task processing 10x more data than others
#    Solution: check Spark UI → Stages → click the slow stage → Tasks tab
#    repartition data on high-cardinality key before the skewed operation

# 2. External system timeout: Spark waiting on a DB or API call
#    Check: kubectl logs <driver-pod> -n <namespace> | grep "timeout\|connection"

# 3. GC pause: executor spending most time in garbage collection
#    Check: Spark UI → Executors tab → GC Time column
#    Solution: increase executor memory or reduce partition count
```

### Runbook: Flink Consumer Lag Spike

```bash
# Check Kafka consumer lag
kafka-consumer-groups.sh \
  --bootstrap-server kafka.streaming:9092 \
  --describe --group flink-revenue-processor-cg

# Check Flink job status
kubectl get flinkdeployment -n streaming-de
kubectl describe flinkdeployment revenue-stream-processor -n streaming-de

# Trigger a savepoint for safe restart
kubectl edit flinkdeployment revenue-stream-processor -n streaming-de
# Change: job.state: suspended (triggers graceful stop with savepoint)
# Then change back: job.state: running (restarts from savepoint)

# Scale up parallelism if lag is sustained
kubectl patch flinkdeployment revenue-stream-processor -n streaming-de \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/job/parallelism", "value": 64}]'
```

---

## Production Checklist: Data Platform on Kubernetes

```
Cluster Fundamentals
✓ EKS with managed node groups + Karpenter for elasticity
✓ VPC CNI with security groups per pod (SG can reference RDS SG directly)
✓ EKS pod identity / IRSA for all AWS API access — no hardcoded credentials
✓ AWS Load Balancer Controller for Ingress (not nginx for cost/simplicity)

Security
✓ RBAC: ClusterAdmin restricted to platform team; teams get namespace-scoped Roles
✓ Pod Security Standards: Restricted for team namespaces; Baseline for platform
✓ NetworkPolicy: default-deny per namespace; explicit allow for cross-team communication
✓ External Secrets Operator: all secrets from AWS Secrets Manager
✓ Image scanning: ECR scan-on-push; block images with critical CVEs

Cost Controls
✓ ResourceQuotas on every team namespace
✓ LimitRanges with sensible defaults (catch pods without resource specs)
✓ Karpenter consolidation: WhenUnderutilized for batch, WhenEmpty for streaming
✓ Spot instances: 80%+ of batch workload compute on spot
✓ Kubecost/OpenCost: monthly team cost reports + budget alerts

Observability
✓ kube-prometheus-stack installed in monitoring namespace
✓ Spark metrics exported via PrometheusServlet
✓ Flink metrics exported via PrometheusReporter
✓ Alerts: scheduler heartbeat, quota saturation, job failures, Kafka consumer lag
✓ Grafana dashboards: per-team resource usage, job success rates, cost trends

GitOps
✓ ArgoCD: all platform manifests in git; auto-sync enabled
✓ AppProjects: teams can only deploy to their own namespace
✓ Branch protection: main branch requires PR + platform team approval
✓ Image tagging: no 'latest' tags in production; always use commit SHA or semver

Disaster Recovery
✓ etcd backup: AWS Backup or Velero with S3 backend
✓ Terraform: EKS cluster infrastructure reproducible in < 30 minutes
✓ ArgoCD: full platform state restored from git in < 2 hours after cluster recreation
✓ Flink savepoints: taken before any upgrade; retained for 30 days in S3
✓ Airflow metadata DB: RDS Multi-AZ with automated backups + PITR
```

---
