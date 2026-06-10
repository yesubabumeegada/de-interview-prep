---
title: "Clusters and Compute - Senior Deep Dive"
topic: databricks
subtopic: clusters-and-compute
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, clusters, compute, cost-optimization, capacity-planning, production]
---

# Clusters and Compute — Senior-Level Deep Dive

## Capacity Planning

```python
def plan_databricks_capacity(workloads: list[dict]) -> dict:
    """Plan compute capacity for an organization's Databricks workloads."""
    
    total_dbu_per_month = 0
    total_aws_cost = 0
    
    for workload in workloads:
        # Calculate DBUs
        dbu_per_worker_hour = get_dbu_rate(workload["instance_type"])
        workers = workload["avg_workers"]
        hours = workload["hours_per_day"] * 30
        
        monthly_dbus = workers * dbu_per_worker_hour * hours
        
        # DBU cost (Jobs compute vs All-purpose)
        dbu_rate = 0.15 if workload["type"] == "job" else 0.40
        dbu_cost = monthly_dbus * dbu_rate
        
        # AWS instance cost
        hourly_rate = get_instance_hourly(workload["instance_type"])
        spot_discount = 0.3 if workload.get("spot") else 1.0
        aws_cost = workers * hourly_rate * spot_discount * hours
        
        total_dbu_per_month += monthly_dbus
        total_aws_cost += aws_cost + dbu_cost
    
    return {
        "total_monthly_dbus": total_dbu_per_month,
        "total_monthly_cost": total_aws_cost,
        "recommendations": generate_recommendations(workloads),
    }

# Example capacity plan:
workloads = [
    {"name": "ETL pipelines", "type": "job", "instance_type": "i3.xlarge", 
     "avg_workers": 8, "hours_per_day": 6, "spot": True},
    {"name": "SQL Warehouses", "type": "sql", "instance_type": "serverless",
     "avg_workers": 4, "hours_per_day": 10, "spot": False},
    {"name": "ML Training", "type": "job", "instance_type": "g5.xlarge",
     "avg_workers": 4, "hours_per_day": 4, "spot": True},
    {"name": "Interactive dev", "type": "all_purpose", "instance_type": "m5.xlarge",
     "avg_workers": 2, "hours_per_day": 8, "spot": False},
]
```

---

## Cost Optimization Deep Dive

### Identifying Waste

```sql
-- Find over-provisioned clusters (low utilization)
SELECT 
    cluster_id,
    cluster_name,
    avg_cpu_utilization,
    avg_memory_utilization,
    max_workers_configured,
    max_workers_actually_used,
    monthly_cost_estimate
FROM system.compute.cluster_metrics
WHERE time_period = 'last_30_days'
  AND avg_cpu_utilization < 30  -- Under-utilized!
ORDER BY monthly_cost_estimate DESC;

-- Find forgotten all-purpose clusters (running but idle)
SELECT 
    cluster_id, cluster_name, state, 
    creator_user_name,
    TIMESTAMPDIFF(HOUR, last_activity_time, current_timestamp()) as idle_hours,
    num_workers * hourly_rate * idle_hours as wasted_cost
FROM system.compute.clusters
WHERE state = 'RUNNING'
  AND last_activity_time < current_timestamp() - INTERVAL 4 HOURS;
-- Alert: "Cluster X has been idle for 8 hours. Terminate?"
```

### Cost Reduction Playbook

```python
COST_REDUCTION_STRATEGIES = {
    "1_job_clusters": {
        "action": "Switch all-purpose → job clusters for pipelines",
        "savings": "60% per DBU (0.40 → 0.15/DBU)",
        "effort": "Low (config change)",
    },
    "2_spot_instances": {
        "action": "Enable SPOT_WITH_FALLBACK for all job clusters",
        "savings": "60-70% on AWS instance cost",
        "effort": "Low (config change, Spark is fault-tolerant)",
    },
    "3_auto_terminate": {
        "action": "Set 15-30 min idle timeout on all-purpose clusters",
        "savings": "50-80% (eliminate overnight idle)",
        "effort": "Low (policy enforcement)",
    },
    "4_right_size": {
        "action": "Reduce max_workers based on actual utilization data",
        "savings": "20-40% (eliminate over-provisioning)",
        "effort": "Medium (requires analysis of metrics)",
    },
    "5_photon": {
        "action": "Enable Photon (faster execution = less runtime = less cost)",
        "savings": "30-50% (same work in half the time)",
        "effort": "Low (runtime change, slight DBU premium offset by speed)",
    },
    "6_pools": {
        "action": "Use instance pools for frequent job clusters",
        "savings": "Reduced startup time (less billed idle waiting)",
        "effort": "Medium (pool configuration)",
    },
    "7_serverless_sql": {
        "action": "Switch classic SQL warehouse → serverless",
        "savings": "Variable (pay only for queries, not idle time)",
        "effort": "Low (config change)",
    },
}
```

---

## Advanced Autoscaling Strategies

### Optimized Autoscaling for ETL

```python
# Problem: default autoscaling reacts too slowly for bursty workloads
# Solution: tune scaling parameters

{
    "autoscale": {
        "min_workers": 4,
        "max_workers": 32,
    },
    "spark_conf": {
        # Scale up aggressively (don't wait for tasks to queue too long)
        "spark.databricks.aggressiveWindowDownS": "40",  # Scale down after 40s idle
        
        # For streaming: scale based on backlog
        "spark.databricks.streaming.autoScaling.enabled": "true",
    },
}

# Strategy by workload type:
# Batch ETL: autoscale 4-16, let it scale up quickly (peak during transforms)
# Streaming: fixed 8 workers (predictable, avoid scaling latency)
# Interactive: autoscale 1-4 (small, mostly idle, scales for heavy queries)
# ML training: fixed (GPU jobs know exactly what they need)
```

### Dynamic Resource Allocation (within a cluster)

```python
# Enable Spark dynamic allocation (within workers already running)
"spark_conf": {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "12",
    # Different from autoscaling! This manages executors WITHIN existing VMs
    # Autoscaling: adds/removes VMs
    # Dynamic allocation: adds/removes Spark executors on existing VMs
}
```

---

## Cluster Security

### Network Isolation

```python
# VPC-deployed clusters (data doesn't leave your network)
{
    "aws_attributes": {
        "zone_id": "us-east-1a",
        "availability": "SPOT_WITH_FALLBACK",
    },
    # Cluster deployed in customer VPC (not Databricks-managed)
    # Traffic between cluster and S3 stays within VPC (VPC endpoint)
    # No public IP on workers (enhanced security)
}

# IP Access Lists (restrict who can reach the cluster)
# Configured at workspace level — only corporate IPs can connect
```

### Secrets and Credentials

```python
# Never hardcode credentials in notebooks!
# Use Databricks Secrets:
db_password = dbutils.secrets.get(scope="production", key="postgres-password")
api_key = dbutils.secrets.get(scope="production", key="api-key")

# Secrets are:
# - Encrypted at rest (AES-256)
# - Redacted in logs (shown as [REDACTED])
# - Scoped by permission (teams can only access their scope)
# - Backed by AWS Secrets Manager, Azure Key Vault, or Databricks-managed
```

---

## Troubleshooting Common Issues

### Out of Memory (OOM)

```python
# Symptoms: java.lang.OutOfMemoryError, task killed by YARN
# Diagnosis:
# - Spark UI → Executors tab → look for high GC time or killed executors
# - Spark UI → Stages tab → check for large shuffle read

# Fixes (in order of preference):
# 1. More workers (distribute data across more executors)
cluster["autoscale"]["max_workers"] = 16  # Was 8

# 2. Larger instance type (more RAM per executor)
cluster["node_type_id"] = "r5.2xlarge"  # Was m5.xlarge (16→64 GB RAM)

# 3. More partitions (smaller tasks, less memory per task)
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Was 200

# 4. Avoid collect_list / large aggregations on skewed keys
# 5. Broadcast small tables instead of shuffle-joining
```

### Slow Cluster Startup

```python
# Symptoms: job takes 5 min to start before processing
# Causes:
# 1. VM provisioning (cloud provider allocating machines)
# 2. Databricks Runtime initialization
# 3. Library installation (pip install at startup)

# Fixes:
# 1. Instance pools (VMs pre-provisioned, startup < 60s)
cluster["instance_pool_id"] = "pool-xyz"

# 2. Minimize init scripts (move libraries to cluster config)
# 3. Use docker containers with pre-installed libraries
# 4. For Workflows: shared cluster across sequential tasks (one startup for all)
```

---

## Compute Cost Attribution

```sql
-- Monthly cost report by team + workload
SELECT 
    custom_tags.team,
    custom_tags.cost_center,
    SUM(CASE WHEN sku_name = 'JOBS_COMPUTE' THEN usage_quantity * 0.15 ELSE 0 END) AS jobs_cost,
    SUM(CASE WHEN sku_name = 'ALL_PURPOSE_COMPUTE' THEN usage_quantity * 0.40 ELSE 0 END) AS interactive_cost,
    SUM(CASE WHEN sku_name = 'SQL_COMPUTE' THEN usage_quantity * 0.22 ELSE 0 END) AS sql_cost,
    SUM(usage_quantity * 
        CASE sku_name 
            WHEN 'JOBS_COMPUTE' THEN 0.15
            WHEN 'ALL_PURPOSE_COMPUTE' THEN 0.40
            WHEN 'SQL_COMPUTE' THEN 0.22
            ELSE 0.30
        END
    ) AS total_dbu_cost
FROM system.billing.usage
WHERE usage_date >= DATE_TRUNC('month', current_date())
GROUP BY custom_tags.team, custom_tags.cost_center
ORDER BY total_dbu_cost DESC;
```

---

## Interview Tips

> **Tip 1:** "How do you reduce Databricks costs by 50%?" — Five quick wins: (1) Job clusters instead of all-purpose (60% cheaper DBU rate), (2) Spot instances (70% cheaper instances), (3) Auto-terminate idle clusters (eliminate waste), (4) Right-size based on utilization data (reduce over-provisioning), (5) Photon (same work in half the time). Together: easily 50-70% savings.

> **Tip 2:** "How do you troubleshoot OOM in a Databricks job?" — Check Spark UI: Executors tab (GC time, memory usage), Stages tab (shuffle read size, spill). Common fixes: more workers (distribute data), larger instances (more RAM), more shuffle partitions (smaller tasks), avoid collect_list on large groups, broadcast small tables in joins.

> **Tip 3:** "How do you handle compute governance for 100+ users?" — Cluster policies (restrict instance types, enforce spot, limit max workers, require tags), instance pools (standardize available resources), auto-termination policies (prevent forgotten clusters), system table monitoring (detect over-provisioning and idle clusters), and monthly cost chargebacks per team (accountability drives optimization).

## ⚡ Cheat Sheet

**Cluster types**
| Type | Use case | Auto-terminate |
|---|---|---|
| All-purpose | Interactive dev/notebooks | Yes (configurable) |
| Job cluster | Single job run | Always (on completion) |
| SQL warehouse | Databricks SQL queries | Yes |

**Sizing rules**
- Driver = 1 node; bottleneck for `collect()`, large XComs, UDFs — size ≥ largest executor
- Workers: start with 4–8, use autoscale min=2 max=8 × expected peak
- Memory per core: 4–8 GB for standard ETL; 16+ GB for ML
- Delta cache: SSD-backed; use `cache=true` nodes for repeated scans

**Runtimes**
- DBR (Databricks Runtime): optimized Spark + Delta
- ML Runtime: includes MLflow, sklearn, TF, PyTorch pre-installed
- Photon: vectorized C++ engine; best for SQL/scan-heavy; not for Python UDFs
- GPU runtime: required for deep learning; use spot for training, on-demand for serving

**Autoscaling**
- Scale-up: triggered when all executors busy for 2 scheduler backlogs
- Scale-down: executor idle > `spark.databricks.aggressiveWindowDownS` (default 600s)
- Enhanced autoscaling (SQL warehouses): scales in ~30s vs ~4min for job clusters

**Cost optimization**
- Spot/preemptible: 60–80% cheaper; use for non-SLA batch jobs; avoid for streaming
- Instance pools: pre-warmed instances → reduce cluster start from 5min to 30s
- Photon: same price as DBR, 2–3× faster for SQL → lower cost per query

**Common gotchas**
- Single-node cluster: driver IS executor; no parallelism; only for small datasets
- `spark.executor.cores` default = all vCPUs; reduce to 4–5 to avoid memory pressure
- High shuffle partitions (>5000) = small task overhead; use AQE coalescing
