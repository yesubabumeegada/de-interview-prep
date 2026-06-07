---
title: "Clusters and Compute - Real-World Production Examples"
topic: databricks
subtopic: clusters-and-compute
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, clusters, compute, production, cost, governance]
---

# Clusters and Compute — Real-World Production Examples

## Pattern 1: Enterprise Compute Architecture

```python
# Organization with 200 users across 5 teams
COMPUTE_LAYOUT = {
    "shared_interactive": {
        "purpose": "Data exploration and development",
        "config": {
            "cluster_name": "shared-dev",
            "node_type_id": "m5.xlarge",
            "autoscale": {"min_workers": 1, "max_workers": 4},
            "autotermination_minutes": 30,
            "spark_version": "14.3.x-photon-scala2.12",
        },
        "policy": "dev-policy (max 4 workers, 30 min timeout)",
        "users": "All data team members",
        "monthly_cost": "~$800 (mostly idle)",
    },
    "etl_job_clusters": {
        "purpose": "Scheduled production pipelines",
        "config": {
            "node_type_id": "i3.xlarge",
            "autoscale": {"min_workers": 4, "max_workers": 16},
            "aws_attributes": {"availability": "SPOT_WITH_FALLBACK", "first_on_demand": 1},
            "spark_version": "14.3.x-photon-scala2.12",
        },
        "policy": "production-etl-policy",
        "users": "Workflows only (no human access)",
        "monthly_cost": "~$3,500 (6 hrs/day active)",
    },
    "sql_warehouses": {
        "small": {"purpose": "Analyst ad-hoc queries", "size": "Small", "auto_stop": "10 min"},
        "medium": {"purpose": "Dashboard refresh", "size": "Medium", "auto_stop": "15 min"},
        "large": {"purpose": "Heavy reports (monthly)", "size": "Large", "auto_stop": "10 min"},
        "monthly_cost": "~$2,000 (serverless, pay per query)",
    },
    "ml_clusters": {
        "purpose": "Model training (GPU)",
        "config": {
            "node_type_id": "g5.xlarge",
            "num_workers": 4,
            "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"},
        },
        "policy": "ml-policy (max 8 GPU workers, requires approval for >4)",
        "users": "ML team",
        "monthly_cost": "~$1,500 (4 hrs/day)",
    },
    "total_monthly": "~$7,800",
}
```

---

## Pattern 2: Cost Optimization Case Study

```python
# BEFORE: $18K/month Databricks spend
# Problems identified:
# 1. 3 all-purpose clusters running 24/7 (teams forgot to stop them)
# 2. ETL running on all-purpose clusters (expensive DBU rate)
# 3. No spot instances anywhere
# 4. SQL warehouse always-on (Classic, not serverless)
# 5. Over-provisioned clusters (16 workers configured, peak usage: 6)

# AFTER: $5.5K/month (69% reduction!)
OPTIMIZATIONS_APPLIED = [
    {
        "change": "All-purpose → Job clusters for ETL",
        "before": "$4,800/month",
        "after": "$1,800/month",
        "savings": "$3,000 (DBU rate: $0.40 → $0.15)",
    },
    {
        "change": "Enable spot instances on all job clusters",
        "before": "$1,800/month (on-demand AWS)",
        "after": "$540/month (spot)",
        "savings": "$1,260 (70% instance cost reduction)",
    },
    {
        "change": "Auto-terminate all-purpose + policy enforcement",
        "before": "$5,400/month (3 clusters × 24/7)",
        "after": "$800/month (same clusters, 30 min timeout)",
        "savings": "$4,600 (clusters only run when actively used)",
    },
    {
        "change": "Classic SQL warehouse → Serverless",
        "before": "$3,200/month (always-on Medium warehouse)",
        "after": "$1,200/month (serverless, pay per query)",
        "savings": "$2,000 (only pay during active queries)",
    },
    {
        "change": "Right-size clusters (16 → 8 max workers)",
        "before": "$3,800/month",
        "after": "$1,900/month",
        "savings": "$1,900 (half the compute, same performance)",
    },
]
# Total savings: $12,760/month → from $18K to $5.5K
```

---

## Pattern 3: Cluster Pool Management

```python
# Scenario: 15 hourly ETL jobs, each taking 5-10 minutes
# Without pools: each job waits 4-5 min for cluster to start
# Total wasted time: 15 × 4 min = 60 min/hour wasted on startup!

POOL_SETUP = {
    "pool_name": "etl-hourly-pool",
    "instance_type": "i3.xlarge",
    "min_idle": 4,        # Always keep 4 VMs warm
    "max_capacity": 16,   # Can burst to 16
    "idle_timeout": 15,   # Remove idle VMs after 15 min (keep min_idle)
}

# With pools: startup drops from 4-5 min to 30-60 seconds
# Net improvement: 15 jobs × 4 min saved = 60 min/hour
# Pool cost: 4 idle instances × $0.312/hr = $1.25/hr
# Savings: $1.25/hr pool cost vs reduced job runtime and happier SLAs

# Pool best practices:
# - One pool per instance type (don't mix i3 and r5)
# - min_idle based on average concurrent jobs (not max)
# - Set idle_timeout to match your schedule gap (15 min for hourly jobs)
# - Monitor pool utilization: if min_idle VMs are rarely used, reduce
```

---

## Pattern 4: Monitoring Dashboard

```sql
-- Build a Grafana/Databricks dashboard for compute health

-- Panel 1: Active clusters and cost (real-time)
SELECT 
    cluster_name, state, num_workers, 
    node_type_id,
    TIMESTAMPDIFF(HOUR, start_time, current_timestamp()) as running_hours,
    num_workers * 0.312 * TIMESTAMPDIFF(HOUR, start_time, current_timestamp()) as aws_cost_running
FROM system.compute.clusters
WHERE state = 'RUNNING'
ORDER BY aws_cost_running DESC;

-- Panel 2: Daily compute spend trend
SELECT 
    usage_date,
    SUM(CASE WHEN sku_name = 'JOBS_COMPUTE' THEN usage_quantity * 0.15 ELSE 0 END) as jobs_spend,
    SUM(CASE WHEN sku_name = 'ALL_PURPOSE_COMPUTE' THEN usage_quantity * 0.40 ELSE 0 END) as interactive_spend,
    SUM(CASE WHEN sku_name = 'SQL_COMPUTE' THEN usage_quantity * 0.22 ELSE 0 END) as sql_spend
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
GROUP BY usage_date
ORDER BY usage_date;

-- Panel 3: Top cost offenders (which jobs cost the most)
SELECT 
    j.settings.name as job_name,
    j.settings.tags.team as team,
    COUNT(r.run_id) as runs_this_month,
    SUM(r.run_duration_ms / 3600000.0 * 8 * 0.15) as estimated_monthly_dbu_cost
FROM system.lakeflow.jobs j
JOIN system.lakeflow.job_run_timeline r ON j.job_id = r.job_id
WHERE r.start_time >= DATE_TRUNC('month', current_date())
GROUP BY j.settings.name, j.settings.tags.team
ORDER BY estimated_monthly_dbu_cost DESC
LIMIT 20;

-- Panel 4: Cluster utilization efficiency
-- Alert if clusters are consistently under-utilized
```

---

## Pattern 5: Automated Cost Governance

```python
# Automated checks that run daily to enforce cost policies

class ComputeGovernance:
    def daily_checks(self):
        violations = []
        
        # Check 1: All-purpose clusters running > 12 hours
        long_running = spark.sql("""
            SELECT cluster_name, creator, 
                   TIMESTAMPDIFF(HOUR, start_time, current_timestamp()) as hours_running
            FROM system.compute.clusters
            WHERE state = 'RUNNING' AND cluster_source = 'UI'
              AND TIMESTAMPDIFF(HOUR, start_time, current_timestamp()) > 12
        """).collect()
        
        for cluster in long_running:
            violations.append(f"Cluster '{cluster.cluster_name}' by {cluster.creator} running {cluster.hours_running}h")
            # Auto-terminate if > 24 hours (safety net)
            if cluster.hours_running > 24:
                self.terminate_cluster(cluster.cluster_id)
        
        # Check 2: Jobs using all-purpose compute (should be job compute)
        expensive_jobs = spark.sql("""
            SELECT job_name, sku_name, SUM(usage_quantity) as dbus
            FROM system.billing.usage u
            JOIN system.lakeflow.jobs j ON ...
            WHERE sku_name = 'ALL_PURPOSE_COMPUTE' AND usage_date = current_date() - 1
            GROUP BY job_name, sku_name
            HAVING dbus > 10
        """).collect()
        
        for job in expensive_jobs:
            violations.append(f"Job '{job.job_name}' using all-purpose compute ({job.dbus} DBUs). Switch to jobs compute!")
        
        # Check 3: Budget threshold (80% alert)
        month_spend = spark.sql("""
            SELECT SUM(usage_quantity * 0.25) as total_spend
            FROM system.billing.usage
            WHERE usage_date >= DATE_TRUNC('month', current_date())
        """).collect()[0]["total_spend"]
        
        budget = 8000  # Monthly budget
        if month_spend > budget * 0.8:
            violations.append(f"Monthly spend at ${month_spend:.0f} ({month_spend/budget*100:.0f}% of ${budget} budget)")
        
        if violations:
            self.send_alert(violations)
        
        return violations
```

---

## Interview Tips

> **Tip 1:** "How do you save 50%+ on Databricks compute?" — Five proven strategies: job clusters (60% cheaper DBU), spot instances (70% cheaper VMs), auto-terminate (eliminate idle), right-size (match actual utilization), serverless SQL (pay per query). Applied together: typical savings of 50-70% from unoptimized baselines.

> **Tip 2:** "How do you monitor compute costs in an enterprise?" — System billing tables for cost attribution (per team, per job), cluster metrics for utilization (identify waste), automated governance checks (long-running clusters, all-purpose for jobs), monthly chargeback reports (accountability drives optimization), and budget alerts at 80% threshold.

> **Tip 3:** "Instance pools — when and why?" — Use pools when jobs run frequently (every 15-60 min) and cluster startup time (4-5 min) is a significant fraction of total runtime. Pools keep VMs warm (instant startup, 30-60s). Cost: you pay for idle instances in the pool. Break-even: if startup savings > idle cost. For hourly jobs on i3.xlarge: pool definitely worth it.
