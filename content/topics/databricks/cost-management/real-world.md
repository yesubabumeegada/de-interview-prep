---
title: "Cost Management - Real-World Examples"
topic: databricks
subtopic: cost-management
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, cost, production, finops, optimization, chargeback]
---

# Cost Management — Real-World Production Examples

## Production Pattern: Surprise Bill Investigation

A startup received a $28,000 Databricks bill (was normally $4,000/month). Investigation revealed three causes:

**Cause 1: Developer left a large cluster running over a long weekend**

```python
# Diagnosis: cluster usage spike on Friday-Monday
spike = spark.sql("""
    SELECT
        usage_metadata.cluster_name,
        DATE_TRUNC('day', usage_start_time) AS day,
        SUM(usage_quantity) AS daily_dbus
    FROM system.billing.usage
    WHERE usage_start_time BETWEEN '2024-03-15' AND '2024-03-19'  -- the weekend
    GROUP BY 1, 2
    ORDER BY daily_dbus DESC
""").collect()

# Found: "alice-analysis-cluster" running 16-node cluster over 3.5 days
# Cost: 16 DBU/hr × 84 hours × $0.40 = $537.60 just that cluster

# Fix: Enforce 60-min auto-termination via cluster policy
spark.sql("""
    UPDATE admin.cluster_policies
    SET definition = JSON_SET(definition, '$.autotermination_minutes.maxValue', 60)
    WHERE policy_name = 'engineer-standard'
""")
```

**Cause 2: ML training job accidentally ran 50 nodes instead of 5**

```python
# Bug in notebook: wrong variable name
# num_workers = training_data_size_gb  # = 50 (GB), not 5 (workers)
# This ran a 50-node cluster for 12 hours

# Cost: 50 × 12 × $0.40 = $240 instead of 5 × 12 × $0.40 = $24

# Fix: Add sanity check to cluster creation
MAX_WORKERS_ALLOWED = {
    "dev": 20,
    "staging": 30,
    "prod": 100
}

def create_cluster_with_guard(env: str, num_workers: int, **kwargs):
    limit = MAX_WORKERS_ALLOWED.get(env, 20)
    if num_workers > limit:
        raise ValueError(f"num_workers={num_workers} exceeds limit={limit} for env={env}")
    return w.clusters.create(num_workers=num_workers, **kwargs)
```

**Cause 3: Streaming job restarted 800 times in 2 days (runaway retry)**

```python
# Structured Streaming job failing, retrying every 3 minutes
# Each restart = new job cluster = ~0.5 DBU per restart
# 800 restarts × 0.5 DBU × $0.40 = $160

# Diagnosis
spark.sql("""
    SELECT COUNT(*) AS restart_count, MIN(start_time), MAX(end_time)
    FROM system.lakeflow.job_runs
    WHERE job_id = 12345
      AND start_time >= DATEADD(day, -3, CURRENT_TIMESTAMP())
      AND result_state = 'FAILED'
""")

# Fix: add max retry limit and backoff
job_config = {
    "max_concurrent_runs": 1,
    "tasks": [{
        "retry_on_timeout": False,
        "max_retries": 3,    # was: unlimited
        "min_retry_interval_millis": 300000  # 5-minute backoff
    }]
}
```

**Total savings after fixes: $28K → $4.5K the following month.**

---

## Production Pattern: 40% Cost Reduction Without Performance Impact

A logistics company systematically reduced Databricks costs over 2 months:

**Audit findings:**
```python
# Week 1: Identify waste
idle_clusters = spark.sql("""
    SELECT
        u.usage_metadata.cluster_name,
        SUM(u.usage_quantity) AS total_dbus,
        ROUND(SUM(u.usage_quantity) * 0.40, 2) AS est_cost_usd,
        COUNT(DISTINCT j.run_id) AS job_runs
    FROM system.billing.usage u
    LEFT JOIN system.lakeflow.job_runs j
        ON u.usage_metadata.cluster_id = j.cluster_id
    WHERE u.usage_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
      AND u.sku_name LIKE '%ALL_PURPOSE%'
    GROUP BY 1
    HAVING job_runs = 0  -- clusters with no jobs — pure idle waste
    ORDER BY est_cost_usd DESC
""")

# Found: 12 "team" clusters that nobody was actively using
# Idle cost: $1,800/month
```

**Optimization actions:**

```python
# 1. Convert all ETL jobs from all-purpose to job clusters
# Savings: 50% DBU rate reduction on job workloads
# Before: $3,200/month (all-purpose rate)
# After:  $1,600/month (job compute rate)

# 2. Enable spot instances for all non-streaming clusters
# Before: 100% on-demand
# After:  85% spot, 15% on-demand
# Savings: ~60% on EC2 costs = $1,100/month

# 3. Tighten auto-termination from 120 min to 30 min
# Eliminated idle weekend cost
# Savings: $800/month

# 4. Enable Photon on SQL-heavy jobs
# 2.5x speedup → 60% DBU reduction on those jobs
# Savings: $600/month

total_monthly_savings = 1800 + 1600 + 800 + 600  # $4,800/month
print(f"Monthly savings: ${total_monthly_savings:,} (40% reduction)")
```

---

## Production Pattern: Team Chargeback System

A 200-person company implemented monthly cost chargeback to 12 product teams:

```python
# Monthly chargeback report generation (runs on 1st of each month)
monthly_chargeback = spark.sql("""
    SELECT
        usage_metadata.cluster_source_tags.team AS team,
        usage_metadata.cluster_source_tags.project AS project,
        ROUND(SUM(usage_quantity) * 0.40, 2) AS compute_cost_usd,
        ROUND(
            SUM(CASE WHEN sku_name LIKE '%STORAGE%' THEN usage_quantity * 0.023 ELSE 0 END), 2
        ) AS storage_cost_usd,
        ROUND(SUM(usage_quantity) * 0.40 +
              SUM(CASE WHEN sku_name LIKE '%STORAGE%' THEN usage_quantity * 0.023 ELSE 0 END), 2
        ) AS total_cost_usd
    FROM system.billing.usage
    WHERE usage_start_time >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE()))
      AND usage_start_time < DATE_TRUNC('month', CURRENT_DATE())
      AND usage_metadata.cluster_source_tags.team IS NOT NULL
    GROUP BY 1, 2
    ORDER BY total_cost_usd DESC
""")

display(monthly_chargeback)

# Save to chargeback table for finance team
monthly_chargeback.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("finance.chargeback.databricks_monthly")

# Send each team their cost summary via Slack
for row in monthly_chargeback.collect():
    send_slack_message(
        channel=f"#team-{row['team']}-data",
        message=(
            f"📊 Databricks cost report for {row['team']}/{row['project']} — last month:\n"
            f"  Compute: ${row['compute_cost_usd']:,.2f}\n"
            f"  Storage: ${row['storage_cost_usd']:,.2f}\n"
            f"  Total: ${row['total_cost_usd']:,.2f}\n"
            f"  Budget: see #finops-channel for your team's monthly budget"
        )
    )
```

**Outcome:** Teams became cost-conscious immediately after seeing their bill. Month-over-month spend per team dropped 18% on average — without any top-down mandates. Accountability drove optimization.

---

## Cost Monitoring Checklist

```python
# Daily monitoring job (runs at 8am via Databricks Workflow)

def daily_cost_health_check():
    checks = {}

    # 1. Yesterday's cost vs 30-day average
    cost_data = spark.sql("""
        WITH daily AS (
            SELECT DATE_TRUNC('day', usage_start_time) AS day,
                   SUM(usage_quantity) AS dbus
            FROM system.billing.usage
            WHERE usage_start_time >= DATEADD(day, -31, CURRENT_TIMESTAMP())
            GROUP BY 1
        )
        SELECT
            MAX(CASE WHEN day = DATEADD(day, -1, CURRENT_DATE()) THEN dbus END) AS yesterday,
            AVG(CASE WHEN day < DATEADD(day, -1, CURRENT_DATE()) THEN dbus END) AS avg_30d
        FROM daily
    """).collect()[0]

    if cost_data["yesterday"] > cost_data["avg_30d"] * 1.5:
        checks["cost_spike"] = f"ALERT: yesterday {cost_data['yesterday']:.0f} DBU vs {cost_data['avg_30d']:.0f} avg"

    # 2. Any clusters without auto-termination
    no_autoterminate = spark.sql("""
        SELECT cluster_name FROM system.compute.clusters
        WHERE autotermination_minutes IS NULL OR autotermination_minutes > 120
          AND cluster_source = 'UI'  -- interactive clusters only
    """).collect()
    if no_autoterminate:
        checks["no_autoterminate"] = f"WARNING: {len(no_autoterminate)} clusters without proper auto-termination"

    # 3. Spot adoption rate (should be > 70%)
    spot_rate = spark.sql("""
        SELECT
            SUM(CASE WHEN sku_name LIKE '%SPOT%' THEN usage_quantity ELSE 0 END) /
            SUM(usage_quantity) AS spot_rate
        FROM system.billing.usage
        WHERE usage_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    """).collect()[0]["spot_rate"]
    if spot_rate < 0.70:
        checks["low_spot_rate"] = f"WARNING: spot adoption {spot_rate:.0%} < 70% target"

    if checks:
        for check, message in checks.items():
            send_alert(message)
    else:
        print("Daily cost health check: all OK")

daily_cost_health_check()
```
