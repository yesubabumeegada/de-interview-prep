---
title: "Cost Management - Intermediate"
topic: databricks
subtopic: cost-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, cost, system-tables, dbu-monitoring, cost-attribution, optimization]
---

# Cost Management — Intermediate Concepts

## Monitoring Costs with System Tables

Databricks exposes usage data in `system.billing.*` tables:

```sql
-- Daily DBU usage by workload type
SELECT
    DATE_TRUNC('day', usage_start_time) AS day,
    sku_name,
    SUM(usage_quantity) AS total_dbus,
    ROUND(SUM(usage_quantity) * 0.40, 2) AS est_cost_usd  -- adjust for your rate
FROM system.billing.usage
WHERE usage_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, total_dbus DESC;

-- Usage by workspace (multi-workspace accounts)
SELECT
    workspace_id,
    sku_name,
    SUM(usage_quantity) AS total_dbus,
    ROUND(SUM(usage_quantity) * 0.40, 2) AS est_cost_usd
FROM system.billing.usage
WHERE usage_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY est_cost_usd DESC;

-- Top clusters by cost this week
SELECT
    usage_metadata.cluster_id AS cluster_id,
    usage_metadata.cluster_name AS cluster_name,
    SUM(usage_quantity) AS total_dbus,
    ROUND(SUM(usage_quantity) * 0.40, 2) AS est_cost_usd
FROM system.billing.usage
WHERE usage_start_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
  AND usage_metadata.cluster_id IS NOT NULL
GROUP BY 1, 2
ORDER BY total_dbus DESC
LIMIT 20;
```

---

## Cost Attribution (Chargeback)

Tag clusters and jobs to attribute cost to teams/projects:

```python
# Set cluster tags for cost attribution
cluster_config = {
    "cluster_name": "marketing-etl",
    "custom_tags": {
        "team": "marketing",
        "project": "customer-segmentation",
        "cost_center": "MKTG-001",
        "jira_ticket": "MKTG-442"
    },
    ...
}

# Query cost by team
spark.sql("""
    SELECT
        usage_metadata.cluster_source_tags.team AS team,
        usage_metadata.cluster_source_tags.project AS project,
        SUM(usage_quantity) AS total_dbus,
        ROUND(SUM(usage_quantity) * 0.40, 2) AS est_cost_usd
    FROM system.billing.usage
    WHERE usage_start_time >= DATE_TRUNC('month', CURRENT_DATE())
      AND usage_metadata.cluster_source_tags.team IS NOT NULL
    GROUP BY 1, 2
    ORDER BY est_cost_usd DESC
""")
```

---

## Identifying Optimization Opportunities

```sql
-- Clusters with high idle time (auto-termination not working)
SELECT
    u.usage_metadata.cluster_id,
    u.usage_metadata.cluster_name,
    SUM(u.usage_quantity) AS total_dbus,
    -- Estimate idle DBUs: clusters running but no jobs
    SUM(CASE WHEN j.job_id IS NULL THEN u.usage_quantity ELSE 0 END) AS idle_dbus
FROM system.billing.usage u
LEFT JOIN system.lakeflow.job_run_timeline j
    ON u.usage_metadata.cluster_id = j.cluster_id
    AND u.usage_start_time BETWEEN j.start_time AND j.end_time
WHERE u.usage_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND u.sku_name LIKE '%ALL_PURPOSE%'
GROUP BY 1, 2
HAVING idle_dbus > 10
ORDER BY idle_dbus DESC;

-- Jobs that ran longer than expected (potential optimization targets)
SELECT
    job_id,
    run_id,
    duration_seconds / 60 AS duration_minutes,
    result_state,
    trigger_type
FROM system.lakeflow.job_runs
WHERE period_start_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
  AND duration_seconds > 3600   -- more than 1 hour
ORDER BY duration_seconds DESC
LIMIT 20;

-- Storage costs: tables not accessed in 90 days
SELECT
    table_catalog,
    table_schema,
    table_name,
    ROUND(data_length / 1e9, 2) AS size_gb,
    last_accessed_at
FROM system.information_schema.tables
WHERE table_catalog = 'prod'
  AND (last_accessed_at < DATEADD(day, -90, CURRENT_TIMESTAMP())
       OR last_accessed_at IS NULL)
ORDER BY size_gb DESC;
```

---

## Autoscaling Configuration

Autoscaling adjusts worker count based on workload — cheaper than over-provisioning:

```python
# Cluster config with autoscaling
cluster_config = {
    "cluster_name": "analytics-autoscale",
    "autoscale": {
        "min_workers": 2,   # minimum (always running for fast start)
        "max_workers": 20   # maximum (never exceeds this)
    },
    "node_type_id": "i3.xlarge",
    "autotermination_minutes": 30,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    }
}

# Enhanced autoscaling (better algorithm — enabled by default for job clusters)
cluster_config["spark_conf"]["spark.databricks.adaptive.autoscaling.enabled"] = "true"
```

**Autoscaling tips:**
- Set min_workers high enough to avoid cold start (2-4 is typical)
- Max should reflect peak, not average — you pay for max only when needed
- Enhanced autoscaling is more aggressive at scaling down → saves more on batch jobs

---

## Serverless Compute: Pay Per Query

For SQL warehouses, serverless eliminates idle cluster cost:

```sql
-- Create a serverless SQL warehouse (pays only for query time)
-- Via UI: SQL Warehouses → Create → Type: Serverless

-- Compare cost models:
-- Classic warehouse: you pay for warehouse uptime (even idle)
-- Serverless:        you pay only for query execution time
-- Break-even: if warehouse is idle > 30% of the time, serverless is cheaper
```

```python
# Serverless also available for Workflows (job clusters)
# In job config:
{
    "job_clusters": [{
        "job_cluster_key": "main",
        "new_cluster": {
            "kind": "classic",  # or "serverless"
            ...
        }
    }]
}
```

---

## Delta Cache Optimization

Delta cache stores recently accessed Parquet data on local executor SSDs:

```python
# Enable Delta IO cache (Databricks Runtime — enabled by default on storage-optimized instances)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Cache size is determined by available local SSD on the instance type
# i3.xlarge: 950GB NVMe — ideal for Delta cache
# r5.xlarge: no local SSD — Delta cache not effective

# Force-cache specific tables for repeated access
spark.sql("CACHE SELECT * FROM prod.sales.orders WHERE order_date >= '2024-01-01'")

# View cache hit rate in the Spark UI
# Stage metrics → "Local Bytes Read" (from cache) vs "Remote Bytes Read" (from S3)
# Target: > 70% local for repeated queries on the same cluster
```

---

## Interview Tips

> **Tip 1:** "How do you track Databricks costs by team?" — "Use cluster tags — add `team`, `project`, `cost_center` tags to every cluster and job cluster. Then query `system.billing.usage` joining on `usage_metadata.cluster_source_tags.team`. Generate a monthly chargeback report by team. Enforce tagging via cluster policies that require these fields."

> **Tip 2:** "What's the difference between a Classic SQL Warehouse and Serverless?" — "Classic warehouses are pre-provisioned compute — you pay for the warehouse uptime even when idle. Serverless warehouses have no idle cost — you pay only for query execution time. Serverless is cheaper when query utilization is < 70% of warehouse uptime. Classic is cheaper for always-on workloads like continuous dashboards."

> **Tip 3:** "How does autoscaling save money?" — "Autoscaling adds workers when tasks are queued and removes them when they complete — matching compute to actual workload. A fixed 10-node cluster running a 1-hour ETL uses 10 DBU/hr × 1 hr = 10 DBUs. Autoscaling (2-10 nodes) might use 2 DBUs at start, scale to 10 for the heavy phase, then back to 2 at end — total 6-7 DBUs instead of 10. Bigger savings on jobs with variable data volume."
