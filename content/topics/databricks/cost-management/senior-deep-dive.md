---
title: "Cost Management - Senior Deep Dive"
topic: databricks
subtopic: cost-management
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, cost, finops, budget-alerts, optimization, unit-economics]
---

# Cost Management — Senior Deep Dive

## FinOps Framework for Databricks

FinOps = financial accountability for cloud spend. Three pillars applied to Databricks:

```
1. INFORM (visibility)
   → system.billing.usage → monthly cost by team/project
   → Anomaly detection → alert when daily cost > 2x rolling average

2. OPTIMIZE (right-sizing)
   → Cluster efficiency scores → jobs-per-dollar
   → Spot adoption rate → % of compute using spot
   → Idle time analysis → DBUs consumed with no active job

3. OPERATE (governance)
   → Cluster policies → prevent oversized clusters
   → Budget alerts → stop overspend before month-end surprise
   → Chargeback → teams accountable for their own cost
```

---

## Budget Alerts and Anomaly Detection

```sql
-- Daily cost anomaly detection (z-score based)
WITH daily_costs AS (
    SELECT
        DATE_TRUNC('day', usage_start_time) AS day,
        SUM(usage_quantity) AS daily_dbus
    FROM system.billing.usage
    WHERE usage_start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),
stats AS (
    SELECT
        AVG(daily_dbus) AS avg_dbus,
        STDDEV(daily_dbus) AS std_dbus
    FROM daily_costs
    WHERE day < CURRENT_DATE()
),
today AS (
    SELECT MAX(daily_dbus) AS today_dbus
    FROM daily_costs
    WHERE day = CURRENT_DATE()
)
SELECT
    today.today_dbus,
    stats.avg_dbus,
    ROUND((today.today_dbus - stats.avg_dbus) / stats.std_dbus, 2) AS z_score,
    CASE WHEN (today.today_dbus - stats.avg_dbus) / stats.std_dbus > 2
         THEN 'ANOMALY — investigate'
         ELSE 'Normal'
    END AS status
FROM today, stats;
```

```python
# Automated budget enforcement (run as scheduled job)
def check_monthly_budget(budget_usd: float, dbu_rate: float):
    """Suspend most expensive clusters if monthly budget is 80% consumed."""

    result = spark.sql(f"""
        SELECT
            SUM(usage_quantity) * {dbu_rate} AS month_cost_usd
        FROM system.billing.usage
        WHERE usage_start_time >= DATE_TRUNC('month', CURRENT_DATE())
    """).collect()[0]

    month_cost = result["month_cost_usd"]
    pct_consumed = month_cost / budget_usd

    if pct_consumed >= 1.0:
        # Hard cap: terminate all non-production clusters
        w = WorkspaceClient()
        for cluster in w.clusters.list():
            if "prod" not in cluster.cluster_name.lower():
                w.clusters.delete(cluster_id=cluster.cluster_id)
                print(f"Terminated: {cluster.cluster_name}")
        send_alert(f"BUDGET EXCEEDED: ${month_cost:.0f} / ${budget_usd:.0f} ({pct_consumed:.0%})")

    elif pct_consumed >= 0.8:
        send_alert(f"BUDGET WARNING: {pct_consumed:.0%} of monthly budget consumed (${month_cost:.0f})")
```

---

## Unit Economics: Cost Per Business Outcome

Go beyond "total DBUs" — measure cost per meaningful unit:

```python
# Cost per 1M rows processed
pipeline_runs = spark.sql("""
    SELECT
        j.job_id,
        j.run_name,
        j.duration_seconds / 3600 AS duration_hours,
        u.total_dbus,
        u.total_dbus * 0.40 AS cost_usd,
        m.rows_processed,
        ROUND(u.total_dbus * 0.40 / (m.rows_processed / 1e6), 4) AS cost_per_million_rows
    FROM system.lakeflow.job_runs j
    JOIN (
        SELECT usage_metadata.job_id, SUM(usage_quantity) AS total_dbus
        FROM system.billing.usage
        WHERE usage_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
        GROUP BY 1
    ) u ON j.job_id = u.job_id
    JOIN prod.ops.pipeline_metrics m ON j.run_id = m.run_id
    WHERE j.period_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
    ORDER BY j.period_start_time DESC
""")

display(pipeline_runs)

# Target: $0.05 per million rows for batch ETL
# Red flag: > $0.50 per million rows (5x → investigate)
```

---

## Storage Cost Optimization

Storage is often overlooked — Delta tables accumulate old files:

```python
# Identify tables with high storage cost and low access frequency
storage_analysis = spark.sql("""
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        ROUND(t.data_length / 1e9, 2) AS size_gb,
        ROUND(t.data_length / 1e9 * 0.023, 4) AS est_monthly_storage_usd,  -- S3 rate
        t.last_accessed_at,
        DATEDIFF(day, t.last_accessed_at, CURRENT_DATE()) AS days_since_access
    FROM system.information_schema.tables t
    WHERE t.table_catalog = 'prod'
      AND t.data_length > 1e10  -- > 10GB
    ORDER BY t.data_length DESC
""")

# Tables with high storage cost + no recent access → candidates for archival
storage_analysis.filter("days_since_access > 90 AND size_gb > 100").display()
```

**Storage optimization levers:**
```sql
-- 1. VACUUM to remove old Delta versions (reduces file count and storage)
VACUUM prod.logs.raw_events RETAIN 168 HOURS;   -- 7 days retention

-- 2. Convert large text columns to more compact types
-- (String → Integer where possible; use ZSTD compression)
ALTER TABLE prod.events.clicks
SET TBLPROPERTIES ('delta.parquet.compression.codec' = 'zstd');

-- 3. Liquid clustering reduces write amplification (less storage growth over time)
ALTER TABLE prod.events.clicks CLUSTER BY (user_id, event_date);

-- 4. Archive old partitions to cheaper storage tier
-- Move data older than 1 year to S3 Glacier via lifecycle policy
```

---

## Managed vs External Tables: Cost Implications

```sql
-- Managed tables: Databricks manages storage lifecycle
-- External tables: you manage the S3/ADLS location

-- Cost impact:
-- Managed: DROP TABLE removes all data — careful! Recoverable only via Time Travel
-- External: DROP TABLE removes metadata only — data stays in your S3 bucket

-- For cost control: use managed tables in Unity Catalog
-- They support fine-grained VACUUM and retention policies
-- External tables can have orphaned files that don't get cleaned up

-- Check for external tables with orphaned files
SELECT
    table_catalog, table_schema, table_name, table_type, location
FROM system.information_schema.tables
WHERE table_type = 'EXTERNAL'
  AND table_catalog = 'prod';
-- For each: verify S3 lifecycle policy is set or files will accumulate
```

---

## Photon Cost-Benefit Analysis

```python
# Photon costs more per DBU but processes more work per DBU
# Break-even analysis:

baseline_query_time_hours = 2.0      # without Photon
photon_query_time_hours = 0.8        # with Photon (typical 2.5x speedup for SQL)

cluster_dbu_rate = 8.0               # 8 DBUs/hr for the cluster
photon_dbu_premium = 1.0             # Photon adds 1 DBU/hr (example)

baseline_cost = baseline_query_time_hours * cluster_dbu_rate * 0.40
photon_cost = photon_query_time_hours * (cluster_dbu_rate + photon_dbu_premium) * 0.40

print(f"Without Photon: {baseline_cost:.2f} USD ({baseline_query_time_hours}hr)")
print(f"With Photon: {photon_cost:.2f} USD ({photon_query_time_hours}hr)")
print(f"Savings: {baseline_cost - photon_cost:.2f} USD ({(1 - photon_cost/baseline_cost)*100:.0f}%)")

# Output:
# Without Photon: 6.40 USD (2.0hr)
# With Photon: 2.88 USD (0.8hr)
# Savings: 3.52 USD (55%)

# Photon is almost always worth it for SQL-heavy workloads
# Doesn't help (and costs more) for Python UDF-heavy workloads
```

---

## Interview Tips

> **Tip 1:** "How would you reduce Databricks spend by 30% without affecting performance?" — "Three levers: (1) Spot instances — convert all-purpose clusters to SPOT_WITH_FALLBACK, saves 50-70% on compute. (2) Job clusters — move all scheduled pipelines from all-purpose to job clusters, saves ~50% DBU rate. (3) Auto-termination — enforce 30-min auto-termination on all interactive clusters via policy. Together these typically achieve 30-50% reduction."

> **Tip 2:** "How do you implement chargeback for Databricks costs?" — "Tag every cluster and job with `team` and `project` custom tags. Query `system.billing.usage` joining on `usage_metadata.cluster_source_tags.team`. Generate a monthly report with cost per team. To enforce, use cluster policies that require tags — clusters without tags can't be created. Send each team their cost report monthly and hold team leads accountable."

> **Tip 3:** "What's the most important metric for measuring Databricks cost efficiency?" — "Cost per business outcome — not total DBUs. Total DBUs goes up as the team grows, which is expected. But cost per million rows processed, cost per model trained, or cost per dashboard query should stay flat or decrease as you optimize. Track these unit economics monthly and alert when they spike — that signals a specific pipeline that needs optimization."
