---
title: "Cost Management - Scenario Questions"
topic: databricks
subtopic: cost-management
content_type: scenario_question
tags: [databricks, cost, scenarios, interview, finops, optimization]
---

# Scenario Questions — Cost Management

<article data-difficulty="junior">

## 🟢 Junior: Configure a Cluster for Cost Efficiency

**Scenario:** You're setting up a new Databricks cluster for a daily ETL job that runs for 2 hours at 7am and processes 50GB of data. The job is fault-tolerant and can be retried if interrupted. Design the cluster configuration to minimize cost while meeting the job requirements.

<details>
<summary>✅ Solution</summary>

```python
# Optimal cluster configuration for this use case
cluster_config = {
    "cluster_name": "daily-etl-7am",

    # 1. Use Job Cluster (not all-purpose) — 50% cheaper DBU rate
    # This is set at the Workflow/Job level, not cluster level
    # The cluster is created per-run and auto-terminates when job completes

    # 2. Spot instances — job is fault-tolerant (key requirement met)
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",  # spot first, on-demand as backup
        "spot_bid_price_percent": 100,
        "first_on_demand": 1  # keep 1 on-demand worker as safety net
    },

    # 3. Right-sized for 50GB data
    # Rule: ~200MB per partition, 100 partitions for 50GB = manageable
    # 4 workers × 16GB RAM each = 64GB total = enough for 50GB + overhead
    "num_workers": 4,
    "node_type_id": "i3.xlarge",   # 16GB RAM, 4 vCPU, local NVMe for Delta cache
    "driver_node_type_id": "i3.xlarge",

    # 4. Auto-termination as safety net (job clusters auto-terminate anyway)
    "autotermination_minutes": 30,

    # 5. Runtime — use latest LTS for best performance/stability
    "spark_version": "13.3.x-scala2.12",

    # 6. Cost attribution tags
    "custom_tags": {
        "team": "data-platform",
        "pipeline": "daily-etl",
        "cost_center": "DE-001"
    }
}

# Expected cost:
# - Cluster: 5 nodes (1 driver + 4 workers) × 1 DBU/hr × 2 hrs = 10 DBUs
# - At jobs compute rate ($0.20/DBU vs $0.40 all-purpose): $2.00/day
# - Plus EC2 costs: ~$0.80/node/hr spot × 5 × 2 hrs ≈ $8.00
# - Total: ~$10/day vs ~$25/day with all-purpose on-demand

print("Configuration advantages:")
print("  - Job cluster: 50% cheaper DBU rate vs all-purpose")
print("  - Spot instances: ~60% cheaper EC2 cost")
print("  - Auto-terminates when job finishes: no idle time")
print("  - 4 workers: right-sized for 50GB, not over-provisioned")
```

**What NOT to do:**
- Don't use an all-purpose cluster that stays running all day
- Don't use on-demand if the job is fault-tolerant
- Don't use 20 workers "just in case" — right-size to the actual data volume

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Investigate a 3x Cost Spike

**Scenario:** This month's Databricks bill is 3x higher than last month ($60K vs $20K). The finance team wants an explanation. Using system tables, identify the root cause(s) and propose fixes.

<details>
<summary>✅ Solution</summary>

```python
# Step 1: Find when the spike started
daily_cost = spark.sql("""
    SELECT
        DATE_TRUNC('day', usage_start_time) AS day,
        SUM(usage_quantity) AS total_dbus,
        ROUND(SUM(usage_quantity) * 0.40, 2) AS est_cost_usd
    FROM system.billing.usage
    WHERE usage_start_time >= DATEADD(month, -2, CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 1
""")
display(daily_cost)
# Spike started 15 days ago

# Step 2: What changed 15 days ago?
spike_vs_baseline = spark.sql("""
    SELECT
        usage_metadata.cluster_name,
        SUM(CASE WHEN usage_start_time >= DATEADD(day, -15, CURRENT_TIMESTAMP())
                 THEN usage_quantity ELSE 0 END) AS spike_period_dbus,
        SUM(CASE WHEN usage_start_time < DATEADD(day, -15, CURRENT_TIMESTAMP())
                 THEN usage_quantity ELSE 0 END) AS baseline_dbus,
        ROUND(SUM(CASE WHEN usage_start_time >= DATEADD(day, -15, CURRENT_TIMESTAMP())
                       THEN usage_quantity ELSE 0 END) * 0.40, 2) AS spike_cost_usd
    FROM system.billing.usage
    WHERE usage_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
    GROUP BY 1
    HAVING spike_period_dbus > baseline_dbus * 2  -- clusters that grew > 2x
    ORDER BY spike_cost_usd DESC
""")
display(spike_vs_baseline)

# Results:
# "ml-training-cluster": 0 DBUs baseline → 28,000 DBUs spike ($11,200)
# "analytics-team-cluster": 1,200 DBUs → 3,800 DBUs spike ($1,040)
# "streaming-prod": 2,000 DBUs → 5,200 DBUs spike ($1,280)

# Step 3: Investigate "ml-training-cluster" (biggest new cost)
ml_cluster_detail = spark.sql("""
    SELECT
        DATE_TRUNC('day', usage_start_time) AS day,
        SUM(usage_quantity) AS dbus,
        usage_metadata.cluster_source_tags.team AS team,
        usage_metadata.cluster_source_tags.project AS project
    FROM system.billing.usage
    WHERE usage_metadata.cluster_name = 'ml-training-cluster'
      AND usage_start_time >= DATEADD(day, -20, CURRENT_TIMESTAMP())
    GROUP BY 1, 3, 4
    ORDER BY 1
""")
display(ml_cluster_detail)
# 80-node cluster running 24/7 for 15 days — no auto-termination!
# Tags show: team=ml-research, project=gpt-fine-tuning

# Root causes:
# 1. ML team started a GPU training cluster (80 nodes!) for a new project
#    No auto-termination, running 24/7 including idle weekends
#    Cost: 80 DBU/hr × 360 hrs × $0.40 = $11,520

# 2. Analytics cluster had a dashboard user who never stopped querying
#    All-purpose cluster should have had auto-suspend on SQL warehouse

# 3. Streaming job had a bug causing 2x reprocessing
```

**Proposed fixes:**

```python
# Fix 1: Immediately terminate the idle ML cluster (or set auto-termination)
w.clusters.delete(cluster_id="ml-training-cluster-id")

# Fix 2: Add ML team to cluster policy that enforces auto-termination
# Max 4-hour termination for GPU clusters (not 60 min — training needs time)
spark.sql("""
    UPDATE admin.cluster_policies
    SET definition = JSON_SET(definition,
        '$.autotermination_minutes.maxValue', 240)
    WHERE policy_name = 'gpu-training-policy'
""")

# Fix 3: Implement budget alerts so this doesn't happen again
# Alert at 50% and 80% of monthly budget
# Auto-suspend non-prod clusters if > 100% of budget

# Estimated recovery: back to ~$22K/month after fixes
```

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a FinOps Program for 20 Teams

**Scenario:** Your company has grown to 20 data teams using Databricks. Total monthly spend is $180K, growing 15% monthly with no clear accountability. Leadership wants to slow cost growth while still supporting team growth. Design a FinOps program that provides visibility, accountability, and optimization without slowing teams down.

<details>
<summary>✅ Solution</summary>

**Phase 1: Visibility (Month 1)**

```python
# Instrument everything — can't manage what you can't see

# 1. Enforce cluster tagging via policy
required_tags_policy = {
    "custom_tags.team": {
        "type": "regex",
        "pattern": ".+",
        "defaultValue": "",
        "isRequired": True  # cannot create cluster without tag
    },
    "custom_tags.project": {
        "type": "regex",
        "pattern": ".+",
        "isRequired": True
    },
    "custom_tags.cost_center": {
        "type": "regex",
        "pattern": "^[A-Z]{2,6}-\\d{3}$",  # e.g., "DE-001", "MLTG-042"
        "isRequired": True
    }
}

# 2. Monthly cost visibility report (automated)
monthly_report = spark.sql("""
    SELECT
        usage_metadata.cluster_source_tags.team AS team,
        usage_metadata.cluster_source_tags.cost_center AS cost_center,
        ROUND(SUM(usage_quantity) * 0.40, 2) AS compute_cost_usd,
        SUM(usage_quantity) AS total_dbus,
        ROUND(SUM(usage_quantity) / MAX(day_count), 2) AS avg_daily_dbus,
        ROUND(
            SUM(CASE WHEN sku_name LIKE '%SPOT%' THEN usage_quantity ELSE 0 END) /
            SUM(usage_quantity), 2
        ) AS spot_adoption_rate
    FROM system.billing.usage
    CROSS JOIN (SELECT COUNT(DISTINCT DATE_TRUNC('day', usage_start_time)) AS day_count
                FROM system.billing.usage
                WHERE usage_start_time >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE()))) d
    WHERE usage_start_time >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE()))
    GROUP BY 1, 2
    ORDER BY compute_cost_usd DESC
""")
display(monthly_report)
```

**Phase 2: Accountability (Month 2)**

```python
# Set budgets per team, alert at 80%
team_budgets = {
    "platform":     8000,
    "ml-research":  25000,
    "analytics":    15000,
    "product-de":   12000,
    # ... etc for 20 teams
}

def weekly_budget_check():
    for team, budget in team_budgets.items():
        month_to_date = spark.sql(f"""
            SELECT ROUND(SUM(usage_quantity) * 0.40, 2) AS cost_usd
            FROM system.billing.usage
            WHERE usage_metadata.cluster_source_tags.team = '{team}'
              AND usage_start_time >= DATE_TRUNC('month', CURRENT_DATE())
        """).collect()[0]["cost_usd"]

        pct = month_to_date / budget
        if pct >= 0.80:
            send_slack(
                channel=f"#team-{team}-data",
                message=f"⚠️ Budget alert: ${month_to_date:,.0f} / ${budget:,.0f} ({pct:.0%}) MTD"
            )
```

**Phase 3: Optimization Standards (Month 3)**

```python
# Scorecard: each team gets a monthly efficiency score
def compute_team_scorecard(team: str, month: str) -> dict:
    metrics = spark.sql(f"""
        SELECT
            -- Spot adoption (target: > 70%)
            ROUND(SUM(CASE WHEN sku_name LIKE '%SPOT%' THEN usage_quantity ELSE 0 END) /
                  SUM(usage_quantity), 2) AS spot_rate,

            -- Job cluster vs all-purpose ratio (target: > 80% job compute)
            ROUND(SUM(CASE WHEN sku_name LIKE '%JOBS%' THEN usage_quantity ELSE 0 END) /
                  SUM(usage_quantity), 2) AS job_cluster_rate,

            -- Idle time (target: < 10%)
            -- (Approximated: hours with no active runs / total hours)
            0.0 AS idle_rate  -- simplified; compute from job_runs join

        FROM system.billing.usage
        WHERE usage_metadata.cluster_source_tags.team = '{team}'
          AND DATE_TRUNC('month', usage_start_time) = '{month}'
    """).collect()[0]

    score = (
        (1 if metrics["spot_rate"] >= 0.70 else 0) +
        (1 if metrics["job_cluster_rate"] >= 0.80 else 0) +
        (1 if metrics["idle_rate"] <= 0.10 else 0)
    )

    return {
        "team": team,
        "score": f"{score}/3",
        "spot_rate": f"{metrics['spot_rate']:.0%}",
        "job_cluster_rate": f"{metrics['job_cluster_rate']:.0%}",
    }

# Publish scorecards monthly — teams see peer performance
# Top performer gets recognized; bottom teams get 1:1 optimization review
```

**Outcome targets (6-month horizon):**
- Month 1: 100% cluster tagging compliance
- Month 2: All teams on budgets, 80% alerts working
- Month 3: Spot adoption > 70% company-wide
- Month 4: Job cluster rate > 80% for scheduled workloads
- Month 6: Cost growth rate < 5%/month (down from 15%) while team count stays constant

**Trade-offs:**
- More process overhead (tagging, budget reviews) — invest in self-service tooling to keep it lightweight
- Teams may resist chargeback initially — frame it as visibility, not punishment
- Budget alerts can slow teams if limits are too tight — set budgets at 120% of trailing 3-month average, not from top-down guess

</details>
</article>
