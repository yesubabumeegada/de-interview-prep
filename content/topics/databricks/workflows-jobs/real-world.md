---
title: "Workflows & Jobs - Real-World Production Examples"
topic: databricks
subtopic: workflows-jobs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, workflows, jobs, production, patterns, orchestration]
---

# Databricks Workflows & Jobs — Real-World Production Examples

## Pattern 1: Daily ETL Workflow (Medallion)

```python
# Complete daily ETL: ingest → transform → validate → serve

JOB_CONFIG = {
    "name": "daily_ecommerce_etl",
    "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"},
    "tasks": [
        # Task 1: Ingest (Auto Loader — processes new files)
        {"task_key": "ingest_orders", "notebook_task": {"notebook_path": "/pipelines/ingest_orders"}},
        {"task_key": "ingest_events", "notebook_task": {"notebook_path": "/pipelines/ingest_events"}},
        # Task 1b: Parallel ingestion (no dependencies between sources)
        
        # Task 2: Silver transforms (depends on ingestion)
        {"task_key": "silver_orders", 
         "depends_on": [{"task_key": "ingest_orders"}],
         "notebook_task": {"notebook_path": "/pipelines/silver_orders"}},
        {"task_key": "silver_events",
         "depends_on": [{"task_key": "ingest_events"}],
         "notebook_task": {"notebook_path": "/pipelines/silver_events"}},
        
        # Task 3: Gold aggregations (depends on silver)
        {"task_key": "gold_revenue",
         "depends_on": [{"task_key": "silver_orders"}],
         "notebook_task": {"notebook_path": "/pipelines/gold_revenue"}},
        {"task_key": "gold_funnel",
         "depends_on": [{"task_key": "silver_events"}],
         "notebook_task": {"notebook_path": "/pipelines/gold_funnel"}},
        
        # Task 4: Quality validation (depends on all gold)
        {"task_key": "validate",
         "depends_on": [{"task_key": "gold_revenue"}, {"task_key": "gold_funnel"}],
         "notebook_task": {"notebook_path": "/pipelines/validate_quality"}},
        
        # Task 5: Notify on success
        {"task_key": "notify_success",
         "depends_on": [{"task_key": "validate"}],
         "notebook_task": {"notebook_path": "/pipelines/send_slack_success"}},
        
        # Error handler: notify on ANY failure
        {"task_key": "notify_failure",
         "depends_on": [{"task_key": "validate", "outcome": "task_failure"}],
         "notebook_task": {"notebook_path": "/pipelines/send_pagerduty_alert"}},
    ],
    "job_clusters": [{
        "job_cluster_key": "etl",
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "autoscale": {"min_workers": 4, "max_workers": 12},
            "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"},
        }
    }],
}
```

---

## Pattern 2: Multi-Environment Deployment

```python
# Same workflow code, different configs per environment
# Managed via Terraform with variables

# terraform/variables.tf
ENVIRONMENTS = {
    "development": {
        "schedule": None,  # Manual trigger only
        "cluster_size": {"min": 1, "max": 2},
        "instance_type": "m5.large",
        "target_catalog": "development",
        "alert_emails": ["dev-team@company.com"],
    },
    "staging": {
        "schedule": "0 0 5 * * ?",  # 5 AM (1 hour before prod)
        "cluster_size": {"min": 2, "max": 4},
        "instance_type": "i3.xlarge",
        "target_catalog": "staging",
        "alert_emails": ["data-team@company.com"],
    },
    "production": {
        "schedule": "0 0 6 * * ?",  # 6 AM
        "cluster_size": {"min": 4, "max": 16},
        "instance_type": "i3.xlarge",
        "target_catalog": "production",
        "alert_emails": ["data-team@company.com", "oncall@company.com"],
    },
}

# CI/CD flow:
# 1. PR creates/updates staging job (auto-deploy)
# 2. Staging job runs successfully (validates the change)
# 3. PR merged → production job updated (with approval gate)
```

---

## Pattern 3: Backfill Workflow

```python
# Backfill: re-process historical data for a date range

def create_backfill_job(start_date: str, end_date: str, parallelism: int = 5):
    """Generate a backfill workflow that processes dates in parallel."""
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    # Create tasks for each date (with controlled parallelism)
    tasks = []
    for i, date in enumerate(dates):
        task = {
            "task_key": f"backfill_{date.replace('-', '')}",
            "notebook_task": {
                "notebook_path": "/pipelines/transform_orders",
                "base_parameters": {"run_date": date, "mode": "backfill"},
            },
        }
        # Limit parallelism: each batch of N tasks depends on previous batch
        if i >= parallelism:
            task["depends_on"] = [{"task_key": f"backfill_{dates[i-parallelism].replace('-', '')}"}]
        
        tasks.append(task)
    
    return {
        "name": f"backfill_{start_date}_to_{end_date}",
        "tasks": tasks,
        "max_concurrent_runs": 1,
    }

# Usage: backfill last 30 days, 5 dates in parallel at a time
config = create_backfill_job("2024-02-01", "2024-03-01", parallelism=5)
```

---

## Pattern 4: Workflow Composition (Parent-Child)

```python
# Master workflow triggers child workflows
# Useful for: team boundaries, different SLAs, independent failure handling

MASTER_WORKFLOW = {
    "name": "master_daily_orchestrator",
    "tasks": [
        # Trigger ingestion workflow (owned by platform team)
        {
            "task_key": "trigger_ingestion",
            "run_job_task": {"job_id": 1001},  # Triggers another job
        },
        # Trigger sales analytics (owned by sales team)
        {
            "task_key": "trigger_sales_analytics",
            "depends_on": [{"task_key": "trigger_ingestion"}],
            "run_job_task": {"job_id": 1002},
        },
        # Trigger ML feature computation (owned by ML team)
        {
            "task_key": "trigger_ml_features",
            "depends_on": [{"task_key": "trigger_ingestion"}],
            "run_job_task": {"job_id": 1003},
        },
    ],
}

# Benefits:
# - Each team owns and maintains their own child workflow
# - Master just orchestrates dependencies between teams
# - Teams can update their workflow without changing master
# - Failures are isolated to the team that owns the failing workflow
```

---

## Pattern 5: Cost Tracking and Chargeback

```sql
-- Track workflow costs by team using tags + system tables
SELECT 
    j.settings.name AS job_name,
    j.settings.tags.team AS team,
    j.settings.tags.cost_center AS cost_center,
    COUNT(r.run_id) AS runs_this_month,
    SUM(TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time)) AS total_minutes,
    SUM(TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) * 
        r.cluster_spec.num_workers * 0.15 / 60) AS estimated_dbu_cost
FROM system.lakeflow.jobs j
JOIN system.lakeflow.job_run_timeline r ON j.job_id = r.job_id
WHERE r.start_time >= DATE_TRUNC('month', current_date())
  AND r.state.result_state = 'SUCCESS'
GROUP BY j.settings.name, j.settings.tags.team, j.settings.tags.cost_center
ORDER BY estimated_dbu_cost DESC;

-- Result:
-- | job_name | team | cost_center | runs | minutes | est_cost |
-- | daily_etl | platform | DE-001 | 30 | 1800 | $450 |
-- | ml_features | ml-team | ML-002 | 30 | 3600 | $900 |
-- | hourly_ingest | platform | DE-001 | 720 | 2160 | $540 |
```

---

## Pattern 6: SLA Monitoring Dashboard

```python
# Notebook: /monitoring/sla_check.py
# Runs every 15 minutes via its own workflow

from datetime import datetime, time

SLA_DEFINITIONS = {
    "daily_etl": {"must_complete_by": time(8, 0), "max_duration_min": 90},
    "hourly_ingest": {"max_duration_min": 20},
    "weekly_report": {"must_complete_by": time(6, 0), "day": "Monday"},
}

def check_slas():
    violations = []
    
    for job_name, sla in SLA_DEFINITIONS.items():
        last_run = spark.sql(f"""
            SELECT start_time, end_time, state.result_state as status
            FROM system.lakeflow.job_run_timeline r
            JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
            WHERE j.settings.name = '{job_name}'
            ORDER BY start_time DESC LIMIT 1
        """).collect()
        
        if not last_run:
            violations.append(f"{job_name}: No recent run found!")
            continue
        
        run = last_run[0]
        duration = (run["end_time"] - run["start_time"]).total_seconds() / 60
        
        if run["status"] == "FAILED":
            violations.append(f"{job_name}: FAILED (last run)")
        
        if "max_duration_min" in sla and duration > sla["max_duration_min"]:
            violations.append(f"{job_name}: Duration {duration:.0f}min > SLA {sla['max_duration_min']}min")
        
        if "must_complete_by" in sla:
            completion_time = run["end_time"].time()
            if completion_time > sla["must_complete_by"]:
                violations.append(f"{job_name}: Completed at {completion_time} (SLA: {sla['must_complete_by']})")
    
    if violations:
        send_slack_alert("\n".join(violations))
    
    return violations

check_slas()
```

---

## Interview Tips

> **Tip 1:** "Design a production ETL workflow" — Parallel ingestion tasks (one per source, no dependencies between them), sequential silver transforms (depend on their bronze), fan-in for gold (depends on all silver), quality validation (depends on all gold), conditional alerting (success → Slack, failure → PagerDuty). Use shared job cluster to avoid startup overhead between tasks.

> **Tip 2:** "How do you handle backfills?" — Generate a dynamic workflow with one task per date to backfill. Control parallelism (5-10 dates concurrently to avoid overwhelming the cluster). Each task must be idempotent (OVERWRITE for the specific date, not append). Use replaceWhere for partition-level overwrites. Separate backfill workflow from daily production workflow (different cluster sizes, no schedule).

> **Tip 3:** "How do you track workflow costs?" — Tag every job with team/cost_center metadata. Query system.lakeflow tables for run history + duration. Calculate DBU cost from duration × workers × DBU rate. Build monthly chargeback reports per team. Alert if a team's daily spend exceeds budget threshold. Spot instances reduce cost 60-70% for non-critical workflows.
