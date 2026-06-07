---
title: "Workflows & Jobs - Intermediate"
topic: databricks
subtopic: workflows-jobs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, workflows, jobs, orchestration, parameters, branching, monitoring]
---

# Databricks Workflows & Jobs — Intermediate

## Parameterized Jobs

### Job Parameters

```python
# Define job-level parameters (accessible by all tasks)
job_config = {
    "name": "daily_etl",
    "parameters": [
        {"name": "run_date", "default": "{{job.start_time.iso_date}}"},
        {"name": "environment", "default": "production"},
        {"name": "source_path", "default": "s3://lake/landing/"},
    ],
    "tasks": [...]
}

# Access in notebook:
run_date = dbutils.widgets.get("run_date")      # "2024-03-15"
environment = dbutils.widgets.get("environment") # "production"
source_path = dbutils.widgets.get("source_path") # "s3://lake/landing/"

# Dynamic parameters with built-in variables:
# {{job.start_time.iso_date}} → "2024-03-15"
# {{job.start_time.epoch_ms}} → 1710460800000
# {{job.id}} → 12345
# {{task.run_id}} → 67890
```

### Task-Level Parameters

```python
# Each task can have its own parameters
{
    "task_key": "ingest_orders",
    "notebook_task": {
        "notebook_path": "/pipelines/generic_ingest",
        "base_parameters": {
            "source_table": "orders",
            "target_table": "production.bronze.orders",
            "format": "json",
        }
    }
},
{
    "task_key": "ingest_events",
    "notebook_task": {
        "notebook_path": "/pipelines/generic_ingest",  # Same notebook!
        "base_parameters": {
            "source_table": "events",
            "target_table": "production.bronze.events",
            "format": "parquet",
        }
    }
}
# One generic notebook, different params per task = reusable pipeline
```

---

## Conditional Execution and Branching

### If/Else Task Logic

```python
# Run different tasks based on a condition
tasks = [
    {
        "task_key": "check_data_quality",
        "notebook_task": {"notebook_path": "/pipelines/quality_check"},
        # This notebook sets taskValues: "quality_status" = "pass" or "fail"
    },
    {
        "task_key": "proceed_to_gold",
        "depends_on": [{"task_key": "check_data_quality"}],
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{tasks.check_data_quality.values.quality_status}}",
            "right": "pass",
        },
        "notebook_task": {"notebook_path": "/pipelines/build_gold"},
    },
    {
        "task_key": "alert_and_quarantine",
        "depends_on": [{"task_key": "check_data_quality"}],
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{tasks.check_data_quality.values.quality_status}}",
            "right": "fail",
        },
        "notebook_task": {"notebook_path": "/pipelines/quarantine_bad_data"},
    },
]
```

### Run If / Run Depends On Outcome

```python
# Task runs only if upstream FAILED (for error handling)
{
    "task_key": "send_failure_alert",
    "depends_on": [{"task_key": "main_etl", "outcome": "task_failure"}],
    "notebook_task": {"notebook_path": "/pipelines/alert_slack"},
}

# Task runs only if upstream SUCCEEDED
{
    "task_key": "update_dashboard",
    "depends_on": [{"task_key": "main_etl", "outcome": "task_success"}],
    "notebook_task": {"notebook_path": "/pipelines/refresh_dashboard"},
}
```

---

## For Each Task (Dynamic Loops)

Process a list of items with parallel task execution:

```python
# Process multiple tables in parallel using ForEach
{
    "task_key": "get_tables_to_process",
    "notebook_task": {"notebook_path": "/pipelines/list_tables"},
    # This task returns: ["orders", "events", "customers", "products"]
},
{
    "task_key": "process_table",
    "depends_on": [{"task_key": "get_tables_to_process"}],
    "for_each_task": {
        "inputs": "{{tasks.get_tables_to_process.values.table_list}}",
        "task": {
            "task_key": "process_single_table",
            "notebook_task": {
                "notebook_path": "/pipelines/process_table",
                "base_parameters": {
                    "table_name": "{{input}}"
                }
            }
        },
        "concurrency": 4,  # Process 4 tables in parallel
    }
}
# Dynamically creates N parallel tasks based on the input list
```

---

## Multi-Cluster Workflows

```python
# Different tasks can use different cluster sizes
{
    "tasks": [
        {
            "task_key": "light_ingest",
            "job_cluster_key": "small_cluster",  # 2 workers (ingestion is light)
            "notebook_task": {"notebook_path": "/pipelines/ingest"},
        },
        {
            "task_key": "heavy_transform",
            "job_cluster_key": "large_cluster",  # 16 workers (heavy joins)
            "depends_on": [{"task_key": "light_ingest"}],
            "notebook_task": {"notebook_path": "/pipelines/transform"},
        },
        {
            "task_key": "sql_aggregation",
            "sql_task": {"warehouse_id": "abc123"},  # SQL warehouse (not cluster)
            "depends_on": [{"task_key": "heavy_transform"}],
        },
    ],
    "job_clusters": [
        {
            "job_cluster_key": "small_cluster",
            "new_cluster": {"num_workers": 2, "node_type_id": "m5.large"},
        },
        {
            "job_cluster_key": "large_cluster",
            "new_cluster": {"autoscale": {"min_workers": 8, "max_workers": 16}, "node_type_id": "r5.2xlarge"},
        },
    ],
}
# Right-size each task's compute independently (cost optimization)
```

---

## Workflow Monitoring and Alerts

### Health Rules

```python
"health": {
    "rules": [
        # Alert if single run takes too long
        {"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 7200},
        # Alert if consecutive failures
        {"metric": "STREAMING_BACKLOG_BYTES", "op": "GREATER_THAN", "value": 1073741824},
    ]
}
```

### Custom Monitoring via System Tables

```sql
-- Query workflow run history
SELECT 
    job_id,
    run_id,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_min,
    state.result_state AS status,
    trigger
FROM system.lakeflow.job_run_timeline
WHERE start_time >= current_date() - 7
ORDER BY start_time DESC;

-- Detect degrading performance (runs getting slower)
SELECT 
    job_id,
    DATE(start_time) AS run_date,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS avg_duration_min
FROM system.lakeflow.job_run_timeline
WHERE state.result_state = 'SUCCESS'
GROUP BY job_id, DATE(start_time)
ORDER BY run_date;
```

---

## Workflows vs External Orchestrators

| Aspect | Databricks Workflows | Apache Airflow | Prefect/Dagster |
|--------|---------------------|----------------|-----------------|
| Setup | Zero (built-in) | Deploy + maintain cluster | SaaS or self-hosted |
| Databricks integration | Native (clusters, UC, DLT) | Via operators/hooks | Via SDK |
| Non-Databricks tasks | Limited | Excellent | Good |
| Complexity | Low-medium | High | Medium |
| Cost | Included in Databricks | Separate infra cost | Subscription |
| Best for | Databricks-only pipelines | Multi-system orchestration | Modern data stacks |

**When to use Workflows:** Your pipeline is 100% on Databricks (notebooks, DLT, SQL).
**When to use Airflow:** You orchestrate across multiple systems (Databricks + dbt + APIs + Snowflake).

---

## Repair and Re-Run

```python
# If a task fails, you can repair (re-run) just the failed task + downstream
# No need to re-run the entire workflow from scratch

# Via API:
response = requests.post(
    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/repair",
    json={
        "run_id": 12345,
        "rerun_tasks": ["failed_task_key"],  # Only re-run this task
    }
)
# Saves time: successful upstream tasks are NOT re-executed
# Only the failed task and its downstream dependencies re-run
```

---

## Interview Tips

> **Tip 1:** "How do you handle task failures in Workflows?" — Three levels: (1) Task retry (max_retries=3 with backoff), (2) Conditional error task (runs only on upstream failure — sends alert), (3) Manual repair (re-run just the failed task + downstream without re-running successful upstream tasks).

> **Tip 2:** "How do you parameterize workflows for multiple environments?" — Job-level parameters with defaults. Same workflow definition for dev/staging/prod — only parameters differ (source paths, target schemas, cluster sizes). Use dynamic variables like `{{job.start_time.iso_date}}` for date-driven pipelines.

> **Tip 3:** "Workflows vs Airflow?" — Workflows: zero-setup, native Databricks integration (clusters, UC, DLT), good for Databricks-only pipelines. Airflow: multi-system orchestration (Databricks + dbt + APIs + non-Databricks systems), more flexible but requires deployment and maintenance. Use Workflows if you're 80%+ Databricks; add Airflow when you need to orchestrate external systems.
