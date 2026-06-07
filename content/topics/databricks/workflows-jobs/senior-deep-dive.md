---
title: "Workflows & Jobs - Senior Deep Dive"
topic: databricks
subtopic: workflows-jobs
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, workflows, jobs, production, ci-cd, terraform, scaling]
---

# Databricks Workflows & Jobs — Senior-Level Deep Dive

## Infrastructure-as-Code with Terraform

```hcl
# terraform/workflows.tf — manage jobs as code

resource "databricks_job" "daily_etl" {
  name = "daily_etl_${var.environment}"
  
  # Schedule
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"  # 6 AM daily
    timezone_id            = "UTC"
  }
  
  # Job cluster
  job_cluster {
    job_cluster_key = "etl_cluster"
    new_cluster {
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = var.environment == "production" ? "i3.xlarge" : "m5.large"
      autoscale {
        min_workers = var.environment == "production" ? 4 : 1
        max_workers = var.environment == "production" ? 16 : 4
      }
      aws_attributes {
        availability = "SPOT_WITH_FALLBACK"
      }
    }
  }
  
  # Tasks
  task {
    task_key = "ingest"
    job_cluster_key = "etl_cluster"
    notebook_task {
      notebook_path = "/Repos/${var.environment}/pipelines/ingest"
      base_parameters = {
        target_catalog = var.environment == "production" ? "production" : "staging"
      }
    }
  }
  
  task {
    task_key = "transform"
    depends_on { task_key = "ingest" }
    job_cluster_key = "etl_cluster"
    notebook_task {
      notebook_path = "/Repos/${var.environment}/pipelines/transform"
    }
  }
  
  # Notifications
  email_notifications {
    on_failure = var.alert_emails
  }
  
  health {
    rules {
      metric = "RUN_DURATION_SECONDS"
      op     = "GREATER_THAN"
      value  = 7200
    }
  }
  
  tags = {
    team        = "data-engineering"
    environment = var.environment
    cost_center = "DE-001"
  }
}
```

---

## CI/CD Pipeline for Workflows

```yaml
# .github/workflows/deploy_jobs.yml
name: Deploy Databricks Workflows
on:
  push:
    branches: [main]
    paths: ['pipelines/**', 'terraform/**']

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run unit tests
        run: pytest tests/ -v
      
      - name: Validate Terraform
        run: terraform validate
        working-directory: terraform/
  
  deploy-staging:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to staging
        run: |
          terraform apply -auto-approve -var="environment=staging"
        working-directory: terraform/
      
      - name: Run staging pipeline
        run: |
          databricks jobs run-now --job-id $STAGING_JOB_ID --wait
  
  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production  # Requires approval
    steps:
      - name: Deploy to production
        run: |
          terraform apply -auto-approve -var="environment=production"
        working-directory: terraform/
```

---

## Advanced Workflow Patterns

### Sensor Pattern (Wait for External Event)

```python
# Task that polls until a condition is met (like Airflow sensors)
# Notebook: /pipelines/wait_for_data.py

import time

def wait_for_data(path: str, timeout_minutes: int = 120):
    """Wait until files appear at the specified path."""
    start = time.time()
    timeout = timeout_minutes * 60
    
    while (time.time() - start) < timeout:
        files = dbutils.fs.ls(path)
        today_files = [f for f in files if f.name.startswith(str(date.today()))]
        
        if today_files:
            print(f"Found {len(today_files)} files. Proceeding.")
            dbutils.jobs.taskValues.set(key="files_found", value=len(today_files))
            return
        
        print(f"No files yet. Waiting 60 seconds...")
        time.sleep(60)
    
    raise TimeoutError(f"No files appeared at {path} within {timeout_minutes} minutes")

wait_for_data(dbutils.widgets.get("source_path"))
```

### Idempotent Tasks (Safe Re-Runs)

```python
# Every task should be idempotent (safe to re-run without side effects)

def transform_orders(run_date: str):
    """Idempotent: OVERWRITE partition for the specific date."""
    result = spark.sql(f"""
        SELECT order_id, customer_id, amount, order_date
        FROM production.bronze.orders
        WHERE _ingested_at::DATE = '{run_date}'
    """)
    
    # OVERWRITE (not append!) for idempotency
    (result.write
        .mode("overwrite")
        .option("replaceWhere", f"order_date = '{run_date}'")  # Only overwrite this partition
        .saveAsTable("production.silver.orders")
    )
    # Re-running for the same date produces the same result (no duplicates!)
```

### Dynamic DAG Generation

```python
# Generate workflow tasks dynamically based on configuration
import json

def generate_workflow_config(tables_config: list[dict]) -> dict:
    """Generate job config from table configuration."""
    tasks = []
    
    # Ingestion tasks (parallel)
    for table in tables_config:
        tasks.append({
            "task_key": f"ingest_{table['name']}",
            "notebook_task": {
                "notebook_path": "/pipelines/generic_ingest",
                "base_parameters": {
                    "source": table["source_path"],
                    "target": table["target_table"],
                    "format": table["format"],
                }
            },
            "job_cluster_key": "ingest_cluster",
        })
    
    # Transform task (depends on all ingestion tasks)
    tasks.append({
        "task_key": "transform_all",
        "depends_on": [{"task_key": f"ingest_{t['name']}"} for t in tables_config],
        "notebook_task": {"notebook_path": "/pipelines/transform"},
        "job_cluster_key": "transform_cluster",
    })
    
    return {"tasks": tasks}
```

---

## Cost Optimization

### Spot Instance Strategies

```python
"aws_attributes": {
    # SPOT_WITH_FALLBACK: Use spot, fall back to on-demand if unavailable
    "availability": "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent": 100,  # Bid at on-demand price (maximize availability)
    "first_on_demand": 1,  # First node on-demand (driver stability)
}

# Cost savings:
# On-demand i3.xlarge: $0.312/hr
# Spot i3.xlarge: ~$0.094/hr (70% savings!)
# With fallback: guaranteed to get capacity (just more expensive if spot unavailable)
```

### Right-Sizing Clusters

```sql
-- Analyze job performance to right-size
SELECT 
    j.job_id,
    j.settings.name AS job_name,
    AVG(TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time)) AS avg_duration_min,
    MAX(r.cluster_spec.autoscale.max_workers) AS max_workers_configured,
    -- Check if max workers were actually used:
    AVG(r.state.metrics.max_active_executors) AS avg_max_executors_used
FROM system.lakeflow.jobs j
JOIN system.lakeflow.job_run_timeline r ON j.job_id = r.job_id
WHERE r.start_time >= current_date() - 30
GROUP BY j.job_id, j.settings.name
HAVING avg_max_executors_used < max_workers_configured * 0.5;
-- These jobs are over-provisioned — reduce max_workers!
```

---

## Monitoring and SLA Management

```python
class WorkflowSLAMonitor:
    """Monitor workflow SLAs and alert on breaches."""
    
    def check_slas(self):
        slas = {
            "daily_etl": {"max_duration_min": 60, "must_complete_by": "08:00 UTC"},
            "hourly_ingest": {"max_duration_min": 15, "must_complete_by": None},
            "weekly_report": {"max_duration_min": 120, "must_complete_by": "MON 06:00 UTC"},
        }
        
        for job_name, sla in slas.items():
            last_run = self.get_last_run(job_name)
            
            if last_run["duration_min"] > sla["max_duration_min"]:
                self.alert(f"SLA breach: {job_name} took {last_run['duration_min']}min (limit: {sla['max_duration_min']}min)")
            
            if sla["must_complete_by"] and not self.completed_by(last_run, sla["must_complete_by"]):
                self.alert(f"SLA breach: {job_name} did not complete by {sla['must_complete_by']}")
```

---

## Interview Tips

> **Tip 1:** "How do you manage Databricks workflows in production?" — Infrastructure-as-Code (Terraform): all job configs version-controlled, same code deploys to dev/staging/prod with different parameters. CI/CD: PR → test → deploy staging → run staging → deploy production (with approval gate). Monitoring: system tables for run history, SLA alerting, cost tracking.

> **Tip 2:** "How do you handle idempotency in workflows?" — Every task must be safe to re-run. Pattern: OVERWRITE (not append) with `replaceWhere` for partitioned tables, MERGE with deduplication keys for upserts. If a task fails mid-run and is retried, it produces the same result without duplicates.

> **Tip 3:** "How do you optimize workflow costs?" — (1) Job clusters not all-purpose (60% cheaper), (2) Spot instances with fallback (70% savings), (3) Right-size by analyzing actual executor utilization vs configured max, (4) Shared cluster across sequential tasks (avoids cluster start per task), (5) Auto-terminate — cluster dies immediately after job, no idle cost.
