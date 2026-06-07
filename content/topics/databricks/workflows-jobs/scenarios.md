---
title: "Workflows & Jobs - Scenario Questions"
topic: databricks
subtopic: workflows-jobs
content_type: scenario_question
tags: [databricks, workflows, jobs, interview, scenarios]
---

# Scenario Questions — Workflows & Jobs

<article data-difficulty="junior">

## 🟢 Junior: Creating a Basic Scheduled Job

**Scenario:** Create a Databricks workflow that runs a notebook `/pipelines/daily_ingest` every day at 6 AM UTC. It should use a job cluster with 4 workers and send an email on failure.

<details>
<summary>💡 Hint</summary>
Use a cron expression for 6 AM daily ("0 0 6 * * ?"), configure a job cluster (not all-purpose), and add email_notifications for on_failure.
</details>

<details>
<summary>✅ Solution</summary>

```python
job_config = {
    "name": "daily_ingest_pipeline",
    "tasks": [
        {
            "task_key": "ingest",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/pipelines/daily_ingest",
            },
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "etl_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4,
                "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"},
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "UTC",
    },
    "email_notifications": {
        "on_failure": ["data-team@company.com"],
    },
    "max_concurrent_runs": 1,
}
```

**Key Points:**
- Cron: `0 0 6 * * ?` = every day at 06:00 UTC (second, minute, hour, day, month, day-of-week)
- Job cluster: created fresh for each run, terminated after (60% cheaper than all-purpose)
- `max_concurrent_runs: 1` prevents overlap if a run takes longer than 24 hours
- SPOT_WITH_FALLBACK: uses spot instances when available, on-demand otherwise
- Email on failure: immediate notification when something goes wrong

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Task Dependencies

**Scenario:** Your pipeline has 3 steps: (1) ingest raw data, (2) transform to silver, (3) build gold aggregations. Step 2 needs step 1 to finish first, and step 3 needs step 2. Create the task dependency chain.

<details>
<summary>💡 Hint</summary>
Use `depends_on` with the upstream task's `task_key` to create sequential execution.
</details>

<details>
<summary>✅ Solution</summary>

```python
tasks = [
    {
        "task_key": "ingest",
        "notebook_task": {"notebook_path": "/pipelines/ingest"},
        "job_cluster_key": "shared",
        # No depends_on — runs first
    },
    {
        "task_key": "transform",
        "depends_on": [{"task_key": "ingest"}],  # Waits for ingest
        "notebook_task": {"notebook_path": "/pipelines/transform"},
        "job_cluster_key": "shared",
    },
    {
        "task_key": "aggregate",
        "depends_on": [{"task_key": "transform"}],  # Waits for transform
        "notebook_task": {"notebook_path": "/pipelines/aggregate"},
        "job_cluster_key": "shared",
    },
]

# Execution order: ingest → transform → aggregate (sequential)
# All use "shared" job_cluster_key (same cluster, no restart between tasks)
# If ingest fails: transform and aggregate are SKIPPED (not attempted)
```

**Key Points:**
- `depends_on` creates the execution order (DAG)
- Tasks without `depends_on` run first (entry points)
- If an upstream task fails, all downstream tasks are skipped
- Using the same `job_cluster_key` shares the cluster (no startup delay between tasks)
- The workflow DAG is visible in the UI as a graph

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Passing Data Between Tasks

**Scenario:** Task 1 (ingest) counts how many rows were loaded. Task 2 (validate) needs this count to verify data quality (alert if count is 0). How do you pass the row count from Task 1 to Task 2?

<details>
<summary>💡 Hint</summary>
Use `dbutils.jobs.taskValues.set()` in the upstream task and `dbutils.jobs.taskValues.get()` in the downstream task.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ===== Task 1: ingest.py =====
# Count rows loaded
df = spark.table("production.bronze.orders").filter(col("_ingested_at") >= current_date())
row_count = df.count()

# Pass to downstream tasks
dbutils.jobs.taskValues.set(key="rows_loaded", value=row_count)
dbutils.jobs.taskValues.set(key="load_date", value=str(date.today()))
print(f"Ingested {row_count} rows for {date.today()}")

# ===== Task 2: validate.py =====
# Read value from Task 1
rows = dbutils.jobs.taskValues.get(taskKey="ingest", key="rows_loaded")
load_date = dbutils.jobs.taskValues.get(taskKey="ingest", key="load_date")

# Validate
if rows == 0:
    raise Exception(f"ALERT: Zero rows ingested for {load_date}! Check source system.")
elif rows < 1000:
    print(f"WARNING: Only {rows} rows (usually 50K+). Possible partial load.")
else:
    print(f"OK: {rows} rows loaded for {load_date}")
```

**Key Points:**
- `dbutils.jobs.taskValues.set(key, value)` — store a value from current task
- `dbutils.jobs.taskValues.get(taskKey="task_name", key="key_name")` — read from upstream task
- Values must be JSON-serializable (strings, numbers, booleans, lists, dicts)
- NOT for large data (use Delta tables for that) — just metadata/status/counts
- If the upstream task failed, `get()` will raise an error (task didn't complete)
- Values are only available within the same job run (not across runs)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Job Cluster vs All-Purpose

**Scenario:** Your team runs ETL notebooks on an all-purpose cluster that's always on (costs $2,400/month). The notebooks only run for 3 hours/day total. How much would switching to a job cluster save?

<details>
<summary>💡 Hint</summary>
Job clusters only exist during job execution (3 hrs/day), and have a lower DBU rate (Jobs compute vs All-purpose compute). Calculate both savings.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT: All-purpose cluster, always on
# i3.xlarge: 4 workers
# All-purpose DBU rate: $0.40/DBU/hr
# Running 24/7: 730 hours/month
# DBUs: 4 workers × 1 DBU/hr = 4 DBU/hr
# Cost: (4 DBU × $0.40 + 4 × $0.312 AWS) × 730 = ($1.60 + $1.25) × 730 = $2,080/month
# Plus driver: ~$320/month
# TOTAL: ~$2,400/month

# AFTER: Job cluster, runs 3 hours/day
# Jobs compute DBU rate: $0.15/DBU/hr (62% cheaper!)
# Running 3 hrs/day × 30 days = 90 hours/month
# Cost: (4 DBU × $0.15 + 4 × $0.312 AWS) × 90 = ($0.60 + $1.25) × 90 = $166/month
# Plus driver (90 hrs): ~$40/month
# TOTAL: ~$206/month

# SAVINGS: $2,400 - $206 = $2,194/month (91% reduction!)

# WHY such massive savings:
# 1. Jobs compute DBU rate is 62% cheaper than all-purpose
# 2. Cluster only runs 90 hrs/month vs 730 hrs (88% less time)
# 3. Combined: ~91% total cost reduction

# With spot instances on top: additional 60-70% off AWS instance cost
# Final: ~$120/month (95% savings vs original $2,400)
```

**Key Points:**
- All-purpose: $0.40/DBU, runs 24/7 (for interactive work)
- Jobs compute: $0.15/DBU, runs only during job (for production pipelines)
- DBU rate difference alone: 62% savings
- Plus: cluster only running during actual processing saves another 88%
- Combined: 90-95% savings for typical ETL workloads (few hours/day)
- Rule: NEVER use all-purpose clusters for scheduled production pipelines

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Handling Task Failures

**Scenario:** Your daily workflow has 5 tasks. Task 3 fails due to a temporary network issue. You don't want to re-run tasks 1 and 2 (they succeeded). How do you recover?

<details>
<summary>💡 Hint</summary>
Use the "Repair Run" feature — it re-executes only the failed task and its downstream dependencies, without re-running successful upstream tasks.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Workflow: T1(success) → T2(success) → T3(FAILED) → T4(skipped) → T5(skipped)
# Problem: T3 failed due to transient network issue (now resolved)
# Goal: Re-run T3, T4, T5 without re-running T1, T2

# Option 1: Repair via UI
# Workflows → Job Runs → Select failed run → Click "Repair Run"
# → Select "T3" → Click "Repair"
# Result: T3 re-runs, then T4 and T5 (T1 and T2 are NOT re-run)

# Option 2: Repair via API
import requests
response = requests.post(
    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/repair",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json={
        "run_id": 12345,                        # The failed run ID
        "rerun_tasks": ["transform"],           # Task key that failed
        # Downstream tasks (validate, report) will also re-run automatically
    }
)

# Option 3: Automatic retry (configured at task level)
{
    "task_key": "transform",
    "max_retries": 3,                           # Retry up to 3 times
    "min_retry_interval_millis": 60000,         # Wait 60s between retries
    "retry_on_timeout": True,                   # Also retry on timeout
}
# With retries: transient failures are handled automatically without manual intervention
```

**Key Points:**
- Repair Run: re-runs ONLY the failed task + downstream (saves time and cost)
- Upstream successful tasks are NOT re-executed (their results are preserved)
- Auto-retry (max_retries): handles transient failures without human intervention
- For persistent failures: fix the issue, then manually repair
- Repair preserves the same run context (parameters, cluster config)
- Best practice: configure retries for ALL tasks (handles ~80% of transient issues)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Complex DAG with Branching

**Scenario:** Design a workflow where: (1) Ingest runs first, (2) Three transform tasks run in parallel after ingest, (3) A quality check runs after ALL transforms complete, (4) If quality passes → refresh dashboard, (5) If quality fails → send alert and quarantine bad data.

<details>
<summary>💡 Hint</summary>
Use fan-out (parallel) after ingest, fan-in before quality check, and conditional execution based on task values for the pass/fail branching.
</details>

<details>
<summary>✅ Solution</summary>

```python
tasks = [
    # Step 1: Ingest (entry point)
    {
        "task_key": "ingest",
        "notebook_task": {"notebook_path": "/pipelines/ingest_all_sources"},
    },
    
    # Step 2: Parallel transforms (fan-out from ingest)
    {
        "task_key": "transform_orders",
        "depends_on": [{"task_key": "ingest"}],
        "notebook_task": {"notebook_path": "/pipelines/transform_orders"},
    },
    {
        "task_key": "transform_events",
        "depends_on": [{"task_key": "ingest"}],
        "notebook_task": {"notebook_path": "/pipelines/transform_events"},
    },
    {
        "task_key": "transform_customers",
        "depends_on": [{"task_key": "ingest"}],
        "notebook_task": {"notebook_path": "/pipelines/transform_customers"},
    },
    
    # Step 3: Quality check (fan-in — waits for ALL transforms)
    {
        "task_key": "quality_check",
        "depends_on": [
            {"task_key": "transform_orders"},
            {"task_key": "transform_events"},
            {"task_key": "transform_customers"},
        ],
        "notebook_task": {"notebook_path": "/pipelines/quality_check"},
        # This notebook sets taskValues: quality_status = "pass" or "fail"
    },
    
    # Step 4a: If quality passes → refresh dashboard
    {
        "task_key": "refresh_dashboard",
        "depends_on": [{"task_key": "quality_check"}],
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{tasks.quality_check.values.quality_status}}",
            "right": "pass",
        },
        "notebook_task": {"notebook_path": "/pipelines/refresh_dashboard"},
    },
    
    # Step 4b: If quality fails → alert + quarantine
    {
        "task_key": "alert_and_quarantine",
        "depends_on": [{"task_key": "quality_check"}],
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{tasks.quality_check.values.quality_status}}",
            "right": "fail",
        },
        "notebook_task": {"notebook_path": "/pipelines/quarantine_and_alert"},
    },
]

# DAG visualization:
#                    ┌─ transform_orders ─┐
# ingest → ──┼─ transform_events ──┼─→ quality_check → ─┬─ refresh_dashboard (if pass)
#                    └─ transform_customers─┘                           └─ alert_and_quarantine (if fail)
```

**Key Points:**
- Fan-out: multiple tasks depend on the same upstream (parallel execution)
- Fan-in: one task depends on multiple upstream tasks (waits for all)
- Conditional branching: `condition_task` evaluates task values for routing
- Only ONE branch executes (pass OR fail, not both)
- Quality check notebook must set taskValues for the condition to evaluate
- This pattern handles 90% of real-world ETL workflow requirements

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: ForEach Dynamic Tasks

**Scenario:** You have 20 tables to process daily. Each table uses the same transformation notebook with different parameters. Instead of creating 20 hardcoded tasks, use ForEach to dynamically generate tasks.

<details>
<summary>💡 Hint</summary>
Use a setup task that returns the list of tables, then a `for_each_task` that iterates over the list and runs the transformation notebook with each table as a parameter.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Task 1: Generate the list of tables to process
# Notebook: /pipelines/get_table_list.py
tables = ["orders", "events", "customers", "products", "inventory",
          "payments", "shipments", "returns", "reviews", "sessions",
          "campaigns", "clicks", "signups", "subscriptions", "invoices",
          "tickets", "logs", "metrics", "alerts", "notifications"]

# Pass as task value (JSON-serializable list)
dbutils.jobs.taskValues.set(key="table_list", value=json.dumps(tables))

# Task 2: ForEach — process each table in parallel
{
    "task_key": "get_tables",
    "notebook_task": {"notebook_path": "/pipelines/get_table_list"},
},
{
    "task_key": "process_tables",
    "depends_on": [{"task_key": "get_tables"}],
    "for_each_task": {
        "inputs": "{{tasks.get_tables.values.table_list}}",  # JSON array
        "task": {
            "task_key": "process_single_table",
            "notebook_task": {
                "notebook_path": "/pipelines/generic_transform",
                "base_parameters": {
                    "table_name": "{{input}}",  # Current item from the list
                    "source_schema": "production.bronze",
                    "target_schema": "production.silver",
                }
            },
            "job_cluster_key": "etl_cluster",
        },
        "concurrency": 5,  # Process 5 tables in parallel (not all 20 at once)
    }
}

# generic_transform.py notebook:
table_name = dbutils.widgets.get("table_name")
source = f"{dbutils.widgets.get('source_schema')}.{table_name}"
target = f"{dbutils.widgets.get('target_schema')}.{table_name}"

df = spark.table(source).filter(col("_ingested_at") >= current_date())
# ... transform logic ...
df.write.mode("overwrite").option("replaceWhere", f"load_date = '{today}'").saveAsTable(target)
```

**Key Points:**
- ForEach dynamically generates N tasks from a list (no hardcoding)
- `concurrency`: controls parallelism (5 = process 5 tables simultaneously)
- One generic notebook handles all tables (parameterized)
- Adding a new table: just add it to the list (no workflow config change)
- If one table fails: others continue (failure is per-item, not all-or-nothing)
- Workflow UI shows progress for each item individually

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Workflow Monitoring

**Scenario:** You manage 15 production workflows. Set up monitoring to: (1) alert on failures within 5 minutes, (2) track SLA compliance (each job has a deadline), (3) detect performance degradation (jobs getting slower over time).

<details>
<summary>💡 Hint</summary>
Use: email_notifications for immediate failure alerts, system tables for SLA tracking, and historical run analysis for performance trends.
</details>

<details>
<summary>✅ Solution</summary>

```python
# 1. IMMEDIATE FAILURE ALERTS (per job configuration)
# Every job has email_notifications configured:
"email_notifications": {
    "on_failure": ["data-team@company.com"],
    "on_duration_warning_threshold_exceeded": ["oncall@company.com"],
}
# Plus webhook to PagerDuty for critical jobs

# 2. SLA COMPLIANCE MONITORING (separate monitoring job, runs every 15 min)
# Notebook: /monitoring/sla_check.py

sla_config = {
    "daily_etl": {"deadline": "08:00", "max_duration_min": 90},
    "hourly_ingest": {"max_duration_min": 20},
    "customer_sync": {"deadline": "07:00", "max_duration_min": 45},
}

for job_name, sla in sla_config.items():
    result = spark.sql(f"""
        SELECT j.settings.name, r.end_time, 
               TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) as duration_min,
               r.state.result_state as status
        FROM system.lakeflow.jobs j
        JOIN system.lakeflow.job_run_timeline r ON j.job_id = r.job_id
        WHERE j.settings.name = '{job_name}'
          AND r.start_time >= current_date()
        ORDER BY r.start_time DESC LIMIT 1
    """).collect()
    
    if result:
        run = result[0]
        if run["status"] == "FAILED":
            alert(f"SLA BREACH: {job_name} failed today!")
        elif "deadline" in sla and run["end_time"].strftime("%H:%M") > sla["deadline"]:
            alert(f"SLA BREACH: {job_name} completed at {run['end_time']} (deadline: {sla['deadline']})")
        elif run["duration_min"] > sla["max_duration_min"]:
            alert(f"SLA WARNING: {job_name} took {run['duration_min']}min (limit: {sla['max_duration_min']}min)")

# 3. PERFORMANCE DEGRADATION DETECTION (weekly analysis)
degradation = spark.sql("""
    SELECT j.settings.name as job_name,
           AVG(CASE WHEN r.start_time >= current_date() - 7 
               THEN TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) END) as last_week_avg,
           AVG(CASE WHEN r.start_time BETWEEN current_date() - 14 AND current_date() - 7 
               THEN TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) END) as prev_week_avg
    FROM system.lakeflow.jobs j
    JOIN system.lakeflow.job_run_timeline r ON j.job_id = r.job_id
    WHERE r.state.result_state = 'SUCCESS'
      AND r.start_time >= current_date() - 14
    GROUP BY j.settings.name
    HAVING last_week_avg > prev_week_avg * 1.3  -- 30% slower than previous week
""")

for row in degradation.collect():
    alert(f"PERFORMANCE: {row['job_name']} is {((row['last_week_avg']/row['prev_week_avg'])-1)*100:.0f}% slower than last week")
```

**Key Points:**
- Layer 1 (immediate): email/webhook notifications on every job config
- Layer 2 (SLA): dedicated monitoring job checks deadlines every 15 minutes
- Layer 3 (trends): weekly comparison detects gradual performance degradation
- system.lakeflow tables: gold mine for workflow analytics (run history, duration, status)
- Alert escalation: warning (Slack) → SLA breach (PagerDuty) → repeated failure (incident)
- Performance degradation often indicates: data growth, cluster under-provisioning, or code regression

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: CI/CD for Workflows

**Scenario:** Your team updates ETL notebooks weekly. Sometimes changes break production (wrong table names, bad logic). Set up CI/CD that: tests changes in staging before production deployment.

<details>
<summary>💡 Hint</summary>
Use Databricks Repos + GitHub Actions. On PR: deploy to staging workflow → run staging → validate output → if passes, merge and deploy to production.
</details>

<details>
<summary>✅ Solution</summary>

```yaml
# .github/workflows/etl_ci_cd.yml
name: ETL Pipeline CI/CD

on:
  pull_request:
    branches: [main]
    paths: ['pipelines/**']
  push:
    branches: [main]
    paths: ['pipelines/**']

jobs:
  # Step 1: Unit tests (fast, no cluster needed)
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pytest pyspark
      - run: pytest tests/ -v

  # Step 2: Deploy to staging and run
  staging:
    needs: unit-tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Update staging Repo
        run: |
          databricks repos update /Repos/staging/etl-pipelines --branch ${{ github.head_ref }}
      
      - name: Trigger staging workflow
        run: |
          RUN_ID=$(databricks jobs run-now $STAGING_JOB_ID --wait | jq '.run_id')
          echo "run_id=$RUN_ID" >> $GITHUB_OUTPUT
      
      - name: Validate staging output
        run: |
          python scripts/validate_staging.py --run-id $RUN_ID

  # Step 3: Deploy to production (only on merge to main)
  production:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    needs: staging
    runs-on: ubuntu-latest
    steps:
      - name: Update production Repo
        run: |
          databricks repos update /Repos/production/etl-pipelines --branch main
      
      - name: Verify production job config is current
        run: |
          terraform plan -detailed-exitcode terraform/production/
          # Exit code 2 = changes needed, apply them
```

```python
# scripts/validate_staging.py
"""Post-staging validation: check outputs are correct."""
import sys

def validate():
    # Check staging tables have data
    count = spark.table("staging.silver.orders").filter("_loaded_at >= current_date()").count()
    if count == 0:
        print("FAIL: No data in staging silver table")
        sys.exit(1)
    
    # Check no quality regressions
    null_rate = spark.sql("""
        SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*)
        FROM staging.silver.orders WHERE _loaded_at >= current_date()
    """).collect()[0][0]
    
    if null_rate > 0.01:
        print(f"FAIL: Null rate {null_rate:.2%} exceeds 1% threshold")
        sys.exit(1)
    
    print(f"PASS: {count} rows, {null_rate:.4%} null rate")
    sys.exit(0)

validate()
```

**Key Points:**
- PR triggers: unit tests → staging deployment → staging run → validation
- Only merge to main after staging passes (quality gate)
- Merge to main triggers production deployment (automatic, but validated)
- Databricks Repos: git branch → workspace folder (staging uses PR branch, prod uses main)
- Terraform manages job configs (cluster sizes, schedules) — separate from notebook code
- Validation script checks: data exists, quality metrics within threshold, no regressions
- Rollback: revert the merge → Repos auto-updates to previous code

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise Workflow Architecture

**Scenario:** Design the workflow architecture for 50 data engineers supporting 200+ pipelines. Requirements: standardized patterns, cost governance, self-service for teams, and centralized monitoring. How do you organize workflows?

<details>
<summary>💡 Hint</summary>
Template-based approach: provide reusable workflow templates, shared cluster policies, Terraform modules per team, centralized monitoring dashboard, and cost guardrails.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ENTERPRISE WORKFLOW ARCHITECTURE

ARCHITECTURE = {
    "governance": {
        "cluster_policies": {
            # Platform team defines policies; teams pick from approved configs
            "small_etl": {"max_workers": 4, "node_type": ["m5.large", "m5.xlarge"]},
            "medium_etl": {"max_workers": 16, "node_type": ["i3.xlarge", "r5.xlarge"]},
            "large_etl": {"max_workers": 32, "requires_approval": True},
            "gpu_ml": {"max_workers": 8, "node_type": ["g5.xlarge"], "requires_approval": True},
        },
        "cost_limits": {
            # Per-team monthly budget limits
            "max_monthly_dbu_per_team": 10000,
            "alert_at_80_percent": True,
            "hard_stop_at_100_percent": False,  # Alert only, don't kill jobs
        },
        "naming_convention": "team_domain_pipeline_name",
        "required_tags": ["team", "domain", "cost_center", "sla_tier"],
    },
    
    "self_service": {
        "terraform_modules": {
            # Teams use shared Terraform modules for standardized job creation
            "standard_etl": "Module: creates job with ingest→transform→validate pattern",
            "streaming_pipeline": "Module: creates continuous DLT + monitoring",
            "ml_training": "Module: creates GPU job with experiment tracking",
        },
        "cookiecutter_templates": {
            # Project templates for new pipelines
            "new_pipeline": "Generates: notebook, tests, terraform, CI/CD config",
        },
    },
    
    "monitoring": {
        "central_dashboard": "Grafana dashboard showing all 200+ jobs: status, duration, cost",
        "alerting": {
            "tier_1": "Critical pipelines: PagerDuty within 5 min of failure",
            "tier_2": "Important pipelines: Slack alert within 15 min",
            "tier_3": "Low-priority: daily summary email",
        },
        "sla_tracking": "system tables → nightly SLA compliance report",
        "cost_tracking": "per-team, per-job cost attribution (monthly chargeback)",
    },
    
    "organization": {
        "by_team": "Each team owns 20-30 workflows in their domain",
        "shared_workflows": "Platform team owns: ingestion layer, monitoring, infra",
        "cross_team": "Master orchestrator triggers team-owned child workflows",
    },
}

# TERRAFORM MODULE EXAMPLE (teams use this to create standardized jobs):
"""
module "sales_daily_etl" {
  source = "../modules/standard_etl"
  
  team          = "sales"
  pipeline_name = "daily_orders"
  schedule      = "0 0 6 * * ?"
  
  ingest_notebook    = "/Repos/sales/pipelines/ingest_orders"
  transform_notebook = "/Repos/sales/pipelines/transform_orders"
  validate_notebook  = "/Repos/sales/pipelines/validate_orders"
  
  cluster_policy = "medium_etl"
  sla_tier       = "tier_1"
  alert_emails   = ["sales-data@company.com"]
}
# The module handles: cluster config, notifications, monitoring, cost tags
# Team only specifies: their notebooks and schedule
"""
```

**Key Points:**
- Platform team provides guardrails: cluster policies, cost limits, naming conventions
- Teams get self-service within guardrails: Terraform modules for standard patterns
- Centralized monitoring: one dashboard for all 200+ workflows (SLA, cost, health)
- Cost governance: per-team budgets, required tags for attribution, monthly chargeback
- Standardization: templates ensure consistent structure (easier to debug, maintain)
- Scale: 50 engineers can independently manage their 20-30 pipelines each
- The platform team (5 people) manages shared infrastructure and tooling, not individual pipelines

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Workflow Performance Optimization

**Scenario:** Your daily ETL workflow takes 90 minutes (SLA: 60 minutes). It has 12 tasks: 4 ingestion (10 min each, parallel), 5 transforms (15 min each, sequential), 3 gold (5 min each, parallel). Optimize to meet the 60-minute SLA.

<details>
<summary>💡 Hint</summary>
Find the critical path. Sequential transforms (5 × 15 = 75 min) dominate. Solutions: parallelize transforms, increase cluster for transforms, or make them incremental.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT TIMELINE:
# Ingest (parallel): max(10, 10, 10, 10) = 10 min
# Transform (sequential): 15 + 15 + 15 + 15 + 15 = 75 min  ← BOTTLENECK
# Gold (parallel): max(5, 5, 5) = 5 min
# TOTAL: 10 + 75 + 5 = 90 min (exceeds 60 min SLA)

# OPTIMIZATION 1: Parallelize transforms where possible
# Analyze dependencies: do all 5 transforms depend on each other?
# Often: transform_A and transform_B are independent!
# Before: A → B → C → D → E (sequential, 75 min)
# After:  A → C → E (sequential, 45 min)
#         B → D      (parallel, 30 min)
# Critical path: max(45, 30) = 45 min for transforms
# NEW TOTAL: 10 + 45 + 5 = 60 min ✓

# OPTIMIZATION 2: Make transforms incremental (if still needed)
# Before: transform_orders processes ALL 500M rows (15 min)
# After: processes only NEW rows since last run (~500K rows, 2 min)
# Using: streaming silver tables or CDF-based incremental

# OPTIMIZATION 3: Larger cluster for the bottleneck phase
# Before: 4 workers for transforms
# After: 12 workers (3x parallelism within each transform)
# Expected: 15 min → 7 min per transform (not linear but significant)

# OPTIMIZATION 4: Shared cluster (eliminate startup times)
# Each task on a new cluster: 4 min startup × 12 tasks = 48 min wasted!
# Shared cluster: start once, reuse across all tasks = save 44 min
# (This alone might fix the SLA!)

# RECOMMENDED APPROACH (combining optimizations):
OPTIMIZED_CONFIG = {
    "tasks": [
        # Ingest: 4 tasks, parallel, shared cluster (10 min)
        {"task_key": "ingest_1", "job_cluster_key": "shared"},
        {"task_key": "ingest_2", "job_cluster_key": "shared"},
        {"task_key": "ingest_3", "job_cluster_key": "shared"},
        {"task_key": "ingest_4", "job_cluster_key": "shared"},
        
        # Transform: 5 tasks, MAXIMIZE PARALLELISM
        # A, B run parallel after ingest (independent tables)
        {"task_key": "transform_A", "depends_on": [{"task_key": "ingest_1"}], "job_cluster_key": "shared"},
        {"task_key": "transform_B", "depends_on": [{"task_key": "ingest_2"}], "job_cluster_key": "shared"},
        # C depends on A only
        {"task_key": "transform_C", "depends_on": [{"task_key": "transform_A"}], "job_cluster_key": "shared"},
        # D depends on B only
        {"task_key": "transform_D", "depends_on": [{"task_key": "transform_B"}], "job_cluster_key": "shared"},
        # E depends on C and D (fan-in)
        {"task_key": "transform_E", "depends_on": [{"task_key": "transform_C"}, {"task_key": "transform_D"}], "job_cluster_key": "shared"},
        
        # Gold: 3 tasks, parallel after E (5 min)
        {"task_key": "gold_1", "depends_on": [{"task_key": "transform_E"}], "job_cluster_key": "shared"},
        {"task_key": "gold_2", "depends_on": [{"task_key": "transform_E"}], "job_cluster_key": "shared"},
        {"task_key": "gold_3", "depends_on": [{"task_key": "transform_E"}], "job_cluster_key": "shared"},
    ],
    "job_clusters": [{
        "job_cluster_key": "shared",
        "new_cluster": {
            "autoscale": {"min_workers": 8, "max_workers": 16},  # Larger cluster
            "node_type_id": "i3.xlarge",
        }
    }]
}

# NEW CRITICAL PATH:
# Ingest: 10 min
# Transform: max(A→C, B→D) → E = max(30, 30) → 15 = 45 min
# But with larger cluster (3x compute): each transform takes ~7 min
# Critical path: 10 + max(14, 14) + 7 + 5 = 36 min (well within 60 min SLA!)
```

**Key Points:**
- First: find the critical path (longest sequence of dependent tasks)
- Parallelize independent tasks (biggest impact, zero cost increase)
- Shared cluster: eliminates 4-min startup per task (12 tasks × 4 min = 48 min saved!)
- Larger cluster: more workers = faster individual transforms (diminishing returns)
- Incremental processing: the ultimate fix (500K rows instead of 500M = 90% time reduction)
- Combine strategies: parallelism + shared cluster alone often halves execution time
- Always benchmark after changes — theoretical speedup vs actual may differ

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Region Disaster Recovery

**Scenario:** Your critical ETL workflow must have 99.99% availability (< 53 min downtime/year). If the primary region fails, the workflow must resume in the DR region within 15 minutes. Design the failover mechanism.

<details>
<summary>💡 Hint</summary>
Active-passive with synchronized state: same job definition in DR region (Terraform), S3 cross-region replication for data/checkpoints, automated failover detection, and DNS switching.
</details>

<details>
<summary>✅ Solution</summary>

```python
DR_WORKFLOW_ARCHITECTURE = {
    "primary": {
        "region": "us-east-1",
        "workspace": "prod-us-east",
        "job_id": 1001,
        "storage": "s3://lake-primary/",
    },
    "secondary": {
        "region": "us-west-2",
        "workspace": "prod-us-west (warm standby)",
        "job_id": 2001,  # Same config, different region
        "storage": "s3://lake-dr/",  # CRR from primary
    },
    
    "synchronization": {
        "data": "S3 Cross-Region Replication (RPO < 15 min)",
        "job_config": "Terraform applies same config to both regions",
        "checkpoints": "Replicated with S3 CRR (Auto Loader resumes from checkpoint)",
        "code": "Same Repos branch deployed to both workspaces",
    },
    
    "failover_automation": {
        "detection": "Lambda monitors primary job status every 1 minute",
        "trigger": "3 consecutive health check failures → initiate failover",
        "steps": [
            "1. Verify primary is truly down (not just a slow run)",
            "2. Verify S3 CRR is caught up (check replication lag)",
            "3. Trigger DR job in secondary workspace",
            "4. Update monitoring to track DR job",
            "5. Alert team: failover activated",
        ],
        "rto": "~10 minutes (detection: 3 min + verification: 2 min + trigger: 5 min)",
    },
    
    "failback": {
        "when": "Primary region recovers",
        "steps": [
            "1. Verify primary region stability (wait 30 min)",
            "2. Sync any data written to DR back to primary",
            "3. Resume primary job, stop DR job",
            "4. Update monitoring back to primary",
        ],
    },
    
    "testing": "Quarterly failover drill (actually fail over, run for 1 hour, fail back)",
    
    "cost": {
        "dr_workspace": "$200/month (warm standby, no active jobs)",
        "s3_crr": "$300/month (data replication)",
        "lambda_monitoring": "$5/month",
        "quarterly_drill": "$200/quarter (1 hour of DR cluster time)",
        "total_monthly": "~$520/month for 99.99% availability",
    },
}

# FAILOVER LAMBDA (simplified):
def failover_handler(event, context):
    """Triggered when primary health check fails 3 times."""
    
    # Verify primary is down
    primary_status = check_primary_health()
    if primary_status == "healthy":
        return {"action": "false_alarm"}
    
    # Check replication lag
    lag_seconds = get_s3_replication_lag()
    if lag_seconds > 900:  # 15 min
        alert("WARNING: DR data may be up to {lag_seconds}s behind!")
    
    # Trigger DR job
    response = requests.post(
        f"{DR_WORKSPACE_HOST}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DR_TOKEN}"},
        json={"job_id": 2001},
    )
    
    # Alert
    send_pagerduty("FAILOVER: Primary ETL workflow failed over to DR region")
    
    return {"action": "failover_triggered", "dr_run_id": response.json()["run_id"]}
```

**Key Points:**
- Same job config in both regions (Terraform ensures consistency)
- S3 CRR: data + checkpoints replicated (Auto Loader resumes from replicated checkpoint)
- Automated detection: Lambda health checks every minute (faster than human response)
- 10-minute RTO: 3 min detection + 2 min verification + 5 min job start
- 99.99% availability: 53 min/year downtime budget → 10 min RTO with quarterly testing is sufficient
- Cost: $520/month for DR (trivial for critical data pipelines)
- Key risk: S3 CRR lag (eventually consistent) → verify lag before activating DR
- Test regularly: a DR plan that's never tested will fail when you need it

</details>

</article>
