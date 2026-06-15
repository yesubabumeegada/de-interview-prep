---
title: "dbt Orchestration - Fundamentals"
topic: dbt
subtopic: dbt-orchestration
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, orchestration, airflow, dagster, prefect, dbt-cloud]
---

# dbt Orchestration — Fundamentals

## Why Orchestration Matters

dbt builds a transformation layer, but it doesn't schedule itself. You need an orchestrator to:
- Trigger dbt runs on a schedule (nightly, hourly, event-driven)
- Handle dependencies (run dbt after data lands in the warehouse)
- Retry failures automatically
- Alert on failures
- Coordinate dbt with other pipeline steps (extraction, loading, BI refresh)

## Core Orchestration Options

| Tool | Type | dbt Integration |
|---|---|---|
| dbt Cloud Jobs | Managed | Built-in UI scheduler |
| Apache Airflow | Open-source | BashOperator, Cosmos, dbt Cloud API |
| Dagster | Open-source | Native dbt-dagster integration |
| Prefect | Open-source/Cloud | dbt Cloud task, shell task |
| GitHub Actions | CI/CD | CLI in workflow steps |

## dbt Cloud Jobs (Simplest Path)

dbt Cloud has a built-in scheduler — no external orchestrator needed for simple cases.

```
dbt Cloud UI:
  Jobs → Create Job → Configure:
    - Commands: dbt build
    - Schedule: cron "0 6 * * *" (6 AM daily)
    - Target: production
    - Generate docs: ✓
    - Run source freshness: ✓
```

**dbt Cloud API trigger (for external systems):**

```bash
# Trigger a dbt Cloud job via API
curl -X POST \
  -H "Authorization: Token ${DBT_CLOUD_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"cause": "Triggered by upstream pipeline"}' \
  "https://cloud.getdbt.com/api/v2/accounts/${ACCOUNT_ID}/jobs/${JOB_ID}/run/"
```

## Airflow + dbt: BashOperator (Simplest)

The most basic integration: just run dbt CLI commands from Airflow.

```python
# dags/dbt_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_daily_build',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/dbt && dbt deps',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt && dbt run --target prod',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt && dbt test --target prod',
    )

    dbt_deps >> dbt_run >> dbt_test
```

**Problem with BashOperator:** One big task — if 1 model fails, the whole task fails. No model-level visibility.

## Airflow + dbt Cloud Operator

Airflow has a dbt Cloud provider that integrates with dbt Cloud API:

```bash
pip install apache-airflow-providers-dbt-cloud
```

```python
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

dbt_cloud_job = DbtCloudRunJobOperator(
    task_id='run_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud_default',
    job_id=12345,
    check_interval=30,  # poll every 30 seconds
    timeout=3600,       # fail if not done in 1 hour
)
```

## Prefect + dbt

```bash
pip install prefect-dbt
```

```python
from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow(name="dbt-daily-build")
def dbt_flow():
    dbt_build = DbtCoreOperation(
        commands=["dbt deps", "dbt build"],
        project_dir="/opt/dbt",
        profiles_dir="/home/user/.dbt",
    )
    dbt_build.run()

if __name__ == "__main__":
    dbt_flow()
```

## dbt CLI in CI/CD (GitHub Actions)

For PR validation — run dbt on every pull request:

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: dbt deps
        run: dbt deps

      - name: dbt build (modified models only)
        run: dbt build --select state:modified+
        env:
          DBT_TARGET: ci
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

## Key Concepts

**Slim CI (`state:modified+`):** Only run models that changed in the PR plus their downstream dependents. Much faster than running everything.

**`--defer`:** When a model hasn't been built in the CI environment, defer to the production version for upstream dependencies. Avoids rebuilding the entire DAG.

**Model-level vs. job-level orchestration:** BashOperator gives job-level visibility; Cosmos/Dagster give model-level visibility (each dbt model = an Airflow task or Dagster asset).

## Interview Questions

**Q: What is the simplest way to run dbt on a schedule?**
A: dbt Cloud Jobs — configure via UI, no external orchestrator needed.

**Q: Why would you use Airflow over dbt Cloud Jobs?**
A: When you need to coordinate dbt with other pipeline steps (e.g., run after Fivetran sync completes, before BI tool cache refresh), or when you need model-level task dependencies and retry logic.

**Q: What is slim CI in dbt?**
A: Running only `state:modified+` — models changed in the PR plus their downstream dependents. This uses dbt's state comparison against a production manifest to identify what changed, drastically reducing CI run times.
