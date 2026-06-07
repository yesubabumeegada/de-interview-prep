---
title: "dbt Cloud & CI/CD - Real-World"
topic: dbt
subtopic: dbt-cloud-and-ci-cd
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, production, deployment, cicd, real-world]
---

# dbt Cloud & CI/CD — Real-World Examples

## Example 1: Full Production CI/CD Pipeline

```yaml
# .github/workflows/dbt-full-pipeline.yml
name: dbt Full CI/CD

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

env:
  DBT_PROFILES_DIR: .
  DBT_TARGET: ${{ github.ref == 'refs/heads/main' && 'prod' || 'ci' }}
  ARTIFACT_BUCKET: my-company-dbt-artifacts

jobs:
  # PR: Slim CI
  slim-ci:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with: {python-version: '3.11'}

      - name: Cache packages
        uses: actions/cache@v3
        with:
          path: dbt_packages/
          key: dbt-packages-${{ hashFiles('packages.yml') }}

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: dbt deps
        run: dbt deps

      - name: Download prod state
        run: |
          aws s3 cp s3://$ARTIFACT_BUCKET/prod/manifest.json ./prod-state/

      - name: Slim CI build
        run: |
          dbt build \
            --select state:modified+ \
            --defer \
            --state ./prod-state \
            --target ci \
            --fail-fast
        env:
          DBT_SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
          DBT_SNOWFLAKE_PASS: ${{ secrets.SNOWFLAKE_CI_PASS }}

      - name: Comment PR with results
        if: always()
        run: |
          python3 scripts/pr_comment.py \
            --results target/run_results.json \
            --pr ${{ github.event.number }}

  # Merge to main: Full production deploy
  production-deploy:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install + dbt build prod
        run: |
          pip install dbt-snowflake==1.7.0
          dbt deps
          dbt snapshot
          dbt build --target prod --threads 16

      - name: Upload prod manifest
        if: success()
        run: |
          aws s3 cp target/manifest.json \
            s3://$ARTIFACT_BUCKET/prod/manifest.json
          aws s3 cp target/manifest.json \
            s3://$ARTIFACT_BUCKET/archive/$(date +%Y-%m-%d)/manifest.json

      - name: Slack notification
        if: always()
        uses: 8398a7/action-slack@v3
        with:
          status: ${{ job.status }}
          channel: '#data-deploys'
```

## Example 2: dbt Cloud + Airflow Integration

```python
# dags/data_pipeline_dag.py
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_pagerduty_alert,
}

with DAG(
    dag_id='daily_data_pipeline',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # 1. Fivetran sync trigger
    fivetran_sync = FivetranOperator(
        task_id='sync_shopify',
        connector_id='shopify_prod',
    )

    # 2. dbt Core transform
    dbt_run = DbtCloudRunJobOperator(
        task_id='dbt_production_run',
        dbt_cloud_conn_id='dbt_cloud_prod',
        job_id=12345,
        check_interval=30,
        timeout=3600,
    )

    # 3. Data quality check
    quality_check = DbtCloudRunJobOperator(
        task_id='dbt_quality_tests',
        dbt_cloud_conn_id='dbt_cloud_prod',
        job_id=12346,  # Job with just `dbt test --select tag:critical`
    )

    fivetran_sync >> dbt_run >> quality_check
```

## Example 3: Zero-Downtime Deployment Pattern

For financial systems where dashboards must not show partial data:

```bash
#!/bin/bash
# deploy_atomic.sh

echo "=== Atomic dbt Deployment ==="

# Step 1: Build into shadow schema
echo "Building into shadow schema..."
dbt build \
  --target prod \
  --vars '{"target_schema": "analytics_shadow"}' \
  --threads 16

# Step 2: Run reconciliation tests
echo "Running reconciliation tests..."
dbt run-operation compare_shadow_to_prod

# Step 3: Atomic schema swap (Snowflake)
echo "Swapping schemas..."
snowsql -q "
ALTER SCHEMA analytics RENAME TO analytics_old;
ALTER SCHEMA analytics_shadow RENAME TO analytics;
"

# Step 4: Verify
echo "Verifying new schema..."
dbt test --select tag:smoke_test --target prod

# Step 5: Cleanup
echo "Cleaning up old schema..."
snowsql -q "DROP SCHEMA IF EXISTS analytics_old CASCADE;"

echo "=== Deployment complete! ==="
```

## Example 4: Team Onboarding Automation

New engineer setup in one command:

```bash
#!/bin/bash
# scripts/setup_dev_environment.sh

echo "Setting up dbt development environment for $DBT_USER..."

# Create personal dev schema
snowsql -q "CREATE SCHEMA IF NOT EXISTS DEV_DB.dbt_${DBT_USER};"

# Clone prod tables for fast dev start (zero-copy)
dbt run-operation clone_prod_to_dev \
  --args "{\"target_schema\": \"dbt_${DBT_USER}\"}"

# Verify setup
dbt debug
dbt compile

echo "Setup complete! Your schema: DEV_DB.dbt_${DBT_USER}"
echo "Run: dbt run --select staging.* to start"
```
