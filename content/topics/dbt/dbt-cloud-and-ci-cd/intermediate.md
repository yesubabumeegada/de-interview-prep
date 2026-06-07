---
title: "dbt Cloud & CI/CD - Intermediate"
topic: dbt
subtopic: dbt-cloud-and-ci-cd
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, slim-ci, state, artifacts, deployment-strategies]
---

# dbt Cloud & CI/CD — Intermediate

## Slim CI — Only Test What Changed

The most important CI optimization for dbt:

```bash
# Traditional CI (SLOW): rebuild everything
dbt build --target ci

# Slim CI (FAST): only build changed models + their dependents
dbt build \
  --select state:modified+ \
  --defer \
  --state ./prod-manifest \
  --target ci
```

### Setup Slim CI in GitHub Actions

```yaml
# .github/workflows/dbt-slim-ci.yml
jobs:
  slim-ci:
    steps:
      - name: Download prod manifest
        run: |
          aws s3 cp \
            s3://${{ env.ARTIFACT_BUCKET }}/prod/manifest.json \
            ./prod-state/manifest.json

      - name: dbt slim CI build
        run: |
          dbt build \
            --select state:modified+ \
            --defer \
            --state ./prod-state \
            --target ci \
            --fail-fast

      - name: Upload CI manifest
        if: success()
        run: |
          aws s3 cp \
            ./target/manifest.json \
            s3://${{ env.ARTIFACT_BUCKET }}/ci/pr-${{ github.event.number }}/manifest.json
```

### Artifact Storage Strategy

```
s3://company-dbt-artifacts/
├── prod/
│   └── manifest.json          # Updated on every production deploy
├── ci/
│   ├── pr-123/manifest.json   # Each PR's CI result
│   └── pr-456/manifest.json
└── archive/
    └── 2024-01-15/manifest.json  # Daily backup
```

## dbt Cloud API Integration

Trigger dbt Cloud jobs from Airflow:

```python
# airflow/operators/dbt_cloud_operator.py
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

trigger_dbt_job = SimpleHttpOperator(
    task_id='trigger_dbt_production_run',
    http_conn_id='dbt_cloud',
    endpoint='/api/v2/accounts/{{ var("DBT_ACCOUNT_ID") }}/jobs/{{ var("DBT_JOB_ID") }}/run/',
    method='POST',
    headers={'Authorization': 'Token {{ var("DBT_API_TOKEN") }}'},
    data=json.dumps({
        "cause": "Triggered by Airflow DAG",
        "git_branch": "main",
        "steps_override": ["dbt build --select tag:daily"]
    }),
    response_check=lambda response: response.json()['status']['is_complete'] == False,
)
```

## Multi-Environment Job Configuration

```yaml
# Production job (dbt Cloud UI configuration)
Job: "Nightly Production"
Environment: prod
Commands:
  - dbt source freshness --select source:critical.*
  - dbt snapshot
  - dbt build --exclude tag:weekly --threads 16
  - dbt source freshness  # Final freshness check after run
Schedule: 0 5 * * *  # 5am UTC daily
Generate docs: true
Run on schedule: true

# Staging job (quality gate before prod)
Job: "Staging Validation"
Environment: staging
Triggered by: Production deploy complete
Commands:
  - dbt build --select tag:staging_validation
  - dbt test --select tag:reconciliation
```

## Environment Variables in dbt Cloud

Set environment-level vars in dbt Cloud UI:
```
Environment: Production
Variables:
  DBT_SNOWFLAKE_ACCOUNT: xy12345.us-east-1
  DBT_THREADS: 16
  DBT_SCHEMA: analytics

Sensitive (encrypted):
  DBT_PASSWORD: *** 
```

Reference in profiles.yml:
```yaml
prod:
  account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
  threads: "{{ env_var('DBT_THREADS', '8') | int }}"
```

## Run Status Monitoring

```python
# Poll dbt Cloud job status
import requests
import time

def wait_for_dbt_job(account_id, run_id, api_token, timeout=3600):
    headers = {'Authorization': f'Token {api_token}'}
    
    for _ in range(timeout // 30):
        response = requests.get(
            f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/',
            headers=headers
        )
        status = response.json()['data']['status']
        
        if status == 10:  # Success
            return True
        elif status in [20, 30]:  # Error or Cancelled
            raise Exception(f"dbt Cloud job failed with status {status}")
        
        time.sleep(30)
    
    raise TimeoutError("dbt Cloud job timed out")
```

## Blue/Green Deployments

```bash
# Blue/Green deployment pattern for zero-downtime schema updates

# 1. Build into staging schema
dbt run --target prod --vars '{"schema_suffix": "_staging"}'

# 2. Validate
dbt test --target prod --vars '{"schema_suffix": "_staging"}'

# 3. Swap schemas atomically (Snowflake)
dbt run-operation swap_schemas --args \
  '{"schema_a": "analytics_staging", "schema_b": "analytics"}'
```
