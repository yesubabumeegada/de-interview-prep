---
title: "dbt Orchestration - Senior Deep Dive"
topic: dbt
subtopic: dbt-orchestration
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, orchestration, mesh, slim-ci, defer, architecture, multi-project]
---

# dbt Orchestration — Senior Deep Dive

## Production-Grade Slim CI Architecture

### Complete CI/CD Pipeline Design

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI/CD

on:
  pull_request:
    branches: [main]
    paths:
      - 'models/**'
      - 'macros/**'
      - 'tests/**'
      - 'dbt_project.yml'
      - 'packages.yml'

jobs:
  dbt-slim-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.2

      - name: dbt deps
        run: dbt deps

      - name: Download production manifest
        env:
          DBT_API_TOKEN: ${{ secrets.DBT_API_TOKEN }}
          DBT_ACCOUNT_ID: ${{ secrets.DBT_ACCOUNT_ID }}
          DBT_JOB_ID: ${{ secrets.DBT_PROD_JOB_ID }}
        run: |
          mkdir -p prod_state
          curl -fL \
            -H "Authorization: Token ${DBT_API_TOKEN}" \
            "https://cloud.getdbt.com/api/v2/accounts/${DBT_ACCOUNT_ID}/jobs/${DBT_JOB_ID}/artifacts/manifest.json" \
            -o prod_state/manifest.json

      - name: dbt build (slim CI)
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: dbt_ci_role
          SNOWFLAKE_WAREHOUSE: ci_warehouse
          SNOWFLAKE_DATABASE: analytics
          SNOWFLAKE_SCHEMA: dbt_ci_${{ github.event.pull_request.number }}
        run: |
          dbt build \
            --select "state:modified+" \
            --defer \
            --state prod_state/ \
            --target ci

      - name: Cleanup CI schema
        if: always()
        run: |
          dbt run-operation drop_schema \
            --args '{"schema_name": "dbt_ci_${{ github.event.pull_request.number }}"}'
```

### Multi-Environment State Management

```
Production manifest storage strategy:
  Option A: dbt Cloud artifact API (simplest)
  Option B: S3 bucket (most flexible)
  Option C: GCS bucket

# S3-based state storage
aws s3 cp prod_state/manifest.json \
  s3://my-dbt-artifacts/prod/manifest.json

# In CI, download it:
aws s3 cp s3://my-dbt-artifacts/prod/manifest.json prod_state/
```

## Advanced Dagster Patterns

### Partitioned dbt Assets

```python
from dagster import DailyPartitionsDefinition, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")

@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="tag:daily_incremental",
    partitions_def=daily_partitions,
)
def daily_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Pass the partition date as a dbt variable
    dbt_vars = {"partition_date": context.partition_key}
    yield from dbt.cli(
        ["build", "--vars", str(dbt_vars)],
        context=context
    ).stream()
```

In your dbt model:

```sql
-- models/incremental/fct_events.sql
{{ config(materialized='incremental', unique_key='event_id') }}

select * from {{ ref('stg_events') }}
{% if is_incremental() %}
where event_date = '{{ var("partition_date") }}'
{% endif %}
```

### Coordinating dbt with Fivetran/dlt in Dagster

```python
from dagster_fivetran import FivetranResource, build_fivetran_assets
from dagster_dbt import DbtCliResource, dbt_assets

fivetran_assets = build_fivetran_assets(
    connector_id="salesforce_connector",
    destination_tables=["salesforce.account", "salesforce.opportunity"],
)

@dbt_assets(manifest=...)
def dbt_assets_group(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Dagster automatically wires Fivetran tables as upstream of dbt models
# that ref() them via dbt sources
```

## dbt Mesh Architecture

### Project Structure for Mesh

```
data-platform/
├── projects/
│   ├── platform/          # Core infrastructure models (shared)
│   │   ├── dbt_project.yml
│   │   └── models/
│   │       └── core/      # fct_orders, dim_customers (public)
│   ├── marketing/         # Marketing analytics
│   │   ├── dbt_project.yml
│   │   └── models/        # refs platform.fct_orders
│   └── finance/           # Finance analytics
│       ├── dbt_project.yml
│       └── models/        # refs platform.fct_orders
```

### Declaring Public Models with Contracts

```yaml
# platform/models/schema.yml
models:
  - name: fct_orders
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: varchar
        constraints:
          - type: not_null
      - name: customer_id
        data_type: varchar
        constraints:
          - type: not_null
      - name: order_date
        data_type: date
      - name: total_amount
        data_type: numeric
```

### Model Versioning for Breaking Changes

```yaml
# platform/models/schema.yml
models:
  - name: fct_orders
    latest_version: 2
    versions:
      - v: 1
        defined_in: fct_orders_v1  # legacy, deprecated
      - v: 2
        # default: no defined_in needed if filename matches
```

```sql
-- Consuming projects: reference specific version
select * from {{ ref('fct_orders', v=2) }}
-- or just ref the latest
select * from {{ ref('fct_orders') }}  -- resolves to v2
```

## Advanced CI/CD Patterns

### Matrix CI — Test Multiple Warehouses

```yaml
# .github/workflows/dbt_matrix.yml
jobs:
  dbt-test:
    strategy:
      matrix:
        target: [snowflake, bigquery, redshift]
    runs-on: ubuntu-latest
    steps:
      - name: dbt build
        run: dbt build --target ${{ matrix.target }}
```

### Progressive Delivery

```bash
# Deploy in stages: canary → staging → production

# Stage 1: Run against 1% sample in canary schema
dbt run --target canary \
  --vars '{"sample_rate": 0.01}'

# Stage 2: Run in staging with full data
dbt run --target staging

# Stage 3: Production
dbt run --target prod
```

### dbt Job Chaining in dbt Cloud

```bash
# Job A: Source freshness + extract
# Job B: Triggered after A succeeds via webhook/API

# After Job A completes, trigger Job B:
curl -X POST \
  -H "Authorization: Token ${DBT_API_TOKEN}" \
  "https://cloud.getdbt.com/api/v2/accounts/${ACCOUNT_ID}/jobs/${JOB_B_ID}/run/" \
  -d '{"cause": "Triggered by Job A", "git_sha": "'"${GIT_SHA}"'"}'
```

## Orchestration Decision Framework

```
Q: Who manages infrastructure?
  → Nobody → dbt Cloud Jobs

Q: Need to coordinate with non-dbt tasks (Fivetran, custom Python)?
  → Yes + asset-based thinking → Dagster
  → Yes + task-based thinking → Airflow + Cosmos

Q: Need model-level visibility in Airflow?
  → Yes → Cosmos
  → No → BashOperator

Q: Multi-team, domain-oriented projects?
  → Yes → dbt Mesh (requires dbt Cloud)
```

## Interview Questions for Seniors

**Q: How do you handle a scenario where slim CI misses a breaking change because the changed file isn't a dbt model (e.g., a macro change)?**
A: `state:modified` detects macro changes that affect compiled model SQL — when a macro changes, dbt recompiles dependent models and the compiled SQL changes, triggering state detection. However, changes to tests/, seeds/, or analyses/ files may not always be caught. The defense is: (1) always run `state:modified+` not just `state:modified`, (2) add a "full rebuild" job that runs weekly, (3) test macro changes explicitly with `--select "exposure:*"` or by tagging affected models.

**Q: In dbt Mesh, what happens if the upstream project changes the contract of a public model?**
A: dbt enforces contracts at build time. If the upstream project removes or renames a column in a contracted public model, downstream projects will fail their builds when they try to `ref()` that model. The solution is model versioning: maintain `v1` alongside `v2`, give consumers time to migrate, then deprecate `v1`.

**Q: How would you design observability for a dbt + Airflow pipeline to minimize MTTR (mean time to repair)?**
A: Multiple layers: (1) Airflow task-level alerting (Slack on failure); (2) model-level via Cosmos so the exact failing model is the first thing visible; (3) dbt `store_failures: true` on key tests so failed rows are persisted for inspection; (4) structured logging of dbt run results to a warehouse table via `dbt_run_results`; (5) data quality dashboards (e.g., Elementary) for trend monitoring.
