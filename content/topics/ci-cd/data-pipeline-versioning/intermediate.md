---
title: "Data Pipeline Versioning - Intermediate"
topic: ci-cd
subtopic: data-pipeline-versioning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, versioning, dbt, slim-ci, blue-green, feature-flags, delta-schema, data-contracts]
---

# Data Pipeline Versioning — Intermediate

## dbt Slim CI with `state:modified+`

Running the full dbt project on every PR is slow and wasteful when you have hundreds of models. dbt's **Slim CI** feature solves this by running only the models that changed and their downstream dependencies.

The key is the `state:modified+` selector, which uses dbt's state comparison feature to identify what changed relative to a known baseline (the last production run):

```yaml
# .github/workflows/dbt-ci.yml
name: dbt Slim CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Download production manifest
        run: |
          # Pull the manifest.json from the last successful prod run
          # This file describes the state of every model in production
          aws s3 cp s3://my-dbt-artifacts/prod/manifest.json ./prod-manifest/manifest.json

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: dbt deps
        run: dbt deps

      - name: dbt build — modified models only
        run: |
          dbt build \
            --select state:modified+ \
            --state ./prod-manifest \
            --target ci \
            --profiles-dir .
        # state:modified+ means: models changed in this PR PLUS all their downstream dependents
        # --state points to the production manifest for comparison
        # --target ci uses a separate Snowflake schema (e.g., ANALYTICS_CI_PR123)

      - name: Upload CI manifest
        if: success()
        run: |
          aws s3 cp ./target/manifest.json \
            s3://my-dbt-artifacts/ci/pr-${{ github.event.number }}/manifest.json
```

```yaml
# profiles.yml
analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      schema: "ANALYTICS_DEV_{{ env_var('DBT_TARGET_SCHEMA', 'default') }}"

    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      schema: "ANALYTICS_CI_PR_{{ env_var('PR_NUMBER', '0') }}"
      # Each PR gets its own isolated schema
```

On merge to main, the production run uploads a fresh `manifest.json` to S3. The next PR's CI job downloads this manifest and uses it as the comparison baseline. This creates a chain of incremental comparisons.

## Blue-Green DAG Deployments

Blue-green deployment is a release strategy where two identical environments run in parallel: "blue" (current production) and "green" (new version). You test green, switch traffic, then decommission blue.

For Airflow DAGs, the pattern is:

```python
# Blue — current production DAG (running normally)
with DAG(
    dag_id='customer_metrics_blue',
    schedule_interval='0 3 * * *',
    is_paused_upon_creation=False,
) as blue_dag:
    # ... existing tasks ...

# Green — new version deployed alongside blue
with DAG(
    dag_id='customer_metrics_green',
    schedule_interval='0 4 * * *',  # Offset schedule — runs 1 hour later
    is_paused_upon_creation=True,   # Start paused
) as green_dag:
    # ... new version tasks ...
```

**Blue-green transition process:**

```python
# deployment/switch_to_green.py
from airflow.api.client.local_client import Client

client = Client(None, None)

# 1. Verify green ran successfully for N days
green_dag_runs = client.get_dag_runs('customer_metrics_green')
recent_runs = [r for r in green_dag_runs if r['state'] == 'success']
assert len(recent_runs) >= 3, "Green must succeed 3+ times before switching"

# 2. Compare output of blue vs green for data quality
# (You'd do this with a separate validation DAG)

# 3. Pause blue, activate green on same schedule
client.trigger_dag('customer_metrics_green', run_id='enable-prod-schedule')
# Update green's schedule_interval to match blue via Airflow API
# Pause blue
```

## Feature Flags for Pipeline Rollouts

Feature flags let you deploy code changes without activating them, then turn features on/off without redeployment. For data pipelines:

```python
# Using Airflow Variables as feature flags
from airflow.models import Variable

def should_enable_new_transform():
    """Check feature flag from Airflow Variables (set via UI or API)."""
    return Variable.get('enable_new_customer_scoring', default_var='false') == 'true'

def transform_customer_data(**context):
    if should_enable_new_customer_scoring():
        # New ML-based scoring logic
        result = new_ml_scoring(context['ds'])
    else:
        # Existing rule-based scoring
        result = rule_based_scoring(context['ds'])

    result.to_parquet(f"s3://data-lake/customers/scoring/{context['ds']}/data.parquet")
```

```python
# dbt: feature flags via project variables
# dbt_project.yml
vars:
  enable_new_attribution_model: false  # Toggle in CI or per-environment

# In your dbt model
{% if var('enable_new_attribution_model', false) %}
  -- New multi-touch attribution
  SELECT * FROM {{ ref('multi_touch_attribution') }}
{% else %}
  -- Existing last-touch attribution
  SELECT * FROM {{ ref('last_touch_attribution') }}
{% endif %}
```

```bash
# Enable for specific run without code change
dbt run --vars '{"enable_new_attribution_model": true}'
```

## Delta Schema Evolution

Delta Lake (used in Databricks) has built-in schema evolution support that's critical for production data pipelines where upstream source schemas change:

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read new data with potential schema additions
new_data = spark.read.json("s3://raw/orders/2024-01-15/")

# Option 1: mergeSchema — automatically adds new columns
new_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \   # New columns are added to table schema
    .save("s3://curated/orders/")

# Option 2: overwriteSchema — replace the schema (use with extreme caution)
new_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \  # Destructive! Replaces schema
    .save("s3://curated/orders/")
```

```python
# Table-level schema enforcement (default: strict, rejects extra columns)
# To enable evolution at the table level:
spark.sql("""
    ALTER TABLE curated.orders
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
                       'delta.minReaderVersion' = '2',
                       'delta.minWriterVersion' = '5')
""")

# Check schema history
delta_table = DeltaTable.forName(spark, "curated.orders")
print(delta_table.history().select("version", "operation", "operationParameters").show())
```

## Data Contract Versioning

A data contract is a formal agreement between a data producer and consumer specifying what data will be provided and in what format. Contracts use semantic versioning:

```yaml
# data-contracts/orders-v2.1.0.yaml
apiVersion: datacontract/v1
id: orders-contract
version: 2.1.0  # MAJOR.MINOR.PATCH
status: active

info:
  title: Orders Data Contract
  owner: checkout-team@company.com
  description: Daily order transaction data

models:
  orders:
    fields:
      order_id:
        type: string
        required: true
        description: Unique order identifier (UUID)
      customer_id:
        type: string
        required: true
      total_amount:
        type: decimal(12,2)
        required: true
      # Added in v2.1.0 — backward compatible (new optional field)
      discount_amount:
        type: decimal(12,2)
        required: false
        description: "Added in 2.1.0"

quality:
  freshness:
    warn_after: 25h
    error_after: 49h
  row_count:
    minimum: 1000  # Fail if orders drop below this threshold

changelog:
  - version: "2.1.0"
    date: "2024-01-15"
    type: minor
    description: "Add discount_amount field (optional, backward compatible)"
  - version: "2.0.0"
    date: "2023-11-01"
    type: major
    description: "Rename 'total' to 'total_amount' — BREAKING CHANGE"
```

## ADF Pipeline Export/Import (ARM Templates)

Azure Data Factory pipelines can be exported as ARM (Azure Resource Manager) templates and versioned in Git:

```bash
# Export ADF pipeline to ARM template
az datafactory pipeline export \
    --factory-name my-adf \
    --resource-group data-platform-rg \
    --name orders_ingestion_pipeline \
    --output-file adf-templates/orders_ingestion_pipeline.json

# Commit the ARM template to Git
git add adf-templates/
git commit -m "feat: add retry logic to orders ADF pipeline"

# Deploy ARM template to another environment
az deployment group create \
    --resource-group data-platform-prod-rg \
    --template-file adf-templates/orders_ingestion_pipeline.json \
    --parameters factoryName=my-adf-prod
```

ADF also supports native Git integration through the ADF Studio — changes are committed to a `adf_publish` branch as ARM templates automatically.

## Key Interview Takeaways

- **Slim CI** (`state:modified+`) is the production dbt CI pattern — full runs are too slow for PRs
- **Blue-green DAG deployment** is the safe way to release breaking Airflow changes — run both versions, validate, then switch
- **Feature flags** in Airflow Variables or dbt `vars` let you activate code changes without redeployment
- **Delta `mergeSchema`** handles additive schema evolution automatically; `overwriteSchema` is destructive
- **Data contracts** formalize producer-consumer agreements with semantic versioning — changelogs document breaking vs. non-breaking changes
