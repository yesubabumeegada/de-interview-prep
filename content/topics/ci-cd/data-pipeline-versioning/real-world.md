---
title: "Data Pipeline Versioning - Real-World Patterns"
topic: ci-cd
subtopic: data-pipeline-versioning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, versioning, data-contracts, production-patterns, migration, dbt, airflow, rollback]
---

# Data Pipeline Versioning — Real-World Patterns

## The Production Reality of Pipeline Versioning

Most teams start with no versioning discipline at all — models are modified directly in production, there's no record of what changed when, and a bad deployment means scrambling to remember "what did the code look like last Tuesday?" The maturity progression looks like this:

**Level 0 — No versioning**: Direct edits, no Git, no rollback capability  
**Level 1 — Basic Git**: Code in Git but no release process, no CI, no tagging  
**Level 2 — CI with tests**: PR-based workflow, dbt test runs on PRs, manual apply  
**Level 3 — Full GitOps**: Slim CI, automated deploys, data contracts, version tags  
**Level 4 — Contract-first**: Data contracts defined before pipelines, consumers own SLAs, automated contract testing

Most companies interview at Level 3-4 expectations. This section covers the patterns that separate Level 2 from Level 3/4.

## Real Pattern: The dbt Release Train

A "release train" approach treats dbt deployments like software releases — a regular cadence rather than ad-hoc pushes whenever a PR merges:

```yaml
# .github/workflows/dbt-release-train.yml
name: dbt Release Train

on:
  schedule:
    - cron: '0 18 * * 1-5'  # Weekdays at 6pm — release train departs daily

  workflow_dispatch:  # Allow manual trigger for urgent fixes
    inputs:
      reason:
        description: 'Reason for out-of-band release'
        required: true

jobs:
  release:
    runs-on: ubuntu-latest
    environment: production

    steps:
      - uses: actions/checkout@v4
        with:
          ref: main  # Always deploy from main

      - name: Get version
        id: version
        run: |
          # Use today's date as the release version identifier
          echo "version=$(date +%Y.%m.%d)" >> $GITHUB_OUTPUT
          echo "git_sha=$(git rev-parse --short HEAD)" >> $GITHUB_OUTPUT

      - name: dbt deps
        run: dbt deps

      - name: dbt build — production
        run: |
          dbt build \
            --target prod \
            --threads 8

      - name: Upload release manifest
        run: |
          VERSION=${{ steps.version.outputs.version }}
          SHA=${{ steps.version.outputs.git_sha }}

          # Save manifest for next CI run's slim CI comparison
          aws s3 cp ./target/manifest.json \
            s3://dbt-artifacts/prod/manifest.json

          # Also save a versioned copy for rollback and audit
          aws s3 cp ./target/manifest.json \
            s3://dbt-artifacts/releases/${VERSION}-${SHA}/manifest.json

          # Save run_results.json for observability
          aws s3 cp ./target/run_results.json \
            s3://dbt-artifacts/prod/run_results.json

      - name: Tag release
        run: |
          git tag "dbt-release-${{ steps.version.outputs.version }}"
          git push origin "dbt-release-${{ steps.version.outputs.version }}"
```

The key insight: by deploying on a schedule rather than on every merge, you get natural batching of changes. Small fixes accumulate during the day and ship together at 6pm. Emergency fixes can still trigger a manual release with the `workflow_dispatch` event.

## Real Pattern: Rolling Back a dbt Deployment

When a deployed dbt change causes bad data, you need a fast rollback. This is more nuanced than rolling back application code because the data written by the bad version already exists in the warehouse.

```bash
# Scenario: Release 2024.01.15 introduced a bug in the revenue model
# Bad data was written to analytics.fct_revenue for 2024-01-15

# Step 1: Immediately pause dependent dashboards (ideally via API)
# Step 2: Roll back the code
git checkout dbt-release-2024.01.14  # Previous known-good release

# Step 3: Rerun ONLY the affected models using the old code
# Download the previous production manifest (for slim CI targeting)
aws s3 cp s3://dbt-artifacts/releases/2024.01.14-abc1234/manifest.json ./rollback-manifest/manifest.json

# Rerun the affected model and all downstreams with old code
dbt build \
  --select fct_revenue+ \
  --target prod \
  --full-refresh  # For snapshot/incremental models, full refresh to fix all affected data

# Step 4: Verify the fix
# Run data quality checks
dbt test --select fct_revenue

# Step 5: PR the fix to main (don't leave main broken)
git checkout main
git checkout -b fix/revenue-calculation-bug
# Apply the fix...
git commit -m "fix(revenue): correct division by zero in daily revenue aggregation"
```

The critical lesson: a rollback has two parts — rollback the code AND fix the data. Rolling back only the code leaves bad data in the warehouse.

## Real Pattern: Airflow DAG Versioning with Git-Synced Deployments

In production Airflow (typically on Kubernetes with KubernetesExecutor), DAGs are synchronized from a Git repository:

```yaml
# Helm values for Airflow with git-sync (sync DAGs from Git)
dags:
  gitSync:
    enabled: true
    repo: https://github.com/company/data-platform.git
    branch: main
    subPath: airflow/dags
    period: 60  # Sync every 60 seconds

# This means: merging to main deploys to production within ~60 seconds
# No separate deploy step needed — Git IS the deployment mechanism
```

The challenge: with git-sync, every merge to main immediately goes to production. This makes DAG versioning critical:

```python
# airflow/dags/orders_etl.py
"""
Orders ETL Pipeline
Version: 2.1.0
Changelog:
  2.1.0 (2024-01-15): Add discount calculations, fix null handling in customer_id
  2.0.0 (2024-01-01): BREAKING — split single orders DAG into orders + returns
  1.5.0 (2023-11-01): Add retry logic for API calls
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Version metadata — queryable from Airflow UI and API
PIPELINE_VERSION = "2.1.0"
PIPELINE_OWNER = "checkout-team"

default_args = {
    'owner': PIPELINE_OWNER,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['checkout-oncall@company.com'],
}

with DAG(
    dag_id='orders_etl',
    default_args=default_args,
    description=f'Orders ETL v{PIPELINE_VERSION}',
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['orders', 'checkout-team', f'v{PIPELINE_VERSION}'],
    doc_md=f"""
    # Orders ETL Pipeline
    **Version:** {PIPELINE_VERSION}
    **Owner:** {PIPELINE_OWNER}
    **Schedule:** Daily at 3am UTC

    ## Description
    Ingests daily order transactions from the checkout API and loads into
    the curated orders table in Snowflake.

    ## Changelog
    See `CHANGELOG.md` in the data-platform repository.
    """,
) as dag:
    pass
```

## Real Pattern: Schema Migration with Zero Downtime

Renaming a column in Snowflake without breaking any downstream consumers:

```sql
-- Phase 1: Add new column (no downtime, backward compatible)
ALTER TABLE analytics.fct_orders ADD COLUMN order_total_amount DECIMAL(12,2);
UPDATE analytics.fct_orders SET order_total_amount = order_total;

-- Phase 2: Update pipeline to write to BOTH columns (deploy code change)
-- In your ETL/dbt model:
-- SELECT
--   total AS order_total,           -- old name (still populated)
--   total AS order_total_amount     -- new name (also populated)

-- Phase 3: Migrate consumers (track in GitHub issue)
-- Update dashboard A: change references from order_total → order_total_amount
-- Update pipeline B: change references from order_total → order_total_amount
-- Give consumers 4 weeks

-- Phase 4: Deprecate old column
-- Add COMMENT to old column indicating deprecation
COMMENT ON COLUMN analytics.fct_orders.order_total
  IS 'DEPRECATED as of 2024-02-01. Use order_total_amount. Will be removed 2024-03-01.';

-- Phase 5: Remove old column (after confirming no consumers)
-- Verify using query history first
ALTER TABLE analytics.fct_orders DROP COLUMN order_total;
```

## Real Pattern: Data Contract Testing in CI

Contracts should be tested automatically, not just documented:

```python
# scripts/test_data_contract.py
import yaml
import great_expectations as gx
from pathlib import Path

def test_contract(contract_path: str, table_name: str):
    """Validate a table against its data contract."""
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    context = gx.get_context()
    datasource = context.sources.add_snowflake(
        name="snowflake",
        connection_string=os.environ["SNOWFLAKE_CONN"]
    )

    # Test each field in the contract
    suite = context.add_expectation_suite(f"{table_name}_contract_suite")

    for field_name, field_spec in contract['models'][table_name]['fields'].items():
        if field_spec.get('required', False):
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=field_name)
            )
        if 'type' in field_spec:
            # Map contract types to GX expectations
            suite.add_expectation(
                gx.expectations.ExpectColumnToExist(column=field_name)
            )

    # Test freshness SLO from contract
    if 'quality' in contract and 'freshness' in contract['quality']:
        warn_hours = contract['quality']['freshness']['warn_after'].replace('h', '')
        suite.add_expectation(
            gx.expectations.ExpectColumnMaxToBeBetween(
                column='updated_at',
                min_value=datetime.now() - timedelta(hours=int(warn_hours)),
                max_value=datetime.now()
            )
        )

    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=f"contract_test_{datetime.now().isoformat()}"
    )
    return results.success
```

## Key Real-World Takeaways

- The **release train pattern** (scheduled deployment rather than on every merge) gives natural batching and a clear rollback boundary
- **Code rollback ≠ data rollback** — a complete rollback requires both reverting the code AND reprocessing affected data partitions
- Git-synced Airflow (git-sync in Kubernetes) makes `main` branch = production — DAG versioning conventions (docstrings, tags) are the only history
- **Zero-downtime column migration** is a 5-phase process: add new → populate both → migrate consumers → deprecate → drop old
- **Contract testing in CI** ensures contracts stay honest — if the pipeline violates its own contract, the build fails
