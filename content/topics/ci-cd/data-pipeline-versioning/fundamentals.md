---
title: "Data Pipeline Versioning - Fundamentals"
topic: ci-cd
subtopic: data-pipeline-versioning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, versioning, data-pipelines, git, dbt, airflow, schema-evolution]
---

# Data Pipeline Versioning — Fundamentals

## Why Versioning Data Pipelines Is Different from Versioning Applications

When you version an application, a bug in version 1.2 means you rollback to 1.1 and the app works again. When you version a data pipeline, a bug in version 1.2 may have already written incorrect data to your warehouse, downstream reports may have already consumed it, and rolling back the code doesn't undo the data. This asymmetry is what makes data pipeline versioning uniquely challenging.

The goals of data pipeline versioning are:
1. **Auditability**: Know exactly what code produced a given dataset on a given date
2. **Reproducibility**: Rerun the pipeline from any past version and get the same output
3. **Safe deployment**: Release changes without breaking downstream consumers
4. **Rollback capability**: Revert to a previous version without data corruption

## Git as the Foundation

Everything starts with Git. Every data pipeline — whether it's a dbt project, an Airflow DAG, an ADF pipeline, or a PySpark script — should live in a Git repository. This gives you:

- A complete history of every change
- The ability to tag releases (`git tag v1.2.0`)
- Branching for development without affecting production
- Pull requests for peer review of pipeline changes
- `git blame` to find who changed what and when

### Semantic Versioning for Data Pipelines

Software uses semantic versioning (`MAJOR.MINOR.PATCH`). For data pipelines, apply the same convention with data-specific semantics:

- **MAJOR** (`2.0.0`): Breaking change — consumers must update. Examples: column renamed, column removed, data type changed from string to integer, aggregation logic changed in a way that affects values.
- **MINOR** (`1.1.0`): Backward-compatible addition. Examples: new column added, new table added, performance improvement that doesn't change output.
- **PATCH** (`1.0.1`): Bug fix that doesn't change the schema or expected output for correct data.

```bash
# Tagging a dbt project release
git tag -a v2.1.0 -m "Add customer lifetime value model, fix null handling in orders"
git push origin v2.1.0
```

## dbt Project Versioning with Git Tags

dbt projects are particularly well-suited to Git-based versioning because the entire transformation logic lives in `.sql` and `.yml` files:

```yaml
# dbt_project.yml — version your project
name: 'analytics'
version: '2.1.0'       # Matches your git tag
config-version: 2

# This version is accessible in macros as project.version
# and appears in dbt docs
```

A disciplined release process for dbt:

```bash
# Feature branch workflow
git checkout -b feature/add-ltv-model
# ... make changes ...
git commit -m "feat: add customer_ltv model to marts layer"
git push origin feature/add-ltv-model

# Open PR → CI runs dbt compile + dbt test
# PR is reviewed, approved, and merged to main

# Release: tag the version
git checkout main
git pull
git tag -a v2.1.0 -m "Release 2.1.0: Add LTV model"
git push origin v2.1.0

# Deploy: CI/CD picks up the tag and deploys to prod
```

## Airflow DAG Versioning

Airflow DAGs have a specific versioning challenge: the `dag_id` is the primary identifier for a DAG, and changing it creates a new DAG in Airflow's history while the old one disappears.

### The dag_id Versioning Pattern

One approach is embedding the version in the `dag_id`:

```python
# v1 — original DAG
dag = DAG(
    dag_id='orders_etl_v1',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# v2 — breaking change: adding new transformation step
# Keep v1 running until v2 is verified, then pause v1
dag = DAG(
    dag_id='orders_etl_v2',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),  # V2 starts from today
    catchup=False,
)
```

### run_id and Idempotency

Airflow's `run_id` is the unique identifier for a specific execution of a DAG. For idempotency (running the same DAG run twice produces the same result), every task should be designed to be re-runnable:

```python
# Non-idempotent (bad): appends data every time
def load_orders(**context):
    df = fetch_orders(date=context['ds'])
    df.to_sql('orders', engine, if_exists='append')  # Duplicate rows on rerun!

# Idempotent (good): replaces data for the partition
def load_orders(**context):
    execution_date = context['ds']
    df = fetch_orders(date=execution_date)

    # Delete existing data for this date, then insert
    engine.execute(f"DELETE FROM orders WHERE date = '{execution_date}'")
    df.to_sql('orders', engine, if_exists='append')
    # Now reruns are safe — same result every time
```

## Schema Versioning Basics

Schema versioning is about managing changes to the structure of your data over time. Two key concepts:

**Backward compatibility**: New code can read data written by old code. A new pipeline version can read files written by the previous version.

**Forward compatibility**: Old code can read data written by new code. A previous pipeline version can still read files written by the new version (important for gradual rollouts).

The safest schema changes (backward compatible):
- Adding a new optional column with a default value
- Adding a new table or partition
- Widening a column (e.g., INT to BIGINT)

Breaking schema changes:
- Removing a column
- Renaming a column
- Changing a column's data type in an incompatible way
- Changing the meaning of an existing column

```sql
-- SAFE: Adding a nullable column (backward compatible)
ALTER TABLE orders ADD COLUMN discount_amount DECIMAL(10,2);

-- BREAKING: Renaming a column (all downstream queries break)
-- NEVER do this without a migration plan
ALTER TABLE orders RENAME COLUMN total TO total_amount;
```

## Key Interview Takeaways

- Data pipeline versioning is harder than application versioning because bad data outlives the bad code
- Git tagging with semantic versioning is the foundation — MAJOR for breaking changes, MINOR for additions, PATCH for fixes
- dbt's code-based approach makes it naturally well-suited to Git versioning
- Airflow DAG versioning often uses `dag_id` versioning (`orders_etl_v1`, `orders_etl_v2`) to manage transitions
- Idempotency in pipeline tasks is required for safe reruns — design every task to produce the same output given the same input
- Backward compatibility vs. breaking change is the central design decision in schema changes
