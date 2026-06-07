---
title: "dbt Seeds & Snapshots - Intermediate"
topic: dbt
subtopic: seeds-and-snapshots
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, snapshots, invalidate-hard-deletes, snapshot-patterns]
---

# dbt Seeds & Snapshots — Intermediate

## invalidate_hard_deletes

Handle records deleted from the source:

```sql
{% snapshot snap_customers %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True   -- set dbt_valid_to when row disappears from source
) }}

SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```

Without this option, deleted source rows remain in the snapshot as "current" (`dbt_valid_to IS NULL`) indefinitely.

## Snapshot on Snapshot (Derived Snapshots)

Reference a snapshot in a model to compute SCD2 dimensions:

```sql
-- models/marts/dim_customers.sql
WITH customer_history AS (
    SELECT
        customer_id,
        email,
        tier,
        dbt_valid_from,
        dbt_valid_to,
        dbt_valid_to IS NULL AS is_current
    FROM {{ ref('snap_customers') }}
)

SELECT * FROM customer_history
```

## Snapshot with Hard Delete Detection Model

```sql
-- models/marts/dim_customers_current.sql
SELECT
    s.customer_id,
    s.email,
    s.tier,
    s.dbt_valid_from AS effective_from,
    CASE WHEN s.dbt_valid_to IS NULL AND src.customer_id IS NULL
         THEN CURRENT_TIMESTAMP()
         ELSE s.dbt_valid_to
    END AS effective_to,
    s.dbt_valid_to IS NULL AND src.customer_id IS NOT NULL AS is_active
FROM {{ ref('snap_customers') }} s
LEFT JOIN {{ source('raw', 'customers') }} src
    USING (customer_id)
WHERE s.dbt_valid_to IS NULL
```

## Advanced Seed Patterns

### Seed with Metadata

```yaml
# models/staging/schema.yml
models:
  - name: country_codes
    description: "ISO 3166-1 country code mapping — updated annually"
    meta:
      last_updated: "2024-01-15"
      update_frequency: annual
      owner: "@data-governance"
    columns:
      - name: country_code
        description: "ISO 3166-1 alpha-2 code"
        tests: [unique, not_null]
      - name: region
        description: "Geographic region grouping"
        tests:
          - accepted_values:
              values: ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'EMEA']
```

### Environment-Specific Seeds

```yaml
# dbt_project.yml
seeds:
  my_project:
    +schema: |
      {{ 'reference_data' if target.name == 'prod' else target.schema ~ '_reference' }}
```

### Large Seed Performance

For seeds > 10MB, consider alternatives:
1. Load as a source table via your ELT tool instead
2. Use `dbt seed --full-refresh` only on schema changes, not daily
3. Split into multiple smaller seed files

## Snapshot Scheduling

Snapshots should run **before** models that depend on them:

```bash
# Correct order
dbt snapshot && dbt run

# Or with build (handles ordering automatically)
dbt build
```

In Airflow:
```python
# dbt_dag.py
snapshot_task = BashOperator(
    task_id='dbt_snapshot',
    bash_command='dbt snapshot --target prod'
)
run_task = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --target prod'
)
snapshot_task >> run_task
```

## Snapshot Best Practices

| Practice | Why |
|---|---|
| Always use `unique_key` on the natural business key | Prevents duplicate history rows |
| Prefer `timestamp` over `check` strategy | 10-50x faster on large tables |
| Keep snapshots in a separate schema | Clear separation from staging/marts |
| Run snapshots more frequently than daily | Miss fewer changes between runs |
| Test `dbt_valid_to IS NULL` row count | Detect unexpected full-table invalidation |
