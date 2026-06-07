---
title: "dbt Fundamentals - Real-World Examples"
topic: dbt
subtopic: dbt-fundamentals
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, production, medallion, real-world, team-patterns]
---

# dbt Fundamentals — Real-World Production Examples

## Example 1: Medallion Architecture with dbt

A common production pattern mapping dbt layers to medallion tiers:

```
Bronze (raw source data)
   ↓  [dbt staging models]
Silver (cleaned, typed, deduplicated)
   ↓  [dbt intermediate models]
Gold (business metrics, aggregates)
   ↓  [dbt mart models]
Platinum (executive KPIs, SLA-governed)
```

### Project Layout

```
models/
├── staging/           # Bronze → Silver
│   ├── _sources.yml
│   ├── stg_orders.sql
│   ├── stg_customers.sql
│   └── stg_products.sql
├── intermediate/      # Silver → Gold joins
│   ├── int_order_items_enriched.sql
│   └── int_customer_lifetime.sql
└── marts/
    ├── core/          # Gold — cross-domain facts/dims
    │   ├── fct_orders.sql
    │   ├── dim_customers.sql
    │   └── dim_products.sql
    └── finance/       # Platinum — KPIs
        ├── rpt_revenue_daily.sql
        └── rpt_cohort_retention.sql
```

## Example 2: Multi-Environment Strategy

A real team uses three environments:

```yaml
# profiles.yml
analytics:
  outputs:
    dev:
      schema: "dbt_{{ env_var('DBT_USER') }}"   # dbt_jsmith
      threads: 4
    ci:
      schema: "ci_{{ env_var('PR_NUMBER') }}"    # ci_1234
      threads: 8
    prod:
      schema: analytics
      threads: 16
```

### Environment-Aware Model

```sql
-- Limit data in dev for speed, full in prod
{{ config(materialized='table') }}

SELECT *
FROM {{ source('raw', 'events') }}

{% if target.name == 'dev' %}
WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'
{% elif target.name == 'ci' %}
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
{% endif %}
```

## Example 3: SCD Type 2 with dbt Snapshots

Tracking customer address changes over time:

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}

SELECT
    customer_id,
    email,
    address_line1,
    city,
    country,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

Resulting table columns:
- `dbt_scd_id` — unique row key
- `dbt_updated_at` — when snapshot captured change
- `dbt_valid_from` — record start date
- `dbt_valid_to` — record end date (NULL = current)

Querying current state:
```sql
SELECT * FROM {{ ref('snap_customers') }}
WHERE dbt_valid_to IS NULL
```

Querying point-in-time:
```sql
SELECT * FROM {{ ref('snap_customers') }}
WHERE '2023-06-01' BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31')
```

## Example 4: Full CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    paths: ['models/**', 'tests/**', 'macros/**']

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: Download prod manifest
        run: |
          aws s3 cp s3://dbt-artifacts/prod/manifest.json ./prod-state/

      - name: dbt deps
        run: dbt deps

      - name: dbt build (changed only)
        run: |
          dbt build \
            --select state:modified+ \
            --state ./prod-state \
            --target ci
        env:
          DBT_USER: ${{ secrets.DBT_CI_USER }}
          DBT_PASSWORD: ${{ secrets.DBT_CI_PASSWORD }}
          PR_NUMBER: ${{ github.event.number }}

      - name: Upload CI manifest
        if: success()
        run: |
          aws s3 cp ./target/manifest.json \
            s3://dbt-artifacts/ci/pr-${{ github.event.number }}/
```

## Example 5: Error Handling and Alerting

```yaml
# dbt_project.yml
on-run-end:
  - "{{ alert_on_failure() }}"
```

```sql
-- macros/alert_on_failure.sql
{% macro alert_on_failure() %}
  {% if results | selectattr('status', 'equalto', 'error') | list | length > 0 %}
    {% set failed_models = results
        | selectattr('status', 'equalto', 'error')
        | map(attribute='node.name')
        | list %}
    {{ log("FAILED MODELS: " ~ failed_models | join(', '), info=True) }}
    -- In production, call a webhook or Slack API here
  {% endif %}
{% endmacro %}
```

## Example 6: Data Freshness Dashboard

Use dbt metadata to build a freshness monitoring view:

```sql
-- models/monitoring/model_run_stats.sql
{{ config(materialized='view') }}

SELECT
    node_id,
    run_started_at,
    execution_time,
    rows_affected,
    status
FROM {{ ref('dbt_run_results') }}  -- from elementary or custom
WHERE run_started_at >= CURRENT_DATE - 7
ORDER BY run_started_at DESC
```

## Production Checklist

| Item | Check |
|---|---|
| All models have tests (unique + not_null on PKs) | ✅ |
| Source freshness checks configured | ✅ |
| Incremental models use `unique_key` | ✅ |
| `profiles.yml` not in version control | ✅ |
| CI only runs changed models | ✅ |
| Prod manifest stored in artifact storage | ✅ |
| Thread count tuned for warehouse size | ✅ |
| Mart models have column-level documentation | ✅ |
| Exposures document downstream consumers | ✅ |
| Model contracts on public models | ✅ |
