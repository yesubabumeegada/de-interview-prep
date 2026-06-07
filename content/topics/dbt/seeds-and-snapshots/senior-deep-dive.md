---
title: "dbt Seeds & Snapshots - Senior Deep Dive"
topic: dbt
subtopic: seeds-and-snapshots
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, snapshots, performance, custom-snapshot-logic]
---

# dbt Seeds & Snapshots — Senior Deep Dive

## Custom Snapshot Strategies

Build a custom strategy for complex change detection:

```sql
-- macros/strategies/custom_snapshot_hash.sql
{% macro custom_snapshot_hash_strategy(node, snapshotted_rel, current_rel, config, target_exists) -%}
    {% set primary_key = config['unique_key'] %}
    {% set hash_columns = config['hash_cols'] %}

    select
        *,
        {{ snapshot_hash_arguments(hash_columns) }} as dbt_scd_id,
        {{ snapshot_get_time() }} as dbt_updated_at,
        {{ snapshot_get_time() }} as dbt_valid_from,
        nullif({{ snapshot_get_time() }}, {{ snapshot_get_time() }}) as dbt_valid_to
    from (
        select current_data.*,
               {{ snapshot_hash_arguments(hash_columns) }} as _dbt_hash
        from {{ current_rel }} current_data
    ) snap
    where (
        select _dbt_hash from {{ snapshotted_rel }}
        where {{ snapshotted_rel }}.{{ primary_key }} = snap.{{ primary_key }}
          and {{ snapshotted_rel }}.dbt_valid_to is null
    ) != snap._dbt_hash
       or (
        select {{ primary_key }} from {{ snapshotted_rel }}
        where {{ snapshotted_rel }}.{{ primary_key }} = snap.{{ primary_key }}
    ) is null
{%- endmacro %}
```

## Snapshot Performance at Scale

For large tables (100M+ rows), standard `check` strategy is too slow:

### Optimized Snapshot with Pre-filter

```sql
{% snapshot snap_large_orders %}
{{ config(
    target_schema='snapshots',
    unique_key='order_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

-- Only snapshot orders modified in last 7 days + new orders
SELECT *
FROM {{ source('raw', 'orders') }}
WHERE updated_at >= CURRENT_DATE - 7
   OR order_id NOT IN (
       SELECT DISTINCT order_id
       FROM {{ this }}
       WHERE dbt_valid_to IS NULL
   )

{% endsnapshot %}
```

### Clustered Snapshot Tables

```sql
-- After snapshot runs, apply clustering (Snowflake)
-- In post_hook:
{{ config(
    post_hook="ALTER TABLE {{ this }} CLUSTER BY (customer_id, dbt_valid_from)"
) }}
```

## Bi-Temporal Modeling with Snapshots

Track both when data changed in the source system AND when dbt recorded it:

```sql
-- models/mart/dim_customers_bitemporal.sql
SELECT
    customer_id,
    email,
    tier,
    -- Transaction time (when it happened in the business)
    business_updated_at AS valid_from_business,
    LEAD(business_updated_at) OVER (
        PARTITION BY customer_id ORDER BY dbt_valid_from
    ) AS valid_to_business,
    -- System time (when dbt captured it)
    dbt_valid_from AS valid_from_system,
    dbt_valid_to AS valid_to_system
FROM {{ ref('snap_customers') }}
ORDER BY customer_id, dbt_valid_from
```

## Seed Version Control Patterns

```yaml
# Git workflow for seeds
# seeds are just CSV files — full git history available
git log --follow seeds/product_categories.csv
git diff HEAD~1 seeds/product_categories.csv
```

For regulated industries, each seed change is auditable:
```
2024-01-15 | Add new region APAC       | jsmith
2024-03-01 | Update EU tax categories  | mjones  
2024-06-01 | Deprecate legacy codes    | admin
```

## Seed Validation Tests

```yaml
# models/reference/schema.yml (or seeds/properties.yml)
seeds:
  - name: country_codes
    description: "ISO 3166-1 alpha-2 country codes"
    columns:
      - name: country_code
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_value_lengths_to_be_between:
              min_value: 2
              max_value: 2
      - name: region
        tests:
          - not_null
          - accepted_values:
              values: ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'EMEA', 'Middle East', 'Africa']
```

## Snapshot Observability

Monitor snapshot health:

```sql
-- models/monitoring/snapshot_health.sql
{{ config(materialized='view') }}

SELECT
    SPLIT_PART(table_name, '_', 2) AS source_entity,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END) AS current_rows,
    SUM(CASE WHEN dbt_valid_to IS NOT NULL THEN 1 ELSE 0 END) AS historical_rows,
    MIN(dbt_valid_from) AS earliest_history,
    MAX(dbt_updated_at) AS last_snapshot_run,
    DATEDIFF('hour', MAX(dbt_updated_at), CURRENT_TIMESTAMP()) AS hours_since_last_run
FROM information_schema.tables t
JOIN LATERAL (
    SELECT * FROM IDENTIFIER(t.table_schema || '.' || t.table_name)
) snap
WHERE t.table_schema = 'SNAPSHOTS'
GROUP BY 1
HAVING hours_since_last_run > 25  -- Alert if snapshot hasn't run in 25 hours
```

## Rebuilding Snapshots

Snapshots are stateful — rebuilding requires care:

```bash
# Scenario: need to rebuild snap_customers from scratch
# 1. Drop the existing snapshot table
dbt run-operation drop_relation --args '{"relation_type": "table", "schema": "snapshots", "identifier": "snap_customers"}'

# 2. Re-run snapshot (creates fresh)
dbt snapshot --select snap_customers

# 3. This loses all history! — always backup first
# Before step 1:
CREATE TABLE snapshots.snap_customers_backup_20240601 AS
SELECT * FROM snapshots.snap_customers;
```
