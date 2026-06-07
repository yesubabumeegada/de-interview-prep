---
title: "dbt Models & Materializations - Intermediate"
topic: dbt
subtopic: models-and-materializations
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, incremental-strategies, materialized-views, merge, insert-overwrite]
---

# dbt Models & Materializations — Intermediate

## Incremental Strategies

dbt supports multiple strategies for how incremental models handle new data:

### append (fastest, no dedup)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
-- Simply inserts new rows, no deduplication
-- Use for immutable event streams
```

### merge (default for most adapters)

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}
-- MERGE (upsert): update existing rows, insert new ones
-- Requires unique_key
```

### delete+insert

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}
-- Delete matching rows, then insert new batch
-- Atomic, good for Redshift
```

### insert_overwrite (partition-based)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    }
) }}
-- Replaces entire partitions
-- Most efficient for BigQuery/Spark with date partitioning
```

### microbatch (dbt 1.9+)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_date',
    begin='2020-01-01',
    batch_size='day'
) }}
-- Automatically processes one day at a time
-- Supports parallel batch processing
-- Ideal for large historical backfills
```

## Materialized Views

Available in Snowflake, BigQuery, Postgres (dbt 1.6+):

```sql
{{ config(materialized='materialized_view') }}

SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    customer_id,
    SUM(total_amount) AS monthly_spend
FROM {{ ref('fct_orders') }}
GROUP BY 1, 2
```

Benefits vs regular views:
- Pre-computed results stored physically
- Auto-refreshed by the warehouse (Snowflake incremental refresh)
- Much faster query performance
- No dbt run needed after initial build

## on_schema_change Behavior

Controls what happens when your incremental model adds/removes columns:

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'  -- recommended
) }}
```

| Value | Behavior |
|---|---|
| `ignore` (default) | New columns silently dropped |
| `fail` | Build fails if schema changes |
| `append_new_columns` | Adds new columns, ignores removed |
| `sync_all_columns` | Adds new, drops removed columns |

## Advanced Incremental Patterns

### Late-Arriving Data Lookback

```sql
{{ config(materialized='incremental', unique_key='event_id') }}

SELECT event_id, user_id, event_type, event_ts
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
-- Look back 3 days to catch late-arriving events
WHERE event_ts >= (
    SELECT DATEADD('day', -3, MAX(event_ts))
    FROM {{ this }}
)
{% endif %}
```

### Conditional Full Refresh

```sql
{% if var('full_refresh', false) or is_incremental() == false %}
    -- Full load
    SELECT * FROM {{ source('raw', 'orders') }}
{% else %}
    -- Incremental
    SELECT * FROM {{ source('raw', 'orders') }}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

Force full refresh: `dbt run --full-refresh --select fct_orders`

## Custom Materializations

For advanced use cases, define your own materialization:

```sql
-- macros/materializations/clone_table.sql
{% materialization clone_table, adapter='snowflake' %}
  {%- set target_relation = this -%}
  {%- set source_relation = ref(config.get('source_model')) -%}

  {% call statement('main') %}
    CREATE OR REPLACE TABLE {{ target_relation }}
    CLONE {{ source_relation }}
  {% endcall %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
```

Usage:
```sql
{{ config(
    materialized='clone_table',
    source_model='fct_orders_prod'
) }}
```

## Adapter-Specific Configurations

### Snowflake

```sql
{{ config(
    materialized='table',
    transient=true,           -- No Fail-safe storage (cheaper)
    cluster_by=['customer_id'],
    automatic_clustering=false,
    copy_grants=true,
    snowflake_warehouse='LARGE_WH'
) }}
```

### BigQuery

```sql
{{ config(
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'event_type'],
    partition_expiration_days=365,
    require_partition_filter=true
) }}
```

### Redshift

```sql
{{ config(
    materialized='table',
    sort=['customer_id', 'order_date'],
    dist='customer_id',
    sort_type='compound'
) }}
```
