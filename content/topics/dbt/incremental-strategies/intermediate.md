---
title: "Incremental Strategies - Intermediate"
topic: dbt
subtopic: incremental-strategies
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, incremental, merge, late-arriving, unique_key, incremental_predicates]
---

# Incremental Strategies — Intermediate

## Merge Strategy: `merge_update_columns`

By default, `merge` updates ALL columns for matching rows. Use `merge_update_columns` to update only specific columns:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    merge_update_columns=['status', 'updated_at', 'shipped_at']
    -- Columns NOT listed (e.g., created_at, customer_id) will NEVER be overwritten
) }}

select
    order_id,
    customer_id,
    status,
    created_at,
    updated_at,
    shipped_at
from {{ source('orders', 'raw_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**Use case:** Protect immutable fields (e.g., `created_at`, `original_price`) from being overwritten if source sends incorrect historical values.

## Compound `unique_key`

When a single column doesn't uniquely identify a row:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'line_item_id']  -- compound key
) }}

select
    order_id,
    line_item_id,
    product_id,
    quantity,
    unit_price,
    updated_at
from {{ source('orders', 'raw_order_lines') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

dbt generates: `MERGE ... ON target.order_id = source.order_id AND target.line_item_id = source.line_item_id`

## Incremental Predicates (Performance Optimization)

Without predicates, the MERGE statement scans the entire target table to find matches. For billion-row tables, this is expensive.

**Incremental predicates** add a `WHERE` clause to the target table scan:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    incremental_predicates=[
        "dbt_internal_dest.event_date >= dateadd('day', -7, current_date)"
    ]
) }}

select
    event_id,
    event_date,
    user_id,
    event_type
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where event_date >= dateadd('day', -7, current_date)
{% endif %}
```

**What this generates:**

```sql
MERGE INTO target t
USING source s ON t.event_id = s.event_id
  AND t.event_date >= dateadd('day', -7, current_date)  -- predicate limits scan!
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

The predicate prevents a full table scan on the target — only recent partitions are scanned.

**Caution:** If a row's `event_date` is outside the predicate window but the row changes, the change is silently missed. Design predicates conservatively.

## Late-Arriving Data Patterns

**Problem:** Events arrive up to 72 hours late due to mobile clients syncing when reconnected.

**Solution: Lookback window pattern**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

{% set lookback_days = 3 %}

select
    event_id,
    user_id,
    event_type,
    event_time,
    server_received_at
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  -- Process last 3 days to catch late arrivals
  where server_received_at >= dateadd('day', -{{ lookback_days }}, current_date)
{% endif %}
```

**Trade-off:** Larger lookback = more data processed = slower runs. Tune based on SLA.

**Alternative: Use `server_received_at` as the filter, not `event_time`**

```sql
{% if is_incremental() %}
  -- Filter on when WE received it, not when it happened
  where server_received_at > (select max(server_received_at) from {{ this }})
{% endif %}
```

This is simpler but misses events that arrive with old `server_received_at` due to bugs.

## Snowflake-Specific: `merge` with Clustering

For large Snowflake tables, combine incremental merge with automatic clustering:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    cluster_by=['event_date', 'user_id'],
    incremental_predicates=[
        "dbt_internal_dest.event_date >= dateadd('day', -3, current_date)"
    ]
) }}
```

Snowflake automatically clusters new micro-partitions. The `incremental_predicates` ensure the MERGE only touches relevant clusters.

## BigQuery-Specific: `insert_overwrite` with Date Partitions

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["user_id", "event_type"],
    require_partition_filter=true
) }}

select
    date(event_timestamp) as event_date,
    user_id,
    event_type,
    count(*) as event_count
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where date(event_timestamp) >= date_sub(current_date(), interval 3 day)
{% endif %}
group by 1, 2, 3
```

BigQuery replaces the entire affected partition(s) atomically. No MERGE overhead.

## Databricks/Spark: `insert_overwrite`

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='delta',
    partition_by=['event_year', 'event_month']
) }}

select
    year(event_time) as event_year,
    month(event_time) as event_month,
    user_id,
    event_type,
    event_time
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where event_time >= date_trunc('month', current_date())
{% endif %}
```

## `on_schema_change` Deep Dive

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}
```

**Scenario — you add a new column `discount_amount`:**

| `on_schema_change` | Result |
|---|---|
| `ignore` | `discount_amount` never added to the table. Data is lost. |
| `fail` | dbt errors: "Schema mismatch detected." Build stops. |
| `append_new_columns` | `discount_amount` column added; existing rows get `null`. Safe. |
| `sync_all_columns` | Adds `discount_amount`; removes any columns in table but not in model. Destructive. |

**Recommendation:** Use `append_new_columns` in production. Use `fail` for strict data contracts. Avoid `sync_all_columns` unless you're very sure.

## Idempotency and the `--full-refresh` Contract

An incremental model should be **idempotent** — running it multiple times produces the same result as running it once.

```sql
-- Good: idempotent with merge + unique_key
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

-- Bad: not idempotent with append + no deduplication
{{ config(
    materialized='incremental',
    incremental_strategy='append'
    -- No unique_key -- duplicate rows on rerun!
) }}
```

Test idempotency: run the model twice without `--full-refresh`, then compare row counts.

## Interview Questions

**Q: What are incremental predicates and why do they matter?**
A: Predicates added to the `MERGE` target table scan, limiting how much of the target table is scanned when finding matches. Without them, every merge scans the full table, which is expensive for billion-row tables. They're a key performance optimization.

**Q: How do you handle late-arriving data in incremental models?**
A: Use a lookback window — filter on the last N days (e.g., 3 days) rather than just "newer than max timestamp." Use `merge` or `delete+insert` with `unique_key` so late-arriving rows update existing records rather than creating duplicates.

**Q: When would you use `merge_update_columns`?**
A: When you want to protect certain columns from being overwritten. For example, `created_at` should never change once set — if the source sends a wrong value in an update, `merge_update_columns` prevents it from clobbering the original.

**Q: What is the danger of `on_schema_change='sync_all_columns'`?**
A: It drops columns from the target table that no longer appear in the model's SELECT. If a column was intentionally kept for historical purposes or is referenced by downstream queries, this silently deletes the data.
