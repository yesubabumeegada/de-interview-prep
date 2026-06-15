---
title: "Incremental Strategies - Fundamentals"
topic: dbt
subtopic: incremental-strategies
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, incremental, materialization, is_incremental, unique_key]
---

# Incremental Strategies — Fundamentals

## What Is an Incremental Model?

An incremental model only processes **new or changed rows** on each run, rather than rebuilding the entire table from scratch. This dramatically reduces compute cost and run time for large datasets.

```sql
-- Without incremental: rebuilds 1 billion rows every run
select * from {{ source('events', 'raw_events') }}

-- With incremental: only processes today's new rows
{{ config(materialized='incremental') }}

select * from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where event_time > (select max(event_time) from {{ this }})
{% endif %}
```

`{{ this }}` — a Jinja variable that refers to the current model's table in the warehouse.

`is_incremental()` — a macro that returns `true` when the model is being run incrementally (table already exists), `false` on first run or `--full-refresh`.

## The Four Core Incremental Strategies

### 1. `append` (simplest)

Just inserts new rows. Never updates or deletes.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select * from {{ source('logs', 'app_logs') }}
{% if is_incremental() %}
  where log_time > (select max(log_time) from {{ this }})
{% endif %}
```

**Use when:** Data is truly append-only (event logs, audit trails). Fastest strategy.
**Risk:** If source data is reprocessed, you get duplicate rows.

### 2. `delete+insert` (default for some adapters)

Deletes matching rows, then inserts new ones. Controlled by `unique_key`.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_id'
) }}

select
    event_id,
    user_id,
    event_type,
    event_time
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where event_time >= dateadd('day', -3, current_date)  -- 3-day lookback
{% endif %}
```

**Use when:** Source data may be updated within a lookback window. Handles late arrivals.
**How it works:** dbt deletes rows in the target where `event_id` matches the incoming batch, then inserts all incoming rows.

### 3. `merge` (most common for Snowflake/BigQuery)

SQL MERGE statement — insert, update, or delete in one operation.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    status,
    updated_at,
    total_amount
from {{ source('orders', 'raw_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**Use when:** Source records can be created or updated (most OLTP → DW scenarios).
**What happens:** Rows with matching `unique_key` are updated; new rows are inserted.

### 4. `insert_overwrite` (BigQuery, Spark, Databricks)

Overwrites specific partitions rather than the entire table.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    }
) }}

select
    date(event_time) as event_date,
    user_id,
    event_type,
    count(*) as event_count
from {{ source('events', 'raw_events') }}
{% if is_incremental() %}
  where date(event_time) >= date_sub(current_date(), interval 3 day)
{% endif %}
group by 1, 2, 3
```

**Use when:** Data is naturally partitioned (by day/week/month). The entire affected partition is replaced atomically.

## First Run vs. Incremental Run

```
First Run (table doesn't exist yet):
  → is_incremental() = false
  → WHERE clause is skipped
  → Full table is built from scratch

Subsequent Runs (table exists):
  → is_incremental() = true
  → WHERE clause filters to recent data
  → Strategy determines how rows are merged

--full-refresh flag:
  → Forces a full rebuild even if table exists
  → is_incremental() = false
  → Drops and recreates the table
```

## `on_schema_change` Options

What happens when you add/remove/rename columns in your model?

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'  -- most permissive default in practice
) }}
```

| Option | Behavior |
|---|---|
| `ignore` (default) | Schema changes are silently ignored; new columns are not added to the table |
| `fail` | dbt errors if the schema doesn't match exactly |
| `append_new_columns` | New columns are added; removed columns are kept (no data loss) |
| `sync_all_columns` | Adds new columns, removes dropped columns (risky!) |

## When to Use Each Strategy

| Situation | Strategy |
|---|---|
| Append-only event stream | `append` |
| Orders that can be updated | `merge` |
| Daily partitioned aggregates | `insert_overwrite` |
| Source replays/corrections | `delete+insert` with lookback |

## Full Refresh

```bash
# Force a complete rebuild
dbt run --full-refresh --select my_incremental_model

# Full refresh all models
dbt run --full-refresh
```

Use `--full-refresh` when:
- Schema changes require a rebuild
- Historical data was reprocessed in the source
- `on_schema_change` alone won't fix the state

## Key Interview Points

**Q: What does `{{ this }}` mean in dbt?**
A: It refers to the current model's relation (database.schema.table) in the warehouse. Used in incremental models to query the existing table for the max timestamp.

**Q: What is `is_incremental()`?**
A: A Jinja macro that returns `true` when the model runs incrementally (table exists, no `--full-refresh`). Used to add a `WHERE` clause that filters to only new/changed rows.

**Q: What is the difference between `merge` and `delete+insert`?**
A: Both update existing rows, but `merge` uses a SQL MERGE statement (one operation). `delete+insert` explicitly deletes matching rows first, then inserts — more portable across databases that don't support MERGE natively.

**Q: When would you NOT use an incremental model?**
A: For small tables (< 1M rows), for models requiring a full scan of historical data (e.g., running totals, certain window functions), or when source data can be fully reconstructed cheaply.
