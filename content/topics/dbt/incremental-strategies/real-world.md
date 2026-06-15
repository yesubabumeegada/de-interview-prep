---
title: "Incremental Strategies - Real World"
topic: dbt
subtopic: incremental-strategies
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, incremental, production, patterns, late-arriving, backfill]
---

# Incremental Strategies — Real World Patterns

## Pattern 1: E-commerce Order Tracking with Status Updates

**Problem:** Orders table has 200M rows. Orders can transition through statuses (pending → processing → shipped → delivered → returned) for up to 30 days. Source sends full order records on every status change.

```sql
-- models/marts/fct_orders.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    merge_update_columns=['status', 'shipped_at', 'delivered_at', 'updated_at'],
    -- Protect immutable fields from incorrect updates
    -- created_at and customer_id will NEVER be overwritten
    incremental_predicates=[
        "dbt_internal_dest.order_date >= dateadd('day', -30, current_date)"
    ]
) }}

select
    order_id,
    customer_id,
    order_date,
    status,
    created_at,
    updated_at,
    shipped_at,
    delivered_at,
    total_amount,
    currency_code
from {{ ref('stg_orders') }}

{% if is_incremental() %}
  -- 30-day lookback to catch all in-flight orders
  where updated_at >= dateadd('day', -30, current_date)
{% endif %}
```

**Why 30-day lookback matches the predicates:** The predicate limits the MERGE target scan to the same 30-day window. Any order older than 30 days is considered settled and won't be updated.

## Pattern 2: Mobile Event Stream with Late Arrivals

**Problem:** Mobile app events arrive up to 96 hours late. Events have `client_event_time` (when on device) and `server_received_at` (when we received it).

```sql
-- models/incremental/fct_mobile_events.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    incremental_predicates=[
        "dbt_internal_dest.client_event_date >= dateadd('day', -5, current_date)"
    ]
) }}

select
    event_id,
    user_id,
    session_id,
    event_type,
    client_event_time,
    date(client_event_time) as client_event_date,
    server_received_at,
    platform,
    app_version,
    properties
from {{ ref('stg_mobile_events') }}

{% if is_incremental() %}
  -- Filter on server_received_at for processing new batches
  -- but use a 5-day lookback window for late client events
  where server_received_at >= (
    select dateadd('hour', -1, max(server_received_at))
    from {{ this }}
  )
  -- Also catch late-arriving events by client time
  or client_event_time >= dateadd('day', -5, current_date)
{% endif %}
```

## Pattern 3: Daily Aggregated Fact with BigQuery Partitions

**Problem:** A BI dashboard needs `fct_daily_user_activity` to load in <2 seconds. The raw events table is 10TB. You need fresh data by 8 AM daily.

```sql
-- models/marts/fct_daily_user_activity.sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "activity_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["user_segment", "country_code"],
    require_partition_filter=true
) }}

select
    date(event_timestamp) as activity_date,
    user_id,
    user_segment,
    country_code,
    count(distinct session_id) as sessions,
    count(*) as events,
    countif(event_type = 'purchase') as purchases,
    sum(case when event_type = 'purchase' then revenue else 0 end) as revenue
from {{ ref('stg_events') }}

{% if is_incremental() %}
  -- Reprocess last 3 days to catch late arrivals
  where date(event_timestamp) >= date_sub(current_date(), interval 3 day)
{% endif %}

group by 1, 2, 3, 4
```

**Result:** BigQuery replaces 3 partitions atomically. Query costs are controlled via partition filter. BI queries hit clustered data for sub-second scans.

## Pattern 4: Handling Source Replays and Corrections

**Problem:** The data source team sends you a message: "We had a bug in our ETL for the last 2 weeks. All events from Oct 1-15 have incorrect `revenue` values. We've corrected the source."

**Solution: Targeted full refresh using date variables**

```sql
-- models/marts/fct_events.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

select * from {{ ref('stg_events') }}

{% if is_incremental() %}
  {% if var('backfill_start_date', none) is not none %}
    -- Targeted backfill mode: reprocess specific date range
    where event_date >= '{{ var("backfill_start_date") }}'
      and event_date <= '{{ var("backfill_end_date", "9999-12-31") }}'
  {% else %}
    -- Normal incremental run
    where updated_at > (select max(updated_at) from {{ this }})
  {% endif %}
{% endif %}
```

```bash
# Trigger targeted backfill for Oct 1-15
dbt run \
  --select fct_events \
  --vars '{"backfill_start_date": "2024-10-01", "backfill_end_date": "2024-10-15"}'

# Or use microbatch for cleaner syntax:
dbt run \
  --select fct_events \
  --event-time-start 2024-10-01 \
  --event-time-end 2024-10-16
```

## Pattern 5: Incremental Model Testing in CI

**Problem:** You changed the incremental logic. How do you verify correctness without running against production data?

```bash
# Step 1: Full refresh in CI to establish baseline
dbt run --full-refresh --select fct_events --target ci

# Step 2: Run incrementally against the same data
dbt run --select fct_events --target ci

# Step 3: Compare row counts (should be identical)
dbt run-operation check_row_counts --args '{"model_name": "fct_events"}'

# Step 4: Run again (idempotency check)
dbt run --select fct_events --target ci
# Row counts and values should still match the full-refresh result
```

```sql
-- macros/check_row_counts.sql
{% macro check_row_counts(model_name) %}
  {% set full_count %}
    select count(*) from {{ ref(model_name) }}
  {% endset %}
  {{ log("Row count: " ~ run_query(full_count).columns[0][0], info=true) }}
{% endmacro %}
```

## Pattern 6: Multi-Strategy Model (Environment-Dependent)

**Problem:** In development, you want a table materialization (simpler debugging). In production, you want incremental for performance.

```sql
{{ config(
    materialized=('incremental' if target.name == 'prod' else 'table'),
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

select
    event_id,
    user_id,
    event_type,
    event_time
from {{ ref('stg_events') }}

{% if is_incremental() %}
  where event_time > (select max(event_time) from {{ this }})
{% endif %}
```

## Production Pitfalls and Solutions

### Pitfall 1: Max Timestamp Drift

```sql
-- DANGEROUS: if updated_at has nulls, max(updated_at) returns null
-- → is_incremental() WHERE clause evaluates to nothing
-- → On next run, ALL rows are processed!

where updated_at > (select max(updated_at) from {{ this }})

-- SAFE: coalesce to a safe historical fallback
where updated_at > (
  select coalesce(max(updated_at), '2020-01-01'::timestamp)
  from {{ this }}
)
```

### Pitfall 2: Schema Changes Without Full Refresh

If you add a column to the SELECT and `on_schema_change='ignore'`, the new column is silently excluded. New rows in the incremental run won't have the new column populated.

**Always test schema changes with `--full-refresh` first in a dev environment.**

### Pitfall 3: Forgetting `is_incremental()` on Full Refresh

On `--full-refresh`, the table is dropped and recreated. `is_incremental()` returns `false`, so your WHERE clause is skipped. If your incremental filter was wrong, `--full-refresh` will still process all historical data correctly — this is the recovery mechanism.

## Interview Scenarios

**Scenario:** Your incremental model has been running for 6 months. You realize the `unique_key` was set incorrectly (using `event_id` but events can have duplicate `event_id`s across shards). You now need `(shard_id, event_id)` as the unique key. How do you fix this?

**Answer:** 
1. Update the `config` block: `unique_key=['shard_id', 'event_id']`
2. **Must run `--full-refresh`** — changing `unique_key` doesn't automatically fix existing duplicates in the target table
3. After full refresh, verify: `select count(*), count(distinct shard_id || event_id) from fct_events` — should be equal
4. Update CI tests to assert uniqueness on the composite key

**Scenario:** Stakeholders report that yesterday's revenue numbers changed overnight. Investigation shows your `fct_orders` incremental model updated historical order amounts. How do you prevent this?

**Answer:** Use `merge_update_columns` to explicitly list which columns can be updated. Exclude `total_amount` if orders shouldn't have their amounts corrected after the fact, or add an explicit guard:

```sql
merge_update_columns=['status', 'shipped_at', 'updated_at']
-- total_amount is not in this list → can never be overwritten
```

If amounts legitimately need correction, add a `correction_run` flag and only allow amount updates when that flag is set via a dbt variable.
