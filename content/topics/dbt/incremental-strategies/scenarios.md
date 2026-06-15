---
title: "Incremental Strategies - Scenario Questions"
topic: dbt
subtopic: incremental-strategies
content_type: scenario_question
tags: [dbt, incremental, scenarios, interview]
---

# Incremental Strategies — Scenario Questions

<article data-difficulty="junior">

## Scenario: First Incremental Model

You're building a dbt model for a `page_views` table that receives 10 million new rows per day. Currently the model is materialized as a `table` and rebuilds all 3 billion rows nightly — taking 2 hours. Convert it to incremental.

**The source table has columns: `view_id` (unique), `page_url`, `user_id`, `viewed_at`, `session_id`**

<details>
<summary>✅ Solution</summary>

```sql
-- models/incremental/fct_page_views.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='view_id',
    on_schema_change='append_new_columns'
) }}

select
    view_id,
    page_url,
    user_id,
    viewed_at,
    session_id
from {{ source('web', 'page_views') }}

{% if is_incremental() %}
  -- Only process new events since last run
  where viewed_at > (
    select coalesce(max(viewed_at), '2020-01-01'::timestamp)
    from {{ this }}
  )
{% endif %}
```

**Key decisions explained:**

1. **`materialized='incremental'`** — processes only new rows after first run
2. **`incremental_strategy='merge'`** — handles the case where a `view_id` might be resent (deduplicates)
3. **`unique_key='view_id'`** — tells dbt how to identify existing rows to update/replace
4. **`on_schema_change='append_new_columns'`** — safe default; won't break if a column is added later
5. **`coalesce(..., '2020-01-01')`** — prevents null issues when the table is empty or `viewed_at` has nulls

**On first run:**
- `is_incremental()` = false
- Full scan of `page_views` source

**On subsequent runs:**
- `is_incremental()` = true
- Only rows with `viewed_at` > max timestamp are processed
- MERGE upserts into the existing table

**Expected result:** Runtime drops from 2 hours to ~5 minutes for the daily incremental run.

**Run a full rebuild when needed:**
```bash
dbt run --full-refresh --select fct_page_views
```

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Late-Arriving Data Breaking Metrics

Your `fct_session_metrics` incremental model uses the standard `updated_at > max(updated_at)` filter. Business stakeholders notice that weekly cohort reports are inconsistent — numbers change day over day for the same cohort week. Investigation shows mobile sessions are arriving 48-72 hours late.

**Current model:**

```sql
{{ config(materialized='incremental', unique_key='session_id') }}

select session_id, user_id, started_at, ended_at, event_count, revenue
from {{ source('mobile', 'sessions') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**How do you fix this while keeping the model performant?**

<details>
<summary>✅ Solution</summary>

**Root cause:** Sessions arriving 48-72 hours late have `updated_at` values in the past. Your filter `updated_at > max(updated_at)` misses them because `max(updated_at)` advances past their timestamp.

**Fix: Dual-filter with lookback window**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='session_id',
    incremental_predicates=[
        -- Limit target table scan to recent sessions only
        "dbt_internal_dest.started_at >= dateadd('day', -4, current_date)"
    ]
) }}

select
    session_id,
    user_id,
    started_at,
    ended_at,
    event_count,
    revenue,
    updated_at
from {{ source('mobile', 'sessions') }}

{% if is_incremental() %}
  -- Process sessions that:
  -- 1. Were recently updated (new data from source)
  -- 2. OR started in the last 4 days (catches late arrivals)
  where updated_at > (
    select dateadd('hour', -1, max(updated_at)) from {{ this }}
  )
  or started_at >= dateadd('day', -4, current_date)
{% endif %}
```

**Why 4 days?** 72 hours late + 1 day buffer = 4 days. Adjust based on your actual SLA.

**The `incremental_predicates` optimization:** Without it, the MERGE would scan all 3 billion target rows to find matches. The predicate `dbt_internal_dest.started_at >= dateadd('day', -4, current_date)` limits the scan to only the last 4 days of data — matching the source filter.

**Trade-off acknowledged:**
- More data processed per run (4 days vs. only new data)
- But eliminates inconsistent cohort metrics, which is the business requirement

**Monitoring:** Add a test to detect unexpected staleness:

```yaml
models:
  - name: fct_session_metrics
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: started_at
          interval: 6
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Designing Incremental Architecture for a Complex Pipeline

You're joining a company as a senior DE. Their largest fact table, `fct_transactions` (500M rows, growing 2M/day), has these characteristics:
- Transactions can be updated for up to 14 days (chargebacks, refunds, dispute resolution)
- Source sends full transaction records on every update
- The MERGE currently takes 4 hours — blocking downstream models
- Two columns (`original_amount`, `created_at`) should NEVER be updated after first insert
- Historical data (>14 days old) is never updated

**Design the complete solution. Consider all performance optimizations.**

<details>
<summary>✅ Solution</summary>

**Full solution:**

```sql
-- models/marts/fct_transactions.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id',

    -- Only update mutable columns; protect immutable ones
    merge_update_columns=[
        'status',
        'settled_amount',
        'chargeback_amount',
        'refund_amount',
        'dispute_status',
        'updated_at',
        'settled_at',
        'closed_at'
    ],

    -- Limit target table scan to 14-day window
    -- (historical data is never updated, so no need to scan it)
    incremental_predicates=[
        "dbt_internal_dest.transaction_date >= dateadd('day', -14, current_date)"
    ],

    -- Snowflake clustering on the most-queried columns
    cluster_by=['transaction_date', 'merchant_id'],

    -- Schema safety
    on_schema_change='append_new_columns'
) }}

select
    transaction_id,
    merchant_id,
    customer_id,
    transaction_date,
    original_amount,      -- immutable: not in merge_update_columns
    settled_amount,
    chargeback_amount,
    refund_amount,
    currency_code,
    status,
    dispute_status,
    created_at,           -- immutable: not in merge_update_columns
    updated_at,
    settled_at,
    closed_at
from {{ ref('stg_transactions') }}

{% if is_incremental() %}
  -- Source filter matches the predicate window exactly
  where updated_at >= dateadd('day', -14, current_date)
{% endif %}
```

**Performance analysis:**

| Optimization | Impact |
|---|---|
| `incremental_predicates` | MERGE scans only 14 days of target data (~28M rows) instead of 500M |
| `merge_update_columns` | Reduces DML width; less data written per matched row |
| `cluster_by=['transaction_date']` | Predicate aligns with cluster key → only relevant micro-partitions scanned |
| 14-day window matches business SLA | No wasted processing of settled transactions |

**Expected result:** Runtime drops from 4 hours to ~15-30 minutes.

**Additional architectural recommendations:**

1. **Separate immutability enforcement with a singular test:**
```sql
-- tests/fct_transactions_immutability.sql
-- Verify created_at never changed from first insert
select t.transaction_id
from {{ ref('fct_transactions') }} t
join {{ source('transactions', 'raw') }} r using (transaction_id)
where t.created_at != r.original_created_at  -- comparing against a gold standard
```

2. **Monitor the lookback window:**
```yaml
- name: fct_transactions
  tests:
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: 490000000
        max_value: 600000000
```

3. **Full refresh strategy:** Schedule monthly `--full-refresh` during low-traffic hours to rebase and reclaim storage from updated micro-partitions.

4. **Contract enforcement:** Add a dbt model contract so column types and not-null constraints are compile-time validated — prevents schema drift from breaking the merge:

```yaml
models:
  - name: fct_transactions
    config:
      contract:
        enforced: true
    columns:
      - name: transaction_id
        data_type: varchar
        constraints:
          - type: not_null
      - name: original_amount
        data_type: numeric
        constraints:
          - type: not_null
```

</details>
</article>
