---
title: "Incremental Strategies - Senior Deep Dive"
topic: dbt
subtopic: incremental-strategies
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, incremental, microbatch, performance, architecture, warehouse-specific]
---

# Incremental Strategies — Senior Deep Dive

## Microbatch Strategy (dbt 1.9+)

Microbatch processes data in small, configurable time slices. dbt automatically partitions the job into batches and handles failures at the batch level.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_timestamp',
    begin='2024-01-01',
    batch_size='day',
    lookback=3
) }}

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    properties
from {{ source('events', 'raw_events') }}
-- No manual is_incremental() filter needed!
-- dbt automatically injects the batch filter
```

**How microbatch differs from standard incremental:**

| Aspect | Standard incremental | Microbatch |
|---|---|---|
| Filter injection | Manual (`is_incremental()`) | Automatic |
| Failure recovery | Re-run whole model | Re-run failed batches only |
| Backfill | `--full-refresh` | `--event-time-start` / `--event-time-end` |
| Parallelism | Single run | Batches run in sequence (parallel planned) |

**Targeted backfill:**

```bash
# Reprocess only October 2024
dbt run \
  --select my_microbatch_model \
  --event-time-start 2024-10-01 \
  --event-time-end 2024-11-01
```

## Warehouse-Specific Strategy Details

### Snowflake: `merge` Performance Anatomy

The generated Snowflake MERGE SQL:

```sql
-- What dbt generates for merge with incremental_predicates:
MERGE INTO analytics.dbt_prod.fct_events AS dbt_internal_dest
USING (
    -- Your model's SELECT
    SELECT event_id, user_id, event_type, event_date
    FROM analytics.raw.events
    WHERE event_date >= DATEADD('day', -3, CURRENT_DATE)
) AS dbt_internal_source
ON dbt_internal_dest.event_id = dbt_internal_source.event_id
   AND dbt_internal_dest.event_date >= DATEADD('day', -3, CURRENT_DATE)  -- predicate
WHEN MATCHED THEN UPDATE SET
    user_id = dbt_internal_source.user_id,
    event_type = dbt_internal_source.event_type,
    event_date = dbt_internal_source.event_date
WHEN NOT MATCHED THEN INSERT (event_id, user_id, event_type, event_date)
VALUES (dbt_internal_source.event_id, ...)
```

**Snowflake merge best practices:**
- Use `incremental_predicates` to limit target table scan
- Enable automatic clustering on the `unique_key` column
- Use warehouse caching: run incremental models on consistent warehouse names for micro-partition cache hits
- Use `merge_update_columns` to reduce DML width

### BigQuery: Partition-Level Operations

BigQuery's `insert_overwrite` is highly efficient because it replaces entire partitions atomically via storage API, not row-level DML:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["user_id"],
    partitions=[
        "date_sub(current_date(), interval 0 day)",
        "date_sub(current_date(), interval 1 day)",
        "date_sub(current_date(), interval 2 day)"
    ]
) }}
```

For BigQuery `merge` (when updates are needed):

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_id', 'event_date'],
    merge_update_columns=['event_count', 'total_spend', 'updated_at']
) }}
```

**BigQuery merge costs:** Charged per bytes scanned. Use `require_partition_filter=true` to prevent accidental full scans.

### Databricks Delta Lake: `merge` with ZORDER

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    file_format='delta',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY (user_id, event_date)"
    ]
) }}
```

Delta Lake's MERGE is ACID-compliant and efficient for concurrent writers. ZORDER after merge colocates related data for faster scans.

### Redshift: `delete+insert`

Redshift doesn't support MERGE natively (pre-2023). Use `delete+insert`:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    dist='order_id',        -- distribution key
    sort=['order_date']     -- sort key
) }}
```

Modern Redshift (RA3) does support MERGE, but `delete+insert` remains more portable.

## Advanced Pattern: Conditional Strategy by Environment

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge' if target.type == 'snowflake' else 'delete+insert',
    unique_key='event_id'
) }}
```

Or use adapter dispatch:

```yaml
# dbt_project.yml
models:
  my_project:
    fct_events:
      +incremental_strategy: "{{ 'merge' if target.type in ['snowflake', 'bigquery'] else 'delete+insert' }}"
```

## Testing Incremental Models

### State Comparison Test

```bash
# 1. Build the model normally
dbt run --select fct_events

# 2. Re-run (incremental)
dbt run --select fct_events

# 3. Compare full refresh vs incremental result
dbt run --full-refresh --select fct_events_full_refresh
dbt run-operation audit_helper.compare_relations --args '{
  "a_relation": {"identifier": "fct_events_full_refresh"},
  "b_relation": {"identifier": "fct_events"},
  "primary_key": "event_id"
}'
```

Any differences indicate a bug in your incremental logic.

### Row Count Monitoring

```yaml
models:
  - name: fct_events
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: event_time
          interval: 2   # Fail if no events in last 2 hours
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 100000
          max_value: 50000000
```

## Architecture Decision: When Incremental Breaks Down

### Signs You Need a Different Approach

1. **Window functions over all historical data:** Cannot be incrementalized without recomputing full history.

```sql
-- This CANNOT be incrementalized:
select
    user_id,
    event_time,
    sum(revenue) over (partition by user_id order by event_time) as cumulative_revenue
from events
-- Cumulative sum requires all historical rows
```

**Solution:** Materialize the base fact as incremental, then compute cumulative sums in a downstream table-materialized model.

2. **SCD Type 2 (history tables):** Use dbt snapshots instead of incremental models.

3. **Aggregation fact tables:** Use `insert_overwrite` on date partitions — recompute recent partitions entirely rather than merging aggregated rows.

### The Two-Step Incremental Pattern

```sql
-- Step 1: Raw incremental (fast, append-only)
-- models/incremental/stg_events_raw.sql
{{ config(materialized='incremental', incremental_strategy='append') }}
select * from {{ source('events', 'raw') }}
{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}

-- Step 2: Deduplication model (table, reads from incremental)
-- models/marts/fct_events.sql
{{ config(materialized='table') }}  -- or insert_overwrite by partition
select distinct on (event_id) *
from {{ ref('stg_events_raw') }}
order by event_id, ingested_at desc
```

This separates concerns: fast ingestion vs. clean deduplication.

## Interview Questions for Seniors

**Q: Explain incremental predicates and describe a scenario where misconfiguring them causes silent data quality issues.**
A: Predicates add a filter to the MERGE target scan. Misconfiguration example: `incremental_predicates = ["dbt_internal_dest.event_date >= '2024-01-01'"]` — a hardcoded date. As time passes, events before 2024-01-01 can never be updated even if source data corrects them. The model silently stops fixing historical errors. Predicates should use relative dates (`current_date - interval 7 days`).

**Q: How does dbt's microbatch strategy differ from standard incremental, and when would you choose it?**
A: Microbatch auto-injects date filters based on `event_time` and `batch_size`, processes one batch at a time, and allows targeted reprocessing of specific time ranges without `--full-refresh`. Choose it when you need fine-grained failure recovery, targeted backfills, or when your pipeline processes data in naturally time-ordered batches. Standard incremental is simpler for straightforward use cases.

**Q: How do you handle a scenario where source data can be reprocessed (replayed) and you need the incremental model to reflect the replay?**
A: Use `delete+insert` or `merge` with a large enough lookback window to cover the replay window. If the full table can be replayed, run `--full-refresh`. For partial replays (e.g., last 7 days), use `incremental_predicates` to define the affected window and ensure your source filter covers the same window. For microbatch, use `--event-time-start / --event-time-end` to reprocess specific batches.

**Q: A senior engineer asks why your incremental model is running MERGE against 500GB of data even though only 1GB of new data arrived. How do you diagnose and fix?**
A: The MERGE is scanning the full target table without a predicate. Add `incremental_predicates` to limit the target scan to recent data. Also check: (1) is the `unique_key` indexed/clustered? (2) Is the source filter correctly limiting incoming data? (3) Is Snowflake's query profile showing a full micro-partition scan? Fix: add predicates, ensure clustering keys match the predicate column.
