---
title: "dbt Models & Materializations - Scenarios"
topic: dbt
subtopic: models-and-materializations
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, materialization, interview, scenarios]
---

# dbt Models & Materializations — Scenario Questions

## Scenario 1 (Junior): Choose the Right Materialization

**Situation:** You have these models to build. Which materialization do you pick for each?

| Model | Description | Your Choice |
|---|---|---|
| `stg_events` | 1B-row source table, read-only cleanup | ? |
| `dim_products` | 50K products, rarely changes | ? |
| `fct_orders` | 500M rows, 100K new rows/day | ? |
| `int_order_enriched` | Intermediate join, used by 2 models | ? |
| `rpt_sales_today` | Live dashboard, needs real-time | ? |

**Answer:**

| Model | Materialization | Reason |
|---|---|---|
| `stg_events` | `view` | No storage cost, just renaming/typing |
| `dim_products` | `table` | Small, infrequently changed, fast reads |
| `fct_orders` | `incremental` | Too large for full rebuild daily |
| `int_order_enriched` | `ephemeral` or `table` | Ephemeral if simple, table if expensive join |
| `rpt_sales_today` | `materialized_view` | Auto-refreshed, always current |

---

## Scenario 2 (Mid-Level): Incremental Model Breaking

**Situation:** Your incremental model `fct_events` has been running for 6 months. You added a new column `device_type` to the source last week. Now `dbt run --select fct_events` silently drops the new column from the output. Why?

**Answer:**

The default `on_schema_change='ignore'` means dbt **ignores new columns** and only inserts the columns that existed when the incremental table was first created.

**Fix options:**

Option 1 — Change `on_schema_change`:
```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns'  -- auto-adds new columns
) }}
```

Option 2 — Full refresh once:
```bash
dbt run --full-refresh --select fct_events
# Rebuilds the entire table with the new schema
```

Option 3 — Manual ALTER + incremental going forward:
```sql
-- Run manually in warehouse
ALTER TABLE fct_events ADD COLUMN device_type VARCHAR;
-- Then normal incremental run picks it up
```

**Best practice:** Always set `on_schema_change='sync_all_columns'` for evolving models.

---

## Scenario 3 (Senior): Duplicate Rows in Incremental Table

**Situation:** You run `SELECT COUNT(*), COUNT(DISTINCT order_id) FROM fct_orders` and find 10% more rows than distinct `order_id` values — meaning ~1M duplicate rows. The model uses `incremental_strategy='append'`. How do you fix this without a full rebuild that would take 4 hours?

**Answer:**

**Root cause:** `append` strategy doesn't deduplicate — reruns (e.g., from failures or manual reruns) append rows again.

**Immediate fix — deduplicate in place:**
```sql
-- Run directly in warehouse to clean duplicates
CREATE OR REPLACE TABLE fct_orders AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM fct_orders
)
WHERE rn = 1;
```

**Permanent fix — switch strategy:**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'  -- was: append
) }}
```

**Prevention:** Use `merge` for any data where reruns are possible. Only use `append` for truly immutable, guaranteed-once delivery (e.g., from a Kafka consumer with exactly-once semantics).

**Add a test to catch duplicates early:**
```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique     # Fails fast if duplicates sneak in
          - not_null
```
