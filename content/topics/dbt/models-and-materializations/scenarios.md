---
title: "dbt Models & Materializations - Scenarios"
topic: dbt
subtopic: models-and-materializations
content_type: scenario_question
tags: [dbt, materialization, interview, scenarios]
---

# dbt Models & Materializations — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Choosing the Right Materialization

**Scenario:** You have these models to build. Which materialization do you pick for each, and why?

| Model | Description |
|---|---|
| `stg_events` | 1B-row source table, read-only cleanup |
| `dim_products` | 50K products, rarely changes |
| `fct_orders` | 500M rows, 100K new rows/day |
| `int_order_enriched` | Intermediate join, used by 2 models |
| `rpt_sales_today` | Live dashboard, needs real-time |

<details>
<summary>💡 Hint</summary>

Match materialization to the data volume and update pattern: views for thin wrappers, tables for small frequently-read models, incremental for large growing tables, ephemeral for intermediate steps, and materialized views for always-current dashboards.

</details>

<details>
<summary>✅ Solution</summary>

| Model | Materialization | Reason |
|---|---|---|
| `stg_events` | `view` | No storage cost, just renaming/typing |
| `dim_products` | `table` | Small, infrequently changed, fast reads |
| `fct_orders` | `incremental` | Too large for full rebuild daily |
| `int_order_enriched` | `ephemeral` or `table` | Ephemeral if simple, table if expensive join |
| `rpt_sales_today` | `materialized_view` | Auto-refreshed, always current |

Key rule: never use `table` for 500M-row models that grow daily — you'll rebuild them from scratch on every run. Use `incremental` instead.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: New Column Silently Dropped in Incremental Model

**Scenario:** Your incremental model `fct_events` has been running for 6 months. You added a new column `device_type` to the source last week. Now `dbt run --select fct_events` silently drops the new column from the output. Why?

<details>
<summary>💡 Hint</summary>

The default `on_schema_change` behavior is to ignore new columns. dbt only inserts columns that existed when the table was first created.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Removing Duplicate Rows Without a Full Rebuild

**Scenario:** You run `SELECT COUNT(*), COUNT(DISTINCT order_id) FROM fct_orders` and find 10% more rows than distinct `order_id` values — meaning ~1M duplicate rows. The model uses `incremental_strategy='append'`. How do you fix this without a full rebuild that would take 4 hours?

<details>
<summary>💡 Hint</summary>

Fix the duplicates in-place using a ROW_NUMBER window function (no need to rebuild). Then switch the strategy from `append` to `merge` to prevent recurrence. Add a `unique` test to catch this early going forward.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What materialization would you use for a 500M-row fact table?" — Incremental, always. Full table rebuilds on large fact tables are too expensive. Explain your `unique_key`, `on_schema_change`, and whether you'd use merge, append, or insert_overwrite.

> **Tip 2:** "Why is my incremental model not picking up new columns?" — The default `on_schema_change='ignore'` means new source columns are silently dropped. Set `sync_all_columns` for evolving models, or run `--full-refresh` once to rebuild with the new schema.

> **Tip 3:** "How do you fix duplicate rows in a production table without downtime?" — Use `CREATE OR REPLACE TABLE AS SELECT ... WHERE rn = 1` with a ROW_NUMBER window function. This runs in the warehouse and doesn't require dbt at all. Then fix the model to use `merge` strategy going forward and add a `unique` test.
