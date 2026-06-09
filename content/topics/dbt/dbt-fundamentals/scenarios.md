---
title: "dbt Fundamentals - Scenario Questions"
topic: dbt
subtopic: dbt-fundamentals
content_type: scenario_question
tags: [dbt, interview, scenarios, troubleshooting]
---

# dbt Fundamentals — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Adding a Derived Column to an Existing Model

**Scenario:** You joined a company that uses dbt with Snowflake. Your first task is to add a new column `customer_tier` (derived as Gold/Silver/Bronze based on total spend) to the existing `dim_customers` model.

<details>
<summary>💡 Hint</summary>

Think about the full workflow: branching, editing the SQL, documenting with tests, running locally, and opening a PR. A CASE expression handles tiered logic cleanly.

</details>

<details>
<summary>✅ Solution</summary>

1. **Pull latest code** from the repo, create a feature branch
2. **Open** `models/marts/core/dim_customers.sql`
3. **Add the column** using a CASE expression:
   ```sql
   CASE
       WHEN lifetime_spend >= 10000 THEN 'Gold'
       WHEN lifetime_spend >= 1000  THEN 'Silver'
       ELSE 'Bronze'
   END AS customer_tier
   ```
4. **Add documentation** in `schema.yml`:
   ```yaml
   - name: customer_tier
     description: "Customer tier based on lifetime spend: Gold (>=10k), Silver (>=1k), Bronze (<1k)"
     tests:
       - accepted_values:
           values: ['Gold', 'Silver', 'Bronze']
   ```
5. **Run locally** to test: `dbt run --select dim_customers`
6. **Run tests**: `dbt test --select dim_customers`
7. **Open a PR** — CI will run the model in the CI environment

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnosing and Fixing a Slow Production Model

**Scenario:** The `fct_orders` model runs fine in dev (30 seconds) but takes 45 minutes in production. Production has 500M rows. The model uses `materialized='table'`. How do you diagnose and fix it?

<details>
<summary>💡 Hint</summary>

Start by examining the query plan — look for full table scans or missing partition pruning. Switching to incremental materialization with proper partitioning is the standard fix for large fact tables.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1 — Understand the model:**
```sql
-- Check if it's doing a full scan with a large join
EXPLAIN
SELECT * FROM fct_orders;
-- Look for: full table scans, no partition pruning, cross joins
```

**Step 2 — Apply incremental materialization:**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    partition_by={"field": "order_date", "data_type": "date"}
) }}

SELECT ...
FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }}) - INTERVAL '3 days'
{% endif %}
```

**Step 3 — Cluster the table** (Snowflake/BigQuery):
```sql
{{ config(
    cluster_by=['customer_id', 'order_date']
) }}
```

**Step 4 — Pre-filter upstream models** — if `stg_orders` scans 500M rows on every join, push filters upstream.

**Result:** Model drops from 45 min → 2 min after switching to incremental + partitioning.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Managing a Breaking Column Rename Across a dbt Mesh

**Scenario:** You need to rename the column `cust_id` to `customer_id` in `dim_customers`, which is used by 15 downstream models across 3 dbt projects (dbt Mesh setup). How do you manage this breaking change without downtime?

<details>
<summary>💡 Hint</summary>

The key principle is a backward-compatible deprecation cycle: keep both names for 2 sprints, use dbt model contracts to enforce the new column, notify downstream teams, and only remove the alias after confirming zero references.

</details>

<details>
<summary>✅ Solution</summary>

**Strategy: Backward-compatible migration with deprecation cycle**

**Phase 1 — Add new column alongside old:**
```sql
-- dim_customers.sql
SELECT
    customer_id,
    customer_id AS cust_id,  -- deprecated alias, keep for 2 sprints
    ...
FROM ...
```

**Phase 2 — Enforce contract on the new column:**
```yaml
models:
  - name: dim_customers
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: cust_id
        description: "DEPRECATED: Use customer_id instead. Will be removed in v2.1"
```

**Phase 3 — Notify downstream teams** via:
- Slack announcement with migration guide
- PR to each downstream project updating references
- Set deprecation `meta` flag for lineage visibility

**Phase 4 — Remove alias** after all consumers are migrated (confirm via `manifest.json` lineage analysis):
```bash
# Find all references to cust_id across projects
grep -r "cust_id" models/ --include="*.sql"
```

**Phase 5 — Bump model version** using dbt's versioning:
```yaml
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
        defined_in: dim_customers_v1
      - v: 2
```

Key principle: **never rename a public model column without a deprecation cycle** in a multi-team environment.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "Walk me through how you'd add a column to a dbt model." — Start with branching and local changes, add schema.yml tests, run + test locally, then open a PR. Show you know the full development workflow, not just the SQL.

> **Tip 2:** "Why is this model slow in production but fast in dev?" — Dev has little data; production has 500M rows. The first thing to check is materialization strategy: table rebuilds are expensive. Switching to incremental with the right partition key is the most impactful fix.

> **Tip 3:** "How do you handle breaking changes in dbt?" — Never rename or drop a public column without a deprecation cycle. Add the new column alongside the old, announce a migration window, update downstream references, then remove the old alias. In dbt Mesh, model contracts enforce this formally.

---

## ⚡ Quick-fire Q&A

**Q: What's the difference between a model, source, and seed?**
A: A model is a SQL file in `models/` that dbt compiles and runs — it defines a transformation. A source is a declaration of a raw table that exists in the warehouse but was loaded by an external process (not dbt) — defined in `schema.yml` with `sources:`. A seed is a CSV file in `seeds/` that dbt loads directly into the warehouse as a table — used for small static reference data (e.g., country codes, exchange rates).

**Q: What are the 4 materializations in dbt?**
A: `view` (default — creates a SQL view, no data stored), `table` (drops and recreates as a physical table on each run), `incremental` (inserts/updates only new or changed rows — uses a filter on `is_incremental()`), and `ephemeral` (a CTE injected into downstream models — never materialized in the warehouse).

**Q: What is `ref()` and why use it instead of hard-coding table names?**
A: `ref('model_name')` references another dbt model and resolves to the correct schema and table name at compile time. It builds the DAG automatically (dbt knows the dependency order), enables cross-environment portability (dev schema vs prod schema), and ensures models are always run in the right order. Hard-coded table names bypass the DAG and break in non-prod environments.

**Q: What is the dbt DAG?**
A: The Directed Acyclic Graph of all models and their dependencies, inferred automatically from `ref()` and `source()` calls. dbt uses this graph to determine execution order — upstream models run before downstream ones. You can visualize it with `dbt docs generate && dbt docs serve`.

**Q: What's the difference between `dbt test` and `dbt run`?**
A: `dbt run` executes the SQL transformations and materializes models in the warehouse. `dbt test` runs data quality assertions defined in `schema.yml` (e.g., `not_null`, `unique`, `accepted_values`, `relationships`) against the materialized data. Neither does the other's job — run models first, then test them.

**Q: What is a `schema.yml` file?**
A: A YAML configuration file in dbt that documents and tests models, sources, macros, and seeds. It defines column descriptions, data quality tests, source freshness checks, and model-level metadata. It is the primary way to attach tests and documentation to dbt objects.

**Q: What does `dbt compile` do?**
A: `dbt compile` resolves all Jinja templating and `ref()`/`source()` calls and writes the compiled SQL to the `target/compiled/` directory — without running anything against the warehouse. Useful for debugging: you can inspect the exact SQL that will be executed before running it.

**Q: What's the difference between `incremental` and `table` materializations?**
A: `table` drops and fully rebuilds the table on every `dbt run` — simple but expensive for large datasets. `incremental` only processes new or changed rows (filtered by a date/timestamp condition inside `{% if is_incremental() %}`), appending or merging into the existing table. Incremental is faster for large, append-heavy tables; table is safer when historical data can change and you need a clean rebuild.
