---
title: "dbt Fundamentals - Scenario Questions"
topic: dbt
subtopic: dbt-fundamentals
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, interview, scenarios, troubleshooting]
---

# dbt Fundamentals — Scenario Questions

## Scenario 1 (Junior): New to dbt

**Situation:** You joined a company that uses dbt with Snowflake. Your first task is to add a new column `customer_tier` (derived as Gold/Silver/Bronze based on total spend) to the existing `dim_customers` model.

**What steps do you take?**

**Answer:**
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

---

## Scenario 2 (Mid-Level): Slow Model Investigation

**Situation:** The `fct_orders` model runs fine in dev (30 seconds) but takes 45 minutes in production. Production has 500M rows. The model uses `materialized='table'`. How do you diagnose and fix it?

**Answer:**

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

---

## Scenario 3 (Senior): Breaking Change in a Shared Model

**Situation:** You need to rename the column `cust_id` to `customer_id` in `dim_customers`, which is used by 15 downstream models across 3 dbt projects (dbt Mesh setup). How do you manage this breaking change without downtime?

**Answer:**

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
