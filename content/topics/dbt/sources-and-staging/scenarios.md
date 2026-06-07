---
title: "dbt Sources & Staging - Scenarios"
topic: dbt
subtopic: sources-and-staging
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, sources, staging, interview, scenarios]
---

# dbt Sources & Staging — Scenario Questions

## Scenario 1 (Junior): Source Not Found Error

**Situation:** Running `dbt run` gives this error:
```
Compilation Error in model stg_orders
  Database Error: Object 'RAW_DB.shopify.orders' does not exist or not authorized
```

Your `_sources.yml` says:
```yaml
sources:
  - name: shopify
    schema: shopify_prod
    tables:
      - name: orders
```

**What's wrong and how do you fix it?**

**Answer:**

The schema in the source definition (`shopify_prod`) doesn't match what's in the warehouse. Options:

1. **Check actual schema name** in Snowflake/BigQuery — it might be `shopify` not `shopify_prod`
2. **Fix the source definition:**
   ```yaml
   sources:
     - name: shopify
       schema: shopify   # corrected
   ```
3. **Check database** — the error says `RAW_DB` but maybe your profile targets `DEV_DB`
4. **Check permissions** — the role used by dbt may not have USAGE on that schema

Debug with: `dbt debug` and `dbt compile --select stg_orders` to see the resolved SQL.

---

## Scenario 2 (Mid-Level): Source Freshness Failure in Production

**Situation:** At 8am Monday, dbt Cloud alerts that `raw_shopify.orders` source freshness has failed (error threshold: 12 hours). The data team is panicking because the executive dashboard shows no weekend sales. How do you triage?

**Answer:**

**Step 1 — Verify the failure:**
```bash
dbt source freshness --select source:shopify.orders
# Shows: Last loaded 18 hours ago
```

**Step 2 — Check Fivetran/Airbyte logs:**
- Was there a connector failure Saturday night?
- Is there a schema change error in the connector?

**Step 3 — Check the source system:**
- Did Shopify's API have an outage? (Check Shopify Status page)
- Was there a weekend maintenance window?

**Step 4 — Communicate:**
- Notify stakeholders: "Dashboard shows no weekend data due to data pipeline delay. Investigating."
- Set ETA for resolution

**Step 5 — Remediate:**
- If Fivetran failure: trigger a manual sync, monitor until caught up
- If schema change: update source definition + run `dbt run --full-refresh --select stg_shopify_orders+`

**Step 6 — Prevent recurrence:**
- Add PagerDuty alert on source freshness failure
- Add a dbt test: `dbt_utils.recency` to catch issues before dashboard users notice

---

## Scenario 3 (Senior): Migrate Source System Without Downstream Impact

**Situation:** Your company is migrating from Fivetran to Airbyte for Salesforce data. Fivetran loads to `RAW_DB.salesforce_fivetran`, Airbyte will load to `RAW_DB.salesforce_airbyte`. You have 20+ staging models. How do you migrate with zero downtime?

**Answer:**

**Strategy: Parallel run → cutover → cleanup**

**Phase 1 — Add new source definition:**
```yaml
sources:
  - name: salesforce_fivetran   # existing
    schema: salesforce_fivetran
  - name: salesforce_airbyte    # new
    schema: salesforce_airbyte
```

**Phase 2 — Create parallel staging models:**
```sql
-- stg_salesforce_accounts_airbyte.sql (new)
SELECT ... FROM {{ source('salesforce_airbyte', 'account') }}
```

**Phase 3 — Validation:**
```sql
-- Use audit_helper package to compare row counts and columns
{{ audit_helper.compare_relations(
    a_relation=ref('stg_salesforce_accounts'),        -- Fivetran
    b_relation=ref('stg_salesforce_accounts_airbyte') -- Airbyte
) }}
```

**Phase 4 — Cutover (in one PR):**
- Rename `stg_salesforce_accounts_airbyte.sql` → `stg_salesforce_accounts.sql`
- Update source reference: `{{ source('salesforce_airbyte', ...) }}`
- Remove old Fivetran source definition

**Phase 5 — Monitor** first run post-cutover for row count parity.

Key principle: **source abstraction via staging models** means downstream `fct_` and `dim_` models never need to change — only the staging layer changes.
