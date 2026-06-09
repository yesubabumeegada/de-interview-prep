---
title: "dbt Sources & Staging - Scenarios"
topic: dbt
subtopic: sources-and-staging
content_type: scenario_question
tags: [dbt, sources, staging, interview, scenarios]
---

# dbt Sources & Staging — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Fixing a Source Not Found Error

**Scenario:** Running `dbt run` gives this error:
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

What's wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>

The error mentions `shopify` but your YAML says `shopify_prod`. One of them is wrong — check the actual schema name in the warehouse.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Triaging a Source Freshness Failure in Production

**Scenario:** At 8am Monday, dbt Cloud alerts that `raw_shopify.orders` source freshness has failed (error threshold: 12 hours). The data team is panicking because the executive dashboard shows no weekend sales. How do you triage?

<details>
<summary>💡 Hint</summary>

Work backwards from the symptom: freshness check failed → why didn't the data arrive? Check the ingestion tool (Fivetran/Airbyte), then the source system, then communicate status before diving into a fix.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Zero-Downtime Source System Migration

**Scenario:** Your company is migrating from Fivetran to Airbyte for Salesforce data. Fivetran loads to `RAW_DB.salesforce_fivetran`, Airbyte will load to `RAW_DB.salesforce_airbyte`. You have 20+ staging models. How do you migrate with zero downtime?

<details>
<summary>💡 Hint</summary>

The key is that staging models abstract away the source location. Run both sources in parallel, validate parity between them, then do a single-PR cutover to switch staging models to the new source. Downstream models never change.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

---

## Interview Tips

> **Tip 1:** "Why do we have a staging layer in dbt?" — Staging models are a thin, source-specific transformation layer. They rename columns, cast types, and reference the source exactly once. All downstream models join through staging, not raw sources. This means if the source system changes, you only update the staging model.

> **Tip 2:** "How do you handle a source freshness failure?" — Structured triage: verify the failure with `dbt source freshness`, check the ingestion tool logs, check the source system status, communicate to stakeholders with an ETA, then fix and rerun only the affected models.

> **Tip 3:** "How do you migrate source systems without downtime?" — Run both sources in parallel for validation. Use `audit_helper.compare_relations` to verify parity. Do the cutover in a single PR that swaps the source reference in the staging model. Downstream models never see the change because staging abstracts the source location.
