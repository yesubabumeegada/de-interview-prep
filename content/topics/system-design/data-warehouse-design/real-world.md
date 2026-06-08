---
title: "Data Warehouse Design — Real World"
topic: system-design
subtopic: data-warehouse-design
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, data-warehouse, dbt, snowflake, production, design-patterns]
---

# Data Warehouse Design — Real World

## Pattern 1: Complete Modern DW Stack

**Company:** SaaS company, 50M users, analytics on subscriptions + usage + support

```
Source Systems:
  Postgres (subscriptions, users) → Fivetran → Snowflake raw
  Stripe (billing)                → Fivetran → Snowflake raw
  Salesforce (CRM)                → Fivetran → Snowflake raw
  Application logs (clickstream)  → Kafka → S3 → Snowflake COPY INTO

Layer Structure (dbt):
  raw (Fivetran auto-loaded):
    raw.fivetran_postgres.subscriptions
    raw.stripe.charges
    raw.salesforce.accounts
  
  staging (dbt stg_ models):
    stg_subscriptions: renamed columns, cast types, null handling
    stg_charges: deduped, convert Stripe cents to dollars
    stg_accounts: standardize company size categories
  
  intermediate (dbt int_ models):
    int_customers_unified: join postgres users + salesforce accounts → one customer view
    int_subscription_status: calculate subscription state at each point in time
  
  marts (dbt mart_ / fct_ / dim_ models):
    fct_charges: fact table of all charges (grain: one charge)
    fct_subscription_events: fact table of subscription changes
    dim_customer: conformed customer dimension with SCD type 2
    dim_date: standard date dimension
  
  reporting (gold):
    rpt_mrr_by_segment: monthly recurring revenue by customer segment
    rpt_churn_analysis: cohort churn rates
    rpt_support_csat: support ticket satisfaction scores
```

---

## Pattern 2: SCD Type 2 in Production with dbt

```sql
-- dbt model: dim_customer (SCD type 2 using dbt snapshots)

-- snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}
{{
  config(
    target_database='analytics',
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['region', 'segment', 'customer_name'],
    invalidate_hard_deletes=True
  )
}}

SELECT
  customer_id,
  customer_name,
  email,
  region,
  segment,
  updated_at
FROM {{ source('postgres', 'customers') }}

{% endsnapshot %}

-- dbt adds: dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at
-- Query: customer as they were during a sale:
SELECT f.sale_id, c.region, f.total_amount
FROM fct_sales f
JOIN snapshots.customer_snapshot c
  ON f.customer_id = c.customer_id
  AND f.sale_date BETWEEN c.dbt_valid_from AND COALESCE(c.dbt_valid_to, CURRENT_DATE)
```

---

## Common DW Design Mistakes and Fixes

| Mistake | Problem | Fix |
|---|---|---|
| Fact table too granular | 100B rows, slow queries | Aggregate to appropriate grain; build summary tables |
| Fact table too coarse | Can't answer item-level questions | Add line-item granularity from source |
| No conformed dimensions | Each team has their own `dim_customer` | Build enterprise conformed dimensions; enforce via data contracts |
| Natural keys as fact table FKs | Source system changes ID → broken joins | Always use surrogate keys in fact tables |
| SCD Type 1 everywhere | Historical analysis wrong (customer moved, shows new region for old sales) | SCD Type 2 for any attribute used in historical analysis |
| No `dim_date` | Date calculations in every SQL query | Build standard dim_date; add business calendar flags |
| Metrics defined in BI tool | Revenue defined 5 ways in 5 dashboards | Semantic layer; define metrics in dbt/LookML once |

---

## Interview Tips

> **Tip 1:** "How do you handle null values in a star schema?" — Null foreign keys in fact tables (e.g., sale with no customer) should point to a special "Unknown" or "Not Applicable" surrogate key row in the dimension (customer_key = -1). Never use NULL as a FK — it breaks joins and causes rows to disappear silently in query results. Unknown dimension rows have attributes like customer_name = 'Unknown', region = 'N/A'.

> **Tip 2:** "A business user says the numbers in two dashboards don't match. How do you debug?" — First: check if they're using the same metric definition (revenue = gross? net? after discount?). Second: check the grain difference (one dashboard shows orders, another shows line items). Third: check the date filter (order_date vs ship_date vs payment_date). Fourth: check if SCD type 2 is applied consistently (some queries may not join with time-bound dimension version). Use query profiling to trace exactly what SQL each dashboard generates.

> **Tip 3:** "How would you approach building a DW from scratch at a startup?" — Start narrow, not wide. Sprint 1: one subject area (e.g., revenue). Pick the most important business question and build exactly what's needed. Sprint 2: add the next subject area. Avoid building a full enterprise model before you understand what questions the business will actually ask. Use dbt from day 1 (documentation, testing, lineage free). Choose a cloud DW (Snowflake/BigQuery) for instant scale. Add conformed dimensions as you identify shared entities across subject areas.
