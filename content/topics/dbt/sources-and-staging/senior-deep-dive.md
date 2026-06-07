---
title: "dbt Sources & Staging - Senior Deep Dive"
topic: dbt
subtopic: sources-and-staging
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, source-governance, data-contracts, freshness-sla]
---

# dbt Sources & Staging — Senior Deep Dive

## Source-Level Data Contracts

Enforce column schemas at the source boundary — fail fast if upstream changes break your assumptions:

```yaml
sources:
  - name: raw_orders
    tables:
      - name: orders
        config:
          contract:
            enforced: true
        columns:
          - name: id
            data_type: bigint
            constraints:
              - type: not_null
          - name: total_price
            data_type: numeric
          - name: created_at
            data_type: timestamp_tz
```

If Fivetran suddenly delivers `total_price` as `varchar`, the build fails immediately rather than silently corrupting downstream models.

## Multi-Source Governance at Scale

For large organizations with 50+ sources:

```yaml
# models/staging/_meta_sources.yml
# Central source registry with governance metadata
sources:
  - name: fivetran_salesforce
    database: RAW_DB
    schema: salesforce
    meta:
      owner: "@revenue-ops"
      classification: confidential
      pii_columns: [email, phone, name]
      sla: "4 hours"
      data_steward: "revenue-ops@company.com"
      gdpr_relevant: true
    freshness:
      error_after: {count: 6, period: hour}
    loaded_at_field: _fivetran_synced
    tables:
      - name: opportunity
        meta:
          row_count_expected: "50000-200000"
          business_criticality: high
```

## Dynamic Source Schema Resolution

Handle multi-tenant or region-based source schemas:

```yaml
# For multi-region deployments
sources:
  - name: "{{ var('region') }}_transactions"
    database: "{{ var('region') | upper }}_RAW"
    schema: payments
    tables:
      - name: transactions
```

```bash
dbt run --vars '{"region": "eu"}' --select staging.eu.*
dbt run --vars '{"region": "us"}' --select staging.us.*
```

## Source Anomaly Detection

Detect sudden row count drops or schema drift:

```yaml
sources:
  - name: raw
    tables:
      - name: orders
        tests:
          # Row count should not drop by more than 50% vs yesterday
          - dbt_utils.expression_is_true:
              expression: >
                (SELECT COUNT(*) FROM {{ source('raw', 'orders') }}
                 WHERE _loaded_at >= CURRENT_DATE)
                >= 0.5 *
                (SELECT COUNT(*) FROM {{ source('raw', 'orders') }}
                 WHERE _loaded_at >= CURRENT_DATE - 1
                   AND _loaded_at < CURRENT_DATE)
              severity: warn
```

## Staging Layer as an API

Treat staging as a **stable public API** between your ELT tool and your transformation layer:

```
[Fivetran] → [raw.shopify.orders]
                     ↓
         [stg_shopify_orders] ← stable contract
                     ↓
         [int_order_items_enriched]
         [fct_orders]
         [dim_customers]
```

Benefits of stable staging:
1. Can change source system (switch from Fivetran to Airbyte) without touching downstream models — only update `stg_` models
2. Schema changes in source → update staging → no impact downstream
3. Easy to mock staging in tests with `unit_tests:`

## Unit Testing Staging Models (dbt 1.8+)

```yaml
# models/staging/schema.yml
unit_tests:
  - name: test_stg_orders_deduplication
    model: stg_orders
    given:
      - input: source('raw', 'orders')
        rows:
          - {id: 1, status: 'pending', _cdc_ts: '2024-01-01 10:00:00'}
          - {id: 1, status: 'shipped', _cdc_ts: '2024-01-01 11:00:00'}  # later
          - {id: 2, status: 'pending', _cdc_ts: '2024-01-01 10:00:00'}
    expect:
      rows:
        - {order_id: 1, status: 'shipped'}   # latest wins
        - {order_id: 2, status: 'pending'}
```

## Source Documentation Site Integration

```yaml
sources:
  - name: raw_stripe
    description: |
      Stripe payments data loaded by Fivetran.
      
      **Data Flow:**
      Stripe API → Fivetran → RAW_DB.stripe → [stg_stripe_*]
      
      **SLA:** Loaded every 30 minutes. Alert on >2hr delay.
      
      **Runbook:** [Link to Confluence page](https://confluence.company.com/stripe-data)
    tables:
      - name: charge
        description: |
          One row per Stripe charge. Maps to Stripe Charge object.
          [Stripe API Docs](https://stripe.com/docs/api/charges/object)
```

The `dbt docs generate` site renders Markdown — treat it as your team's data dictionary.
