---
title: "dbt Sources & Staging"
topic: dbt
subtopic: sources-and-staging
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, sources, staging, freshness, schema-yml]
---

# dbt Sources & Staging

## What Are Sources?

Sources in dbt represent **raw tables loaded by your ELT tool** (Fivetran, Airbyte, etc.). Declaring them enables freshness checks, lineage tracking, and clean references.

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: raw_shopify          # logical source name
    database: RAW_DB           # actual database
    schema: shopify            # actual schema
    description: "Shopify raw data loaded by Fivetran, refreshed every hour"

    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 12, period: hour}
    loaded_at_field: _fivetran_synced

    tables:
      - name: orders
        description: "One row per Shopify order"
        freshness:
          warn_after: {count: 2, period: hour}
        columns:
          - name: id
            description: "Shopify order ID"
            tests:
              - unique
              - not_null

      - name: customers
        description: "One row per Shopify customer"

      - name: order_line_items
        description: "One row per line item per order"
```

## source() vs ref()

```sql
-- Use source() to reference raw tables
SELECT * FROM {{ source('raw_shopify', 'orders') }}

-- Use ref() to reference other dbt models
SELECT * FROM {{ ref('stg_shopify_orders') }}
```

`source()` resolves to: `RAW_DB.shopify.orders`
`ref()` resolves based on target environment (dev/prod schemas)

## Staging Models — Purpose and Pattern

Staging models are the **first transformation layer** — they clean raw data but add no business logic:

```sql
-- models/staging/stg_shopify_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw_shopify', 'orders') }}
),

renamed AS (
    SELECT
        -- IDs
        id                          AS order_id,
        customer_id,
        -- Timestamps
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        -- Strings: standardize case
        LOWER(financial_status)     AS payment_status,
        LOWER(fulfillment_status)   AS fulfillment_status,
        -- Numerics: cast and round
        CAST(total_price AS NUMERIC) AS order_total_usd,
        -- Booleans
        CAST(test AS BOOLEAN)       AS is_test_order,
        -- Metadata
        _fivetran_synced            AS _loaded_at
    FROM source
    WHERE id IS NOT NULL
        AND test = false   -- exclude test orders
)

SELECT * FROM renamed
```

## Staging Rules

| Rule | Example |
|---|---|
| Only reference `source()`, never `ref()` | `FROM {{ source('raw', 'orders') }}` |
| Rename columns to standard names | `id → order_id` |
| Cast to correct data types | `CAST(price AS NUMERIC)` |
| Standardize strings | `LOWER(status)` |
| No business logic | No joins, no aggregations |
| One staging model per source table | `stg_shopify_orders` for `shopify.orders` |

## Source Freshness

```bash
# Check all sources freshness
dbt source freshness

# Check specific source
dbt source freshness --select source:raw_shopify.orders
```

Output:
```
Found 3 sources. Checking freshness of 3 of them...
  raw_shopify.orders ......... [WARN] Last loaded 7 hours 12 minutes ago
  raw_shopify.customers ....... [PASS] Last loaded 1 hour 3 minutes ago
  raw_shopify.order_line_items  [PASS] Last loaded 1 hour 5 minutes ago
```

## Source Schema Documentation

```yaml
sources:
  - name: raw_postgres
    description: "Production Postgres DB via Debezium CDC"
    meta:
      owner: "@platform-team"
      pii_present: true
    tables:
      - name: users
        columns:
          - name: email
            description: "User email — PII, masked in non-prod"
            meta:
              pii: true
              classification: sensitive
```

## Multiple Sources, Multiple Files

Organize source files by source system:

```
models/staging/
├── shopify/
│   ├── _shopify_sources.yml
│   ├── stg_shopify_orders.sql
│   ├── stg_shopify_customers.sql
│   └── stg_shopify_products.sql
├── stripe/
│   ├── _stripe_sources.yml
│   ├── stg_stripe_payments.sql
│   └── stg_stripe_refunds.sql
└── salesforce/
    ├── _salesforce_sources.yml
    └── stg_salesforce_accounts.sql
```
