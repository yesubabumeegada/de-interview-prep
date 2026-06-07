---
title: "dbt Seeds & Snapshots - Real-World"
topic: dbt
subtopic: seeds-and-snapshots
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, seeds, snapshots, scd2, production]
---

# dbt Seeds & Snapshots — Real-World Examples

## Example 1: E-Commerce SCD2 Customer Dimension

Track customer tier upgrades and address changes over time:

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}

SELECT
    customer_id,
    email,
    full_name,
    shipping_address,
    billing_address,
    membership_tier,   -- tracks Gold/Silver/Bronze changes
    is_subscribed,
    updated_at
FROM {{ source('raw_shopify', 'customers') }}
{% endsnapshot %}
```

Report: How many customers upgraded tier in Q4?
```sql
SELECT COUNT(DISTINCT customer_id) AS tier_upgrades
FROM {{ ref('snap_customers') }}
WHERE dbt_valid_from BETWEEN '2023-10-01' AND '2023-12-31'
  AND membership_tier IN ('Gold', 'Silver')
  AND dbt_valid_to IS NOT NULL  -- version was superseded (tier changed again)
```

## Example 2: Pricing History for Revenue Analytics

Track product price changes — essential for accurate historical revenue calculations:

```sql
-- snapshots/snap_product_prices.sql
{% snapshot snap_product_prices %}
{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='check',
    check_cols=['price_usd', 'sale_price_usd', 'cost_usd']
) }}

SELECT
    product_id,
    product_name,
    price_usd,
    sale_price_usd,
    cost_usd
FROM {{ source('raw', 'products') }}
{% endsnapshot %}
```

Historical revenue with correct prices at time of order:
```sql
-- models/marts/fct_orders_historical_revenue.sql
SELECT
    o.order_id,
    o.order_date,
    o.product_id,
    o.quantity,
    -- Use price that was active at time of order
    p.price_usd AS price_at_order,
    o.quantity * p.price_usd AS historical_revenue,
    -- Compare to current price
    p_current.price_usd AS current_price
FROM {{ ref('fct_orders') }} o

-- Join to price snapshot at time of order
JOIN {{ ref('snap_product_prices') }} p
    ON o.product_id = p.product_id
    AND o.order_date BETWEEN p.dbt_valid_from
        AND COALESCE(p.dbt_valid_to, '9999-12-31')

-- Join to current price
JOIN {{ ref('snap_product_prices') }} p_current
    ON o.product_id = p_current.product_id
    AND p_current.dbt_valid_to IS NULL
```

## Example 3: Reference Data Management (Seeds)

Managing 12+ lookup tables as seeds:

```
seeds/
├── reference/
│   ├── country_codes.csv        # ISO country codes
│   ├── currency_rates.csv       # Static FX rates (updated quarterly)
│   ├── product_categories.csv   # Internal category taxonomy
│   ├── channel_mapping.csv      # UTM source → marketing channel
│   ├── region_mapping.csv       # Postal code → sales region
│   └── holiday_calendar.csv     # Business days calendar
└── finance/
    ├── gl_account_mapping.csv   # GL codes → business categories
    └── cost_center_hierarchy.csv # Finance org hierarchy
```

```yaml
# dbt_project.yml
seeds:
  my_project:
    reference:
      +schema: reference_data
      +tags: ['reference', 'seed']
    finance:
      +schema: finance_reference
      +tags: ['reference', 'finance', 'seed']
      +grants:
        select: ['ROLE_FINANCE_ANALYST']
```

## Example 4: Compliance Audit Trail

Using snapshots to prove data values at a specific point in time for audits:

```sql
-- Auditor request: "What was customer 12345's tier on Dec 31, 2023?"
SELECT
    customer_id,
    membership_tier,
    dbt_valid_from,
    dbt_valid_to,
    'Active as of 2023-12-31' AS note
FROM {{ ref('snap_customers') }}
WHERE customer_id = 12345
  AND '2023-12-31' BETWEEN dbt_valid_from
      AND COALESCE(dbt_valid_to, '9999-12-31')
```

This is admissible audit evidence — dbt snapshots provide a tamper-evident history because:
1. All snapshot runs are logged with timestamps
2. `dbt_updated_at` records exactly when each version was captured
3. Git history shows who triggered the snapshot run
