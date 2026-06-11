---
title: "dbt Seeds & Snapshots"
topic: dbt
subtopic: seeds-and-snapshots
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, seeds, snapshots, scd-type-2, history-tracking]
---

# dbt Seeds & Snapshots


## 🎯 Analogy

Think of seeds like lookup tables checked into Git — small CSV files (country codes, category mappings) that dbt loads into your warehouse. Snapshots are a time machine: they track how a row changed over time using Type 2 SCD logic.

---
## Seeds — Static Reference Data

Seeds are CSV files that dbt loads into your warehouse as tables. Use them for small, slowly-changing reference data:

```
seeds/
├── country_codes.csv
├── product_categories.csv
└── marketing_channel_mapping.csv
```

```csv
# seeds/country_codes.csv
country_code,country_name,region,currency
US,United States,North America,USD
GB,United Kingdom,Europe,GBP
DE,Germany,Europe,EUR
JP,Japan,Asia Pacific,JPY
```

```bash
dbt seed              # Load all seeds
dbt seed --select country_codes   # Load one seed
dbt seed --full-refresh           # Drop and recreate
```

Reference in models:
```sql
SELECT
    o.order_id,
    o.country_code,
    c.country_name,
    c.region
FROM {{ ref('fct_orders') }} o
JOIN {{ ref('country_codes') }} c
    ON o.country_code = c.country_code
```

## Seed Configuration

```yaml
# dbt_project.yml
seeds:
  my_project:
    +schema: reference_data   # All seeds go to this schema
    +quote_columns: false
    country_codes:
      +column_types:
        country_code: varchar(2)
        currency: varchar(3)
    product_categories:
      +tags: ['reference']
      +docs:
        show: true
```

## When to Use Seeds vs. Sources

| Use Seeds | Use Sources |
|---|---|
| < 1000 rows | Any size |
| Changes rarely (yearly) | Updated by ELT tools |
| Maintained by analysts | Maintained by engineering |
| Lives in git | Lives in warehouse |
| mapping tables, lookups | transactional data |

## Snapshots — SCD Type 2 History

Snapshots track historical changes to source data, creating a Type 2 slowly-changing dimension:

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}

{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT
    customer_id,
    email,
    first_name,
    last_name,
    address,
    tier,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

```bash
dbt snapshot         # Run all snapshots
dbt snapshot --select snap_customers
```

## Snapshot Metadata Columns

dbt automatically adds 4 columns to snapshot tables:

| Column | Description |
|---|---|
| `dbt_scd_id` | Unique row identifier (MD5 hash) |
| `dbt_updated_at` | When dbt captured this version |
| `dbt_valid_from` | When this version became active |
| `dbt_valid_to` | When this version was superseded (NULL = current) |

## Snapshot Strategies

### timestamp strategy

```sql
{{ config(
    strategy='timestamp',
    unique_key='customer_id',
    updated_at='updated_at'  -- source must have this column
) }}
```

Triggers snapshot when `updated_at` changes. Fast, requires a reliable `updated_at` column.

### check strategy

```sql
{{ config(
    strategy='check',
    unique_key='customer_id',
    check_cols=['email', 'address', 'tier']  -- track these columns
    -- or check_cols='all' to track every column
) }}
```

Triggers snapshot when any tracked column changes. Slower (compares all rows), but works without `updated_at`.

## Querying Snapshots

```sql
-- Current state only
SELECT * FROM {{ ref('snap_customers') }}
WHERE dbt_valid_to IS NULL;

-- State at a specific point in time
SELECT * FROM {{ ref('snap_customers') }}
WHERE '2023-06-01' BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31');

-- Full history for a customer
SELECT *
FROM {{ ref('snap_customers') }}
WHERE customer_id = 12345
ORDER BY dbt_valid_from;
```

## ▶️ Try It Yourself

```sql
-- seeds/country_codes.csv (committed to Git)
-- code,name
-- US,United States
-- DE,Germany
-- dbt seed  →  loads into warehouse as a table

-- snapshots/orders_snapshot.sql  (Type 2 SCD)
{% snapshot orders_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='order_id',
    strategy='timestamp',
    updated_at='updated_at',
) }}
SELECT * FROM {{ source('raw', 'orders') }}
{% endsnapshot %}
-- dbt snapshot  →  adds dbt_valid_from / dbt_valid_to columns
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
