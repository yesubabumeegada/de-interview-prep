---
title: "dbt Sources & Staging - Real-World"
topic: dbt
subtopic: sources-and-staging
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, staging, production, cdc, multi-source]
---

# dbt Sources & Staging — Real-World Examples

## Example 1: CDC Source Staging (Debezium + Kafka)

Staging a CDC stream from Debezium (op field: c=create, u=update, d=delete):

```sql
-- models/staging/stg_orders_cdc.sql
WITH raw_cdc AS (
    SELECT * FROM {{ source('kafka_cdc', 'orders') }}
),

-- Keep only the latest operation per order
deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY after__id
                ORDER BY ts_ms DESC
            ) AS rn
    ) t
    WHERE rn = 1
),

-- Flatten the Debezium envelope
final AS (
    SELECT
        after__id                           AS order_id,
        after__customer_id                  AS customer_id,
        after__total_amount                 AS total_amount,
        after__status                       AS status,
        TO_TIMESTAMP(ts_ms / 1000)          AS cdc_timestamp,
        op                                  AS cdc_operation
    FROM deduped
    WHERE op != 'd'   -- exclude deletes for active record view
)

SELECT * FROM final
```

## Example 2: Multi-Region Source Consolidation

Global company with EU and US Postgres sources:

```yaml
# _sources.yml
sources:
  - name: raw_us
    database: US_RAW
    schema: app
    tables:
      - name: orders
  - name: raw_eu
    database: EU_RAW
    schema: app
    tables:
      - name: orders
```

```sql
-- stg_all_orders.sql
WITH us_orders AS (
    SELECT *, 'US' AS region
    FROM {{ source('raw_us', 'orders') }}
),
eu_orders AS (
    SELECT *, 'EU' AS region
    FROM {{ source('raw_eu', 'orders') }}
)
SELECT * FROM us_orders
UNION ALL
SELECT * FROM eu_orders
```

## Example 3: Freshness Monitoring Dashboard

```sql
-- models/monitoring/source_freshness_status.sql
{{ config(materialized='view') }}

SELECT
    source_name,
    table_name,
    max_loaded_at,
    DATEDIFF('minute', max_loaded_at, CURRENT_TIMESTAMP) AS minutes_since_load,
    CASE
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP) > 12 THEN 'ERROR'
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP) > 6  THEN 'WARN'
        ELSE 'OK'
    END AS freshness_status
FROM {{ ref('dbt_source_freshness') }}  -- from elementary package
ORDER BY minutes_since_load DESC
```

## Example 4: PII Masking in Staging

```sql
-- stg_customers.sql — mask PII in non-prod
SELECT
    customer_id,
    {% if target.name == 'prod' %}
        email,
        phone_number,
        full_name
    {% else %}
        -- Mask PII in dev/ci environments
        MD5(email) || '@masked.com'    AS email,
        REGEXP_REPLACE(phone_number, '[0-9]', 'X') AS phone_number,
        'MASKED USER'                  AS full_name
    {% endif %},
    created_at,
    country_code
FROM {{ source('raw', 'customers') }}
```

## Example 5: Source Validation Macro

```sql
-- macros/validate_source_count.sql
{% macro validate_source_count(source_name, table_name, min_rows) %}
    {% set row_count_query %}
        SELECT COUNT(*) AS cnt
        FROM {{ source(source_name, table_name) }}
        WHERE _loaded_at >= CURRENT_DATE
    {% endset %}

    {% set results = run_query(row_count_query) %}

    {% if execute %}
        {% set row_count = results.columns[0].values()[0] %}
        {% if row_count < min_rows %}
            {{ exceptions.raise_compiler_error(
                "Source " ~ source_name ~ "." ~ table_name ~
                " has only " ~ row_count ~ " rows today (min: " ~ min_rows ~ ")"
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}
```

Usage in an `on-run-start` hook:
```yaml
on-run-start:
  - "{{ validate_source_count('raw_shopify', 'orders', 1000) }}"
```
