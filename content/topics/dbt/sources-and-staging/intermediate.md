---
title: "dbt Sources & Staging - Intermediate"
topic: dbt
subtopic: sources-and-staging
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, source-overrides, staging-patterns, external-sources]
---

# dbt Sources & Staging — Intermediate

## Source Overrides and Environment-Specific Sources

Point different environments to different databases:

```yaml
# models/staging/_sources.yml
sources:
  - name: raw
    database: "{{ env_var('DBT_RAW_DATABASE', 'RAW_DB') }}"
    schema: "{{ env_var('DBT_RAW_SCHEMA', 'public') }}"
    tables:
      - name: orders
```

```bash
# Development
DBT_RAW_DATABASE=DEV_RAW dbt run

# Production
DBT_RAW_DATABASE=PROD_RAW dbt run
```

## External Sources (dbt-external-tables)

Reference tables in external storage (S3, GCS) directly:

```yaml
# Install: dbt-external-tables package
sources:
  - name: external_data
    schema: external
    tables:
      - name: clickstream
        external:
          location: "s3://my-bucket/clickstream/"
          file_format: parquet
          row_format: serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          table_properties: "('classification'='parquet')"
        columns:
          - name: user_id
            data_type: bigint
          - name: event_type
            data_type: varchar
```

```bash
dbt run-operation stage_external_sources
```

## Advanced Staging Patterns

### Deduplication in Staging

```sql
-- stg_orders.sql — deduplicate CDC log
WITH source AS (
    SELECT * FROM {{ source('cdc', 'orders') }}
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _cdc_timestamp DESC
            ) AS rn
    ) ranked
    WHERE rn = 1
        AND _cdc_operation != 'DELETE'
)

SELECT
    order_id,
    customer_id,
    total_amount,
    status
FROM deduped
```

### Union Multiple Source Tables

```sql
-- stg_all_events.sql — union events from multiple systems
{{ config(materialized='view') }}

WITH web_events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        created_at,
        'web' AS platform
    FROM {{ source('web_analytics', 'events') }}
),

mobile_events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        created_at,
        'mobile' AS platform
    FROM {{ source('mobile_analytics', 'events') }}
),

all_events AS (
    SELECT * FROM web_events
    UNION ALL
    SELECT * FROM mobile_events
)

SELECT * FROM all_events
```

### Generate Surrogate Keys in Staging

```sql
-- stg_orders.sql using dbt_utils
SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'source_system']) }} AS sk_order,
    order_id,
    source_system,
    ...
FROM {{ source('raw', 'orders') }}
```

## Source Tests at the Raw Layer

```yaml
sources:
  - name: raw
    tables:
      - name: orders
        tests:
          - dbt_utils.expression_is_true:
              expression: "total_amount >= 0"
          - dbt_utils.recency:
              datepart: hour
              field: _loaded_at
              interval: 6
        columns:
          - name: id
            tests:
              - unique
              - not_null
          - name: customer_id
            tests:
              - not_null
              - relationships:
                  to: source('raw', 'customers')
                  field: id
```

## Freshness as SLA Alerting

```yaml
sources:
  - name: raw_payments
    loader: Fivetran
    freshness:
      warn_after: {count: 1, period: hour}
      error_after: {count: 3, period: hour}
    loaded_at_field: _fivetran_synced
    tables:
      - name: transactions
        # Per-table override — payments are more critical
        freshness:
          warn_after: {count: 30, period: minute}
          error_after: {count: 2, period: hour}
```

Integrate with alerting:
```yaml
# dbt Cloud: configure notifications on source freshness failures
# Or in CLI orchestration:
dbt source freshness || send_slack_alert "Source freshness check failed!"
```

## Staging Documentation Patterns

The "self-documenting staging" pattern — every column has a description:

```yaml
models:
  - name: stg_shopify_orders
    description: >
      Cleaned Shopify orders. One row per order. Excludes test orders and 
      soft-deleted records. Grain: order_id.
    config:
      tags: ['staging', 'shopify', 'hourly']
    meta:
      owner: "@data-platform"
      source_freshness_sla: "2 hours"
    columns:
      - name: order_id
        description: "Shopify order ID. Source column: id"
        tests: [unique, not_null]
      - name: payment_status
        description: "Lowercased from source financial_status. Values: pending, authorized, paid, refunded, voided"
```
