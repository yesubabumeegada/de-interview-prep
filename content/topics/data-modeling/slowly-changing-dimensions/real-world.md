---
title: "Slowly Changing Dimensions - Real-World Production Examples"
topic: data-modeling
subtopic: slowly-changing-dimensions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [scd, production, dbt-snapshots, merge, incremental, enterprise]
---

# Slowly Changing Dimensions — Real-World Production Examples

## Example 1: dbt Snapshot + Dimension Model

The most common production pattern: dbt snapshot for change tracking + dimension model for presentation.

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['name', 'email', 'city', 'state', 'segment', 'tier']
) }}

SELECT 
    customer_id,
    name,
    email,
    city,
    state,
    segment,
    tier,
    updated_at
FROM {{ source('app_db', 'customers') }}

{% endsnapshot %}

-- models/marts/dim_customer.sql
{{ config(materialized='table') }}

WITH snapshot_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} AS customer_key,
        customer_id,
        name AS customer_name,
        email,
        city,
        state,
        segment,
        tier AS loyalty_tier,
        dbt_valid_from AS effective_start,
        COALESCE(dbt_valid_to, '9999-12-31'::DATE) AS effective_end,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        -- Computed fields:
        DATEDIFF('day', dbt_valid_from, COALESCE(dbt_valid_to, CURRENT_DATE)) AS version_duration_days
    FROM {{ ref('snap_customers') }}
)

SELECT 
    *,
    -- SCD Type 6 addition: current values on ALL rows
    FIRST_VALUE(city) OVER (
        PARTITION BY customer_id ORDER BY effective_start DESC
    ) AS current_city,
    FIRST_VALUE(segment) OVER (
        PARTITION BY customer_id ORDER BY effective_start DESC
    ) AS current_segment
FROM snapshot_data
```

### Fact Table Integration

```sql
-- models/marts/fact_sales.sql
{{ config(materialized='incremental', unique_key='sale_key') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id', 'li.line_number']) }} AS sale_key,
    dd.date_key,
    -- SCD Type 2 point-in-time lookup:
    dc.customer_key,    -- Gets the CORRECT version for the order date!
    dp.product_key,
    li.quantity,
    li.unit_price,
    li.quantity * li.unit_price AS revenue,
    o.order_date
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_line_items') }} li ON o.order_id = li.order_id
JOIN {{ ref('dim_date') }} dd ON o.order_date::DATE = dd.full_date
-- KEY: Join using date range (point-in-time!)
JOIN {{ ref('dim_customer') }} dc 
    ON o.customer_id = dc.customer_id
    AND o.order_date::DATE >= dc.effective_start
    AND o.order_date::DATE < dc.effective_end
JOIN {{ ref('dim_product') }} dp ON li.product_id = dp.product_id
{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

---

## Example 2: Snowflake Streams + Tasks for Real-Time SCD

```sql
-- Source table (application writes here):
CREATE TABLE raw.customers (
    customer_id     VARCHAR(20) PRIMARY KEY,
    name            VARCHAR(200),
    email           VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50),
    segment         VARCHAR(20),
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Stream captures changes:
CREATE STREAM raw.customers_stream ON TABLE raw.customers;

-- SCD Type 2 dimension:
CREATE TABLE gold.dim_customer (
    customer_key    INT AUTOINCREMENT PRIMARY KEY,
    customer_id     VARCHAR(20),
    customer_name   VARCHAR(200),
    email           VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50),
    segment         VARCHAR(20),
    hash_diff       VARCHAR(32),
    effective_start TIMESTAMP_NTZ,
    effective_end   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
);

-- Task: Process SCD every 5 minutes
CREATE TASK gold.process_scd_customer
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw.customers_stream')
AS
BEGIN
    -- Step 1: Expire changed records
    UPDATE gold.dim_customer d
    SET effective_end = CURRENT_TIMESTAMP(),
        is_current = FALSE
    FROM raw.customers_stream s
    WHERE d.customer_id = s.customer_id
      AND d.is_current = TRUE
      AND d.hash_diff != MD5(CONCAT_WS('||', s.name, s.email, s.city, s.state, s.segment))
      AND s.metadata$action = 'INSERT';  -- Stream INSERT = source UPDATE
    
    -- Step 2: Insert new versions (changed + new customers)
    INSERT INTO gold.dim_customer 
        (customer_id, customer_name, email, city, state, segment, hash_diff,
         effective_start, effective_end, is_current)
    SELECT 
        s.customer_id,
        s.name,
        s.email,
        s.city,
        s.state,
        s.segment,
        MD5(CONCAT_WS('||', s.name, s.email, s.city, s.state, s.segment)),
        CURRENT_TIMESTAMP(),
        '9999-12-31'::TIMESTAMP,
        TRUE
    FROM raw.customers_stream s
    WHERE s.metadata$action = 'INSERT'
      AND (
          -- Changed: hash differs from current
          EXISTS (
              SELECT 1 FROM gold.dim_customer d
              WHERE d.customer_id = s.customer_id
                AND d.is_current = FALSE
                AND d.effective_end = CURRENT_TIMESTAMP()
          )
          OR
          -- New: no existing record
          NOT EXISTS (
              SELECT 1 FROM gold.dim_customer d
              WHERE d.customer_id = s.customer_id
          )
      );
END;

ALTER TASK gold.process_scd_customer RESUME;
```

---

## Example 3: Mixed SCD Types in One Dimension

Real dimensions use DIFFERENT SCD types for different columns:

```sql
CREATE TABLE gold.dim_customer (
    customer_key        INT PRIMARY KEY,
    customer_id         VARCHAR(20),
    
    -- Type 1 columns (overwrite, no history needed):
    customer_name       VARCHAR(200),      -- Name corrections don't need history
    phone               VARCHAR(50),       -- Old phone is irrelevant
    
    -- Type 2 columns (full history tracked):
    city                VARCHAR(100),      -- "Revenue by region at time of purchase"
    state               VARCHAR(50),
    segment             VARCHAR(20),       -- "Revenue by segment at time of purchase"
    
    -- Type 6 columns (current value on ALL rows):
    current_city        VARCHAR(100),      -- Always shows where customer IS NOW
    current_segment     VARCHAR(20),       -- Always shows current classification
    
    -- SCD metadata:
    effective_start     DATE,
    effective_end       DATE DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    hash_type2          VARCHAR(32)        -- Hash of Type 2 columns only!
);

-- Load logic:
-- 1. Check hash of Type 2 columns (city, state, segment)
--    → If changed: expire old row, insert new row
-- 2. ALWAYS update Type 1 columns on current row (name, phone)
-- 3. ALWAYS update Type 6 columns on ALL rows (current_city, current_segment)

-- Type 1 update (always, even if no Type 2 change):
UPDATE gold.dim_customer d
SET customer_name = s.name,
    phone = s.phone
FROM staging_customers s
WHERE d.customer_id = s.customer_id AND d.is_current = TRUE;

-- Type 6 update (after Type 2 insert, update current_ on ALL versions):
UPDATE gold.dim_customer
SET current_city = (SELECT city FROM staging_customers WHERE customer_id = dim.customer_id),
    current_segment = (SELECT segment FROM staging_customers WHERE customer_id = dim.customer_id)
WHERE customer_id IN (SELECT customer_id FROM staging_customers);
```

---

## Example 4: Performance-Optimized SCD Queries

```sql
-- COMMON PATTERN: "Show revenue by customer's segment AT TIME OF PURCHASE"
-- This is WHY we do SCD Type 2!

-- Fast query (point-in-time join via fact's customer_key):
SELECT 
    dc.segment AS segment_at_purchase,    -- Historical value!
    dd.year,
    SUM(f.revenue) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_customer dc ON f.customer_key = dc.customer_key  -- Direct join!
JOIN gold.dim_date dd ON f.date_key = dd.date_key
GROUP BY dc.segment, dd.year;
-- This works because fact_sales.customer_key already points to the correct VERSION

-- ALTERNATIVE: "Show revenue by customer's CURRENT segment"
-- (Reattribute historical sales to where customer is NOW)
SELECT 
    dc.current_segment,                    -- Type 6 column (always current!)
    dd.year,
    SUM(f.revenue) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_customer dc ON f.customer_key = dc.customer_key
JOIN gold.dim_date dd ON f.date_key = dd.date_key
GROUP BY dc.current_segment, dd.year;
-- Works because current_segment is updated on ALL rows (Type 6!)

-- COMPARISON: Both in one query
SELECT 
    dc.segment AS segment_at_purchase,
    dc.current_segment AS segment_now,
    SUM(f.revenue) AS revenue,
    CASE WHEN dc.segment != dc.current_segment THEN 'MIGRATED' ELSE 'SAME' END AS migration_flag
FROM gold.fact_sales f
JOIN gold.dim_customer dc ON f.customer_key = dc.customer_key
GROUP BY dc.segment, dc.current_segment;
-- Shows revenue attribution from BOTH perspectives!
```

---

## Interview Tips

> **Tip 1:** "How do you implement SCD in dbt?" — Use `dbt snapshot` with `strategy='check'` and specify the columns to track. Run snapshots before dimension models. The dimension model reads from the snapshot table (using dbt_valid_from/to for effective dates). Point-in-time fact join uses `AND order_date BETWEEN effective_start AND effective_end`.

> **Tip 2:** "How do you handle mixed SCD types in one dimension?" — Common in production. Hash only the Type 2 columns for change detection. Type 1 columns (name, phone) are always overwritten on the current row. Type 2 columns (city, segment) trigger new version. Type 6 columns (current_city) are updated across ALL rows after any Type 2 change. Keep them all in one table — separate the processing logic.

> **Tip 3:** "How do you do near-real-time SCD?" — Snowflake: Streams (CDC capture) + Tasks (scheduled every 1-5 min). Databricks: Structured Streaming + Delta Lake MERGE. Both: compute hash_diff in the streaming layer, MERGE into SCD table (expire matched + insert changed). Key: stream processing must be idempotent (re-runnable without duplicates).
