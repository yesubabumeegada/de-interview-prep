---
title: "Data Warehouse Design — Intermediate"
topic: system-design
subtopic: data-warehouse-design
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, data-warehouse, kimball, inmon, data-vault, olap]
---

# Data Warehouse Design — Intermediate

## Kimball vs Inmon vs Data Vault

| Approach | Philosophy | Structure | Who builds first |
|---|---|---|---|
| **Kimball (bottom-up)** | Build DW as integration of data marts | Star schemas, denormalized | Individual business subject areas |
| **Inmon (top-down)** | Central enterprise DW first, then data marts | Normalized (3NF) EDW, then star schemas for reporting | Enterprise-wide integrated model |
| **Data Vault** | Audit-first, hybrid | Hubs, Links, Satellites (raw, historized) | Flexible, additive, audit trail |

### Kimball (Most Common in Modern DE)
```
Key principles:
  1. Conformed dimensions: dim_date, dim_customer shared across all fact tables
  2. Bus matrix: defines which dimensions apply to which fact tables
  3. Grain: fact table row = one specific event (one sale, one click)
  4. Additive measures: metrics that can be summed (revenue)
     Semi-additive: can sum by some dimensions (account balance — sum by date, not time)
     Non-additive: cannot sum at all (ratios, percentages)
```

### Data Vault
```sql
-- Data Vault components:
-- Hub: unique business keys (one row per entity)
CREATE TABLE hub_customer (
  hub_customer_hk   CHAR(32) PRIMARY KEY,  -- MD5 hash of business key
  customer_id       VARCHAR(20),           -- source business key
  load_dts          TIMESTAMP,
  record_source     VARCHAR(50)
);

-- Link: relationships between hubs (no attributes)
CREATE TABLE link_sale (
  link_sale_hk      CHAR(32) PRIMARY KEY,
  hub_customer_hk   CHAR(32),
  hub_product_hk    CHAR(32),
  hub_date_hk       CHAR(32),
  load_dts          TIMESTAMP,
  record_source     VARCHAR(50)
);

-- Satellite: descriptive attributes + history
CREATE TABLE sat_customer_details (
  hub_customer_hk   CHAR(32),
  load_dts          TIMESTAMP,            -- when this version was loaded
  load_end_dts      TIMESTAMP,            -- NULL = current
  customer_name     VARCHAR(200),
  region            VARCHAR(50),
  email             VARCHAR(200),
  hash_diff         CHAR(32),             -- MD5 of all attributes (change detection)
  record_source     VARCHAR(50),
  PRIMARY KEY (hub_customer_hk, load_dts)
);

-- Data Vault advantages:
-- Auditability: every row traceable to source and load time
-- Flexibility: add new sources without schema changes to existing tables
-- Parallel loading: hubs, links, satellites load independently
-- Disadvantages: complex querying (need business vault / information mart on top)
```

---

## Aggregate Tables and Summary Tables

```sql
-- Problem: reporting queries scan billions of fact rows even for simple aggregations
-- Solution: pre-aggregate into summary tables

-- Daily sales summary (rebuilt nightly from fact_sales)
CREATE TABLE summary_daily_sales AS
SELECT
  d.full_date,
  d.year, d.quarter, d.month_number,
  p.category, p.brand,
  c.region,
  COUNT(DISTINCT f.sale_id)      AS transaction_count,
  COUNT(DISTINCT f.customer_key) AS unique_customers,
  SUM(f.quantity)                AS units_sold,
  SUM(f.total_amount)            AS gross_revenue,
  SUM(f.discount_pct * f.total_amount / 100) AS total_discount
FROM fact_sales f
JOIN dim_date d     ON f.date_key     = d.date_key
JOIN dim_product p  ON f.product_key  = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY d.full_date, d.year, d.quarter, d.month_number,
         p.category, p.brand, c.region;

-- BI tool queries summary table instead of fact table:
-- SELECT region, SUM(gross_revenue) FROM summary_daily_sales
-- WHERE year = 2024 AND quarter = 1 GROUP BY region;
-- ← reads 90 rows (90 days × 1 region) instead of 500M fact rows
```

---

## Slowly Changing Dimensions at Scale

```sql
-- SCD Type 2 loading pattern (dbt-style)

-- Step 1: Identify changed records
WITH source_current AS (
  SELECT * FROM stg_customers  -- today's snapshot from source
),
dw_current AS (
  SELECT * FROM dim_customer WHERE is_current = TRUE
),
changed AS (
  SELECT s.*
  FROM source_current s
  JOIN dw_current d ON s.customer_id = d.customer_id
  WHERE s.region != d.region OR s.segment != d.segment  -- attributes changed
),
new_customers AS (
  SELECT s.*
  FROM source_current s
  LEFT JOIN dw_current d ON s.customer_id = d.customer_id
  WHERE d.customer_id IS NULL  -- not in DW yet
)

-- Step 2: Expire old records
UPDATE dim_customer
SET end_date = CURRENT_DATE - 1, is_current = FALSE
WHERE customer_id IN (SELECT customer_id FROM changed)
  AND is_current = TRUE;

-- Step 3: Insert new versions + new customers
INSERT INTO dim_customer
  (customer_key, customer_id, customer_name, region, segment,
   start_date, end_date, is_current)
SELECT
  NEXTVAL('seq_customer_key'),
  customer_id, customer_name, region, segment,
  CURRENT_DATE, NULL, TRUE
FROM (SELECT * FROM changed UNION ALL SELECT * FROM new_customers);
```

---

## Conformed Dimensions and Bus Matrix

```
Conformed dimensions: shared dimension tables used consistently across all fact tables
  dim_date: used in fact_sales, fact_orders, fact_web_events, fact_support_tickets
  dim_customer: used in fact_sales, fact_support_tickets, fact_subscriptions
  dim_product: used in fact_sales, fact_inventory, fact_returns

Bus Matrix: defines which fact tables use which dimensions
                    dim_date  dim_customer  dim_product  dim_store  dim_channel
  fact_sales           ✓          ✓            ✓           ✓
  fact_orders          ✓          ✓            ✓                       ✓
  fact_web_events      ✓          ✓                                    ✓
  fact_inventory       ✓                       ✓           ✓
  fact_support         ✓          ✓

Benefits:
  Cross-fact analysis: "What % of customers who purchased also had support tickets?"
  JOIN fact_sales s ON s.customer_key = dim_customer.customer_key
  JOIN fact_support t ON t.customer_key = dim_customer.customer_key
  Works because both fact tables use the SAME dim_customer surrogate keys
```

---

## Interview Tips

> **Tip 1:** "Kimball vs Inmon — which would you choose?" — Kimball for most modern DE projects: faster time-to-value (start with one subject area / data mart), easier for BI teams to use (star schemas are intuitive), better fit for cloud DWs. Inmon when: enterprise-wide data integration is the primary goal, you need a single version of truth across many incompatible sources, and you have time/budget for the full EDW model upfront. Data Vault when: audit trail is a hard requirement (financial regulatory compliance), or sources change frequently.

> **Tip 2:** "How would you design a DW for a company with 10 different source systems?" — Use conformed dimensions and a bus matrix. First, identify the shared entities (Customer, Product, Date, Location) and build conformed dimension tables that integrate all 10 sources (one customer_key regardless of which source they came from). Then build fact tables per subject area. Use a Data Vault for the raw/historical layer if auditability is needed; build a Kimball-style presentation layer on top for BI consumption.

> **Tip 3:** "What is the grain of a fact table and why does it matter?" — The grain defines what one row in the fact table represents. Example: grain = one transaction line item (not one order). Choosing the wrong grain: storing one row per order when users need item-level analysis → can't calculate per-product revenue without modifying the DW. Always define the grain before building: "This fact table stores one row per [order line item / customer per day / click event]." The grain must be the lowest atomic level needed for analysis — you can always aggregate up, but can't disaggregate down.
