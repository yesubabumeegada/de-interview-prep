---
title: "Data Warehouse Design — Scenarios"
topic: system-design
subtopic: data-warehouse-design
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, data-warehouse, interview, scenarios, star-schema]
---

# Data Warehouse Design — Interview Scenarios

## Scenario 1: Design a DW for an E-Commerce Company

**Question:** Design a data warehouse for an e-commerce company. They need to answer: revenue by region/category/channel, customer lifetime value, inventory turnover, and return rates. Source: PostgreSQL (orders, inventory), Shopify (transactions), Salesforce (CRM).

**Answer:**

```sql
-- Conformed Dimensions (shared across all fact tables):

-- dim_date (always first)
CREATE TABLE dim_date (
  date_key     INT PRIMARY KEY,  -- YYYYMMDD integer
  full_date    DATE,
  year         INT, quarter INT, month_number INT, month_name VARCHAR(10),
  week_number  INT, day_of_week VARCHAR(10), is_weekend BOOLEAN, is_holiday BOOLEAN
);

-- dim_customer (SCD Type 2 — region/segment can change)
CREATE TABLE dim_customer (
  customer_key  BIGINT PRIMARY KEY,
  customer_id   VARCHAR(50),          -- natural key
  customer_name VARCHAR(200),
  email_hashed  CHAR(64),             -- SHA256 (PII masked in DW)
  region        VARCHAR(50),
  country       VARCHAR(50),
  segment       VARCHAR(20),          -- new/returning/vip
  acquired_channel VARCHAR(50),
  start_date    DATE, end_date DATE, is_current BOOLEAN
);

-- dim_product (SCD Type 1 — category changes OK to overwrite)
CREATE TABLE dim_product (
  product_key   INT PRIMARY KEY,
  product_id    VARCHAR(50),
  product_name  VARCHAR(200),
  category      VARCHAR(50),
  subcategory   VARCHAR(50),
  brand         VARCHAR(100),
  cost_usd      DECIMAL(10,2),
  is_active     BOOLEAN
);

-- Fact tables:

-- fct_orders (grain: one order line item)
CREATE TABLE fct_orders (
  order_line_key  BIGINT PRIMARY KEY,
  date_key        INT REFERENCES dim_date,
  customer_key    BIGINT REFERENCES dim_customer,
  product_key     INT REFERENCES dim_product,
  channel_key     INT REFERENCES dim_channel,
  -- Measures:
  quantity        INT,
  unit_price      DECIMAL(10,2),
  unit_cost       DECIMAL(10,2),
  gross_amount    DECIMAL(12,2),
  discount_amount DECIMAL(10,2),
  net_amount      DECIMAL(12,2),
  is_return       BOOLEAN
);

-- fct_inventory (grain: product × date snapshot)
CREATE TABLE fct_inventory (
  date_key         INT REFERENCES dim_date,
  product_key      INT REFERENCES dim_product,
  warehouse_key    INT REFERENCES dim_warehouse,
  units_on_hand    INT,
  units_sold_day   INT,
  units_received   INT,
  days_of_supply   DECIMAL(6,1)  -- = units_on_hand / avg_daily_sales
);

-- Answers each question:
-- Revenue by region: fct_orders JOIN dim_customer (region) GROUP BY region
-- Revenue by category: fct_orders JOIN dim_product (category) GROUP BY category
-- CLV: SUM(net_amount) GROUP BY customer_key (all time)
-- Inventory turnover: SUM(units_sold) / AVG(units_on_hand) from fct_inventory
-- Return rate: SUM(CASE WHEN is_return THEN 1 ELSE 0 END) / COUNT(*) from fct_orders
```

---

## Scenario 2: Performance Problem — DW Queries Are Slow

**Question:** The Snowflake DW `fct_orders` table has 5 billion rows. BI queries take 20+ minutes. The table is not partitioned or clustered. What do you do?

**Answer:**

```sql
-- Step 1: Diagnose — find the worst queries
SELECT query_text,
       total_elapsed_time/1000 AS elapsed_sec,
       partitions_scanned, partitions_total,
       bytes_scanned/1e9 AS gb_scanned
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%fct_orders%'
  AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY bytes_scanned DESC
LIMIT 20;

-- Result: queries scan 100% of partitions (no pruning)

-- Step 2: Identify most common filters in queries
-- Usually: date range + region or category

-- Step 3: Add clustering key
ALTER TABLE fct_orders CLUSTER BY (TO_DATE(order_date), region);
-- Snowflake auto-clustering: reorganizes data incrementally
-- Cost: ~$50-200 one-time reclustering depending on data size

-- Step 4: Build pre-aggregated summary tables for common patterns
CREATE TABLE summary_orders_daily AS
SELECT
  d.year, d.quarter, d.month_number, TO_DATE(o.order_date) AS order_date,
  c.region, c.segment,
  p.category, p.brand,
  ch.channel_name,
  COUNT(*) AS order_count,
  SUM(o.gross_amount) AS gross_revenue,
  SUM(o.net_amount) AS net_revenue,
  SUM(o.discount_amount) AS total_discount,
  SUM(o.quantity) AS units_sold,
  SUM(o.is_return::INT) AS return_count
FROM fct_orders o
JOIN dim_date d ON o.date_key = d.date_key
JOIN dim_customer c ON o.customer_key = c.customer_key AND c.is_current = TRUE
JOIN dim_product p ON o.product_key = p.product_key
JOIN dim_channel ch ON o.channel_key = ch.channel_key
GROUP BY 1,2,3,4,5,6,7,8,9;

-- Step 5: Point BI tool at summary table
-- Dashboard queries on summary_orders_daily (< 1M rows): < 1 second
-- Power users can still query fct_orders for detailed drill-down
```

---

## Scenario 3: Modeling Customer Lifetime Value

**Question:** The business wants a CLV (Customer Lifetime Value) metric in the DW. How do you design the model?

**Answer:**

```sql
-- CLV = total net revenue per customer over their entire relationship
-- Design: calculate at different granularities and time windows

-- Base: customer revenue summary
CREATE TABLE fct_customer_revenue AS
SELECT
  c.customer_id,
  c.customer_name,
  c.segment,
  c.region,
  c.acquired_channel,
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date,
  DATEDIFF('day', MIN(o.order_date), CURRENT_DATE) AS customer_age_days,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.net_amount) AS total_revenue_lifetime,
  SUM(CASE WHEN o.order_date >= CURRENT_DATE - 365 THEN o.net_amount ELSE 0 END) AS revenue_last_12m,
  SUM(CASE WHEN o.order_date >= CURRENT_DATE - 30 THEN o.net_amount ELSE 0 END) AS revenue_last_30d,
  -- Annualized CLV: revenue / age in years
  ROUND(SUM(o.net_amount) / GREATEST(DATEDIFF('day', MIN(o.order_date), CURRENT_DATE)/365.0, 0.1), 2) AS annualized_clv
FROM dim_customer c
JOIN fct_orders o ON c.customer_key = o.customer_key AND c.is_current = TRUE
GROUP BY c.customer_id, c.customer_name, c.segment, c.region, c.acquired_channel;

-- Segment customers by CLV
SELECT
  customer_id, total_revenue_lifetime,
  NTILE(4) OVER (ORDER BY total_revenue_lifetime DESC) AS clv_quartile
  -- Q1 = top 25% by revenue = "VIP"
  -- Q4 = bottom 25% = "At risk / low value"
FROM fct_customer_revenue;
```
