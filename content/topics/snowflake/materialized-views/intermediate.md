---
title: "Materialized Views - Intermediate"
topic: snowflake
subtopic: materialized-views
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, materialized-views, clustering, optimization, refresh]
---

# Snowflake Materialized Views — Intermediate

## Clustering Materialized Views

```sql
-- MVs can have their own clustering (independent of source table):
CREATE MATERIALIZED VIEW gold.orders_by_date
    CLUSTER BY (order_date)  -- MV clustered by date for fast date-range queries
AS
    SELECT order_date, region, product_category,
           SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders
    GROUP BY order_date, region, product_category;

-- Queries filtering by date → excellent data skipping on the MV!
SELECT * FROM gold.orders_by_date WHERE order_date BETWEEN '2024-03-01' AND '2024-03-31';
-- Scans only March partitions (skips 11/12 months of data)
```

---

## Refresh Behavior

```sql
-- MVs refresh AUTOMATICALLY in the background (serverless):
-- - Triggered when source table changes (DML on source)
-- - Incremental: only processes changed micro-partitions
-- - No warehouse needed (uses Snowflake serverless compute)
-- - Typically completes within seconds to minutes

-- You CANNOT manually trigger a refresh (it's fully automated)
-- You CANNOT control refresh frequency (Snowflake decides)

-- Suspend/Resume MV refresh:
ALTER MATERIALIZED VIEW gold.revenue_by_region SUSPEND;
-- MV stops refreshing (becomes stale, but still queryable!)
-- Queries still work, but results may be outdated

ALTER MATERIALIZED VIEW gold.revenue_by_region RESUME;
-- Resumes background refresh (catches up with source changes)

-- Check staleness:
SHOW MATERIALIZED VIEWS LIKE 'revenue_by_region';
-- is_stale column: TRUE if MV is behind source
-- behind_by column: shows how many transactions behind
```

---

## Query Rewriting Deep Dive

```sql
-- The optimizer rewrites queries to use MVs when:
-- 1. The MV's grouping columns are a SUPERSET of the query's GROUP BY
-- 2. The query's aggregates can be derived from MV's aggregates
-- 3. The query's WHERE can be applied to the MV's result

-- Example MV:
CREATE MATERIALIZED VIEW mv_monthly_revenue AS
    SELECT DATE_TRUNC('month', order_date) AS month, region, product_category,
           SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders
    GROUP BY month, region, product_category;

-- These queries ALL benefit from the MV (automatic rewriting):
-- Q1: exact match
SELECT month, region, SUM(revenue) FROM mv_monthly_revenue GROUP BY month, region;

-- Q2: subset of dimensions (MV has region+category, query only uses region)
SELECT region, SUM(amount) FROM silver.orders GROUP BY region;
-- Optimizer: read from MV, sum across categories → faster than scanning source!

-- Q3: with filter (applied to MV result)
SELECT region, SUM(amount) FROM silver.orders 
WHERE order_date >= '2024-01-01' GROUP BY region;
-- Optimizer: filter MV on month >= '2024-01', then sum

-- This query CANNOT use the MV (non-matching aggregate):
SELECT region, MEDIAN(amount) FROM silver.orders GROUP BY region;
-- MEDIAN can't be derived from SUM/COUNT → must scan source
```

---

## MV Maintenance Costs

```sql
-- Check MV refresh credit usage:
SELECT 
    MATERIALIZED_VIEW_NAME,
    SUM(CREDITS_USED) AS total_credits_7d,
    COUNT(*) AS refresh_count_7d
FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
GROUP BY MATERIALIZED_VIEW_NAME
ORDER BY total_credits_7d DESC;

-- If refresh cost exceeds query savings: DROP the MV!
-- Rule: MV is worth it if it saves more in query compute than it costs in refresh

-- High-refresh-cost scenarios:
-- Source table updated every second (streaming inserts) → constant MV refresh
-- Very large MV result (full table materialized) → expensive to maintain
-- Source has high churn (UPDATEs changing every row) → full MV rebuild per change

-- Low-refresh-cost scenarios (ideal for MVs):
-- Source updated hourly/daily (infrequent refresh)
-- MV result is small (aggregation of large table → tiny result)
-- Source is append-only (new rows → incremental MV update)
```

---

## Secure Materialized Views

```sql
-- Combine MV performance with data sharing security:
CREATE SECURE MATERIALIZED VIEW shared.partner_metrics AS
    SELECT region, DATE_TRUNC('month', order_date) AS month,
           SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders
    WHERE partner_id = 'PARTNER_A'  -- Row filter
    GROUP BY region, month;

-- Benefits:
-- 1. Fast reads (pre-computed)
-- 2. Secure (definition hidden from consumers)
-- 3. Shareable (can grant to a SHARE object)
-- 4. Auto-maintained (refreshes when source changes)

-- Use in Data Sharing:
GRANT SELECT ON SECURE MATERIALIZED VIEW shared.partner_metrics TO SHARE partner_share;
```

---

## Interview Tips

> **Tip 1:** "How does MV query rewriting work?" — The optimizer checks if your query's GROUP BY and aggregates can be answered from an existing MV (without scanning the source). If yes: transparently reads from the MV (pre-computed, fast). Users don't need to know the MV exists — optimization is automatic.

> **Tip 2:** "When is an MV too expensive to maintain?" — When source changes are extremely frequent (streaming inserts every second), or when the MV is large and changes require near-full rebuild. Check: MATERIALIZED_VIEW_REFRESH_HISTORY for credit usage. If refresh credits > query savings: drop the MV and use a regular view or Dynamic Table instead.

> **Tip 3:** "MV refresh model vs Dynamic Table refresh?" — MV: fully automatic serverless refresh (you can't control timing), incremental, no warehouse needed. DT: warehouse-based refresh, controlled by TARGET_LAG (you set freshness), supports JOINs. MV for: simple aggregation acceleration. DT for: complex transformations with controlled freshness.
