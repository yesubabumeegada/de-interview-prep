---
title: "Materialized Views - Real-World Production Examples"
topic: snowflake
subtopic: materialized-views
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, materialized-views, production, dashboards, optimization]
---

# Snowflake Materialized Views — Real-World Production Examples

## Pattern 1: Dashboard Acceleration

```sql
-- Problem: CEO dashboard queries 2TB orders table 50x/day (each scan: 30s, $0.50)
-- Solution: MV pre-aggregates → queries finish in <1s

CREATE MATERIALIZED VIEW gold.mv_executive_metrics
    CLUSTER BY (metric_date)
AS
    SELECT 
        order_date AS metric_date,
        region,
        product_category,
        COUNT(*) AS total_orders,
        SUM(amount) AS revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,
        AVG(amount) AS avg_order_value,
        SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END) AS returns
    FROM silver.orders
    GROUP BY order_date, region, product_category;

-- Dashboard queries (all served from MV automatically):
-- Widget 1: "Total revenue this month"
SELECT SUM(revenue) FROM silver.orders WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE());
-- Rewritten to: SELECT SUM(revenue) FROM gold.mv_executive_metrics WHERE metric_date >= ...

-- Widget 2: "Revenue by region"
SELECT region, SUM(revenue) FROM silver.orders WHERE order_date = CURRENT_DATE() GROUP BY region;
-- Rewritten to: SELECT region, SUM(revenue) FROM mv WHERE metric_date = CURRENT_DATE() ...

-- Cost impact:
-- Before MV: 50 queries × $0.50 = $25/day = $750/month
-- After MV: 50 queries × $0.001 + MV refresh $0.50/day = $0.55/day = $16.50/month
-- Savings: $733.50/month (98% reduction!) for ONE dashboard
```

---

## Pattern 2: Multi-MV Strategy

```sql
-- Different MVs for different user groups:

-- MV for executives (daily aggregates by region):
CREATE MATERIALIZED VIEW gold.mv_exec_daily 
    CLUSTER BY (order_date) AS
    SELECT order_date, region, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders GROUP BY order_date, region;

-- MV for product team (category performance):
CREATE MATERIALIZED VIEW gold.mv_product_performance
    CLUSTER BY (product_category) AS
    SELECT product_category, DATE_TRUNC('week', order_date) AS week,
           SUM(amount) AS revenue, COUNT(DISTINCT customer_id) AS buyers,
           AVG(amount) AS avg_price
    FROM silver.orders GROUP BY product_category, week;

-- MV for operations (order volume monitoring):
CREATE MATERIALIZED VIEW gold.mv_hourly_volume AS
    SELECT DATE_TRUNC('hour', created_at) AS hour, status,
           COUNT(*) AS order_count, SUM(amount) AS volume
    FROM silver.orders
    WHERE created_at >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY hour, status;

-- Each team queries silver.orders naturally — optimizer picks the right MV!
```

---

## Pattern 3: Search Optimization + MV Combo

```sql
-- For a table with BOTH aggregation queries AND point lookups:

-- Aggregation queries → Materialized View
CREATE MATERIALIZED VIEW gold.mv_customer_totals AS
    SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total_spent
    FROM silver.orders GROUP BY customer_id;
-- "Total spend for all customers?" → MV answers instantly

-- Point lookups → Search Optimization Service
ALTER TABLE silver.orders ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, order_id);
-- "Find order #12345?" → Search Optimization provides sub-second lookup

-- Together: MV handles aggregations, Search Optimization handles lookups
-- Both work transparently — users just query silver.orders normally
```

---

## Pattern 4: MV for Data Sharing

```sql
-- Share pre-aggregated data with partners (fast + secure):

CREATE SECURE MATERIALIZED VIEW shared.partner_kpis AS
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        product_category,
        SUM(quantity) AS units_sold,
        SUM(amount) AS revenue,
        COUNT(DISTINCT customer_id) AS reach
    FROM silver.orders
    WHERE brand_id = 'PARTNER_BRAND_123'
    GROUP BY month, product_category;

-- Share via Data Sharing:
GRANT SELECT ON SECURE MATERIALIZED VIEW shared.partner_kpis TO SHARE partner_share;

-- Partner benefit:
-- Gets pre-aggregated KPIs (fast, < 1 second queries)
-- Never scans your 2TB orders table (reduced I/O on your storage)
-- SECURE: can't see the aggregation logic or filter conditions
-- Auto-maintained: always shows latest data without partner doing anything
```

---

## Pattern 5: MV Lifecycle Management

```sql
-- Automated MV review: keep useful MVs, drop wasteful ones

CREATE OR REPLACE PROCEDURE ops.mv_cost_benefit_review()
RETURNS TABLE (mv_name VARCHAR, monthly_refresh_cost DECIMAL, recommendation VARCHAR)
LANGUAGE SQL
AS
$$
    WITH mv_costs AS (
        SELECT 
            MATERIALIZED_VIEW_NAME AS mv_name,
            SUM(CREDITS_USED) * 4.3 AS monthly_refresh_cost  -- 7-day × 4.3 = monthly
        FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
            DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
        ))
        GROUP BY MATERIALIZED_VIEW_NAME
    ),
    mv_usage AS (
        -- Check if MV appears in query plans (is it being used?)
        SELECT 
            mv_name,
            monthly_refresh_cost,
            -- Heuristic: if MV exists but queries still scan source → MV not useful
            CASE 
                WHEN monthly_refresh_cost > 50 THEN 'DROP: high cost, review if needed'
                WHEN monthly_refresh_cost > 10 THEN 'REVIEW: moderate cost'
                ELSE 'KEEP: low cost'
            END AS recommendation
        FROM mv_costs
    )
    SELECT * FROM mv_usage ORDER BY monthly_refresh_cost DESC;
$$;

-- Run monthly:
CALL ops.mv_cost_benefit_review();
-- Drop MVs that cost > $50/month in refresh with no proven query benefit
```

---

## Interview Tips

> **Tip 1:** "How do you use MVs for dashboard acceleration?" — Identify the dashboard's most expensive queries (QUERY_HISTORY). Create MVs that pre-aggregate the results. The optimizer automatically rewrites dashboard queries to use MVs (transparent). Typical result: 30-second queries → <1 second, 90-98% cost reduction for that dashboard.

> **Tip 2:** "How do you manage MV costs in production?" — Monitor: MATERIALIZED_VIEW_REFRESH_HISTORY for credit usage per MV. Compare: refresh cost vs query savings. Review: monthly, drop MVs that cost more than they save. Strategy: create MVs for high-frequency queries (>50x/month), not one-off reports.

> **Tip 3:** "MV + Data Sharing — how do they work together?" — SECURE MATERIALIZED VIEW: pre-computed, fast for partner queries, doesn't expose raw data. Partners query the MV (small, fast) instead of triggering full table scans on your storage. Benefits: faster partner experience + less I/O on your storage + secure (logic hidden).
