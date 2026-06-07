---
title: "Databricks SQL - Real-World Production Examples"
topic: databricks
subtopic: databricks-sql
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, sql, warehouse, production, bi, dashboards]
---

# Databricks SQL — Real-World Production Examples

## Pattern 1: Self-Service Analytics Platform

```sql
-- Gold layer tables designed for analyst consumption

-- Table 1: Orders fact (analysts' primary table)
CREATE TABLE production.gold.fact_orders AS
SELECT 
    o.order_id,
    o.order_date,
    o.amount,
    o.quantity,
    c.customer_name,
    c.region,
    c.segment,
    p.product_name,
    p.category,
    p.brand
FROM production.silver.orders o
LEFT JOIN production.silver.customers c ON o.customer_id = c.customer_id
LEFT JOIN production.silver.products p ON o.product_id = p.product_id;

-- Optimize for analyst query patterns:
OPTIMIZE production.gold.fact_orders ZORDER BY (order_date, region, category);

-- Table 2: Pre-aggregated metrics (fast dashboard queries)
CREATE MATERIALIZED VIEW production.gold.mv_daily_metrics AS
SELECT 
    order_date,
    region,
    category,
    segment,
    COUNT(*) AS orders,
    SUM(amount) AS revenue,
    AVG(amount) AS aov,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM production.gold.fact_orders
GROUP BY order_date, region, category, segment;

-- Analysts query the MV (instant, pre-computed):
SELECT region, SUM(revenue) AS monthly_revenue
FROM production.gold.mv_daily_metrics
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY region;
-- Result returned in <1 second (from pre-aggregated MV)
```

---

## Pattern 2: Dashboard Architecture

```python
# Dashboard refresh strategy for 50 dashboards, 200 widgets

DASHBOARD_STRATEGY = {
    "tier_1_realtime": {
        "dashboards": ["CEO metrics", "ops monitoring"],
        "refresh": "Every 1 minute",
        "warehouse": "dashboard-medium (always-on)",
        "optimization": "Materialized views for all widgets",
        "cost": "$800/month",
    },
    "tier_2_frequent": {
        "dashboards": ["sales dashboard", "marketing funnel", "product analytics"],
        "refresh": "Every 15 minutes",
        "warehouse": "dashboard-medium (auto-stop 10 min)",
        "optimization": "Result caching (most refreshes hit cache)",
        "cost": "$500/month",
    },
    "tier_3_daily": {
        "dashboards": ["weekly reports", "finance summary", "HR analytics"],
        "refresh": "Daily at 7 AM",
        "warehouse": "dashboard-small (auto-stop 5 min)",
        "optimization": "Scheduled query refreshes once per day",
        "cost": "$100/month",
    },
}

# Key optimization: stagger dashboard refreshes
# DON'T refresh all 50 dashboards at the same time (query storm)
# Stagger: 5 dashboards every 3 minutes = smooth load
```

---

## Pattern 3: Data Quality Alerting

```sql
-- Set up SQL alerts for data quality monitoring

-- Alert 1: No data loaded today (pipeline failure)
-- Query:
SELECT 
    COUNT(*) AS rows_today,
    CASE WHEN COUNT(*) = 0 THEN 'ALERT' ELSE 'OK' END AS status
FROM production.silver.orders
WHERE _loaded_at >= CURRENT_DATE;
-- Trigger: rows_today = 0
-- Action: Slack #data-pipeline-alerts
-- Schedule: every 30 minutes starting 7 AM

-- Alert 2: Revenue anomaly (unusual drop)
-- Query:
SELECT 
    today.revenue AS today_revenue,
    avg_7d.avg_revenue AS week_avg,
    (today.revenue - avg_7d.avg_revenue) / avg_7d.avg_revenue * 100 AS pct_diff
FROM (
    SELECT SUM(amount) AS revenue FROM production.gold.fact_orders WHERE order_date = CURRENT_DATE
) today
CROSS JOIN (
    SELECT AVG(daily_revenue) AS avg_revenue FROM (
        SELECT order_date, SUM(amount) AS daily_revenue 
        FROM production.gold.fact_orders 
        WHERE order_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 1
        GROUP BY order_date
    )
) avg_7d;
-- Trigger: pct_diff < -30 (revenue dropped >30% vs 7-day average)
-- Action: PagerDuty alert to data-eng oncall

-- Alert 3: Null rate spike
-- Query:
SELECT 
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_pct
FROM production.silver.orders
WHERE _loaded_at >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR;
-- Trigger: null_pct > 5
-- Action: Slack #data-quality
```

---

## Pattern 4: Lakehouse Federation

```sql
-- Query across Delta Lake + external databases without ETL

-- Connect to operational PostgreSQL
CREATE CONNECTION operational_db TYPE POSTGRESQL
OPTIONS (host 'prod-db.internal', port '5432', 
         user secret('connections', 'pg_user'), 
         password secret('connections', 'pg_password'));

CREATE FOREIGN CATALOG operational USING CONNECTION operational_db;

-- Join lakehouse data with live operational data:
-- "What's the current status of orders placed this week?"
SELECT 
    d.order_id,
    d.customer_name,
    d.amount,
    d.order_date,
    op.current_status,        -- Live from PostgreSQL!
    op.last_updated           -- Real-time status
FROM production.gold.fact_orders d
JOIN operational.public.order_status op ON d.order_id = op.order_id
WHERE d.order_date >= CURRENT_DATE - 7
  AND op.current_status IN ('pending', 'processing');

-- Benefits:
-- No ETL needed for real-time data (query it live)
-- Historical data from lakehouse + current state from operational DB
-- Filter pushdown: WHERE clause sent to PostgreSQL (only fetches needed rows)
```

---

## Pattern 5: Cost-Optimized Warehouse Strategy

```python
# Before: Single large warehouse, always on = $8K/month
# After: Right-sized, multi-warehouse = $3K/month

OPTIMIZED_SETUP = {
    "before": {
        "warehouse": "one-size-fits-all",
        "size": "Large",
        "mode": "Classic, always-on",
        "cost": "$8,000/month",
        "issues": [
            "Paying for idle time (evenings, weekends)",
            "Analysts and dashboards compete for resources",
            "Over-provisioned for 80% of queries",
        ]
    },
    "after": {
        "warehouses": [
            {
                "name": "analyst-adhoc",
                "type": "Serverless",
                "size": "Small",
                "auto_stop": "5 min",
                "cost": "$600/month (pay per query)",
            },
            {
                "name": "dashboard-refresh",
                "type": "Serverless",
                "size": "Medium",
                "auto_stop": "10 min",
                "cost": "$1,200/month (scheduled refreshes only)",
            },
            {
                "name": "heavy-reports",
                "type": "Pro",
                "size": "Large",
                "auto_stop": "10 min",
                "cost": "$800/month (runs only during report generation)",
            },
            {
                "name": "api-serving",
                "type": "Pro",
                "size": "Small",
                "auto_stop": "Never (low latency SLA)",
                "cost": "$400/month (small, always-on for API)",
            },
        ],
        "total_cost": "$3,000/month",
        "savings": "$5,000/month (63% reduction)",
    },
}
```

---

## Pattern 6: Query Performance Debugging

```sql
-- Debug a slow query (took 45 seconds, should be <5 seconds)

-- Step 1: Check query profile
EXPLAIN EXTENDED
SELECT c.region, SUM(o.amount) AS revenue
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_date = '2024-03-15'
GROUP BY c.region;

-- Step 2: Look for issues in the plan:
-- ❌ "Scan: files_read=1000, files_total=1000" → NO data skipping! Need Z-ORDER
-- ❌ "SortMergeJoin" on customers (50M rows) → should broadcast (small table)
-- ❌ No partition pruning → table not partitioned by order_date

-- Step 3: Apply fixes:
-- Fix 1: Z-ORDER for data skipping
OPTIMIZE production.silver.orders ZORDER BY (order_date, customer_id);

-- Fix 2: Reduce customers table or let optimizer broadcast it
ANALYZE TABLE production.silver.customers COMPUTE STATISTICS;
-- If < 100 MB after stats, optimizer will auto-broadcast

-- Fix 3: Verify improvement
-- Re-run query → check profile: "files_read=3, files_total=1000" ← 99.7% skipped!
-- Duration: 45s → 0.8s
```

---

## Interview Tips

> **Tip 1:** "How do you design gold tables for analysts?" — Denormalized, pre-joined facts with dimensions (no complex joins for analysts). Descriptive column names (not IDs). Materialized views for common aggregation patterns. Z-ORDER on commonly filtered columns. Add table/column comments for discoverability. Goal: analysts write simple SELECT...WHERE...GROUP BY, not 5-table joins.

> **Tip 2:** "How do you handle dashboard performance at scale?" — Tier dashboards by refresh frequency. Use materialized views for all dashboard queries (pre-computed aggregations). Stagger refresh schedules (avoid query storms). Separate warehouses for dashboards vs ad-hoc (isolation). Result caching handles repeated identical queries automatically.

> **Tip 3:** "How do you debug a slow SQL query in DBSQL?" — Query Profile shows: files scanned (data skipping effectiveness), join type (broadcast vs sort-merge), memory spill, and time per operator. Common fixes: Z-ORDER + OPTIMIZE (data skipping), ANALYZE TABLE (better statistics for join planning), materialized views (pre-compute expensive parts), or reduce data with better filters.
