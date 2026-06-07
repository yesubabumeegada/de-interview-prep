---
title: "Databricks SQL - Intermediate"
topic: databricks
subtopic: databricks-sql
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, sql, warehouse, optimization, materialized-views, query-profile]
---

# Databricks SQL — Intermediate

## Query Performance Optimization

### Query Profile (Execution Plan Analysis)

```sql
-- View the query plan to identify bottlenecks
EXPLAIN COST
SELECT c.region, COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.region;

-- Key things to look for in the plan:
-- 1. "Scan" nodes: how many files/bytes read? (less = better, data skipping working)
-- 2. "BroadcastHashJoin" vs "SortMergeJoin": broadcast = faster for small tables
-- 3. "Filter" pushed down: filters applied during scan (not after reading all data)
-- 4. Partition pruning: "partitions read: 90 of 365" (only reads relevant partitions)
```

### Result Caching

```sql
-- DBSQL caches query results automatically
-- If the same query runs again and underlying data hasn't changed → instant result!

-- Cache behavior:
-- 1. Query executes → result stored in cache
-- 2. Same query (exact match) runs again → returns cached result (0 compute!)
-- 3. Underlying Delta table gets new commits → cache invalidated → re-executes

-- For dashboards that refresh every 5 minutes:
-- If data updates hourly → 11 out of 12 refreshes serve from cache (free!)

-- Disable for testing (force re-execution):
SET use_cached_result = false;
SELECT ... ;
```

### Materialized Views

```sql
-- Materialized views pre-compute expensive aggregations
-- Stored as Delta tables, automatically refreshed incrementally

CREATE MATERIALIZED VIEW production.gold.mv_daily_revenue AS
SELECT 
    order_date,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue,
    AVG(amount) AS avg_order_value
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
GROUP BY order_date, region;

-- Refresh (only processes new data, not full recompute):
REFRESH MATERIALIZED VIEW production.gold.mv_daily_revenue;

-- Queries automatically use the MV (transparent query rewriting):
SELECT region, SUM(revenue) FROM production.gold.mv_daily_revenue WHERE order_date >= '2024-03-01';
-- Returns instantly (pre-computed!) even though the original query would take minutes

-- Schedule refresh via Workflows (every hour):
-- The MV stays fresh without manual management
```

---

## Advanced SQL Features

### PIVOT and UNPIVOT

```sql
-- PIVOT: rows to columns
SELECT * FROM (
    SELECT region, product_category, amount
    FROM production.silver.orders
    WHERE order_date >= '2024-01-01'
)
PIVOT (
    SUM(amount) AS revenue
    FOR product_category IN ('Electronics', 'Clothing', 'Food', 'Books')
);
-- Result: region | Electronics_revenue | Clothing_revenue | Food_revenue | Books_revenue

-- UNPIVOT: columns to rows
SELECT region, category, revenue FROM production.gold.revenue_wide
UNPIVOT (revenue FOR category IN (electronics_revenue, clothing_revenue, food_revenue));
```

### QUALIFY (Filter Window Functions)

```sql
-- QUALIFY: filter on window function results (cleaner than subquery)
-- Find top 3 products by revenue per category:
SELECT product_name, category, revenue
FROM production.gold.product_metrics
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) <= 3;
-- No CTE or subquery needed! QUALIFY filters after window functions execute
```

### Parameterized Queries

```sql
-- Dashboard parameters (user selects from dropdown):
SELECT *
FROM production.gold.daily_revenue
WHERE order_date BETWEEN :start_date AND :end_date
  AND region = :region;

-- :start_date, :end_date, :region are dashboard parameters
-- Users select values in the dashboard UI → query re-runs with their filters
```

---

## SQL Warehouse Sizing Guide

| Size | vCPUs | Memory | Concurrent Queries | Use Case |
|------|-------|--------|-------------------|----------|
| 2X-Small | 4 | 16 GB | 5-10 | Light ad-hoc |
| Small | 8 | 32 GB | 10-20 | Analyst exploration |
| Medium | 16 | 64 GB | 20-40 | Dashboard refresh |
| Large | 32 | 128 GB | 40-60 | Heavy reporting |
| 2X-Large | 64 | 256 GB | 60-100 | Data warehouse workloads |

```python
# Multi-cluster scaling for high concurrency:
{
    "size": "Medium",              # Each cluster = Medium
    "max_num_clusters": 5,         # Scale to 5 clusters = 5x Medium concurrency
    # Total: handles 100-200 concurrent queries during peak
    # Auto-scales: 1 cluster quiet → 5 clusters during dashboard storm → back to 1
}
```

---

## Query Federation (Lakehouse Federation)

Query external databases directly from DBSQL without moving data:

```sql
-- Create connection to external PostgreSQL
CREATE CONNECTION pg_connection
TYPE POSTGRESQL
OPTIONS (
    host 'prod-db.us-east-1.rds.amazonaws.com',
    port '5432',
    user secret('federation', 'pg_user'),
    password secret('federation', 'pg_password')
);

-- Create foreign catalog
CREATE FOREIGN CATALOG pg_catalog USING CONNECTION pg_connection;

-- Query external data alongside Delta tables!
SELECT 
    d.order_id,
    d.amount,
    pg.customer_name  -- From PostgreSQL!
FROM production.silver.orders d
JOIN pg_catalog.public.customers pg ON d.customer_id = pg.id
WHERE d.order_date = CURRENT_DATE;
-- Databricks optimizes: pushes filters to PostgreSQL, only fetches needed rows
```

---

## SQL Alerts and Monitoring

```sql
-- Alert: revenue dropped below threshold
-- Create in UI: Alerts → New Alert

-- Alert query:
SELECT 
    COALESCE(SUM(amount), 0) AS today_revenue,
    50000 AS threshold
FROM production.sales.orders
WHERE order_date = CURRENT_DATE;

-- Alert configuration:
-- Condition: today_revenue < threshold
-- Notify: #revenue-ops Slack channel
-- Frequency: every 30 minutes
-- Mute: after first trigger (don't spam)

-- Alert: data freshness SLA breach
SELECT 
    TIMESTAMPDIFF(MINUTE, MAX(_loaded_at), CURRENT_TIMESTAMP) AS minutes_stale
FROM production.silver.orders;
-- Condition: minutes_stale > 120 (data older than 2 hours)
```

---

## Query History and Governance

```sql
-- View query history (who ran what, how long, how expensive)
SELECT 
    user_name,
    query_text,
    start_time,
    duration / 1000 AS duration_seconds,
    rows_produced,
    bytes_read / 1024 / 1024 AS mb_read,
    warehouse_id
FROM system.query.history
WHERE start_time >= CURRENT_DATE - 7
ORDER BY duration DESC
LIMIT 20;
-- Find: slowest queries, heaviest users, most expensive queries

-- Identify queries that would benefit from materialized views:
SELECT query_text, COUNT(*) AS run_count, AVG(duration) AS avg_ms
FROM system.query.history
WHERE start_time >= CURRENT_DATE - 30
GROUP BY query_text
HAVING run_count > 100 AND avg_ms > 5000;
-- These queries run frequently and are slow → create materialized views!
```

---

## Interview Tips

> **Tip 1:** "How do you optimize SQL warehouse queries?" — Layer approach: (1) Table optimization (Z-ORDER, partitioning, OPTIMIZE), (2) Query design (filter early, select specific columns, avoid SELECT *), (3) Materialized views for repeated expensive queries, (4) Result caching (automatic for identical queries), (5) Right-size the warehouse (don't over-provision).

> **Tip 2:** "What are materialized views in DBSQL?" — Pre-computed query results stored as Delta tables. They refresh incrementally (only process new data). The query optimizer automatically rewrites queries to use MVs when beneficial (transparent to users). Perfect for: dashboard queries that aggregate large tables repeatedly.

> **Tip 3:** "How do you handle 200 analysts querying simultaneously?" — Multi-cluster SQL Warehouse: set max_num_clusters=5-10. Each cluster handles ~20-40 concurrent queries. Auto-scales during peak (morning dashboard refresh) and scales back during quiet periods. Serverless warehouses handle this automatically with zero configuration.
