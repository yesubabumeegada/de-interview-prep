---
title: "Databricks SQL - Scenario Questions"
topic: databricks
subtopic: databricks-sql
content_type: scenario_question
tags: [databricks, sql, warehouse, interview, scenarios]
---

# Scenario Questions — Databricks SQL

<article data-difficulty="junior">

## 🟢 Junior: Choosing Warehouse Size

**Scenario:** Your team has 20 analysts who run ad-hoc queries throughout the day. Peak time is 9-11 AM (all 20 querying). Most queries scan 1-10 GB and return in 5-30 seconds. What warehouse size and configuration do you recommend?

<details>
<summary>💡 Hint</summary>
Consider: concurrent queries at peak (20), query size (small-medium), and cost efficiency. Serverless auto-handles scaling. Size affects per-query speed.
</details>

<details>
<summary>✅ Solution</summary>

```python
# 20 concurrent analysts, queries scanning 1-10 GB, target <30s response

WAREHOUSE_CONFIG = {
    "name": "analyst-adhoc",
    "type": "Serverless",           # Instant startup, auto-scales
    "size": "Small",                # Handles 10-20 concurrent queries well
    "max_num_clusters": 3,          # Scale to 3 during peak (60 concurrent queries capacity)
    "auto_stop_mins": 10,           # Stop after 10 min idle (cost savings)
}

# Why this configuration:
# - Serverless: instant startup (no 3-5 min wait when first analyst arrives at 9 AM)
# - Small: sufficient for 1-10 GB queries (not CPU/memory bound at this size)
# - max_clusters=3: handles peak of 20 analysts (Small handles ~10 concurrent, 3x = 30)
# - auto_stop=10: saves cost during lunch break and evenings
# - Analysts don't notice scaling (serverless adds clusters in seconds)

# Cost estimate:
# Peak (9-11 AM): 3 clusters × 2 hours × Small rate ≈ $12/day
# Normal (11 AM - 5 PM): 1 cluster × 6 hours ≈ $12/day
# Off-peak: warehouse stopped (free)
# Monthly: ~$500-800/month (much less than always-on Large warehouse)
```

**Key Points:**
- Serverless: best for bursty workloads (peak vs quiet periods)
- Small size handles 1-10 GB queries efficiently (don't over-size)
- Multi-cluster scaling handles concurrent users (not warehouse size)
- Auto-stop 10 min: covers brief idle periods without restart penalty (serverless restarts instantly)
- Don't use a Large warehouse just because you have many users — use multi-cluster Small instead

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Creating a Dashboard Alert

**Scenario:** Create an alert that notifies #data-ops Slack channel if no new orders have been loaded into `production.silver.orders` in the last 2 hours (indicating pipeline failure).

<details>
<summary>💡 Hint</summary>
Write a query that checks data freshness (MAX of _loaded_at timestamp). Set an alert condition on the staleness threshold. Schedule to check every 15 minutes.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Alert query: check data freshness
SELECT 
    TIMESTAMPDIFF(MINUTE, MAX(_loaded_at), CURRENT_TIMESTAMP) AS minutes_since_last_load,
    MAX(_loaded_at) AS last_load_time,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_loaded_at), CURRENT_TIMESTAMP) > 120 
        THEN 'STALE - Pipeline may have failed!'
        ELSE 'OK'
    END AS status
FROM production.silver.orders;

-- Alert configuration (in DBSQL UI):
-- Name: "Orders Pipeline Freshness"
-- Trigger condition: minutes_since_last_load > 120
-- Notification: Slack webhook to #data-ops
-- Schedule: every 15 minutes
-- Mute after: 1 hour (don't spam repeatedly for same issue)
```

**Key Points:**
- Check freshness of the TARGET table (not source) — this catches pipeline failures
- 120 minutes = 2 hours threshold (adjust based on your pipeline's normal cadence)
- Schedule alert check every 15 minutes (balance between timely detection and cost)
- Mute after trigger: prevents flood of notifications for the same ongoing issue
- Include both minutes_since_last_load AND last_load_time for debugging context
- Alternative: check row count (0 rows today = also indicates failure)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Result Caching

**Scenario:** Your dashboard has 10 widgets, each running a query every 5 minutes. The underlying data updates every hour. You're spending $2K/month on warehouse compute. How much can result caching save?

<details>
<summary>💡 Hint</summary>
If data updates hourly but queries run every 5 minutes, 11 out of 12 query executions per hour will hit the cache (data hasn't changed). Only 1 execution per hour actually computes.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Current: 10 widgets × 12 refreshes/hour × 24 hours = 2,880 query executions/day
# Each execution costs compute (warehouse running)

# With result caching:
# Data updates hourly → cache invalidates once per hour
# Per widget: 12 refreshes/hour, but only 1 needs compute (cache miss after data update)
# 11 refreshes hit cache (free! no compute needed)

# Compute reduction:
# Before: 2,880 executions/day requiring compute
# After: 10 widgets × 1 compute/hour × 24 hours = 240 executions/day
# Cache hit rate: (2,880 - 240) / 2,880 = 91.7% cache hits!

# Cost savings:
# Before: $2,000/month (warehouse running to serve 2,880 queries/day)
# After: $167/month (warehouse only computes 240 queries/day, auto-stops between)
# Savings: ~$1,833/month (92% reduction!)

# No configuration needed — result caching is AUTOMATIC in DBSQL
# Cache is invalidated when underlying Delta table has new commits
# Users see fresh data within seconds of pipeline updating the table

# To verify caching is working:
# Query history → look for "cached" indicator on repeated queries
# Or: SET use_cached_result = false; to force recompute (testing only)
```

**Key Points:**
- Result caching is automatic (zero configuration needed)
- Cache invalidates when the underlying Delta table gets new data (commits)
- For hourly-update data with 5-min dashboard refresh: 91% cache hit rate
- Cached results return in <100ms (no compute, just memory lookup)
- This is why auto-stop works well: warehouse can stop between the few actual computes
- Dashboard designers don't need to know about caching — it just works

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Materialized View Design

**Scenario:** Your analyst team's most common query pattern is: "revenue by region by month, filtered by product category." This query scans 500M rows and takes 45 seconds. Design a materialized view to make it instant.

<details>
<summary>💡 Hint</summary>
Create an MV that pre-aggregates by the common dimensions (region, month, category). Queries matching this pattern will be rewritten to use the MV automatically.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Identify the common query pattern
-- Analysts write variations of:
SELECT region, DATE_TRUNC('month', order_date) AS month, SUM(amount) AS revenue
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
JOIN production.silver.products p ON o.product_id = p.product_id
WHERE p.category = 'Electronics'
GROUP BY region, DATE_TRUNC('month', order_date);
-- Takes 45 seconds (scans 500M rows, joins 3 tables)

-- Step 2: Create MV that covers this pattern (and more)
CREATE MATERIALIZED VIEW production.gold.mv_revenue_by_dimensions AS
SELECT 
    DATE_TRUNC('month', o.order_date) AS month,
    c.region,
    p.category,
    c.segment,                      -- Include extra dims analysts might filter by
    COUNT(*) AS order_count,
    SUM(o.amount) AS revenue,
    AVG(o.amount) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
JOIN production.silver.products p ON o.product_id = p.product_id
GROUP BY DATE_TRUNC('month', o.order_date), c.region, p.category, c.segment;

-- Step 3: Verify the optimizer uses the MV
EXPLAIN SELECT region, month, SUM(revenue) 
FROM production.gold.mv_revenue_by_dimensions
WHERE category = 'Electronics' AND month >= '2024-01-01'
GROUP BY region, month;
-- Plan shows: "Scan mv_revenue_by_dimensions" (pre-aggregated, small table)
-- Duration: 45 seconds → 0.5 seconds!

-- Step 4: Schedule refresh (after ETL updates silver tables)
-- Workflow task: runs every hour after silver_orders is updated
REFRESH MATERIALIZED VIEW production.gold.mv_revenue_by_dimensions;
-- Incremental refresh: only processes new rows since last refresh
```

**Key Points:**
- MV covers the common pattern (region × month × category aggregations)
- Include extra dimensions analysts might filter by (segment, brand) — wider MV serves more queries
- Optimizer automatically rewrites matching queries to use the MV (transparent)
- Refresh is incremental (only new data processed, not full 500M row recompute)
- Schedule refresh after upstream ETL completes (keep MV fresh)
- One MV can serve hundreds of different dashboard widget queries (if they match the pattern)
- Size: 500M rows → ~50K rows in MV (10,000x smaller = instant queries)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Federation

**Scenario:** Product managers need a report combining: order data from Delta Lake (lakehouse) + real-time inventory from PostgreSQL + customer satisfaction scores from a SaaS API (stored in MySQL). Design the federation approach.

<details>
<summary>💡 Hint</summary>
Use Lakehouse Federation to create foreign catalogs for PostgreSQL and MySQL. Join all three in a single SQL query. DBSQL pushes filters to external databases for efficiency.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create connections to external databases
CREATE CONNECTION inventory_db TYPE POSTGRESQL
OPTIONS (
    host 'inventory.internal.company.com',
    port '5432',
    user secret('federation', 'inventory_user'),
    password secret('federation', 'inventory_pass')
);

CREATE CONNECTION satisfaction_db TYPE MYSQL
OPTIONS (
    host 'satisfaction-db.internal.company.com',
    port '3306',
    user secret('federation', 'csat_user'),
    password secret('federation', 'csat_pass')
);

-- Step 2: Create foreign catalogs
CREATE FOREIGN CATALOG inventory USING CONNECTION inventory_db;
CREATE FOREIGN CATALOG satisfaction USING CONNECTION satisfaction_db;

-- Step 3: Join all three sources in one query!
SELECT 
    o.order_id,
    o.product_name,
    o.amount,
    o.order_date,
    inv.current_stock,           -- Live from PostgreSQL!
    inv.reorder_point,
    csat.avg_score AS satisfaction_score,  -- From MySQL!
    CASE 
        WHEN inv.current_stock < inv.reorder_point THEN 'REORDER NEEDED'
        ELSE 'OK'
    END AS stock_status
FROM production.gold.fact_orders o
LEFT JOIN inventory.public.product_inventory inv 
    ON o.product_id = inv.product_id
LEFT JOIN satisfaction.surveys.product_scores csat 
    ON o.product_id = csat.product_id
WHERE o.order_date >= CURRENT_DATE - 7
ORDER BY o.amount DESC;

-- DBSQL optimizes:
-- 1. Pushes WHERE to PostgreSQL/MySQL (only fetches needed rows)
-- 2. Delta Lake scan uses data skipping (only recent order_date files)
-- 3. Results combined in DBSQL warehouse (join executed here)
```

**Key Points:**
- No ETL needed — query external databases directly from DBSQL
- Connections are secure (credentials in Databricks Secrets)
- Filter pushdown: WHERE/JOIN conditions sent to external DBs (minimal data transferred)
- Unity Catalog governance: can GRANT/REVOKE access to foreign catalogs like any table
- Best for: real-time external data that changes frequently (inventory, status)
- Limitation: large joins between external + Delta should pre-load external data first (avoid network transfer of millions of rows)
- Trade-off: no ETL = no transformation/quality checks on external data

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Warehouse Cost Optimization

**Scenario:** Your SQL warehouse costs $8K/month. Analysis shows: 60% is idle time (warehouse sitting warm with no queries), 25% is dashboard refresh (200 widgets refreshing every 5 min), 15% is actual analyst ad-hoc queries. Reduce to $3K without impacting users.

<details>
<summary>💡 Hint</summary>
Split into purpose-specific warehouses with different auto-stop and sizing policies. Leverage result caching for dashboards. Consider serverless for variable workloads.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT: 1 Large warehouse, always-on = $8K/month
# ISSUE: 60% idle ($4,800 wasted), dashboards and ad-hoc compete

# OPTIMIZED ARCHITECTURE:
OPTIMIZED = {
    "warehouse_1_dashboards": {
        "type": "Serverless",
        "size": "Medium",
        "auto_stop": "5 min",
        "purpose": "Dashboard refresh (200 widgets, every 5 min)",
        "optimization": "Result caching: data updates hourly → 92% cache hits",
        "effective_compute": "Only ~10 min/hour of actual work (rest cached)",
        "cost": "$800/month",
    },
    "warehouse_2_adhoc": {
        "type": "Serverless",
        "size": "Small",
        "auto_stop": "10 min",
        "purpose": "Analyst ad-hoc queries (20 users, 8 hrs/day)",
        "optimization": "Serverless = instant startup, pay only during queries",
        "cost": "$1,200/month",
    },
    "warehouse_3_heavy_reports": {
        "type": "Pro",
        "size": "Large",
        "auto_stop": "10 min",
        "schedule": "Only starts for weekly/monthly reports",
        "cost": "$200/month (runs ~4 hours/month)",
    },
    "total": "$2,200/month ✓ (under $3K target, 73% savings!)",
}

# WHY THIS WORKS:
# 1. Dashboard caching: 200 widgets × 5 min refresh = lots of queries
#    BUT underlying data updates hourly → 92% hit cache → minimal compute
# 2. Serverless: no idle cost (warehouse stops between queries)
# 3. Separation: dashboards and analysts don't compete for resources
# 4. Heavy reports: only spin up Large warehouse when needed (monthly)
# 5. Auto-stop everywhere: no more paying for overnight/weekend idle

# MIGRATION PLAN:
# Week 1: Create serverless dashboard warehouse, move dashboard queries
# Week 2: Create serverless adhoc warehouse, redirect analyst connections
# Week 3: Shut down old always-on warehouse
# Week 4: Monitor costs and adjust sizes if needed
```

**Key Points:**
- The #1 cost killer: idle warehouse time (60% of spend for 0% of value!)
- Serverless eliminates idle cost completely (pay only during actual query execution)
- Result caching for dashboards: 92% of refreshes are free (data hasn't changed)
- Separate warehouses: prevents workload interference AND enables right-sizing
- Heavy reports: don't pay for a Large warehouse 24/7 for a monthly 2-hour report
- This is a standard optimization — every DBSQL deployment should implement it

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Performance Tuning

**Scenario:** A critical report query takes 90 seconds (SLA: 15 seconds). It joins orders (500M rows) with customers (5M rows) and products (100K rows), groups by 4 dimensions, and filters by date range. Optimize to meet the SLA.

<details>
<summary>💡 Hint</summary>
Multi-pronged: Z-ORDER the fact table on filter columns, ensure small tables are broadcast-joined, create a materialized view if the query runs frequently, and verify Photon is active.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- STEP 1: Analyze the query profile
EXPLAIN COST
SELECT 
    c.region, p.category, DATE_TRUNC('month', o.order_date) AS month, c.segment,
    COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM production.silver.orders o              -- 500M rows
JOIN production.silver.customers c ON o.customer_id = c.customer_id  -- 5M rows
JOIN production.silver.products p ON o.product_id = p.product_id      -- 100K rows
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY c.region, p.category, DATE_TRUNC('month', o.order_date), c.segment;

-- ISSUES FOUND in profile:
-- 1. Full scan of orders table (1000 files read out of 1000 = no data skipping!)
-- 2. SortMergeJoin for products (100K rows should be broadcast!)
-- 3. No partition pruning (table not partitioned by date)

-- STEP 2: Optimize the fact table
OPTIMIZE production.silver.orders ZORDER BY (order_date, customer_id);
-- Now queries filtering by order_date skip 90% of files

-- STEP 3: Update table statistics (helps optimizer choose broadcast)
ANALYZE TABLE production.silver.customers COMPUTE STATISTICS;
ANALYZE TABLE production.silver.products COMPUTE STATISTICS;
-- Optimizer now knows products is 100K rows → auto-broadcasts it

-- STEP 4: If query runs frequently (>10x/day), create MV
CREATE MATERIALIZED VIEW production.gold.mv_report_dims AS
SELECT 
    c.region, p.category, DATE_TRUNC('month', o.order_date) AS month, c.segment,
    COUNT(*) AS orders, SUM(o.amount) AS revenue, AVG(o.amount) AS aov
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
JOIN production.silver.products p ON o.product_id = p.product_id
GROUP BY c.region, p.category, DATE_TRUNC('month', o.order_date), c.segment;
-- MV: ~50K rows (pre-aggregated), query returns in <1 second

-- RESULTS:
-- Before: 90 seconds (full scan, sort-merge joins, no data skipping)
-- After Z-ORDER + stats: 12 seconds (data skipping + broadcast join)
-- After MV: 0.5 seconds (pre-computed aggregation)
-- SLA met: ✓ (either approach meets 15-second SLA)
```

**Key Points:**
- Z-ORDER on filter columns enables data skipping (90%+ files skipped for date-range queries)
- ANALYZE TABLE provides statistics for broadcast join decisions (100K table should always broadcast)
- Materialized view: pre-computes the entire query (500M rows → 50K rows = instant)
- Photon (default in DBSQL): 2-3x faster for aggregations (verify it's active in profile)
- Fix in order of effort: stats (1 min) → Z-ORDER (5 min) → MV (20 min setup)
- MV refresh: schedule hourly after ETL updates the source tables

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Tenant SQL Analytics

**Scenario:** Your SaaS platform has 50 enterprise customers. Each customer's analysts need SQL access to ONLY their data. Design the SQL warehouse + governance setup that provides self-service analytics with strict tenant isolation.

<details>
<summary>💡 Hint</summary>
Unity Catalog row-level security (one table, filtered by tenant) OR separate schemas per tenant. SQL Warehouses shared across tenants (cost-efficient) with UC enforcing isolation.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- APPROACH: Shared gold tables + row-level security
-- (More cost-efficient than separate schemas for 50 tenants)

-- Step 1: Gold tables include tenant_id
CREATE TABLE production.gold.customer_analytics (
    tenant_id STRING,
    metric_date DATE,
    region STRING,
    orders INT,
    revenue DECIMAL(12,2),
    unique_customers INT
);

-- Step 2: Row filter ensures each tenant sees only their data
CREATE FUNCTION production.security.tenant_filter(tid STRING)
RETURN tid = (
    SELECT tenant_id FROM production.security.user_tenant_mapping
    WHERE user_email = CURRENT_USER()
);

ALTER TABLE production.gold.customer_analytics
SET ROW FILTER production.security.tenant_filter ON (tenant_id);

-- Step 3: Shared SQL warehouse (all tenants use the same compute)
-- Cost: 1 warehouse × $2K/month (shared across 50 tenants = $40/tenant!)
-- vs 50 separate warehouses × $500/month = $25K/month (500x more expensive!)

-- Step 4: Each tenant's analysts connect via their own credentials
-- DBSQL connection string is the same for all tenants
-- Unity Catalog + row filter ensures data isolation automatically

-- Step 5: Verify isolation
-- Logged in as tenant_acme@customer.com:
SELECT * FROM production.gold.customer_analytics;
-- Returns: ONLY rows where tenant_id = 'acme' (filter applied server-side)
-- They cannot see tenant_globex data regardless of what SQL they write!
```

**Key Points:**
- Row-level security: one table, 50 tenants, automatic filtering (most cost-efficient)
- Shared warehouse: $40/tenant/month vs $500/tenant with dedicated warehouses
- Security is server-side (analysts can't bypass by writing clever SQL)
- Unity Catalog audit logs track which tenant accessed what (compliance)
- For strict compliance (healthcare/finance): consider schema-per-tenant (stronger isolation)
- Performance: add tenant_id to Z-ORDER for fast per-tenant queries (data skipping)

</details>

</article>
