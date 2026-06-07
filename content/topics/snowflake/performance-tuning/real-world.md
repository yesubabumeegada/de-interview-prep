---
title: "Performance Tuning - Real-World Production Examples"
topic: snowflake
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, performance, tuning, production, optimization]
---

# Snowflake Performance Tuning — Real-World Production Examples

## Pattern 1: Slow Dashboard Optimization

```sql
-- BEFORE: Dashboard query takes 45 seconds (scans 2 TB)
SELECT region, product_category, SUM(amount) AS revenue
FROM production.orders WHERE order_date >= '2024-01-01' GROUP BY region, product_category;
-- partitions_scanned: 5000/5000 (NO data skipping! Table not clustered)

-- FIX 1: Cluster the table
ALTER TABLE production.orders CLUSTER BY (order_date, region);
-- After reclustering: same query
-- partitions_scanned: 250/5000 (95% pruned!) → 8 seconds

-- FIX 2: Materialized View (pre-computed)
CREATE MATERIALIZED VIEW gold.mv_revenue AS
    SELECT order_date, region, product_category, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM production.orders GROUP BY order_date, region, product_category;
-- Dashboard query: rewritten to use MV → < 1 second!

-- RESULT: 45 seconds → 8 seconds (clustering) → < 1 second (MV)
```

---

## Pattern 2: ETL Job Optimization

```sql
-- BEFORE: Daily ETL takes 2 hours on Medium warehouse
-- Root cause: large shuffle (JOIN between 500M + 100M row tables)

-- ANALYSIS:
SELECT query_id, bytes_spilled_to_remote_storage/POWER(1024,3) AS remote_spill_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text LIKE '%daily_etl%' AND start_time >= CURRENT_DATE() - 1;
-- remote_spill_gb: 45 GB (massive spill → storage-bound!)

-- FIX 1: Scale up warehouse (more memory = less spill)
ALTER WAREHOUSE etl_wh SET WAREHOUSE_SIZE = 'XLARGE';
-- X-Large: 16 nodes × 16 GB = 256 GB memory
-- Spill: 45 GB → 0 GB (fits in memory now!)
-- Duration: 2 hours → 35 minutes

-- FIX 2: Optimize the JOIN (reduce data before joining)
-- BEFORE:
SELECT * FROM fact_orders f JOIN dim_customers c ON f.customer_id = c.customer_id;
-- Joins 500M × ALL customer columns

-- AFTER: select only needed columns + filter first
SELECT f.order_id, f.amount, f.order_date, c.region, c.segment
FROM fact_orders f 
JOIN (SELECT customer_id, region, segment FROM dim_customers) c  -- Only needed columns!
ON f.customer_id = c.customer_id
WHERE f.order_date = CURRENT_DATE() - 1;  -- Filter BEFORE join!
-- Joins only yesterday's orders (1.5M) with minimal customer columns
-- Duration: 35 minutes → 8 minutes!

-- FIX 3: Cluster both tables on join key
ALTER TABLE fact_orders CLUSTER BY (customer_id);
ALTER TABLE dim_customers CLUSTER BY (customer_id);
-- Join finds matching partitions faster (partition-level pruning on join key)
-- Duration: 8 minutes → 5 minutes

-- TOTAL: 2 hours → 5 minutes (24x improvement!)
```

---

## Pattern 3: Concurrency Optimization

```sql
-- PROBLEM: 9 AM "query storm" — 100 analysts log in, run dashboards simultaneously
-- Symptoms: queries queuing for 30+ seconds, frustrated users

-- ANALYSIS:
SELECT 
    DATE_TRUNC('hour', start_time) AS hour,
    COUNT(*) AS queries,
    AVG(queued_overload_time)/1000 AS avg_queue_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'BI_WH'
GROUP BY hour
ORDER BY avg_queue_seconds DESC LIMIT 10;
-- 9 AM: 200 queries, avg 35 seconds queue time!

-- FIX: Multi-cluster warehouse
ALTER WAREHOUSE bi_wh SET
    MAX_CLUSTER_COUNT = 5,        -- Scale to 5 clusters during peak
    SCALING_POLICY = 'STANDARD';  -- Scale out quickly on queue buildup

-- RESULT:
-- 9 AM peak: auto-scales to 4 clusters within 2 minutes
-- Queue time: 35 seconds → 2 seconds
-- By 10 AM: scales back to 1 cluster (cost efficient!)
-- Monthly cost increase: ~$200 (for those peak-hour clusters)
-- User satisfaction: dramatically improved (no more waiting!)
```

---

## Pattern 4: Cost Optimization Audit

```sql
-- Find the most expensive queries (credit consumers):
SELECT 
    user_name, warehouse_name,
    COUNT(*) AS query_count,
    SUM(total_elapsed_time)/3600000 AS total_hours,
    SUM(credits_used_cloud_services) AS credits,
    AVG(bytes_scanned)/POWER(1024,4) AS avg_tb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY user_name, warehouse_name
ORDER BY total_hours DESC LIMIT 20;

-- Common findings:
-- 1. User running SELECT * on 5 TB table 50x/day (should use filtered view/MV)
-- 2. ETL warehouse running 24/7 but processing only 4 hours/day (reduce auto_suspend!)
-- 3. Same dashboard query runs 200x/day but can't hit cache (non-deterministic function)

-- ACTIONS:
-- 1. Create MV for repeated expensive queries ($500/month savings)
-- 2. Set AUTO_SUSPEND=60 on ETL warehouse ($2000/month savings)
-- 3. Fix dashboard query (remove CURRENT_TIMESTAMP from SELECT → enable caching)
-- 4. Add resource monitor: alert at 80% monthly budget, suspend at 100%
```

---

## Pattern 5: Search Optimization for Lookup Queries

```sql
-- Application queries: WHERE order_id = ? (point lookups on 2B-row table)
-- Without Search Optimization: 3-5 seconds per lookup (scans partitions)
-- With Search Optimization: 100-200 ms per lookup (hash index)

-- Enable:
ALTER TABLE production.orders ADD SEARCH OPTIMIZATION ON EQUALITY(order_id, customer_id);

-- Check cost estimate:
SELECT * FROM TABLE(SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('production.orders'));
-- Shows: estimated maintenance credits per day

-- RESULT:
-- Before: 3-5 seconds per lookup × 10,000 lookups/day = 500+ compute-seconds/day
-- After: 0.1-0.2 seconds per lookup × 10,000 = 1500 compute-seconds/day
-- Plus: search optimization maintenance credits
-- Net: usually 50-80% faster for the lookup use case

-- Best for: application backends that do point lookups on large Snowflake tables
-- NOT for: analytical queries (GROUP BY, full scans) — clustering is better for those
```

---

## Interview Tips

> **Tip 1:** "Walk through tuning a slow query" — (1) Query Profile: check partitions scanned (clustering), spill (memory), join behavior. (2) If bad pruning → cluster on filter columns. (3) If spill → scale up warehouse. (4) If repeated → create MV. (5) If point lookup → Search Optimization. Start cheap (clustering costs nothing extra at query time) → escalate to more expensive fixes only if needed.

> **Tip 2:** "How do you reduce Snowflake costs by 40%?" — Audit: find unused/idle warehouses (auto-suspend), oversized warehouses (scale down), repeated expensive queries (add MVs), inefficient ETL (incremental instead of full refresh), and SELECT * patterns (column pruning). The 80/20 rule: 20% of queries consume 80% of credits — optimize those first.

> **Tip 3:** "How do you handle the 9 AM dashboard rush?" — Multi-cluster warehouse: auto-scales from 1 to 5 clusters during peak, scales back during quiet. Pre-warm: set MIN_CLUSTER_COUNT=2 during business hours (instant response for first queries). MVs: pre-compute dashboard aggregations (queries hit cached MV, not full table scans). Result caching: identical queries return from cache (free, instant).
