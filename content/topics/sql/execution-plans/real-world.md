---
title: "SQL Execution Plans - Real-World Production Examples"
topic: sql
subtopic: execution-plans
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, execution-plans, performance-tuning, production, slow-query, postgresql, snowflake]
---

# SQL Execution Plans — Real-World Production Examples

## Scenario 1: Diagnosing a Cascading Slow Query Incident

**Business context:** A Monday morning ops alert fires at 8am: the customer dashboard is timing out for all users. The engineering team sees 100% CPU on the primary PostgreSQL database. pg_stat_activity shows 50 identical queries running simultaneously, each consuming 30+ seconds. The query:

```sql
-- The slow query (simplified):
SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.name
ORDER BY lifetime_value DESC
LIMIT 50;
```

**Step 1: EXPLAIN ANALYZE on a dev replica**

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.customer_id, c.name, COUNT(o.order_id), SUM(o.amount), MAX(o.order_date)
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.name
ORDER BY SUM(o.amount) DESC LIMIT 50;
```

```
Limit  (cost=999999.00..999999.13 rows=50)
        (actual time=28442.1..28442.2 rows=50 loops=1)
  -> Sort  (cost=999999.00..1000124.00 rows=50000)
            (actual time=28442.0..28442.1 rows=50 loops=1)
       Sort Key: (sum(o.amount)) DESC
       Sort Method: external merge  Disk: 126000kB  ← 126MB disk sort!
       -> HashAggregate  (actual rows=50000 loops=1)
            Group Key: c.customer_id, c.name
            -> Hash Left Join  (actual rows=2000000 loops=1)
                 Hash Cond: (o.customer_id = c.customer_id)
                 -> Seq Scan on orders o  (actual rows=10000000 loops=1)
                      Buffers: shared hit=1234 read=88765  ← 88K disk reads!
                 -> Hash  (actual rows=50000 loops=1)
                      Buckets: 65536  Batches: 1  Memory Usage: 3500kB
                      -> Seq Scan on customers c  (actual rows=50000 loops=1)
                           Filter: (is_active = TRUE)
                           Rows Removed by Filter: 5000
Planning Time: 2.1 ms
Execution Time: 28445.3 ms
```

**Diagnosis:**
1. `Seq Scan on orders o (rows=10000000)` — scanning all 10M orders with 88K disk reads
2. `Sort Method: external merge Disk: 126MB` — sort of 50K customers spilling to disk
3. `Buffers: read=88765` — nearly all data coming from disk, not cache
4. The result of the join is 2M rows before aggregation — too much intermediate data

**Step 2: Identify root causes**

```sql
-- Check: why is there no index on orders.customer_id?
\d orders
-- No index found!  ← MISSING INDEX (most impactful fix)

-- Check: is_active filter removes only 5K of 55K customers (9%)
-- A partial index would help but not dramatically

-- Check: why is 126MB spilling to disk?
SHOW work_mem;
-- 4MB  ← Way too low; sort of 50K customers exceeds 4MB
```

**Step 3: Apply fixes**

```sql
-- Fix 1 (highest impact): Add missing index
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);

-- Fix 2: Increase work_mem for dashboard connections
-- In pgBouncer/connection config for dashboard role:
ALTER ROLE dashboard_user SET work_mem = '64MB';

-- Fix 3: Rewrite query to leverage index and reduce sort size
-- Use a subquery to pre-aggregate orders, then join to customers
-- This avoids the 10M row join before aggregation:
SELECT 
    c.customer_id,
    c.name,
    COALESCE(agg.order_count, 0) AS order_count,
    COALESCE(agg.lifetime_value, 0) AS lifetime_value,
    agg.last_order_date
FROM customers c
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS lifetime_value, MAX(order_date) AS last_order_date
    FROM orders GROUP BY customer_id
) agg ON c.customer_id = agg.customer_id
WHERE c.is_active = TRUE
ORDER BY lifetime_value DESC NULLS LAST
LIMIT 50;
```

**Step 4: Verify improvements**

```sql
-- After index creation and work_mem increase:
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.customer_id, c.name, COUNT(o.order_id), SUM(o.amount), MAX(o.order_date)
FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.is_active = TRUE GROUP BY c.customer_id, c.name ORDER BY SUM(o.amount) DESC LIMIT 50;
```

```
Limit  (actual time=220.1..220.2 rows=50 loops=1)
  -> Sort  (actual time=220.0..220.1 rows=50 loops=1)
       Sort Method: quicksort  Memory: 6kB   ← In-memory sort! (only 50 rows after LIMIT)
       -> HashAggregate  (actual rows=50000)
            -> Hash Left Join  (actual rows=2000000)
                 -> Seq Scan on orders  (rows=10000000)
                      Buffers: shared hit=89999 read=0  ← 0 disk reads! All in cache now
                 -> Hash (customers, 50000 rows)
Execution Time: 220.5 ms   ← From 28 seconds to 220ms (127× improvement)
```

**Root cause summary:**
- Missing index on orders.customer_id caused 88K disk reads per query
- 50 concurrent queries × 88K disk reads = disk I/O saturation → cascading slowdown
- work_mem too low caused disk spills even for medium-sized sorts

---

## Scenario 2: BigQuery Query Costing $5,000/Day Due to Missing Partition Filter

**Business context:** The data team's BigQuery bill spikes from $500/day to $5,000/day. Investigation shows a new dashboard query is running every 5 minutes, processing 50TB each run.

**The offending query:**

```sql
-- Analyst wrote this query without realizing the cost implications:
SELECT 
    DATE_TRUNC(event_date, MONTH) AS month,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users
FROM `analytics.user_events`  -- 50TB table, partitioned by event_date
GROUP BY 1, 2;
-- No WHERE clause → scans entire 50TB table!
-- $5/TB × 50TB × 288 runs/day = $72,000/day!
```

**Investigation via INFORMATION_SCHEMA:**

```sql
SELECT 
    job_id,
    user_email,
    query,
    total_bytes_processed / POW(1024, 4) AS tb_processed,
    total_bytes_processed / POW(1024, 4) * 5 AS cost_usd,
    creation_time
FROM `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE query LIKE '%user_events%'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 10;
```

| job_id | user_email | tb_processed | cost_usd | creation_time |
|--------|-----------|-------------|---------|--------------|
| job_abc | analyst@co.com | 50.2 | 251.0 | 2024-01-15 08:00 |
| job_def | analyst@co.com | 50.2 | 251.0 | 2024-01-15 08:05 |

**Fixes applied:**

```sql
-- Fix 1: Add require_partition_filter to the table (prevent future accidents)
ALTER TABLE `analytics.user_events`
SET OPTIONS (require_partition_filter = TRUE);
-- Now any query without WHERE event_date = ... gets an error instead of a $251 charge

-- Fix 2: Rewrite the dashboard query with correct partition filter
-- The dashboard only needs the last 13 months for rolling year-over-year:
SELECT 
    DATE_TRUNC(event_date, MONTH) AS month,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users
FROM `analytics.user_events`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)  -- Partition filter!
GROUP BY 1, 2;
-- Now scans: 13 months / 36 months of data = 36% of table ≈ 18TB
-- Better: schedule this as a nightly materialized view

-- Fix 3: Pre-aggregate into a summary table (reduces scan to near zero)
CREATE OR REPLACE TABLE `analytics.monthly_event_summary`
PARTITION BY month
AS
SELECT 
    DATE_TRUNC(event_date, MONTH) AS month,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users
FROM `analytics.user_events`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
GROUP BY 1, 2;

-- Dashboard now queries the 36-row summary table instead of 50TB:
SELECT month, event_type, unique_users FROM `analytics.monthly_event_summary`
WHERE month >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
ORDER BY month, event_type;
-- Bytes processed: < 1MB → cost: $0.000005 per query
```

**Result:** Dashboard cost dropped from $5,000/day to $2/day. The `require_partition_filter` option is now applied to all fact tables to prevent recurrence.

---

## Scenario 3: Snowflake Spillover Causing a 45-Minute Query

**Business context:** A data scientist runs a product recommendation model feature extraction query. It processes 2 billion event rows and has been running for 45 minutes with no end in sight. The Snowflake Query Profile shows massive "Bytes Spilled to Remote Disk" in the join stage.

**Investigation in Snowflake:**

```sql
-- Find the query in query history:
SELECT 
    query_id,
    query_text,
    execution_time / 1000 AS seconds,
    bytes_scanned / POW(1024,3) AS gb_scanned,
    bytes_spilled_to_remote_storage / POW(1024,3) AS gb_spilled,
    partitions_scanned,
    partitions_total
FROM snowflake.account_usage.query_history
WHERE execution_time > 300000  -- > 5 minutes
ORDER BY execution_time DESC
LIMIT 5;
```

| query_id | seconds | gb_scanned | gb_spilled | partitions_scanned |
|---------|---------|-----------|-----------|-------------------|
| abc123 | 2700 | 1840 | 450 | 8920 | 

**The problematic query (simplified):**

```sql
-- Massive join with a large right side:
SELECT 
    e.user_id,
    e.product_id,
    COUNT(*) AS interactions,
    p.category_id,
    p.price_tier
FROM events e  -- 2B rows
JOIN products p ON e.product_id = p.product_id  -- 5M rows (not small!)
WHERE e.event_date >= DATEADD(day, -90, CURRENT_DATE())
GROUP BY e.user_id, e.product_id, p.category_id, p.price_tier;
```

**Root cause:** The products table (5M rows × many columns) is too large to fit in Snowflake's virtual warehouse memory for the broadcast join. Snowflake falls back to a distributed hash join with disk spilling — the 450GB spill.

**Fixes:**

```sql
-- Fix 1: Reduce the right side of the join to only needed columns
SELECT 
    e.user_id,
    e.product_id,
    COUNT(*) AS interactions,
    p.category_id,
    p.price_tier
FROM events e
JOIN (
    SELECT product_id, category_id, price_tier  -- Only 3 columns vs. all 40
    FROM products
) p ON e.product_id = p.product_id
WHERE e.event_date >= DATEADD(day, -90, CURRENT_DATE())
GROUP BY 1, 2, 3, 4;
-- Smaller right-side → fits in memory → no spill → 5 minutes vs. 45 minutes

-- Fix 2: Pre-aggregate events before the join (reduce left side rows too)
WITH daily_product_events AS (
    SELECT user_id, product_id, COUNT(*) AS interactions
    FROM events
    WHERE event_date >= DATEADD(day, -90, CURRENT_DATE())
    GROUP BY user_id, product_id  -- Pre-aggregate from 2B to ~200M rows
)
SELECT 
    dpe.user_id,
    dpe.product_id,
    dpe.interactions,
    p.category_id,
    p.price_tier
FROM daily_product_events dpe
JOIN (SELECT product_id, category_id, price_tier FROM products) p 
    ON dpe.product_id = p.product_id;
-- Join is now 200M × 5M (much better) vs. 2B × 5M

-- Fix 3: Use CLUSTER BY on events table (reduce partitions scanned)
-- Only needed if Fix 1+2 aren't sufficient:
ALTER TABLE events CLUSTER BY (user_id, event_date);
-- Events for same user are co-located → better cache utilization during aggregation
```

**Result after Fix 1+2:** Query runtime dropped from 45 minutes to 4 minutes, and `gb_spilled` dropped from 450GB to 0 (no spill). The data science team adopted the pre-aggregation pattern as a standard template for feature extraction queries.

---

## Interview Tips

> **Tip 1:** "What would you look for first in an EXPLAIN ANALYZE output for a slow query?" — "I check four things in order: (1) actual vs estimated rows — a large gap is the root cause of most bad plans; (2) sort or hash join batches > 1 — means spilling to disk, fix with more work_mem or an index; (3) Seq Scan on large tables where a filter removes most rows (high Rows Removed by Filter) — missing or unusable index; (4) loops count on subqueries — reveals N+1 correlated subqueries. The item with the highest absolute execution time AND the biggest estimated vs actual discrepancy is the starting point for optimization."

> **Tip 2:** "How do you handle a query that was fast for months and suddenly became slow?" — "First I check if anything changed: new data distribution (ANALYZE to refresh statistics), a new index that the optimizer is now choosing incorrectly, or a recently added partition. I compare the current plan (EXPLAIN ANALYZE) with a plan from before the slowdown (from auto_explain logs or pg_stat_statements history). If the plan changed, I identify what triggered the change and either fix the statistics, add a hint to restore the old plan, or restructure the query. I also check if the data volume changed dramatically — plans optimized for 1M rows can be wrong for 100M rows."

> **Tip 3:** "What's the fastest way to check if a query is using partition pruning in Snowflake?" — "Query `snowflake.account_usage.query_history` and check `partitions_scanned / partitions_total`. If this ratio is high (close to 1.0), pruning is not happening — all micro-partitions are being scanned. The fix is to add a filter on the cluster key in the WHERE clause. I also check the `compilation_time` column — very long compilation times (>30s) on simple queries indicate schema complexity issues, not pruning problems."
