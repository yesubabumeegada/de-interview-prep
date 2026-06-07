---
title: "Performance Tuning - Intermediate"
topic: snowflake
subtopic: performance-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, performance, tuning, query-profile, search-optimization, scaling]
---

# Snowflake Performance Tuning — Intermediate

## Query Profile Deep Dive

```sql
-- READING THE QUERY PROFILE (most important skill for tuning):

-- METRIC 1: Partition Pruning
-- partitions_scanned=50, partitions_total=1000 → 95% pruned (good!)
-- partitions_scanned=980, partitions_total=1000 → 2% pruned (bad — needs clustering!)

-- METRIC 2: Bytes Spilled
-- bytes_spilled_to_local=0, bytes_spilled_to_remote=0 → fits in memory (good!)
-- bytes_spilled_to_local=5GB → insufficient memory → scale up warehouse
-- bytes_spilled_to_remote=20GB → very insufficient → use 2X-Large warehouse!

-- METRIC 3: Join Explosion
-- TableScan1: 1M rows, TableScan2: 500K rows, JoinOutput: 500M rows → BAD!
-- Likely: bad join condition (creating cartesian product)
-- Fix: check ON clause for correctness, add additional join conditions

-- METRIC 4: Network/Remote I/O
-- If > 50% time is "Remote Disk I/O": storage-bound, not compute-bound
-- Fix: better clustering (less data read) or larger warehouse (more parallel I/O)

-- Find problematic queries:
SELECT query_id, query_text, total_elapsed_time/1000 AS sec,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage / POWER(1024,3) AS spill_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 30000  -- > 30 seconds
ORDER BY total_elapsed_time DESC LIMIT 10;
```

---

## Search Optimization Service

For point-lookup queries (WHERE id = ?), clustering alone isn't enough:

```sql
-- Enable Search Optimization on specific columns:
ALTER TABLE production.orders ADD SEARCH OPTIMIZATION ON EQUALITY(order_id, customer_id);
ALTER TABLE production.events ADD SEARCH OPTIMIZATION ON EQUALITY(event_id), SUBSTRING(url);

-- HOW IT WORKS:
-- Creates a secondary search index (hash-based)
-- Point lookups: O(1) instead of scanning partitions
-- Best for: WHERE order_id = 12345 (exact match)
-- Also works for: IN, BETWEEN, LIKE patterns

-- COST: background maintenance (serverless credits for index updates)
-- SAVINGS: 10-100x faster for point lookups on large tables

-- When to use:
-- ✅ Large tables (>100M rows) with point-lookup queries
-- ✅ High-cardinality columns (order_id, user_id) queried by equality
-- ❌ Small tables (< 1M rows — already fast)
-- ❌ Columns only used in GROUP BY (clustering is better)
-- ❌ Full table scans (search optimization doesn't help full scans)

-- Check effectiveness:
SELECT SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('production.orders', 'EQUALITY(order_id)');
```

---

## Multi-Cluster Warehouses (Scaling for Concurrency)

```sql
-- Problem: 50 analysts querying simultaneously → queries queue up
-- Solution: multi-cluster warehouse (horizontal scaling)

ALTER WAREHOUSE bi_warehouse SET
    MIN_CLUSTER_COUNT = 1      -- Minimum clusters running
    MAX_CLUSTER_COUNT = 5      -- Scale up to 5 clusters for concurrency
    SCALING_POLICY = 'STANDARD';  -- Add clusters when queries queue

-- HOW IT WORKS:
-- 1 cluster handles ~8-12 concurrent queries (depends on complexity)
-- When >12 queries queuing → Snowflake adds another cluster (2 clusters = 24 concurrent)
-- Up to max_cluster_count (5 = ~60 concurrent queries)
-- When load drops → clusters scale back down (pay only for what you use)

-- SCALING POLICIES:
-- STANDARD: scale out quickly (add cluster after short queue), scale in after idle
-- ECONOMY: scale out slowly (tolerate more queuing), scale in quickly (save costs)
-- Use STANDARD for: BI dashboards (users expect fast response)
-- Use ECONOMY for: batch/ETL (queries can wait a bit)
```

---

## Materialized Views for Performance

```sql
-- MVs pre-compute expensive aggregations:
CREATE MATERIALIZED VIEW gold.mv_daily_revenue AS
    SELECT order_date, region, SUM(amount) AS revenue
    FROM production.orders GROUP BY order_date, region;

-- Queries automatically rewritten to use MV (transparent):
SELECT region, SUM(amount) FROM production.orders WHERE order_date = '2024-03-15' GROUP BY region;
-- → reads from MV (10 MB) instead of scanning base table (500 GB)!
-- Speed: 30 sec → < 1 sec

-- WHEN to create MVs:
-- 1. Query runs >10x/day AND takes >5 seconds
-- 2. Single-table aggregation (MVs don't support JOINs)
-- 3. Source table changes infrequently (reduces refresh cost)
```

---

## Join Optimization

```sql
-- RULE 1: Filter BEFORE joining (reduce rows entering the join)
-- BAD: join first, filter later
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_date = '2024-03-15';
-- Snowflake optimizer usually pushes this down, but be explicit when complex

-- RULE 2: Join on clustered/indexed columns
-- If you frequently join orders.customer_id with customers.customer_id:
ALTER TABLE orders CLUSTER BY (customer_id);
ALTER TABLE customers CLUSTER BY (customer_id);
-- Now the join can skip non-matching partitions!

-- RULE 3: Avoid cartesian products (check ON clause!)
-- BAD: SELECT * FROM a, b WHERE a.date = b.date (if date is not unique → explosion!)
-- GOOD: SELECT * FROM a JOIN b ON a.id = b.id AND a.date = b.date (unique join key)

-- RULE 4: Small table last in LEFT JOIN (for Snowflake's hash join)
-- Snowflake builds hash table from the SMALLER side automatically
-- But: ensure your WHERE doesn't accidentally filter the smaller side away!
```

---

## Warehouse Auto-Suspend and Resume

```sql
-- Auto-suspend (saves credits when idle):
ALTER WAREHOUSE bi_wh SET AUTO_SUSPEND = 300;  -- Suspend after 5 min idle
-- Queries arrive → warehouse resumes (2-5 sec startup) → processes → suspends when idle

-- For BI (interactive, latency-sensitive):
AUTO_SUSPEND = 300;  -- 5 min (short idle timeout, minimize wake-up waits)

-- For ETL (scheduled, latency-tolerant):
AUTO_SUSPEND = 60;   -- 1 min (suspend quickly after each batch completes)

-- For always-on (API serving, real-time dashboards):
AUTO_SUSPEND = 0;    -- Never suspend (always ready, pays 24/7)
-- Only use when sub-second response is REQUIRED (expensive!)
```

---

## Interview Tips

> **Tip 1:** "How do you diagnose a slow Snowflake query?" — Query Profile: check (1) partitions scanned vs total (pruning), (2) bytes spilled (memory), (3) join output vs input rows (explosion), (4) queuing time (concurrency). Based on findings: add clustering (pruning), scale warehouse (memory/spill), fix join keys (explosion), or add clusters (concurrency).

> **Tip 2:** "Clustering vs Search Optimization — when to use each?" — Clustering: best for RANGE queries (WHERE date BETWEEN x AND y) and queries scanning many rows. Search Optimization: best for POINT lookups (WHERE id = 12345) on high-cardinality columns. They're complementary: cluster by date (common range filter) + search optimization on order_id (point lookups).

> **Tip 3:** "How do you handle 100 concurrent analysts?" — Multi-cluster warehouse: MIN=1, MAX=5 clusters, STANDARD scaling policy. Each cluster handles ~10 concurrent queries. Total: 50 concurrent = auto-scales to 5 clusters. When load drops: scales back to 1 cluster (cost efficient). No manual intervention — fully automatic scaling.
