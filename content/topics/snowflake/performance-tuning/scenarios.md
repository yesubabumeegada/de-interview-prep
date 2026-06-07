---
title: "Performance Tuning - Scenario Questions"
topic: snowflake
subtopic: performance-tuning
content_type: scenario_question
tags: [snowflake, performance, tuning, interview, scenarios]
---

# Scenario Questions — Performance Tuning

<article data-difficulty="junior">

## 🟢 Junior: Identifying a Slow Query

**Scenario:** A query scanning `production.orders` (500M rows) takes 60 seconds. The Query Profile shows: partitions_scanned=5000, partitions_total=5000 (zero data skipping). The WHERE clause filters by `order_date = '2024-03-15'`. What's the problem and fix?

<details>
<summary>💡 Hint</summary>
5000/5000 partitions scanned = no data skipping. The table isn't clustered by order_date, so Snowflake can't skip partitions based on the date filter.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- PROBLEM: No clustering → no data skipping → full table scan on every query

-- FIX: Cluster the table on the filter column
ALTER TABLE production.orders CLUSTER BY (order_date);

-- Snowflake will reorganize data so each micro-partition contains a narrow date range
-- After reclustering (may take hours for 500M rows):

-- Same query:
SELECT * FROM production.orders WHERE order_date = '2024-03-15';
-- NOW: partitions_scanned=15, partitions_total=5000 (99.7% pruned!)
-- Duration: 60 seconds → ~1 second!

-- Verify clustering effectiveness:
SELECT SYSTEM$CLUSTERING_INFORMATION('production.orders', '(order_date)');
-- average_depth should be 1-2 (well-clustered)
-- If average_depth > 5: reclustering hasn't completed yet

-- COST: reclustering uses serverless credits (background process)
-- Ongoing: auto-reclustering maintains the clustering as new data arrives
-- ROI: massive — every query filtering by date benefits immediately
```

**Key Points:**
- partitions_scanned = partitions_total → ZERO data skipping (always bad for filtered queries)
- Fix: cluster on the column(s) used in WHERE clauses
- After clustering: 99%+ partitions skipped → 10-100x faster
- One-time effort (ALTER TABLE CLUSTER BY) with automatic ongoing maintenance
- This is the #1 performance optimization for Snowflake queries

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Memory Spill

**Scenario:** Your ETL MERGE statement takes 90 minutes and the Query Profile shows 30 GB spilled to remote storage. The warehouse is Medium (4 nodes × 16 GB = 64 GB memory). Diagnose and fix without just "throwing money at it" (i.e., not just scaling up).

<details>
<summary>💡 Hint</summary>
Remote spill = data doesn't fit in memory during processing (sort/join). Before scaling up: can you reduce the data volume entering the expensive operation? (Filter earlier, select fewer columns, split the operation.)
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- CURRENT MERGE (causes 30 GB spill):
MERGE INTO silver.orders t
USING (SELECT * FROM raw.orders_stream) s  -- ALL columns, ALL rows!
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- Problem: "SELECT *" reads ALL columns (50+) into memory for the join

-- FIX 1: Select only needed columns (reduces memory 60-80%!)
MERGE INTO silver.orders t
USING (
    SELECT order_id, customer_id, amount, order_date, status  -- Only 5 columns!
    FROM raw.orders_stream
    WHERE METADATA$ACTION = 'INSERT'
) s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET 
    t.amount = s.amount, t.status = s.status
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount, order_date, status)
    VALUES (s.order_id, s.customer_id, s.amount, s.order_date, s.status);
-- Memory: 50 columns → 5 columns = 90% less data in memory!

-- FIX 2: Deduplicate BEFORE merge (reduce rows)
USING (
    SELECT order_id, customer_id, amount, order_date, status
    FROM raw.orders_stream WHERE METADATA$ACTION = 'INSERT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1
) s
-- Fewer rows entering the MERGE = less memory needed

-- FIX 3: If still spilling: cluster target table on join key
ALTER TABLE silver.orders CLUSTER BY (order_id);
-- MERGE can now use partition-level matching (less data shuffled in memory)

-- RESULT:
-- Before: 90 min, 30 GB remote spill (Medium warehouse)
-- After (columns + dedup + cluster): 12 min, 0 GB spill (same Medium warehouse!)
-- No warehouse scale-up needed (saved $$ on compute!)
```

**Key Points:**
- Remote spill = 10x slower than local spill = 100x slower than in-memory
- First: reduce data volume (select fewer columns, filter earlier, deduplicate)
- Then: cluster target table on join key (reduces shuffle in MERGE)
- Last resort: scale up warehouse (more memory per node)
- "Fix the query first, scale the warehouse last" — always cheaper!

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Comprehensive Performance Audit

**Scenario:** Your Snowflake environment costs $25K/month. Management wants 30% reduction without impacting users. Perform a performance/cost audit and recommend optimizations.

<details>
<summary>💡 Hint</summary>
Audit: warehouse utilization, expensive queries, unused resources, auto-suspend settings. Common savings: idle warehouses, SELECT *, missing clustering, no MVs for repeated queries, oversized warehouses.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- AUDIT STEP 1: Warehouse utilization (find idle warehouses)
SELECT warehouse_name,
       SUM(credits_used) AS credits_30d,
       COUNT(DISTINCT DATE(start_time)) AS active_days,
       AVG(credits_used) / NULLIF(COUNT(DISTINCT DATE(start_time)), 0) AS credits_per_active_day
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name ORDER BY credits_30d DESC;
-- Finding: DEV_WH used only 5 days/month but auto_suspend=3600 (keeps running!)
-- Fix: SET AUTO_SUSPEND = 60; → saves ~$2K/month

-- AUDIT STEP 2: Expensive repeated queries (MV candidates)
SELECT query_hash, COUNT(*) AS runs,
       AVG(total_elapsed_time)/1000 AS avg_sec,
       AVG(bytes_scanned)/POWER(1024,4) AS avg_tb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 10000
GROUP BY query_hash HAVING runs > 100
ORDER BY runs * avg_sec DESC LIMIT 10;
-- Finding: 3 dashboard queries run 200x/day, scan 500 GB each time
-- Fix: Create MVs for these 3 queries → saves ~$3K/month

-- AUDIT STEP 3: Oversized warehouses
SELECT warehouse_name, warehouse_size,
       AVG(avg_running) AS avg_queries_running,
       MAX(avg_queued_load) AS max_queued
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name, warehouse_size;
-- Finding: ETL_LARGE used at 30% capacity → downsize to MEDIUM
-- Fix: SET SIZE = 'MEDIUM'; → saves ~$1.5K/month

-- AUDIT STEP 4: Full table scans (missing clustering)
SELECT query_id, query_text, partitions_scanned, partitions_total,
       bytes_scanned/POWER(1024,3) AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE partitions_scanned > partitions_total * 0.9  -- >90% partitions scanned
  AND bytes_scanned > 100 * POWER(1024,3)  -- >100 GB scanned
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY bytes_scanned DESC LIMIT 10;
-- Finding: 5 tables queried without clustering
-- Fix: CLUSTER BY on appropriate columns → saves ~$1K/month (less data scanned)

-- SUMMARY:
-- Auto-suspend fix: $2,000/month saved
-- Materialized Views: $3,000/month saved
-- Warehouse downsize: $1,500/month saved
-- Clustering: $1,000/month saved
-- TOTAL: $7,500/month (30% of $25K!) ✓
```

**Key Points:**
- Auto-suspend is the easiest win (idle warehouses = pure waste)
- MVs for repeated queries: high-frequency queries pre-computed = massive savings
- Warehouse right-sizing: if avg utilization < 50%, you're over-provisioned
- Clustering: eliminates full table scans (the biggest per-query cost driver)
- This audit pattern works for any Snowflake environment — use the system views!
- 30% savings typically achievable with these four optimizations alone

</details>

</article>
