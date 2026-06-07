---
title: "Performance Tuning - Senior Deep Dive"
topic: snowflake
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [snowflake, performance, tuning, production, cost, architecture]
---

# Snowflake Performance Tuning — Senior-Level Deep Dive

## Micro-Partition Architecture and Data Skipping

```sql
-- Snowflake stores data in immutable micro-partitions (50-500 MB compressed)
-- Each partition has METADATA: min/max values per column, distinct count, null count
-- Query with WHERE: Snowflake checks metadata → skips partitions that CAN'T match

-- EXAMPLE: WHERE order_date = '2024-03-15'
-- Partition 1 metadata: order_date min=2024-03-01, max=2024-03-10 → SKIP (can't contain 3/15)
-- Partition 2 metadata: order_date min=2024-03-11, max=2024-03-20 → READ (might contain 3/15)
-- Partition 3 metadata: order_date min=2024-03-21, max=2024-03-31 → SKIP
-- Result: reads 1 out of 3 partitions (67% reduction for this simple example)

-- With GOOD clustering (data sorted by order_date):
-- Partition 1: dates 3/01-3/05 → SKIP
-- Partition 2: dates 3/06-3/10 → SKIP
-- Partition 3: dates 3/11-3/15 → READ (contains 3/15!)
-- Partition 4: dates 3/16-3/20 → SKIP
-- ... (all others skip too)
-- Result: reads 1 out of 100 partitions (99% reduction!)

-- KEY INSIGHT: Clustering NARROWS the min/max range per partition
-- Narrow range = better data skipping = faster queries = less data read = cheaper!
```

---

## Advanced Clustering Strategies

```sql
-- MULTI-KEY CLUSTERING:
ALTER TABLE production.orders CLUSTER BY (order_date, customer_id);
-- First key: coarse-grained pruning (order_date eliminates most partitions)
-- Second key: fine-grained within date (customer_id within a date's partitions)

-- CLUSTER BY EXPRESSION:
ALTER TABLE production.events CLUSTER BY (TO_DATE(event_time), event_type);
-- Cluster by derived expression (not just raw columns)
-- Useful when: queries filter by DATE(timestamp) not the raw timestamp

-- MONITORING cluster quality:
SELECT SYSTEM$CLUSTERING_INFORMATION('production.orders', '(order_date, customer_id)');
-- Returns:
-- average_overlap: how many partitions share the same key range (lower = better)
-- average_depth: how many partitions must be read for a single key value (lower = better)

-- WHEN TO RE-CLUSTER:
-- average_depth > 2-3 for frequently filtered columns → consider re-clustering
-- Automatic reclustering: Snowflake does this in background (serverless credits)
-- Manual: ALTER TABLE ... RECLUSTER; (forces immediate re-clustering)

-- COST of reclustering:
-- Serverless credits for background maintenance
-- Monitor: AUTOMATIC_CLUSTERING_HISTORY table function
SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    TABLE_NAME => 'ORDERS', DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
));
```

---

## Query Optimization at Scale

### Spill Reduction

```sql
-- SPILL = data written to local/remote disk because memory is full
-- Cause: operation needs more memory than available (large sorts, joins, aggregations)

-- FIX 1: Larger warehouse (more memory per node)
-- Medium warehouse: ~16 GB memory per node × 4 nodes = 64 GB total
-- Large warehouse: ~16 GB × 8 nodes = 128 GB total (2x more memory!)

-- FIX 2: Reduce data volume before expensive operations
-- BAD: sort THEN filter
SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM orders) WHERE rn <= 100;
-- Sorts ALL rows → spills if table is large

-- GOOD: filter THEN sort (QUALIFY is optimized)
SELECT * FROM orders QUALIFY ROW_NUMBER() OVER (ORDER BY amount DESC) <= 100;
-- Snowflake optimizes: doesn't sort everything (top-N optimization)

-- FIX 3: Break large operations into stages
-- Instead of one massive 10-table join:
-- Create intermediate temp tables → join them in stages → less memory per stage
```

### Resource Monitor and Cost Control

```sql
-- Prevent runaway queries from consuming excessive credits:
CREATE RESOURCE MONITOR cost_control
    WITH CREDIT_QUOTA = 1000  -- Monthly limit: 1000 credits
    TRIGGERS ON 75 PERCENT DO NOTIFY      -- Alert at 75%
             ON 90 PERCENT DO NOTIFY      -- Alert at 90%
             ON 100 PERCENT DO SUSPEND;   -- Hard stop at 100%

ALTER WAREHOUSE etl_wh SET RESOURCE_MONITOR = cost_control;

-- Statement timeout (kill queries that run too long):
ALTER WAREHOUSE bi_wh SET STATEMENT_TIMEOUT_IN_SECONDS = 300;
-- Any query > 5 minutes gets killed (prevents runaway joins eating credits)

-- Query tag for cost attribution:
ALTER SESSION SET QUERY_TAG = 'team=analytics,dashboard=revenue';
-- All queries in this session tagged → attribute costs to team/dashboard
```

---

## Workload Isolation Strategy

```sql
-- SEPARATE WAREHOUSES by workload type (prevent interference):

-- ETL warehouse (large, short-lived, scheduled):
CREATE WAREHOUSE etl_wh SIZE = 'LARGE' AUTO_SUSPEND = 60
    COMMENT = 'Scheduled ETL pipelines';

-- BI warehouse (medium, bursty, multi-cluster for concurrency):
CREATE WAREHOUSE bi_wh SIZE = 'MEDIUM' AUTO_SUSPEND = 300
    MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 5
    COMMENT = 'Analyst ad-hoc and dashboards';

-- ML warehouse (x-large, GPU-like operations, long-running):
CREATE WAREHOUSE ml_wh SIZE = 'XLARGE' AUTO_SUSPEND = 120
    COMMENT = 'ML training and feature engineering';

-- WHY separate:
-- ETL large queries don't slow down analyst small queries (no resource competition)
-- BI auto-scales for concurrency (ETL doesn't need this)
-- ML runs expensive operations without affecting production dashboards
-- Each warehouse has its own resource monitor and budget
```

---

## Performance Anti-Patterns

```sql
-- ANTI-PATTERN 1: SELECT * (reads ALL columns from storage)
SELECT * FROM production.orders;
-- Reads all 50 columns × all micro-partitions!
-- FIX: SELECT only needed columns (column pruning)
SELECT order_id, amount, order_date FROM production.orders;

-- ANTI-PATTERN 2: DISTINCT on large result sets
SELECT DISTINCT * FROM production.events;  -- Sorts EVERYTHING for dedup!
-- FIX: use GROUP BY (optimizer handles better) or QUALIFY (windowed dedup)

-- ANTI-PATTERN 3: UNION (requires dedup) vs UNION ALL (no dedup)
SELECT * FROM table_a UNION SELECT * FROM table_b;  -- Sorts for dedup!
-- FIX: UNION ALL if duplicates are impossible or acceptable
SELECT * FROM table_a UNION ALL SELECT * FROM table_b;

-- ANTI-PATTERN 4: Correlated subqueries
SELECT * FROM orders o WHERE amount > (SELECT AVG(amount) FROM orders WHERE region = o.region);
-- Executes subquery PER ROW in outer query!
-- FIX: use window function
SELECT * FROM orders QUALIFY amount > AVG(amount) OVER (PARTITION BY region);

-- ANTI-PATTERN 5: Cross-joins (accidental cartesian products)
SELECT * FROM table_a a, table_b b;  -- |a| × |b| rows!
-- FIX: always use explicit JOIN with ON clause
```

---

## Interview Tips

> **Tip 1:** "Explain Snowflake's data skipping mechanism" — Each micro-partition stores min/max metadata per column. When querying with WHERE, Snowflake checks metadata and SKIPS partitions whose range can't contain matching rows. Clustering narrows the min/max range (less overlap) → more effective skipping → faster queries. Well-clustered: 95-99% partitions skipped.

> **Tip 2:** "How do you handle query spill in Snowflake?" — Spill = operation exceeds available memory. Solutions: (1) larger warehouse (more memory per node), (2) reduce data before expensive ops (filter first, then sort/join), (3) break multi-table joins into stages. Monitor: bytes_spilled in QUERY_HISTORY. Spill to local disk = moderate issue. Spill to remote storage = severe (10x slower than local).

> **Tip 3:** "Workload isolation strategy?" — Separate warehouses: ETL (large, scheduled, short runs), BI (medium, multi-cluster for concurrency), ML (x-large, long-running). Each has: own resource monitor (budget control), own sizing (right-sized for workload), own auto-suspend (cost control). Prevents: ETL starving dashboards, runaway ML queries eating BI budget.
