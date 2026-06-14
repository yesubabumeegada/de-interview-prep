---
title: "Snowflake Cost Management - Intermediate"
topic: snowflake
subtopic: cost-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, cost, query-optimization, warehouse-sizing, clustering, caching]
---

# Snowflake Cost Management — Intermediate Concepts

## The Three Caches (Free Compute)

Snowflake has three caching layers — leveraging them reduces credit consumption:

| Cache | What It Stores | Duration | Key |
|-------|---------------|----------|-----|
| Result Cache | Exact query results | 24 hours | Query text must be identical |
| Metadata Cache | Object metadata, aggregates | Persistent | `MIN`, `MAX`, `COUNT` without warehouse |
| Local Disk Cache | Remote data read by warehouse | Warehouse lifetime | Same warehouse, same data |

```sql
-- Result cache: second run is instant and FREE (no warehouse credits)
SELECT COUNT(*) FROM fact_sales WHERE year = 2023;  -- 3 seconds (cold)
SELECT COUNT(*) FROM fact_sales WHERE year = 2023;  -- 0 ms (result cache hit)

-- Result cache is invalidated if underlying data changes

-- Metadata cache: these queries use NO warehouse
SELECT COUNT(*) FROM large_table;               -- metadata only
SELECT MIN(sale_date) FROM fact_sales;          -- metadata only
SELECT MAX(order_id), COUNT(*) FROM orders;     -- metadata only

-- Local disk cache: keep the same warehouse running for related queries
-- (don't suspend between queries in a query pattern)
USE WAREHOUSE analytics_wh;
SELECT * FROM fact_sales WHERE region = 'APAC';   -- reads from S3, fills local cache
SELECT * FROM fact_sales WHERE region = 'EMEA';   -- S3 reads (different rows)
SELECT * FROM fact_sales WHERE region = 'APAC' AND amount > 1000;  -- local cache hit!
```

---

## Warehouse Sizing: Right-Sizing vs Over-Sizing

**Rule of thumb:** A query that takes > 5 minutes — try the next size up. Larger = faster, often same total credit cost.

```sql
-- Time a query at different sizes
ALTER WAREHOUSE test_wh SET WAREHOUSE_SIZE = 'SMALL';
SELECT SUM(amount) FROM fact_orders WHERE order_date >= '2024-01-01';
-- 4 minutes → 2 credits consumed

ALTER WAREHOUSE test_wh SET WAREHOUSE_SIZE = 'LARGE';
SELECT SUM(amount) FROM fact_orders WHERE order_date >= '2024-01-01';
-- 45 seconds → 2 credits consumed  (same cost, 5x faster)
```

**When sizing doesn't help (anti-patterns):**
- Query is slow due to bad clustering → Snowflake scans too many micro-partitions
- Query is memory-limited → Disk spilling → Larger warehouse helps more than sizing
- Many small concurrent queries → Multi-cluster, not bigger size

```sql
-- Detect disk spilling (symptom: query is much slower than expected)
SELECT query_text, bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND bytes_spilled_to_remote_storage > 0
ORDER BY bytes_spilled_to_remote_storage DESC;
-- If spilling to remote storage → increase warehouse size
```

---

## Query Profiling for Cost Attribution

```sql
-- Which queries are burning the most credits?
SELECT
    user_name,
    warehouse_name,
    query_text,
    TOTAL_ELAPSED_TIME / 1000 AS secs,
    CREDITS_USED_CLOUD_SERVICES,
    BYTES_SCANNED / 1e9 AS gb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(100 * PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL,0), 1) AS pct_partitions_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;

-- Poor partition pruning indicator (scanning > 50% of partitions on large tables)
-- → Add or reconfigure clustering key
```

---

## Clustering and Partition Pruning

Clustering controls which micro-partitions are skipped — directly impacts credits:

```sql
-- Check current clustering health
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(sale_date)');
-- Returns: average_depth (should be close to 1), total_partition_count, etc.

-- Add clustering key on the most-filtered column
ALTER TABLE fact_sales CLUSTER BY (sale_date);

-- After re-clustering, compare partition scans
-- Before clustering: PARTITIONS_SCANNED = 5000 / 5000 (100% scan)
-- After clustering:  PARTITIONS_SCANNED = 12 / 5000 (0.24% scan = 99.76% savings)

-- Monitor clustering cost (automatic clustering has its own credit consumption)
SELECT credits_used, schema_name, table_name
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY credits_used DESC;
```

---

## Materialized Views for Repeated Aggregations

```sql
-- Expensive aggregation that runs 100+ times/day
SELECT region, DATE_TRUNC('day', sale_date) AS day, SUM(amount) AS revenue
FROM fact_sales
GROUP BY 1, 2;

-- Create materialized view — Snowflake maintains it automatically
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT region, DATE_TRUNC('day', sale_date) AS day, SUM(amount) AS revenue
FROM fact_sales
GROUP BY 1, 2;

-- Now queries hitting this pattern use the MV (no full table scan)
SELECT * FROM mv_daily_revenue WHERE day >= '2024-01-01';

-- MV maintenance cost
SELECT credits_used, schema_name, table_name
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());
```

---

## Dedicated Warehouses per Workload

Mixing workloads on one warehouse causes queue contention:

```sql
-- Separate warehouses per workload type
CREATE WAREHOUSE etl_wh     WAREHOUSE_SIZE = 'XLARGE' AUTO_SUSPEND = 60;  -- heavy batch
CREATE WAREHOUSE bi_wh      WAREHOUSE_SIZE = 'MEDIUM'  AUTO_SUSPEND = 300; -- BI tools
CREATE WAREHOUSE adhoc_wh   WAREHOUSE_SIZE = 'SMALL'   AUTO_SUSPEND = 60;  -- analyst queries
CREATE WAREHOUSE system_wh  WAREHOUSE_SIZE = 'XSMALL'  AUTO_SUSPEND = 60;  -- tasks, jobs

-- Result: ETL doesn't slow down BI, BI doesn't queue behind ETL
-- Each warehouse suspends independently — no idle cost when that workload is inactive
```

---

## Spend Visibility: Building a Cost Dashboard

```sql
-- Daily credit burn by warehouse (last 30 days)
SELECT
    DATE_TRUNC('day', start_time) AS day,
    warehouse_name,
    SUM(credits_used) AS credits,
    ROUND(SUM(credits_used) * 3.0, 2) AS cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Storage trend (weekly)
SELECT
    DATE_TRUNC('week', usage_date) AS week,
    ROUND(SUM(average_bytes) / 1e12, 2) AS avg_tb_stored
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE usage_date >= DATEADD('week', -12, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

---

## Interview Tips

> **Tip 1:** "How do you reduce query costs without changing warehouse size?" — "Three angles: (1) Clustering — fewer micro-partitions scanned. (2) Result cache — identical queries within 24h are free. (3) Materialized views — pre-aggregate repeated heavy queries. Start with query profiling to see `PARTITIONS_SCANNED / PARTITIONS_TOTAL` — high ratio = clustering problem."

> **Tip 2:** "What causes unexpected Snowflake credit consumption?" — "Common culprits: warehouses left running idle (auto-suspend too high), automatic clustering on large tables (check AUTOMATIC_CLUSTERING_HISTORY), full table scans from missing cluster keys, disk spilling from underpowered warehouses running large sorts/joins. Use WAREHOUSE_METERING_HISTORY and QUERY_HISTORY to trace."

> **Tip 3:** "When should you use multi-cluster vs larger single cluster?" — "Larger single cluster = more power per query (parallel within-query execution). Multi-cluster = more concurrent queries without queuing. A BI tool with 50 users all firing dashboards simultaneously needs multi-cluster, not a bigger single warehouse. A nightly ETL running one giant query needs a bigger size, not more clusters."
