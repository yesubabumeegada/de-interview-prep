---
title: "Materialized Views - Senior Deep Dive"
topic: snowflake
subtopic: materialized-views
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [snowflake, materialized-views, production, cost-optimization, strategy]
---

# Snowflake Materialized Views — Senior-Level Deep Dive

## MV Strategy for Query Acceleration

### Identifying MV Candidates

```sql
-- Find the most expensive repeated queries (MV candidates):
SELECT 
    query_hash,  -- Same query structure
    COUNT(*) AS execution_count,
    AVG(total_elapsed_time / 1000) AS avg_seconds,
    SUM(bytes_scanned) / COUNT(*) / POWER(1024,3) AS avg_gb_scanned,
    SUM(credits_used_cloud_services) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 5000  -- > 5 seconds
  AND query_type = 'SELECT'
GROUP BY query_hash
HAVING execution_count > 50  -- Run > 50 times/month
ORDER BY execution_count * avg_seconds DESC  -- Impact = frequency × duration
LIMIT 20;

-- TOP CANDIDATES: queries that are FREQUENT + SLOW + scan lots of data
-- These benefit most from materialization (pre-compute once, serve many times)
```

### Cost-Benefit Analysis

```sql
-- For each candidate MV, calculate ROI:
-- Cost savings = (query_credits_without_MV - query_credits_with_MV) × frequency
-- MV cost = refresh_credits_per_month
-- ROI = savings - MV_cost

-- Example:
-- Query without MV: scans 500 GB, takes 30 sec, costs ~$0.50/run
-- Query frequency: 200 runs/month
-- Monthly query cost: 200 × $0.50 = $100/month

-- MV: stores 100 MB (small aggregation), refreshes 100 times/month
-- Refresh cost: ~$0.01/refresh × 100 = $1/month
-- Query with MV: scans 100 MB, takes 0.5 sec, costs ~$0.001/run
-- Monthly query cost with MV: 200 × $0.001 = $0.20/month

-- Net savings: $100 - $0.20 - $1 = $98.80/month (99% reduction!)
-- CLEAR WIN: create this MV!
```

---

## Advanced MV Patterns

### Multiple MVs on Same Source

```sql
-- Create MVs for different query patterns on the same table:

-- MV 1: Region-level aggregation (dashboard queries)
CREATE MATERIALIZED VIEW gold.mv_region_daily AS
    SELECT order_date, region, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders GROUP BY order_date, region;

-- MV 2: Category-level aggregation (product analytics)
CREATE MATERIALIZED VIEW gold.mv_category_monthly AS
    SELECT DATE_TRUNC('month', order_date) AS month, product_category,
           SUM(amount) AS revenue, COUNT(DISTINCT customer_id) AS customers
    FROM silver.orders GROUP BY month, product_category;

-- MV 3: High-value order filter (fraud detection)
CREATE MATERIALIZED VIEW gold.mv_large_orders AS
    SELECT order_id, customer_id, amount, order_date
    FROM silver.orders WHERE amount > 5000;

-- The optimizer picks the BEST MV for each query automatically:
-- "Revenue by region?" → uses mv_region_daily
-- "Revenue by category?" → uses mv_category_monthly
-- "Orders over $5K?" → uses mv_large_orders
-- All transparent — users just query silver.orders as usual!
```

### MV + Clustering for Maximum Performance

```sql
-- Cluster the MV on the most common filter column:
CREATE MATERIALIZED VIEW gold.mv_orders_clustered
    CLUSTER BY (order_date)
AS
    SELECT order_date, region, product_category,
           SUM(amount) AS revenue, COUNT(*) AS orders, AVG(amount) AS aov
    FROM silver.orders
    GROUP BY order_date, region, product_category;

-- Query: WHERE order_date = '2024-03-15'
-- Double optimization: MV (pre-aggregated) + clustering (data skipping)
-- Source: 1 TB silver.orders → MV: 500 MB → After date filter: 2 MB scanned!
-- Speed: 30 seconds → 0.1 seconds (300x faster!)
```

---

## Production MV Governance

```sql
-- Monitor all MVs health and cost:
CREATE OR REPLACE VIEW ops.mv_health AS
WITH refresh_stats AS (
    SELECT 
        MATERIALIZED_VIEW_NAME,
        COUNT(*) AS refreshes_7d,
        SUM(CREDITS_USED) AS credits_7d,
        AVG(DATA_SIZE) AS avg_data_size
    FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
        DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
    ))
    GROUP BY MATERIALIZED_VIEW_NAME
)
SELECT 
    mv.TABLE_NAME AS mv_name,
    mv.IS_SECURE,
    mv.BYTES / POWER(1024,2) AS mv_size_mb,
    rs.refreshes_7d,
    rs.credits_7d AS refresh_cost_7d,
    CASE 
        WHEN rs.credits_7d > 10 THEN 'HIGH_COST'
        WHEN mv.BYTES > 10 * POWER(1024,3) THEN 'LARGE'
        ELSE 'HEALTHY'
    END AS status
FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS mv
LEFT JOIN refresh_stats rs ON mv.TABLE_NAME = rs.MATERIALIZED_VIEW_NAME;

-- DROP MVs that cost more than they save:
-- If refresh_cost_7d > estimated query savings → MV is net negative → drop it!
```

---

## MV vs Alternatives Decision Framework

```sql
-- DECISION TREE for query acceleration:

-- Q: Is the slow query an aggregation on a SINGLE table?
--   YES → Materialized View (automatic rewriting, serverless refresh)
--   NO (has JOINs) → Continue...

-- Q: Can you pre-join the tables?
--   YES → Dynamic Table (pre-joins + aggregates, warehouse refresh)
--   NO → Continue...

-- Q: Is the query pattern predictable?
--   YES → Create a gold table (manual refresh via task/DT)
--   NO (ad-hoc, varying) → Search Optimization Service (automatic indexing)

-- Q: Is it a point-lookup query (WHERE id = ?)
--   YES → Search Optimization Service (creates indexes)
--   NO → Consider clustering the table (better data skipping)

-- SUMMARY:
-- Single-table aggregation → MV
-- Multi-table transformation → Dynamic Table
-- Point lookups → Search Optimization
-- Range scans → Clustering
-- All of the above are complementary (can use together!)
```

---

## Interview Tips

> **Tip 1:** "How do you decide which queries deserve a Materialized View?" — Find queries that are: frequent (>50x/month), slow (>5 seconds), and scan large tables. Calculate ROI: query savings per month vs MV refresh cost. If savings > 10× refresh cost: definitely create the MV. Use QUERY_HISTORY to identify candidates systematically.

> **Tip 2:** "Can you have multiple MVs on the same table?" — Yes! Create different MVs for different query patterns (region aggregation, category aggregation, filtered subsets). The optimizer picks the best MV for each query automatically. Each MV has its own refresh cost, so only create MVs that are actively used.

> **Tip 3:** "MV governance — how do you manage them?" — Monitor: refresh credit usage (MATERIALIZED_VIEW_REFRESH_HISTORY), MV size, staleness. Alert if: refresh cost exceeds threshold, MV is stale (suspended accidentally), or MV is large but never queried. Drop unused MVs (they cost refresh credits for no benefit). Review quarterly.

## ⚡ Cheat Sheet

**Snowflake architecture layers**
```
Cloud Services:   metadata, optimizer, access control, query planning
Virtual Warehouse: compute (T-shirt sizes: XS to 6XL); auto-suspend + auto-resume
Storage:          columnar Parquet on S3/Blob/GCS; billed separately from compute
```

**Virtual warehouse management**
```sql
CREATE WAREHOUSE analytics_wh WITH WAREHOUSE_SIZE='MEDIUM'
  AUTO_SUSPEND=60 AUTO_RESUME=TRUE MAX_CLUSTER_COUNT=3 MIN_CLUSTER_COUNT=1
  SCALING_POLICY='ECONOMY';  -- or STANDARD
ALTER WAREHOUSE analytics_wh SUSPEND;
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE='LARGE';
```

**Time travel**
```sql
SELECT * FROM orders AT (OFFSET => -60*60);                          -- 1 hour ago
SELECT * FROM orders AT (TIMESTAMP => '2024-01-15 08:00:00'::TIMESTAMP);
SELECT * FROM orders BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');
-- Restore from time travel
CREATE TABLE orders_restored CLONE orders AT (OFFSET => -3600);
-- Default retention: 1 day (standard), up to 90 days (enterprise)
```

**Streams and Tasks**
```sql
-- Stream: CDC on a table
CREATE STREAM orders_stream ON TABLE orders;
SELECT * FROM orders_stream;  -- METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID

-- Task: scheduled or triggered compute
CREATE TASK process_orders
  WAREHOUSE = 'etl_wh'
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
  INSERT INTO gold.orders SELECT * FROM orders_stream WHERE METADATA$ACTION = 'INSERT';

ALTER TASK process_orders RESUME;
```

**Dynamic Tables**
```sql
CREATE DYNAMIC TABLE gold.orders_summary
  TARGET_LAG = '5 minutes'
  WAREHOUSE = etl_wh
AS
  SELECT region, SUM(amount) AS total FROM silver.orders GROUP BY region;
-- Snowflake automatically refreshes when source changes; no task/stream needed
```

**Snowpipe (continuous ingestion)**
```sql
CREATE PIPE orders_pipe AUTO_INGEST=TRUE AS
  COPY INTO orders FROM @orders_stage FILE_FORMAT=(TYPE='CSV');
-- S3 event notification → SQS → Snowpipe auto-triggers COPY on new files
-- Latency: ~1 minute; cost: per-file compute credits
```

**Data sharing**
```sql
CREATE SHARE sales_share;
GRANT USAGE ON DATABASE prod TO SHARE sales_share;
GRANT SELECT ON TABLE prod.gold.orders TO SHARE sales_share;
ALTER SHARE sales_share ADD ACCOUNTS = partner_account_id;
-- Consumer sees a read-only database — no data copy, no egress charges
```

**Stored procedures (JavaScript/Python/Snowflake Scripting)**
```sql
CREATE OR REPLACE PROCEDURE load_and_validate(p_date STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION='3.10'
PACKAGES=('snowflake-snowpark-python') HANDLER='run'
AS $$
def run(session, p_date):
    df = session.table("staging.orders").filter(f"order_date = '{p_date}'")
    if df.count() == 0:
        return f"No data for {p_date}"
    df.write.save_as_table("gold.orders", mode="append")
    return f"Loaded {df.count()} rows"
$$;
```

**External tables**
```sql
CREATE EXTERNAL TABLE ext_orders (
    order_id NUMBER AS (VALUE:c1::NUMBER),
    amount   FLOAT  AS (VALUE:c3::FLOAT)
) WITH LOCATION=@orders_stage FILE_FORMAT=(TYPE='PARQUET')
AUTO_REFRESH=TRUE;
-- Reads directly from S3; no data copy to Snowflake storage
```

**Materialized views**
```sql
CREATE MATERIALIZED VIEW mv_orders_by_region AS
  SELECT region, SUM(amount) AS total FROM orders GROUP BY region;
-- Auto-incremental refresh by Snowflake when base table changes
-- Best for: complex aggregations queried frequently; available in Enterprise+
```

**Key interview points**
- Micro-partitions: 50-500 MB compressed Parquet; automatic clustering per load order
- Cluster keys: explicit clustering on high-cardinality columns (date, customer_id)
- Query profile: check for partition pruning, spillage to disk, heavy operators
- Zero-copy clone: CREATE TABLE dev_orders CLONE gold.orders — instant, no storage cost
- Fail-safe: 7-day recovery window after time travel expires (Snowflake internal only)
