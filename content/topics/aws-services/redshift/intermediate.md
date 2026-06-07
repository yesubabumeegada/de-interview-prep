---
title: "AWS Redshift - Intermediate"
topic: aws-services
subtopic: redshift
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, redshift, optimization, vacuum, wlm, concurrency-scaling, materialized-views]
---

# AWS Redshift — Intermediate Concepts

## VACUUM and ANALYZE — Table Maintenance

Redshift uses copy-on-write for UPDATEs and DELETEs. Deleted rows are marked (not removed), and UPDATEs create new rows + mark old ones dead. VACUUM reclaims this space and re-sorts.

```sql
-- VACUUM: reclaims dead rows and re-sorts data
VACUUM FULL fact_orders;          -- Full sort + reclaim (most thorough, slowest)
VACUUM SORT ONLY fact_orders;     -- Just re-sort (no space reclaim)
VACUUM DELETE ONLY fact_orders;   -- Just reclaim space (no re-sort)

-- ANALYZE: updates table statistics for the query optimizer
ANALYZE fact_orders;
-- Without current stats, optimizer may choose bad query plans

-- Best practice: run after every significant data load
-- Redshift auto-runs VACUUM/ANALYZE in background, but manual is faster after big loads
```

**When to VACUUM:**

| Scenario | VACUUM Type | Frequency |
|----------|-------------|-----------|
| After large DELETE operations | DELETE ONLY | After each DELETE batch |
| After bulk UPDATE | FULL | After ETL updates |
| After INSERT to unsorted table | SORT ONLY | After each load cycle |
| Routine maintenance | FULL | Weekly (auto handles most) |

> **Auto VACUUM:** Redshift automatically runs VACUUM in background during low-activity periods. For large tables with heavy churn, manual VACUUM during maintenance windows is still recommended.

---

## Workload Management (WLM)

WLM controls how queries are queued and how resources are allocated between different workloads:

```sql
-- WLM Queues (configured in parameter group):
-- Queue 1: ETL (large queries, limited concurrency)
--   Concurrency: 3, Memory: 60%, Timeout: 3600s
-- Queue 2: BI Dashboards (fast queries, high concurrency)  
--   Concurrency: 15, Memory: 30%, Timeout: 300s
-- Queue 3: Default (catch-all)
--   Concurrency: 5, Memory: 10%

-- Route queries to queues using query groups:
SET query_group TO 'etl_workload';
INSERT INTO target SELECT * FROM staging;  -- Routes to ETL queue

SET query_group TO 'dashboard';
SELECT region, SUM(amount) FROM fact_sales GROUP BY region;  -- Routes to BI queue

-- Or use user groups (automatic routing based on who's querying)
-- ETL service account → ETL queue
-- Analyst users → BI queue
```

**Short Query Acceleration (SQA):**
```sql
-- Enable SQA: short queries bypass the queue and run immediately
-- Redshift automatically identifies queries < threshold and prioritizes them
-- Configuration in parameter group: max_execution_time = 5 seconds
-- Queries predicted to finish in <5s get fast-tracked
```

---

## Concurrency Scaling

Automatically adds transient clusters during traffic spikes:

```sql
-- Enable concurrency scaling on a WLM queue
-- When queue is full (all slots occupied), burst queries go to transient clusters
-- Transient clusters spin up in seconds, handle overflow, spin down when idle

-- You get 1 hour FREE concurrency scaling per day per active cluster
-- Beyond that: charged per-second of transient cluster usage

-- Check concurrency scaling usage:
SELECT * FROM svl_concurrency_scaling_usage ORDER BY start_time DESC LIMIT 20;
```

**When concurrency scaling helps:**
- Dashboard rush at 9 AM (many users at once)
- ETL running while analysts also query
- Unpredictable burst workloads

---

## Materialized Views

Pre-compute and store query results for instant access:

```sql
-- Create a materialized view
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT 
    order_date,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue
FROM fact_orders
GROUP BY order_date, region;

-- Refresh when underlying data changes
REFRESH MATERIALIZED VIEW mv_daily_revenue;

-- Auto-refresh (Redshift can do this automatically)
CREATE MATERIALIZED VIEW mv_daily_revenue
AUTO REFRESH YES AS
SELECT ...;

-- Queries automatically use the MV when the optimizer determines it's faster
SELECT region, SUM(total_revenue) 
FROM mv_daily_revenue 
WHERE order_date >= '2024-01-01';
-- Reads from MV (pre-aggregated, tiny) instead of fact table (billions of rows)
```

---

## Data Sharing (Cross-Cluster)

Share live data between Redshift clusters without copying:

```sql
-- Producer cluster: create a datashare
CREATE DATASHARE sales_share SET PUBLICACCESSIBLE TRUE;
ALTER DATASHARE sales_share ADD SCHEMA public;
ALTER DATASHARE sales_share ADD TABLE public.fact_orders;
ALTER DATASHARE sales_share ADD TABLE public.dim_customer;

-- Grant to consumer cluster's namespace
GRANT USAGE ON DATASHARE sales_share TO NAMESPACE 'consumer-cluster-namespace-id';

-- Consumer cluster: access shared data
CREATE DATABASE shared_sales FROM DATASHARE sales_share 
    OF NAMESPACE 'producer-cluster-namespace-id';

SELECT * FROM shared_sales.public.fact_orders WHERE order_date = CURRENT_DATE;
-- Reads live data from producer cluster — always current, no ETL sync needed
```

---

## Compression Encodings

Redshift automatically chooses column compression, but understanding it helps with tuning:

| Encoding | Best For | Compression Ratio |
|----------|----------|:-:|
| AZ64 | Numeric, date (default for numbers) | 4-8x |
| LZO | Large VARCHAR, JSON | 3-5x |
| BYTEDICT | Low-cardinality VARCHAR (<256 distinct) | 10-20x |
| RUNLENGTH | Sorted columns with many repeats | 5-50x |
| ZSTD | General purpose (good default) | 3-6x |
| RAW | No compression (tiny tables) | 1x |

```sql
-- Check current encodings
SELECT "column", type, encoding FROM pg_table_def WHERE tablename = 'fact_orders';

-- Recommend best encodings based on actual data:
ANALYZE COMPRESSION fact_orders;
-- Returns: column → recommended encoding → estimated reduction %
```

---

## Redshift Spectrum — Querying S3

Query data that lives in S3 without loading it into Redshift:

```sql
-- Create external schema (connects to Glue Catalog)
CREATE EXTERNAL SCHEMA lake
FROM DATA CATALOG DATABASE 'curated'
IAM_ROLE 'arn:aws:iam::123:role/SpectrumRole'
REGION 'us-east-1';

-- Query S3 data as a regular table
SELECT event_type, COUNT(*) 
FROM lake.events  -- This data is on S3, not in Redshift!
WHERE year = 2024 AND month = 1
GROUP BY event_type;

-- Join local Redshift tables with S3 external tables
SELECT c.name, SUM(e.amount)
FROM local_schema.dim_customer c
JOIN lake.fact_events e ON c.customer_id = e.user_id
WHERE e.year = 2024
GROUP BY c.name;
```

**Spectrum pricing:** $5 per TB scanned from S3 (same as Athena). Use partitioning + Parquet to minimize scan costs.

---

## Interview Tips

> **Tip 1:** "How do you handle concurrent workloads in Redshift?" — "WLM queues: separate ETL (few large queries, lots of memory) from dashboards (many small queries, high concurrency). Short Query Acceleration fast-tracks sub-5-second queries. Concurrency Scaling adds burst capacity during peak hours."

> **Tip 2:** "What maintenance does Redshift need?" — "VACUUM to reclaim space from DELETEs and re-sort data. ANALYZE to update statistics for the optimizer. Both run automatically in background, but after large ETL loads I trigger them manually for immediate effect."

> **Tip 3:** "How do you optimize storage costs in Redshift?" — "Three approaches: (1) RA3 nodes with managed storage (only pay for data you use). (2) Spectrum for cold data — keep historical data in S3, query via Spectrum. (3) Compression: verify columns use optimal encodings (ANALYZE COMPRESSION). (4) Data sharing instead of copying data between clusters."
