---
title: "AWS Athena - Scenario Questions"
topic: aws-services
subtopic: athena
content_type: scenario_question
tags: [aws, athena, interview, scenarios, cost-optimization]
---

# Scenario Questions — AWS Athena

<article data-difficulty="junior">

## 🟢 Junior: Reduce a $500/Month Athena Bill

**Scenario:** Your team's Athena queries cost $500/month. Investigation shows: data is stored as CSV, queries use `SELECT *`, and there's no partitioning. The data is 100 GB of daily logs (3 years of data). Propose optimizations and estimate the new cost.

<details>
<summary>✅ Solution</summary>

**Current cost breakdown:**
- 100 GB CSV × multiple queries/day × $5/TB = $500/month
- Each query scans the entire 100 GB regardless of date filter

**Optimization plan:**

| Step | Action | Data Scanned Impact |
|------|--------|-------------------|
| 1 | Convert CSV → Parquet | 100 GB → ~20 GB (5x compression) |
| 2 | Add date partitioning | 20 GB → ~0.5 GB per day's query (40x pruning) |
| 3 | Select specific columns | 0.5 GB → ~0.1 GB (only 20% of columns needed) |
| **Combined** | All three | **100 GB → 0.1 GB per query (1000x reduction)** |

**Implementation:**

```sql
-- Step 1+2+3: CTAS to convert format + partition + structure
CREATE TABLE optimized.daily_logs
WITH (
    format = 'PARQUET',
    external_location = 's3://lake/optimized/logs/',
    partitioned_by = ARRAY['year', 'month', 'day'],
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    timestamp, user_id, action, status_code, response_time,
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month,
    DAY(timestamp) AS day
FROM raw.csv_logs;
```

**New cost estimate:**
- Per query: 0.1 GB × $5/TB = $0.0005
- 1000 queries/month: $0.50/month
- **Savings: $500 → $0.50/month (99.9% reduction)**

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Data Lake Query Layer

**Scenario:** Your data lake has 50 TB of data across multiple zones (raw, curated, analytics). Different teams need to query it: analysts want SQL, data scientists want to explore raw data, and dashboards need fast cached results. Design the query layer using Athena.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```sql
-- Workgroup per team (separate cost tracking + query limits)
-- Workgroup: analysts (10 GB scan limit per query, curated access only)
-- Workgroup: data-science (100 GB scan limit, all zones)
-- Workgroup: dashboards (pre-computed views, cached results)

-- Catalog structure:
-- Database: raw (JSON/CSV, no partitions — exploratory only)
-- Database: curated (Parquet, partitioned — main analytics)
-- Database: analytics (pre-aggregated, optimized for dashboards)
```

**For Analysts (curated zone):**
```sql
-- Partitioned Parquet tables with clear schema
CREATE EXTERNAL TABLE curated.fact_sales (...)
PARTITIONED BY (sale_date DATE)
STORED AS PARQUET
LOCATION 's3://lake/curated/fact_sales/';

-- Pre-built views for common queries
CREATE VIEW curated.v_monthly_revenue AS
SELECT sale_month, region, SUM(amount) AS revenue
FROM curated.fact_sales
GROUP BY sale_month, region;
```

**For Dashboards (analytics zone):**
```sql
-- Pre-aggregated tables refreshed daily (CTAS with INSERT INTO)
CREATE TABLE analytics.daily_kpis
WITH (format='PARQUET', external_location='s3://lake/analytics/kpis/')
AS SELECT ...;

-- Daily refresh via scheduled Athena query (or Glue job)
INSERT INTO analytics.daily_kpis
SELECT ... WHERE date = CURRENT_DATE - 1;
```

**For Data Scientists (raw zone):**
```sql
-- Raw JSON with schema-on-read (flexible but expensive)
CREATE EXTERNAL TABLE raw.events (
    event_id STRING,
    payload STRING  -- Raw JSON string
)
STORED AS TEXTFILE
LOCATION 's3://lake/raw/events/';

-- Query with JSON extraction
SELECT 
    JSON_EXTRACT_SCALAR(payload, '$.user_id') AS user_id,
    JSON_EXTRACT_SCALAR(payload, '$.action') AS action
FROM raw.events
WHERE dt = '2024-01-15'  -- Always filter by partition!
LIMIT 1000;
```

**Cost controls per workgroup:**
- Analysts: max 10 GB/query, $100/month budget
- Data Science: max 100 GB/query, $500/month budget
- Dashboards: max 1 GB/query (pre-aggregated data is tiny)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Athena Performance at Scale

**Scenario:** Your Athena queries on a 10 TB partitioned Parquet table take 5+ minutes. The table has 2M+ partitions (partitioned by year/month/day/hour — over-partitioned). How do you fix the performance without re-loading all data?

<details>
<summary>✅ Solution</summary>

**Problem:** 2M partitions causes extreme overhead:
- Glue Catalog must list all matching partitions (slow with millions)
- S3 LIST operations for each partition directory
- Each partition may have tiny files (<1 MB)

**Fix 1: Reduce partition granularity (re-partition to daily instead of hourly)**

```sql
-- CTAS to create a new table with fewer, larger partitions
CREATE TABLE optimized.events
WITH (
    format = 'PARQUET',
    external_location = 's3://lake/optimized/events/',
    partitioned_by = ARRAY['year', 'month'],  -- Monthly instead of hourly!
    parquet_compression = 'SNAPPY',
    bucketed_by = ARRAY['user_id'],
    bucket_count = 100
) AS
SELECT *, YEAR(event_time) AS year, MONTH(event_time) AS month
FROM original.events;
-- 2M partitions → ~60 partitions (36 months × 1-2 years)
```

**Fix 2: If you can't re-partition, use partition projection (skip catalog lookup)**

```sql
-- Partition projection: Athena generates partition values from a pattern
-- instead of querying the Glue Catalog (MUCH faster for many partitions)
CREATE EXTERNAL TABLE events_projected (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    event_time TIMESTAMP
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION 's3://lake/curated/events/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2022,2025',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'projection.day.type' = 'integer',
    'projection.day.range' = '1,31',
    'projection.hour.type' = 'integer',
    'projection.hour.range' = '0,23',
    'storage.location.template' = 
        's3://lake/curated/events/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);
-- Athena now CALCULATES partition locations instead of looking them up
-- Query planning: 30 seconds → 1 second
```

**Fix 3: Compact small files within partitions**

```python
# Glue job to compact hourly partitions into larger files
for partition in get_partitions_with_small_files():
    spark.read.parquet(partition.location) \
        .coalesce(1) \
        .write.mode("overwrite") \
        .parquet(partition.location)
# Each partition: 24 tiny files → 1 optimally-sized file
```

**Expected improvement:**
- Partition projection: query planning 30s → 1s
- Fewer partitions (monthly): catalog lookup 10s → 0.1s
- File compaction: S3 read overhead 60s → 5s
- **Total: 5 minutes → 30 seconds**

</details>

</article>
