---
title: "External Tables - Intermediate"
topic: snowflake
subtopic: external-tables
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, external-tables, partitioning, iceberg, performance]
---

# Snowflake External Tables — Intermediate

## Advanced Partitioning

```sql
-- Custom partition expressions (not just Hive-style):
CREATE EXTERNAL TABLE ext.events (
    event_id VARCHAR AS (VALUE:event_id::VARCHAR),
    event_type VARCHAR AS (VALUE:event_type::VARCHAR),
    event_time TIMESTAMP AS (VALUE:event_time::TIMESTAMP),
    user_id NUMBER AS (VALUE:user_id::NUMBER)
)
PARTITION BY (
    event_date DATE AS TO_DATE(SPLIT_PART(METADATA$FILENAME, '/', 3)),  -- Extract date from path
    source VARCHAR AS SPLIT_PART(METADATA$FILENAME, '/', 2)            -- Extract source from path
)
LOCATION = @lake_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE;

-- File path: s3://lake/events/api/2024-03-15/file001.parquet
-- Extracts: event_date='2024-03-15', source='api'
-- Partition pruning works: WHERE event_date = '2024-03-15' reads only that date's files!
```

---

## Iceberg Tables (Open Table Format)

```sql
-- Snowflake supports Apache Iceberg tables as external tables!
-- Benefits: ACID transactions, schema evolution, time travel — all on S3

CREATE ICEBERG TABLE ext.iceberg_orders
    EXTERNAL_VOLUME = 'my_s3_volume'
    CATALOG = 'SNOWFLAKE'  -- Snowflake manages the Iceberg catalog
    BASE_LOCATION = 'orders_iceberg/';

-- Now you get:
-- ✅ Full DML (INSERT, UPDATE, DELETE) on S3 data!
-- ✅ Time Travel (Iceberg snapshots)
-- ✅ Schema evolution
-- ✅ Data still in open format (other tools can read it: Spark, Trino, etc.)
-- ✅ Snowflake-optimized queries

INSERT INTO ext.iceberg_orders VALUES (1, 100.50, '2024-03-15', 'US');
-- Data written to S3 in Iceberg format (Parquet + metadata)
-- Queryable by Snowflake AND other engines (open format!)
```

---

## Materialized Views on External Tables

```sql
-- Problem: external table queries are slow (reads S3 each time)
-- Solution: create a Materialized View (pre-computes + caches result)

CREATE MATERIALIZED VIEW gold.ext_daily_revenue AS
    SELECT order_date, region, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM ext.orders_partitioned
    WHERE order_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY order_date, region;

-- Queries now hit the MV (pre-computed, stored in Snowflake) instead of S3!
-- Speed: 30 seconds → <1 second
-- The MV auto-refreshes when new files are detected (via AUTO_REFRESH on external table)
-- Best of both worlds: data stays in S3 (cheap) + queries are fast (MV cached)
```

---

## Performance Optimization

```sql
-- OPTIMIZATION 1: Use Parquet (columnar, compressed — much faster than JSON/CSV)
-- Parquet: reads only requested columns (column pruning)
-- JSON: must read entire record to get one field (no column pruning!)

-- OPTIMIZATION 2: Partition by query pattern
-- If queries filter by date: partition by date
-- Files: s3://lake/orders/date=2024-03-15/part-001.parquet
-- Query: WHERE date = '2024-03-15' → reads only 1 day's files (not all data!)

-- OPTIMIZATION 3: Right-size files (100-250 MB compressed)
-- Too small: overhead per file, many S3 API calls (slow)
-- Too large: can't skip irrelevant data within a file (reads too much)

-- OPTIMIZATION 4: Cluster within files by common filter columns
-- If queries filter by customer_id: sort data by customer_id within each file
-- Result: min/max metadata enables row-group skipping within Parquet files!

-- PERFORMANCE COMPARISON (1 TB data lake):
-- Unoptimized (JSON, no partitioning): 120 seconds per query
-- Parquet + date partitioning: 15 seconds
-- + MV on top: <1 second
-- Effort: format change + partition layout = 85% improvement for free!
```

---

## Security and Access Control

```sql
-- Storage integration (IAM role):
CREATE STORAGE INTEGRATION lake_access
    TYPE = EXTERNAL_STAGE, STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456:role/snowflake-lake-access'
    STORAGE_ALLOWED_LOCATIONS = ('s3://company-data-lake/');

-- Grant access to specific roles:
GRANT USAGE ON EXTERNAL TABLE ext.orders TO ROLE data_analysts;
-- Analysts can query the external table (Snowflake checks RBAC)
-- But: they can't access the raw S3 files directly (only through Snowflake)
-- Double protection: Snowflake RBAC + IAM role scoping
```

---

## Interview Tips

> **Tip 1:** "External tables vs Iceberg tables?" — External tables: read-only, Snowflake metadata on existing files. Iceberg tables: full DML (INSERT/UPDATE/DELETE), ACID transactions, open format (readable by Spark/Trino too). Use external for: existing data lake files you just want to query. Use Iceberg for: shared open-format tables with write support.

> **Tip 2:** "How do you make external table queries fast?" — (1) Parquet format (column pruning, compression), (2) Partition by date/key (partition pruning), (3) Right-size files (100-250 MB), (4) Materialized View on top (pre-computed cache). Combined: 100x faster than naive JSON + no partitioning.

> **Tip 3:** "When NOT to use external tables?" — When queries are frequent and latency-sensitive (use internal tables). When you need DML (UPDATE/DELETE) on non-Iceberg data. When you need Time Travel or clustering. External tables are best for: occasional queries on cold/archive data, data lake exploration, and shared storage with other tools.
