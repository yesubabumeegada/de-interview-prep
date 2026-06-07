---
title: "External Tables - Real-World Production Examples"
topic: snowflake
subtopic: external-tables
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, external-tables, production, data-lake, patterns]
---

# Snowflake External Tables — Real-World Production Examples

## Pattern 1: Data Lake Query Layer

```sql
-- Company has 50 TB data lake on S3 (managed by Spark)
-- Analysts need SQL access without loading into Snowflake

-- Create external tables for each major dataset:
CREATE EXTERNAL TABLE ext.events (
    event_id VARCHAR AS (VALUE:event_id::VARCHAR),
    user_id NUMBER AS (VALUE:user_id::NUMBER),
    event_type VARCHAR AS (VALUE:event_type::VARCHAR),
    event_time TIMESTAMP AS (VALUE:event_time::TIMESTAMP),
    properties VARIANT AS (VALUE:properties::VARIANT)
)
PARTITION BY (event_date DATE)
LOCATION = @lake_stage/events/ PARTITION_TYPE = HIVE
FILE_FORMAT = (TYPE = 'PARQUET') AUTO_REFRESH = TRUE;

-- Analysts query:
SELECT event_type, COUNT(*) FROM ext.events
WHERE event_date = '2024-03-15' GROUP BY event_type;
-- Reads from S3 directly (no Snowflake storage cost for 50 TB lake!)

-- For frequent queries: add MV cache
CREATE MATERIALIZED VIEW gold.mv_event_counts AS
    SELECT event_date, event_type, COUNT(*) AS count
    FROM ext.events WHERE event_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY event_date, event_type;
```

## Pattern 2: Archive Access

```sql
-- 3 years of historical data in S3 Glacier (cheap storage)
-- Occasional queries for compliance/audit
CREATE EXTERNAL TABLE ext.historical_orders (...)
LOCATION = @archive_stage/orders/ FILE_FORMAT = (TYPE = 'PARQUET');

-- Combined view: internal (fast, recent) + external (slow, historical)
CREATE VIEW unified.orders AS
    SELECT * FROM production.orders WHERE order_date >= '2023-01-01'
    UNION ALL
    SELECT * FROM ext.historical_orders WHERE order_date < '2023-01-01';
```

## Pattern 3: Cross-Platform Data Sharing

```sql
-- Spark writes Iceberg tables → Snowflake queries them
-- No data duplication between platforms!
CREATE ICEBERG TABLE ext.spark_output
    EXTERNAL_VOLUME = 'lake_volume'
    CATALOG = 'AWS_GLUE'
    CATALOG_TABLE_NAME = 'spark_feature_store';

-- Data scientists use Spark for feature engineering
-- Analysts query the same data via Snowflake SQL
-- One dataset, two engines, zero duplication!
```

---

## Interview Tips

> **Tip 1:** "Production use case for external tables?" — Data lake query layer: 50 TB on S3, managed by Spark, analysts query via Snowflake external tables (SQL access to the lake without loading). Add MVs for hot queries. Result: $0 Snowflake storage for 50 TB, full SQL access, fast for common queries (MV), acceptable for ad-hoc (direct S3 read).

> **Tip 2:** "Hot/cold pattern in production?" — Internal tables for last 30-90 days (BI dashboards need speed). External tables for historical data (compliance queries, occasional analysis). Unified view makes the split transparent to users. Saves 50-70% on storage while maintaining same query interface.

> **Tip 3:** "Iceberg in production?" — Spark writes Iceberg tables to S3 (heavy ETL). Snowflake queries the same tables (analyst SQL). Both see consistent ACID snapshots. No data movement between systems. This is the open lakehouse pattern — choose the best engine for each workload without duplicating data.
