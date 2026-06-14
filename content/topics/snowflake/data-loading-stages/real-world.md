---
title: "Snowflake Data Loading & Stages - Real-World Examples"
topic: snowflake
subtopic: data-loading-stages
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, data-loading, production, pipeline, kafka, snowpipe, batch]
---

# Snowflake Data Loading & Stages — Real-World Production Examples

## Production Pattern: Multi-Source Ingestion Hub

An e-commerce company ingests from 6 different source systems into one Snowflake account:

```
PostgreSQL (orders) ─── Airbyte ──→ S3 (Parquet) ──→ COPY INTO (hourly task)
Kafka (events) ──────── Kafka Connector ──────────→ Snowpipe Streaming (real-time)
Salesforce (CRM) ─────── dbt Cloud + Singer ──────→ COPY INTO (daily)
S3 Data Lake (raw) ───── External Stage ──────────→ External Table (query-time)
MySQL (inventory) ─────── AWS DMS → S3 ───────────→ Snowpipe (event-driven)
Webhooks (payments) ───── Lambda → Firehose → S3 → Snowpipe (event-driven)
```

**Stage strategy:**
```sql
-- One stage per source system (separate IAM roles, separate prefixes)
CREATE STAGE stg_postgres   URL = 's3://data-lake/postgres/'   STORAGE_INTEGRATION = si_s3;
CREATE STAGE stg_kafka      URL = 's3://data-lake/kafka/'      STORAGE_INTEGRATION = si_s3;
CREATE STAGE stg_salesforce URL = 's3://data-lake/salesforce/' STORAGE_INTEGRATION = si_s3;

-- Organized landing schema
CREATE SCHEMA raw.postgres_landing;
CREATE SCHEMA raw.kafka_landing;
CREATE SCHEMA raw.salesforce_landing;
```

---

## Production Incident: Duplicate Orders from Snowpipe

**What happened:** Snowpipe re-processed a backlog of 48 hours of order files after a connectivity issue. ~850,000 duplicate order rows appeared in `fact_orders`. Downstream revenue reports doubled overnight.

**Root cause:** Orders were loaded with simple `INSERT` via COPY INTO — no deduplication.

**Remediation:**
```sql
-- Step 1: Identify duplicates
SELECT order_id, COUNT(*) AS cnt
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
-- Found 850,432 duplicate order_ids

-- Step 2: Remove duplicates using Time Travel
-- Get table state from before the bad load
CREATE TABLE fact_orders_clean CLONE fact_orders
    AT (TIMESTAMP => '2024-03-15 08:00:00'::TIMESTAMP_TZ);

-- Step 3: Swap in the clean table
ALTER TABLE fact_orders RENAME TO fact_orders_bad;
ALTER TABLE fact_orders_clean RENAME TO fact_orders;

-- Step 4: Fix the pipe to use idempotent MERGE
CREATE TASK orders_dedup_task
    WAREHOUSE = etl_wh
    SCHEDULE = '2 minutes'
AS
MERGE INTO fact_orders t
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
        FROM raw.orders_stream
    ) WHERE rn = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED AND s._loaded_at > t._loaded_at THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Prevention:** All Snowpipe targets now have MERGE-based deduplication tasks. Pure INSERT is only used for immutable append-only tables (like raw event logs with surrogate keys).

---

## Production Pattern: CSV Bulk Load with Data Validation

Monthly data vendor drops 200 GB of CSV files to S3. Full load pipeline:

```sql
-- Step 1: Count files and estimate load
LIST @vendor_stage/2024-03/;

-- Step 2: Validate sample before full load
COPY INTO orders_staging
FROM @vendor_stage/2024-03/orders_0001.csv
FILE_FORMAT = (FORMAT_NAME = 'vendor_csv')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Step 3: Full load with CONTINUE (don't let one bad file block everything)
COPY INTO orders_staging
FROM @vendor_stage/2024-03/
FILE_FORMAT = (FORMAT_NAME = 'vendor_csv')
ON_ERROR = 'CONTINUE'
FORCE = FALSE;  -- skip already-loaded files (idempotent re-runs)

-- Step 4: Capture errors for review
CREATE TABLE load_errors_2024_03 AS
SELECT * FROM TABLE(VALIDATE(orders_staging, JOB_ID => LAST_QUERY_ID()));

-- Step 5: Quality check
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_ids,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    MIN(order_date) AS earliest_date,
    MAX(order_date) AS latest_date
FROM orders_staging
WHERE _load_batch = '2024-03';

-- Step 6: Promote staging → production only if quality passes
INSERT INTO fact_orders
SELECT order_id, customer_id, amount, order_date, CURRENT_TIMESTAMP() AS loaded_at
FROM orders_staging
WHERE _load_batch = '2024-03'
  AND order_id IS NOT NULL
  AND amount > 0;
```

---

## Production Pattern: Zero-Copy Cloning for Load Testing

```sql
-- Load new data format to a clone first (no storage cost for unchanged data)
CREATE TABLE orders_test CLONE orders;

-- Test new load pipeline against clone
COPY INTO orders_test
FROM @s3_stage/new_format/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Compare row counts
SELECT 'original' AS tbl, COUNT(*) FROM orders
UNION ALL
SELECT 'test', COUNT(*) FROM orders_test;

-- Compare distributions
SELECT 'original' AS tbl, AVG(amount), STDDEV(amount) FROM orders
UNION ALL
SELECT 'test', AVG(amount), STDDEV(amount) FROM orders_test;

-- Only if validation passes: swap into production
-- (blue-green deployment pattern)
ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE orders_test RENAME TO orders;
DROP TABLE orders_old;
```

---

## Production: Monitoring Load Pipeline Health

Dashboard query for a data engineering team:

```sql
-- Last 24h load summary per table
SELECT
    table_name,
    COUNT(*) AS load_count,
    SUM(rows_loaded) AS total_rows_loaded,
    SUM(errors_seen) AS total_errors,
    MAX(last_load_time) AS last_load,
    DATEDIFF('minute', MAX(last_load_time), CURRENT_TIMESTAMP()) AS minutes_since_last_load
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE last_load_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY table_name
ORDER BY minutes_since_last_load DESC;

-- Pipe health check
SELECT
    pipe_name,
    PARSE_JSON(SYSTEM$PIPE_STATUS(pipe_name)):executionState::STRING AS state,
    PARSE_JSON(SYSTEM$PIPE_STATUS(pipe_name)):pendingFileCount::INT AS pending_files,
    PARSE_JSON(SYSTEM$PIPE_STATUS(pipe_name)):lastIngestedTimestamp::STRING AS last_ingested
FROM (VALUES ('orders_pipe'), ('events_pipe'), ('inventory_pipe')) AS t(pipe_name);
```
