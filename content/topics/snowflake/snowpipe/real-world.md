---
title: "Snowpipe - Real-World Production Examples"
topic: snowflake
subtopic: snowpipe
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, snowpipe, production, patterns, ingestion]
---

# Snowpipe — Real-World Production Examples

## Pattern 1: Complete Ingestion Pipeline

```sql
-- End-to-end: S3 files → Snowpipe → Raw → Stream → Task → Silver

-- Stage and file format
CREATE FILE FORMAT raw.json_format TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;
CREATE STAGE raw.landing_stage URL = 's3://company-lake/landing/'
    STORAGE_INTEGRATION = s3_integration FILE_FORMAT = raw.json_format;

-- Target table with metadata
CREATE TABLE raw.orders (
    data VARIANT,                          -- Raw JSON as VARIANT
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR DEFAULT METADATA$FILENAME,
    _file_row_number NUMBER DEFAULT METADATA$FILE_ROW_NUMBER
);

-- Snowpipe (auto-ingest from S3)
CREATE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders (data, _source_file, _file_row_number)
    FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @raw.landing_stage/orders/);

-- Stream on raw table
CREATE STREAM raw.orders_stream ON TABLE raw.orders;

-- Task: parse JSON and load to silver (every 10 min)
CREATE TASK silver.parse_orders
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '10 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
    INSERT INTO silver.orders
    SELECT 
        data:order_id::NUMBER AS order_id,
        data:customer_id::NUMBER AS customer_id,
        data:amount::DECIMAL(10,2) AS amount,
        data:order_date::DATE AS order_date,
        data:status::VARCHAR AS status,
        _loaded_at,
        _source_file
    FROM raw.orders_stream
    WHERE METADATA$ACTION = 'INSERT'
      AND data:order_id IS NOT NULL;

ALTER TASK silver.parse_orders RESUME;
```

---

## Pattern 2: Multi-Format Ingestion

```sql
-- Different file formats from different sources, all via Snowpipe

-- Source 1: JSON events from application
CREATE PIPE raw.events_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.events FROM @landing_stage/events/
    FILE_FORMAT = (TYPE = 'JSON');

-- Source 2: CSV from partner (pipe-delimited, has header)
CREATE PIPE raw.partner_data_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.partner_data FROM @landing_stage/partner/
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '|', SKIP_HEADER = 1, 
                   NULL_IF = ('', 'NULL', 'N/A'));

-- Source 3: Parquet from data lake (schema-on-read)
CREATE PIPE raw.lake_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.lake_data FROM @landing_stage/parquet/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Source 4: Avro from Kafka (via Kafka Connect S3 sink)
CREATE PIPE raw.kafka_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.kafka_events FROM @landing_stage/kafka/
    FILE_FORMAT = (TYPE = 'AVRO');

-- Each pipe independently processes its source files
-- One S3 bucket notification → filters by prefix → routes to correct pipe
```

---

## Pattern 3: Error Recovery and Reprocessing

```sql
-- Production monitoring and error recovery workflow

-- Step 1: Identify failed loads
CREATE OR REPLACE PROCEDURE ops.check_pipe_errors()
RETURNS TABLE (pipe_name VARCHAR, file_name VARCHAR, error_msg VARCHAR, load_time TIMESTAMP)
LANGUAGE SQL
AS
$$
    SELECT PIPE_NAME, FILE_NAME, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        START_TIME => DATEADD('hour', -6, CURRENT_TIMESTAMP())
    ))
    WHERE STATUS IN ('LOAD_FAILED', 'PARTIALLY_LOADED')
    ORDER BY LAST_LOAD_TIME DESC;
$$;

-- Step 2: Re-process failed files manually
-- Option A: Fix the file (correct format issues) and re-upload with new name
-- Snowpipe will pick up the new file automatically

-- Option B: Use COPY INTO with FORCE = TRUE for specific files
COPY INTO raw.orders 
FROM @landing_stage/orders/failed_file_fixed.json
FORCE = TRUE;  -- Bypasses load history (loads even if file was seen before)

-- Step 3: Automated alerting on failures
CREATE TASK ops.pipe_error_alert
    WAREHOUSE = 'OPS_WH_XS'
    SCHEDULE = '15 MINUTE'
AS
BEGIN
    LET error_count := (
        SELECT COUNT(*) FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            START_TIME => DATEADD('minute', -20, CURRENT_TIMESTAMP())
        )) WHERE STATUS = 'LOAD_FAILED'
    );
    
    IF (error_count > 0) THEN
        CALL system$send_email(
            'data-team@company.com',
            'Snowpipe Load Failures!',
            error_count || ' files failed to load in the last 20 minutes.'
        );
    END IF;
END;
```

---

## Pattern 4: Cost-Optimized Architecture

```sql
-- Different ingestion methods based on source characteristics

-- HIGH FREQUENCY + SMALL FILES (1000+ files/day, <10 MB each):
-- Use COPY INTO with scheduled task (cheaper than Snowpipe per-file)
CREATE TASK batch_ingest_events
    WAREHOUSE = 'LOAD_WH_XS'
    SCHEDULE = '5 MINUTE'
AS
    COPY INTO raw.events FROM @landing_stage/events/
    FILE_FORMAT = (TYPE = 'JSON');
-- Cost: ~$50/month (vs $3000/month with Snowpipe for 1000 files/day!)

-- MEDIUM FREQUENCY + MEDIUM FILES (10-100 files/day, 100-500 MB):
-- Use Snowpipe AUTO_INGEST (sweet spot for Snowpipe)
CREATE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders FROM @landing_stage/orders/ FILE_FORMAT = (TYPE = 'PARQUET');
-- Cost: ~$20/month (reasonable, hands-off)

-- LOW FREQUENCY + LARGE FILES (1-5 files/day, 1+ GB):
-- Use COPY INTO on schedule (cheapest for large files)
CREATE TASK daily_load
    WAREHOUSE = 'LOAD_WH_M'
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
    COPY INTO raw.daily_feed FROM @landing_stage/daily/
    FILE_FORMAT = (TYPE = 'PARQUET') PATTERN = '.*\.parquet';
-- Cost: ~$5/month (one warehouse start per day)

-- REAL-TIME EVENTS (continuous stream from application):
-- Use Snowpipe Streaming (sub-second, per-byte pricing)
-- Cost: ~$2.50/TB ingested (cheapest for high-frequency micro-inserts)
```

---

## Pattern 5: Snowpipe + dbt Integration

```sql
-- Pattern: Snowpipe loads raw data → dbt transforms → analytics-ready tables

-- Layer 1: Snowpipe (automatic ingestion)
-- Files → raw.orders, raw.customers, raw.events
-- (managed by Snowpipe, no dbt involvement)

-- Layer 2: dbt models read from raw tables
-- models/staging/stg_orders.sql:
-- SELECT order_id::NUMBER, amount::DECIMAL(10,2), ...
-- FROM {{ source('raw', 'orders') }}
-- WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})  -- Incremental

-- Layer 3: dbt builds silver/gold layers on schedule
-- dbt run triggered by: Airflow DAG, Snowflake Task, or GitHub Actions
-- Frequency: every 15-60 minutes (after Snowpipe has loaded new data)

-- Integration: Snowflake Stream tells dbt when new data is available
-- Task checks stream → if data exists → triggers dbt run
CREATE TASK trigger_dbt_run
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_stream')
AS
    -- Call external function that triggers dbt Cloud API
    SELECT system$trigger_dbt_cloud_run('job_id_12345');
```

---

## Interview Tips

> **Tip 1:** "Design a production Snowpipe setup for 20 data sources" — Group by characteristics: continuous (Snowpipe auto-ingest for file-based), scheduled (COPY INTO for bulk/cheap), streaming (Snowpipe Streaming for real-time events). One pipe per source table. S3 event notifications with prefix filters. Monitor via COPY_HISTORY. Alert on failures. Each pipe independent (one failure doesn't affect others).

> **Tip 2:** "How do you keep Snowpipe costs under control?" — (1) Consolidate small files before writing to S3 (biggest savings), (2) Use COPY INTO + Task for high-volume small files (per-task cost < per-file Snowpipe cost), (3) Use Snowpipe Streaming for application events (per-byte, not per-file), (4) Monitor: track files/day per pipe, compare cost vs COPY INTO alternative.

> **Tip 3:** "Snowpipe + Streams + Tasks: how do they work together?" — Snowpipe continuously loads files → raw table (minutes). Stream captures new rows in raw table (automatic). Task runs on schedule, consumes stream, transforms and loads silver table. End-to-end: file arrives → queryable in silver within 15-20 minutes. Each component is independent and failure-isolated.
