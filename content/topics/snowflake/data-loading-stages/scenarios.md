---
title: "Snowflake Data Loading & Stages - Scenario Questions"
topic: snowflake
subtopic: data-loading-stages
content_type: scenario_question
tags: [snowflake, data-loading, stages, scenarios, interview]
---

# Scenario Questions — Snowflake Data Loading & Stages

<article data-difficulty="junior">

## 🟢 Junior: Load a CSV File from S3

**Scenario:** You have daily sales CSV files landing in `s3://company-data/sales/` in the format `sales_YYYY-MM-DD.csv`. Each file has a header row, uses comma delimiters, and may have some NULL values represented as empty strings. Create a stage, file format, and COPY INTO to load these into a `sales` table.

<details>
<summary>✅ Solution</summary>

```sql
-- 1. Create storage integration (one-time setup by admin)
-- Assumes this already exists: CREATE STORAGE INTEGRATION si_s3 ...

-- 2. Create named stage pointing at S3 prefix
CREATE STAGE sales_s3_stage
    URL = 's3://company-data/sales/'
    STORAGE_INTEGRATION = si_s3;

-- 3. Create reusable file format
CREATE FILE FORMAT sales_csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('', 'NULL', 'null');

-- 4. Create target table
CREATE TABLE sales (
    sale_id     INT,
    customer_id VARCHAR,
    product_id  VARCHAR,
    amount      FLOAT,
    sale_date   DATE
);

-- 5. Load today's file
COPY INTO sales
FROM @sales_s3_stage/sales_2024-03-15.csv
FILE_FORMAT = (FORMAT_NAME = 'sales_csv_format');

-- 6. Verify
SELECT COUNT(*), MIN(sale_date), MAX(sale_date) FROM sales;
```

**Key decisions:**
- `STORAGE_INTEGRATION` over inline credentials — never put AWS keys in SQL
- `EMPTY_FIELD_AS_NULL = TRUE` handles empty CSV fields correctly
- After load, always verify row count and date ranges match expectations

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Design a Snowpipe Pipeline for Real-Time Events

**Scenario:** A mobile app publishes click events to S3 via Kinesis Firehose every 60 seconds. Each file is JSON (one event per line). Design a Snowpipe pipeline to load events into a `click_events` table within 2 minutes of landing. Handle potential duplicates.

<details>
<summary>✅ Solution</summary>

```sql
-- 1. Stage + file format
CREATE STAGE mobile_events_stage
    URL = 's3://mobile-data/click-events/'
    STORAGE_INTEGRATION = si_mobile_s3;

CREATE FILE FORMAT json_nl_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE;  -- one JSON object per line (newline-delimited JSON)

-- 2. Landing table (raw, with metadata for deduplication)
CREATE TABLE click_events_raw (
    raw          VARIANT,
    _loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Create pipe
CREATE PIPE click_events_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Loads Firehose click events from S3'
AS
COPY INTO click_events_raw (raw)
FROM @mobile_events_stage
FILE_FORMAT = (FORMAT_NAME = 'json_nl_format');
-- After creating: SHOW PIPES → get NOTIFICATION_CHANNEL SQS ARN
-- Add SQS ARN to S3 bucket event notification (All object create events)

-- 4. Deduplicated view (query on this, not raw table)
CREATE OR REPLACE VIEW click_events AS
SELECT
    raw:event_id::STRING    AS event_id,
    raw:user_id::STRING     AS user_id,
    raw:session_id::STRING  AS session_id,
    raw:element::STRING     AS element_clicked,
    raw:page_url::STRING    AS page_url,
    raw:ts::TIMESTAMP       AS event_ts,
    _loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY raw:event_id::STRING ORDER BY _loaded_at DESC) AS rn
    FROM click_events_raw
) WHERE rn = 1;

-- 5. Monitor
SELECT SYSTEM$PIPE_STATUS('click_events_pipe');
```

**Why raw VARIANT + view:** Avoids schema changes if the app adds new event fields. The view evolves; the table stays fixed.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Recover from a Failed Bulk Load That Partially Loaded Data

**Scenario:** A 200 GB Parquet bulk load ran overnight. The job failed after processing 60% of files due to a network timeout. The `fact_orders` table now has partial data — ~6M rows loaded, 4M missing. The files are all in S3. How do you complete the load safely without duplicating the 6M rows already loaded?

<details>
<summary>✅ Solution</summary>

**Key insight:** Snowflake tracks every file loaded via COPY INTO in metadata for 64 days. Re-running COPY INTO on the same stage will automatically skip already-loaded files — this is the default behavior (`FORCE = FALSE`).

```sql
-- Step 1: Verify what was already loaded
SELECT file_name, rows_loaded, status, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FACT_ORDERS',
    START_TIME => DATEADD('hour', -12, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time;
-- You'll see ~60% of files with status='LOADED'

-- Step 2: Simply re-run the original COPY INTO — it will skip already-loaded files
COPY INTO fact_orders
FROM @s3_stage/orders/2024-03-15/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ON_ERROR = 'CONTINUE';
-- Only unloaded files are processed — 0 duplicates from previously loaded files

-- Step 3: Verify completion
SELECT COUNT(*) FROM fact_orders WHERE order_date = '2024-03-15';
-- Should match expected 10M rows

-- Step 4: Check for any errors in the resumed load
SELECT file_name, errors_seen, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FACT_ORDERS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE errors_seen > 0;

-- Step 5: Cross-check totals
-- If you have a source manifest or row count from the upstream system, validate:
SELECT
    COUNT(*) AS snowflake_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(amount) AS total_revenue
FROM fact_orders
WHERE order_date = '2024-03-15';
```

**Important caveats:**
- This only works if the table and load used the same stage path and COPY INTO session — Snowflake tracks by stage + file path
- If files were moved or stage was recreated, use `FORCE = FALSE` explicitly and verify with COPY_HISTORY before proceeding
- If duplicate rows are suspected, use Time Travel to compare pre/post counts: `SELECT COUNT(*) FROM fact_orders AT (OFFSET => -3600)`

</details>
</article>
