---
title: "Snowpipe - Scenario Questions"
topic: snowflake
subtopic: snowpipe
content_type: scenario_question
tags: [snowflake, snowpipe, interview, scenarios, ingestion]
---

# Scenario Questions — Snowpipe

<article data-difficulty="junior">

## 🟢 Junior: Setting Up Snowpipe

**Scenario:** JSON files land in `s3://company-data/landing/orders/` every 5 minutes. Set up Snowpipe to automatically load them into a Snowflake table `raw.orders`.

<details>
<summary>💡 Hint</summary>
Create: stage (points to S3), table (target), file format (JSON), and pipe (AUTO_INGEST=TRUE with COPY INTO statement).
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: File format
CREATE OR REPLACE FILE FORMAT raw.json_ff TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;

-- Step 2: External stage
CREATE OR REPLACE STAGE raw.orders_stage
    URL = 's3://company-data/landing/orders/'
    STORAGE_INTEGRATION = my_s3_integration
    FILE_FORMAT = raw.json_ff;

-- Step 3: Target table
CREATE OR REPLACE TABLE raw.orders (
    order_id NUMBER,
    customer_id NUMBER,
    amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR DEFAULT METADATA$FILENAME
);

-- Step 4: Create pipe
CREATE OR REPLACE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders (order_id, customer_id, amount, order_date, status)
    FROM (
        SELECT $1:order_id, $1:customer_id, $1:amount, $1:order_date, $1:status
        FROM @raw.orders_stage
    );

-- Step 5: Get SQS queue ARN for S3 event notification
SHOW PIPES LIKE 'orders_pipe';
-- Copy the notification_channel value (SQS ARN)
-- Configure S3 bucket: Event Notifications → send ObjectCreated events → this SQS ARN

-- Step 6: Verify
SELECT SYSTEM$PIPE_STATUS('raw.orders_pipe');
-- Should show: executionState = 'RUNNING'
```

**Key Points:**
- AUTO_INGEST = TRUE: Snowpipe listens for S3 events automatically
- STORAGE_INTEGRATION: IAM role-based access (no access keys in SQL!)
- METADATA$FILENAME: captures source file for traceability
- One-time S3 setup: configure event notification → Snowpipe's SQS queue
- After setup: files land → loaded within 1-2 minutes (zero maintenance)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Monitoring Snowpipe

**Scenario:** Your Snowpipe has been running for a week. How do you check: (A) if it's currently active, (B) how many files loaded today, (C) if any files failed?

<details>
<summary>💡 Hint</summary>
Use SYSTEM$PIPE_STATUS for current state, COPY_HISTORY for load details, and filter by STATUS for failures.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- (A) Check if pipe is active:
SELECT SYSTEM$PIPE_STATUS('raw.orders_pipe');
-- Returns JSON: {"executionState":"RUNNING","pendingFileCount":0}
-- executionState: RUNNING = active, PAUSED = stopped
-- pendingFileCount: 0 = caught up, >0 = files waiting to be processed

-- (B) Files loaded today:
SELECT 
    COUNT(*) AS files_loaded,
    SUM(ROW_COUNT) AS total_rows,
    SUM(FILE_SIZE) / 1024 / 1024 AS total_mb
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('day', 0, CURRENT_DATE())  -- Since midnight
))
WHERE STATUS = 'LOADED';

-- (C) Failed files:
SELECT 
    FILE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUM,
    ERROR_COUNT,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE STATUS IN ('LOAD_FAILED', 'PARTIALLY_LOADED')
ORDER BY LAST_LOAD_TIME DESC;
```

**Key Points:**
- `SYSTEM$PIPE_STATUS`: real-time status (running? files pending?)
- `COPY_HISTORY`: historical view of all load operations (loaded, failed, skipped)
- STATUS values: LOADED (success), LOAD_FAILED (error), PARTIALLY_LOADED (some rows failed)
- FIRST_ERROR_MESSAGE: tells you exactly what went wrong in the file
- pendingFileCount growing = pipe can't keep up (investigate file size or format issues)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cost Optimization

**Scenario:** Your Snowpipe processes 5,000 small files per day (2-5 MB each). Monthly Snowpipe cost: $900. The data only needs to be available within 15 minutes. Reduce costs by 80%.

<details>
<summary>💡 Hint</summary>
5000 files × $0.06/file = expensive per-file overhead for tiny files. Switch to COPY INTO with a scheduled task every 15 minutes — loads all pending files in one warehouse operation.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- CURRENT: Snowpipe per-file
-- 5000 files/day × $0.06/file × 30 days = $9,000/month (!)
-- Actually closer to $900 because my estimate of $0.06 was high
-- Real cost: 5000 × ~$0.006 = $30/day × 30 = $900/month

-- OPTIMIZED: COPY INTO with scheduled task (batch processing)
CREATE OR REPLACE TASK batch_ingest_orders
    WAREHOUSE = 'LOAD_WH_XS'  -- Extra-small warehouse
    SCHEDULE = '15 MINUTE'     -- Every 15 minutes (meets SLA!)
AS
    COPY INTO raw.orders FROM @raw.orders_stage
    FILE_FORMAT = (TYPE = 'JSON')
    PATTERN = '.*\.json';

ALTER TASK batch_ingest_orders RESUME;

-- Cost calculation:
-- XS warehouse: 1 credit/hour
-- Runtime per batch: ~30 seconds (loading ~50 files × 3 MB = 150 MB)
-- Daily: 96 runs × 30 sec = 48 min = 0.8 credits
-- Monthly: 0.8 × 30 = 24 credits × $3 = $72/month

-- SAVINGS: $900 → $72 = 92% reduction!

-- Trade-off:
-- Latency: 1-2 min (Snowpipe) → up to 15 min (task schedule)
-- Since SLA allows 15 min: perfectly acceptable!

-- ALSO: Pause the Snowpipe
ALTER PIPE raw.orders_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
-- Don't pay for both!
```

**Key Points:**
- Snowpipe per-file cost adds up fast for many small files
- COPY INTO + Task: loads ALL pending files in one operation (per-warehouse-second, not per-file)
- For 5000 small files: COPY INTO is 10-90% cheaper depending on warehouse size
- The trade-off is latency (minutes → Snowpipe, 15+ min → scheduled COPY)
- If SLA allows 15-minute latency: always use COPY INTO for small files
- If SLA requires <2 min: Snowpipe is necessary (pay the premium)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Snowpipe + Streams Integration

**Scenario:** Files land via Snowpipe into `raw.orders`. You need to transform and load into `silver.orders` incrementally (only new data, not full table scan each time). Design the pipeline using Streams + Tasks.

<details>
<summary>💡 Hint</summary>
Create a stream on raw.orders (captures Snowpipe inserts). Task consumes stream periodically and MERGEs into silver. Stream advances after successful MERGE.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Layer 1: Snowpipe loads raw data continuously
-- (Already set up: raw.orders_pipe → raw.orders)
-- Files arrive → loaded within 1-2 minutes

-- Layer 2: Stream tracks new rows in raw.orders
CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders;

-- Layer 3: Task transforms and loads to silver every 10 min
CREATE OR REPLACE TASK silver.orders_etl
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '10 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
    MERGE INTO silver.orders t
    USING (
        SELECT 
            data:order_id::NUMBER AS order_id,
            data:customer_id::NUMBER AS customer_id,
            data:amount::DECIMAL(10,2) AS amount,
            TRY_TO_DATE(data:order_date::VARCHAR) AS order_date,
            data:status::VARCHAR AS status,
            _loaded_at,
            _source_file
        FROM raw.orders_stream
        WHERE METADATA$ACTION = 'INSERT'
          AND data:order_id IS NOT NULL
          AND data:amount > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY data:order_id ORDER BY _loaded_at DESC) = 1
    ) s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        t.amount = s.amount, t.status = s.status, t.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (order_id, customer_id, amount, order_date, status, created_at, updated_at)
        VALUES (s.order_id, s.customer_id, s.amount, s.order_date, s.status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

ALTER TASK silver.orders_etl RESUME;

-- End-to-end flow:
-- File arrives in S3 → Snowpipe loads to raw (1-2 min)
-- Stream captures the new rows → Task runs every 10 min
-- MERGE into silver: dedup + type + validate
-- Total latency: 1-12 minutes (file arrival to silver)
```

**Key Points:**
- Snowpipe: handles ingestion (raw layer, no transformation)
- Stream: tracks what's new in raw (automatic, no polling)
- Task + MERGE: transforms and deduplicates into silver (incremental!)
- WHEN clause: task skips if no new data (saves compute)
- QUALIFY ROW_NUMBER: handles duplicates within the same batch
- This is the standard Snowflake incremental ETL pattern

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: High-Volume Ingestion Architecture

**Scenario:** Design ingestion for an e-commerce platform: 50K orders/hour (during peak), 5M events/hour, data from 3 regions (US, EU, APAC). Requirements: <5 min latency for orders, <15 min for events, total cost under $3K/month.

<details>
<summary>💡 Hint</summary>
Orders (high priority, lower volume): Snowpipe or Snowpipe Streaming for <5 min. Events (high volume, relaxed SLA): COPY INTO every 15 min (cheaper). Separate by region for isolation. Cost: optimize for each tier.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- ARCHITECTURE: Tiered ingestion by priority and volume

-- TIER 1: Orders (50K/hour, <5 min SLA) — Snowpipe
-- 50K orders/hour ≈ 14 orders/second, ~5 MB/minute
-- File pattern: partner writes files every 1 minute to S3
CREATE PIPE raw.orders_us_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders FROM @landing/us/orders/ FILE_FORMAT = (TYPE='PARQUET');
CREATE PIPE raw.orders_eu_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders FROM @landing/eu/orders/ FILE_FORMAT = (TYPE='PARQUET');
CREATE PIPE raw.orders_apac_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders FROM @landing/apac/orders/ FILE_FORMAT = (TYPE='PARQUET');
-- Cost: 3 regions × ~1000 files/day × $0.006/file = $18/day = $540/month
-- Latency: 1-2 minutes ✓ (within 5 min SLA)

-- TIER 2: Events (5M/hour, <15 min SLA) — COPY INTO (cheaper!)
-- 5M events/hour ≈ 1400/second, ~500 MB/hour
-- Files batch every 5 minutes, ~100 MB each
CREATE TASK ingest_events_us
    WAREHOUSE = 'LOAD_WH_S'
    SCHEDULE = '10 MINUTE'
AS COPY INTO raw.events FROM @landing/us/events/ FILE_FORMAT = (TYPE='JSON');

CREATE TASK ingest_events_eu WAREHOUSE = 'LOAD_WH_S' SCHEDULE = '10 MINUTE'
AS COPY INTO raw.events FROM @landing/eu/events/ FILE_FORMAT = (TYPE='JSON');

CREATE TASK ingest_events_apac WAREHOUSE = 'LOAD_WH_S' SCHEDULE = '10 MINUTE'
AS COPY INTO raw.events FROM @landing/apac/events/ FILE_FORMAT = (TYPE='JSON');
-- Cost: SMALL warehouse × 144 runs/day × 30 sec/run = 1.2 hrs/day × $4 = $4.80/day = $144/month
-- Latency: up to 10 min ✓ (within 15 min SLA)

-- TIER 3: Transformation (Streams + Tasks)
CREATE STREAM orders_stream ON TABLE raw.orders;
CREATE TASK silver_orders WAREHOUSE = 'ETL_WH_S' SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS MERGE INTO silver.orders USING orders_stream ...;
-- Cost: ~$200/month (frequent small task)

-- TOTAL COST:
-- Orders Snowpipe: $540/month
-- Events COPY INTO: $144/month
-- Silver transform: $200/month
-- Gold aggregation: $100/month
-- TOTAL: ~$984/month ✓ (well under $3K budget!)

-- WHY this works:
-- Orders (high priority): Snowpipe gives 1-2 min latency (meets 5 min SLA)
-- Events (high volume): COPY INTO is 10x cheaper per-file than Snowpipe
-- Regional separation: each region fails independently
-- Streams: only process NEW data (not full table scan)
```

**Key Points:**
- Tier by priority: high-priority → Snowpipe (fast, expensive per-file), low-priority → COPY INTO (batch, cheap)
- Events (high volume, relaxed SLA): COPY INTO saves 80% vs Snowpipe
- Regional pipes: failure isolation (EU issue doesn't affect US)
- Stream + Task for transformation: incremental, not full scan
- Total: $984/month for 5M+ events/hour across 3 regions (cost-efficient!)
- Scale-out ready: add more pipes/tasks per region as volume grows

</details>

</article>
