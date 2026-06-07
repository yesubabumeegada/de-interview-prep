---
title: "Snowpipe - Intermediate"
topic: snowflake
subtopic: snowpipe
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, snowpipe, streaming, error-handling, optimization, monitoring]
---

# Snowpipe — Intermediate

## Snowpipe Streaming (Snowpipe v2)

Snowpipe Streaming provides **sub-second latency** by inserting rows directly via API (no files needed):

```python
# Snowpipe Streaming: insert rows via SDK (no file staging!)
from snowflake.snowpark import Session
from snowflake.connector import SnowflakeConnection

# Direct row insertion (sub-second latency):
# Your application sends rows → Snowflake ingests immediately
# No file creation, no S3, no event notifications

# Use case: real-time event streaming from applications
# (IoT sensors, clickstream, transaction events)

# Snowpipe Streaming is the Snowflake equivalent of "Kafka consumer writing to table"
# But native, serverless, and managed by Snowflake

# Configuration:
# Channel → Table mapping
# Application → Snowpipe Streaming API → Snowflake Table
# Latency: sub-second (vs 1-2 minutes for standard Snowpipe)
```

### Snowpipe Streaming vs Standard Snowpipe

| Aspect | Snowpipe (File-based) | Snowpipe Streaming |
|--------|----------------------|-------------------|
| Input | Files in cloud storage | Rows via API |
| Latency | 1-2 minutes | Sub-second |
| Trigger | S3 event notification | API call |
| Best for | File-based sources | Application events |
| Cost | Per-file | Per-row (very cheap at scale) |
| SDK | REST API or auto-ingest | Java/Python SDK |

---

## Error Handling Strategies

### ON_ERROR Options

```sql
-- Control what happens when a file has errors:

-- ABORT_STATEMENT (default for COPY INTO): stop on first error
COPY INTO raw.orders FROM @stage
    ON_ERROR = 'ABORT_STATEMENT';
-- All or nothing: if ANY row fails, nothing is loaded

-- CONTINUE: skip bad rows, load the rest
COPY INTO raw.orders FROM @stage
    ON_ERROR = 'CONTINUE';
-- Good for: dirty data where some rows are expected to fail

-- SKIP_FILE: skip entire file if it has errors
COPY INTO raw.orders FROM @stage
    ON_ERROR = 'SKIP_FILE';
-- Good for: corrupted files (don't partially load)

-- SKIP_FILE_n: skip file if error count exceeds n
COPY INTO raw.orders FROM @stage
    ON_ERROR = 'SKIP_FILE_3';
-- Skip if >3 errors in one file (tolerate a few bad rows)

-- For Snowpipe: errors are handled at file level
-- Bad files go to an error state (viewable in COPY_HISTORY)
-- Pipe continues processing other files (doesn't stop!)
```

### Error Monitoring

```sql
-- Find files that failed to load
SELECT 
    FILE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUM,
    FIRST_ERROR_CHARACTER_POS,
    ERROR_COUNT,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'LOADED'  -- Failed or partially loaded
ORDER BY LAST_LOAD_TIME DESC;

-- Common errors:
-- "Number of columns in file does not match" → schema mismatch
-- "Numeric value '' is not recognized" → empty string in numeric field
-- "Date '' is not recognized" → invalid date format
```

---

## Multi-Table Ingestion Pattern

```sql
-- Pattern: one S3 path → multiple tables based on file prefix

-- Files organized by type:
-- s3://bucket/landing/orders/2024/03/15/order_001.json
-- s3://bucket/landing/customers/2024/03/15/cust_001.json
-- s3://bucket/landing/events/2024/03/15/event_001.json

-- Create separate pipes for each prefix:
CREATE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.orders FROM @landing_stage/orders/
    FILE_FORMAT = (TYPE = 'JSON');

CREATE PIPE raw.customers_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.customers FROM @landing_stage/customers/
    FILE_FORMAT = (TYPE = 'JSON');

CREATE PIPE raw.events_pipe AUTO_INGEST = TRUE AS
    COPY INTO raw.events FROM @landing_stage/events/
    FILE_FORMAT = (TYPE = 'JSON');

-- Each pipe independently processes files from its prefix
-- One pipe failure doesn't affect others
-- S3 notifications can use prefix filters (reduce noise)
```

---

## Snowpipe + Streams Integration

```sql
-- Complete pattern: Snowpipe → Raw Table → Stream → Task → Silver Table

-- 1. Snowpipe loads files into raw table automatically
-- (files arrive → loaded within 1-2 min)

-- 2. Stream captures new rows in raw table
CREATE STREAM raw_orders_stream ON TABLE raw.orders;

-- 3. Task processes stream into silver (every 15 min)
CREATE TASK silver_orders_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_stream')
AS
    MERGE INTO silver.orders t
    USING (SELECT * FROM raw_orders_stream WHERE METADATA$ACTION = 'INSERT') s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET t.amount = s.amount
    WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.amount, s.order_date);

-- End-to-end latency:
-- File arrives → Snowpipe loads (1-2 min) → Stream captures → Task runs (0-15 min)
-- Total: 1-17 minutes from file arrival to silver table
```

---

## Performance Optimization

### File Sizing

```sql
-- OPTIMAL file size for Snowpipe: 100-250 MB (compressed)
-- Too small (<10 MB): overhead per file exceeds content (expensive)
-- Too large (>500 MB): single file takes too long to load

-- If your source produces tiny files (1-5 MB):
-- Option 1: Buffer on producer side (collect for 5 min, then write larger file)
-- Option 2: Accept the cost (Snowpipe handles tiny files, just more expensive)
-- Option 3: Use COPY INTO on a schedule instead (batch many small files)

-- If your source produces huge files (>1 GB):
-- Snowpipe handles them fine, but consider splitting for faster parallel load
```

### Parallel Loading

```sql
-- Snowpipe automatically parallelizes loading:
-- Multiple files landing simultaneously → loaded in parallel
-- No configuration needed (Snowflake manages parallelism)

-- For high-volume scenarios (1000+ files/hour):
-- Snowpipe handles this natively (scales serverless compute)
-- Monitor: SYSTEM$PIPE_STATUS shows pendingFileCount
-- If pendingFileCount grows over time → ingestion can't keep up
-- Fix: typically file format/size issue (not Snowpipe capacity)
```

---

## Security and Access Control

```sql
-- Storage integration (IAM role for S3 access):
CREATE STORAGE INTEGRATION my_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-access'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/landing/');

-- Grant usage to roles:
GRANT USAGE ON INTEGRATION my_s3_integration TO ROLE etl_role;
GRANT OPERATE ON PIPE raw.orders_pipe TO ROLE etl_role;

-- Pipe ownership:
-- The pipe owner needs: INSERT on target table, USAGE on stage, USAGE on file format
-- Service account (not personal) should own production pipes
```

---

## Snowpipe with Kafka (via Kafka Connector)

```python
# Snowflake Kafka Connector: Kafka → Snowpipe → Table
# (Alternative to Snowpipe Streaming for Kafka sources)

# Configuration in Kafka Connect:
KAFKA_CONNECTOR_CONFIG = {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "topics": "orders,events,customers",
    "snowflake.url.name": "myaccount.snowflakecomputing.com",
    "snowflake.user.name": "kafka_user",
    "snowflake.private.key": "...",
    "snowflake.database.name": "RAW",
    "snowflake.schema.name": "KAFKA",
    "snowflake.topic2table.map": "orders:ORDERS,events:EVENTS",
    "buffer.count.records": "10000",     # Buffer before flushing to Snowflake
    "buffer.flush.time": "60",           # Or flush every 60 seconds
    "buffer.size.bytes": "5000000",      # Or flush at 5 MB
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",  # Use streaming (fastest)
}

# Flow: Kafka → Kafka Connect → Snowpipe Streaming → Snowflake Table
# Latency: <10 seconds end-to-end
```

---

## Interview Tips

> **Tip 1:** "Snowpipe vs Snowpipe Streaming?" — Standard Snowpipe: file-based (S3 events → load file → 1-2 min latency). Snowpipe Streaming: row-based (API inserts → sub-second latency). Use standard for: files from batch systems, partner drops. Use Streaming for: real-time application events, IoT, Kafka consumers.

> **Tip 2:** "How do you handle errors in Snowpipe?" — Snowpipe skips files with errors (pipe continues for other files). Check COPY_HISTORY for failed files with error details. Common pattern: monitor COPY_HISTORY for STATUS != 'LOADED', alert on failures, fix source data, manually re-trigger failed files via REST API.

> **Tip 3:** "Optimal file size for Snowpipe?" — 100-250 MB compressed. Too small (<10 MB): per-file overhead makes it expensive. Too large (>500 MB): slower to load. If source produces tiny files: buffer them before writing to S3, or accept higher per-file cost. If source produces huge files: consider splitting for faster parallel ingestion.
