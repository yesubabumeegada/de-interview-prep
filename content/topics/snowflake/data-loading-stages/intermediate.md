---
title: "Snowflake Data Loading & Stages - Intermediate"
topic: snowflake
subtopic: data-loading-stages
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, data-loading, snowpipe, streaming, transformation, parquet, semi-structured]
---

# Snowflake Data Loading & Stages — Intermediate Concepts

## Transformation During Load

Don't load raw then transform — do it in one step with a SELECT in COPY INTO:

```sql
-- Transform during load: type casting, column reordering, computed columns
COPY INTO orders (order_id, customer_id, amount_cents, created_date, loaded_at)
FROM (
    SELECT
        $1::INT                                    AS order_id,
        $2::VARCHAR                                AS customer_id,
        ROUND($3::FLOAT * 100)::INT                AS amount_cents,   -- convert dollars to cents
        TRY_TO_DATE($4, 'MM/DD/YYYY')              AS created_date,   -- safe type cast
        CURRENT_TIMESTAMP()                         AS loaded_at       -- watermark
    FROM @s3_stage/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';
```

**Column position references (`$1`, `$2`)** refer to columns in the source file. Use `$1:field_name` for JSON.

---

## Loading Semi-Structured Data (JSON)

Snowflake's `VARIANT` type stores JSON natively:

```sql
-- Table with a VARIANT column
CREATE TABLE raw_events (
    loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    raw        VARIANT
);

-- Load JSON from stage — each line = one JSON object
COPY INTO raw_events (raw)
FROM @s3_stage/events/
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- Query JSON with colon notation
SELECT
    raw:event_id::STRING        AS event_id,
    raw:user_id::INT            AS user_id,
    raw:properties:page_url::STRING AS page_url,   -- nested access
    raw:tags[0]::STRING         AS first_tag       -- array access
FROM raw_events
WHERE raw:event_type::STRING = 'page_view';

-- Flatten nested arrays
SELECT
    r.raw:user_id::STRING AS user_id,
    f.value::STRING        AS tag
FROM raw_events r,
LATERAL FLATTEN(input => r.raw:tags) f;
```

---

## COPY INTO Validation and Error Handling

```sql
-- Dry run: validate without loading
COPY INTO orders
FROM @s3_stage/orders/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
VALIDATION_MODE = 'RETURN_ERRORS';   -- returns errors without loading
-- Also: RETURN_ALL_ERRORS, RETURN_5_ROWS

-- Check errors from last COPY INTO
SELECT * FROM TABLE(VALIDATE(orders, JOB_ID => LAST_QUERY_ID()));

-- COPY history: see all loads (90-day retention in INFORMATION_SCHEMA, 1-year in ACCOUNT_USAGE)
SELECT file_name, status, rows_loaded, errors_seen, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- Account-level copy history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE table_name = 'ORDERS'
  AND last_load_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());
```

---

## Loading Parquet: Column Name Matching

Parquet files carry schema metadata — use column name matching instead of positional `$1`:

```sql
CREATE TABLE fact_sales (
    sale_id     INT,
    customer_id VARCHAR,
    amount      FLOAT,
    sale_date   DATE
);

-- MATCH_BY_COLUMN_NAME auto-maps Parquet columns to table columns by name
COPY INTO fact_sales
FROM @s3_stage/parquet/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

-- With transformation (still works with Parquet)
COPY INTO fact_sales (sale_id, customer_id, amount, sale_date)
FROM (
    SELECT $1:sale_id::INT, $1:customer_id::STRING, $1:amount::FLOAT, $1:sale_date::DATE
    FROM @s3_stage/parquet/
)
FILE_FORMAT = (TYPE = 'PARQUET');
```

---

## Parallel Loading and Performance

```sql
-- PARALLEL option in PUT (upload speed)
PUT file:///data/*.csv @my_stage PARALLEL = 8;   -- 8 concurrent upload threads

-- COPY INTO runs parallel automatically — split large files for parallelism
-- Rule: 1 file per vCPU in the warehouse (XL = 16 vCPUs → ~16 files for max parallelism)

-- Check: how many files were processed in parallel?
SELECT file_name, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'ORDERS', START_TIME=>DATEADD('hour',-1,CURRENT_TIMESTAMP())))
ORDER BY last_load_time;
```

**Optimal file size:** 100–250 MB compressed (matches Snowflake micro-partition size). Too small = too much overhead. Too large = can't parallelize.

---

## Storage Integrations in Depth

```sql
-- Step 1: Create storage integration (admin step, done once)
CREATE STORAGE INTEGRATION my_s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://prod-data-lake/', 's3://staging-data-lake/');

-- Step 2: Get the IAM trust information
DESC INTEGRATION my_s3_int;
-- Returns STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- → Add these to AWS IAM role trust policy

-- Step 3: Create stage using integration (no credentials in SQL)
CREATE STAGE prod_s3
    URL = 's3://prod-data-lake/snowflake/'
    STORAGE_INTEGRATION = my_s3_int
    FILE_FORMAT = (TYPE = 'PARQUET');

-- List files — uses IAM role, not static keys
LIST @prod_s3;
```

---

## External Tables vs COPY INTO

| Approach | When Data Lands | Schema | Latency |
|----------|----------------|--------|---------|
| COPY INTO | On demand (batch) | Explicit table | Batch (minutes) |
| External Table | Immediately (read-through) | Defined on stage | Query-time |
| Snowpipe | Continuous (event-driven) | Explicit table | Near-real-time |

```sql
-- External table: query S3 without copying data in
CREATE EXTERNAL TABLE ext_events (
    event_id   VARCHAR AS ($1:event_id::STRING),
    event_type VARCHAR AS ($1:event_type::STRING),
    event_ts   TIMESTAMP AS ($1:ts::TIMESTAMP)
)
LOCATION = @s3_stage/events/
FILE_FORMAT = (TYPE = 'JSON');

-- Query works — data stays in S3, Snowflake reads on-demand
SELECT event_type, COUNT(*) FROM ext_events WHERE event_ts >= '2024-01-01' GROUP BY 1;
```

---

## Interview Tips

> **Tip 1:** "How do you handle schema changes when loading JSON?" — "Load the entire JSON into a VARIANT column — no schema defined upfront, no reloads needed when structure changes. Query the VARIANT with colon notation and cast to types at query time. Run `FLATTEN` for arrays. For structured queries, create a view on top that extracts the fields you need."

> **Tip 2:** "What happens when COPY INTO encounters a bad row?" — "Depends on `ON_ERROR`. `ABORT_STATEMENT` (default) rolls back the entire COPY. `CONTINUE` loads all good rows and records errors in metadata. `SKIP_FILE` skips the entire file containing bad rows. Best practice for production: use `CONTINUE`, then query `VALIDATE()` or `COPY_HISTORY` to inspect and reprocess rejected rows."

> **Tip 3:** "What's the difference between COPY HISTORY in INFORMATION_SCHEMA vs ACCOUNT_USAGE?" — "INFORMATION_SCHEMA: 14-day retention, query-level granularity, faster. ACCOUNT_USAGE: 1-year retention, aggregate stats per load, ~1h lag. Use INFORMATION_SCHEMA for debugging recent loads; ACCOUNT_USAGE for historical audits and chargeback reporting."
