---
title: "Snowflake Data Loading & Stages - Fundamentals"
topic: snowflake
subtopic: data-loading-stages
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, data-loading, stages, copy-into, put, get, csv, parquet]
---

# Snowflake Data Loading & Stages — Fundamentals

## 🎯 Analogy

A **stage** in Snowflake is like a loading dock for a warehouse. Before goods (data) go into storage (tables), they're offloaded at the dock (stage) first. The dock can be inside the building (internal stage) or at an external lot (external stage like S3). The `COPY INTO` command is the forklift that moves data from the dock to the shelves.

---

## What Is a Stage?

A **stage** is a named location (file storage) where data files sit before being loaded into a Snowflake table, or after being unloaded from one.

```
Files (CSV, JSON, Parquet, Avro, ORC, XML)
        ↓
   [STAGE: internal or external]
        ↓
   COPY INTO → Snowflake Table
```

---

## Types of Stages

### Internal Stages (Snowflake-Managed Storage)

| Stage Type | Syntax | Best For |
|------------|--------|---------|
| User stage | `@~` | Personal staging area per user |
| Table stage | `@%table_name` | Files destined for one specific table |
| Named stage | `@my_stage` | Shared, reusable staging area |

```sql
-- Named internal stage (most common in production)
CREATE STAGE my_internal_stage
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- List files in a stage
LIST @my_internal_stage;

-- Remove a file from stage
REMOVE @my_internal_stage/old_file.csv;
```

### External Stages (Cloud Storage)

Point directly at S3, Azure Blob, or GCP Cloud Storage:

```sql
-- AWS S3 external stage
CREATE STAGE s3_stage
    URL = 's3://my-data-bucket/raw/'
    CREDENTIALS = (AWS_KEY_ID = 'AKIAIOSFODNN7' AWS_SECRET_KEY = 'secret')
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Azure Blob external stage
CREATE STAGE azure_stage
    URL = 'azure://myaccount.blob.core.windows.net/container/raw/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '?sv=2020-...')
    FILE_FORMAT = (TYPE = 'CSV');

-- Better practice: use storage integration (no credentials in SQL)
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-data-bucket/');

CREATE STAGE s3_stage_secure
    URL = 's3://my-data-bucket/raw/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = (TYPE = 'PARQUET');
```

---

## File Formats

Define how Snowflake interprets files:

```sql
-- Named file format (reusable across stages and COPY commands)
CREATE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'UTF8';

CREATE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;   -- if file is an array [...] instead of one obj per line

CREATE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;
```

| Format | Typical Use |
|--------|------------|
| CSV | Relational data from databases, Excel exports |
| JSON | API responses, event logs |
| Parquet | Columnar analytics, data lake files |
| Avro | Kafka/streaming data |
| ORC | Hive/Hadoop exports |

---

## Uploading Files: PUT Command

`PUT` uploads local files to an internal stage (from SnowSQL CLI):

```bash
# Upload a local file to the named stage
PUT file:///home/user/data/sales_2024.csv @my_internal_stage AUTO_COMPRESS=TRUE;

# Upload all CSVs in a directory
PUT file:///home/user/data/*.csv @my_internal_stage PARALLEL=4;

# Upload to table stage
PUT file:///home/user/data/orders.csv @%orders;
```

> `PUT` is only available from **SnowSQL CLI** or Snowflake connectors — not from the Snowflake web UI (which has drag-and-drop upload instead).

---

## Loading Data: COPY INTO

Move files from a stage into a table:

```sql
-- Basic load from named stage
COPY INTO orders
FROM @my_internal_stage/orders_2024.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

-- Load with inline file format (no named format needed)
COPY INTO orders
FROM @s3_stage/orders/
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Load specific files using pattern
COPY INTO orders
FROM @s3_stage
PATTERN = '.*orders_2024_\d{2}\.csv'
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

-- Load from multiple files with column mapping
COPY INTO orders (order_id, customer_id, amount, created_at)
FROM (
    SELECT $1, $2, $3::NUMBER(10,2), TO_TIMESTAMP($4, 'YYYY-MM-DD HH24:MI:SS')
    FROM @s3_stage/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
```

---

## COPY INTO Options

| Option | Values | Purpose |
|--------|--------|---------|
| `ON_ERROR` | `CONTINUE`, `SKIP_FILE`, `ABORT_STATEMENT` | How to handle bad rows |
| `PURGE` | `TRUE/FALSE` | Delete files from stage after successful load |
| `FORCE` | `TRUE/FALSE` | Reload files already loaded (normally skipped) |
| `TRUNCATECOLUMNS` | `TRUE/FALSE` | Truncate strings to column max width instead of erroring |
| `MATCH_BY_COLUMN_NAME` | `CASE_SENSITIVE/INSENSITIVE` | Auto-map Parquet/JSON columns by name |

```sql
-- Skip bad rows, continue loading the rest
COPY INTO orders
FROM @s3_stage
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

-- Check what was rejected
SELECT * FROM TABLE(VALIDATE(orders, JOB_ID => '<last_query_id>'));
```

---

## Unloading Data: COPY INTO (Stage)

Export data FROM a table TO a stage:

```sql
-- Export to internal stage as CSV
COPY INTO @my_internal_stage/export/orders_
FROM orders
FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
OVERWRITE = TRUE
SINGLE = FALSE;     -- Multiple files (parallel), or TRUE for one file

-- Export as Parquet to S3
COPY INTO @s3_stage/exports/
FROM (SELECT order_id, customer_id, amount FROM orders WHERE year = 2024)
FILE_FORMAT = (TYPE = 'PARQUET')
HEADER = TRUE;

-- Download from internal stage (SnowSQL GET command)
-- GET @my_internal_stage/export/ file:///local/path/
```

---

## Try It Yourself

```sql
-- Full example: create stage, load CSV, verify

-- 1. Create file format
CREATE FILE FORMAT demo_csv
    TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 2. Create internal stage
CREATE STAGE demo_stage FILE_FORMAT = demo_csv;

-- 3. (From SnowSQL: PUT file://sales.csv @demo_stage)

-- 4. Preview file before loading
SELECT $1, $2, $3 FROM @demo_stage/sales.csv (FILE_FORMAT => 'demo_csv') LIMIT 10;

-- 5. Load into table
CREATE TABLE sales (id INT, amount FLOAT, sale_date DATE);
COPY INTO sales FROM @demo_stage/sales.csv;

-- 6. Verify load
SELECT COUNT(*) FROM sales;
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'SALES', START_TIME=>DATEADD('hour',-1,CURRENT_TIMESTAMP())));
```

---

## Interview Tips

> **Tip 1:** "What's the difference between an internal and external stage?" — "Internal stages store files in Snowflake-managed cloud storage (you don't see the S3 bucket). External stages point at your own S3/Azure/GCP bucket — Snowflake reads from it but doesn't own the storage. External stages are used when files are produced by external systems and you don't want to copy them twice."

> **Tip 2:** "How does Snowflake avoid reloading the same files?" — "COPY INTO tracks loaded files in metadata. By default, it skips files that have already been loaded (same path + file size + modification time). Use `FORCE = TRUE` to override. This makes COPY INTO idempotent — safe to re-run."

> **Tip 3:** "What's a storage integration and why use it?" — "A storage integration is an IAM role trust relationship between Snowflake and your cloud account. Instead of putting AWS keys directly in stage definitions (a security risk), you delegate access via IAM. The keys never appear in SQL — rotation is handled at the IAM level."
