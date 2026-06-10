---
title: "External Tables - Scenario Questions"
topic: snowflake
subtopic: external-tables
content_type: scenario_question
tags: [snowflake, external-tables, interview, scenarios]
---

# Scenario Questions — External Tables

<article data-difficulty="junior">

## 🟢 Junior: When to Use External Tables

**Scenario:** Your company has 10 TB of event data in S3 (Parquet, Hive-partitioned by date). Analysts want SQL access. Loading all 10 TB into Snowflake would cost $230/month in storage. The data is queried ~5 times/day. Should you use external tables or load it?

<details>
<summary>💡 Hint</summary>
Compare: storage cost ($230/mo for internal) vs query performance trade-off. With only 5 queries/day, the slower external table performance is acceptable. Storage savings matter more than sub-second speed for 5 queries/day.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- RECOMMENDATION: External Table (saves $230/month!)

-- 5 queries/day = low frequency (external table's slower speed is acceptable)
-- 10 TB = significant storage cost to avoid
-- Parquet + Hive partitioning = external tables perform well!

CREATE EXTERNAL TABLE ext.events (
    event_id VARCHAR AS (VALUE:event_id::VARCHAR),
    user_id NUMBER AS (VALUE:user_id::NUMBER),
    event_type VARCHAR AS (VALUE:event_type::VARCHAR),
    event_time TIMESTAMP AS (VALUE:event_time::TIMESTAMP)
)
PARTITION BY (event_date DATE)
LOCATION = @s3_stage/events/ PARTITION_TYPE = HIVE
FILE_FORMAT = (TYPE = 'PARQUET') AUTO_REFRESH = TRUE;

-- Performance: 10-30 seconds per query (vs 2-5 seconds if internal)
-- Acceptable for 5 queries/day? YES!
-- Storage savings: $230/month (10 TB × $23/TB)

-- If queries increase to 50+/day or need <5s response:
-- Load the most-queried subset into internal table
-- Keep the rest in external table
-- Hybrid approach: best of both worlds
```

**Key Points:**
- 5 queries/day with 10-30s each = totally acceptable (not a dashboard with hundreds of users)
- $230/month storage savings is significant for infrequent queries
- Parquet + Hive partitioning makes external tables perform reasonably well
- If query volume increases later: load hot data internally (hybrid approach)
- Decision factor: query frequency × storage cost → low frequency = external wins

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Partitioned External Table

**Scenario:** Create a partitioned external table over S3 data organized as: `s3://lake/orders/year=2024/month=03/day=15/file.parquet`. Ensure queries filtering by date only read the relevant partition's files.

<details>
<summary>💡 Hint</summary>
Use PARTITION BY with PARTITION_TYPE = HIVE. Snowflake auto-detects the year/month/day structure and prunes partitions when queries filter on these columns.
</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE EXTERNAL TABLE ext.orders_partitioned (
    order_id NUMBER AS (VALUE:order_id::NUMBER),
    customer_id NUMBER AS (VALUE:customer_id::NUMBER),
    amount DECIMAL(10,2) AS (VALUE:amount::DECIMAL(10,2)),
    status VARCHAR AS (VALUE:status::VARCHAR)
)
PARTITION BY (year VARCHAR, month VARCHAR, day VARCHAR)
WITH LOCATION = @lake_stage/orders/
PARTITION_TYPE = HIVE
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE;

-- Query with partition pruning:
SELECT customer_id, SUM(amount) AS total
FROM ext.orders_partitioned
WHERE year = '2024' AND month = '03' AND day = '15'
GROUP BY customer_id;
-- ONLY reads files in: s3://lake/orders/year=2024/month=03/day=15/
-- Skips all other dates entirely!

-- Without partitioning: would read ALL files (all dates) = 100x more data!
-- With partitioning: reads 1 day out of 365+ days = ~0.3% of data scanned
```

**Key Points:**
- PARTITION_TYPE = HIVE: auto-detects key=value directory structure
- Partition columns (year, month, day) are derived from the file path
- Query with WHERE on partition columns = partition pruning (only relevant files read)
- AUTO_REFRESH = TRUE: new partitions (new dates) detected automatically
- This is the #1 performance optimization for external tables

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Open Lakehouse with Iceberg

**Scenario:** Design an architecture where: Spark writes data to S3 (ETL), Snowflake queries it (analytics), and both see the same consistent data. Use Apache Iceberg for ACID consistency. Show how to set it up.

<details>
<summary>💡 Hint</summary>
Iceberg provides ACID across engines. Spark writes Iceberg tables to S3. Register them in AWS Glue Catalog. Snowflake reads via Iceberg external table pointing to the same Glue catalog entry.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- ARCHITECTURE:
-- Spark (writer) → S3 Iceberg table → Glue Catalog → Snowflake (reader)
-- Both engines see ACID-consistent snapshots of the same data!

-- STEP 1: Spark writes Iceberg table (ETL)
-- (PySpark code)
-- df.writeTo("glue_catalog.production.orders").using("iceberg").createOrReplace()
-- Data lands in: s3://lake/iceberg/production/orders/ (Parquet + Iceberg metadata)

-- STEP 2: Create external volume in Snowflake (S3 access)
CREATE EXTERNAL VOLUME iceberg_lake_vol
    STORAGE_LOCATIONS = (
        (NAME = 'lake_s3', STORAGE_BASE_URL = 's3://lake/iceberg/',
         STORAGE_PROVIDER = 'S3', STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123:role/sf-iceberg')
    );

-- STEP 3: Register Iceberg table in Snowflake (points to Glue catalog)
CREATE ICEBERG TABLE ext.spark_orders
    EXTERNAL_VOLUME = 'iceberg_lake_vol'
    CATALOG = 'AWS_GLUE'
    CATALOG_TABLE_NAME = 'production.orders';

-- STEP 4: Query from Snowflake (sees Spark's latest committed data!)
SELECT order_date, region, SUM(amount) AS revenue
FROM ext.spark_orders
WHERE order_date >= '2024-01-01'
GROUP BY order_date, region;
-- Reads the SAME Parquet files that Spark wrote
-- Sees the LATEST Iceberg snapshot (ACID consistency!)

-- STEP 5: Both engines work on same data without duplication:
-- Spark: writes daily ETL output to Iceberg
-- Snowflake: queries for BI/analytics (always sees latest committed state)
-- Athena/Trino: can also query same table (open format!)

-- BENEFITS:
-- Zero data duplication (one copy on S3)
-- ACID across engines (no partial reads)
-- Open format (no vendor lock-in)
-- Each engine used for its strength (Spark=ETL, Snowflake=SQL/BI)
```

**Key Points:**
- Iceberg = open table format that provides ACID across any engine
- Spark writes (ETL) → Snowflake reads (analytics) — same files, consistent
- AWS Glue Catalog: shared catalog that both engines reference
- No data duplication (single copy on S3 serves all engines)
- This is the "open lakehouse" pattern (vs proprietary single-engine approach)
- Snowflake-managed Iceberg: Snowflake can also WRITE Iceberg (not just read)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Snowflake External Table?**
A: An External Table is a Snowflake object that allows querying files stored in an external stage (S3, GCS, Azure Blob) as if they were a native Snowflake table. The data never moves into Snowflake storage; queries read directly from the cloud object store.

**Q: What is the VALUE column in an External Table?**
A: External Tables expose raw file content through a special VARIANT column named `VALUE`. Each row represents one record from the file, and you access fields using semi-structured notation (e.g., `VALUE:field_name::STRING`). Column expressions can be added to project specific fields as typed columns.

**Q: What is an External Table partition and how does it improve performance?**
A: External Tables support partition columns derived from file path patterns (e.g., year/month/day folders). Declaring these as partition columns allows Snowflake to prune files at query time—only files matching the partition predicate are scanned. Without partitioning, every query scans all files in the stage.

**Q: What is AUTO_REFRESH for External Tables and how does it work?**
A: AUTO_REFRESH uses Snowflake's event infrastructure (SNS for S3, Azure Event Grid, GCS Pub/Sub) to automatically detect when new files land in the external stage and update the External Table's metadata. Without it, you must manually call `ALTER EXTERNAL TABLE ... REFRESH` to pick up new files.

**Q: What are the performance trade-offs of External Tables vs. native Snowflake tables?**
A: External Tables are slower—queries must read directly from object storage without Snowflake's micro-partition pruning, local caching, or columnar compression benefits. Native tables benefit from all of these. External Tables are best for large cold data you rarely query or for keeping data in its source location for governance reasons.

**Q: What file formats are supported for External Tables?**
A: Snowflake External Tables support Parquet, ORC, Avro, JSON, CSV, and other formats that Snowflake can natively read. Parquet is the most efficient choice because its columnar format allows column pruning during reads from object storage.

**Q: How does an External Table differ from a Snowflake Stage?**
A: A Stage is a pointer to an external storage location used for loading/unloading. An External Table builds a SQL-queryable abstraction on top of a stage, with schema definition and optional partitioning. You can query a stage directly with `SELECT` from `@stage`, but External Tables provide better ergonomics, partition pruning, and metadata tracking.

**Q: When would you use an External Table over loading the data into Snowflake?**
A: Use External Tables when data must remain in the external store for regulatory/data residency reasons, when querying infrequently accessed cold data where ingestion cost isn't justified, or when you want to query data that another system writes continuously without managing a separate ingestion pipeline.

---

## 💼 Interview Tips

- Lead with the key trade-off: External Tables offer flexibility and avoid data duplication, but they're significantly slower than native tables. Show you'd quantify this before recommending them in production.
- Mention partition columns and AUTO_REFRESH together—they're the two primary levers for making External Tables usable in production. Omitting either signals surface-level knowledge.
- Be honest about when NOT to use External Tables. If a table is queried frequently by many users, the performance penalty of external storage outweighs the convenience—native Snowflake tables with Snowpipe ingestion are usually the right answer.
- Senior interviewers may ask about Iceberg Tables (Snowflake's newer capability for open-table-format external data)—be aware this is the evolution of external table patterns and positions Snowflake for data lakehouse architectures.
- Connect External Tables to data lake architectures: they're the bridge between a Snowflake warehouse and a cloud data lake, enabling a lakehouse query pattern without full data migration.
