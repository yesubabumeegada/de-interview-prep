---
title: "AWS Athena - Intermediate"
topic: aws-services
subtopic: athena
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, athena, optimization, partition-projection, iceberg, federated-queries]
---

# AWS Athena — Intermediate Concepts

## Partition Projection — Skip Catalog Lookups

For tables with many partitions (millions), querying the Glue Catalog for partition info becomes slow. Partition projection tells Athena to CALCULATE partition locations from a pattern instead:

```sql
CREATE EXTERNAL TABLE events (
    event_id STRING, user_id STRING, event_type STRING, amount DOUBLE
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://lake/events/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2020,2030',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'projection.day.type' = 'integer',
    'projection.day.range' = '1,31',
    'storage.location.template' = 's3://lake/events/year=${year}/month=${month}/day=${day}/'
);
-- No MSCK REPAIR TABLE needed! Athena generates paths mathematically.
-- Query planning: catalog lookup 30s → instant calculation (<1s)
```

**When to use projection:**
- Tables with >10,000 partitions (catalog lookup becomes a bottleneck)
- Predictable partition patterns (date-based, sequential IDs)
- New partitions added frequently (no need to register each one)

---

## Iceberg Tables in Athena

Athena v3 supports Apache Iceberg for ACID operations on S3:

```sql
-- Create an Iceberg table
CREATE TABLE iceberg_orders (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE
)
PARTITIONED BY (order_date)
LOCATION 's3://lake/iceberg/orders/'
TBLPROPERTIES ('table_type' = 'ICEBERG');

-- INSERT (standard)
INSERT INTO iceberg_orders VALUES ('O-001', 'C-100', 99.99, DATE '2024-01-15');

-- UPDATE (not possible with plain Parquet on S3!)
UPDATE iceberg_orders SET amount = 109.99 WHERE order_id = 'O-001';

-- DELETE
DELETE FROM iceberg_orders WHERE order_date < DATE '2023-01-01';

-- MERGE (upsert)
MERGE INTO iceberg_orders t
USING staging_orders s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.amount, s.order_date);

-- Time travel
SELECT * FROM iceberg_orders FOR TIMESTAMP AS OF TIMESTAMP '2024-01-14 10:00:00';

-- Schema evolution
ALTER TABLE iceberg_orders ADD COLUMNS (shipping_status STRING);
```

> **Game changer:** Iceberg on Athena gives you warehouse-like capabilities (UPDATE, DELETE, MERGE, time travel) on serverless S3 storage. No Redshift or Glue needed for simple transformations.

---

## Federated Queries — Query Other Data Sources

Athena can query data in RDS, DynamoDB, and other sources directly:

```sql
-- Create a connector to RDS
-- (Deploy the Athena connector Lambda via AWS console)

-- Query RDS directly from Athena
SELECT * FROM "lambda:rds-connector".mydb.public.customers
WHERE signup_date > DATE '2024-01-01';

-- Join S3 data with RDS data in one query!
SELECT c.name, SUM(o.amount) AS total_spent
FROM s3_catalog.curated.fact_orders o
JOIN "lambda:rds-connector".mydb.public.customers c 
    ON o.customer_id = c.id
WHERE o.order_date >= DATE '2024-01-01'
GROUP BY c.name;
```

**Supported connectors:** RDS (PostgreSQL, MySQL), DynamoDB, Redshift, CloudWatch Logs, DocumentDB, ElasticSearch, and custom (write your own Lambda connector).

---

## CTAS and INSERT INTO for ETL

Use Athena as a lightweight ETL tool (no Glue/Spark needed):

```sql
-- CTAS: Create new table from query result
CREATE TABLE curated.daily_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://lake/curated/daily_summary/',
    partitioned_by = ARRAY['order_date'],
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    customer_id,
    SUM(amount) AS daily_total,
    COUNT(*) AS order_count,
    order_date
FROM raw.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY customer_id, order_date;

-- INSERT INTO: append new data to existing table
INSERT INTO curated.daily_summary
SELECT customer_id, SUM(amount), COUNT(*), order_date
FROM raw.orders
WHERE order_date = CURRENT_DATE - INTERVAL '1' DAY
GROUP BY customer_id, order_date;
```

**CTAS vs Glue for ETL:**

| Use CTAS (Athena) | Use Glue |
|-------------------|----------|
| SQL-only transforms (filter, join, aggregate) | Complex PySpark logic |
| One-time conversions | Recurring scheduled jobs |
| Quick ad-hoc transforms | Bookmark-based incremental loads |
| < 100 GB data | > 100 GB (Athena may timeout) |

---

## Workgroups — Cost and Access Control

```sql
-- Separate workgroups for different teams
-- Each workgroup has: query result location, scan limit, tags

-- Workgroup: analysts (max 10 GB per query, output to their bucket)
-- Workgroup: etl-team (max 500 GB per query, output to ETL bucket)
-- Workgroup: finance (max 50 GB, specific databases only)
```

**Per-query cost control:**
- Set "Per-query data usage control" = 10 GB
- Any query that would scan >10 GB fails immediately (prevents accidental full scans)
- Useful for teams new to the data lake (prevents $50 queries from typos)

---

## Query Performance Optimization

| Technique | Impact | Implementation |
|-----------|--------|---------------|
| Parquet/ORC format | 5-10x less scanned | CTAS to convert from CSV/JSON |
| Partition pruning | 10-100x less scanned | `WHERE year=2024 AND month=1` |
| Column selection | 2-10x less scanned | `SELECT col1, col2` not `SELECT *` |
| Partition projection | Faster query planning | Table property configuration |
| File size (128 MB-1 GB) | Fewer S3 requests | Compact small files |
| Bucketing (Athena v3) | Better join performance | `CLUSTERED BY (col) INTO N BUCKETS` |
| Compression (Snappy/Zstd) | 3-5x less scanned | Set in CTAS/TBLPROPERTIES |

---

## Interview Tips

> **Tip 1:** "How do you handle UPDATE/DELETE on S3 via Athena?" — "Use Iceberg tables. Standard Parquet on S3 doesn't support updates. With Iceberg, Athena supports full DML (INSERT, UPDATE, DELETE, MERGE) with ACID transactions. Iceberg handles the underlying file management (copy-on-write or merge-on-read)."

> **Tip 2:** "What is partition projection?" — "Instead of querying the Glue Catalog for partition locations (slow with millions of partitions), partition projection tells Athena to CALCULATE paths from a pattern. Athena never queries the catalog for partitions — it generates 's3://bucket/year=X/month=Y/day=Z/' mathematically. Speeds up query planning from 30s to <1s."

> **Tip 3:** "Can Athena replace Redshift?" — "For some workloads, yes. Athena + Iceberg gives you DML on S3 with ACID transactions. For ad-hoc analytics with infrequent access, Athena is cheaper (pay-per-query vs always-on cluster). But Redshift still wins for: high-concurrency dashboards, sub-second latency, complex multi-join queries, and workloads with many repeated queries (caching, materialized views)."
