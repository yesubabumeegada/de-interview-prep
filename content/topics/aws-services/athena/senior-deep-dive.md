---
title: "AWS Athena - Senior Deep Dive"
topic: aws-services
subtopic: athena
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, athena, performance, cost-management, architecture, query-engine]
---

# AWS Athena — Senior-Level Deep Dive

## Athena Engine Versions

| Version | Engine | Key Features |
|---------|--------|-------------|
| Athena v2 | Presto 0.217 | Standard SQL, partitions, CTAS |
| Athena v3 | Trino 388+ | Iceberg/Hudi/Delta, MERGE, better performance, Spark notebook |

**v3 improvements over v2:**
- 2-5x faster query performance (improved optimizer)
- Apache Iceberg, Hudi, Delta Lake table format support
- MERGE, UPDATE, DELETE support
- Better partition pruning and predicate pushdown
- Views, prepared statements, parameterized queries

---

## Cost Engineering at Scale

### Understanding Athena Pricing

```
Cost = Data Scanned (bytes) × $5/TB
Minimum charge per query: 10 MB (even if query scans less)
DDL operations (CREATE TABLE, ALTER): Free
Failed queries: Not charged
Cancelled queries: Charged for data already scanned
```

### Cost Reduction Formula

```
Original cost (CSV, no partitions, SELECT *):
  1 TB × $5 = $5.00 per query

After optimization (Parquet + partitioning + column pruning):
  1 TB × 0.2 (Parquet compression) × 0.03 (partition = 1/30 days) × 0.3 (3 of 10 columns)
  = 1 TB × 0.0018 = 1.8 GB scanned
  Cost: 1.8 GB × $5/TB = $0.009 per query

Savings: $5.00 → $0.009 = 99.8% reduction
```

### Preventing Expensive Queries

```sql
-- Workgroup setting: fail queries that scan > N bytes
-- Set per-query limit to 100 GB in workgroup configuration
-- Any query exceeding this limit fails immediately with an error

-- Monitor costs programmatically:
-- Query execution details include bytes_scanned
-- Set up CloudWatch alarm on TotalBytesScanned metric
```

---

## Advanced Query Patterns

### Approximate Queries (Faster, Cheaper)

```sql
-- Exact COUNT DISTINCT: expensive (scans and deduplicates all values)
SELECT COUNT(DISTINCT user_id) FROM events WHERE year = 2024;
-- Scans: 500 GB, Time: 45 seconds

-- Approximate: HyperLogLog (2% error, much faster)
SELECT approx_distinct(user_id) FROM events WHERE year = 2024;
-- Scans: 500 GB, Time: 15 seconds (less memory/compute)

-- Approximate percentiles
SELECT approx_percentile(amount, 0.95) AS p95_amount FROM orders;
```

### Window Functions (Athena SQL)

```sql
-- Sessionization directly in Athena (no Spark needed)
WITH gap_detection AS (
    SELECT user_id, event_time,
        DATE_DIFF('second', 
            LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
            event_time
        ) AS gap_seconds
    FROM events WHERE year = 2024 AND month = 1
),
session_flags AS (
    SELECT *, CASE WHEN gap_seconds > 1800 OR gap_seconds IS NULL THEN 1 ELSE 0 END AS new_session
    FROM gap_detection
)
SELECT user_id, event_time,
    SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
FROM session_flags;
```

### Querying Nested JSON

```sql
-- S3 contains JSON with nested structures:
-- {"user": {"id": "U1", "name": "Alice"}, "events": [{"type": "click"}, {"type": "view"}]}

CREATE EXTERNAL TABLE raw_json (
    user struct<id: string, name: string>,
    events array<struct<type: string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://lake/raw/json_events/';

-- Query nested fields
SELECT 
    user.id AS user_id,
    user.name,
    event.type AS event_type
FROM raw_json
CROSS JOIN UNNEST(events) AS t(event)
WHERE user.id = 'U1';
```

---

## Athena + Step Functions for SQL-Based ETL

```json
{
    "StartAt": "ExtractRaw",
    "States": {
        "ExtractRaw": {
            "Type": "Task",
            "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
            "Parameters": {
                "QueryString": "INSERT INTO curated.orders SELECT * FROM raw.orders WHERE dt = '${date}'",
                "WorkGroup": "etl-workgroup",
                "ResultConfiguration": {"OutputLocation": "s3://athena-results/etl/"}
            },
            "Next": "ValidateCount"
        },
        "ValidateCount": {
            "Type": "Task",
            "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
            "Parameters": {
                "QueryString": "SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END FROM curated.orders WHERE dt = '${date}'"
            },
            "Next": "CheckResult"
        },
        "CheckResult": {
            "Type": "Choice",
            "Choices": [{"Variable": "$.ResultSet.Rows[1].Data[0].VarCharValue", "StringEquals": "PASS", "Next": "Success"}],
            "Default": "Fail"
        },
        "Success": {"Type": "Succeed"},
        "Fail": {"Type": "Fail", "Error": "DataValidationFailed"}
    }
}
```

> **Athena as ETL engine:** For SQL-only transformations, you don't need Glue or Spark at all. Step Functions orchestrate Athena queries directly. Cost: $5/TB scanned (may be cheaper than Glue DPU-hours for small-medium data).

---

## Multi-Account Data Lake Query Pattern

```sql
-- Cross-account: query another account's S3 data via Glue resource sharing

-- Account A (data owner): shares Glue database via RAM (Resource Access Manager)
-- Account B (consumer): creates a resource link in their catalog

-- In Account B:
CREATE DATABASE LINK shared_data_link
    FROM 'arn:aws:glue:us-east-1:ACCOUNT_A:database/curated';

-- Query Account A's data as if it were local
SELECT * FROM shared_data_link.fact_orders WHERE order_date = CURRENT_DATE;
-- Data stays in Account A's S3 — no copying
-- Cost: Account B pays for scanned bytes (query cost), Account A pays for S3 storage
```

---

## Performance Comparison by Data Format

Tested on 1 TB of order data (same content, different formats):

| Format | Size on S3 | Query Time (full scan) | Cost/Query | Query Time (partitioned + column pruned) |
|--------|-----------|----------------------|-----------|----------------------------------------|
| CSV (uncompressed) | 1 TB | 120 sec | $5.00 | 45 sec / $0.50 |
| CSV (gzip) | 250 GB | 60 sec | $1.25 | 25 sec / $0.13 |
| JSON | 800 GB | 90 sec | $4.00 | 40 sec / $0.40 |
| Parquet (snappy) | 150 GB | 15 sec | $0.75 | 2 sec / $0.008 |
| ORC (zstd) | 130 GB | 13 sec | $0.65 | 2 sec / $0.007 |

> **Conclusion:** Converting CSV → Parquet = 85% storage reduction + 8x faster + 99% cost reduction (with partitioning + column selection).

---

## Interview Tips

> **Tip 1:** "Design a cost-effective analytics layer on S3" — "Store data as Parquet with Snappy compression, partitioned by date. Use Athena with partition projection for fast planning on millions of partitions. Set workgroup per-query scan limits to prevent expensive mistakes. For repeated queries, pre-compute results with CTAS and refresh daily. Expected cost: $0.01-0.10 per typical analytical query vs $5+ for unoptimized."

> **Tip 2:** "Athena vs Spark/Glue for ETL?" — "Athena for: SQL-only transforms, small-medium data, one-time migrations, ad-hoc data quality checks. Spark/Glue for: complex Python logic, UDFs, large-scale (100+ GB per run), streaming, machine learning. Athena is simpler and cheaper for SQL-expressible work."

> **Tip 3:** "How do you handle Athena query failures at scale?" — "Three layers: (1) Workgroup scan limits prevent runaway queries, (2) Step Functions with retry + error handling orchestrate multi-step SQL jobs, (3) CloudWatch alarms on failed query count and bytes scanned. For idempotency: use INSERT OVERWRITE (Iceberg) or CTAS with IF NOT EXISTS + DROP if re-running."
