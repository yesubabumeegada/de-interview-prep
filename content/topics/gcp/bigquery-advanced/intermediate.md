---
title: "BigQuery Advanced — Intermediate"
topic: gcp
subtopic: bigquery-advanced
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [gcp, bigquery, security, optimization, storage-api, interview]
---

# BigQuery Advanced — Intermediate

At the mid-level, you're expected to own the security model of your BigQuery datasets, tune queries for production SLAs, and integrate BigQuery with external compute (Spark, Dataflow). This layer covers column-level security, row-level security, Storage Read API, query optimization patterns, and cost controls.

---

## Column-Level Security with Policy Tags

BigQuery integrates with **Data Catalog Policy Tags** to implement column-level access control. A policy tag is a label from a hierarchical taxonomy that you attach to sensitive columns. Access to a tagged column requires the user or service account to have the `Fine-Grained Reader` role on that specific tag.

### Setup Flow

```
Data Catalog Taxonomy
└─ "PII" (policy tag group)
    ├─ "email_address"  (policy tag)
    └─ "ssn"            (policy tag)
```

```sql
-- Attach a policy tag to a column during table creation
CREATE TABLE `project.dataset.customers` (
  customer_id INT64,
  name STRING,
  email STRING OPTIONS (policy_tags = '["projects/my-project/locations/us/taxonomies/123/policyTags/456"]'),
  phone STRING OPTIONS (policy_tags = '["projects/my-project/locations/us/taxonomies/123/policyTags/789"]')
);
```

```bash
# Grant fine-grained reader access to a specific policy tag
gcloud data-catalog taxonomies policy-tags add-iam-policy-binding \
  projects/my-project/locations/us/taxonomies/123/policyTags/456 \
  --member="user:analyst@company.com" \
  --role="roles/datacatalog.categoryFineGrainedReader"
```

Users without `Fine-Grained Reader` on the `email_address` policy tag will receive an error if they SELECT that column — even if they have `roles/bigquery.dataViewer` on the dataset. Masking rules can be configured to show NULL or HASH values instead of errors.

---

## Row-Level Security with Data Policies

**Row Access Policies** (row-level security, RLS) restrict which rows a user can see within a table. You define a filter expression per policy and assign it to users/groups.

```sql
-- Policy: region-us analysts only see US rows
CREATE ROW ACCESS POLICY us_rows_only
ON `project.dataset.sales`
GRANT TO ("group:us-analysts@company.com")
FILTER USING (region = 'US');

-- Policy: all other authenticated users see EMEA rows
CREATE ROW ACCESS POLICY emea_rows
ON `project.dataset.sales`
GRANT TO ("group:emea-analysts@company.com")
FILTER USING (region = 'EMEA');
```

**Key behavior**: if a user has no matching row access policy, they see zero rows (not an error). The `FILTER USING` expression is applied as a WHERE clause before any user query runs. Row access policies combine with column-level policy tags — you can have both simultaneously.

**Important limitation**: aggregation queries (SUM, COUNT) are subject to RLS — a user summing revenue only sums rows they can see. This is intentional but must be documented to avoid data discrepancy confusion across teams.

---

## BigQuery Storage Read API

The **BigQuery Storage Read API** provides a high-throughput, columnar read path (Apache Arrow or Avro format) for reading BigQuery data from external compute like Apache Spark, Dataflow, or custom applications. It replaces the older `tabledata.list` REST method, which was slow and non-parallel.

### Why It Matters for Data Engineers

- Spark jobs that read from BigQuery use this API via the `spark-bigquery-connector`.
- Arrow format allows zero-copy deserialization — much faster than converting through JSON.
- Supports server-side column projection and row filtering — Spark's `pushdown` filters are sent to BigQuery, reducing data transferred.

```python
# PySpark: reading BigQuery table using Storage Read API
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("bq-storage-api-demo") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
    .getOrCreate()

df = spark.read \
    .format("bigquery") \
    .option("table", "project.dataset.orders") \
    .option("filter", "order_date >= '2024-01-01'") \  # pushed down to BQ
    .option("readDataFormat", "ARROW") \
    .load()

df.groupBy("customer_id").agg({"revenue": "sum"}).show()
```

The connector creates multiple read **streams** in parallel (one per Spark partition), each scanning a shard of the BigQuery table simultaneously — this is how petabyte-scale reads remain fast.

---

## Query Optimization Techniques

### Partition Pruning

Always filter on the partition column in your WHERE clause. BigQuery's query plan shows `partition_filter: true` in INFORMATION_SCHEMA if pruning is active.

```sql
-- Good: partition pruning active on event_date
SELECT COUNT(*) FROM `project.dataset.events`
WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31';

-- Bad: function on partition column prevents pruning
SELECT COUNT(*) FROM `project.dataset.events`
WHERE EXTRACT(YEAR FROM event_date) = 2024;  -- full scan!
```

### UNNEST and Array Optimization

BigQuery stores ARRAY columns efficiently. UNNEST flattens them — use `LEFT JOIN UNNEST` to avoid dropping rows with empty arrays.

```sql
-- Efficient: UNNEST with CROSS JOIN (implicit inner join)
SELECT order_id, item
FROM `project.dataset.orders`, UNNEST(line_items) AS item
WHERE order_date = '2024-11-01';
```

### Approximate Aggregations

For analytics that tolerate ~1% error, approximate functions are dramatically faster and cheaper:

```sql
-- Exact (expensive for high-cardinality columns):
SELECT COUNT(DISTINCT user_id) FROM events;  -- requires full shuffle

-- Approximate (much faster, uses HLL++ sketches):
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;

-- Approximate quantiles:
SELECT APPROX_QUANTILES(revenue, 100) FROM orders;
```

### Avoid SELECT *

`SELECT *` in BigQuery scans all columns regardless of which you need. In a table with 100 columns, selecting only 5 reduces bytes scanned by ~95%.

```sql
-- Expensive: scans all columns
SELECT * FROM `project.dataset.large_table` WHERE date = '2024-01-01';

-- Cheap: columnar storage means only these 3 columns are scanned
SELECT user_id, event_type, revenue
FROM `project.dataset.large_table`
WHERE date = '2024-01-01';
```

---

## INFORMATION_SCHEMA for Cost Analysis

`INFORMATION_SCHEMA` views expose metadata about jobs, tables, partitions, and usage — critical for cost governance.

```sql
-- Top 10 most expensive queries in the last 7 days
SELECT
  job_id,
  user_email,
  total_bytes_processed / POW(1024, 4) AS tb_processed,
  total_slot_ms / 1000 AS slot_seconds,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_sec,
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND error_result IS NULL
ORDER BY total_bytes_processed DESC
LIMIT 10;

-- Partition metadata: find partitions that haven't been queried
SELECT
  table_name,
  partition_id,
  total_rows,
  total_logical_bytes / POW(1024, 3) AS size_gb,
  last_modified_time
FROM `project.dataset`.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'events'
ORDER BY last_modified_time ASC;
```

---

## Cost Controls: Custom Quotas and Max Bytes Billed

### Per-Query Byte Limit

Set `maximum_bytes_billed` per query to abort scans that exceed a threshold before you're charged:

```python
from google.cloud import bigquery

client = bigquery.Client()
job_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=10 * 1024**3  # abort if query would scan > 10 GB
)

try:
    query_job = client.query(
        "SELECT * FROM `project.dataset.huge_table`",
        job_config=job_config
    )
    results = query_job.result()
except Exception as e:
    print(f"Query aborted: {e}")  # billingTierLimitExceeded error
```

### Custom Quotas via IAM

In BigQuery Admin → Custom Quotas, you can set per-project, per-user, or per-day byte limits. These are enforced at the project level regardless of which query tool is used.

### BigQuery Omni

**BigQuery Omni** extends BigQuery's query engine to data stored in other clouds (AWS S3, Azure Blob Storage) via **external tables** backed by BigQuery Omni compute running in those clouds. This enables cross-cloud analytics without data movement:

```sql
-- BigQuery Omni: query AWS S3 data from BigQuery
CREATE EXTERNAL TABLE `project.dataset.aws_sales`
WITH CONNECTION `aws-us-east-1.my-aws-connection`
OPTIONS (
  format = 'PARQUET',
  uris = ['s3://my-bucket/sales/*.parquet']
);

SELECT region, SUM(revenue) FROM `project.dataset.aws_sales`
WHERE sale_date = '2024-01-01'
GROUP BY region;
```

Data processing happens in AWS us-east-1 — no data crosses cloud boundaries, which addresses data residency concerns.

---

## Connected Sheets

Connected Sheets integrates BigQuery directly into Google Sheets, allowing business users to run queries, pivot tables, and charts against BigQuery tables without writing SQL. Data engineers must understand Connected Sheets because:

1. It can generate large, expensive queries if users work on unpartitioned tables.
2. Results are cached in the sheet but can be refreshed on schedule.
3. Each refresh = a BigQuery job billed to the connected project.

Best practice: create dedicated views or materialized views for Connected Sheets consumers, with appropriate column/row security already applied.
