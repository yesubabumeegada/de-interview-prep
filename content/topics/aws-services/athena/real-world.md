---
title: "AWS Athena - Real-World Production Examples"
topic: aws-services
subtopic: athena
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, athena, production, data-lake, analytics, cost-optimization]
---

# AWS Athena — Real-World Production Examples

## Pattern 1: Self-Service Analytics Platform

```sql
-- Architecture: Glue Catalog + Athena + Workgroups per team

-- Team-specific workgroups with cost controls:
-- analytics-team: 50 GB/query limit, output → s3://results/analytics/
-- finance-team: 100 GB/query limit, access to finance databases only
-- data-science: 500 GB/query limit, access to all databases
-- etl-service: no limit, used by automation only

-- Common SQL patterns analysts use daily:

-- Revenue trend (partition-pruned, fast and cheap)
SELECT DATE_TRUNC('week', order_date) AS week, SUM(amount) AS weekly_revenue
FROM curated.fact_orders
WHERE year = 2024 AND month >= 1  -- Partition columns in WHERE
GROUP BY 1 ORDER BY 1;

-- Customer cohort analysis
WITH first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM curated.fact_orders
    WHERE year >= 2023
    GROUP BY customer_id
)
SELECT DATE_TRUNC('month', first_order_date) AS cohort,
       DATE_DIFF('month', first_order_date, o.order_date) AS months_since_first,
       COUNT(DISTINCT o.customer_id) AS active_customers
FROM curated.fact_orders o
JOIN first_orders f ON o.customer_id = f.customer_id
WHERE o.year >= 2023
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## Pattern 2: Scheduled ETL with Athena + EventBridge + Step Functions

```python
# Nightly ETL: raw JSON → curated Parquet, entirely in SQL via Athena

# Step Function definition:
steps = {
    "Stage1_ConvertRawToParquet": {
        "query": """
            INSERT INTO curated.orders
            SELECT 
                json_extract_scalar(payload, '$.order_id') AS order_id,
                json_extract_scalar(payload, '$.customer_id') AS customer_id,
                CAST(json_extract_scalar(payload, '$.amount') AS DOUBLE) AS amount,
                CAST(json_extract_scalar(payload, '$.date') AS DATE) AS order_date,
                YEAR(CAST(json_extract_scalar(payload, '$.date') AS DATE)) AS year,
                MONTH(CAST(json_extract_scalar(payload, '$.date') AS DATE)) AS month
            FROM raw.json_orders
            WHERE dt = '{yesterday}'
        """
    },
    "Stage2_Validate": {
        "query": """
            SELECT 
                COUNT(*) AS row_count,
                SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_ids,
                SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS bad_amounts
            FROM curated.orders
            WHERE year = {year} AND month = {month} AND order_date = DATE '{yesterday}'
        """
    },
    "Stage3_Aggregate": {
        "query": """
            INSERT INTO analytics.daily_summary
            SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
            FROM curated.orders
            WHERE year = {year} AND month = {month} AND order_date = DATE '{yesterday}'
            GROUP BY order_date
        """
    }
}

# Triggered by EventBridge at 6 AM daily
# Total cost: ~$0.05-0.50/day depending on data volume (serverless!)
```

---

## Pattern 3: Data Quality Monitoring

```sql
-- Daily quality check query (run via Lambda + Athena)
-- Compares today's metrics against 30-day baseline

WITH today_metrics AS (
    SELECT 
        COUNT(*) AS row_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(amount) AS total_revenue,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
        AVG(amount) AS avg_amount
    FROM curated.fact_orders
    WHERE order_date = CURRENT_DATE - INTERVAL '1' DAY
),
baseline AS (
    SELECT 
        AVG(daily_count) AS avg_row_count,
        STDDEV(daily_count) AS std_row_count
    FROM (
        SELECT order_date, COUNT(*) AS daily_count
        FROM curated.fact_orders
        WHERE order_date BETWEEN CURRENT_DATE - INTERVAL '31' DAY AND CURRENT_DATE - INTERVAL '2' DAY
        GROUP BY order_date
    )
)
SELECT 
    t.row_count,
    b.avg_row_count,
    CASE 
        WHEN t.row_count < b.avg_row_count - 2 * b.std_row_count THEN 'ANOMALY_LOW'
        WHEN t.row_count > b.avg_row_count + 2 * b.std_row_count THEN 'ANOMALY_HIGH'
        ELSE 'NORMAL'
    END AS status,
    t.null_amounts,
    CASE WHEN t.null_amounts > 0 THEN 'FAIL' ELSE 'PASS' END AS null_check
FROM today_metrics t, baseline b;
```

---

## Pattern 4: Log Analytics at Scale

```sql
-- Query application logs stored in S3 (100s of GBs daily)
-- Partition by date + hour for fine-grained pruning

CREATE EXTERNAL TABLE logs.application_logs (
    timestamp STRING,
    level STRING,
    service STRING,
    message STRING,
    trace_id STRING,
    request_duration_ms INT
)
PARTITIONED BY (dt STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://logs-bucket/application/'
TBLPROPERTIES ('projection.enabled' = 'true',
               'projection.dt.type' = 'date',
               'projection.dt.range' = '2023/01/01,NOW',
               'projection.dt.format' = 'yyyy/MM/dd',
               'projection.hour.type' = 'integer',
               'projection.hour.range' = '0,23',
               'storage.location.template' = 
                   's3://logs-bucket/application/${dt}/${hour}/');

-- Find error patterns in last hour
SELECT service, message, COUNT(*) AS error_count
FROM logs.application_logs
WHERE dt = DATE_FORMAT(NOW(), '%Y/%m/%d')
  AND hour = CAST(HOUR(NOW()) - 1 AS VARCHAR)
  AND level = 'ERROR'
GROUP BY service, message
ORDER BY error_count DESC
LIMIT 20;

-- P99 latency by service (last 24 hours)
SELECT service,
       approx_percentile(request_duration_ms, 0.50) AS p50,
       approx_percentile(request_duration_ms, 0.95) AS p95,
       approx_percentile(request_duration_ms, 0.99) AS p99
FROM logs.application_logs
WHERE dt = DATE_FORMAT(NOW(), '%Y/%m/%d')
GROUP BY service
ORDER BY p99 DESC;
```

---

## Production Cost Report

```sql
-- Monthly Athena cost analysis (query the query execution metadata)
-- Use the Athena query history API or CloudTrail

-- Estimated from workgroup metrics:
-- analytics-team: ~200 queries/day × 5 GB avg scan × $5/TB = $5/day = $150/month
-- finance-team: ~50 queries/day × 2 GB avg = $0.50/day = $15/month  
-- data-science: ~30 queries/day × 50 GB avg = $7.50/day = $225/month
-- etl-service: ~10 queries/day × 100 GB avg = $5/day = $150/month
-- Total: ~$540/month (serverless! No cluster to manage)

-- Compare with alternative (Redshift):
-- Equivalent always-on cluster: ~$3,000-5,000/month
-- Athena is 5-10x cheaper for this access pattern (sporadic, varied queries)
```

---

## Interview Tips

> **Tip 1:** "Describe a production Athena implementation" — "Data stored as partitioned Parquet in S3. Glue Catalog provides schema. Partition projection for fast planning on date-partitioned tables. Workgroups per team with scan limits and separate cost tracking. Step Functions orchestrate SQL-based ETL. CloudWatch alarms on failed queries and cost thresholds. Total cost: $500/month for an analytics platform serving 50 users."

> **Tip 2:** "How would you use Athena for real-time analytics?" — "Athena isn't real-time (query latency is seconds to minutes). For near-real-time: stream data to S3 via Firehose in 1-minute batches, partition by hour. Athena queries the latest hour's partition for 'fresh enough' data. For true real-time (<1 second), use Kinesis Analytics or a streaming consumer directly."

> **Tip 3:** "How do you prevent a $1000 Athena bill from one bad query?" — "Three defenses: (1) Workgroup per-query scan limit (e.g., 100 GB max — query fails if it would exceed). (2) All tables stored as Parquet with partitions (inherently limits scan volume). (3) Budget alerts in AWS Budgets on the Athena service. The scan limit is the most immediate protection — prevents any single query from being catastrophically expensive."
