---
title: "AWS Redshift - Real-World Production Examples"
topic: aws-services
subtopic: redshift
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, redshift, production, migration, cost-optimization, monitoring]
---

# AWS Redshift — Real-World Production Examples

## Pattern 1: Production ETL Load Pattern

```sql
-- Standard nightly load pattern: staging → validate → swap

-- Step 1: COPY into staging table (fastest load method)
CREATE TEMP TABLE staging_orders (LIKE fact_orders);

COPY staging_orders
FROM 's3://data-lake/curated/orders/dt=2024-01-15/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftLoadRole'
FORMAT AS PARQUET;

-- Step 2: Data quality validation
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT order_id) AS unique_orders,
       SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts
FROM staging_orders;
-- Assert: row_count > 0, null_amounts = 0

-- Step 3: Delete-and-insert for the partition (idempotent)
BEGIN TRANSACTION;
DELETE FROM fact_orders WHERE order_date = '2024-01-15';
INSERT INTO fact_orders SELECT * FROM staging_orders;
COMMIT;

-- Step 4: Cleanup and maintenance
DROP TABLE staging_orders;
ANALYZE fact_orders;
-- VACUUM runs automatically in background
```

---

## Pattern 2: Spectrum Hot/Cold Architecture

```sql
-- Hot data (last 90 days): in Redshift local storage (fast)
-- Cold data (older): in S3 via Spectrum (cheap)

-- Create unified view
CREATE VIEW unified_orders AS
-- Hot: local Redshift table (sub-second queries)
SELECT order_id, customer_id, amount, order_date, 'local' AS source
FROM local_schema.fact_orders
WHERE order_date >= CURRENT_DATE - 90

UNION ALL

-- Cold: S3 via Spectrum (cheaper storage, slightly slower)
SELECT order_id, customer_id, amount, order_date, 'spectrum' AS source
FROM spectrum_schema.fact_orders_archive
WHERE order_date < CURRENT_DATE - 90;

-- Nightly archival job: move old data to S3
UNLOAD ('SELECT * FROM fact_orders WHERE order_date < CURRENT_DATE - 90')
TO 's3://archive/fact_orders/dt=' 
IAM_ROLE 'arn:aws:iam::123:role/RedshiftUnloadRole'
PARQUET PARTITION BY (order_date)
ALLOWOVERWRITE;

DELETE FROM fact_orders WHERE order_date < CURRENT_DATE - 90;
VACUUM DELETE ONLY fact_orders;
```

---

## Pattern 3: Real-Time Dashboard with Materialized Views

```sql
-- Problem: dashboard queries hit fact table (5B rows) every 30 seconds
-- Solution: materialized view refreshed every 5 minutes

CREATE MATERIALIZED VIEW mv_realtime_kpis
AUTO REFRESH YES AS
SELECT 
    DATE_TRUNC('hour', order_timestamp) AS hour,
    region,
    COUNT(*) AS orders,
    SUM(amount) AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(amount) AS avg_order_value
FROM fact_orders
WHERE order_date >= CURRENT_DATE - 7  -- Only last 7 days
GROUP BY 1, 2;

-- Dashboard queries hit the MV (few thousand rows) not the fact (billions)
SELECT hour, region, revenue 
FROM mv_realtime_kpis 
WHERE hour >= NOW() - INTERVAL '24 hours'
ORDER BY hour, revenue DESC;
-- Response time: <100ms (was 30 seconds on raw fact table)
```

---

## Pattern 4: Cost Monitoring and Right-Sizing

```sql
-- Monitor cluster utilization to determine if over/under-provisioned
SELECT 
    DATE_TRUNC('hour', starttime) AS hour,
    AVG(cpu_utilization) AS avg_cpu,
    MAX(cpu_utilization) AS peak_cpu,
    AVG(disk_space_used_mb) / AVG(disk_space_total_mb) * 100 AS disk_pct
FROM stv_node_storage_capacity 
CROSS JOIN stl_query_metrics
WHERE starttime > GETDATE() - 7
GROUP BY 1
ORDER BY 1;

-- If avg_cpu < 20%: cluster is over-provisioned (consider downsizing)
-- If peak_cpu > 90% frequently: consider upsizing or concurrency scaling

-- Query cost attribution (which team/query group uses most resources)
SELECT 
    query_group,
    COUNT(*) AS query_count,
    SUM(elapsed) / 1000000 AS total_seconds,
    SUM(aborted) AS aborted_queries
FROM stl_query q
JOIN stl_wlm_query w ON q.query = w.query
WHERE q.starttime > GETDATE() - 30
GROUP BY query_group
ORDER BY total_seconds DESC;
```

---

## Pattern 5: Migration from Redshift to Redshift Serverless

```python
# Step 1: Snapshot the existing provisioned cluster
redshift = boto3.client('redshift')
redshift.create_cluster_snapshot(
    SnapshotIdentifier='migration-snapshot',
    ClusterIdentifier='prod-cluster'
)

# Step 2: Restore to Serverless namespace
redshift_serverless = boto3.client('redshift-serverless')
redshift_serverless.restore_from_snapshot(
    namespaceName='prod-serverless',
    workgroupName='prod-workgroup',
    snapshotName='migration-snapshot',
    ownerAccount='123456789'
)

# Step 3: Update application connection strings
# Old: prod-cluster.xxx.us-east-1.redshift.amazonaws.com:5439
# New: prod-workgroup.xxx.us-east-1.redshift-serverless.amazonaws.com:5439

# Step 4: Monitor performance and costs for 2 weeks before decommissioning old cluster
```

---

## Production Operations Checklist

| Task | Frequency | Command/Method |
|------|-----------|----------------|
| VACUUM heavily-updated tables | Weekly (auto + manual for critical) | `VACUUM FULL table_name` |
| ANALYZE after large loads | After each ETL | `ANALYZE table_name` |
| Check distribution skew | Monthly | Query `SVV_TABLE_INFO` for `skew_rows` |
| Review slow queries | Weekly | `STL_QUERY` sorted by elapsed time |
| Check optimizer alerts | Weekly | `STL_ALERT_EVENT_LOG` |
| Validate COPY errors | After each load | `STL_LOAD_ERRORS` |
| Monitor disk usage | Daily (alarm) | CloudWatch `PercentageDiskSpaceUsed` |
| Review WLM queue times | Weekly | `STL_WLM_QUERY` for queued time |
| Rotate credentials | Quarterly | IAM role rotation, not user passwords |
| Test disaster recovery | Quarterly | Restore from snapshot to new cluster |

---

## Interview Tips

> **Tip 1:** "Describe a production Redshift architecture" — "COPY from S3 Parquet into staging tables (fastest load). Validate quality in staging. DELETE+INSERT into target (idempotent partition replacement). ANALYZE after load. Spectrum for cold/historical data on S3. Materialized views for dashboards. WLM queues separating ETL from analytics. CloudWatch alarms on disk and CPU."

> **Tip 2:** "How do you handle a growing Redshift cluster that's getting expensive?" — "Four strategies: (1) Archive cold data to S3 + Spectrum (query via external tables). (2) Use RA3 nodes with managed storage (decouple compute from storage growth). (3) Migrate to Serverless for variable workloads (pay-per-query instead of always-on). (4) Add materialized views to reduce compute load from repeated dashboard queries."

> **Tip 3:** "How do you ensure zero-downtime migrations in Redshift?" — "Use a blue-green pattern: create new table with correct schema/distribution, COPY data using INSERT INTO...SELECT, validate row counts, then rename tables in a single transaction (swap). Applications never see incomplete data. For schema changes: late-binding views provide a stable interface while underlying tables change."
