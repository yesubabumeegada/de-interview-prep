---
title: "Snowflake Architecture - Real-World Production Examples"
topic: snowflake
subtopic: architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, architecture, production, cost-optimization, multi-warehouse, governance]
---

# Snowflake Architecture — Real-World Production Examples

## Pattern 1: Multi-Warehouse Strategy

Production Snowflake environments use dedicated warehouses per workload:

```sql
-- ETL warehouse: Large, runs during load windows, suspends after
CREATE WAREHOUSE etl_wh
    WAREHOUSE_SIZE = 'XLARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Nightly ETL loads. Schedule: 1-6 AM UTC';

-- Analytics warehouse: Medium, multi-cluster for concurrent users
CREATE WAREHOUSE analytics_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 300
    COMMENT = 'BI tools and analyst ad-hoc queries';

-- Data science warehouse: Large, for heavy notebooks
CREATE WAREHOUSE datascience_wh
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 600
    COMMENT = 'Jupyter/Databricks notebook queries';

-- Dev warehouse: XSmall, cost-controlled
CREATE WAREHOUSE dev_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    COMMENT = 'Development and testing';
```

**Cost governance — resource monitors:**

```sql
-- Cap monthly spending per warehouse
CREATE RESOURCE MONITOR etl_monitor
    WITH CREDIT_QUOTA = 500           -- 500 credits/month max
    TRIGGERS
        ON 80 PERCENT DO NOTIFY       -- Alert at 80%
        ON 100 PERCENT DO SUSPEND;    -- Stop warehouse at 100%

ALTER WAREHOUSE etl_wh SET RESOURCE_MONITOR = etl_monitor;
```

---

## Pattern 2: Continuous Data Pipeline (Streams + Tasks)

Snowflake's native CDC (Change Data Capture) using Streams and Tasks:

```sql
-- Step 1: Create a stream on the source table (tracks changes)
CREATE STREAM raw_orders_stream ON TABLE staging.raw_orders;

-- Step 2: Create a task that processes changes every 5 minutes
CREATE TASK process_new_orders
    WAREHOUSE = etl_wh
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_stream')  -- Only run if new data
AS
MERGE INTO curated.fact_orders t
USING (
    SELECT 
        order_id,
        customer_id,
        amount,
        order_date,
        METADATA$ACTION AS action,      -- INSERT, DELETE
        METADATA$ISUPDATE AS is_update  -- TRUE if UPDATE
    FROM raw_orders_stream
) s
ON t.order_id = s.order_id
WHEN MATCHED AND s.action = 'DELETE' AND NOT s.is_update THEN
    DELETE
WHEN MATCHED AND s.is_update THEN
    UPDATE SET 
        t.customer_id = s.customer_id,
        t.amount = s.amount,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.action = 'INSERT' THEN
    INSERT (order_id, customer_id, amount, order_date, loaded_at)
    VALUES (s.order_id, s.customer_id, s.amount, s.order_date, CURRENT_TIMESTAMP());

-- Step 3: Enable the task
ALTER TASK process_new_orders RESUME;
```

**How streams work:**
- A stream is a metadata-only object that tracks changes (DML) on a table
- It records INSERT, UPDATE (as DELETE + INSERT pair), and DELETE
- Once consumed (read by a task), changes are removed from the stream
- No storage cost for the stream itself — it's just an offset pointer

---

## Pattern 3: Cost-Optimized Data Lake Architecture

```sql
-- External stage pointing to S3 (data stays in your S3 bucket)
CREATE STAGE raw_data_stage
    URL = 's3://my-data-lake/raw/'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123:role/snowflake-role')
    FILE_FORMAT = (TYPE = PARQUET);

-- External table: query S3 data without loading into Snowflake
CREATE EXTERNAL TABLE ext_clickstream (
    user_id VARCHAR AS (VALUE:user_id::VARCHAR),
    event_type VARCHAR AS (VALUE:event_type::VARCHAR),
    event_timestamp TIMESTAMP AS (VALUE:event_ts::TIMESTAMP),
    page_url VARCHAR AS (VALUE:page_url::VARCHAR)
)
WITH LOCATION = @raw_data_stage/clickstream/
FILE_FORMAT = (TYPE = PARQUET)
PARTITION BY (event_date)
AUTO_REFRESH = TRUE;

-- Hybrid approach: hot data loaded, cold data external
CREATE VIEW unified_events AS
-- Recent data (loaded, fast)
SELECT * FROM curated.fact_events WHERE event_date >= DATEADD(day, -30, CURRENT_DATE)
UNION ALL
-- Historical data (external, slower but cheap)
SELECT * FROM ext_clickstream WHERE event_date < DATEADD(day, -30, CURRENT_DATE);
```

**Cost benefit:**
- Last 30 days: loaded in Snowflake (fast queries, ~$23/TB/month storage)
- Older data: stays in S3 ($23/TB/month vs $0.023/GB = $23/TB in S3 too, but no compute cost for storage)
- Query both through a single view transparently

---

## Pattern 4: Automated Data Quality Framework

```sql
-- Create a quality results table
CREATE TABLE data_quality.check_results (
    check_id VARCHAR,
    table_name VARCHAR,
    check_type VARCHAR,
    check_sql VARCHAR,
    result_value NUMBER,
    threshold NUMBER,
    status VARCHAR,  -- 'PASS', 'WARN', 'FAIL'
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Task: run quality checks after each ETL load
CREATE TASK run_quality_checks
    WAREHOUSE = etl_wh
    AFTER process_new_orders  -- Runs after the ETL task completes
AS
BEGIN
    -- Check 1: No NULL primary keys
    LET null_count := (SELECT COUNT(*) FROM curated.fact_orders WHERE order_id IS NULL);
    INSERT INTO data_quality.check_results 
    VALUES ('NULL_PK', 'fact_orders', 'null_check', 'order_id IS NULL', 
            :null_count, 0, IFF(:null_count = 0, 'PASS', 'FAIL'), CURRENT_TIMESTAMP());
    
    -- Check 2: Row count not anomalously low
    LET today_count := (SELECT COUNT(*) FROM curated.fact_orders WHERE order_date = CURRENT_DATE - 1);
    LET avg_count := (SELECT AVG(daily_count) FROM (
        SELECT order_date, COUNT(*) AS daily_count 
        FROM curated.fact_orders 
        WHERE order_date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 2
        GROUP BY order_date
    ));
    LET status := IFF(:today_count > :avg_count * 0.5, 'PASS', 'FAIL');
    INSERT INTO data_quality.check_results
    VALUES ('ROW_COUNT', 'fact_orders', 'anomaly', 'count vs 30-day avg',
            :today_count, :avg_count * 0.5, :status, CURRENT_TIMESTAMP());
    
    -- Alert on failures
    IF (EXISTS (SELECT 1 FROM data_quality.check_results 
                WHERE status = 'FAIL' AND executed_at > DATEADD(minute, -5, CURRENT_TIMESTAMP()))) THEN
        CALL system$send_email('data-alerts@company.com', 'DQ Check Failed', 
            'One or more data quality checks failed. Check data_quality.check_results.');
    END IF;
END;

ALTER TASK run_quality_checks RESUME;
```

---

## Pattern 5: Role-Based Access Control (RBAC)

```sql
-- Hierarchy: ACCOUNTADMIN → SYSADMIN → Custom roles → Users
-- Principle: Least privilege, role inheritance

-- Create functional roles
CREATE ROLE analyst_role;
CREATE ROLE engineer_role;
CREATE ROLE loader_role;

-- Grant warehouse access per role
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE analyst_role;
GRANT USAGE ON WAREHOUSE etl_wh TO ROLE loader_role;
GRANT USAGE ON WAREHOUSE dev_wh TO ROLE engineer_role;

-- Grant schema access
GRANT USAGE ON DATABASE analytics TO ROLE analyst_role;
GRANT USAGE ON SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;

-- Engineers can write to staging
GRANT ALL ON SCHEMA analytics.staging TO ROLE engineer_role;

-- Row-level security via secure views
CREATE SECURE VIEW curated.v_regional_sales AS
SELECT * FROM curated.fact_sales
WHERE region = CURRENT_ROLE();  -- Each role only sees their region
```

---

## Production Cost Monitoring Dashboard

```sql
-- Monthly cost breakdown by warehouse
SELECT 
    warehouse_name,
    DATE_TRUNC('month', start_time) AS month,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3.00 AS estimated_cost_usd,  -- $3/credit example
    COUNT(*) AS query_count,
    AVG(credits_used / NULLIF(DATEDIFF(second, start_time, end_time), 0) * 3600) AS avg_credits_per_hour
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(month, -3, CURRENT_DATE)
GROUP BY warehouse_name, month
ORDER BY month DESC, total_credits DESC;

-- Find most expensive queries
SELECT 
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time / 1000 AS elapsed_sec,
    bytes_scanned / POWER(1024, 3) AS gb_scanned,
    partitions_scanned,
    partitions_total,
    ROUND(partitions_scanned / NULLIF(partitions_total, 0) * 100, 1) AS pct_scanned
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_DATE)
  AND total_elapsed_time > 60000  -- Queries > 60 seconds
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

---

## Interview Tips

> **Tip 1:** "How do you optimize Snowflake costs?" — "Five levers: (1) Auto-suspend warehouses aggressively (60-300s). (2) Right-size — bigger runs faster but same credits per query. (3) Transient tables for staging (no fail-safe storage cost). (4) Clustering only for large, frequently-filtered tables. (5) Resource monitors to cap spending with alerts at 80%."

> **Tip 2:** "Describe a Snowflake pipeline you'd build" — "Data lands in S3 via CDC. Snowpipe auto-ingests into a staging schema. A Stream captures changes. A Task runs every 5 minutes: if stream has data, MERGE into curated fact table. A downstream Task runs quality checks and alerts on failure. Analysts query via a separate multi-cluster warehouse."

> **Tip 3:** "How do you handle slowly changing dimensions in Snowflake?" — "For SCD Type 2: I use MERGE with a multi-step approach. First close the current record (set effective_to). Then insert the new version. Streams capture changes from the source automatically. The Task-based pipeline handles the MERGE logic. Time Travel gives me a safety net if the logic goes wrong."
