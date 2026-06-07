---
title: "Streams and Tasks - Real-World Production Examples"
topic: snowflake
subtopic: streams-and-tasks
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, streams, tasks, production, patterns, etl]
---

# Snowflake Streams and Tasks — Real-World Production Examples

## Pattern 1: Complete Medallion Pipeline

```sql
-- End-to-end: Raw → Bronze → Silver → Gold using Streams + Tasks

-- SETUP: Streams on each layer
CREATE STREAM raw_orders_stream ON TABLE raw.orders;
CREATE STREAM silver_orders_stream ON TABLE silver.orders;

-- TASK 1: Raw → Silver (clean, type, dedup)
CREATE OR REPLACE TASK raw_to_silver_orders
    WAREHOUSE = 'ETL_WH_SMALL'
    SCHEDULE = 'USING CRON */15 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_stream')
AS
    MERGE INTO silver.orders t
    USING (
        SELECT 
            order_id::NUMBER AS order_id,
            customer_id::NUMBER AS customer_id,
            amount::DECIMAL(10,2) AS amount,
            TRY_TO_DATE(order_date) AS order_date,
            status,
            CURRENT_TIMESTAMP() AS _loaded_at
        FROM raw_orders_stream
        WHERE METADATA$ACTION = 'INSERT'
          AND order_id IS NOT NULL
          AND amount > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1
    ) s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        t.amount = s.amount, t.status = s.status, t._loaded_at = s._loaded_at
    WHEN NOT MATCHED THEN INSERT 
        (order_id, customer_id, amount, order_date, status, _loaded_at)
        VALUES (s.order_id, s.customer_id, s.amount, s.order_date, s.status, s._loaded_at);

-- TASK 2: Silver → Gold (aggregate)
CREATE OR REPLACE TASK silver_to_gold_revenue
    WAREHOUSE = 'ETL_WH_SMALL'
    AFTER raw_to_silver_orders
AS
    MERGE INTO gold.daily_revenue t
    USING (
        SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
        FROM silver.orders
        WHERE order_date >= DATEADD('day', -3, CURRENT_DATE())
        GROUP BY order_date
    ) s ON t.revenue_date = s.order_date
    WHEN MATCHED THEN UPDATE SET t.orders = s.orders, t.revenue = s.revenue
    WHEN NOT MATCHED THEN INSERT VALUES (s.order_date, s.orders, s.revenue);

-- Resume tasks
ALTER TASK silver_to_gold_revenue RESUME;
ALTER TASK raw_to_silver_orders RESUME;
```

---

## Pattern 2: Multi-Source CDC Consolidation

```sql
-- Consolidate CDC from 3 regional databases into one global table

-- Streams on each regional landing table
CREATE STREAM us_orders_stream ON TABLE raw.us_orders;
CREATE STREAM eu_orders_stream ON TABLE raw.eu_orders;
CREATE STREAM apac_orders_stream ON TABLE raw.apac_orders;

-- Task: merge all regions into global silver table
CREATE OR REPLACE TASK consolidate_global_orders
    WAREHOUSE = 'ETL_WH_MEDIUM'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('us_orders_stream')
       OR SYSTEM$STREAM_HAS_DATA('eu_orders_stream')
       OR SYSTEM$STREAM_HAS_DATA('apac_orders_stream')
AS
    MERGE INTO silver.global_orders t
    USING (
        SELECT order_id, customer_id, amount, order_date, 'US' AS region
        FROM us_orders_stream WHERE METADATA$ACTION = 'INSERT'
        UNION ALL
        SELECT order_id, customer_id, amount, order_date, 'EU' AS region
        FROM eu_orders_stream WHERE METADATA$ACTION = 'INSERT'
        UNION ALL
        SELECT order_id, customer_id, amount, order_date, 'APAC' AS region
        FROM apac_orders_stream WHERE METADATA$ACTION = 'INSERT'
    ) s ON t.order_id = s.order_id AND t.region = s.region
    WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.status = s.status
    WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.amount, s.order_date, s.region);
```

---

## Pattern 3: Real-Time Alerting

```sql
-- Stream + Task for real-time anomaly detection

-- Stream on transactions (append-only for speed)
CREATE STREAM txn_stream ON TABLE raw.transactions APPEND_ONLY = TRUE;

-- Task: check for high-value transactions every 2 minutes
CREATE OR REPLACE TASK fraud_alert_task
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'  -- Serverless
    SCHEDULE = '2 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('txn_stream')
AS
    INSERT INTO alerts.high_value_transactions
    SELECT 
        transaction_id, customer_id, amount, merchant, transaction_time,
        CURRENT_TIMESTAMP() AS alert_generated_at,
        'HIGH_VALUE' AS alert_type
    FROM txn_stream
    WHERE METADATA$ACTION = 'INSERT'
      AND amount > 10000;
    -- Downstream: notification service polls alerts table every minute

-- Pattern benefit:
-- Latency: transaction occurs → alert generated in ~2-4 minutes
-- Cost: serverless task, only runs when new transactions exist
-- No always-on warehouse needed!
```

---

## Pattern 4: SCD Type 2 with Streams

```sql
-- Maintain full history (SCD Type 2) using streams

CREATE OR REPLACE PROCEDURE etl.apply_scd2_customers()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Close existing records for changed customers
    UPDATE silver.customers_history
    SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE
    WHERE customer_id IN (
        SELECT customer_id FROM customers_stream 
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
    )
    AND is_current = TRUE;
    
    -- Step 2: Insert new versions (for updates and new customers)
    INSERT INTO silver.customers_history 
        (customer_id, name, email, region, valid_from, valid_to, is_current)
    SELECT 
        customer_id, name, email, region,
        CURRENT_TIMESTAMP() AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM customers_stream
    WHERE METADATA$ACTION = 'INSERT';  -- Includes new + updated rows
    
    -- Step 3: Handle hard deletes (close the record)
    UPDATE silver.customers_history
    SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE
    WHERE customer_id IN (
        SELECT customer_id FROM customers_stream
        WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE
    )
    AND is_current = TRUE;
    
    RETURN 'SCD2 applied: ' || SQLROWCOUNT || ' changes processed';
END;
$$;

CREATE TASK scd2_customers_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '30 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('customers_stream')
AS
    CALL etl.apply_scd2_customers();
```

---

## Pattern 5: Task Monitoring Dashboard

```sql
-- Create a monitoring view for all task pipelines

CREATE OR REPLACE VIEW ops.task_health AS
SELECT 
    name AS task_name,
    state AS last_run_state,
    scheduled_time,
    completed_time,
    TIMESTAMPDIFF('second', query_start_time, completed_time) AS duration_sec,
    error_message,
    CASE 
        WHEN state = 'FAILED' THEN 'CRITICAL'
        WHEN TIMESTAMPDIFF('second', query_start_time, completed_time) > 300 THEN 'SLOW'
        ELSE 'HEALTHY'
    END AS health_status
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    RESULT_LIMIT => 1000,
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;

-- Alert query (check every 5 minutes):
SELECT task_name, error_message, scheduled_time
FROM ops.task_health
WHERE health_status = 'CRITICAL'
  AND scheduled_time > DATEADD('minute', -10, CURRENT_TIMESTAMP());
```

---

## Interview Tips

> **Tip 1:** "Design an incremental ETL pipeline with Streams + Tasks" — Stream on source table (captures CDC). Task on schedule with WHEN clause (only runs if data exists). MERGE into target (handles insert + update + delete). Transaction semantics ensure exactly-once. Stream offset advances only on successful commit.

> **Tip 2:** "How do you do SCD Type 2 with Streams?" — Stream captures changes. Stored procedure: (1) Close current records for changed keys (set valid_to, is_current=FALSE), (2) Insert new versions with valid_from=now, is_current=TRUE, (3) Handle deletes (close record). All in one transaction for consistency.

> **Tip 3:** "How do you monitor Tasks in production?" — TASK_HISTORY function shows: run times, durations, success/failure. Build a monitoring view/dashboard tracking: failure rate, duration trends, SLA compliance. Use Snowflake Alerts feature to notify on failures. Suspend tasks after N consecutive failures (SUSPEND_TASK_AFTER_NUM_FAILURES).
