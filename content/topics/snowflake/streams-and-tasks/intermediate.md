---
title: "Streams and Tasks - Intermediate"
topic: snowflake
subtopic: streams-and-tasks
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, streams, tasks, cdc, merge, dag, error-handling]
---

# Snowflake Streams and Tasks — Intermediate

## Advanced Stream Patterns

### MERGE Pattern (SCD Type 1 — Latest State)

```sql
-- The most common pattern: MERGE from stream into target (upsert)
CREATE OR REPLACE TASK merge_customers
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = 'USING CRON */15 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('customers_stream')
AS
    MERGE INTO silver.customers t
    USING (
        -- Get only the LATEST version of each customer from the stream
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn
            FROM customers_stream
            WHERE METADATA$ACTION = 'INSERT'  -- Inserts + new version of updates
        ) WHERE rn = 1
    ) s ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
        t.name = s.name,
        t.email = s.email,
        t.region = s.region,
        t.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (customer_id, name, email, region, created_at, updated_at)
        VALUES (s.customer_id, s.name, s.email, s.region, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

### Handling Deletes

```sql
-- Capture deletes from source and apply to target
MERGE INTO silver.customers t
USING (
    -- Deletes: ACTION='DELETE' and ISUPDATE=FALSE (true delete, not update)
    SELECT customer_id FROM customers_stream
    WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE
) d ON t.customer_id = d.customer_id
WHEN MATCHED THEN DELETE;

-- Combined pattern: handle inserts + updates + deletes in one MERGE
MERGE INTO silver.customers t
USING (
    SELECT customer_id, name, email, region,
           METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update
    FROM customers_stream
) s ON t.customer_id = s.customer_id
WHEN MATCHED AND s.action = 'DELETE' AND s.is_update = FALSE THEN DELETE
WHEN MATCHED AND s.action = 'INSERT' THEN UPDATE SET
    t.name = s.name, t.email = s.email, t.region = s.region
WHEN NOT MATCHED AND s.action = 'INSERT' THEN INSERT
    (customer_id, name, email, region) VALUES (s.customer_id, s.name, s.email, s.region);
```

---

## Multi-Table Stream Processing

```sql
-- Process multiple streams in a task DAG

-- Root task: check multiple streams, process orders
CREATE TASK process_orders_silver
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_raw_stream')
AS
    INSERT INTO silver.orders
    SELECT order_id, customer_id, product_id, amount, order_date, CURRENT_TIMESTAMP()
    FROM orders_raw_stream
    WHERE METADATA$ACTION = 'INSERT';

-- Child task: process products (runs after orders)
CREATE TASK process_products_silver
    WAREHOUSE = 'ETL_WH'
    AFTER process_orders_silver
    WHEN SYSTEM$STREAM_HAS_DATA('products_raw_stream')
AS
    MERGE INTO silver.products t
    USING products_raw_stream s ON t.product_id = s.product_id
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET t.name = s.name, t.price = s.price
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT VALUES (s.product_id, s.name, s.price);

-- Child task: build gold aggregation (after both silver tasks)
CREATE TASK build_gold_revenue
    WAREHOUSE = 'ETL_WH'
    AFTER process_orders_silver, process_products_silver
AS
    INSERT OVERWRITE INTO gold.daily_revenue
    SELECT o.order_date, p.category, SUM(o.amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders o
    JOIN silver.products p ON o.product_id = p.product_id
    WHERE o.order_date = CURRENT_DATE - 1
    GROUP BY o.order_date, p.category;
```

---

## Task Error Handling

```sql
-- Tasks can fail — handle gracefully

-- Stored procedure with error handling (used as task action):
CREATE OR REPLACE PROCEDURE etl.process_orders_sp()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Attempt the MERGE
    MERGE INTO silver.orders t
    USING orders_stream s ON t.order_id = s.order_id
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET t.amount = s.amount
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT VALUES (s.order_id, s.amount, s.order_date);
    
    RETURN 'SUCCESS: ' || SQLROWCOUNT || ' rows processed';
EXCEPTION
    WHEN OTHER THEN
        -- Log the error
        INSERT INTO etl.error_log (task_name, error_message, error_time)
        VALUES ('process_orders', SQLERRM, CURRENT_TIMESTAMP());
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- Task calls the stored procedure
CREATE TASK process_orders_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    CALL etl.process_orders_sp();

-- Task retry configuration:
ALTER TASK process_orders_task SET
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3;  -- Suspend after 3 consecutive failures
-- After suspension: investigate and manually resume

-- Monitor failed tasks:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'FAILED'
  AND SCHEDULED_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY SCHEDULED_TIME DESC;
```

---

## Stream on Views

```sql
-- Streams can be created on VIEWS (not just tables)
-- Useful for: pre-filtering, joining at stream level

CREATE VIEW raw.active_orders AS
SELECT * FROM raw.orders WHERE status != 'cancelled';

-- Stream on the view: only tracks changes to non-cancelled orders
CREATE STREAM active_orders_stream ON VIEW raw.active_orders;

-- Now the stream automatically filters out cancelled orders
-- Downstream processing is simpler (no WHERE clause needed)
```

---

## Stale Streams and Data Retention

```sql
-- IMPORTANT: Streams have a staleness limit!
-- If you don't consume a stream for too long, it becomes STALE (unusable)

-- Stream staleness = table's DATA_RETENTION_TIME_IN_DAYS
-- Default retention: 1 day (Standard edition), up to 90 days (Enterprise)

-- Check stream staleness:
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('orders_stream');
-- If this timestamp is older than retention period → stream is STALE

-- Prevent staleness:
-- 1. Consume stream regularly (at least once within retention period)
-- 2. Increase table retention:
ALTER TABLE raw.orders SET DATA_RETENTION_TIME_IN_DAYS = 14;
-- Now stream can go 14 days without consumption before becoming stale

-- If stream becomes stale:
-- Must recreate it (loses the offset — you'll need a full refresh)
DROP STREAM orders_stream;
CREATE STREAM orders_stream ON TABLE raw.orders;
-- Then do a full load from source to silver (one-time catch-up)
```

---

## Serverless Tasks

```sql
-- Serverless tasks: no warehouse needed (Snowflake manages compute)
CREATE TASK serverless_etl
    -- No WAREHOUSE clause! Snowflake uses serverless compute
    SCHEDULE = '5 MINUTE'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('events_stream')
AS
    INSERT INTO silver.events
    SELECT * FROM events_stream WHERE METADATA$ACTION = 'INSERT';

-- Benefits of serverless tasks:
-- No warehouse to manage (auto-sizes based on workload)
-- Faster startup (no warehouse wake-up time)
-- Pay only for compute used (not idle warehouse time)
-- Ideal for: frequent, short-running tasks (< 1 minute)
```

---

## Monitoring and Observability

```sql
-- Task execution history (last 7 days)
SELECT 
    NAME, STATE, 
    SCHEDULED_TIME, COMPLETED_TIME,
    TIMESTAMPDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS duration_sec,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    RESULT_LIMIT => 100,
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- Task run summary (success/failure rates)
SELECT 
    NAME,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN STATE = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successes,
    SUM(CASE WHEN STATE = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    AVG(TIMESTAMPDIFF('second', QUERY_START_TIME, COMPLETED_TIME)) AS avg_duration_sec
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
GROUP BY NAME
ORDER BY failures DESC;

-- Alert on task failures (use Snowflake Alerts feature):
CREATE ALERT task_failure_alert
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
        WHERE STATE = 'FAILED' AND SCHEDULED_TIME > DATEADD('minute', -10, CURRENT_TIMESTAMP())
    ))
    THEN CALL SYSTEM$SEND_EMAIL('data-team@company.com', 'Task Failed!', 'Check task history.');
```

---

## Interview Tips

> **Tip 1:** "How do you handle the full CDC lifecycle (insert + update + delete) with streams?" — MERGE pattern: filter stream by METADATA$ACTION. INSERTs (ACTION='INSERT', ISUPDATE=FALSE) → INSERT into target. UPDATEs (ACTION='INSERT', ISUPDATE=TRUE) → UPDATE target where key matches. DELETEs (ACTION='DELETE', ISUPDATE=FALSE) → DELETE from target. One MERGE statement handles all three.

> **Tip 2:** "What happens if a stream becomes stale?" — If you don't consume a stream within the table's DATA_RETENTION_TIME_IN_DAYS, it becomes stale (offset points to data that's been purged). Fix: drop and recreate the stream, then do a full refresh from source. Prevention: consume regularly or increase retention period (up to 90 days on Enterprise).

> **Tip 3:** "Serverless tasks vs warehouse-based tasks?" — Serverless: no warehouse management, auto-scales, faster startup, pay per second of compute. Warehouse-based: predictable performance, can share warehouse across tasks, better for large/long tasks. Use serverless for: frequent short tasks (<1 min), many small tasks. Use warehouse for: complex ETL, shared compute, predictable costs.
