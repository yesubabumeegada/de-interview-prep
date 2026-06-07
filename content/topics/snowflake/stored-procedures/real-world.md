---
title: "Stored Procedures - Real-World Production Examples"
topic: snowflake
subtopic: stored-procedures
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, stored-procedures, production, patterns, etl]
---

# Snowflake Stored Procedures — Real-World Production Examples

## Pattern 1: Full ETL Pipeline Procedure

```sql
CREATE OR REPLACE PROCEDURE etl.daily_orders_pipeline()
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    step VARCHAR; rows_count NUMBER; start_ts TIMESTAMP;
BEGIN
    start_ts := CURRENT_TIMESTAMP();
    
    -- Step 1: Ingest (load new files)
    step := 'INGEST';
    COPY INTO raw.orders FROM @raw.landing_stage/orders/ FILE_FORMAT = (TYPE='JSON');
    rows_count := SQLROWCOUNT;
    CALL etl.log_step(step, rows_count, 'SUCCESS');
    
    -- Step 2: Silver (clean + dedup)
    step := 'SILVER';
    MERGE INTO silver.orders t
    USING (SELECT * FROM raw_orders_stream WHERE METADATA$ACTION = 'INSERT'
           QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.status = s.status
    WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.amount, s.order_date, s.status, CURRENT_TIMESTAMP());
    rows_count := SQLROWCOUNT;
    CALL etl.log_step(step, rows_count, 'SUCCESS');
    
    -- Step 3: Gold (aggregate)
    step := 'GOLD';
    INSERT OVERWRITE INTO gold.daily_revenue
    SELECT order_date, region, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders o JOIN silver.customers c ON o.customer_id = c.customer_id
    WHERE order_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY order_date, region;
    rows_count := SQLROWCOUNT;
    CALL etl.log_step(step, rows_count, 'SUCCESS');
    
    RETURN 'PIPELINE COMPLETE in ' || TIMESTAMPDIFF('second', start_ts, CURRENT_TIMESTAMP()) || 's';
EXCEPTION
    WHEN OTHER THEN
        CALL etl.log_step(step, 0, 'FAILED: ' || SQLERRM);
        CALL etl.send_alert('Daily orders pipeline FAILED at step: ' || step || ' - ' || SQLERRM);
        RAISE;
END;
$$;
```

---

## Pattern 2: Data Quality Validation

```sql
CREATE OR REPLACE PROCEDURE dq.validate_table(schema_name VARCHAR, table_name VARCHAR)
RETURNS TABLE (check_name VARCHAR, status VARCHAR, details VARCHAR)
LANGUAGE SQL AS
$$
DECLARE
    full_name VARCHAR := schema_name || '.' || table_name;
    results ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
    -- Check 1: Row count > 0
    LET cnt := (EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || full_name);
    IF (cnt = 0) THEN
        results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('check', 'row_count', 'status', 'FAIL', 'details', 'Table is empty!'));
    ELSE
        results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('check', 'row_count', 'status', 'PASS', 'details', cnt || ' rows'));
    END IF;
    
    -- Check 2: Freshness (last load within 4 hours)
    LET last_load := (EXECUTE IMMEDIATE 'SELECT MAX(_loaded_at) FROM ' || full_name);
    IF (last_load < DATEADD('hour', -4, CURRENT_TIMESTAMP())) THEN
        results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('check', 'freshness', 'status', 'FAIL', 'details', 'Last load: ' || last_load));
    ELSE
        results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('check', 'freshness', 'status', 'PASS', 'details', 'Fresh'));
    END IF;
    
    -- Return results as table
    RETURN TABLE(SELECT value:check::VARCHAR, value:status::VARCHAR, value:details::VARCHAR
                 FROM TABLE(FLATTEN(INPUT => results)));
END;
$$;

CALL dq.validate_table('silver', 'orders');
-- Returns: | row_count | PASS | 5000000 rows |
--          | freshness | PASS | Fresh |
```

---

## Pattern 3: Automated Maintenance

```sql
CREATE OR REPLACE PROCEDURE admin.table_maintenance()
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    tables_optimized NUMBER DEFAULT 0;
BEGIN
    -- Find tables that need OPTIMIZE (many small files)
    FOR t IN (
        SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema IN ('SILVER', 'GOLD')
          AND row_count > 1000000
          AND bytes / NULLIF(row_count, 0) < 100  -- Low bytes/row = likely fragmented
    ) DO
        EXECUTE IMMEDIATE 'ALTER TABLE ' || t.full_name || ' CLUSTER BY (SELECT 1)';  -- Re-cluster
        tables_optimized := tables_optimized + 1;
    END FOR;
    
    RETURN 'Optimized ' || tables_optimized || ' tables';
END;
$$;

-- Schedule weekly maintenance:
CREATE TASK admin.weekly_maintenance WAREHOUSE = 'ADMIN_WH' SCHEDULE = 'USING CRON 0 2 * * 0 UTC'
AS CALL admin.table_maintenance();
```

---

## Interview Tips

> **Tip 1:** "How do you build a production ETL procedure?" — Multi-step with: variable tracking current step, TRY/CATCH for error handling, logging after each step, alerting on failure, and RAISE to mark the task as failed. Pattern: step1 → log → step2 → log → ... → EXCEPTION: log step that failed + alert team.

> **Tip 2:** "How do you make procedures reusable?" — Dynamic SQL with table name parameters. Configuration table drives behavior (metadata-driven). Generic MERGE procedure accepts: source, target, primary key → works for any table. Adding a new source = adding a config row, not new code.

> **Tip 3:** "Stored procedure testing?" — Create test schemas with sample data. Call procedure with test parameters. Assert: expected row counts, data values, error handling behavior. Pattern: setup test data → call procedure → validate results → teardown. Run as part of CI/CD staging validation.
