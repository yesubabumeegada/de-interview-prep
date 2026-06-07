---
title: "Stored Procedures - Fundamentals"
topic: snowflake
subtopic: stored-procedures
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, stored-procedures, sql, javascript, python, automation]
---

# Snowflake Stored Procedures — Fundamentals

## What Are Stored Procedures?

Stored Procedures are **reusable blocks of code** stored in Snowflake that execute SQL, JavaScript, Python, Java, or Scala logic. They support: variables, loops, conditionals, error handling, and multi-statement transactions.

```sql
-- Simple stored procedure (SQL Scripting):
CREATE OR REPLACE PROCEDURE etl.refresh_daily_revenue()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Truncate target
    TRUNCATE TABLE gold.daily_revenue;
    
    -- Step 2: Insert fresh data
    INSERT INTO gold.daily_revenue
    SELECT order_date, region, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders
    WHERE order_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY order_date, region;
    
    RETURN 'SUCCESS: ' || SQLROWCOUNT || ' rows inserted';
END;
$$;

-- Call it:
CALL etl.refresh_daily_revenue();
-- Returns: 'SUCCESS: 8450 rows inserted'
```

> **Key Insight for DE:** Stored procedures enable procedural logic (IF/ELSE, loops, error handling) that plain SQL can't do. Use them for: complex ETL orchestration, conditional data processing, and administrative automation.

---

## Languages Supported

| Language | Use Case | Performance |
|----------|----------|-------------|
| **SQL Scripting** | ETL logic, DML operations | Best (native) |
| **JavaScript** | Complex logic, JSON manipulation | Good |
| **Python** | ML, pandas, external APIs | Good (Snowpark) |
| **Java** | Enterprise integration | Good |
| **Scala** | Spark-like operations | Good |

```sql
-- SQL Scripting (recommended for most ETL):
CREATE PROCEDURE etl.my_proc() RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN ... END; $$;

-- Python (Snowpark — for ML/pandas):
CREATE PROCEDURE ml.train_model() RETURNS VARCHAR LANGUAGE PYTHON
RUNTIME_VERSION = '3.10' PACKAGES = ('pandas', 'scikit-learn')
HANDLER = 'main' AS $$ def main(session): ... $$;

-- JavaScript (legacy, still common):
CREATE PROCEDURE admin.cleanup() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ ... $$;
```

---

## SQL Scripting Basics

### Variables and Control Flow

```sql
CREATE OR REPLACE PROCEDURE etl.process_date_range(start_date DATE, end_date DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    current_date DATE;
    total_processed NUMBER DEFAULT 0;
    batch_count NUMBER;
BEGIN
    current_date := start_date;
    
    -- Loop through each date
    WHILE (current_date <= end_date) DO
        INSERT INTO silver.daily_orders
        SELECT * FROM raw.orders WHERE order_date = current_date;
        
        batch_count := SQLROWCOUNT;
        total_processed := total_processed + batch_count;
        current_date := DATEADD('day', 1, current_date);
    END WHILE;
    
    RETURN 'Processed ' || total_processed || ' rows across ' || 
           DATEDIFF('day', start_date, end_date) + 1 || ' days';
END;
$$;

CALL etl.process_date_range('2024-03-01'::DATE, '2024-03-15'::DATE);
```

### Conditional Logic

```sql
CREATE OR REPLACE PROCEDURE etl.smart_refresh(table_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    row_count NUMBER;
    last_load TIMESTAMP;
BEGIN
    -- Check current state
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || table_name INTO row_count;
    EXECUTE IMMEDIATE 'SELECT MAX(_loaded_at) FROM ' || table_name INTO last_load;
    
    -- Decide strategy based on state
    IF (row_count = 0) THEN
        -- Empty table: full load
        EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || ' SELECT * FROM raw.' || SPLIT_PART(table_name, '.', -1);
        RETURN 'FULL LOAD: ' || SQLROWCOUNT || ' rows';
    ELSEIF (last_load < DATEADD('hour', -24, CURRENT_TIMESTAMP())) THEN
        -- Stale: full refresh
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || table_name;
        EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || ' SELECT * FROM raw.' || SPLIT_PART(table_name, '.', -1);
        RETURN 'FULL REFRESH (stale): ' || SQLROWCOUNT || ' rows';
    ELSE
        -- Fresh: incremental
        EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || 
            ' SELECT * FROM raw.' || SPLIT_PART(table_name, '.', -1) || 
            ' WHERE _loaded_at > ''' || last_load || '''';
        RETURN 'INCREMENTAL: ' || SQLROWCOUNT || ' new rows';
    END IF;
END;
$$;
```

---

## Error Handling

```sql
CREATE OR REPLACE PROCEDURE etl.safe_merge_orders()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    error_msg VARCHAR;
BEGIN
    -- Attempt the MERGE
    MERGE INTO silver.orders t
    USING raw.orders_stream s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.status = s.status
    WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.amount, s.order_date);
    
    RETURN 'SUCCESS: ' || SQLROWCOUNT || ' rows merged';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        error_msg := SQLERRM;
        -- Log the error
        INSERT INTO etl.error_log (procedure_name, error_message, error_time)
        VALUES ('safe_merge_orders', error_msg, CURRENT_TIMESTAMP());
        RETURN 'ERROR: ' || error_msg;
    WHEN OTHER THEN
        error_msg := SQLERRM;
        INSERT INTO etl.error_log (procedure_name, error_message, error_time)
        VALUES ('safe_merge_orders', 'UNEXPECTED: ' || error_msg, CURRENT_TIMESTAMP());
        RAISE;  -- Re-raise unexpected errors
END;
$$;
```

---

## Using Stored Procedures with Tasks

```sql
-- Common pattern: Task calls a stored procedure

CREATE TASK etl.hourly_orders_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    CALL etl.safe_merge_orders();
-- Task handles scheduling; procedure handles logic + error handling

-- Multi-step task calling multiple procedures:
CREATE TASK etl.pipeline_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '30 MINUTE'
AS
BEGIN
    CALL etl.ingest_orders();
    CALL etl.ingest_customers();
    CALL etl.build_gold_metrics();
    CALL etl.send_completion_notification();
END;
```

---

## Caller's Rights vs Owner's Rights

```sql
-- OWNER'S RIGHTS (default): procedure runs with the OWNER's permissions
CREATE PROCEDURE admin.drop_table(table_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER  -- Runs with procedure owner's privileges
AS $$ ... $$;
-- Use for: admin procedures that need elevated access

-- CALLER'S RIGHTS: procedure runs with the CALLER's permissions
CREATE PROCEDURE analytics.query_data(sql_text VARCHAR)
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS CALLER  -- Runs with whoever calls it
AS $$ ... $$;
-- Use for: utility procedures that should respect caller's access level
```

---

## Interview Tips

> **Tip 1:** "When do you use stored procedures vs Dynamic Tables/Tasks?" — Stored procedures for: complex conditional logic (IF/ELSE), multi-step transactions, error handling with logging, administrative tasks, and calling external services. Dynamic Tables for: declarative SQL transformations. Tasks for: scheduling simple SQL. Combine: Task calls a stored procedure (scheduling + complex logic).

> **Tip 2:** "SQL Scripting vs JavaScript vs Python?" — SQL Scripting: default for ETL (fastest, native DML). Python (Snowpark): for ML, pandas, external API calls. JavaScript: legacy (use SQL Scripting for new code). Choose based on: what operations you need (DML → SQL, ML → Python, string manipulation → either).

> **Tip 3:** "How do you handle errors in stored procedures?" — TRY/CATCH (EXCEPTION block): log error to an error_log table, optionally RAISE to propagate. For Tasks: if procedure raises → task is marked FAILED. Pattern: attempt operation → catch error → log → decide: retry, skip, or raise.
