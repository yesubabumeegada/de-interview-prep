---
title: "Stored Procedures - Scenario Questions"
topic: snowflake
subtopic: stored-procedures
content_type: scenario_question
tags: [snowflake, stored-procedures, interview, scenarios]
---

# Scenario Questions — Stored Procedures

<article data-difficulty="junior">

## 🟢 Junior: Basic Stored Procedure

**Scenario:** Write a stored procedure that: truncates gold.daily_revenue, re-inserts today's aggregated data from silver.orders, and returns the row count.

<details>
<summary>💡 Hint</summary>
Use SQL Scripting: TRUNCATE, INSERT, SQLROWCOUNT to capture rows inserted, RETURN the result.
</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE etl.refresh_daily_revenue()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE gold.daily_revenue;
    
    INSERT INTO gold.daily_revenue (order_date, region, revenue, orders)
    SELECT order_date, region, SUM(amount), COUNT(*)
    FROM silver.orders
    WHERE order_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY order_date, region;
    
    RETURN 'SUCCESS: ' || SQLROWCOUNT || ' rows inserted at ' || CURRENT_TIMESTAMP();
END;
$$;

CALL etl.refresh_daily_revenue();
-- Returns: 'SUCCESS: 8450 rows inserted at 2024-03-15 10:30:00'
```

**Key Points:**
- SQLROWCOUNT captures rows affected by the last DML statement
- RETURN provides a status message (visible in task history if called from a task)
- TRUNCATE + INSERT pattern = full refresh (idempotent, safe to re-run)
- No error handling shown (add EXCEPTION block for production)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Generic Incremental Load

**Scenario:** Write a reusable procedure that performs incremental MERGE from any stream into any target table. Parameters: stream name, target table, primary key column.

<details>
<summary>💡 Hint</summary>
Use EXECUTE IMMEDIATE with dynamic SQL. Build the MERGE statement from parameters. Handle the case where stream has no data.
</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE etl.incremental_merge(
    stream_name VARCHAR, target_table VARCHAR, pk_column VARCHAR
)
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    merge_sql VARCHAR;
BEGIN
    -- Check if stream has data
    IF (NOT SYSTEM$STREAM_HAS_DATA(stream_name)) THEN
        RETURN 'SKIPPED: no data in ' || stream_name;
    END IF;
    
    -- Build dynamic MERGE
    merge_sql := 'MERGE INTO ' || target_table || ' t ' ||
        'USING (SELECT * FROM ' || stream_name || 
        ' WHERE METADATA$ACTION = ''INSERT'' ' ||
        'QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || pk_column || 
        ' ORDER BY _loaded_at DESC) = 1) s ' ||
        'ON t.' || pk_column || ' = s.' || pk_column || ' ' ||
        'WHEN MATCHED THEN UPDATE SET * ' ||
        'WHEN NOT MATCHED THEN INSERT *';
    
    EXECUTE IMMEDIATE merge_sql;
    RETURN 'MERGED ' || SQLROWCOUNT || ' rows into ' || target_table;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR on ' || target_table || ': ' || SQLERRM;
END;
$$;

-- Use for any table:
CALL etl.incremental_merge('orders_stream', 'silver.orders', 'order_id');
CALL etl.incremental_merge('customers_stream', 'silver.customers', 'customer_id');
CALL etl.incremental_merge('products_stream', 'silver.products', 'product_id');
```

**Key Points:**
- ONE procedure serves ALL tables (reusable via parameters)
- EXECUTE IMMEDIATE: executes dynamically-built SQL string
- SYSTEM$STREAM_HAS_DATA: skip if nothing to process (saves compute)
- QUALIFY deduplicates within the stream batch (latest version wins)
- EXCEPTION block: catches errors without crashing (logs and returns status)
- Adding a new table: just call with different parameters (zero code changes!)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Step Pipeline with Rollback

**Scenario:** Build a procedure that: (1) loads orders, (2) loads customers, (3) builds enriched table joining both. If step 3 fails, rollback steps 1 and 2 (atomic — all or nothing). Include logging and alerting.

<details>
<summary>💡 Hint</summary>
Use explicit transaction (BEGIN TRANSACTION / COMMIT / ROLLBACK). Wrap all 3 steps in one transaction. On EXCEPTION: ROLLBACK + log + alert.
</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE etl.atomic_enrichment_pipeline()
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    step VARCHAR DEFAULT 'INIT';
    orders_count NUMBER; customers_count NUMBER; enriched_count NUMBER;
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
BEGIN
    BEGIN TRANSACTION;
    
    -- Step 1: Load orders from stream
    step := 'LOAD_ORDERS';
    MERGE INTO silver.orders t
    USING (SELECT * FROM orders_stream WHERE METADATA$ACTION = 'INSERT'
           QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
    orders_count := SQLROWCOUNT;
    
    -- Step 2: Load customers from stream
    step := 'LOAD_CUSTOMERS';
    MERGE INTO silver.customers t
    USING (SELECT * FROM customers_stream WHERE METADATA$ACTION = 'INSERT'
           QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) = 1) s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
    customers_count := SQLROWCOUNT;
    
    -- Step 3: Build enriched (depends on both silver tables)
    step := 'BUILD_ENRICHED';
    INSERT OVERWRITE INTO gold.enriched_orders
    SELECT o.order_id, o.amount, o.order_date, c.name, c.region, c.segment
    FROM silver.orders o
    JOIN silver.customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= DATEADD('day', -30, CURRENT_DATE());
    enriched_count := SQLROWCOUNT;
    
    -- All steps succeeded: commit!
    COMMIT;
    
    -- Log success
    INSERT INTO etl.pipeline_log (pipeline, status, details, duration_sec, run_time)
    VALUES ('enrichment', 'SUCCESS', 
            'Orders: ' || orders_count || ', Customers: ' || customers_count || ', Enriched: ' || enriched_count,
            TIMESTAMPDIFF('second', start_time, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP());
    
    RETURN 'SUCCESS: ' || enriched_count || ' enriched rows';
    
EXCEPTION
    WHEN OTHER THEN
        -- ROLLBACK everything (orders + customers + enriched all reverted!)
        ROLLBACK;
        
        -- Log failure (this INSERT is outside the rolled-back transaction)
        INSERT INTO etl.pipeline_log (pipeline, status, details, run_time)
        VALUES ('enrichment', 'FAILED', 'Step: ' || step || ' Error: ' || SQLERRM, CURRENT_TIMESTAMP());
        
        -- Alert
        CALL system$send_email('data-team@company.com', 'Pipeline FAILED',
            'Enrichment pipeline failed at step: ' || step || '. Error: ' || SQLERRM);
        
        RETURN 'FAILED at ' || step || ': ' || SQLERRM;
END;
$$;
```

**Key Points:**
- BEGIN TRANSACTION: all 3 steps are atomic (all succeed or all roll back)
- If step 3 fails: steps 1 and 2 are ROLLED BACK (streams don't advance either!)
- Step tracking: `step` variable tells us WHERE it failed (for debugging)
- Logging happens OUTSIDE the transaction (INSERT to log table commits separately)
- Alerting: immediately notifies team with the failure step and error message
- This pattern prevents inconsistent state (partial loads between related tables)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are Snowflake Stored Procedures and how do they differ from UDFs?**
A: Stored Procedures contain procedural logic (IF/ELSE, loops, exception handling, multi-statement SQL) and can execute DDL/DML statements. UDFs are stateless scalar or tabular functions that return values and can be called within SQL expressions. Stored Procedures are used for orchestration and data manipulation; UDFs for computation within queries.

**Q: What languages can be used to write Snowflake Stored Procedures?**
A: Snowflake supports JavaScript (original), Snowflake Scripting (SQL-based procedural language), Python, Java, and Scala for stored procedures. Snowflake Scripting is now the recommended approach for most use cases due to its SQL-native syntax and lower learning curve.

**Q: What is Snowflake Scripting and what control structures does it support?**
A: Snowflake Scripting is a procedural SQL extension that adds IF/ELSE, CASE, FOR/WHILE loops, LOOP with EXIT, BEGIN/EXCEPTION/END blocks, and variable declarations to standard SQL. It runs natively in Snowflake without a JavaScript runtime, making it the simplest option for SQL-familiar data engineers.

**Q: What are the caller's rights vs. owner's rights stored procedures?**
A: Owner's rights procedures execute with the privileges of the procedure owner (the role that created it), not the caller. This allows granting controlled access to operations (e.g., INSERT into a table) without granting the caller direct table privileges. Caller's rights procedures execute with the caller's privileges, more restrictive but more transparent.

**Q: How do you pass parameters to and return values from a Snowflake Stored Procedure?**
A: Parameters are declared with name and type in the CREATE PROCEDURE signature. Return values are specified with a RETURNS clause (scalar type, table, or VARIANT). Inside Snowflake Scripting, use RETURN statement; in JavaScript, use `return`. Table-valued results use RETURNS TABLE(...).

**Q: What is exception handling in Snowflake Stored Procedures?**
A: Snowflake Scripting supports BEGIN ... EXCEPTION WHEN <error_code> THEN ... END blocks. You can catch specific SQLSTATE codes or use WHEN OTHER to handle unexpected errors. This enables robust error logging, compensating transactions, and graceful degradation within pipeline procedures.

**Q: How do you call one Stored Procedure from another in Snowflake?**
A: In Snowflake Scripting, use CALL inside the procedure body: `CALL my_other_proc(arg1, arg2)`. Return values can be captured into variables. This enables modular pipeline orchestration entirely within Snowflake without external orchestration tools.

**Q: What are the limitations of Snowflake Stored Procedures for ETL orchestration?**
A: Stored procedures lack native scheduling (you need Tasks for that), don't support parallel execution within a single procedure, have limited observability compared to dedicated orchestration tools like Airflow or dbt, and are harder to test and version-control than Python/SQL scripts. They're best for encapsulating logic, not replacing a full orchestration layer.

---

## 💼 Interview Tips

- Know when NOT to use stored procedures: for complex DAG-based pipeline orchestration, Airflow or dbt provides better observability, testability, and version control. Stored procedures shine for self-contained operational logic.
- Explain caller's rights vs. owner's rights concisely—it's a common interview question and the security implications (privilege escalation control) are the key point, not just the technical difference.
- Mention Snowflake Scripting as the modern recommended approach for new procedures. If you mention JavaScript procedures, acknowledge they're the legacy approach and Scripting has largely superseded them.
- Show that you understand testing challenges: stored procedures are harder to unit test than Python functions. Bring up strategies like parameterized test schemas, rollback-based testing, and integration testing via CI.
- Senior interviewers will probe error handling—always discuss exception blocks and how you'd log errors (e.g., writing to an audit/error table within the exception handler) to enable post-mortem debugging.
