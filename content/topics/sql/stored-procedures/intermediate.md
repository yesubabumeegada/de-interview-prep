---
title: "SQL Stored Procedures - Intermediate"
topic: sql
subtopic: stored-procedures
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, stored-procedures, dynamic-sql, transactions, savepoints, snowflake-procedures, error-handling]
---

# SQL Stored Procedures — Intermediate Concepts

## Dynamic SQL

Dynamic SQL allows you to build and execute SQL strings at runtime — essential when the table name, column name, or query structure can't be known at compile time.

### PostgreSQL: EXECUTE

```sql
CREATE OR REPLACE PROCEDURE dynamic_archive(
    p_table_name TEXT,
    p_cutoff_date DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_archive_table TEXT := p_table_name || '_archive';
    v_sql TEXT;
    v_count INT;
BEGIN
    -- Build the SQL string dynamically
    v_sql := format(
        'INSERT INTO %I SELECT *, NOW() AS archived_at FROM %I WHERE created_at < %L',
        v_archive_table,
        p_table_name,
        p_cutoff_date
    );
    
    EXECUTE v_sql;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    v_sql := format('DELETE FROM %I WHERE created_at < %L', p_table_name, p_cutoff_date);
    EXECUTE v_sql;
    
    COMMIT;
    RAISE NOTICE 'Archived % rows from % before %', v_count, p_table_name, p_cutoff_date;
END;
$$;

CALL dynamic_archive('orders', '2022-01-01');
CALL dynamic_archive('events', '2022-01-01');
```

**Key functions:**
- `format('%I', name)` — safely quotes identifier (table/column name), preventing SQL injection
- `format('%L', value)` — safely quotes a literal value
- `EXECUTE sql_string` — executes the dynamic SQL
- `EXECUTE sql INTO var` — executes and captures the result

### SQL Server: sp_executesql

```sql
CREATE OR ALTER PROCEDURE DynamicPartitionQuery
    @SchemaName NVARCHAR(128),
    @TableName  NVARCHAR(128),
    @StartDate  DATE,
    @EndDate    DATE
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @Params NVARCHAR(500);
    
    -- Parameterized dynamic SQL (safe from SQL injection)
    SET @SQL = N'SELECT * FROM ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +
               N' WHERE order_date BETWEEN @Start AND @End';
    
    SET @Params = N'@Start DATE, @End DATE';
    
    EXEC sp_executesql @SQL, @Params, @Start = @StartDate, @End = @EndDate;
END;
```

> **Security warning:** Never concatenate user input directly into SQL strings — use `format('%I', ...)` in PostgreSQL or `sp_executesql` with parameters in SQL Server. Direct concatenation allows SQL injection attacks.

---

## Transaction Control in Procedures

Procedures are the natural home for multi-statement transactions:

```sql
-- PostgreSQL: complete fund transfer with full transaction control
CREATE OR REPLACE PROCEDURE process_order(
    p_customer_id INT,
    p_product_id  INT,
    p_quantity    INT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_price        NUMERIC;
    v_stock        INT;
    v_order_id     INT;
    v_total        NUMERIC;
BEGIN
    -- Lock the product row to prevent concurrent over-selling
    SELECT price, stock_quantity 
    INTO v_price, v_stock
    FROM products WHERE product_id = p_product_id FOR UPDATE;
    
    IF v_stock < p_quantity THEN
        RAISE EXCEPTION 'Insufficient stock: % available, % requested', v_stock, p_quantity;
    END IF;
    
    v_total := v_price * p_quantity;
    
    -- Deduct stock
    UPDATE products SET stock_quantity = stock_quantity - p_quantity
    WHERE product_id = p_product_id;
    
    -- Create order
    INSERT INTO orders (customer_id, total, status, created_at)
    VALUES (p_customer_id, v_total, 'confirmed', NOW())
    RETURNING order_id INTO v_order_id;
    
    -- Create order item
    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
    VALUES (v_order_id, p_product_id, p_quantity, v_price);
    
    -- Update customer lifetime value
    UPDATE customers SET lifetime_value = lifetime_value + v_total
    WHERE customer_id = p_customer_id;
    
    COMMIT;
    RAISE NOTICE 'Order % created for $%', v_order_id, v_total;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Order failed: %', SQLERRM;
        RAISE;
END;
$$;
```

### Savepoints — Partial Rollback

```sql
-- Use savepoints to rollback only part of a transaction
CREATE OR REPLACE PROCEDURE bulk_import_with_recovery(p_batch_id INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_row RECORD;
    v_errors INT := 0;
    v_success INT := 0;
BEGIN
    FOR v_row IN SELECT * FROM staging_orders WHERE batch_id = p_batch_id LOOP
        BEGIN
            SAVEPOINT row_savepoint;  -- Mark this point
            
            -- Try to process each row
            INSERT INTO orders (customer_id, amount, status)
            VALUES (v_row.customer_id, v_row.amount, 'pending');
            
            v_success := v_success + 1;
            RELEASE SAVEPOINT row_savepoint;
            
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK TO SAVEPOINT row_savepoint;  -- Undo just this row
                v_errors := v_errors + 1;
                
                -- Log the error but continue processing other rows
                INSERT INTO import_errors (batch_id, raw_data, error_msg, failed_at)
                VALUES (p_batch_id, v_row::TEXT, SQLERRM, NOW());
        END;
    END LOOP;
    
    COMMIT;
    RAISE NOTICE 'Batch %: % success, % errors', p_batch_id, v_success, v_errors;
END;
$$;
```

---

## Stored Procedures in Cloud Data Warehouses

### Snowflake: JavaScript Procedures

Snowflake supports stored procedures in JavaScript, Python, Scala, and Java — not just SQL:

```sql
-- Snowflake: JavaScript procedure for complex logic
CREATE OR REPLACE PROCEDURE merge_customer_data(batch_date VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
AS $$
    var today = BATCH_DATE;
    
    // Run SQL from JavaScript using snowflake.execute()
    var merge_result = snowflake.execute({
        sqlText: `
            MERGE INTO customers t
            USING staging_customers s ON t.customer_id = s.customer_id
            WHEN MATCHED THEN UPDATE SET 
                t.email = s.email,
                t.updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (customer_id, email, created_at)
            VALUES (s.customer_id, s.email, CURRENT_TIMESTAMP())
        `
    });
    
    var rows_merged = merge_result.getRowCount();
    
    // Log the run
    snowflake.execute({
        sqlText: `INSERT INTO etl_log (run_date, rows_merged) VALUES ('${today}', ${rows_merged})`
    });
    
    return `Merged ${rows_merged} customers on ${today}`;
$$;

CALL merge_customer_data('2024-01-15');
```

### Snowflake: SQL Scripting (Newer — Preferred)

```sql
-- Snowflake SQL scripting (no JavaScript needed for pure SQL logic)
CREATE OR REPLACE PROCEDURE daily_rollup(run_date DATE)
    RETURNS STRING
    LANGUAGE SQL
AS $$
DECLARE
    v_count INT DEFAULT 0;
BEGIN
    INSERT INTO daily_sales_summary (sale_date, total_revenue, order_count)
    SELECT 
        :run_date,
        COALESCE(SUM(amount), 0),
        COUNT(*)
    FROM orders
    WHERE order_date = :run_date;
    
    SELECT COUNT(*) INTO :v_count
    FROM daily_sales_summary WHERE sale_date = :run_date;
    
    RETURN 'Inserted ' || v_count || ' rows for ' || run_date;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error: ' || SQLERRM;
END;
$$;
```

### BigQuery: Scripting (Not Full Procedures)

BigQuery doesn't have traditional stored procedures but supports scripting:

```sql
-- BigQuery: procedural scripting with variables
DECLARE cutoff_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
DECLARE rows_archived INT64 DEFAULT 0;

BEGIN TRANSACTION;

INSERT INTO `project.dataset.orders_archive`
SELECT *, CURRENT_TIMESTAMP() AS archived_at
FROM `project.dataset.orders`
WHERE order_date < cutoff_date;

SET rows_archived = @@row_count;

DELETE FROM `project.dataset.orders`
WHERE order_date < cutoff_date;

COMMIT TRANSACTION;

SELECT CONCAT('Archived ', CAST(rows_archived AS STRING), ' orders') AS result;
```

---

## Procedure Security: Definer vs Invoker Rights

```sql
-- PostgreSQL: SECURITY DEFINER — runs as the procedure owner, not the caller
-- Useful for granting access to tables the caller can't directly access
CREATE OR REPLACE PROCEDURE admin_only_cleanup()
LANGUAGE plpgsql
SECURITY DEFINER  -- Runs as the owner (admin), not the calling user
AS $$
BEGIN
    DELETE FROM sensitive_logs WHERE created_at < NOW() - INTERVAL '90 days';
    COMMIT;
END;
$$;

-- Grant execute to app_role (which cannot directly DELETE from sensitive_logs)
GRANT EXECUTE ON PROCEDURE admin_only_cleanup() TO app_role;
-- app_role can call this procedure and it will run with admin permissions
-- But app_role cannot DELETE from sensitive_logs directly

-- SECURITY INVOKER (default): runs as the calling user
CREATE OR REPLACE PROCEDURE user_cleanup()
LANGUAGE plpgsql
SECURITY INVOKER  -- Default: runs as the caller
AS $$
BEGIN
    -- User must have DELETE permission on the table themselves
    DELETE FROM my_sessions WHERE user_id = current_user_id();
    COMMIT;
END;
$$;
```

---

## Testing Stored Procedures

```sql
-- PostgreSQL: test in a transaction that gets rolled back
BEGIN;

-- Set up test data
INSERT INTO customers VALUES (9999, 'Test Customer', 'test@test.com', 'US', TRUE);
INSERT INTO accounts VALUES (9999, 1000.00);

-- Call the procedure
CALL safe_transfer_funds(9999, 1, 500.00);

-- Verify results
SELECT balance FROM accounts WHERE account_id = 9999;  -- Should be 500.00

-- Rollback test changes
ROLLBACK;
```

---

## Interview Tips

> **Tip 1:** "What are the risks of dynamic SQL in stored procedures?" — "SQL injection if user input is concatenated directly. The fix is to always use parameterized queries (sp_executesql in SQL Server) or the format('%L', value) quoting function in PostgreSQL. For table/column names, use QUOTENAME() in SQL Server or format('%I', name) in PostgreSQL — these properly quote identifiers. I always treat any value that originated outside the procedure as untrusted input."

> **Tip 2:** "How do you handle partial failures in a procedure that processes multiple rows?" — "Savepoints allow you to rollback just the failing row while keeping successful rows committed. I wrap each row's processing in a nested BEGIN/EXCEPTION block with a SAVEPOINT before the operation and ROLLBACK TO SAVEPOINT in the exception handler. Failed rows are logged to an error table with the error message for investigation, and the batch continues with the remaining rows."

> **Tip 3:** "How do you test stored procedures?" — "I wrap tests in a BEGIN/ROLLBACK block — set up test data, call the procedure, assert the expected state, then rollback. This is non-destructive and can be run against a dev database. For production procedures with transactions that internally COMMIT, I use a test schema with mirrored tables. I also add RAISE NOTICE or logging table inserts to make the procedure self-documenting during testing."
