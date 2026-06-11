---
title: "SQL Stored Procedures - Fundamentals"
topic: sql
subtopic: stored-procedures
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, stored-procedures, plpgsql, t-sql, procedural-sql, parameters, transactions]
---

# SQL Stored Procedures — Fundamentals


## 🎯 Analogy

Think of stored procedures like pre-compiled macros saved in the database: complex multi-step logic (insert + update + audit log) runs in a single call, and the database only needs to parse and plan it once.

---
## What Is a Stored Procedure?

A **stored procedure** is a named, pre-compiled collection of SQL statements and optional control flow logic (IF/ELSE, loops, variables) stored in the database and executed by calling its name. Think of it as a function that lives inside the database.

> **Analogy:** A stored procedure is like a script saved on a server. Instead of sending a 50-line SQL script over the network every time you need to process orders, you call `CALL process_orders(2024, 'Q1')` and the database runs the pre-parsed, pre-compiled script locally.

**Key benefits:**
- **Performance:** Pre-compiled query plans (in some databases)
- **Security:** Grant EXECUTE permission without exposing table access
- **Encapsulation:** Business logic lives in one place
- **Network efficiency:** One call instead of many round-trips

---

## Stored Procedure vs Function

These are related but distinct concepts:

| Feature | Stored Procedure | Function |
|---------|-----------------|---------|
| Called with | `CALL` or `EXEC` | Used in `SELECT` |
| Returns | Nothing (or OUT params) | Must return a value |
| Transactions | Can COMMIT/ROLLBACK | Usually cannot (in most databases) |
| Side effects | Can modify data (INSERT/UPDATE/DELETE) | Usually read-only |
| Use in SQL | Cannot be used in SELECT | Can be used in SELECT |

---

## Basic Syntax by Database

### PostgreSQL (PL/pgSQL)

```sql
-- Create a stored procedure
CREATE OR REPLACE PROCEDURE update_product_price(
    p_product_id INT,
    p_new_price  NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE products
    SET price = p_new_price, updated_at = NOW()
    WHERE product_id = p_product_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Product % not found', p_product_id;
    END IF;
    
    COMMIT;  -- PostgreSQL 11+: procedures can commit
END;
$$;

-- Call the procedure:
CALL update_product_price(42, 29.99);
```

### SQL Server (T-SQL)

```sql
-- Create a stored procedure
CREATE OR ALTER PROCEDURE UpdateProductPrice
    @ProductID INT,
    @NewPrice  DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON;  -- Suppress "rows affected" messages
    
    UPDATE Products
    SET Price = @NewPrice, UpdatedAt = GETDATE()
    WHERE ProductID = @ProductID;
    
    IF @@ROWCOUNT = 0
        THROW 50001, 'Product not found', 1;
END;

-- Execute the procedure:
EXEC UpdateProductPrice @ProductID = 42, @NewPrice = 29.99;
```

### MySQL

```sql
DELIMITER $$

CREATE PROCEDURE UpdateProductPrice(
    IN p_product_id INT,
    IN p_new_price  DECIMAL(10,2)
)
BEGIN
    UPDATE products
    SET price = p_new_price, updated_at = NOW()
    WHERE product_id = p_product_id;
    
    IF ROW_COUNT() = 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Product not found';
    END IF;
END$$

DELIMITER ;

-- Call:
CALL UpdateProductPrice(42, 29.99);
```

---

## Variables and Control Flow

### Variables

```sql
-- PostgreSQL
CREATE OR REPLACE PROCEDURE calculate_order_total(p_order_id INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_subtotal    NUMERIC := 0;
    v_discount    NUMERIC := 0;
    v_tax_rate    NUMERIC := 0.08;
    v_total       NUMERIC;
    v_customer_id INT;
BEGIN
    -- Assign a variable from a query
    SELECT customer_id INTO v_customer_id 
    FROM orders WHERE order_id = p_order_id;
    
    SELECT SUM(quantity * unit_price) INTO v_subtotal
    FROM order_items WHERE order_id = p_order_id;
    
    -- Conditional discount
    IF v_subtotal > 100 THEN
        v_discount := v_subtotal * 0.10;  -- 10% discount on orders > $100
    END IF;
    
    v_total := (v_subtotal - v_discount) * (1 + v_tax_rate);
    
    UPDATE orders SET total = v_total WHERE order_id = p_order_id;
    
    RAISE NOTICE 'Order % total: $%', p_order_id, v_total;
END;
$$;
```

### Loops

```sql
-- PostgreSQL: LOOP, WHILE, FOR
CREATE OR REPLACE PROCEDURE process_daily_reports(p_days_back INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_date DATE := CURRENT_DATE - p_days_back;
    v_report_count INT := 0;
BEGIN
    WHILE v_date <= CURRENT_DATE LOOP
        -- Process each day's report
        INSERT INTO daily_summary (report_date, total_orders, total_revenue)
        SELECT 
            v_date,
            COUNT(*),
            SUM(amount)
        FROM orders
        WHERE order_date = v_date
        ON CONFLICT (report_date) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_revenue = EXCLUDED.total_revenue;
        
        v_report_count := v_report_count + 1;
        v_date := v_date + INTERVAL '1 day';
    END LOOP;
    
    RAISE NOTICE 'Processed % daily reports', v_report_count;
END;
$$;

CALL process_daily_reports(30);  -- Re-process last 30 days
```

### Cursor — Iterating Over a Result Set

```sql
-- PostgreSQL: cursor to process rows one at a time
CREATE OR REPLACE PROCEDURE apply_tiered_discounts()
LANGUAGE plpgsql AS $$
DECLARE
    v_customer RECORD;
    v_discount NUMERIC;
    csr CURSOR FOR 
        SELECT customer_id, total_lifetime_value FROM customers WHERE is_active = TRUE;
BEGIN
    OPEN csr;
    LOOP
        FETCH csr INTO v_customer;
        EXIT WHEN NOT FOUND;
        
        -- Tiered discount logic
        IF v_customer.total_lifetime_value >= 10000 THEN
            v_discount := 0.20;
        ELSIF v_customer.total_lifetime_value >= 5000 THEN
            v_discount := 0.15;
        ELSIF v_customer.total_lifetime_value >= 1000 THEN
            v_discount := 0.10;
        ELSE
            v_discount := 0.05;
        END IF;
        
        UPDATE customers SET discount_rate = v_discount
        WHERE customer_id = v_customer.customer_id;
    END LOOP;
    CLOSE csr;
    
    RAISE NOTICE 'Discounts applied to all customers';
END;
$$;
```

> **Performance note:** Cursors process one row at a time — they're often 10–100× slower than set-based SQL for the same operation. Prefer a single UPDATE statement when possible. Cursors are appropriate when you need to call a separate procedure per row or when each row's logic depends on the previous result.

---

## Parameters: IN, OUT, INOUT

```sql
-- PostgreSQL: OUT parameters return values to the caller
CREATE OR REPLACE PROCEDURE get_customer_summary(
    IN  p_customer_id  INT,
    OUT p_order_count  INT,
    OUT p_total_spent  NUMERIC,
    OUT p_customer_name TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    SELECT name INTO p_customer_name FROM customers WHERE customer_id = p_customer_id;
    SELECT COUNT(*), COALESCE(SUM(amount), 0) 
    INTO p_order_count, p_total_spent
    FROM orders WHERE customer_id = p_customer_id;
END;
$$;

-- Calling with OUT parameters (PostgreSQL):
DO $$
DECLARE
    v_count INT;
    v_total NUMERIC;
    v_name TEXT;
BEGIN
    CALL get_customer_summary(42, v_count, v_total, v_name);
    RAISE NOTICE 'Customer: %, Orders: %, Total: $%', v_name, v_count, v_total;
END;
$$;

-- SQL Server: OUT parameters
CREATE PROCEDURE GetCustomerSummary
    @CustomerID INT,
    @OrderCount INT OUTPUT,
    @TotalSpent DECIMAL(10,2) OUTPUT
AS
BEGIN
    SELECT @OrderCount = COUNT(*), @TotalSpent = SUM(Amount)
    FROM Orders WHERE CustomerID = @CustomerID;
END;

-- Call with OUTPUT:
DECLARE @Count INT, @Total DECIMAL(10,2);
EXEC GetCustomerSummary @CustomerID = 42, @OrderCount = @Count OUTPUT, @TotalSpent = @Total OUTPUT;
SELECT @Count, @Total;
```

---

## Error Handling

```sql
-- PostgreSQL: exception handling with EXCEPTION block
CREATE OR REPLACE PROCEDURE safe_transfer_funds(
    p_from_account INT,
    p_to_account   INT,
    p_amount       NUMERIC
)
LANGUAGE plpgsql AS $$
DECLARE
    v_balance NUMERIC;
BEGIN
    -- Check sufficient balance
    SELECT balance INTO v_balance FROM accounts WHERE account_id = p_from_account FOR UPDATE;
    
    IF v_balance < p_amount THEN
        RAISE EXCEPTION 'Insufficient funds: balance is %, transfer amount is %', v_balance, p_amount;
    END IF;
    
    UPDATE accounts SET balance = balance - p_amount WHERE account_id = p_from_account;
    UPDATE accounts SET balance = balance + p_amount WHERE account_id = p_to_account;
    
    INSERT INTO transfer_log (from_account, to_account, amount, transferred_at)
    VALUES (p_from_account, p_to_account, p_amount, NOW());
    
    COMMIT;
    RAISE NOTICE 'Transfer of $% successful', p_amount;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Transfer failed: %', SQLERRM;
        RAISE;  -- Re-raise the original exception
END;
$$;
```

---

## Common Stored Procedure Patterns

### Pattern 1: Upsert (Insert or Update)

```sql
CREATE OR REPLACE PROCEDURE upsert_product(
    p_product_id INT,
    p_name TEXT,
    p_price NUMERIC
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO products(product_id, name, price, created_at, updated_at)
    VALUES (p_product_id, p_name, p_price, NOW(), NOW())
    ON CONFLICT (product_id) DO UPDATE SET
        name = EXCLUDED.name,
        price = EXCLUDED.price,
        updated_at = NOW();
END;
$$;
```

### Pattern 2: Batch Processing

```sql
CREATE OR REPLACE PROCEDURE archive_old_orders(p_cutoff_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_archived_count INT;
BEGIN
    INSERT INTO orders_archive
    SELECT *, NOW() AS archived_at FROM orders WHERE order_date < p_cutoff_date;
    
    GET DIAGNOSTICS v_archived_count = ROW_COUNT;
    
    DELETE FROM orders WHERE order_date < p_cutoff_date;
    
    COMMIT;
    RAISE NOTICE 'Archived % orders before %', v_archived_count, p_cutoff_date;
END;
$$;
```

---


## ▶️ Try It Yourself

```sql
-- Postgres stored procedure
CREATE OR REPLACE PROCEDURE process_order(
    p_order_id INT,
    p_status TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- Update order status
    UPDATE orders SET status = p_status, updated_at = NOW()
    WHERE id = p_order_id;

    -- Insert audit record
    INSERT INTO audit_log (table_name, record_id, action, changed_at)
    VALUES ('orders', p_order_id, 'STATUS_CHANGE', NOW());

    -- Raise error if order not found
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Order % not found', p_order_id;
    END IF;
END;
$$;

-- Call it
CALL process_order(42, 'completed');
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When would you use a stored procedure vs application-layer logic?" — "Stored procedures are useful for: operations requiring transactions that span multiple statements (like fund transfers), batch operations that are more efficient running in the database, enforcing business rules at the data layer, or granting specific access without exposing tables directly. Modern cloud data warehouses (Snowflake, BigQuery) support JavaScript/Python procedures too. For simple data access, I prefer application-layer logic for testability and version control."

> **Tip 2:** "What's the difference between a stored procedure and a stored function?" — "A function must return a value and can be used in a SELECT statement. A procedure doesn't have to return anything (can use OUT parameters), can COMMIT/ROLLBACK transactions (in most databases), and is called with CALL or EXEC. Functions are typically read-only; procedures can modify data and control transactions."

> **Tip 3:** "What's the performance advantage of stored procedures?" — "Pre-compiled execution plan (SQL Server, Oracle) avoids parsing and optimization overhead on repeated calls. Reduced network round-trips — you send one CALL instead of multiple SQL statements. However, in PostgreSQL, plans are cached per session (not permanently), and in Snowflake/BigQuery, stored procedures don't have the same pre-compilation benefits as traditional RDBMS. Performance advantage is most significant in SQL Server and Oracle."
