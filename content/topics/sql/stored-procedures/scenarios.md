---
title: "SQL Stored Procedures - Scenario Questions"
topic: sql
subtopic: stored-procedures
content_type: scenario_question
tags: [sql, stored-procedures, interview, scenarios, transactions, etl, plpgsql]
---

# Scenario Questions — SQL Stored Procedures

<article data-difficulty="junior">

## 🟢 Junior: Write a Simple Upsert Procedure

**Scenario:** You need to create a stored procedure `upsert_customer` that accepts `p_customer_id INT`, `p_name TEXT`, `p_email TEXT`, and `p_country TEXT`. If the customer exists, update the name and email. If not, insert a new row. The procedure should also update `updated_at` on every call.

```sql
-- Target table:
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    country     VARCHAR(2),
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
```

<details>
<summary>💡 Hint</summary>

PostgreSQL has `INSERT ... ON CONFLICT DO UPDATE` which handles upsert in a single statement. Wrap it in a procedure that takes the parameters and commits at the end.

</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE upsert_customer(
    p_customer_id INT,
    p_name        TEXT,
    p_email       TEXT,
    p_country     TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO customers (customer_id, name, email, country, created_at, updated_at)
    VALUES (p_customer_id, p_name, p_email, p_country, NOW(), NOW())
    ON CONFLICT (customer_id) DO UPDATE SET
        name       = EXCLUDED.name,
        email      = EXCLUDED.email,
        country    = EXCLUDED.country,
        updated_at = NOW();
    -- Note: created_at is NOT updated on conflict — preserved from original insert
    
    COMMIT;
    RAISE NOTICE 'Customer % upserted successfully', p_customer_id;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE EXCEPTION 'Failed to upsert customer %: %', p_customer_id, SQLERRM;
END;
$$;

-- Usage:
CALL upsert_customer(101, 'Alice Smith', 'alice@example.com', 'US');
CALL upsert_customer(101, 'Alice J. Smith', 'alice.new@example.com', 'US');  -- Updates name/email
```

**Key design choices:**
- `ON CONFLICT (customer_id) DO UPDATE` is atomic — no race condition between SELECT and INSERT
- `EXCLUDED.name` refers to the value that WOULD have been inserted (the new values)
- `created_at` is intentionally excluded from the UPDATE SET — first-insert timestamp is preserved
- The EXCEPTION block rolls back and re-raises with context — callers know what failed

**SQL Server equivalent:**
```sql
CREATE OR ALTER PROCEDURE UpsertCustomer
    @CustomerID INT, @Name NVARCHAR(100), @Email NVARCHAR(200), @Country CHAR(2)
AS
BEGIN
    MERGE INTO Customers t USING (SELECT @CustomerID) s(CustomerID) ON t.CustomerID = s.CustomerID
    WHEN MATCHED THEN UPDATE SET Name = @Name, Email = @Email, UpdatedAt = GETDATE()
    WHEN NOT MATCHED THEN INSERT (CustomerID, Name, Email, Country, CreatedAt, UpdatedAt)
        VALUES (@CustomerID, @Name, @Email, @Country, GETDATE(), GETDATE());
END;
```

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Add Logging to an Existing Procedure

**Scenario:** You have a procedure `process_daily_batch(p_date DATE)` that loads data. The ops team can't tell if it succeeded without checking the database manually. Add logging so that every run records: start time, end time, rows processed, and success/failure status in a `batch_runs` table.

```sql
-- Create the logging table:
CREATE TABLE batch_runs (
    run_id      SERIAL PRIMARY KEY,
    proc_name   TEXT,
    run_date    DATE,
    started_at  TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    rows_processed INT,
    status      TEXT,
    error_msg   TEXT
);
```

<details>
<summary>💡 Hint</summary>

Insert a row at the start of the procedure with status='running', then UPDATE it to 'success' or 'failed' at the end. Use a variable to store the `run_id` so you can update the right row. Handle errors in an EXCEPTION block.

</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE process_daily_batch(p_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id      INT;
    v_start_time  TIMESTAMPTZ := clock_timestamp();
    v_row_count   INT := 0;
BEGIN
    -- Log start
    INSERT INTO batch_runs (proc_name, run_date, started_at, status)
    VALUES ('process_daily_batch', p_date, v_start_time, 'running')
    RETURNING run_id INTO v_run_id;

    -- Main processing logic
    INSERT INTO processed_orders (order_id, customer_id, amount, processed_at)
    SELECT order_id, customer_id, amount, NOW()
    FROM raw_orders
    WHERE order_date = p_date
      AND status = 'pending';

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- Mark as success
    UPDATE batch_runs SET
        status         = 'success',
        rows_processed = v_row_count,
        completed_at   = clock_timestamp()
    WHERE run_id = v_run_id;

    COMMIT;
    RAISE NOTICE 'Batch for % complete: % rows processed', p_date, v_row_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log failure (ROLLBACK first, then INSERT in new transaction isn't needed
        -- because the UPDATE to batch_runs is in the same transaction that we ROLLBACK)
        -- Instead: update the log before rollback
        UPDATE batch_runs SET
            status       = 'failed',
            error_msg    = SQLERRM,
            completed_at = clock_timestamp()
        WHERE run_id = v_run_id;
        COMMIT;  -- Commit just the log update
        
        RAISE EXCEPTION 'Batch for % failed: %', p_date, SQLERRM;
END;
$$;
```

**Important subtlety:** In the EXCEPTION block, we UPDATE the log then COMMIT before re-raising. This is because a ROLLBACK would erase the log entry we inserted at the start. By committing the failure record, we always have a trace of what happened.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Write a Procedure to Archive Old Data

**Scenario:** The `orders` table has 200 million rows and is growing. Orders older than 3 years need to be moved to `orders_archive` in batches to avoid locking the table during a single large transaction. Write a procedure `archive_orders(p_cutoff_date DATE, p_batch_size INT)` that archives in batches and stops after each batch to release locks.

<details>
<summary>💡 Hint</summary>

Instead of one big INSERT-DELETE, use a loop that: (1) selects a batch of old order IDs using LIMIT, (2) INSERTs that batch into the archive table, (3) DELETEs those same IDs from the source, (4) COMMITs, then loops again. This commits after each batch, releasing row locks between iterations.

</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE archive_orders(
    p_cutoff_date DATE,
    p_batch_size  INT DEFAULT 10000
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_ids  BIGINT[];
    v_archived   INT := 0;
    v_total      INT := 0;
    v_batch_num  INT := 0;
BEGIN
    RAISE NOTICE 'Starting archive of orders before %', p_cutoff_date;
    
    LOOP
        -- Select a batch of order IDs to archive
        SELECT ARRAY(
            SELECT order_id FROM orders
            WHERE order_date < p_cutoff_date
            ORDER BY order_id
            LIMIT p_batch_size
        ) INTO v_batch_ids;
        
        -- Exit if no more rows to archive
        EXIT WHEN array_length(v_batch_ids, 1) IS NULL OR array_length(v_batch_ids, 1) = 0;
        
        -- Insert batch into archive
        INSERT INTO orders_archive
        SELECT *, NOW() AS archived_at
        FROM orders
        WHERE order_id = ANY(v_batch_ids);
        
        GET DIAGNOSTICS v_archived = ROW_COUNT;
        
        -- Delete the archived rows from source
        DELETE FROM orders WHERE order_id = ANY(v_batch_ids);
        
        COMMIT;  -- Release locks after each batch
        
        v_total   := v_total + v_archived;
        v_batch_num := v_batch_num + 1;
        RAISE NOTICE 'Batch %: archived % rows (total so far: %)', v_batch_num, v_archived, v_total;
        
        -- Optional: sleep briefly between batches to reduce I/O pressure
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    RAISE NOTICE 'Archive complete: % total rows archived in % batches', v_total, v_batch_num;
END;
$$;

-- Call from psql or Airflow:
CALL archive_orders('2021-01-01', 5000);
```

**Why batch processing:**
- A single DELETE of 10M rows holds row locks for the entire duration — blocking all concurrent writes
- Batch commits (every 10K rows) keep lock hold time to milliseconds per batch
- If the procedure is interrupted, partial progress is preserved — only uncommitted batches are lost
- `ORDER BY order_id LIMIT p_batch_size` ensures consistent, forward-only batching (no row can be processed twice)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Debug a Parameter Sniffing Problem (SQL Server)

**Scenario:** A SQL Server stored procedure `GetOrdersByCustomer` runs in 50ms for most customers but takes 45 seconds for the customer with 500,000 orders (customer_id = 1). Both customer IDs use the same procedure. Diagnose and fix the problem.

```sql
CREATE PROCEDURE GetOrdersByCustomer @CustomerID INT
AS
BEGIN
    SELECT order_id, order_date, amount, status
    FROM orders
    WHERE customer_id = @CustomerID
    ORDER BY order_date DESC;
END;

-- Normal customer: returns 20 rows in 50ms
EXEC GetOrdersByCustomer @CustomerID = 999;

-- Large customer: returns 500,000 rows in 45 seconds!
EXEC GetOrdersByCustomer @CustomerID = 1;
```

<details>
<summary>💡 Hint</summary>

The plan cached when customer_id = 999 (20 rows) used an Index Seek. That plan is terrible for 500,000 rows where a Table Scan would be better. This is parameter sniffing. To confirm: clear the plan cache and run with customer_id = 1 first, then check if customer 999 becomes slow.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis:**
```sql
-- Step 1: Check what plan is cached
SELECT 
    qs.plan_handle,
    qs.execution_count,
    qp.query_plan  -- XML execution plan
FROM sys.dm_exec_procedure_stats qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE OBJECT_NAME(qs.object_id) = 'GetOrdersByCustomer';
-- Plan shows: Index Seek on customer_id index
-- This is correct for 20 rows but wrong for 500,000 rows

-- Step 2: Confirm it's parameter sniffing
DBCC FREEPROCCACHE;  -- Clear plan cache (DEV ONLY — never in production!)
EXEC GetOrdersByCustomer @CustomerID = 1;  -- Now runs fast (Table Scan plan cached)
EXEC GetOrdersByCustomer @CustomerID = 999;  -- Now this is slow!
-- Proves the problem: whichever runs first determines the plan for all
```

**Fix 1 — OPTIMIZE FOR UNKNOWN (recommended for most cases):**
```sql
CREATE OR ALTER PROCEDURE GetOrdersByCustomer @CustomerID INT
AS
BEGIN
    SELECT order_id, order_date, amount, status
    FROM orders
    WHERE customer_id = @CustomerID
    ORDER BY order_date DESC
    OPTION (OPTIMIZE FOR (@CustomerID UNKNOWN));
    -- Uses average statistics — not optimized for extremes but consistent for all
END;
```

**Fix 2 — Statement-level RECOMPILE (when distribution varies dramatically):**
```sql
CREATE OR ALTER PROCEDURE GetOrdersByCustomer @CustomerID INT
AS
BEGIN
    SELECT order_id, order_date, amount, status
    FROM orders
    WHERE customer_id = @CustomerID
    ORDER BY order_date DESC
    OPTION (RECOMPILE);  -- Re-plan on every call — overhead per call but always optimal
END;
```

**Fix 3 — Explicit branching (best when you know the "whale" customers):**
```sql
CREATE OR ALTER PROCEDURE GetOrdersByCustomer @CustomerID INT
AS
BEGIN
    DECLARE @OrderCount INT;
    SELECT @OrderCount = COUNT(*) FROM orders WHERE customer_id = @CustomerID;
    
    IF @OrderCount > 100000
        -- Force Table Scan for large customers
        SELECT order_id, order_date, amount, status FROM orders WITH (INDEX(0))
        WHERE customer_id = @CustomerID ORDER BY order_date DESC;
    ELSE
        -- Normal Index Seek for typical customers
        SELECT order_id, order_date, amount, status FROM orders
        WHERE customer_id = @CustomerID ORDER BY order_date DESC;
END;
```

**Recommendation:** Fix 1 (OPTIMIZE FOR UNKNOWN) for most cases. Fix 2 (RECOMPILE) if there are many distinct distributions and the recompile overhead (~1ms) is acceptable. Fix 3 for known extreme outliers.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design an Idempotent Payment Processing Procedure

**Scenario:** Build a PostgreSQL procedure `process_payment(p_payment_request_id UUID, p_customer_id INT, p_amount NUMERIC, p_idempotency_key TEXT)` that processes a payment. Requirements:

1. If called twice with the same `idempotency_key`, return the original result without charging again
2. Verify the customer has sufficient credit
3. Create a payment record and deduct from credit balance atomically
4. Handle race conditions (two concurrent requests for the same customer)
5. Log every attempt (including duplicates)

<details>
<summary>💡 Hint</summary>

The idempotency key check must happen BEFORE acquiring any locks — return early if the key was already processed. Use `FOR UPDATE` on the credit balance row to serialize concurrent requests for the same customer. The entire credit-check + deduct + payment-create must be in one transaction.

</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE PROCEDURE process_payment(
    p_payment_request_id UUID,
    p_customer_id        INT,
    p_amount             NUMERIC,
    p_idempotency_key    TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_existing_payment_id UUID;
    v_credit_balance       NUMERIC;
    v_new_payment_id       UUID := gen_random_uuid();
BEGIN
    -- Step 1: Check idempotency (before any locks)
    SELECT payment_id INTO v_existing_payment_id
    FROM payments
    WHERE idempotency_key = p_idempotency_key;
    
    IF FOUND THEN
        -- Already processed — return the original result without charging again
        INSERT INTO payment_attempt_log (payment_request_id, idempotency_key, result, logged_at)
        VALUES (p_payment_request_id, p_idempotency_key, 'DUPLICATE_SKIPPED', NOW());
        COMMIT;
        RAISE NOTICE 'Duplicate payment request % — returning original payment %',
            p_payment_request_id, v_existing_payment_id;
        RETURN;
    END IF;

    -- Step 2: Lock the customer's credit row (prevents concurrent over-spending)
    -- FOR UPDATE acquires a row lock — concurrent calls for same customer_id wait here
    SELECT credit_balance INTO v_credit_balance
    FROM customer_credits
    WHERE customer_id = p_customer_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer % has no credit account', p_customer_id;
    END IF;

    -- Step 3: Verify sufficient credit
    IF v_credit_balance < p_amount THEN
        -- Log the failed attempt before raising
        INSERT INTO payment_attempt_log (payment_request_id, idempotency_key, result, error_detail, logged_at)
        VALUES (p_payment_request_id, p_idempotency_key, 'INSUFFICIENT_CREDIT',
                format('Balance: %s, Required: %s', v_credit_balance, p_amount), NOW());
        COMMIT;  -- Commit the log entry
        RAISE EXCEPTION 'Insufficient credit: balance=%, required=%', v_credit_balance, p_amount;
    END IF;

    -- Step 4: Deduct credit (within the same lock)
    UPDATE customer_credits
    SET credit_balance = credit_balance - p_amount,
        last_transaction_at = NOW()
    WHERE customer_id = p_customer_id;

    -- Step 5: Create payment record
    INSERT INTO payments (payment_id, payment_request_id, customer_id, amount, idempotency_key, status, created_at)
    VALUES (v_new_payment_id, p_payment_request_id, p_customer_id, p_amount, p_idempotency_key, 'completed', NOW());

    -- Step 6: Log success
    INSERT INTO payment_attempt_log (payment_request_id, idempotency_key, result, logged_at)
    VALUES (p_payment_request_id, p_idempotency_key, 'SUCCESS', NOW());

    COMMIT;
    RAISE NOTICE 'Payment % processed: customer_id=%, amount=%', v_new_payment_id, p_customer_id, p_amount;

EXCEPTION
    WHEN unique_violation THEN
        -- Two concurrent requests with same idempotency_key: one will hit unique constraint
        -- The other's payment already committed — safe to return without error
        ROLLBACK;
        RAISE NOTICE 'Concurrent duplicate for idempotency_key % — already processed', p_idempotency_key;
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- Create necessary constraints for idempotency:
ALTER TABLE payments ADD CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key);
```

**Why this design is correct:**
- Checking idempotency key BEFORE locking prevents all duplicate charges — the early return is the critical path
- `FOR UPDATE` on the credit row serializes all concurrent payments for the same customer — no two transactions can read-then-write the balance concurrently
- The `unique_violation` catch handles a race condition: two requests with the same key pass the initial check simultaneously, but the UNIQUE constraint ensures only one INSERT succeeds
- Each stage (duplicate skip, insufficient credit, success) has its own log entry for full auditability

</details>

</article>
