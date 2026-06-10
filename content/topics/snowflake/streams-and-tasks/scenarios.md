---
title: "Streams and Tasks - Scenario Questions"
topic: snowflake
subtopic: streams-and-tasks
content_type: scenario_question
tags: [snowflake, streams, tasks, interview, scenarios]
---

# Scenario Questions — Streams and Tasks

<article data-difficulty="junior">

## 🟢 Junior: Basic Stream + Task Setup

**Scenario:** New orders are inserted into `raw.orders` every few minutes via Snowpipe. Set up a stream and task to incrementally load new orders into `silver.orders` every 15 minutes (only process new rows, not the full table).

<details>
<summary>💡 Hint</summary>
Create a stream on raw.orders (tracks new inserts). Create a task on a 15-minute schedule with WHEN clause to check if stream has data. INSERT FROM stream with METADATA$ACTION = 'INSERT'.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create stream to track changes
CREATE OR REPLACE STREAM orders_stream ON TABLE raw.orders;

-- Step 2: Create task to process stream every 15 min
CREATE OR REPLACE TASK load_silver_orders
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    INSERT INTO silver.orders (order_id, customer_id, amount, order_date, _loaded_at)
    SELECT 
        order_id,
        customer_id,
        amount::DECIMAL(10,2),
        order_date::DATE,
        CURRENT_TIMESTAMP()
    FROM orders_stream
    WHERE METADATA$ACTION = 'INSERT';

-- Step 3: Resume the task (tasks are created suspended!)
ALTER TASK load_silver_orders RESUME;

-- Verify it's running:
SHOW TASKS LIKE 'load_silver_orders';
-- state should be 'started'
```

**Key Points:**
- Stream automatically tracks new rows (zero maintenance)
- `WHEN SYSTEM$STREAM_HAS_DATA` prevents empty runs (saves compute)
- After successful INSERT: stream offset advances (won't see these rows again)
- Task must be RESUMED after creation (starts in suspended state)
- If task fails: stream offset stays unchanged → next run retries same data

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Understanding Stream Metadata

**Scenario:** You query `orders_stream` and see rows with `METADATA$ACTION = 'DELETE'` and `METADATA$ISUPDATE = TRUE`. What does this mean? How is an UPDATE represented?

<details>
<summary>💡 Hint</summary>
An UPDATE is represented as a DELETE (old values) + INSERT (new values) pair. Both have METADATA$ISUPDATE = TRUE.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- UPDATEs are represented as TWO rows in the stream:

-- Row 1: The OLD values (what was there before)
-- METADATA$ACTION = 'DELETE', METADATA$ISUPDATE = TRUE
-- This is the "before image" of the update

-- Row 2: The NEW values (what it changed to)
-- METADATA$ACTION = 'INSERT', METADATA$ISUPDATE = TRUE
-- This is the "after image" of the update

-- Example: customer changed email from old@co.com to new@co.com
-- Stream shows:
-- | customer_id | email | METADATA$ACTION | METADATA$ISUPDATE |
-- | 1001 | old@co.com | DELETE | TRUE | ← old value
-- | 1001 | new@co.com | INSERT | TRUE | ← new value

-- For most ETL: you only want the NEW values:
SELECT * FROM customers_stream
WHERE METADATA$ACTION = 'INSERT';
-- This gives you: new inserts AND new values of updated rows

-- To find ONLY pure inserts (not updates):
SELECT * FROM customers_stream
WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE;

-- To find ONLY updates (before and after):
SELECT * FROM customers_stream
WHERE METADATA$ISUPDATE = TRUE;
```

**Key Points:**
- UPDATE = DELETE (old) + INSERT (new), both with ISUPDATE=TRUE
- INSERT (new row) = ACTION='INSERT', ISUPDATE=FALSE
- DELETE (remove row) = ACTION='DELETE', ISUPDATE=FALSE
- For upsert ETL: filter `WHERE METADATA$ACTION = 'INSERT'` (gets new + updated)
- For audit trail: keep both DELETE and INSERT rows (full change history)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: MERGE with Full CDC

**Scenario:** Your `customers_stream` captures INSERTs, UPDATEs, and DELETEs. Write a MERGE statement that applies all three operations to `silver.customers` (insert new, update existing, delete removed).

<details>
<summary>💡 Hint</summary>
Use a single MERGE with multiple WHEN clauses. Match on customer_id. Handle: INSERT (new row), UPDATE (matched + action=INSERT), DELETE (matched + action=DELETE + isupdate=FALSE).
</details>

<details>
<summary>✅ Solution</summary>

```sql
MERGE INTO silver.customers t
USING (
    SELECT 
        customer_id, name, email, region,
        METADATA$ACTION AS action,
        METADATA$ISUPDATE AS is_update
    FROM customers_stream
    -- Deduplicate: if same customer appears multiple times, take latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) = 1
) s ON t.customer_id = s.customer_id

-- Case 1: Delete (true delete, not part of update)
WHEN MATCHED AND s.action = 'DELETE' AND s.is_update = FALSE 
THEN DELETE

-- Case 2: Update (new version of existing customer)
WHEN MATCHED AND s.action = 'INSERT'
THEN UPDATE SET 
    t.name = s.name,
    t.email = s.email,
    t.region = s.region,
    t.updated_at = CURRENT_TIMESTAMP()

-- Case 3: Insert (brand new customer)
WHEN NOT MATCHED AND s.action = 'INSERT'
THEN INSERT (customer_id, name, email, region, created_at, updated_at)
VALUES (s.customer_id, s.name, s.email, s.region, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

**Key Points:**
- One MERGE handles all three CDC operations (elegant, atomic)
- Order of WHEN clauses matters: DELETE check before UPDATE (both are MATCHED)
- `action = 'DELETE' AND is_update = FALSE` = true delete (not part of an UPDATE pair)
- `action = 'INSERT'` for both new rows and updated rows (after image)
- QUALIFY deduplicates in case multiple changes for same customer in one batch
- The entire MERGE is one transaction: stream advances only if ALL operations succeed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Task DAG Design

**Scenario:** Design a task DAG for: (1) ingest orders (from stream), (2) ingest customers (from stream), (3) join orders+customers into enriched table (depends on both), (4) send notification (depends on join). Show the complete SQL.

<details>
<summary>💡 Hint</summary>
Root task has the schedule. Children use AFTER clause. The join task depends on BOTH ingest tasks (fan-in). Notification depends on join task. Resume from leaves to root.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Root task (scheduler — triggers the DAG)
CREATE OR REPLACE TASK pipeline_scheduler
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream') OR SYSTEM$STREAM_HAS_DATA('customers_stream')
AS
    SELECT 'pipeline_triggered' AS status;  -- No-op, just starts children

-- Child 1: Ingest orders (runs after scheduler)
CREATE OR REPLACE TASK ingest_orders
    WAREHOUSE = 'ETL_WH'
    AFTER pipeline_scheduler
AS
    INSERT INTO silver.orders
    SELECT order_id, customer_id, amount, order_date, CURRENT_TIMESTAMP()
    FROM orders_stream WHERE METADATA$ACTION = 'INSERT';

-- Child 2: Ingest customers (runs in parallel with orders)
CREATE OR REPLACE TASK ingest_customers
    WAREHOUSE = 'ETL_WH'
    AFTER pipeline_scheduler
AS
    MERGE INTO silver.customers t
    USING (SELECT * FROM customers_stream WHERE METADATA$ACTION = 'INSERT') s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email
    WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.name, s.email, s.region);

-- Child 3: Join (depends on BOTH ingest tasks — fan-in)
CREATE OR REPLACE TASK build_enriched_orders
    WAREHOUSE = 'ETL_WH'
    AFTER ingest_orders, ingest_customers  -- Waits for BOTH to complete!
AS
    INSERT OVERWRITE INTO gold.enriched_orders
    SELECT o.order_id, o.amount, o.order_date, c.name, c.region
    FROM silver.orders o
    JOIN silver.customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= CURRENT_DATE - 7;

-- Child 4: Notification (depends on join task)
CREATE OR REPLACE TASK send_notification
    WAREHOUSE = 'ETL_WH'
    AFTER build_enriched_orders
AS
    CALL system$send_email(
        'data-team@company.com',
        'Pipeline Complete',
        'Hourly enriched orders pipeline completed successfully.'
    );

-- RESUME all tasks (order: leaves first, root last)
ALTER TASK send_notification RESUME;
ALTER TASK build_enriched_orders RESUME;
ALTER TASK ingest_customers RESUME;
ALTER TASK ingest_orders RESUME;
ALTER TASK pipeline_scheduler RESUME;

-- DAG execution flow:
-- scheduler → (ingest_orders || ingest_customers) → build_enriched → notify
-- Total: ~5 minutes for the full pipeline
```

**Key Points:**
- Root task has SCHEDULE (only one task per DAG has a schedule)
- Children use `AFTER parent_task` (no schedule, triggered by parent completion)
- Fan-out: multiple children of same parent run in parallel
- Fan-in: `AFTER task_a, task_b` waits for BOTH to complete
- Resume order: leaves first → root last (reverse dependency order)
- If any task fails: downstream tasks don't run (fail-safe)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Pipeline with Error Recovery

**Scenario:** Design a production Streams + Tasks pipeline that: handles errors gracefully (logs, retries), prevents stale streams, monitors SLA compliance (must complete within 30 minutes of data arrival), and supports manual re-processing.

<details>
<summary>💡 Hint</summary>
Use stored procedures with TRY/CATCH for error handling. SUSPEND_TASK_AFTER_NUM_FAILURES for auto-suspend. Monitor via TASK_HISTORY. For re-processing: recreate stream (resets offset) + manual task trigger.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- STORED PROCEDURE with full error handling
CREATE OR REPLACE PROCEDURE etl.process_orders_production()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INTEGER;
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- Check if stream has data (defensive)
    IF (NOT SYSTEM$STREAM_HAS_DATA('orders_stream')) THEN
        RETURN 'SKIPPED: no data in stream';
    END IF;
    
    -- Process the stream (MERGE into silver)
    MERGE INTO silver.orders t
    USING (
        SELECT order_id, customer_id, amount, order_date,
               METADATA$ACTION, METADATA$ISUPDATE
        FROM orders_stream
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1
    ) s ON t.order_id = s.order_id
    WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' AND NOT s.METADATA$ISUPDATE THEN DELETE
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET
        t.amount = s.amount, t.order_date = s.order_date, t.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT
        (order_id, customer_id, amount, order_date, created_at, updated_at)
        VALUES (s.order_id, s.customer_id, s.amount, s.order_date, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    
    rows_processed := SQLROWCOUNT;
    
    -- Log success
    INSERT INTO etl.pipeline_log (pipeline_name, status, rows_processed, duration_sec, run_time)
    VALUES ('process_orders', 'SUCCESS', rows_processed,
            TIMESTAMPDIFF('second', start_time, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP());
    
    RETURN 'SUCCESS: ' || rows_processed || ' rows in ' || 
           TIMESTAMPDIFF('second', start_time, CURRENT_TIMESTAMP()) || 's';

EXCEPTION
    WHEN OTHER THEN
        -- Log failure (this INSERT runs even though main transaction rolls back)
        INSERT INTO etl.pipeline_log (pipeline_name, status, error_message, run_time)
        VALUES ('process_orders', 'FAILED', SQLERRM, CURRENT_TIMESTAMP());
        
        RAISE;  -- Re-raise to mark task as FAILED
END;
$$;

-- TASK with production settings
CREATE OR REPLACE TASK orders_production_task
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = 'USING CRON */15 * * * * UTC'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3  -- Auto-suspend after 3 failures
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    CALL etl.process_orders_production();

-- SLA MONITORING (separate alert task)
CREATE OR REPLACE TASK sla_monitor
    WAREHOUSE = 'ETL_WH'
    SCHEDULE = '5 MINUTE'
AS
BEGIN
    -- Check: is data older than 30 min without being processed?
    LET stale_minutes := (
        SELECT TIMESTAMPDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP())
        FROM raw.orders
        WHERE _loaded_at > DATEADD('hour', -1, CURRENT_TIMESTAMP())
    );
    
    LET last_silver_update := (
        SELECT TIMESTAMPDIFF('minute', MAX(updated_at), CURRENT_TIMESTAMP())
        FROM silver.orders
    );
    
    IF (last_silver_update > 30) THEN
        CALL system$send_email(
            'data-team@company.com',
            'SLA BREACH: Orders pipeline',
            'Silver orders table has not been updated in ' || last_silver_update || ' minutes.'
        );
    END IF;
END;

-- MANUAL RE-PROCESSING (when needed):
-- If stream became stale or you need to reprocess:
-- 1. Suspend task
ALTER TASK orders_production_task SUSPEND;
-- 2. Recreate stream (resets offset to current state)
CREATE OR REPLACE STREAM orders_stream ON TABLE raw.orders;
-- 3. Do a full load (one-time catch-up)
INSERT INTO silver.orders SELECT * FROM raw.orders WHERE order_date >= '2024-01-01';
-- 4. Resume task (stream now tracks from this point forward)
ALTER TASK orders_production_task RESUME;
```

**Key Points:**
- Stored procedure: TRY/CATCH logs errors, RAISE re-throws to mark task as failed
- SUSPEND_TASK_AFTER_NUM_FAILURES: auto-suspends to prevent infinite error loops
- Pipeline log: tracks success/failure, rows processed, duration (for SLA monitoring)
- SLA monitoring: separate task checking freshness every 5 minutes
- Manual re-processing: suspend → recreate stream → full load → resume
- Stream transactions: failed MERGE rolls back → stream unchanged → next run retries

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Snowflake Stream and what does it capture?**
A: A Stream is a change data capture (CDC) object that records row-level changes (INSERT, UPDATE, DELETE) made to a source table since the stream was last consumed. Querying the stream returns a delta of changes along with metadata columns: METADATA$ACTION, METADATA$ISUPDATE, and METADATA$ROW_ID.

**Q: What are the types of Snowflake Streams?**
A: Standard streams capture all DML changes (INSERT, UPDATE, DELETE). Append-only streams capture only INSERT operations—more efficient for insert-only sources like event or log tables. Insert-only streams are available for external tables. The choice affects what change data is available and processing efficiency.

**Q: What is a Snowflake Task and how is it triggered?**
A: A Task is a scheduled execution unit that runs a SQL statement or stored procedure call on a defined schedule (cron syntax) or when triggered by a predecessor task. Tasks enable lightweight orchestration of SQL-based pipelines entirely within Snowflake.

**Q: How do Streams and Tasks work together for CDC pipelines?**
A: A Task consumes a Stream's changes by running a query or procedure that reads from the stream and applies changes to a target table (MERGE or INSERT). Once consumed, the stream offset advances. The Task is typically scheduled on a regular interval (e.g., every minute) to process arriving changes incrementally.

**Q: What is stream offset staleness and why does it matter?**
A: Streams maintain a pointer to the last consumed position in the table's change history. Snowflake only retains change data for up to the Time Travel retention period (default 14 days). If a stream is not consumed within this window, it becomes stale and must be re-created, potentially missing changes. Monitoring consumption frequency is critical.

**Q: What is a Task tree and how do you create one?**
A: A Task tree is a DAG of tasks where each task can have a predecessor defined via `AFTER <predecessor_task>`. Child tasks trigger automatically when their parent succeeds. The root task is scheduled; downstream tasks are event-driven. This enables multi-step pipeline orchestration with dependencies.

**Q: What is the SYSTEM$STREAM_HAS_DATA function used for?**
A: `SYSTEM$STREAM_HAS_DATA('stream_name')` returns TRUE if the stream has unconsumed changes. It's used in Task definitions to make tasks conditional—a task only runs (and consumes compute) when there's actually new data in the stream, preventing unnecessary warehouse wake-ups.

**Q: What are the limitations of Streams and Tasks for complex pipelines?**
A: Tasks lack rich observability (no built-in lineage graph, limited failure alerting), don't support Python-based transformations natively, have limited error recovery mechanisms, and can be difficult to test. For complex multi-dependency pipelines with SLAs and rich monitoring, Airflow or Dagster is more appropriate.

---

## 💼 Interview Tips

- Always pair Streams with Tasks in your explanation—a Stream alone is just a change log; the real power is the Stream + Task combination that enables event-driven incremental processing.
- Bring up stream staleness proactively—it's an operational pitfall that catches teams off-guard in production, and mentioning it signals you've thought through failure modes.
- Use `SYSTEM$STREAM_HAS_DATA` as a concrete example of cost-conscious design: conditional task execution avoids paying for warehouse compute on empty runs.
- Compare Streams + Tasks to Dynamic Tables honestly. For simple single-hop transformations, Dynamic Tables are simpler. For complex CDC logic with MERGE statements and exception handling, Streams + Tasks offer more control.
- Senior interviewers may ask about ordering guarantees. Streams guarantee that all changes since the last consumption are captured, but processing order within a batch depends on your MERGE logic—understanding this prevents subtle bugs in out-of-order event scenarios.
