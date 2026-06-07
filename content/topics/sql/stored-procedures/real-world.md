---
title: "SQL Stored Procedures - Real-World Production Examples"
topic: sql
subtopic: stored-procedures
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, stored-procedures, etl, transactions, snowflake, production, batch-processing]
---

# SQL Stored Procedures — Real-World Production Examples

## Scenario 1: Nightly Incremental ETL with Idempotent Execution

**Business context:** A retail data warehouse loads the previous day's orders from a transactional database into a fact table every night. The job must be idempotent (safe to run multiple times for the same date), handle partial failures gracefully, and log every run for the ops team to monitor.

```sql
-- PostgreSQL implementation
CREATE OR REPLACE PROCEDURE load_daily_orders(p_load_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id       BIGINT;
    v_stage_count  INT := 0;
    v_load_count   INT := 0;
    v_dupe_count   INT := 0;
    v_start_time   TIMESTAMPTZ := clock_timestamp();
BEGIN
    -- Step 1: Log the run start
    INSERT INTO etl_run_log (procedure_name, load_date, status, started_at)
    VALUES ('load_daily_orders', p_load_date, 'running', v_start_time)
    RETURNING run_id INTO v_run_id;

    -- Step 2: Clear staging for idempotency (safe to re-run)
    DELETE FROM staging_orders WHERE stage_date = p_load_date;

    -- Step 3: Extract from source (foreign table or dblink)
    INSERT INTO staging_orders (order_id, customer_id, amount, status, order_date, stage_date)
    SELECT 
        order_id,
        customer_id,
        COALESCE(amount, 0),                -- Default nulls
        LOWER(TRIM(status)),                -- Normalize
        order_date,
        p_load_date
    FROM source_db.orders                   -- External table via postgres_fdw
    WHERE order_date = p_load_date
      AND order_id IS NOT NULL;             -- Basic quality filter

    GET DIAGNOSTICS v_stage_count = ROW_COUNT;

    -- Step 4: Deduplication (keep latest record per order)
    WITH deduped AS (
        SELECT DISTINCT ON (order_id) order_id, customer_id, amount, status, order_date
        FROM staging_orders
        WHERE stage_date = p_load_date
        ORDER BY order_id, stage_date DESC
    )
    INSERT INTO fact_orders (order_id, customer_id, amount, status, order_date, loaded_at)
    SELECT d.order_id, d.customer_id, d.amount, d.status, d.order_date, NOW()
    FROM deduped d
    ON CONFLICT (order_id) DO UPDATE SET
        amount     = EXCLUDED.amount,
        status     = EXCLUDED.status,
        loaded_at  = EXCLUDED.loaded_at
    WHERE fact_orders.status != EXCLUDED.status OR fact_orders.amount != EXCLUDED.amount;
    -- Only update if something actually changed (reduces write amplification)

    GET DIAGNOSTICS v_load_count = ROW_COUNT;
    v_dupe_count := v_stage_count - v_load_count;

    -- Step 5: Update run log with success metrics
    UPDATE etl_run_log SET
        status        = 'success',
        rows_staged   = v_stage_count,
        rows_loaded   = v_load_count,
        rows_dupes    = v_dupe_count,
        duration_ms   = EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time)),
        completed_at  = clock_timestamp()
    WHERE run_id = v_run_id;

    COMMIT;
    RAISE NOTICE 'load_daily_orders(%) complete: staged=%, loaded=%, dupes=%, run_id=%',
        p_load_date, v_stage_count, v_load_count, v_dupe_count, v_run_id;

EXCEPTION
    WHEN OTHERS THEN
        UPDATE etl_run_log SET
            status        = 'failed',
            error_message = SQLERRM,
            completed_at  = clock_timestamp()
        WHERE run_id = v_run_id;
        COMMIT;  -- Commit the failure log even though we're in an exception

        RAISE NOTICE 'load_daily_orders(%) FAILED: %', p_load_date, SQLERRM;
        RAISE;   -- Re-raise so Airflow marks the task as failed
END;
$$;

-- Called from Airflow PythonOperator via psycopg2:
-- conn.execute("CALL load_daily_orders(%s)", [execution_date])
```

**Why this pattern works in production:**
- `DELETE FROM staging WHERE stage_date = p_load_date` at the start makes the procedure idempotent — re-running for the same date produces the same result
- `ON CONFLICT DO UPDATE WHERE` only updates rows that actually changed, preventing unnecessary WAL writes
- The failure log is committed in a separate implicit transaction inside the EXCEPTION block, ensuring the failure is always recorded even when the main transaction rolls back
- The Airflow task can safely retry on failure because the procedure will re-stage from scratch

---

## Scenario 2: Customer Loyalty Tier Calculation with Snowflake JavaScript Procedure

**Business context:** A loyalty program calculates each customer's tier (Bronze/Silver/Gold/Platinum) at the end of each month based on their trailing 12-month spend. The calculation involves complex business rules (promo orders don't count, refunded orders reduce the total, there's a minimum order count requirement). This runs monthly via a Snowflake Task.

```sql
-- Snowflake JavaScript procedure for complex loyalty tier calculation
CREATE OR REPLACE PROCEDURE calculate_loyalty_tiers(run_month VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS $$
    var summary = { run_month: RUN_MONTH, processed: 0, errors: 0 };
    
    try {
        // Step 1: Compute trailing 12-month metrics per customer
        var metrics_result = snowflake.execute({ sqlText: `
            CREATE OR REPLACE TEMPORARY TABLE tmp_customer_metrics AS
            SELECT 
                o.customer_id,
                COUNT(CASE WHEN o.order_type != 'promotional' AND o.status = 'completed' THEN 1 END) 
                    AS qualifying_orders,
                SUM(CASE WHEN o.order_type != 'promotional' AND o.status = 'completed' 
                    THEN o.amount - COALESCE(r.refund_amount, 0) ELSE 0 END) 
                    AS net_spend
            FROM orders o
            LEFT JOIN refunds r ON o.order_id = r.order_id
            WHERE o.order_date >= DATEADD(month, -12, TO_DATE('${RUN_MONTH}', 'YYYY-MM'))
              AND o.order_date < TO_DATE('${RUN_MONTH}', 'YYYY-MM')
            GROUP BY o.customer_id
        `});
        
        // Step 2: Apply tier rules
        var tier_result = snowflake.execute({ sqlText: `
            CREATE OR REPLACE TEMPORARY TABLE tmp_tier_assignments AS
            SELECT 
                customer_id,
                net_spend,
                qualifying_orders,
                CASE 
                    WHEN qualifying_orders >= 4 AND net_spend >= 5000 THEN 'PLATINUM'
                    WHEN qualifying_orders >= 3 AND net_spend >= 2000 THEN 'GOLD'
                    WHEN qualifying_orders >= 2 AND net_spend >= 500  THEN 'SILVER'
                    WHEN qualifying_orders >= 1                        THEN 'BRONZE'
                    ELSE 'NONE'
                END AS new_tier
            FROM tmp_customer_metrics
        `});
        
        // Step 3: Merge into the loyalty table
        var merge_result = snowflake.execute({ sqlText: `
            MERGE INTO customer_loyalty_tiers t
            USING tmp_tier_assignments s ON t.customer_id = s.customer_id
            WHEN MATCHED AND t.current_tier != s.new_tier THEN
                UPDATE SET
                    previous_tier = t.current_tier,
                    current_tier  = s.new_tier,
                    effective_month = '${RUN_MONTH}',
                    updated_at    = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED AND s.new_tier != 'NONE' THEN
                INSERT (customer_id, current_tier, previous_tier, effective_month, created_at, updated_at)
                VALUES (s.customer_id, s.new_tier, NULL, '${RUN_MONTH}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        `});
        
        summary.processed = merge_result.getRowCount();
        
        // Step 4: Log the run
        snowflake.execute({ sqlText: `
            INSERT INTO loyalty_calc_log (run_month, customers_processed, status, completed_at)
            VALUES ('${RUN_MONTH}', ${summary.processed}, 'SUCCESS', CURRENT_TIMESTAMP())
        `});
        
    } catch(err) {
        summary.errors = 1;
        summary.error_message = err.message;
        
        snowflake.execute({ sqlText: `
            INSERT INTO loyalty_calc_log (run_month, customers_processed, status, error_message, completed_at)
            VALUES ('${RUN_MONTH}', 0, 'FAILED', '${err.message.replace(/'/g, "''")}', CURRENT_TIMESTAMP())
        `});
        
        throw err;  // Re-throw so Snowflake Task marks the run as failed
    }
    
    return summary;
$$;

-- Scheduled via Snowflake Task (runs on the 1st of each month at 2am UTC):
CREATE OR REPLACE TASK calculate_monthly_tiers
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 1 * * UTC'
AS
    CALL calculate_loyalty_tiers(TO_CHAR(DATEADD(month, -1, CURRENT_DATE()), 'YYYY-MM'));
```

**Production considerations:**
- The JavaScript procedure uses temporary tables for intermediate results — these auto-clean after the session ends
- `EXECUTE AS CALLER` ensures the procedure runs with the caller's permissions, allowing role-based access control
- The error handler captures the exception message but must sanitize quotes (`replace(/'/g, "''")`) before embedding in SQL to prevent injection in the logging INSERT
- The Snowflake Task's `USING CRON` schedule runs on the 1st of each month, automatically calculating the previous month's tiers

---

## Scenario 3: PostgreSQL Procedure for Multi-Step Financial Reconciliation

**Business context:** Each morning, the finance team needs to reconcile payments: match bank statement entries to internal payment records, flag unmatched items for investigation, and generate the daily reconciliation report. This requires multiple JOINs, updates to several tables, and must be transactionally consistent (all-or-nothing).

```sql
CREATE OR REPLACE PROCEDURE reconcile_daily_payments(p_bank_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_matched_count   INT := 0;
    v_unmatched_count INT := 0;
    v_over_count      INT := 0;
    v_reconcile_id    INT;
BEGIN
    -- Step 1: Create reconciliation run record
    INSERT INTO reconciliation_runs (bank_date, status, initiated_at)
    VALUES (p_bank_date, 'in_progress', NOW())
    RETURNING reconcile_id INTO v_reconcile_id;

    -- Step 2: Match bank entries to internal payments (exact amount + reference match)
    UPDATE bank_statement_entries bse
    SET 
        matched_payment_id = p.payment_id,
        match_status       = 'matched',
        reconcile_id       = v_reconcile_id
    FROM payments p
    WHERE bse.bank_date = p_bank_date
      AND bse.amount = p.amount
      AND bse.reference_number = p.external_reference
      AND p.status = 'pending_reconciliation'
      AND bse.match_status = 'unmatched';

    GET DIAGNOSTICS v_matched_count = ROW_COUNT;

    -- Step 3: Mark matched payments as reconciled
    UPDATE payments
    SET status = 'reconciled', reconciled_at = NOW(), reconcile_id = v_reconcile_id
    WHERE payment_id IN (
        SELECT matched_payment_id FROM bank_statement_entries
        WHERE reconcile_id = v_reconcile_id AND match_status = 'matched'
    );

    -- Step 4: Flag bank entries that have no matching payment (unusual deposits, errors)
    UPDATE bank_statement_entries SET
        match_status = 'unmatched_bank_entry',
        reconcile_id = v_reconcile_id
    WHERE bank_date = p_bank_date AND match_status = 'unmatched';

    GET DIAGNOSTICS v_unmatched_count = ROW_COUNT;

    -- Step 5: Flag internal payments with no corresponding bank entry (failed/delayed)
    UPDATE payments SET
        status = 'missing_bank_entry',
        reconcile_id = v_reconcile_id
    WHERE status = 'pending_reconciliation'
      AND payment_date = p_bank_date;

    GET DIAGNOSTICS v_over_count = ROW_COUNT;

    -- Step 6: Update run record with final counts
    UPDATE reconciliation_runs SET
        status             = 'completed',
        matched_count      = v_matched_count,
        unmatched_bank     = v_unmatched_count,
        missing_bank_entry = v_over_count,
        completed_at       = NOW()
    WHERE reconcile_id = v_reconcile_id;

    COMMIT;

    RAISE NOTICE 'Reconciliation for % complete: matched=%, unmatched_bank=%, missing=%',
        p_bank_date, v_matched_count, v_unmatched_count, v_over_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Roll back all changes for this reconciliation run
        ROLLBACK;
        
        -- Log the failure in a separate transaction
        BEGIN
            INSERT INTO reconciliation_errors (bank_date, error_msg, failed_at)
            VALUES (p_bank_date, SQLERRM, NOW());
            COMMIT;
        END;
        
        RAISE EXCEPTION 'Reconciliation failed for %: %', p_bank_date, SQLERRM;
END;
$$;
```

**Why a stored procedure here and not application code:**
- The reconciliation is transactionally coupled — if the bank entry update succeeds but the payment update fails, the data is inconsistent. The single transaction in the procedure guarantees atomicity.
- All five UPDATE statements execute in the database — no round-trips between the application and the database for each step
- The finance team can run the procedure manually (`CALL reconcile_daily_payments('2024-01-15')`) for re-processing without deploying new code
- The EXCEPTION block's nested `BEGIN/COMMIT` ensures the error log is written even when the main transaction rolls back

---

## Interview Tips

> **Tip 1:** "How do you make a stored procedure idempotent?" — "The key is ensuring the procedure produces the same result whether run once or ten times for the same inputs. Common techniques: DELETE staging data before re-inserting (clean slate), use ON CONFLICT DO UPDATE (upsert) instead of INSERT, check existence before inserting with IF NOT EXISTS, and use run-state tracking (insert a 'running' record at the start, update to 'success' at the end) so you can detect and safely re-run failed runs."

> **Tip 2:** "Walk me through your approach to writing a complex multi-step ETL procedure." — "I structure it as: (1) log the run start with a status of 'running', (2) clear/stage incoming data, (3) apply transformation and quality rules, (4) load into target with upsert semantics, (5) update run log with success metrics and COMMIT. I wrap everything in a TRY/EXCEPTION block. Failures update the log with 'failed' and the error message. This gives the ops team full visibility and makes the pipeline self-auditing."

> **Tip 3:** "How do stored procedures in Snowflake differ from PostgreSQL?" — "Snowflake procedures support multiple languages (JavaScript, Python, SQL scripting) and run inside the Snowflake compute engine — there's no pre-compilation benefit like SQL Server. Snowflake procedures can call SQL but don't share transaction scope with the caller by default. The main use case is imperative logic that can't be expressed in a single SQL statement — dynamic table names, conditional branching, loops. For pure transformations, Snowflake Tasks calling SQL directly (or dbt models) are preferred."
