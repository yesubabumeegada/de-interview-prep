---
title: "PL/SQL — Real World"
topic: oracle
subtopic: pl-sql
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, pl-sql, etl, scheduling, error-handling, production]
---

# PL/SQL — Real World Patterns

## Pattern 1: Nightly ETL Batch Process

```plsql
CREATE OR REPLACE PROCEDURE etl_load_daily_sales(
  p_run_date IN DATE DEFAULT TRUNC(SYSDATE - 1)
) IS
  v_rows_loaded   NUMBER := 0;
  v_rows_rejected NUMBER := 0;
  v_start_time    TIMESTAMP := SYSTIMESTAMP;
  v_run_id        NUMBER;
  
  -- Batch configuration
  c_batch_size    CONSTANT PLS_INTEGER := 5000;
  
  TYPE t_sales_tab IS TABLE OF sales_staging%ROWTYPE;
  v_batch t_sales_tab;
  
  CURSOR c_staged IS
    SELECT * FROM sales_staging
    WHERE load_date = p_run_date
      AND status = 'PENDING'
    ORDER BY staging_id;
  
BEGIN
  -- Create run log entry
  INSERT INTO etl_run_log(run_id, proc_name, run_date, status, start_time)
  VALUES (etl_run_seq.NEXTVAL, 'etl_load_daily_sales', p_run_date, 'RUNNING', v_start_time)
  RETURNING run_id INTO v_run_id;
  COMMIT;
  
  OPEN c_staged;
  LOOP
    FETCH c_staged BULK COLLECT INTO v_batch LIMIT c_batch_size;
    EXIT WHEN v_batch.COUNT = 0;
    
    BEGIN
      -- Insert valid records
      FORALL i IN 1..v_batch.COUNT SAVE EXCEPTIONS
        INSERT INTO sales_fact(
          sale_id, order_id, product_id, customer_id,
          sale_date, amount_usd, quantity, load_run_id
        ) VALUES (
          sales_fact_seq.NEXTVAL,
          v_batch(i).order_id,
          v_batch(i).product_id,
          v_batch(i).customer_id,
          v_batch(i).sale_date,
          v_batch(i).amount_usd,
          v_batch(i).quantity,
          v_run_id
        );
      
      v_rows_loaded := v_rows_loaded + SQL%ROWCOUNT;
      
    EXCEPTION
      WHEN OTHERS THEN
        -- Handle individual row errors (SAVE EXCEPTIONS)
        FOR j IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
          v_rows_rejected := v_rows_rejected + 1;
          
          -- Log rejected row
          INSERT INTO etl_error_log(run_id, staging_id, error_code, error_msg, log_time)
          VALUES (
            v_run_id,
            v_batch(SQL%BULK_EXCEPTIONS(j).error_index).staging_id,
            SQL%BULK_EXCEPTIONS(j).error_code,
            SQLERRM(-SQL%BULK_EXCEPTIONS(j).error_code),
            SYSTIMESTAMP
          );
        END LOOP;
        
        -- Count successful inserts in this batch
        v_rows_loaded := v_rows_loaded + (v_batch.COUNT - SQL%BULK_EXCEPTIONS.COUNT);
    END;
    
    -- Update staging status
    FORALL i IN 1..v_batch.COUNT
      UPDATE sales_staging
      SET status = 'LOADED', processed_time = SYSTIMESTAMP
      WHERE staging_id = v_batch(i).staging_id;
    
    COMMIT;  -- commit each batch
  END LOOP;
  CLOSE c_staged;
  
  -- Update run log with final status
  UPDATE etl_run_log
  SET status = 'COMPLETE',
      rows_loaded = v_rows_loaded,
      rows_rejected = v_rows_rejected,
      end_time = SYSTIMESTAMP,
      duration_sec = ROUND((SYSTIMESTAMP - v_start_time) * 86400)
  WHERE run_id = v_run_id;
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('ETL Complete: loaded=' || v_rows_loaded || 
                       ', rejected=' || v_rows_rejected ||
                       ', run_id=' || v_run_id);
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    UPDATE etl_run_log
    SET status = 'FAILED', error_msg = SQLERRM, end_time = SYSTIMESTAMP
    WHERE run_id = v_run_id;
    COMMIT;
    RAISE;
END etl_load_daily_sales;
/
```

---

## Pattern 2: Idempotent Data Load (Re-runnable ETL)

```plsql
CREATE OR REPLACE PROCEDURE load_product_dimension(
  p_run_date IN DATE DEFAULT TRUNC(SYSDATE)
) IS
  v_merged   NUMBER := 0;
  v_inserted NUMBER := 0;
  v_updated  NUMBER := 0;
BEGIN
  -- Delete any previous run's data for this date (idempotent)
  DELETE FROM dim_product_load_log WHERE run_date = p_run_date;
  
  -- MERGE: insert new, update existing
  MERGE INTO dim_products tgt
  USING (
    SELECT p.product_id, p.product_name, p.category, 
           p.unit_cost, p.list_price, p.supplier_id,
           SYSDATE AS effective_date
    FROM products_source p
    WHERE p.last_modified >= p_run_date - 1  -- get changes since yesterday
  ) src ON (tgt.product_id = src.product_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.product_name    = src.product_name,
      tgt.category        = src.category,
      tgt.unit_cost       = src.unit_cost,
      tgt.list_price      = src.list_price,
      tgt.supplier_id     = src.supplier_id,
      tgt.last_updated    = SYSDATE
    WHERE tgt.product_name    != src.product_name
       OR tgt.category        != src.category
       OR tgt.unit_cost       != src.unit_cost
       OR tgt.list_price      != src.list_price
  WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, unit_cost, list_price, supplier_id, created_date)
    VALUES (src.product_id, src.product_name, src.category, src.unit_cost, src.list_price, src.supplier_id, SYSDATE);
  
  v_merged := SQL%ROWCOUNT;
  
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Product dimension loaded: ' || v_merged || ' rows merged');
END load_product_dimension;
/
```

---

## Pattern 3: Scheduled Job Using DBMS_SCHEDULER

```plsql
-- Create a repeating nightly ETL job
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'NIGHTLY_SALES_ETL',
    job_type        => 'STORED_PROCEDURE',
    job_action      => 'APP_SCHEMA.ETL_LOAD_DAILY_SALES',
    repeat_interval => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=0',  -- 2:00 AM daily
    start_date      => SYSTIMESTAMP,
    enabled         => TRUE,
    auto_drop       => FALSE,
    comments        => 'Nightly load of daily sales from staging to fact table'
  );
END;
/

-- Monitor job history
SELECT job_name, status, actual_start_date, run_duration, error#, additional_info
FROM dba_scheduler_job_run_details
WHERE job_name = 'NIGHTLY_SALES_ETL'
ORDER BY actual_start_date DESC
FETCH FIRST 10 ROWS ONLY;

-- Manually run the job (for testing)
BEGIN
  DBMS_SCHEDULER.RUN_JOB('NIGHTLY_SALES_ETL');
END;
/

-- Disable/Enable
BEGIN
  DBMS_SCHEDULER.DISABLE('NIGHTLY_SALES_ETL');
  DBMS_SCHEDULER.ENABLE('NIGHTLY_SALES_ETL');
END;
/
```

---

## Common PL/SQL Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Row-by-row LOOP with DML | Context switch per row → slow | BULK COLLECT + FORALL |
| Cursor FOR LOOP for millions of rows | All rows in memory | BULK COLLECT with LIMIT |
| Hardcoded literals instead of %TYPE | Breaks when schema changes | Use `column%TYPE` |
| Empty EXCEPTION WHEN OTHERS THEN NULL | Silently swallows errors | Always log + RAISE |
| COMMIT inside a loop (too frequent) | Undo/redo overhead | Commit every N rows |
| SELECT INTO without exception handler | Unhandled NO_DATA_FOUND | Add EXCEPTION block |
| Dynamic SQL with string concatenation | SQL injection risk | Use bind variables + DBMS_ASSERT |
| DDL in transactions | Auto-COMMIT before and after DDL | Separate DDL into autonomous transactions |

---

## Interview Tips

> **Tip 1:** "How do you make an ETL procedure re-runnable (idempotent)?" — Use MERGE (UPSERT) instead of separate INSERT/UPDATE. Delete then re-insert for aggregate tables. Use a run-date parameter and delete previous results for that date before re-processing. Log each run with a unique run_id so partial runs can be identified and re-run. Never rely on "the data isn't there yet" as an idempotency guarantee.

> **Tip 2:** "How do you handle errors in a FORALL batch without failing the entire batch?" — Use `FORALL ... SAVE EXCEPTIONS`. After the FORALL, catch the `-24381` exception and loop through `SQL%BULK_EXCEPTIONS` to log individual failures. Use an autonomous transaction for error logging so the log entry persists if the main transaction rolls back.

> **Tip 3:** "How would you schedule a PL/SQL job to run nightly?" — Use `DBMS_SCHEDULER.CREATE_JOB` with `FREQ=DAILY; BYHOUR=2` as the repeat interval. Set `job_type=STORED_PROCEDURE` and `job_action` to the full schema-qualified procedure name. Monitor with `dba_scheduler_job_run_details` — check `status` and `error#` fields. DBMS_SCHEDULER is preferred over the older DBMS_JOB package (deprecated).
