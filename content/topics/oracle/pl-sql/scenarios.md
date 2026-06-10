---
title: "PL/SQL — Scenarios"
topic: oracle
subtopic: pl-sql
content_type: scenario_question
tags: [oracle, pl-sql, interview, scenarios, etl, debugging]
---

# PL/SQL — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Write a Procedure to Archive Old Orders

**Scenario:** Write a PL/SQL procedure that moves orders older than 2 years from the `orders` table to the `orders_archive` table, then deletes them from the source. It should process in batches of 10,000 and log the result.

<details>
<summary>💡 Hint</summary>

**Key points the interviewer looks for:** - BULK COLLECT + FORALL (not row-by-row) - LIMIT on BULK COLLECT (not loading millions of IDs into memory) - COMMIT per batch (not one giant transaction) - Exception handling with cursor cleanup - Parameterized cutoff date (not hardcoded)

</details>

<details>
<summary>✅ Solution</summary>

```plsql
CREATE OR REPLACE PROCEDURE archive_old_orders(
  p_cutoff_date IN DATE DEFAULT ADD_MONTHS(TRUNC(SYSDATE), -24),
  p_batch_size  IN PLS_INTEGER DEFAULT 10000
) IS
  TYPE t_order_ids IS TABLE OF orders.order_id%TYPE;
  v_ids       t_order_ids;
  v_total     NUMBER := 0;
  
  CURSOR c_old_orders IS
    SELECT order_id FROM orders
    WHERE order_date < p_cutoff_date
    ORDER BY order_date;
BEGIN
  DBMS_OUTPUT.PUT_LINE('Archiving orders before: ' || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD'));
  
  OPEN c_old_orders;
  LOOP
    FETCH c_old_orders BULK COLLECT INTO v_ids LIMIT p_batch_size;
    EXIT WHEN v_ids.COUNT = 0;
    
    -- Archive the batch
    FORALL i IN 1..v_ids.COUNT
      INSERT INTO orders_archive
      SELECT o.*, SYSDATE AS archived_date
      FROM orders o
      WHERE o.order_id = v_ids(i);
    
    -- Delete from source
    FORALL i IN 1..v_ids.COUNT
      DELETE FROM orders WHERE order_id = v_ids(i);
    
    v_total := v_total + v_ids.COUNT;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Archived batch of ' || v_ids.COUNT || ' (total: ' || v_total || ')');
  END LOOP;
  CLOSE c_old_orders;
  
  -- Log the run
  INSERT INTO archive_log(table_name, rows_archived, archived_before, run_time)
  VALUES ('ORDERS', v_total, p_cutoff_date, SYSDATE);
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Archive complete: ' || v_total || ' orders archived');
EXCEPTION
  WHEN OTHERS THEN
    IF c_old_orders%ISOPEN THEN CLOSE c_old_orders; END IF;
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Archive failed: ' || SQLERRM);
    RAISE;
END archive_old_orders;
/
```

**Key points the interviewer looks for:**
- BULK COLLECT + FORALL (not row-by-row)
- LIMIT on BULK COLLECT (not loading millions of IDs into memory)
- COMMIT per batch (not one giant transaction)
- Exception handling with cursor cleanup
- Parameterized cutoff date (not hardcoded)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Trigger Is Causing ORA-04091 Mutation Error

**Scenario:** You deployed a trigger on `order_items` that should update the parent `orders.total_amount` whenever a line item is inserted or updated. It throws `ORA-04091: table ORDERS is mutating`. How do you fix it?

<details>
<summary>💡 Hint</summary>

**Root cause:** The row-level trigger tries to query `order_items` (the table being modified) to calculate the total. This causes the mutation error.

</details>

<details>
<summary>✅ Solution</summary>

**Root cause:** The row-level trigger tries to query `order_items` (the table being modified) to calculate the total. This causes the mutation error.

**Wrong approach (causes ORA-04091):**
```plsql
-- BAD: queries order_items from within a row-level trigger on order_items
CREATE OR REPLACE TRIGGER trg_update_order_total
AFTER INSERT OR UPDATE ON order_items
FOR EACH ROW
BEGIN
  UPDATE orders
  SET total_amount = (
    SELECT SUM(quantity * unit_price) FROM order_items  -- ORA-04091!
    WHERE order_id = :NEW.order_id
  )
  WHERE order_id = :NEW.order_id;
END;
/
```

**Fix — Compound Trigger:**
```plsql
CREATE OR REPLACE TRIGGER trg_order_items_compound
FOR INSERT OR UPDATE OR DELETE ON order_items
COMPOUND TRIGGER
  
  TYPE t_order_set IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  v_changed_orders t_order_set;
  v_idx PLS_INTEGER := 0;
  
  AFTER EACH ROW IS
  BEGIN
    -- Just collect the order_id (no querying order_items here)
    v_idx := v_idx + 1;
    v_changed_orders(v_idx) := COALESCE(:NEW.order_id, :OLD.order_id);
  END AFTER EACH ROW;
  
  AFTER STATEMENT IS
    -- Now order_items mutations are complete — safe to query
    TYPE t_distinct_orders IS TABLE OF NUMBER;
    v_orders t_distinct_orders;
  BEGIN
    -- Deduplicate order IDs
    SELECT DISTINCT COLUMN_VALUE
    BULK COLLECT INTO v_orders
    FROM TABLE(CAST(MULTISET(
      SELECT v_changed_orders(i) FROM (SELECT LEVEL i FROM DUAL CONNECT BY LEVEL <= v_idx)
    ) AS sys.OdciNumberList));
    
    FORALL i IN 1..v_orders.COUNT
      UPDATE orders
      SET total_amount = (
        SELECT SUM(quantity * unit_price) FROM order_items
        WHERE order_id = v_orders(i)
      )
      WHERE order_id = v_orders(i);
  END AFTER STATEMENT;
  
END trg_order_items_compound;
/
```

**Alternative simpler approach (if trigger logic is not critical-path):**
```plsql
-- Move the recalculation to the application/procedure layer instead of triggers
-- Application code explicitly calls recalculate_order_total() after modifying order_items
PROCEDURE recalculate_order_total(p_order_id IN NUMBER) IS
BEGIN
  UPDATE orders
  SET total_amount = (
    SELECT SUM(quantity * unit_price) FROM order_items WHERE order_id = p_order_id
  )
  WHERE order_id = p_order_id;
END;
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: ETL Procedure Runs 4 Hours — Optimize It

**Scenario:** A nightly ETL procedure takes 4 hours. It processes 5 million rows from a staging table into a fact table using a cursor FOR loop with individual INSERT statements. How do you optimize it to run in under 30 minutes?

<details>
<summary>💡 Hint</summary>

The bottleneck is *row-by-row* processing — a cursor FOR LOOP with individual INSERTs does one context switch per row (5M switches for 5M rows). The fix is to replace it with set-based SQL: `INSERT INTO ... SELECT ...` for the inserts, and `UPDATE ... WHERE ... IN (...)` for the status update. If you must use PL/SQL for business logic, use `BULK COLLECT ... FORALL` to process thousands of rows per round-trip instead of one. Also disable redo logging with `APPEND` hint and `NOLOGGING` for the initial load if recoverability allows.

</details>

<details>
<summary>✅ Solution</summary>

**Current slow code:**
```plsql
-- Current: row-by-row with cursor FOR LOOP (very slow)
BEGIN
  FOR rec IN (SELECT * FROM sales_staging WHERE status = 'PENDING') LOOP
    INSERT INTO sales_fact VALUES (
      sales_seq.NEXTVAL, rec.order_id, rec.product_id, rec.amount_usd, SYSDATE
    );
    UPDATE sales_staging SET status = 'DONE' WHERE staging_id = rec.staging_id;
  END LOOP;
  COMMIT;
END;
```

**Step 1: Profile where time is spent**
```sql
-- Check if it's I/O, CPU, or wait events
SELECT event, COUNT(*) FROM v$active_session_history
WHERE sample_time > SYSDATE - 1/6  -- last 4 hours
  AND program LIKE '%ETL%'
GROUP BY event ORDER BY 2 DESC;
```

**Step 2: Replace with set-based + BULK operations**
```plsql
CREATE OR REPLACE PROCEDURE etl_load_sales_fast IS
  TYPE t_stage_tab IS TABLE OF sales_staging%ROWTYPE;
  v_batch t_stage_tab;
  CURSOR c_pending IS SELECT * FROM sales_staging WHERE status = 'PENDING';
  v_total NUMBER := 0;
BEGIN
  OPEN c_pending;
  LOOP
    FETCH c_pending BULK COLLECT INTO v_batch LIMIT 10000;
    EXIT WHEN v_batch.COUNT = 0;
    
    FORALL i IN 1..v_batch.COUNT
      INSERT INTO sales_fact(
        fact_id, order_id, product_id, amount_usd, load_date
      ) VALUES (
        sales_seq.NEXTVAL,
        v_batch(i).order_id,
        v_batch(i).product_id,
        v_batch(i).amount_usd,
        SYSDATE
      );
    
    FORALL i IN 1..v_batch.COUNT
      UPDATE sales_staging SET status = 'DONE'
      WHERE staging_id = v_batch(i).staging_id;
    
    v_total := v_total + v_batch.COUNT;
    COMMIT;
  END LOOP;
  CLOSE c_pending;
  DBMS_OUTPUT.PUT_LINE('Loaded: ' || v_total);
END etl_load_sales_fast;
/
```

**Step 3: Consider pure SQL approach (fastest)**
```sql
-- If no row-level transformation needed: skip PL/SQL entirely
INSERT /*+ APPEND PARALLEL(8) */ INTO sales_fact(fact_id, order_id, product_id, amount_usd, load_date)
SELECT sales_seq.NEXTVAL, order_id, product_id, amount_usd, SYSDATE
FROM sales_staging WHERE status = 'PENDING';

UPDATE /*+ PARALLEL(8) */ sales_staging SET status = 'DONE' WHERE status = 'PENDING';
COMMIT;
```

**Step 4: Add parallel hints for the INSERT**
```plsql
-- In PL/SQL, enable parallel DML for direct path inserts
EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
-- Use APPEND hint for direct path write (bypasses buffer cache)
EXECUTE IMMEDIATE '
  INSERT /*+ APPEND PARALLEL(sales_fact, 8) */ INTO sales_fact
  SELECT sales_seq.NEXTVAL, order_id, product_id, amount_usd, SYSDATE
  FROM sales_staging WHERE status = ''PENDING''
';
COMMIT;
```

**Expected improvements:**
- Row-by-row → BULK COLLECT/FORALL: **4h → ~45 min** (80% reduction)
- Add APPEND + PARALLEL: **45 min → ~15 min** (additional 65% reduction)
- Pure set-based INSERT with PARALLEL: **4h → ~10 min** (if no row logic needed)

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a PL/SQL procedure and a function?**
A: A procedure performs an action and does not return a value directly (it can use OUT parameters). A function must return a value and can be called within SQL expressions. Functions used in SQL must be deterministic and should not have side effects to be optimizer-safe.

**Q: What is a PL/SQL package and why is it preferred over standalone procedures?**
A: A package groups related procedures, functions, types, and variables into a single unit with a specification (public interface) and body (implementation). Benefits: encapsulation, ability to have private subprograms, package-level (session-lifetime) variables, and reduced inter-object dependency—changing the body does not invalidate callers if the specification is unchanged.

**Q: What is the BULK COLLECT / FORALL pattern and when should you use it?**
A: BULK COLLECT fetches multiple rows into a collection with a single context switch instead of one row at a time. FORALL executes a DML statement for all collection elements in a single batch. This pattern reduces the PL/SQL-to-SQL engine context-switch overhead from N to 1, providing 10x–100x speedup for large set operations.

**Q: What is an autonomous transaction in PL/SQL?**
A: An autonomous transaction (`PRAGMA AUTONOMOUS_TRANSACTION`) is an independent transaction within a procedure that commits or rolls back independently of the calling transaction. It is used for logging/auditing (write a log record even if the main transaction rolls back) but must be used carefully to avoid orphaned data.

**Q: What is the difference between implicit and explicit cursors?**
A: An implicit cursor is created automatically by Oracle for every SQL DML/SELECT statement. An explicit cursor is declared by the developer for fine-grained row-by-row processing. Explicit cursors give control over OPEN/FETCH/CLOSE, support parameterization, and allow FOR UPDATE locking. For most cases, a cursor FOR loop is preferred—it auto-opens, fetches, and closes.

**Q: How do you handle exceptions in PL/SQL and what is RAISE_APPLICATION_ERROR?**
A: PL/SQL has named exceptions (e.g., `NO_DATA_FOUND`, `TOO_MANY_ROWS`) and user-defined exceptions. The EXCEPTION block catches them. `RAISE_APPLICATION_ERROR(-20001, 'message')` raises a custom error with an application-specific error number (between -20000 and -20999) that propagates to the calling application with a meaningful message.

**Q: What is the PL/SQL %ROWTYPE and %TYPE and why use them?**
A: `%TYPE` anchors a variable's datatype to a table column (`employee_id employees.employee_id%TYPE`). `%ROWTYPE` anchors a record variable to an entire table/cursor row. Both eliminate hard-coded type declarations, ensuring PL/SQL code automatically adapts when the underlying column type changes—a key maintainability practice.

**Q: What is DBMS_SCHEDULER and how does it differ from DBMS_JOB?**
A: DBMS_SCHEDULER is the modern Oracle job scheduler supporting cron-style schedules, named programs/schedules, job classes, windows, and chains. DBMS_JOB is the legacy, deprecated scheduler with a simpler interface but no calendar/dependency support. New code should always use DBMS_SCHEDULER.

---

## 💼 Interview Tips

- When discussing performance, always mention BULK COLLECT + FORALL as the go-to optimization for row-by-row PL/SQL that processes large sets—it is the single highest-impact PL/SQL performance technique.
- Know the context-switch concept: PL/SQL and SQL are separate engines; every SQL call from PL/SQL is a context switch. Bulk operations minimize switches. Interviewers appreciate this low-level explanation.
- For packages, explain the spec-vs-body separation: a recompiled body does not invalidate dependent objects if the spec is unchanged, enabling hot-patch deployments.
- Senior interviewers probe autonomous transactions for gotchas: they do NOT see the parent transaction's uncommitted data, and forgetting to commit inside the autonomous block causes a hang. Walk through the mechanism.
- Demonstrate coding discipline: mention using `%TYPE`/`%ROWTYPE`, explicit exception handling with `SQLERRM`, structured logging via an autonomous-transaction log procedure, and avoiding `SELECT *` in production PL/SQL.
