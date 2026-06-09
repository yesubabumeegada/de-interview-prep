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

**Current slow code:**

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