---
title: "PL/SQL — Senior Deep Dive"
topic: oracle
subtopic: pl-sql
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, pl-sql, pipelined-functions, compound-triggers, advanced-packages, performance]
---

# PL/SQL — Senior Deep Dive

## Pipelined Table Functions

Pipelined functions return rows one at a time as they're produced — callers can start processing before the function finishes. Great for ETL transformations used in SQL.

```plsql
-- Define the return type
CREATE OR REPLACE TYPE t_order_row AS OBJECT (
  order_id     NUMBER,
  customer_id  NUMBER,
  amount_usd   NUMBER,
  tax_amount   NUMBER,
  total_amount NUMBER,
  region       VARCHAR2(50)
);

CREATE OR REPLACE TYPE t_order_tab AS TABLE OF t_order_row;

-- Pipelined function
CREATE OR REPLACE FUNCTION transform_orders(
  p_region IN VARCHAR2 DEFAULT NULL
) RETURN t_order_tab PIPELINED IS
  
  CURSOR c_orders IS
    SELECT o.order_id, o.customer_id, o.amount_usd, c.region
    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
    WHERE (p_region IS NULL OR c.region = p_region);
  
  TAX_RATE CONSTANT NUMBER := 0.08;
BEGIN
  FOR rec IN c_orders LOOP
    -- Transform each row and PIPE it immediately (no buffer)
    PIPE ROW(t_order_row(
      rec.order_id,
      rec.customer_id,
      rec.amount_usd,
      ROUND(rec.amount_usd * TAX_RATE, 2),
      ROUND(rec.amount_usd * (1 + TAX_RATE), 2),
      rec.region
    ));
  END LOOP;
  RETURN;  -- required for pipelined functions
END transform_orders;
/

-- Use in SQL like a table
SELECT * FROM TABLE(transform_orders('WEST'));
SELECT region, SUM(total_amount) FROM TABLE(transform_orders()) GROUP BY region;

-- Even works in CTAS
CREATE TABLE orders_transformed AS
SELECT * FROM TABLE(transform_orders());
```

---

## Compound Triggers

Compound triggers combine multiple timing points (BEFORE STATEMENT, BEFORE EACH ROW, AFTER EACH ROW, AFTER STATEMENT) in one trigger body, sharing state between them:

```plsql
-- Problem solved: avoiding mutation table errors and bulking trigger logic
CREATE OR REPLACE TRIGGER trg_orders_compound
FOR INSERT OR UPDATE ON order_items
COMPOUND TRIGGER
  
  -- Package-level collection (shared across all timing points)
  TYPE t_order_ids IS TABLE OF order_items.order_id%TYPE INDEX BY PLS_INTEGER;
  v_order_ids t_order_ids;
  v_idx       PLS_INTEGER := 0;
  
  -- BEFORE STATEMENT: initialize collection
  BEFORE STATEMENT IS
  BEGIN
    v_order_ids.DELETE;
    v_idx := 0;
  END BEFORE STATEMENT;
  
  -- AFTER EACH ROW: accumulate affected order_ids (no DML on affected table)
  AFTER EACH ROW IS
  BEGIN
    v_idx := v_idx + 1;
    v_order_ids(v_idx) := :NEW.order_id;
  END AFTER EACH ROW;
  
  -- AFTER STATEMENT: do the actual work on the parent table (safe — no mutation)
  AFTER STATEMENT IS
  BEGIN
    -- Recalculate order totals for all affected orders
    FORALL i IN 1..v_order_ids.COUNT
      UPDATE orders
      SET total_amount = (
        SELECT SUM(quantity * unit_price) FROM order_items
        WHERE order_id = v_order_ids(i)
      )
      WHERE order_id = v_order_ids(i);
    
    DBMS_OUTPUT.PUT_LINE('Recalculated ' || v_order_ids.COUNT || ' order totals');
  END AFTER STATEMENT;
  
END trg_orders_compound;
/
```

---

## Advanced Exception Handling

```plsql
-- Logging framework for PL/SQL errors
CREATE OR REPLACE PACKAGE error_log_pkg AS
  PROCEDURE log_error(
    p_proc_name IN VARCHAR2,
    p_error_msg IN VARCHAR2,
    p_sql_id    IN VARCHAR2 DEFAULT NULL,
    p_context   IN VARCHAR2 DEFAULT NULL
  );
  
  PROCEDURE reraise_with_context(
    p_proc_name IN VARCHAR2,
    p_context   IN VARCHAR2
  );
END error_log_pkg;
/

CREATE OR REPLACE PACKAGE BODY error_log_pkg AS
  PROCEDURE log_error(
    p_proc_name IN VARCHAR2,
    p_error_msg IN VARCHAR2,
    p_sql_id    IN VARCHAR2 DEFAULT NULL,
    p_context   IN VARCHAR2 DEFAULT NULL
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;  -- write error log even if caller rolls back
  BEGIN
    INSERT INTO error_log(
      log_id, proc_name, error_code, error_msg,
      call_stack, sql_id, context_info, log_time, session_user
    ) VALUES (
      error_log_seq.NEXTVAL,
      p_proc_name,
      SQLCODE,
      SUBSTR(SQLERRM, 1, 4000),
      SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000),
      p_sql_id,
      p_context,
      SYSTIMESTAMP,
      SYS_CONTEXT('USERENV', 'SESSION_USER')
    );
    COMMIT;
  END log_error;
  
  PROCEDURE reraise_with_context(
    p_proc_name IN VARCHAR2,
    p_context   IN VARCHAR2
  ) IS
  BEGIN
    log_error(p_proc_name, SQLERRM, NULL, p_context);
    RAISE;  -- re-raise original exception to caller
  END reraise_with_context;
END error_log_pkg;
/

-- Usage in production code
CREATE OR REPLACE PROCEDURE process_monthly_close(p_year IN NUMBER, p_month IN NUMBER) IS
BEGIN
  -- ... business logic ...
  NULL;
EXCEPTION
  WHEN OTHERS THEN
    error_log_pkg.reraise_with_context(
      'process_monthly_close',
      'year=' || p_year || ', month=' || p_month
    );
END process_monthly_close;
/
```

---

## PRAGMA AUTONOMOUS_TRANSACTION

Autonomous transactions run in a separate transaction context — commits/rollbacks don't affect the calling transaction:

```plsql
-- Use case: audit logging that must persist even if the main transaction rolls back
CREATE OR REPLACE PROCEDURE write_audit_log(
  p_action   IN VARCHAR2,
  p_table_nm IN VARCHAR2,
  p_row_id   IN NUMBER,
  p_details  IN VARCHAR2
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;  -- this runs in its own transaction
BEGIN
  INSERT INTO audit_log(action, table_name, row_id, details, logged_by, log_time)
  VALUES (p_action, p_table_nm, p_row_id, p_details, USER, SYSTIMESTAMP);
  COMMIT;  -- commits only the autonomous transaction
END write_audit_log;
/

-- Main procedure that uses the audit logger
CREATE OR REPLACE PROCEDURE update_account_balance(
  p_account_id IN NUMBER,
  p_amount     IN NUMBER
) IS
  v_current_balance NUMBER;
BEGIN
  SELECT balance INTO v_current_balance FROM accounts WHERE account_id = p_account_id;
  
  write_audit_log('UPDATE', 'ACCOUNTS', p_account_id,
    'Balance change: ' || v_current_balance || ' → ' || (v_current_balance + p_amount));
  
  UPDATE accounts SET balance = balance + p_amount WHERE account_id = p_account_id;
  
  IF v_current_balance + p_amount < 0 THEN
    ROLLBACK;  -- main transaction rolls back, but audit log stays committed
    RAISE_APPLICATION_ERROR(-20050, 'Insufficient funds');
  END IF;
  
  COMMIT;
END update_account_balance;
/
```

---

## Collections — Nested Tables vs VARRAYs vs Associative Arrays

```plsql
DECLARE
  -- Associative array (index-by table): dynamic size, fast random access
  TYPE t_salary_map IS TABLE OF NUMBER INDEX BY VARCHAR2(50);
  v_salaries t_salary_map;
  v_dept     VARCHAR2(50);
  
  -- Nested table: can be stored in database, SQL-accessible
  TYPE t_dept_list IS TABLE OF VARCHAR2(50);
  v_depts t_dept_list := t_dept_list('HR', 'SALES', 'IT');
  
  -- VARRAY: fixed max size, ordered, can be stored as column type
  TYPE t_top3 IS VARRAY(3) OF NUMBER;
  v_top_salaries t_top3 := t_top3(95000, 85000, 75000);
  
BEGIN
  -- Associative array: use like a hash map
  v_salaries('John') := 75000;
  v_salaries('Jane') := 90000;
  v_salaries('Bob')  := 60000;
  
  -- Iterate using FIRST/NEXT
  v_dept := v_salaries.FIRST;
  WHILE v_dept IS NOT NULL LOOP
    DBMS_OUTPUT.PUT_LINE(v_dept || ': ' || v_salaries(v_dept));
    v_dept := v_salaries.NEXT(v_dept);
  END LOOP;
  
  -- Nested table: extend and use SET operations
  v_depts.EXTEND;
  v_depts(v_depts.LAST) := 'FINANCE';
  v_depts.DELETE(2);  -- delete 'SALES' element
  
  -- Built-in collection methods
  DBMS_OUTPUT.PUT_LINE('Count: ' || v_depts.COUNT);   -- 3 (SALES deleted)
  DBMS_OUTPUT.PUT_LINE('Exists(1): ' || CASE WHEN v_depts.EXISTS(1) THEN 'Y' ELSE 'N' END);
END;
/

-- SQL operations on nested tables (stored in DB)
CREATE OR REPLACE TYPE t_skill_list IS TABLE OF VARCHAR2(50);

CREATE TABLE employees_ext (
  employee_id NUMBER,
  emp_name    VARCHAR2(100),
  skills      t_skill_list  -- nested table column
) NESTED TABLE skills STORE AS skills_nt;

INSERT INTO employees_ext VALUES (1, 'Jane', t_skill_list('Python', 'SQL', 'PL/SQL'));

-- Unnest in queries
SELECT e.employee_id, e.emp_name, s.COLUMN_VALUE AS skill
FROM employees_ext e, TABLE(e.skills) s;
```

---

## Performance Patterns

```plsql
-- Pattern: bulk merge for upsert
DECLARE
  TYPE t_emp_list IS TABLE OF employees%ROWTYPE;
  v_new_emps t_emp_list;
BEGIN
  -- Load new/updated employees from staging
  SELECT * BULK COLLECT INTO v_new_emps FROM employees_staging;
  
  -- MERGE instead of separate INSERT/UPDATE
  FORALL i IN 1..v_new_emps.COUNT
    MERGE INTO employees tgt
    USING (SELECT v_new_emps(i).employee_id AS eid,
                  v_new_emps(i).salary AS sal
           FROM DUAL) src
    ON (tgt.employee_id = src.eid)
    WHEN MATCHED THEN
      UPDATE SET tgt.salary = src.sal,
                 tgt.modified_date = SYSDATE
    WHEN NOT MATCHED THEN
      INSERT (employee_id, salary, modified_date)
      VALUES (src.eid, src.sal, SYSDATE);
  
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Merged ' || v_new_emps.COUNT || ' records');
END;
/
```

---

## Interview Tips

> **Tip 1:** "What is a pipelined table function and when would you use it?" — A pipelined function returns rows one at a time using `PIPE ROW` instead of building an entire collection in memory. Use it when: (1) the result set is too large for a collection, (2) you want the caller to start processing rows before the function finishes, (3) you want complex transformation logic to appear as a table in SQL queries (parallel ETL pipelines, complex business rules applied set-at-a-time).

> **Tip 2:** "Explain PRAGMA AUTONOMOUS_TRANSACTION." — An autonomous transaction runs in an independent transaction context, invisible to the calling transaction. COMMIT or ROLLBACK inside affects only the autonomous transaction. Primary use: audit logging — you want the log entry to persist even if the calling transaction rolls back due to an error. Must always commit or rollback within the autonomous block before returning.

> **Tip 3:** "How do you handle performance in PL/SQL procedures processing millions of rows?" — Three techniques: (1) BULK COLLECT with LIMIT to avoid memory issues while reducing context switches, (2) FORALL instead of row-by-row DML, (3) SAVE EXCEPTIONS in FORALL to skip bad rows without stopping the batch. Measure with `DBMS_UTILITY.GET_TIME` or real-time SQL monitoring to confirm the bottleneck before optimizing.
