---
title: "PL/SQL — Intermediate"
topic: oracle
subtopic: pl-sql
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, pl-sql, bulk-collect, forall, packages, triggers, dynamic-sql]
---

# PL/SQL — Intermediate

## BULK COLLECT and FORALL

Row-by-row PL/SQL processing is slow because each SQL call causes a context switch between the PL/SQL engine and SQL engine. BULK operations batch multiple rows to reduce context switches.

```plsql
-- BAD: Row-by-row context switching (slow for large sets)
DECLARE
  CURSOR c_emp IS SELECT employee_id FROM employees WHERE salary < 30000;
BEGIN
  FOR rec IN c_emp LOOP
    UPDATE employees SET salary = salary * 1.1 WHERE employee_id = rec.employee_id;
  END LOOP;
  COMMIT;
END;
/

-- GOOD: BULK COLLECT + FORALL (single context switch for entire batch)
DECLARE
  TYPE t_emp_ids IS TABLE OF employees.employee_id%TYPE;
  v_emp_ids t_emp_ids;
BEGIN
  -- Fetch all matching IDs in one SQL call
  SELECT employee_id
  BULK COLLECT INTO v_emp_ids
  FROM employees
  WHERE salary < 30000;
  
  DBMS_OUTPUT.PUT_LINE('Fetched ' || v_emp_ids.COUNT || ' employees');
  
  -- Update all rows in one SQL call
  FORALL i IN 1..v_emp_ids.COUNT
    UPDATE employees
    SET salary = salary * 1.1
    WHERE employee_id = v_emp_ids(i);
  
  DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' rows');
  COMMIT;
END;
/

-- For very large tables: process in batches using LIMIT
DECLARE
  TYPE t_emp_ids IS TABLE OF employees.employee_id%TYPE;
  v_emp_ids t_emp_ids;
  CURSOR c_emp IS SELECT employee_id FROM employees WHERE salary < 30000;
BEGIN
  OPEN c_emp;
  LOOP
    FETCH c_emp BULK COLLECT INTO v_emp_ids LIMIT 1000;  -- process 1000 at a time
    EXIT WHEN v_emp_ids.COUNT = 0;
    
    FORALL i IN 1..v_emp_ids.COUNT
      UPDATE employees SET salary = salary * 1.1 WHERE employee_id = v_emp_ids(i);
    
    COMMIT;  -- commit each batch
    DBMS_OUTPUT.PUT_LINE('Processed batch of ' || v_emp_ids.COUNT);
  END LOOP;
  CLOSE c_emp;
END;
/
```

---

## FORALL with SAVE EXCEPTIONS

Handle individual row errors without stopping the entire FORALL:

```plsql
DECLARE
  TYPE t_ids IS TABLE OF NUMBER;
  v_ids t_ids := t_ids(1, 2, 999999, 4, 5);  -- 999999 doesn't exist
  e_bulk_errors EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_bulk_errors, -24381);
BEGIN
  FORALL i IN 1..v_ids.COUNT SAVE EXCEPTIONS
    UPDATE employees SET salary = salary * 1.1 WHERE employee_id = v_ids(i);
  
EXCEPTION
  WHEN e_bulk_errors THEN
    -- SQL%BULK_EXCEPTIONS contains the error details
    FOR j IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE(
        'Error at index ' || SQL%BULK_EXCEPTIONS(j).error_index ||
        ': ' || SQLERRM(-SQL%BULK_EXCEPTIONS(j).error_code)
      );
    END LOOP;
    COMMIT;  -- commit successful rows even if some failed
END;
/
```

---

## Packages

Packages group related procedures, functions, types, and variables into a logical unit.

```plsql
-- Package Specification (public interface)
CREATE OR REPLACE PACKAGE employee_pkg AS
  -- Public constants
  c_max_salary CONSTANT NUMBER := 500000;
  
  -- Public types
  TYPE t_emp_record IS RECORD (
    emp_id   employees.employee_id%TYPE,
    emp_name VARCHAR2(100),
    salary   employees.salary%TYPE
  );
  TYPE t_emp_list IS TABLE OF t_emp_record;
  
  -- Public procedure declarations
  PROCEDURE hire_employee(
    p_first_name IN VARCHAR2,
    p_last_name  IN VARCHAR2,
    p_dept_id    IN NUMBER,
    p_salary     IN NUMBER,
    p_emp_id     OUT NUMBER
  );
  
  PROCEDURE terminate_employee(p_emp_id IN NUMBER);
  
  FUNCTION get_salary(p_emp_id IN NUMBER) RETURN NUMBER;
  FUNCTION get_dept_headcount(p_dept_id IN NUMBER) RETURN NUMBER;
  
END employee_pkg;
/

-- Package Body (implementation)
CREATE OR REPLACE PACKAGE BODY employee_pkg AS
  -- Private variable (not visible outside package)
  g_last_hired_id NUMBER;
  
  -- Private procedure
  PROCEDURE log_action(p_action IN VARCHAR2, p_emp_id IN NUMBER) IS
  BEGIN
    INSERT INTO audit_log(action, emp_id, log_time)
    VALUES (p_action, p_emp_id, SYSTIMESTAMP);
  END log_action;
  
  PROCEDURE hire_employee(
    p_first_name IN VARCHAR2,
    p_last_name  IN VARCHAR2,
    p_dept_id    IN NUMBER,
    p_salary     IN NUMBER,
    p_emp_id     OUT NUMBER
  ) IS
  BEGIN
    IF p_salary > c_max_salary THEN
      RAISE_APPLICATION_ERROR(-20010, 'Salary exceeds maximum: ' || c_max_salary);
    END IF;
    
    INSERT INTO employees(employee_id, first_name, last_name, department_id, salary, hire_date)
    VALUES(employees_seq.NEXTVAL, p_first_name, p_last_name, p_dept_id, p_salary, SYSDATE)
    RETURNING employee_id INTO p_emp_id;
    
    g_last_hired_id := p_emp_id;  -- update package-level state
    log_action('HIRE', p_emp_id);
    COMMIT;
  END hire_employee;
  
  FUNCTION get_salary(p_emp_id IN NUMBER) RETURN NUMBER IS
    v_sal employees.salary%TYPE;
  BEGIN
    SELECT salary INTO v_sal FROM employees WHERE employee_id = p_emp_id;
    RETURN v_sal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN NULL;
  END get_salary;
  
  FUNCTION get_dept_headcount(p_dept_id IN NUMBER) RETURN NUMBER IS
    v_cnt NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_cnt FROM employees WHERE department_id = p_dept_id;
    RETURN v_cnt;
  END get_dept_headcount;
  
  PROCEDURE terminate_employee(p_emp_id IN NUMBER) IS
  BEGIN
    UPDATE employees SET termination_date = SYSDATE WHERE employee_id = p_emp_id;
    log_action('TERMINATE', p_emp_id);
    COMMIT;
  END terminate_employee;

END employee_pkg;
/

-- Usage
DECLARE
  v_new_id NUMBER;
BEGIN
  employee_pkg.hire_employee('John', 'Doe', 10, 75000, v_new_id);
  DBMS_OUTPUT.PUT_LINE('Hired employee ID: ' || v_new_id);
  DBMS_OUTPUT.PUT_LINE('Dept headcount: ' || employee_pkg.get_dept_headcount(10));
END;
/
```

---

## Triggers

```plsql
-- BEFORE INSERT trigger: auto-populate audit fields
CREATE OR REPLACE TRIGGER trg_employees_bi
BEFORE INSERT ON employees
FOR EACH ROW
BEGIN
  :NEW.created_by   := NVL(:NEW.created_by, USER);
  :NEW.created_date := NVL(:NEW.created_date, SYSDATE);
  :NEW.modified_by  := USER;
  :NEW.modified_date := SYSDATE;
  
  -- Auto-generate ID if not provided
  IF :NEW.employee_id IS NULL THEN
    :NEW.employee_id := employees_seq.NEXTVAL;
  END IF;
END;
/

-- AFTER UPDATE trigger: audit changes
CREATE OR REPLACE TRIGGER trg_employees_salary_audit
AFTER UPDATE OF salary ON employees
FOR EACH ROW
BEGIN
  INSERT INTO salary_audit_log(
    employee_id, old_salary, new_salary, changed_by, changed_date
  ) VALUES (
    :OLD.employee_id, :OLD.salary, :NEW.salary, USER, SYSDATE
  );
END;
/

-- Statement-level trigger (no FOR EACH ROW)
CREATE OR REPLACE TRIGGER trg_prevent_weekend_updates
BEFORE INSERT OR UPDATE OR DELETE ON orders
BEGIN
  IF TO_CHAR(SYSDATE, 'DY') IN ('SAT', 'SUN') THEN
    RAISE_APPLICATION_ERROR(-20099, 'DML not allowed on weekends');
  END IF;
END;
/

-- Enable/Disable triggers
ALTER TRIGGER trg_employees_bi DISABLE;
ALTER TABLE employees DISABLE ALL TRIGGERS;  -- disable all on table
ALTER TABLE employees ENABLE ALL TRIGGERS;
```

---

## Dynamic SQL (EXECUTE IMMEDIATE)

```plsql
-- Build and run SQL at runtime
DECLARE
  v_table   VARCHAR2(30) := 'EMPLOYEES';
  v_col     VARCHAR2(30) := 'SALARY';
  v_threshold NUMBER := 50000;
  v_count   NUMBER;
  v_sql     VARCHAR2(500);
BEGIN
  -- Static query (prefer this when possible)
  v_sql := 'SELECT COUNT(*) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(v_table) ||
           ' WHERE ' || DBMS_ASSERT.SIMPLE_SQL_NAME(v_col) || ' > :thresh';
  
  EXECUTE IMMEDIATE v_sql INTO v_count USING v_threshold;
  DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
  
  -- DDL via dynamic SQL
  EXECUTE IMMEDIATE 'CREATE TABLE temp_log (id NUMBER, msg VARCHAR2(200), ts DATE)';
  EXECUTE IMMEDIATE 'DROP TABLE temp_log PURGE';
END;
/

-- Dynamic SQL with REF CURSOR (for SELECT returning rows)
DECLARE
  TYPE t_ref_cursor IS REF CURSOR;
  v_cursor t_ref_cursor;
  v_sql    VARCHAR2(500);
  v_emp_id NUMBER;
  v_sal    NUMBER;
  v_dept   NUMBER := 10;
BEGIN
  v_sql := 'SELECT employee_id, salary FROM employees WHERE department_id = :d ORDER BY salary DESC';
  OPEN v_cursor FOR v_sql USING v_dept;
  LOOP
    FETCH v_cursor INTO v_emp_id, v_sal;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('Emp ' || v_emp_id || ': $' || v_sal);
  END LOOP;
  CLOSE v_cursor;
END;
/
```

---

## Interview Tips

> **Tip 1:** "What's the difference between BULK COLLECT and FORALL?" — BULK COLLECT fetches multiple rows from a query into a PL/SQL collection in one operation (reduces SQL→PL/SQL context switches for reads). FORALL sends multiple DML statements using a collection as input in one operation (reduces PL/SQL→SQL context switches for writes). Use them together for batch processing: BULK COLLECT the IDs, then FORALL to DML.

> **Tip 2:** "Why are packages preferred over standalone procedures?" — Packages offer: (1) logical grouping of related code, (2) package-state variables that persist for the session, (3) overloading of procedure/function names, (4) forward declarations, (5) one-time initialization via package body initialization section. They also reduce dependency invalidation — changing a body doesn't invalidate other objects that reference the package spec.

> **Tip 3:** "What are the mutation table error in triggers?" — A mutation error (ORA-04091) occurs when a row-level trigger tries to query or modify the table it's defined on. Workaround: use a compound trigger (AFTER STATEMENT section for DML, package-level collection to accumulate rows during FOR EACH ROW phase), or use an AFTER STATEMENT trigger instead.
