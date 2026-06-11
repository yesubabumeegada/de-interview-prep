---
title: "PL/SQL — Fundamentals"
topic: oracle
subtopic: pl-sql
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, pl-sql, procedures, functions, cursors, exceptions]
---

# PL/SQL — Fundamentals


## 🎯 Analogy

Think of PL/SQL like Python for Oracle: stored procedures, functions, packages, and triggers that run inside the database engine — much faster than round-tripping SQL calls from application code for complex multi-step logic.

---
## What Is PL/SQL?

PL/SQL (Procedural Language/SQL) is Oracle's procedural extension to SQL. It allows you to write programs that combine SQL statements with procedural logic (loops, conditions, exception handling).

**Why PL/SQL over application code for database logic?**
- SQL executes inside the database — no network round trips for each statement
- Stored procedures are compiled and cached
- Exception handling is built into the language
- BULK operations for high-performance DML

---

## Block Structure

Every PL/SQL program is a block:

```plsql
DECLARE
  -- Variable declarations
  v_count    NUMBER := 0;
  v_name     VARCHAR2(100);
  v_hire_date DATE := SYSDATE;

BEGIN
  -- Executable statements
  SELECT COUNT(*) INTO v_count FROM employees WHERE department_id = 10;
  
  IF v_count > 0 THEN
    DBMS_OUTPUT.PUT_LINE('Department has ' || v_count || ' employees');
  ELSE
    DBMS_OUTPUT.PUT_LINE('Department is empty');
  END IF;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('No data found');
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/
```

---

## Variables and Data Types

```plsql
DECLARE
  -- Scalar types
  v_num        NUMBER(10, 2)   := 99.99;
  v_text       VARCHAR2(255)   := 'Hello';
  v_flag       BOOLEAN         := TRUE;
  v_dt         DATE            := SYSDATE;
  v_ts         TIMESTAMP       := SYSTIMESTAMP;
  
  -- %TYPE: inherits column type (safer — survives column type changes)
  v_salary     employees.salary%TYPE;
  v_name       employees.last_name%TYPE;
  
  -- %ROWTYPE: inherits entire row structure
  v_emp_rec    employees%ROWTYPE;
  
  -- Constant
  c_tax_rate   CONSTANT NUMBER := 0.08;
  
BEGIN
  SELECT salary, last_name INTO v_salary, v_name
  FROM employees WHERE employee_id = 100;
  
  SELECT * INTO v_emp_rec FROM employees WHERE employee_id = 100;
  
  DBMS_OUTPUT.PUT_LINE(v_emp_rec.first_name || ' earns ' || v_emp_rec.salary);
END;
/
```

---

## Control Flow

```plsql
DECLARE
  v_score NUMBER := 85;
  v_grade CHAR(1);
BEGIN
  -- IF-ELSIF-ELSE
  IF v_score >= 90 THEN
    v_grade := 'A';
  ELSIF v_score >= 80 THEN
    v_grade := 'B';
  ELSIF v_score >= 70 THEN
    v_grade := 'C';
  ELSE
    v_grade := 'F';
  END IF;
  
  -- CASE
  v_grade := CASE
    WHEN v_score >= 90 THEN 'A'
    WHEN v_score >= 80 THEN 'B'
    WHEN v_score >= 70 THEN 'C'
    ELSE 'F'
  END;
  
  -- Simple LOOP
  DECLARE v_i NUMBER := 1;
  BEGIN
    LOOP
      EXIT WHEN v_i > 5;
      DBMS_OUTPUT.PUT_LINE('i = ' || v_i);
      v_i := v_i + 1;
    END LOOP;
  END;
  
  -- FOR LOOP (numeric)
  FOR i IN 1..10 LOOP
    DBMS_OUTPUT.PUT_LINE('Row ' || i);
  END LOOP;
  
  -- WHILE LOOP
  DECLARE v_j NUMBER := 1;
  BEGIN
    WHILE v_j <= 5 LOOP
      DBMS_OUTPUT.PUT_LINE('j = ' || v_j);
      v_j := v_j + 1;
    END LOOP;
  END;
END;
/
```

---

## Explicit Cursors

Use explicit cursors to process query results row by row:

```plsql
DECLARE
  CURSOR c_employees IS
    SELECT employee_id, first_name, salary
    FROM employees
    WHERE department_id = 10
    ORDER BY salary DESC;
  
  v_emp_id   employees.employee_id%TYPE;
  v_name     employees.first_name%TYPE;
  v_salary   employees.salary%TYPE;
BEGIN
  OPEN c_employees;
  LOOP
    FETCH c_employees INTO v_emp_id, v_name, v_salary;
    EXIT WHEN c_employees%NOTFOUND;
    
    DBMS_OUTPUT.PUT_LINE(v_name || ': $' || v_salary);
  END LOOP;
  CLOSE c_employees;
END;
/

-- Cursor FOR LOOP (simpler — auto OPEN/FETCH/CLOSE)
BEGIN
  FOR rec IN (SELECT employee_id, first_name, salary 
              FROM employees WHERE department_id = 10) LOOP
    DBMS_OUTPUT.PUT_LINE(rec.first_name || ': $' || rec.salary);
  END LOOP;
END;
/
```

---

## Stored Procedures and Functions

```plsql
-- Procedure: no return value
CREATE OR REPLACE PROCEDURE give_raise(
  p_emp_id  IN  employees.employee_id%TYPE,
  p_pct     IN  NUMBER,
  p_new_sal OUT employees.salary%TYPE
) IS
BEGIN
  UPDATE employees
  SET salary = salary * (1 + p_pct / 100)
  WHERE employee_id = p_emp_id
  RETURNING salary INTO p_new_sal;
  
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Raised employee ' || p_emp_id || ' salary to ' || p_new_sal);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20001, 'Employee ' || p_emp_id || ' not found');
END give_raise;
/

-- Execute the procedure
DECLARE
  v_new_sal employees.salary%TYPE;
BEGIN
  give_raise(100, 10, v_new_sal);
  DBMS_OUTPUT.PUT_LINE('New salary: ' || v_new_sal);
END;
/

-- Function: must return a value
CREATE OR REPLACE FUNCTION get_annual_salary(
  p_emp_id IN employees.employee_id%TYPE
) RETURN NUMBER IS
  v_monthly employees.salary%TYPE;
BEGIN
  SELECT salary INTO v_monthly FROM employees WHERE employee_id = p_emp_id;
  RETURN v_monthly * 12;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
END get_annual_salary;
/

-- Use in SQL
SELECT employee_id, get_annual_salary(employee_id) annual_sal FROM employees;
```

---

## Exception Handling

```plsql
DECLARE
  v_sal employees.salary%TYPE;
BEGIN
  SELECT salary INTO v_sal FROM employees WHERE employee_id = 99999;  -- doesn't exist
  
EXCEPTION
  -- Predefined Oracle exceptions
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Employee not found');
  WHEN TOO_MANY_ROWS THEN
    DBMS_OUTPUT.PUT_LINE('Query returned multiple rows — use cursor');
  WHEN DUP_VAL_ON_INDEX THEN
    DBMS_OUTPUT.PUT_LINE('Duplicate value — constraint violated');
  WHEN OTHERS THEN
    -- SQLCODE: error number; SQLERRM: error message
    DBMS_OUTPUT.PUT_LINE('Error ' || SQLCODE || ': ' || SQLERRM);
    -- Always log before re-raising
    RAISE;  -- re-raise to caller
END;
/

-- User-defined exceptions
DECLARE
  e_invalid_salary EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_invalid_salary, -20001);  -- link to custom error number
  v_sal NUMBER := -500;
BEGIN
  IF v_sal < 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Salary cannot be negative: ' || v_sal);
  END IF;
EXCEPTION
  WHEN e_invalid_salary THEN
    DBMS_OUTPUT.PUT_LINE('Caught invalid salary: ' || SQLERRM);
END;
/
```

---


## ▶️ Try It Yourself

```sql
-- PL/SQL stored procedure with exception handling
CREATE OR REPLACE PROCEDURE process_order(
    p_order_id  IN  orders.order_id%TYPE,
    p_status    IN  VARCHAR2,
    p_rows_out  OUT NUMBER
)
AS
BEGIN
    UPDATE orders SET status = p_status, updated_at = SYSDATE
    WHERE order_id = p_order_id;

    p_rows_out := SQL%ROWCOUNT;

    IF p_rows_out = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Order ' || p_order_id || ' not found');
    END IF;

    INSERT INTO audit_log (order_id, action, changed_at)
    VALUES (p_order_id, 'STATUS_CHANGE_' || p_status, SYSDATE);

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END process_order;
/

-- Execute
DECLARE v_rows NUMBER;
BEGIN
    process_order(42, 'COMPLETED', v_rows);
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_rows || ' rows');
END;
/
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is the difference between a procedure and a function in PL/SQL?" — A function must return a value (via RETURN) and can be used in SQL expressions. A procedure doesn't return a value (but can use OUT parameters) and cannot be used directly in SQL. Use functions for calculations; use procedures for DML operations or business logic with multiple OUT values.

> **Tip 2:** "What is %TYPE and why use it?" — `%TYPE` anchors a variable's data type to a column's current type. If the column type changes (e.g., VARCHAR2(100) → VARCHAR2(200)), your PL/SQL variable automatically inherits the new type without code changes. Always use `%TYPE` and `%ROWTYPE` instead of hardcoding types.

> **Tip 3:** "When would you use an explicit cursor vs an implicit cursor?" — Implicit cursors (SELECT INTO) work for exactly one row. Explicit cursors are needed when you expect zero or multiple rows and need to iterate through a result set. The cursor FOR LOOP is the simplest explicit cursor pattern — it auto-handles OPEN, FETCH, and CLOSE.
