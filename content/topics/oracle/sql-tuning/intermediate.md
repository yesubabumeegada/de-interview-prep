---
title: "SQL Tuning — Intermediate"
topic: oracle
subtopic: sql-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, sql-tuning, hints, bind-variables, adaptive-plans, sql-profiles]
---

# SQL Tuning — Intermediate

## DBMS_XPLAN — Reading Real Plans

Always use the plan from V$SQL (the plan that actually ran), not EXPLAIN PLAN (the plan that would run):

```sql
-- Get the SQL_ID for a query
SELECT sql_id, executions, elapsed_time/1000000 elapsed_sec, 
       buffer_gets, disk_reads, sql_text
FROM v$sql
WHERE sql_text LIKE '%orders%'
  AND sql_text NOT LIKE '%v$sql%'
ORDER BY elapsed_time DESC
FETCH FIRST 10 ROWS ONLY;

-- Display the actual plan with row estimates vs actuals
SELECT * FROM TABLE(
  DBMS_XPLAN.DISPLAY_CURSOR(
    sql_id    => 'abc123xyz',
    cursor_child_no => 0,
    format    => 'ALLSTATS LAST'   -- shows E-Rows (estimated) vs A-Rows (actual)
  )
);
```

**ALLSTATS LAST output — look for cardinality mismatches:**
```
----------------------------------------------------------
| Id | Operation          | Name     | E-Rows | A-Rows |
----------------------------------------------------------
|  3 |  TABLE ACCESS FULL | ORDERS   |     50 |  95000 |   ← MISMATCH! Oracle estimated 50, got 95K
----------------------------------------------------------
```
When E-Rows ≠ A-Rows by more than 10×, the optimizer is making wrong decisions — fix by gathering better statistics or adding histogram.

---

## Histograms — When Data Is Skewed

```sql
-- Column with skewed values needs a histogram
-- e.g., 90% of orders have status='COMPLETE', 10% are other statuses

-- Gather height-balanced histogram
EXEC DBMS_STATS.GATHER_TABLE_STATS(
  'HR', 'ORDERS',
  method_opt => 'FOR COLUMNS SIZE 254 STATUS'  -- histogram on STATUS column
);

-- Check histogram exists
SELECT column_name, histogram, num_distinct, num_nulls
FROM dba_tab_col_statistics
WHERE table_name = 'ORDERS'
  AND owner = 'HR';
```

Without histogram: Oracle estimates equal distribution across all values.
With histogram: Oracle knows `status='COMPLETE'` returns 90% of rows (full scan) vs `status='PENDING'` returns 0.1% (index scan).

---

## Optimizer Hints

Hints are directives embedded in SQL to override CBO decisions. Use as a last resort — fix root cause (stats, indexes) first.

```sql
-- Force an index
SELECT /*+ INDEX(o idx_orders_date) */ order_id, amount
FROM orders o
WHERE order_date > SYSDATE - 30;

-- Prevent a full scan
SELECT /*+ NO_FULL(o) */ order_id FROM orders o WHERE status = 'PENDING';

-- Force hash join (override nested loops)
SELECT /*+ USE_HASH(c o) */ c.customer_name, SUM(o.amount)
FROM customers c JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name;

-- Force nested loops join
SELECT /*+ USE_NL(c o) */ c.customer_name, o.order_id
FROM customers c JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = 42;

-- Drive join order (list driving table first in hint)
SELECT /*+ LEADING(c o) USE_NL(o) */ c.customer_name, o.amount
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
  AND c.region = 'WEST';

-- Parallel query hint
SELECT /*+ PARALLEL(o, 8) */ COUNT(*) FROM orders o;
```

**Common hints reference:**
| Hint | Purpose |
|---|---|
| `INDEX(table index_name)` | Force specific index |
| `NO_INDEX(table index_name)` | Prevent index use |
| `FULL(table)` | Force full table scan |
| `USE_HASH(table)` | Use hash join |
| `USE_NL(table)` | Use nested loops join |
| `USE_MERGE(table)` | Use sort-merge join |
| `LEADING(t1 t2)` | Set join order |
| `PARALLEL(table, degree)` | Enable parallel query |
| `NO_PARALLEL` | Disable parallel query |
| `RESULT_CACHE` | Cache query result in SGA |

---

## Bind Variables and Cursor Sharing

```sql
-- Session-level: view cursor sharing stats
SELECT name, value FROM v$parameter WHERE name = 'cursor_sharing';

-- Instance-level setting (can also be set per session)
-- EXACT (default): only exact SQL text matches share cursors
-- FORCE: replace literals with system-generated binds (use carefully — can misfire)
ALTER SYSTEM SET cursor_sharing = FORCE;  -- careful in production

-- The real fix: use bind variables in application code
-- Python/cx_Oracle example:
import cx_Oracle
conn = cx_Oracle.connect("user/pass@dsn")
cur = conn.cursor()

# BAD — new hard parse per customer_id value
cur.execute(f"SELECT * FROM orders WHERE customer_id = {customer_id}")

# GOOD — shared cursor, reuse plan
cur.execute("SELECT * FROM orders WHERE customer_id = :cid", cid=customer_id)
```

---

## SQL Profiles

A SQL Profile stores auxiliary statistics that correct bad cardinality estimates — without changing the SQL text.

```sql
-- Run SQL Tuning Advisor to find issues and create a profile
DECLARE
  task_name VARCHAR2(30);
BEGIN
  task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
    sql_id     => 'abc123xyz',
    scope      => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
    time_limit => 300  -- 5 minutes
  );
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name);
  DBMS_OUTPUT.PUT_LINE('Task: ' || task_name);
END;
/

-- View recommendations
SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK('TASK_12345') FROM DUAL;

-- If advisor recommends a SQL Profile, accept it:
EXEC DBMS_SQLTUNE.ACCEPT_SQL_PROFILE(
  task_name => 'TASK_12345',
  force_match => TRUE  -- match even with different bind values
);

-- Verify profile is in place
SELECT name, category, status, sql_text 
FROM dba_sql_profiles
ORDER BY created DESC;
```

SQL Profile vs SQL Plan Baseline vs Hint:
- **Hint**: embedded in SQL text — changes the code
- **SQL Profile**: auxiliary stats stored out-of-band — SQL text unchanged, better plan
- **SQL Plan Baseline**: locks a specific plan — prevents plan regression after optimizer changes

---

## Adaptive Query Optimization (12c+)

Oracle 12c introduced adaptive plans that change at runtime based on actual row counts:

```sql
-- Check if adaptive plans are enabled
SELECT name, value FROM v$parameter 
WHERE name IN ('optimizer_adaptive_plans', 'optimizer_adaptive_statistics');

-- View an adaptive plan (shows the chosen plan)
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(format => 'ADAPTIVE'));
-- Look for: "- is not chosen" lines showing the alternative that was rejected at runtime
```

Adaptive plans work well for joins where estimated join size is uncertain. They start with a nested loops plan and switch to hash join at a statistics collector node if row count exceeds the threshold.

---

## V$ Views for SQL Tuning

```sql
-- Top SQL by elapsed time
SELECT sql_id, ROUND(elapsed_time/1000000) elapsed_sec,
       executions, ROUND(elapsed_time/1000000/NULLIF(executions,0),2) avg_sec,
       buffer_gets, ROUND(buffer_gets/NULLIF(executions,0)) avg_gets,
       sql_text
FROM v$sql
WHERE executions > 0
ORDER BY elapsed_time DESC
FETCH FIRST 20 ROWS ONLY;

-- Top SQL by buffer gets (logical I/O)
SELECT sql_id, buffer_gets, executions, 
       ROUND(buffer_gets/NULLIF(executions,0)) avg_gets,
       SUBSTR(sql_text, 1, 80) sql_preview
FROM v$sql
ORDER BY buffer_gets DESC
FETCH FIRST 10 ROWS ONLY;

-- Active sessions right now
SELECT s.sid, s.serial#, s.username, s.status,
       s.event, s.seconds_in_wait,
       q.sql_text
FROM v$session s
LEFT JOIN v$sql q ON s.sql_id = q.sql_id
WHERE s.type = 'USER'
  AND s.status = 'ACTIVE'
ORDER BY s.seconds_in_wait DESC;
```

---

## Interview Tips

> **Tip 1:** "What is the difference between EXPLAIN PLAN and DBMS_XPLAN.DISPLAY_CURSOR?" — EXPLAIN PLAN shows what plan Oracle *would* generate if you ran the query now, without actually running it. DISPLAY_CURSOR shows the plan from V$SQL — the plan that *actually ran*, including actual row counts (A-Rows). Always prefer DISPLAY_CURSOR for tuning real performance issues.

> **Tip 2:** "When do you use histograms?" — When a column has highly skewed data distribution (e.g., 90% of rows have one value). Without histograms, CBO assumes uniform distribution and may choose a full scan for a highly selective predicate. Gather histograms with `METHOD_OPT => 'FOR COLUMNS SIZE 254 <column_name>'`.

> **Tip 3:** "What is a SQL Profile and when would you use it?" — A SQL Profile stores optimizer correction factors (adjusted cardinality estimates) out-of-band. Use it when: (1) you can't change the SQL text (vendor app), (2) the plan is bad due to wrong cardinality estimates, (3) SQL Tuning Advisor recommends one. Unlike hints, profiles don't need the SQL text modified.
