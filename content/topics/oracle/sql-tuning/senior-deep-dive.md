---
title: "SQL Tuning — Senior Deep Dive"
topic: oracle
subtopic: sql-tuning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, sql-tuning, spm, adaptive-cursor-sharing, result-cache, parallel-query]
---

# SQL Tuning — Senior Deep Dive

## SQL Plan Management (SPM)

SPM captures and controls execution plans, preventing plan regressions after Oracle version upgrades or stats changes.

```sql
-- Enable automatic capture of baseline plans
ALTER SYSTEM SET optimizer_capture_sql_plan_baselines = TRUE;
ALTER SYSTEM SET optimizer_use_sql_plan_baselines = TRUE;

-- Manually load a known-good plan into a baseline
DECLARE
  cnt PLS_INTEGER;
BEGIN
  -- Load from cursor cache (plan that's currently running well)
  cnt := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(
    sql_id         => 'abc123xyz',
    plan_hash_value => 1234567890
  );
  DBMS_OUTPUT.PUT_LINE('Loaded ' || cnt || ' plan(s)');
END;
/

-- View all baselines
SELECT sql_handle, plan_name, enabled, accepted, fixed,
       last_executed, executions, elapsed_time/1000000 elapsed_sec
FROM dba_sql_plan_baselines
ORDER BY last_executed DESC;

-- Evolve unaccepted plans (compare new plan against accepted baseline)
DECLARE
  report CLOB;
BEGIN
  report := DBMS_SPM.EVOLVE_SQL_PLAN_BASELINE(
    sql_handle => 'SQL_abc123',
    plan_name  => 'SQL_PLAN_abc_1234',  -- the new plan to evaluate
    verify     => 'YES',                -- run both plans, compare
    commit     => 'YES'                 -- auto-accept if new plan is better
  );
  DBMS_OUTPUT.PUT_LINE(report);
END;
/

-- Fix a plan to prevent the optimizer from ever switching away
UPDATE dba_sql_plan_baselines
SET fixed = 'YES'
WHERE sql_handle = 'SQL_abc123'
  AND plan_name  = 'SQL_PLAN_abc_baseline';
-- Note: use DBMS_SPM.ALTER_SQL_PLAN_BASELINE instead in production
```

---

## Adaptive Cursor Sharing (ACS)

ACS allows Oracle to use different plans for the same SQL with different bind values — solving bind variable peeking issues:

```sql
-- Bind variable peeking: Oracle peeks at bind value at FIRST hard parse
-- Problem: plan for status='COMPLETE' (90% of rows) is wrong for status='PENDING' (0.1%)

-- Check if ACS is creating multiple child cursors
SELECT sql_id, child_number, executions,
       is_bind_sensitive, is_bind_aware,
       plan_hash_value
FROM v$sql
WHERE sql_id = 'abc123xyz'
ORDER BY child_number;
-- Multiple rows = ACS created different plans for different bind ranges

-- What bind ranges triggered different plans?
SELECT bind_set_hash_value, range_id, low, high, predicate
FROM v$sql_cs_histogram
WHERE sql_id = 'abc123xyz';

-- Force re-evaluation (flush cursor — use carefully in prod)
EXEC DBMS_SHARED_POOL.PURGE('abc123xyz', 'C');
```

---

## Result Cache

Cache SQL query results in SGA — subsequent runs return results from cache, not executing the query:

```sql
-- Enable result cache
ALTER SYSTEM SET result_cache_mode = MANUAL;  -- or FORCE
ALTER SYSTEM SET result_cache_max_size = 256M;  -- default 1% of SGA

-- Use result cache in a query (hint)
SELECT /*+ RESULT_CACHE */ 
  department_id, AVG(salary) avg_sal
FROM employees
GROUP BY department_id;

-- Use in a view (all queries on view get cached result)
CREATE OR REPLACE VIEW dept_salary_summary AS
SELECT /*+ RESULT_CACHE */ 
  d.department_name, COUNT(e.employee_id) headcount, AVG(e.salary) avg_sal
FROM departments d LEFT JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_name;

-- Check cache utilization
SELECT name, value FROM v$result_cache_statistics;

-- Invalidation: result cache is auto-invalidated when underlying tables change
-- Check what's cached
SELECT id, type, status, name, namespace
FROM v$result_cache_objects
WHERE status = 'Published'
ORDER BY creation_timestamp DESC;

-- Manually flush the result cache
EXEC DBMS_RESULT_CACHE.FLUSH;
```

Best for: aggregation queries on slowly changing reference/dimension tables (lookup tables, org hierarchies).

---

## Parallel Query Tuning

```sql
-- Set table-level parallelism (auto-decides degree)
ALTER TABLE orders PARALLEL;
ALTER TABLE orders PARALLEL 8;   -- fixed degree of 8

-- Force parallel in SQL
SELECT /*+ PARALLEL(o, 8) PARALLEL(c, 8) */ 
  c.region, SUM(o.amount)
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region;

-- Monitor active parallel queries
SELECT qc_sid, server_set, req_degree, actual_degree,
       px_servers_allocated, server_type
FROM v$px_session
WHERE qc_sid IN (SELECT sid FROM v$session WHERE status = 'ACTIVE');

-- Find queries using parallel execution
SELECT s.sid, s.username, s.event, 
       pq.req_degree, pq.actual_degree,
       SUBSTR(q.sql_text, 1, 80) sql_preview
FROM v$session s
JOIN v$px_session pq ON s.sid = pq.sid
JOIN v$sql q ON s.sql_id = q.sql_id;

-- Parallel DML
ALTER SESSION ENABLE PARALLEL DML;
INSERT /*+ PARALLEL(t, 8) */ INTO target_table SELECT * FROM source_table;
COMMIT;
ALTER SESSION DISABLE PARALLEL DML;
```

---

## Cardinality Feedback (12c+) and Statistics Feedback

```sql
-- Enable extended statistics for correlated columns
-- e.g., when filtering on (department_id AND job_id) together
DECLARE
  col_grp VARCHAR2(30);
BEGIN
  col_grp := DBMS_STATS.CREATE_EXTENDED_STATS(
    ownname  => 'HR',
    tabname  => 'EMPLOYEES',
    extension => '(DEPARTMENT_ID, JOB_ID)'  -- combined column group
  );
  DBMS_OUTPUT.PUT_LINE('Created: ' || col_grp);
END;
/

-- Gather stats on the extended stats group
EXEC DBMS_STATS.GATHER_TABLE_STATS('HR', 'EMPLOYEES', 
  method_opt => 'FOR ALL COLUMNS SIZE AUTO');
```

---

## SQL Access Advisor

```sql
-- Run SQL Access Advisor for index/MV recommendations
DECLARE
  task_name VARCHAR2(100) := 'ACCESS_ADVISOR_TASK';
BEGIN
  -- Create task from SQL tuning set or workload
  DBMS_ADVISOR.CREATE_TASK('SQL Access Advisor', task_name);
  
  -- Set the workload (from SQL Tuning Set)
  DBMS_ADVISOR.ADD_STS_REF(task_name, 'MY_STS_WORKLOAD');
  
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'ANALYSIS_SCOPE', 'ALL');
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'MODE', 'COMPREHENSIVE');
  
  DBMS_ADVISOR.EXECUTE_TASK(task_name);
END;
/

-- View recommendations
SELECT rec_id, benefit, rank, action_type,
       attr1 AS object_name, attr2 AS tablespace
FROM dba_advisor_recommendations r
JOIN dba_advisor_actions a ON r.task_name = a.task_name AND r.rec_id = a.rec_id
WHERE r.task_name = 'ACCESS_ADVISOR_TASK'
ORDER BY rank;
```

---

## Real-Time SQL Monitoring

```sql
-- Monitor a long-running query in real-time (requires Tuning Pack license)
SELECT DBMS_SQLTUNE.REPORT_SQL_MONITOR(
  sql_id     => 'abc123xyz',
  type       => 'TEXT',
  report_level => 'ALL'
) FROM DUAL;

-- For HTML report (best for complex plans):
SELECT DBMS_SQLTUNE.REPORT_SQL_MONITOR(
  sql_id     => 'abc123xyz',
  type       => 'HTML'
) FROM DUAL;
-- Then save the CLOB to a .html file and open in browser

-- Get list of monitored SQL
SELECT sql_id, status, elapsed_time/1000000 elapsed_sec,
       cpu_time/1000000 cpu_sec,
       buffer_gets, disk_reads,
       SUBSTR(sql_text, 1, 60) sql_preview
FROM v$sql_monitor
ORDER BY elapsed_time DESC
FETCH FIRST 20 ROWS ONLY;
```

---

## Interview Tips

> **Tip 1:** "What is SQL Plan Management and when do you use it?" — SPM captures execution plans as baselines and prevents the optimizer from switching to unverified plans. Key use case: before a database upgrade, capture baselines for critical queries. After upgrade, the optimizer can only use new plans after they're verified to perform at least as well as the baseline. Prevents plan regressions from being silently introduced.

> **Tip 2:** "Explain Adaptive Cursor Sharing." — ACS solves the bind variable peeking problem. Normally Oracle peeks at a bind value at first hard parse and uses that plan forever. ACS marks cursors as "bind sensitive" when histograms show data skew. If multiple executions show different performance characteristics for different bind ranges, Oracle creates additional child cursors with plans optimized for those ranges.

> **Tip 3:** "When would you use parallel query and what are the risks?" — Use parallel query for batch/warehouse workloads: full table scans, large hash joins, aggregations on fact tables. Avoid in OLTP — parallel query consumes many CPU cores and can degrade concurrency. Key risk: degree of parallelism × query CPU × concurrent users can saturate CPUs and kill response time for other sessions. Always set `PARALLEL_MAX_SERVERS` and consider resource manager plans.

## ⚡ Cheat Sheet

**PL/SQL essentials**
```sql
-- Stored procedure with exception handling
CREATE OR REPLACE PROCEDURE load_orders(p_date IN DATE) AS
    v_count NUMBER;
BEGIN
    INSERT INTO orders SELECT * FROM staging WHERE order_date = p_date;
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('Inserted: ' || v_count);
    COMMIT;
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20001, 'Duplicate order key for ' || p_date);
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END load_orders;
/
```

**AWR / performance tuning**
```sql
-- Top SQL by elapsed time (from AWR)
SELECT sql_id, elapsed_time/1000000 AS elapsed_sec, executions,
       elapsed_time/NULLIF(executions,0)/1000000 AS avg_sec,
       sql_text
FROM v$sqlstats
ORDER BY elapsed_time DESC FETCH FIRST 10 ROWS ONLY;

-- Explain plan
EXPLAIN PLAN FOR SELECT * FROM orders WHERE customer_id = 123;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(format=>'ALL'));

-- Force index hint
SELECT /*+ INDEX(o IDX_ORDERS_CUST) */ * FROM orders o WHERE customer_id = 123;
```

**Partitioning**
```sql
-- Range partitioning (most common for DE)
CREATE TABLE orders (order_id NUMBER, order_date DATE, amount NUMBER)
PARTITION BY RANGE (order_date) INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(PARTITION p_first VALUES LESS THAN (DATE '2024-01-01'));

-- Partition pruning: WHERE order_date = '2024-01-15' reads only one partition
```

**Oracle RAC key concepts**
```
Cache Fusion:  nodes share buffer cache via high-speed interconnect
GCS:           Global Cache Service — coordinates block ownership
GES:           Global Enqueue Service — distributed lock management
Interconnect:  low-latency private network between RAC nodes (mandatory)
VIP:           Virtual IP — client transparent failover on node failure
```

**SQL tuning checklist**
```
1. Check execution plan: is it using the right index?
2. Check cardinality estimates: are they close to actual rows?
3. Statistics stale? Run DBMS_STATS.GATHER_TABLE_STATS
4. High parse time? Consider bind variables or cursor_sharing=FORCE
5. Full table scan on large table? Add index or partition pruning
6. Nested loops on large tables? Consider hash join hint
7. High I/O? Check if result fits in buffer cache (db_cache_size)
```

**Materialized view fast refresh**
```sql
CREATE MATERIALIZED VIEW LOG ON orders WITH ROWID, SEQUENCE (order_id, amount, region)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW mv_orders_by_region
REFRESH FAST ON COMMIT AS
SELECT region, SUM(amount) AS total FROM orders GROUP BY region;
```

**Key interview points**
- Bind variables: prevent hard parse; critical for OLTP performance (cursor reuse)
- Partition pruning: Oracle auto-prunes when filter on partition key
- Data Guard: physical standby (redo apply) vs logical standby (SQL apply)
- Exadata: Smart Scan offloads WHERE/column projection to storage cells (iDB protocol)
- RAC: active-active; all nodes can read/write; best for OLTP scale-out
