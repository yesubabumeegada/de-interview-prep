---
title: "Oracle Interview Scenarios - Intermediate"
topic: oracle
subtopic: interview-scenarios
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, interview, scenarios, sql-tuning, pl-sql]
---

# Oracle Interview Scenarios – Intermediate

## Overview

This guide targets mid-level Oracle developers and data engineers. Topics include PL/SQL debugging techniques, AWR report interpretation, reading advanced execution plans, and real-world tuning scenarios.

---

## 1. PL/SQL Debugging Techniques

### Using DBMS_OUTPUT for lightweight debugging

```plsql
SET SERVEROUTPUT ON SIZE UNLIMITED

CREATE OR REPLACE PROCEDURE process_orders(p_cutoff_date IN DATE) IS
    v_count     NUMBER := 0;
    v_error_msg VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting process_orders at: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

    FOR rec IN (
        SELECT order_id, customer_id, order_total
        FROM   orders
        WHERE  order_date >= p_cutoff_date
        AND    status = 'PENDING'
    ) LOOP
        BEGIN
            -- Simulate processing
            UPDATE orders SET status = 'PROCESSED' WHERE order_id = rec.order_id;
            v_count := v_count + 1;

            IF MOD(v_count, 1000) = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Processed ' || v_count || ' rows...');
                COMMIT;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;
                DBMS_OUTPUT.PUT_LINE('Error on order_id=' || rec.order_id || ': ' || v_error_msg);
                -- Log and continue
                INSERT INTO error_log (proc_name, error_msg, ref_id, logged_at)
                VALUES ('process_orders', v_error_msg, rec.order_id, SYSDATE);
        END;
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Completed. Total processed: ' || v_count);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END process_orders;
/
```

### Using DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

The built-in `SQLERRM` tells you *what* the error is but not *where* in the call stack it originated. `FORMAT_ERROR_BACKTRACE` pinpoints the exact line number.

```plsql
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END;
```

### Conditional compilation for debug logging

```plsql
CREATE OR REPLACE PACKAGE pkg_config AS
    c_debug CONSTANT BOOLEAN := FALSE;  -- flip to TRUE in dev
END;
/

-- In your procedure
$IF pkg_config.c_debug $THEN
    DBMS_OUTPUT.PUT_LINE('Debug: v_count = ' || v_count);
$END
```

### Interview Q&A

**Q: A PL/SQL procedure runs fine locally but throws ORA-06502 (numeric or value error) in production. How do you isolate the exact line?**

**A:**
1. Add `DBMS_UTILITY.FORMAT_ERROR_BACKTRACE` to the exception handler — it returns the precise line number where the exception was raised.
2. Check whether the production table column lengths or NUMBER precision differ from the development schema.
3. Use `DBMS_PROFILER` or `DBMS_PLSQL_CODE_COVERAGE` to instrument the procedure and identify which branch is reached.

---

## 2. AWR Report Interpretation

### What is AWR?

The Automatic Workload Repository (AWR) collects performance snapshots every 60 minutes by default and retains them for 8 days. AWR reports are the primary tool for diagnosing Oracle performance problems.

```sql
-- Generate an AWR report between two snapshots
@$ORACLE_HOME/rdbms/admin/awrrpt.sql

-- Or use the PL/SQL API
SELECT output FROM TABLE(
    DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_TEXT(
        l_dbid       => (SELECT dbid FROM v$database),
        l_inst_num   => 1,
        l_bid        => 1500,   -- begin snapshot id
        l_eid        => 1501    -- end snapshot id
    )
);
```

### Key sections to focus on in an AWR report

#### Top 5 Timed Events (DB Time breakdown)

This is the most important section. It shows where database time is spent.

| Event | Waits | Time (s) | Avg Wait (ms) | % DB Time |
|-------|-------|----------|---------------|-----------|
| db file sequential read | 1,234,567 | 3,210 | 2.6 | 45.2 |
| CPU time | — | 2,100 | — | 29.6 |
| log file sync | 45,000 | 890 | 19.8 | 12.5 |
| db file scattered read | 23,400 | 430 | 18.4 | 6.1 |
| enq: TX - row lock contention | 120 | 210 | 1750.0 | 3.0 |

**Interpreting the events:**
- `db file sequential read` – single-block reads; usually index lookups. High numbers suggest excessive index range scans or lots of ROWID-based fetches.
- `log file sync` – waits for redo to flush to disk on COMMIT. High avg wait indicates slow I/O on the redo log device or too-frequent commits.
- `db file scattered read` – multi-block reads; full table scans or full index scans.
- `enq: TX - row lock contention` – row-level locking conflicts; check for long-running transactions or hot rows.

#### SQL ordered by Elapsed Time

```
SQL Id        Elapsed Time(s)  Executions  Elapsed/Exec  Buffer Gets
------------- ---------------  ----------  ------------  -----------
a7b3x9zqm1f         12,340           50        246.8s   12,450,000
...
```

Drill into the top SQL by:
```sql
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_AWR(sql_id => 'a7b3x9zqm1f', format => 'ALLSTATS LAST')
);
```

#### Instance Efficiency

```
Buffer Hit %:         95.8  (target: > 95)
Soft Parse %:         98.2  (target: > 95)
Redo NoWait %:        99.9
Latch Hit %:          99.95
```

Low **Buffer Hit %** → increase `db_cache_size` or reduce full table scans.
Low **Soft Parse %** → queries not using bind variables; each execution is a hard parse causing CPU and latch contention.

### Interview Q&A

**Q: An AWR report shows `log file sync` averaging 25ms. What do you check?**

**A:**
1. Check I/O latency on the redo log device — target < 5ms.
2. Look for applications committing too frequently (e.g., row-by-row commits inside a loop). Batch commits every N rows.
3. Check `log_buffer` parameter — too small means more frequent flushes.
4. Consider async I/O or moving redo logs to faster storage (NVMe/SSD).
5. In RAC, check `gcs remote cr` and `gc current block 2-way` waits — cross-instance redo shipping adds latency.

---

## 3. Reading Advanced Execution Plans

### DBMS_XPLAN.DISPLAY_CURSOR — the real plan

Always use `DISPLAY_CURSOR` over `DISPLAY` for production diagnosis because it shows the *actual* plan from the cursor cache, including real row counts.

```sql
-- After running the query
SELECT * FROM TABLE(
    DBMS_XPLAN.DISPLAY_CURSOR(
        sql_id     => NULL,       -- NULL = last statement in session
        cursor_child_no => 0,
        format     => 'ALLSTATS LAST +PEEKED_BINDS'
    )
);
```

### Understanding cardinality misestimates

```
| Id | Operation          | Name     | E-Rows | A-Rows | Buffers |
|  3 |  INDEX RANGE SCAN  | ORD_IDX  |      5 |  98765 |  12,340 |
```

`E-Rows=5` vs `A-Rows=98765` is a massive cardinality underestimate. The optimizer built the rest of the plan assuming 5 rows, but got 98,765 — leading to a nested loop join that became catastrophically slow at scale.

**Fix options:**
```sql
-- 1. Regather statistics with histogram
EXEC DBMS_STATS.GATHER_TABLE_STATS('SCHEMA', 'ORDERS',
    method_opt => 'FOR COLUMNS status SIZE 254');

-- 2. Add a dynamic sampling hint
SELECT /*+ DYNAMIC_SAMPLING(o 4) */ * FROM orders o WHERE o.status = 'PENDING';

-- 3. Use extended statistics for correlated columns
SELECT DBMS_STATS.CREATE_EXTENDED_STATS('HR', 'EMPLOYEES',
    '(department_id, job_id)') FROM DUAL;
```

### Adaptive plans (Oracle 12c+)

Oracle 12c introduced adaptive plans — the optimizer can switch from a nested loop to a hash join mid-execution based on actual row counts observed at runtime.

```sql
-- Check if a plan was adaptive
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(format => 'ADAPTIVE'));
```

Lines marked `-` in the plan are operations that were considered but not used (the adaptive alternative).

---

## 4. Common Tuning Scenarios

### Scenario 1: Sudden slowdown on a previously fast query

**Symptoms:** A report that ran in 2 seconds now takes 4 minutes.

**Diagnosis steps:**
```sql
-- 1. Check if plan changed (plan hash value differs)
SELECT sql_id, plan_hash_value, executions, elapsed_time/executions avg_elapsed
FROM   v$sqlstats
WHERE  sql_text LIKE '%order_summary%'
ORDER BY last_active_time DESC;

-- 2. Compare old vs new plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_AWR('abc123xyz', plan_hash_value => 1234567890));
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_AWR('abc123xyz', plan_hash_value => 9876543210));

-- 3. Check statistics age
SELECT table_name, last_analyzed, num_rows, stale_stats
FROM   dba_tab_statistics
WHERE  table_name = 'ORDER_SUMMARY';
```

**Common root causes:**
- Statistics went stale after a bulk load changed data distribution
- A new index was created or dropped, changing the plan
- Bind variable peeking produced a suboptimal plan for the current bind value
- System statistics (CPU speed, I/O throughput) were regathered

**Fix:** Regather stats, or pin the old plan using SQL Plan Management (SPM):
```sql
-- Capture the good plan as a baseline
DECLARE
    v_plans PLS_INTEGER;
BEGIN
    v_plans := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(
        sql_id         => 'good_sql_id_here',
        plan_hash_value => 1234567890  -- the fast plan's hash
    );
    DBMS_OUTPUT.PUT_LINE('Plans loaded: ' || v_plans);
END;
/
```

### Scenario 2: PL/SQL loop running for hours

**Anti-pattern:**
```plsql
-- SLOW: row-by-row "slow-by-slow" processing
FOR rec IN (SELECT order_id FROM orders WHERE status = 'NEW') LOOP
    UPDATE order_items SET status = 'NEW' WHERE order_id = rec.order_id;
    COMMIT;
END LOOP;
```

**Optimized version using BULK COLLECT and FORALL:**
```plsql
DECLARE
    TYPE t_ids IS TABLE OF orders.order_id%TYPE;
    v_ids t_ids;
BEGIN
    SELECT order_id
    BULK COLLECT INTO v_ids
    FROM orders
    WHERE status = 'NEW';

    FORALL i IN 1..v_ids.COUNT SAVE EXCEPTIONS
        UPDATE order_items
        SET    status = 'NEW'
        WHERE  order_id = v_ids(i);

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated: ' || SQL%ROWCOUNT || ' rows');
EXCEPTION
    WHEN OTHERS THEN
        -- Handle FORALL exceptions
        FOR j IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('Error at index ' || SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                || ': ' || SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE));
        END LOOP;
        COMMIT;  -- commit successful rows
END;
/
```

**Performance difference:** FORALL reduces context switches between the PL/SQL and SQL engines from N (one per row) to 1, and sends a single array DML statement to the SQL engine. Typical speedup: 10x–100x.

---

## 5. Intermediate Interview Q&A

**Q: What is bind variable peeking and when does it cause problems?**

**A:** On the first hard parse of a SQL statement with bind variables, Oracle peeks at the current bind values to estimate cardinality. The resulting plan is cached and reused for all subsequent executions — even when different bind values would produce very different row counts. This is problematic for skewed data distributions (e.g., `status = 'ACTIVE'` returns 90% of rows; `status = 'DELETED'` returns 0.1%). The plan optimized for one value is wrong for the other.

**Fixes:**
- `DBMS_STATS.SET_TABLE_PREFS` with `METHOD_OPT => 'FOR COLUMNS status SIZE 254'` to build histograms
- Use `CURSOR_SHARING = EXACT` (default) and rely on adaptive cursor sharing (ACS) in 11g+
- Use the `BIND_AWARE` hint to force ACS evaluation immediately

**Q: What is the difference between `MERGE` and `INSERT ... ON DUPLICATE KEY`?**

**A:** Oracle uses `MERGE` (not MySQL's `ON DUPLICATE KEY UPDATE`). `MERGE` performs an upsert: it matches source rows to target rows and conditionally inserts or updates.

```sql
MERGE INTO target_table t
USING source_table s ON (t.id = s.id)
WHEN MATCHED THEN
    UPDATE SET t.value = s.value, t.updated_at = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (id, value, created_at)
    VALUES (s.id, s.value, SYSDATE);
```

**Q: How would you find the top 10 SQL statements consuming the most I/O right now?**

```sql
SELECT sql_id, sql_text, disk_reads, buffer_gets, executions,
       ROUND(disk_reads / NULLIF(executions, 0)) reads_per_exec
FROM   v$sqlstats
ORDER BY disk_reads DESC
FETCH FIRST 10 ROWS ONLY;
```
