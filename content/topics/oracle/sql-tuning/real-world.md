---
title: "SQL Tuning — Real World"
topic: oracle
subtopic: sql-tuning
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, sql-tuning, awr, ash, production, troubleshooting]
---

# SQL Tuning — Real World Patterns

## Pattern 1: Finding and Fixing Top SQL in Production

Standard workflow when the DBA gets a "database is slow" call:

```sql
-- Step 1: Find what's consuming resources right now (last 1 hour)
SELECT sql_id, 
       ROUND(elapsed_time/1000000/NULLIF(executions,0), 2) avg_sec,
       executions,
       ROUND(buffer_gets/NULLIF(executions,0)) avg_logical_reads,
       ROUND(disk_reads/NULLIF(executions,0)) avg_physical_reads,
       SUBSTR(sql_text, 1, 100) preview
FROM v$sql
WHERE last_active_time > SYSDATE - 1/24
  AND executions > 0
  AND parsing_schema_name NOT IN ('SYS', 'SYSTEM')
ORDER BY elapsed_time DESC
FETCH FIRST 20 ROWS ONLY;

-- Step 2: Get the actual plan for the top offender
SELECT * FROM TABLE(
  DBMS_XPLAN.DISPLAY_CURSOR('bad_sql_id_here', 0, 'ALLSTATS LAST +PEEKED_BINDS')
);

-- Step 3: Check if stats are stale
SELECT table_name, last_analyzed, num_rows, blocks
FROM dba_tables
WHERE owner = 'APP_SCHEMA'
  AND table_name IN ('ORDERS', 'CUSTOMERS')
ORDER BY last_analyzed;

-- Step 4: Regather stats if stale (>24 hours old for hot tables)
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP_SCHEMA', 'ORDERS', 
  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
  degree => 8);

-- Step 5: Flush the bad cursor (force re-parse with new stats)
-- First, find the address/hash_value for DBMS_SHARED_POOL.PURGE
SELECT address, hash_value FROM v$sqlarea WHERE sql_id = 'bad_sql_id_here';
EXEC DBMS_SHARED_POOL.PURGE('address,hash_value', 'C');

-- Step 6: Re-run the query and check the new plan
```

---

## Pattern 2: AWR-Based Tuning (Proactive)

Use AWR reports to find performance trends before they become incidents:

```sql
-- Get recent AWR snapshots
SELECT snap_id, begin_interval_time, end_interval_time
FROM dba_hist_snapshot
WHERE begin_interval_time > SYSDATE - 1
ORDER BY snap_id DESC;

-- Find top SQL from a specific AWR period (snap_id 100 to 110)
SELECT s.sql_id, 
       ROUND(SUM(s.elapsed_time_delta)/1000000) total_elapsed_sec,
       SUM(s.executions_delta) total_executions,
       ROUND(SUM(s.elapsed_time_delta)/1000000/NULLIF(SUM(s.executions_delta),0), 3) avg_sec,
       ROUND(SUM(s.buffer_gets_delta)/NULLIF(SUM(s.executions_delta),0)) avg_gets,
       t.sql_text
FROM dba_hist_sqlstat s
JOIN dba_hist_sqltext t ON s.sql_id = t.sql_id AND s.dbid = t.dbid
WHERE s.snap_id BETWEEN 100 AND 110
  AND s.dbid = (SELECT dbid FROM v$database)
  AND s.parsing_schema_name NOT IN ('SYS', 'SYSTEM')
GROUP BY s.sql_id, t.sql_text
ORDER BY total_elapsed_sec DESC
FETCH FIRST 10 ROWS ONLY;

-- Generate an HTML AWR report programmatically
SELECT DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(
  l_dbid    => (SELECT dbid FROM v$database),
  l_inst_num => 1,
  l_bid     => 100,  -- begin snap_id
  l_eid     => 110   -- end snap_id
) FROM DUAL;
```

---

## Pattern 3: ASH (Active Session History) — Real-Time Diagnosis

```sql
-- What was happening during a 2-minute spike (between 14:00 and 14:02)?
SELECT event, COUNT(*) ash_samples,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) pct
FROM v$active_session_history
WHERE sample_time BETWEEN 
      TO_TIMESTAMP('2024-01-15 14:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
      TO_TIMESTAMP('2024-01-15 14:02:00', 'YYYY-MM-DD HH24:MI:SS')
  AND session_state = 'WAITING'
GROUP BY event
ORDER BY ash_samples DESC;

-- Which SQLs were active during the spike?
SELECT sql_id, COUNT(*) ash_samples,
       SUBSTR(q.sql_text, 1, 100) preview
FROM v$active_session_history ash
JOIN v$sql q ON ash.sql_id = q.sql_id
WHERE ash.sample_time BETWEEN 
      TO_TIMESTAMP('2024-01-15 14:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
      TO_TIMESTAMP('2024-01-15 14:02:00', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY ash.sql_id, q.sql_text
ORDER BY ash_samples DESC
FETCH FIRST 10 ROWS ONLY;

-- Historical ASH (from AWR — goes further back)
SELECT event, COUNT(*) ash_samples
FROM dba_hist_active_sess_history
WHERE sample_time > SYSDATE - 7
  AND session_state = 'WAITING'
  AND event NOT IN ('SQL*Net message from client')
GROUP BY event
ORDER BY ash_samples DESC
FETCH FIRST 20 ROWS ONLY;
```

---

## Pattern 4: Automated Nightly SQL Tuning Job

```sql
-- Create a SQL Tuning Set (STS) capturing top SQL every night
BEGIN
  -- Create STS
  DBMS_SQLTUNE.CREATE_SQLSET(
    sqlset_name => 'NIGHTLY_TOP_SQL',
    description => 'Top 50 SQL by elapsed time, captured nightly'
  );
END;
/

-- Populate the STS from cursor cache
BEGIN
  DBMS_SQLTUNE.LOAD_SQLSET(
    sqlset_name      => 'NIGHTLY_TOP_SQL',
    populate_cursor  => DBMS_SQLTUNE.SELECT_CURSOR_CACHE(
      'elapsed_time > 5000000',  -- > 5 seconds
      NULL, NULL, NULL, NULL,
      50,                         -- top 50
      'elapsed_time DESC'
    )
  );
END;
/

-- Run tuning advisor on the STS
DECLARE
  task_name VARCHAR2(50);
BEGIN
  task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
    sqlset_name  => 'NIGHTLY_TOP_SQL',
    scope        => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
    time_limit   => 3600,  -- 1 hour
    task_name    => 'NIGHTLY_TUNE_' || TO_CHAR(SYSDATE, 'YYYYMMDD')
  );
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name);
END;
/
```

---

## Common SQL Anti-Patterns and Fixes

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `SELECT *` on wide tables | Transfers unused columns, prevents index-only scans | Select only needed columns |
| `NOT IN (subquery)` | NULL values in subquery cause no rows returned | Use `NOT EXISTS` instead |
| Function on indexed column in WHERE | Index cannot be used | Rewrite using range predicates |
| Implicit type conversion | Column index skipped if type mismatch | Ensure bind variable / literal matches column type |
| `DISTINCT` unnecessarily | Forces sort operation | Use proper join conditions instead |
| Row-by-row processing in PL/SQL | Context switching overhead | Use BULK COLLECT + FORALL |
| Cursor FOR LOOP without LIMIT | Fetches one row at a time | Use BULK COLLECT with LIMIT 1000 |
| Nested correlated subqueries | Executes subquery once per outer row | Rewrite as JOIN or analytic function |

---

## Interview Tips

> **Tip 1:** "Walk me through how you'd diagnose a sudden performance degradation at 2pm." — Start with ASH: query `v$active_session_history` for that time window to see wait events and top SQL. Then get the plan for top SQL via DISPLAY_CURSOR with ALLSTATS. Check if stats changed (DBA_TAB_STATISTICS.LAST_ANALYZED changed recently). Check AWR to see if this is a recurring pattern or a one-time spike.

> **Tip 2:** "What's the difference between AWR and ASH?" — AWR (Automatic Workload Repository) stores hourly snapshots of aggregated performance statistics — good for trend analysis and identifying consistently slow SQL over time. ASH (Active Session History) samples active sessions every second — good for pinpointing what happened during a specific short incident. AWR = trends; ASH = drill-down.

> **Tip 3:** "How do you find SQL that started running slower after a stats gather?" — Compare AWR snapshots: query `dba_hist_sqlstat` for the SQL across snap periods before and after the stats gather. Compare avg_elapsed_time. If it worsened, the new stats produced a worse plan. Fix by restoring stats (`DBMS_STATS.RESTORE_TABLE_STATS`) or locking stats and applying a SQL Profile.
