---
title: "AWR Reports — Intermediate"
topic: oracle
subtopic: awr-reports
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, awr, wait-events, top-sql, addm, baseline]
---

# AWR Reports — Intermediate

## Reading AWR Wait Event Analysis

Understanding wait event categories:

```
Wait Event Categories:
├── User I/O
│   ├── db file sequential read    → single-block I/O (index reads)
│   ├── db file scattered read     → multi-block I/O (full scans)
│   └── db file parallel read      → parallel recovery/backup
├── System I/O
│   ├── log file parallel write    → LGWR writing redo
│   └── control file sequential read
├── Commit
│   └── log file sync              → session waiting for LGWR (COMMIT wait)
├── Concurrency
│   ├── enq: TX - row lock contention → application-level row locking
│   ├── enq: TM - contention    → DML lock on table
│   └── library cache lock/pin  → shared pool invalidation
├── Application
│   └── SQL*Net message from client → network round-trip
└── Idle
    └── SQL*Net message from client (when no work) → background noise
```

```sql
-- AWR wait event trend over time
SELECT s.begin_interval_time,
       e.event_name,
       e.total_waits_fg,
       ROUND(e.time_waited_fg / 100, 2) time_sec,
       ROUND(e.time_waited_fg / NULLIF(e.total_waits_fg, 0) / 100, 4) avg_wait_sec
FROM dba_hist_system_event e
JOIN dba_hist_snapshot s ON e.snap_id = s.snap_id
WHERE e.event_name IN ('db file sequential read', 'log file sync', 'enq: TX - row lock contention')
  AND s.begin_interval_time > SYSDATE - 7
ORDER BY s.begin_interval_time DESC, e.time_waited_fg DESC;
```

---

## ADDM — Automatic Database Diagnostic Monitor

ADDM automatically analyzes AWR data and provides recommendations after each snapshot:

```sql
-- View ADDM findings for a specific snapshot pair
SELECT finding_name, type, message, benefit, impact_pct
FROM dba_advisor_findings
WHERE task_name IN (
  SELECT task_name FROM dba_advisor_tasks
  WHERE advisor_name = 'ADDM'
  ORDER BY created DESC
  FETCH FIRST 5 ROWS ONLY
)
ORDER BY impact_pct DESC;

-- Sample ADDM findings (what you'll see):
-- "SQL statements consuming significant database time" → top SQL
-- "Individual SQL statements responsible for significant I/O activity"
-- "Library cache miss resulting in additional SQL parsing"
-- "Hard parse consuming significant DB time" → bind variables needed

-- Run ADDM on-demand for a specific time range
DECLARE
  task_name VARCHAR2(30);
  task_desc VARCHAR2(30);
BEGIN
  task_name := 'MANUAL_ADDM_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MI');
  DBMS_ADVISOR.CREATE_TASK('ADDM', task_name, task_desc, task_name);
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'START_SNAPSHOT', 100);
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'END_SNAPSHOT', 110);
  DBMS_ADVISOR.EXECUTE_TASK(task_name);
  DBMS_OUTPUT.PUT_LINE('ADDM task created: ' || task_name);
END;
/

-- View the ADDM report
SELECT DBMS_ADVISOR.GET_TASK_REPORT(task_name => 'MANUAL_ADDM_202401151400') 
FROM DUAL;
```

---

## AWR Baselines

Baselines capture a "normal" period's statistics — compare current performance against the baseline:

```sql
-- Create a baseline for "Monday morning peak" (best week in January)
EXEC DBMS_WORKLOAD_REPOSITORY.CREATE_BASELINE(
  start_snap_id => 100,
  end_snap_id   => 120,
  baseline_name => 'MONDAY_PEAK_NORMAL'
);

-- Create a moving window baseline (auto-maintained rolling average)
EXEC DBMS_WORKLOAD_REPOSITORY.CREATE_MOVING_WINDOW_BASELINE(
  moving_window_size => 30,  -- 30 days rolling window
  baseline_name      => 'ROLLING_30DAY'
);

-- View all baselines
SELECT baseline_name, baseline_type, start_snap_id, end_snap_id, 
       start_snap_time, end_snap_time
FROM dba_hist_baseline
ORDER BY creation_time DESC;

-- Compare current AWR period to a baseline in the AWR Comparison Report
SELECT * FROM TABLE(
  DBMS_WORKLOAD_REPOSITORY.AWR_DIFF_REPORT_TEXT(
    dbid1      => (SELECT dbid FROM v$database),
    inst_num1  => 1,
    bid1       => 100,   -- baseline period
    eid1       => 120,
    dbid2      => (SELECT dbid FROM v$database),
    inst_num2  => 1,
    bid2       => 200,   -- current period
    eid2       => 220
  )
);
```

---

## SQL Analysis from AWR

```sql
-- Top SQL by elapsed time from AWR (useful when the query is no longer in cursor cache)
SELECT s.sql_id,
       SUM(s.elapsed_time_delta) / 1e6 total_elapsed_sec,
       SUM(s.executions_delta) executions,
       ROUND(SUM(s.elapsed_time_delta) / 1e6 / NULLIF(SUM(s.executions_delta), 0), 3) avg_sec,
       ROUND(SUM(s.buffer_gets_delta) / NULLIF(SUM(s.executions_delta), 0)) avg_logical_reads,
       ROUND(SUM(s.disk_reads_delta) / NULLIF(SUM(s.executions_delta), 0)) avg_phys_reads,
       t.sql_text
FROM dba_hist_sqlstat s
JOIN dba_hist_sqltext t ON s.sql_id = t.sql_id AND s.dbid = t.dbid
WHERE s.snap_id BETWEEN 100 AND 110
  AND s.parsing_schema_name NOT IN ('SYS', 'SYSTEM', 'DBSNMP')
GROUP BY s.sql_id, t.sql_text
ORDER BY total_elapsed_sec DESC
FETCH FIRST 10 ROWS ONLY;

-- Get execution plan from AWR (plan that was used in that snapshot period)
SELECT * FROM TABLE(
  DBMS_XPLAN.DISPLAY_AWR('sql_id_here', format => 'TYPICAL +PEEKED_BINDS')
);
```

---

## System Statistics from AWR

```sql
-- I/O throughput trend
SELECT s.begin_interval_time,
       ROUND(SUM(CASE WHEN stat_name = 'physical read bytes' THEN value END) / 1e9) read_gb,
       ROUND(SUM(CASE WHEN stat_name = 'physical write bytes' THEN value END) / 1e9) write_gb
FROM dba_hist_sysstat st
JOIN dba_hist_snapshot s ON st.snap_id = s.snap_id
WHERE stat_name IN ('physical read bytes', 'physical write bytes')
  AND s.begin_interval_time > SYSDATE - 7
GROUP BY s.begin_interval_time
ORDER BY s.begin_interval_time DESC;

-- Parse activity trend (hard parse = bad; should be < 1% of soft parse)
SELECT s.begin_interval_time,
       SUM(CASE WHEN stat_name = 'parse count (hard)' THEN value END) hard_parses,
       SUM(CASE WHEN stat_name = 'parse count (total)' THEN value END) total_parses,
       ROUND(
         SUM(CASE WHEN stat_name='parse count (hard)' THEN value END) * 100.0 /
         NULLIF(SUM(CASE WHEN stat_name='parse count (total)' THEN value END), 0), 2
       ) hard_parse_pct
FROM dba_hist_sysstat st
JOIN dba_hist_snapshot s ON st.snap_id = s.snap_id
WHERE stat_name IN ('parse count (hard)', 'parse count (total)')
  AND s.begin_interval_time > SYSDATE - 3
GROUP BY s.begin_interval_time
ORDER BY s.begin_interval_time DESC;
```

---

## Interview Tips

> **Tip 1:** "How do you interpret 'log file sync' as a top wait event in AWR?" — `log file sync` means user sessions waited for the LGWR process to write their committed redo to the online redo log. Average wait > 5ms indicates an I/O problem for the redo log devices (use faster storage or move redo to flash). If wait count is extremely high but avg wait is < 1ms, the issue is too many small commits (batching needed). If on Exadata: Smart Flash Log should keep this < 1ms.

> **Tip 2:** "A report shows SQL 'abc123' ran 50,000 times in an hour with avg 200ms each. Is that a problem?" — 50,000 × 0.2s = 10,000 seconds of DB time in 1 hour. If the database has 8 CPUs, total available CPU = 8 × 3,600 = 28,800 CPU-seconds. This one SQL consumes 35% of all CPU capacity — definitely a problem. Even if per-execution it looks fast (200ms), the volume makes it the top consumer. Fixes: reduce executions (caching, better application logic), or reduce per-execution cost (indexes, better SQL).

> **Tip 3:** "What is ADDM and how does it differ from manually reading AWR?" — ADDM (Automatic Database Diagnostic Monitor) runs automatically after each AWR snapshot and analyzes the data using its built-in diagnostic rules. It provides prioritized findings with impact percentages and specific recommendations (e.g., "create this index," "increase SGA," "application is missing bind variables"). Reading AWR manually requires DBA expertise to interpret the raw metrics. ADDM is the first-pass diagnostic; use manual AWR analysis to validate ADDM findings or investigate scenarios ADDM didn't flag.
