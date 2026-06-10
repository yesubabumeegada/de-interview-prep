---
title: "AWR Reports — Scenarios"
topic: oracle
subtopic: awr-reports
content_type: scenario_question
tags: [oracle, awr, interview, scenarios, performance-investigation, wait-events]
---

# AWR Reports — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Interpret an AWR Report

**Scenario:** You're shown this AWR Top 5 Events section. What does it tell you and what do you do next?

```
Top 5 Timed Foreground Events:
Event                         Waits       Time(s)   Avg(ms)  %DB Time
enq: TX - row lock contention 12,450      8,940     718.4    52.3%
CPU time                                  3,200               18.7%
db file sequential read       2.1M        2,400     1.1       14.0%
log file sync                 85,000      1,200     14.1      7.0%
db file scattered read        120,000     800       6.7       4.7%
```

<details>
<summary>💡 Hint</summary>

**Reading the report:** 1. **52% of DB time in row lock contention** — this is a blocking issue, not a performance issue. Sessions are waiting for other sessions to release row locks. Average 718ms per wait = sessions are blocked for over half a second each time.

</details>

<details>
<summary>✅ Solution</summary>

**Reading the report:**
1. **52% of DB time in row lock contention** — this is a blocking issue, not a performance issue. Sessions are waiting for other sessions to release row locks. Average 718ms per wait = sessions are blocked for over half a second each time.

2. **14% CPU time** — normal background CPU usage.

3. **db file sequential read at 14%** — 2.1M single-block reads at 1.1ms average = index reads, performance is fine (< 2ms is acceptable).

4. **log file sync at 7% with 14ms average** — slightly high. Target < 5ms. Might have slow redo log storage.

**Primary issue: Row lock contention. Steps to investigate:**

```sql
-- Step 1: Find what's holding the locks (ASH)
SELECT ash.blocking_session, ash.sql_id, 
       SUBSTR(q.sql_text, 1, 100) blocking_sql,
       COUNT(*) blocked_sessions
FROM v$active_session_history ash
JOIN v$sql q ON ash.blocking_sql_id = q.sql_id
WHERE ash.event = 'enq: TX - row lock contention'
  AND ash.sample_time > SYSDATE - 1/24
GROUP BY ash.blocking_session, ash.sql_id, q.sql_text
ORDER BY blocked_sessions DESC;

-- Step 2: Identify the table involved
SELECT o.object_name, o.object_type
FROM v$lock l
JOIN dba_objects o ON l.id1 = o.object_id
WHERE l.type = 'TM'
ORDER BY lmode DESC;
```

**Root cause options:**
- Long-running UPDATE without COMMIT → fix: commit more frequently in batches
- Deadlock-prone application logic → fix: access tables in consistent order
- Forgotten transaction (application crashed while holding lock) → fix: kill the session

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Performance Degraded After Stats Gather

**Scenario:** AWR shows query 'abc123xyz' went from 0.5 seconds average to 8 seconds average after last night's statistics gather job. How do you diagnose and fix this?

<details>
<summary>💡 Hint</summary>

The new statistics changed the optimizer's cost estimates, causing it to choose a different (worse) execution plan. Pull both plans from AWR — the plan used before last night's snapshot and the current plan. Use `DBA_HIST_SQL_PLAN` to compare `plan_hash_value` before and after. Common cause: the stats gather removed a histogram on a skewed column, making the optimizer underestimate rows and choose NESTED LOOPS over HASH JOIN. Fix: restore old stats with `DBMS_STATS.RESTORE_TABLE_STATS`, or create an SPM baseline to pin the old plan.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Compare execution plans (before vs after)**
```sql
-- Get plan from AWR (before stats gather — snap_id 100)
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_AWR('abc123xyz', NULL, NULL, 'TYPICAL'));
-- Note the plan_hash_value

-- Compare with current plan from cursor cache
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('abc123xyz', 0, 'ALLSTATS LAST'));
-- Different plan_hash_value → plan changed
```

**Step 2: Check what changed in the plan**
```sql
-- AWR historical plan vs current plan
SELECT plan_hash_value, timestamp, operation, options, object_name, cost, cardinality
FROM dba_hist_sql_plan
WHERE sql_id = 'abc123xyz'
ORDER BY plan_hash_value, id;
-- Compare: old plan used INDEX RANGE SCAN; new plan uses TABLE ACCESS FULL
```

**Step 3: Check if statistics changed for the key table**
```sql
SELECT table_name, last_analyzed, num_rows, blocks,
       to_char(last_analyzed, 'YYYY-MM-DD HH24:MI') analyzed_at
FROM dba_tables
WHERE table_name IN (
  SELECT object_name FROM dba_hist_sql_plan WHERE sql_id = 'abc123xyz'
);
-- last_analyzed = today's date → stats gathered last night
-- num_rows may have changed dramatically (if data grew/shrank significantly)
```

**Fix Option A: Restore old statistics (immediate)**
```sql
-- Find the stats timestamp just before last night's gather
SELECT h.savtime, h.stattype_locked
FROM dba_tab_stats_history h
WHERE h.table_name = 'ORDERS'
ORDER BY h.savtime DESC;

-- Restore to previous stats
EXEC DBMS_STATS.RESTORE_TABLE_STATS(
  ownname => 'APP_SCHEMA',
  tabname => 'ORDERS',
  as_of_timestamp => TO_TIMESTAMP('2024-01-14 23:00:00', 'YYYY-MM-DD HH24:MI:SS')
);
-- Flushes the bad cursor; query should use old plan
```

**Fix Option B: Apply SQL Profile (permanent)**
```sql
-- Have SQL Tuning Advisor find the right plan and create a profile
DECLARE
  task_name VARCHAR2(30);
BEGIN
  task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(sql_id => 'abc123xyz');
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name);
  DBMS_SQLTUNE.ACCEPT_SQL_PROFILE(task_name => task_name, force_match => TRUE);
END;
/
-- Profile corrects cardinality estimates without touching the SQL text
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build an AWR-Based Performance Monitoring System

**Scenario:** Management wants a weekly performance scorecard showing: top queries, resource trends, and whether performance is degrading week-over-week. How do you build this?

<details>
<summary>💡 Hint</summary>

The raw material is all in AWR: `DBA_HIST_SQLSTAT` for query-level metrics, `DBA_HIST_SYSSTAT` for instance-level throughput trends, and `DBA_HIST_SNAPSHOT` to define the week boundaries. The architecture is: nightly extraction job that reads last week's AWR into a reporting table, a weekly comparison query that pairs each metric against prior-week average, and a dashboard layer (APEX, Grafana, or a static HTML report) for the scorecard. Key metrics: top queries by elapsed time and CPU, week-over-week change in DB time, and session wait event distribution shift.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**
```
Oracle AWR (DBA_HIST_* tables)
    │
    ▼
nightly ETL procedure → performance_scorecard table
    │
    ▼
Weekly email report (HTML)
    OR
BI tool dashboard (connected to performance_scorecard)
```

**Implementation:**

```sql
-- Scorecard table
CREATE TABLE perf_scorecard (
  report_week     DATE,
  metric_name     VARCHAR2(100),
  metric_value    NUMBER,
  metric_unit     VARCHAR2(50),
  week_over_week_pct NUMBER,
  status          VARCHAR2(10)  -- GREEN / YELLOW / RED
);

-- ETL procedure to populate weekly scorecard
CREATE OR REPLACE PROCEDURE populate_weekly_scorecard(
  p_week_start IN DATE DEFAULT TRUNC(SYSDATE, 'IW') - 7  -- last week
) IS
  v_week_end DATE := p_week_start + 7;
  v_snap_start NUMBER;
  v_snap_end   NUMBER;
  v_prev_value NUMBER;
  v_curr_value NUMBER;
  
  PROCEDURE insert_metric(p_name VARCHAR2, p_val NUMBER, p_unit VARCHAR2, p_prev NUMBER) IS
    v_wow NUMBER;
    v_status VARCHAR2(10);
  BEGIN
    v_wow := ROUND((p_val - p_prev) * 100.0 / NULLIF(p_prev, 0), 1);
    v_status := CASE
      WHEN ABS(v_wow) < 10 THEN 'GREEN'
      WHEN ABS(v_wow) < 30 THEN 'YELLOW'
      ELSE 'RED'
    END;
    INSERT INTO perf_scorecard VALUES (p_week_start, p_name, p_val, p_unit, v_wow, v_status);
  END;
BEGIN
  SELECT MIN(snap_id), MAX(snap_id) INTO v_snap_start, v_snap_end
  FROM dba_hist_snapshot
  WHERE begin_interval_time BETWEEN p_week_start AND v_week_end;
  
  -- Metric 1: Average hourly DB time
  SELECT ROUND(SUM(value) / 1e6 / COUNT(DISTINCT snap_id), 1)
  INTO v_curr_value
  FROM dba_hist_sys_time_model
  WHERE snap_id BETWEEN v_snap_start AND v_snap_end
    AND stat_name = 'DB time';
  
  SELECT ROUND(SUM(value) / 1e6 / COUNT(DISTINCT snap_id), 1) INTO v_prev_value
  FROM dba_hist_sys_time_model
  WHERE snap_id IN (
    SELECT snap_id FROM dba_hist_snapshot 
    WHERE begin_interval_time BETWEEN p_week_start - 7 AND p_week_start
  ) AND stat_name = 'DB time';
  
  insert_metric('avg_dbtime_per_hour_sec', v_curr_value, 'seconds', v_prev_value);
  
  -- Add more metrics: physical reads, redo size, top SQL count, hard parse rate...
  
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Scorecard populated for week: ' || TO_CHAR(p_week_start, 'YYYY-MM-DD'));
END populate_weekly_scorecard;
/

-- Schedule weekly
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name => 'WEEKLY_SCORECARD_JOB',
    job_type => 'STORED_PROCEDURE',
    job_action => 'POPULATE_WEEKLY_SCORECARD',
    repeat_interval => 'FREQ=WEEKLY; BYDAY=MON; BYHOUR=6',
    enabled => TRUE
  );
END;
/
```

**Key metrics to track in the scorecard:**

| Metric | Green | Yellow | Red |
|

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is an AWR report and what information does it contain?**
A: An Automatic Workload Repository (AWR) report is a performance snapshot comparison between two points in time. It contains database statistics including wait events, top SQL by elapsed time/CPU/reads, load profile, instance efficiency percentages, memory statistics, and I/O metrics.

**Q: How do you generate an AWR report?**
A: Run `@$ORACLE_HOME/rdbms/admin/awrrpt.sql` in SQL*Plus and specify HTML or text format, the number of days to display snapshots, and the begin/end snapshot IDs. For a specific RAC instance use `awrrpti.sql`; for a comparison report use `awrddrpt.sql`.

**Q: What is a snapshot in AWR and how often are they taken?**
A: A snapshot is a point-in-time capture of Oracle performance statistics stored in the `SYSAUX` tablespace. By default snapshots are taken every 60 minutes and retained for 8 days. Both intervals are configurable via `DBMS_WORKLOAD_REPOSITORY.MODIFY_SNAPSHOT_SETTINGS`.

**Q: What does the "DB Time" metric in AWR represent?**
A: DB Time is the total time the database spent on user calls (CPU + wait time) across all sessions during the snapshot interval. It is the primary workload indicator—high DB Time with low throughput points to contention or inefficient SQL.

**Q: How do you identify the top SQL statements from an AWR report?**
A: The "SQL Statistics" section ranks SQL by elapsed time, CPU time, buffer gets, disk reads, and executions. Start with "SQL ordered by Elapsed Time" to find the most expensive statements, then drill into the SQL details for execution plans and bind variable information.

**Q: What are the most important wait events to look for in an AWR report?**
A: `db file sequential read` (single-block I/O—index scan on slow disk), `log file sync` (commit latency—I/O or redo log contention), `latch: shared pool` (parse contention), `enq: TX - row lock contention` (blocking locks), and `direct path read` (full table scans hitting disk).

**Q: What is the difference between AWR and Statspack?**
A: Statspack is the older, free predecessor to AWR; AWR requires the Diagnostics Pack license. AWR stores data in `SYSAUX`, has finer granularity, integrates with ADDM/ASH, and is queryable via `DBA_HIST_*` views. Statspack uses the `PERFSTAT` schema and lacks ASH integration.

**Q: How does ADDM use AWR data?**
A: The Automatic Database Diagnostic Monitor (ADDM) runs automatically after each AWR snapshot, analyzing the delta to identify the top performance bottlenecks (SQL, I/O, memory, contention) and generates prioritized findings with recommendations accessible via `DBA_ADVISOR_FINDINGS`.

---

## 💼 Interview Tips

- When asked to diagnose a slow database, walk through AWR sections in order: Load Profile → Instance Efficiency → Top Wait Events → Top SQL. This structured approach impresses interviewers.
- Know the licensing caveat: AWR and ADDM require the Diagnostics Pack. Mentioning this shows you understand real-world procurement constraints.
- Senior interviewers often provide a wait event and ask what it means. Memorize the top 10 wait events and their root causes—`log file sync` (commit/redo I/O) and `db file sequential read` (index scan) come up most.
- Demonstrate you can correlate sections: high `db file sequential read` + a specific SQL in "SQL ordered by Disk Reads" + an index missing from the execution plan tells a complete story.
- Mention ASH (Active Session History) as the complement to AWR for real-time and recent analysis—`V$ACTIVE_SESSION_HISTORY` vs. `DBA_HIST_ACTIVE_SESS_HISTORY` for historical data.
