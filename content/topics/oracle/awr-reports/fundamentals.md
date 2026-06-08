---
title: "AWR Reports — Fundamentals"
topic: oracle
subtopic: awr-reports
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, awr, performance, snapshots, ash, statspack]
---

# AWR Reports — Fundamentals

## What Is AWR?

The Automatic Workload Repository (AWR) automatically collects and stores performance statistics for the Oracle database. It takes snapshots of V$ views every 60 minutes (configurable), stores them in `SYSAUX`, and provides historical performance data for analysis.

**Why AWR matters:**
- Identifies top SQL, wait events, and resource bottlenecks
- Compares performance between time periods (before/after a change)
- Enables proactive capacity planning
- Essential for diagnosing production performance issues

**License note:** AWR requires the Oracle Diagnostics Pack license (part of Oracle Database Enterprise Edition options).

---

## AWR Snapshots

```sql
-- View AWR snapshots
SELECT snap_id, instance_number,
       begin_interval_time, end_interval_time,
       ROUND((end_interval_time - begin_interval_time) * 24 * 60, 1) interval_min
FROM dba_hist_snapshot
WHERE begin_interval_time > SYSDATE - 2  -- last 2 days
ORDER BY snap_id DESC
FETCH FIRST 20 ROWS ONLY;

-- Manual snapshot (capture current state immediately)
EXEC DBMS_WORKLOAD_REPOSITORY.CREATE_SNAPSHOT;

-- Configure snapshot interval and retention
EXEC DBMS_WORKLOAD_REPOSITORY.MODIFY_SNAPSHOT_SETTINGS(
  interval => 30,    -- take snapshot every 30 minutes (default: 60)
  retention => 30 * 24 * 60  -- retain for 30 days (default: 8 days, in minutes)
);

-- Check current settings
SELECT snap_interval, retention, most_recent_snap_id
FROM dba_hist_wr_control;
```

---

## Generating AWR Reports

```sql
-- HTML report (most readable — open in browser)
SELECT DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(
  l_dbid    => (SELECT dbid FROM v$database),
  l_inst_num => 1,
  l_bid     => 100,  -- beginning snap_id
  l_eid     => 110   -- ending snap_id
) AS report
FROM DUAL;

-- Text report (for scripting/automation)
SELECT * FROM TABLE(
  DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_TEXT(
    l_dbid     => (SELECT dbid FROM v$database),
    l_inst_num => 1,
    l_bid      => 100,
    l_eid      => 110
  )
);

-- SQL-specific AWR report (deep dive on one SQL)
SELECT DBMS_WORKLOAD_REPOSITORY.AWR_SQL_REPORT_HTML(
  l_dbid     => (SELECT dbid FROM v$database),
  l_inst_num => 1,
  l_bid      => 100,
  l_eid      => 110,
  l_sqlid    => 'abc123xyz'
) FROM DUAL;
```

---

## Key Sections of an AWR Report

### 1. DB Time and Instance Efficiency

```
DB Time = total time all sessions spent in DB (CPU + wait time)
If DB Time >> elapsed wall clock time × CPU count → overloaded
If DB Time << elapsed wall clock time × CPU count → underloaded / mostly idle

Instance Efficiency Percentages:
  Buffer Cache Hit %:    98.5%  (target: >95%)
  Library Cache Hit %:   99.2%  (target: >95%)
  Soft Parse %:          97.8%  (target: >85% — means bind variables are being reused)
  Latch Hit %:           99.9%  (target: >99%)
```

### 2. Top 5 Timed Events

Most critical section — shows where the database spent its time:

```
Top 5 Timed Foreground Events:
Event                        Waits    Time(s)   Avg Wait(ms)  %DB Time
db file sequential read      1.2M     3,400     2.8           28.5%  ← index scan I/O
CPU time                      -       2,800     -             23.5%  ← pure processing
log file sync                210K     1,800     8.6           15.1%  ← commit waits
db file scattered read       85K      900       10.6          7.5%   ← full scan I/O
db file parallel read        5K       400       80.0          3.4%   ← table space recovery
```

**Reading the top waits:**
- `CPU time` in top: heavy computation — check for full scans, poor algorithms
- `db file sequential read` high: many index range scans or single-block reads
- `log file sync` high: too many COMMITs (batching needed) or slow storage
- `library cache lock/pin`: recompilation of shared objects; hard parse storms

### 3. Top SQL by Various Metrics

```
SQL ordered by Elapsed Time:
SQL_ID        Elapsed/sec  Executions  Avg/sec  SQL Text
abc123xyz     3,450        25          138.0    SELECT * FROM orders WHERE ...
def456uvw     2,100        1200        1.75     UPDATE accounts SET balance...
```

---

## ASH — Active Session History

ASH samples every active session every 1 second — perfect for drilling into specific time windows:

```sql
-- What was consuming time during a 5-minute window?
SELECT event, COUNT(*) samples,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) pct_time
FROM v$active_session_history
WHERE sample_time BETWEEN 
      TO_TIMESTAMP('2024-01-15 14:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
      TO_TIMESTAMP('2024-01-15 14:05:00', 'YYYY-MM-DD HH24:MI:SS')
  AND session_state = 'WAITING'
GROUP BY event
ORDER BY samples DESC;

-- ASH report (pre-built)
SELECT * FROM TABLE(
  DBMS_WORKLOAD_REPOSITORY.ASH_REPORT_TEXT(
    l_btime => TO_TIMESTAMP('2024-01-15 14:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    l_etime => TO_TIMESTAMP('2024-01-15 14:05:00', 'YYYY-MM-DD HH24:MI:SS')
  )
);
```

---

## Interview Tips

> **Tip 1:** "What is an AWR report and how do you use it for performance tuning?" — AWR captures hourly snapshots of all key performance metrics: wait events, top SQL, system statistics, memory usage. To use it: select a time range covering the performance issue (two snap_ids), generate the report, read Top 5 Timed Events first (tells you WHERE time is spent), then Top SQL (tells you WHAT caused it), then system statistics (confirms CPU, I/O, memory). It's the starting point for any proactive performance analysis.

> **Tip 2:** "What is the difference between AWR and ASH?" — AWR stores aggregated metrics at snapshot intervals (default: 1 hour) — great for trend analysis and comparing periods. ASH samples every active session every 1 second — great for drilling into a specific 5-minute performance incident. AWR = aggregate trends; ASH = real-time drill-down. AWR is in `dba_hist_*` views; ASH is in `v$active_session_history` (in-memory, last ~1 hour) and `dba_hist_active_sess_history` (persisted by AWR).

> **Tip 3:** "What does 'DB Time' mean in an AWR report?" — DB Time is the total time (CPU + wait time) that all user sessions spent doing database work during the AWR interval. If a 1-hour report shows 3,600 seconds of DB time on a 4-CPU system: 3,600 / 3,600 seconds / 4 CPUs = 25% utilization — lightly loaded. If DB Time = 50,000 seconds in a 1-hour window on 4 CPUs, that's 50,000/3,600/4 = 3.5× CPU utilization → severely bottlenecked.
