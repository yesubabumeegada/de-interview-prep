---
title: "AWR Reports — Senior Deep Dive"
topic: oracle
subtopic: awr-reports
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, awr, capacity-planning, time-model, sql-tuning-set, performance-hub]
---

# AWR Reports — Senior Deep Dive

## Time Model Statistics

The Time Model breaks down where DB Time is spent in a hierarchical tree:

```sql
-- Time model statistics from AWR
SELECT stat_name, value/1e6 seconds_total,
       ROUND(value * 100.0 / NULLIF(
         SUM(CASE WHEN stat_name = 'DB time' THEN value END) OVER (), 0
       ), 1) pct_db_time
FROM dba_hist_sys_time_model tm
JOIN dba_hist_snapshot sn ON tm.snap_id = sn.snap_id
WHERE sn.snap_id = 110
  AND sn.instance_number = 1
ORDER BY value DESC;

-- Output interpretation:
-- DB time                          → total time
--   sql execute elapsed time       → time in SQL execution
--     parse time elapsed           → time parsing SQL
--       hard parse elapsed time    → hard parses (bind variables missing)
--     background elapsed time      → LGWR, DBW, CKPT background work
--   PL/SQL execution elapsed time  → PL/SQL procedures
--   connection management call elapsed time → login/logout overhead
```

---

## Building a Performance Trending Dashboard

```sql
-- Weekly performance trend report (store in a reporting table)
CREATE TABLE awr_weekly_summary AS
SELECT
  TRUNC(s.begin_interval_time, 'IW') week_start,
  COUNT(DISTINCT s.snap_id) snap_count,
  
  -- DB Time per hour (normalize to 1-hour intervals)
  ROUND(SUM(tm.value) / 1e6 / COUNT(DISTINCT s.snap_id), 1) avg_dbtime_per_hour_sec,
  
  -- Physical I/O rates
  ROUND(SUM(CASE WHEN st.stat_name = 'physical read bytes' THEN st.value END) 
        / 1e9 / COUNT(DISTINCT s.snap_id), 2) avg_phys_read_gb_per_hour,
  
  -- Logical reads (buffer cache)
  ROUND(SUM(CASE WHEN st.stat_name = 'session logical reads' THEN st.value END)
        / 1e6 / COUNT(DISTINCT s.snap_id), 2) avg_logical_reads_m_per_hour,
  
  -- Redo generation rate
  ROUND(SUM(CASE WHEN st.stat_name = 'redo size' THEN st.value END)
        / 1e9 / COUNT(DISTINCT s.snap_id), 2) avg_redo_gb_per_hour

FROM dba_hist_snapshot s
JOIN dba_hist_sys_time_model tm ON s.snap_id = tm.snap_id 
  AND tm.stat_name = 'DB time'
JOIN dba_hist_sysstat st ON s.snap_id = st.snap_id
  AND st.stat_name IN ('physical read bytes', 'session logical reads', 'redo size')
WHERE s.begin_interval_time > SYSDATE - 90  -- last 90 days
GROUP BY TRUNC(s.begin_interval_time, 'IW')
ORDER BY week_start DESC;
```

---

## SQL Tuning Sets (STS) for Workload Capture

SQL Tuning Sets capture a workload snapshot — used for SPM baselines, SQL Access Advisor, and testing optimizer changes:

```sql
-- Create and populate an STS from AWR (capture top 100 SQL from last week)
BEGIN
  -- Create the STS
  DBMS_SQLTUNE.CREATE_SQLSET(
    sqlset_name => 'PROD_WORKLOAD_WK52',
    description => 'Top 100 SQL from production, week 52'
  );
  
  -- Load from AWR
  DBMS_SQLTUNE.LOAD_SQLSET(
    sqlset_name     => 'PROD_WORKLOAD_WK52',
    populate_cursor => DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY(
      begin_snap     => 100,
      end_snap       => 148,
      basic_filter   => 'elapsed_time > 1000000',  -- > 1 second
      attribute_list => 'ALL',
      ranking_measure1 => 'elapsed_time',
      result_limit   => 100
    )
  );
END;
/

-- Export STS to a staging table (for transfer to test environment)
BEGIN
  DBMS_SQLTUNE.CREATE_STGTAB_SQLSET(
    table_name  => 'STS_STAGE_WK52',
    schema_name => 'DBA_TOOLS'
  );
  
  DBMS_SQLTUNE.PACK_STGTAB_SQLSET(
    sqlset_name       => 'PROD_WORKLOAD_WK52',
    staging_table_name => 'STS_STAGE_WK52',
    staging_schema_owner => 'DBA_TOOLS'
  );
END;
/

-- Use STS to test optimizer changes (compare plans before/after a change)
-- On test environment: import STS and run SQL Tuning Advisor
DECLARE
  task_name VARCHAR2(30);
BEGIN
  task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
    sqlset_name => 'PROD_WORKLOAD_WK52',
    scope       => DBMS_SQLTUNE.SCOPE_COMPREHENSIVE,
    time_limit  => 7200  -- 2 hours
  );
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name);
END;
/
```

---

## Optimizer Statistics Advisor

```sql
-- Check the Statistics Advisor recommendations (19c+)
SELECT task_name, status, created, last_modified
FROM dba_advisor_tasks
WHERE advisor_name = 'Statistics Advisor'
ORDER BY created DESC;

-- View findings
SELECT rule_name, type, message, action
FROM dba_advisor_findings f
JOIN dba_advisor_tasks t ON f.task_id = t.task_id
WHERE t.advisor_name = 'Statistics Advisor'
ORDER BY type;
-- Common findings: "Table has no statistics", "Statistics are stale", 
-- "Missing histogram for high-cardinality skewed column"
```

---

## AWR Data Mining — Capacity Planning

```sql
-- Predict when you'll run out of tablespace based on growth rate
SELECT 
  ts.tablespace_name,
  ROUND(ts.bytes / 1e9, 2) current_gb,
  ROUND(ts.max_bytes / 1e9, 2) max_gb,
  ROUND((ts.max_bytes - ts.bytes) / 1e9, 2) free_gb,
  -- Growth rate from AWR segment history
  ROUND(
    (ts.bytes - ts_old.bytes) / GREATEST((SYSDATE - ts_old.snap_time), 1) 
    / 1e9, 3
  ) daily_growth_gb,
  -- Days until full (simple linear extrapolation)
  ROUND(
    (ts.max_bytes - ts.bytes) / 
    NULLIF((ts.bytes - ts_old.bytes) / GREATEST((SYSDATE - ts_old.snap_time), 1), 0)
  ) days_until_full
FROM (
  SELECT tablespace_name, SUM(bytes) bytes, SUM(maxbytes) max_bytes
  FROM dba_data_files GROUP BY tablespace_name
) ts
JOIN (
  SELECT tablespace_name, SUM(space_used_display) bytes, MIN(snap_time) snap_time
  FROM dba_hist_tbspc_space_usage hts
  JOIN dba_hist_snapshot s ON hts.snap_id = s.snap_id
  WHERE s.begin_interval_time BETWEEN SYSDATE - 30 AND SYSDATE - 28  -- 30 days ago
  GROUP BY tablespace_name
) ts_old ON ts.tablespace_name = ts_old.tablespace_name
ORDER BY days_until_full NULLS LAST;
```

---

## Oracle Performance Hub (Cloud Control / EM)

A graphical interface that combines AWR, ASH, and real-time monitoring:

```sql
-- Performance Hub is a UI feature in EM Express (included) or EM Cloud Control
-- Accessible via: https://host:5500/em  (EM Express, 12c+)

-- EM Express query (enable XDB dispatcher):
ALTER SYSTEM SET dispatchers = '(PROTOCOL=TCP)(SERVICE=XDBXDB)';
-- Access Performance Hub at https://server:5500/em

-- Key Performance Hub features:
-- 1. Activity chart: DB Time over time, color-coded by wait class
-- 2. Top SQL: drag time window to see top SQL for that specific period
-- 3. ASH Analytics: filter by SQL, module, user, wait class simultaneously
-- 4. Real Incidents: ADDM findings with impact %
```

---

## Interview Tips

> **Tip 1:** "How do you use AWR for capacity planning?" — AWR's `dba_hist_sysstat` provides historical CPU, I/O, and memory metrics. Build a trend: average hourly DB Time, physical read bytes, redo generation. Project forward: if DB Time is growing 5% week-over-week, in 12 weeks you'll exceed your 4-CPU system's capacity. Combine with tablespace growth from `dba_hist_tbspc_space_usage` for storage planning. This gives a data-driven timeline for infrastructure upgrades.

> **Tip 2:** "What is the Time Model and how does it help tuning?" — The Time Model hierarchically breaks down DB Time: SQL execution → parse → hard parse → PL/SQL → background. High parse/DB time ratio → bind variables issue. High hard parse % → shared pool problem. High SQL execution but low CPU → I/O bound. High background time → LGWR/DBW bottleneck. It tells you which layer to investigate before diving into SQL-level analysis.

> **Tip 3:** "You need to compare performance before and after an application release. What do you do?" — Create AWR baselines before the release (for 3-5 normal business days). After the release: generate an AWR Diff Report comparing the two baseline periods (`DBMS_WORKLOAD_REPOSITORY.AWR_DIFF_REPORT_TEXT`). The diff report highlights: SQL that got slower (plan changed), new wait events, changes in top SQL rankings, hard parse spikes (new SQL not using bind variables). This is the standard post-release performance validation process.
