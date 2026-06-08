---
title: "AWR Reports — Real World"
topic: oracle
subtopic: awr-reports
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, awr, production, performance-investigation, monitoring, alerting]
---

# AWR Reports — Real World Patterns

## Pattern 1: Post-Incident Root Cause Analysis

After a 10-minute performance incident, use AWR + ASH to determine root cause:

```sql
-- Step 1: Find snap_ids covering the incident window
SELECT snap_id, begin_interval_time, end_interval_time
FROM dba_hist_snapshot
WHERE begin_interval_time BETWEEN 
      TO_DATE('2024-01-15 13:45', 'YYYY-MM-DD HH24:MI') AND
      TO_DATE('2024-01-15 15:00', 'YYYY-MM-DD HH24:MI')
ORDER BY snap_id;
-- Say snap_ids 105 and 106 cover the incident

-- Step 2: ASH analysis of the incident window (finest granularity — 1 second)
SELECT event, COUNT(*) samples,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) pct
FROM dba_hist_active_sess_history
WHERE sample_time BETWEEN 
      TO_TIMESTAMP('2024-01-15 14:00', 'YYYY-MM-DD HH24:MI') AND
      TO_TIMESTAMP('2024-01-15 14:10', 'YYYY-MM-DD HH24:MI')
  AND session_state = 'WAITING'
GROUP BY event
ORDER BY samples DESC
FETCH FIRST 10 ROWS ONLY;

-- Output example:
-- enq: TX - row lock contention  |  485  |  68.3%  ← Row lock is the problem
-- db file sequential read         |  120  |  16.9%
-- CPU                            |   75  |  10.6%

-- Step 3: Find the blocking session
SELECT ash.session_id, ash.blocking_session, ash.sql_id,
       q.sql_text
FROM dba_hist_active_sess_history ash
JOIN dba_hist_sqltext q ON ash.sql_id = q.sql_id
WHERE ash.sample_time BETWEEN 
      TO_TIMESTAMP('2024-01-15 14:00', 'YYYY-MM-DD HH24:MI') AND
      TO_TIMESTAMP('2024-01-15 14:10', 'YYYY-MM-DD HH24:MI')
  AND ash.event = 'enq: TX - row lock contention'
ORDER BY ash.sample_time DESC
FETCH FIRST 20 ROWS ONLY;
```

---

## Pattern 2: Automated AWR Exception Report

Weekly automated report emailing the top findings:

```plsql
CREATE OR REPLACE PROCEDURE send_weekly_awr_summary IS
  v_body    CLOB := '';
  v_snap_start NUMBER;
  v_snap_end   NUMBER;
BEGIN
  -- Get snap_ids for last week
  SELECT MIN(snap_id), MAX(snap_id)
  INTO v_snap_start, v_snap_end
  FROM dba_hist_snapshot
  WHERE begin_interval_time > SYSDATE - 7;
  
  v_body := v_body || '=== WEEKLY AWR SUMMARY ===' || CHR(10);
  v_body := v_body || 'Period: ' || TO_CHAR(SYSDATE-7, 'DD-MON-YYYY') || 
            ' to ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY') || CHR(10) || CHR(10);
  
  -- Top SQL this week
  v_body := v_body || '--- Top 5 SQL by Elapsed Time ---' || CHR(10);
  FOR r IN (
    SELECT s.sql_id,
           ROUND(SUM(s.elapsed_time_delta)/1e6/3600, 2) total_hours,
           SUM(s.executions_delta) total_execs,
           SUBSTR(t.sql_text, 1, 100) sql_preview
    FROM dba_hist_sqlstat s
    JOIN dba_hist_sqltext t ON s.sql_id = t.sql_id AND s.dbid = t.dbid
    WHERE s.snap_id BETWEEN v_snap_start AND v_snap_end
      AND s.parsing_schema_name NOT IN ('SYS', 'SYSTEM')
    GROUP BY s.sql_id, t.sql_text
    ORDER BY SUM(s.elapsed_time_delta) DESC
    FETCH FIRST 5 ROWS ONLY
  ) LOOP
    v_body := v_body || r.sql_id || ': ' || r.total_hours || 'hrs, ' || 
              r.total_execs || ' execs | ' || r.sql_preview || CHR(10);
  END LOOP;
  
  -- Top wait events
  v_body := v_body || CHR(10) || '--- Top 5 Wait Events (week total) ---' || CHR(10);
  FOR r IN (
    SELECT e.event_name,
           SUM(e.total_waits_fg) total_waits,
           ROUND(SUM(e.time_waited_fg)/100/3600, 2) hours_waited
    FROM dba_hist_system_event e
    JOIN dba_hist_snapshot s ON e.snap_id = s.snap_id
    WHERE s.snap_id BETWEEN v_snap_start AND v_snap_end
      AND e.wait_class != 'Idle'
    GROUP BY e.event_name
    ORDER BY SUM(e.time_waited_fg) DESC
    FETCH FIRST 5 ROWS ONLY
  ) LOOP
    v_body := v_body || r.event_name || ': ' || r.hours_waited || 'hrs (' || r.total_waits || ' waits)' || CHR(10);
  END LOOP;
  
  -- Send email (using UTL_MAIL or UTL_SMTP)
  UTL_MAIL.SEND(
    sender     => 'dba-monitor@company.com',
    recipients => 'dba-team@company.com',
    subject    => 'Weekly AWR Summary - ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY'),
    message    => v_body
  );
END send_weekly_awr_summary;
/
```

---

## Pattern 3: Pre/Post Change Validation

```sql
-- Before any significant change: create AWR baseline + take manual snapshot
EXEC DBMS_WORKLOAD_REPOSITORY.CREATE_SNAPSHOT;

-- (Record the snap_id)
SELECT MAX(snap_id) AS pre_change_snap FROM dba_hist_snapshot;

-- Make the change (deploy new code, change parameter, gather stats, etc.)
-- ...

-- After change: take another snapshot immediately
EXEC DBMS_WORKLOAD_REPOSITORY.CREATE_SNAPSHOT;
SELECT MAX(snap_id) AS post_change_snap FROM dba_hist_snapshot;

-- Generate comparison report
SELECT * FROM TABLE(
  DBMS_WORKLOAD_REPOSITORY.AWR_DIFF_REPORT_TEXT(
    dbid1     => (SELECT dbid FROM v$database),
    inst_num1 => 1,
    bid1      => 110,  -- normal period (before change) 
    eid1      => 115,
    dbid2     => (SELECT dbid FROM v$database),
    inst_num2 => 1,
    bid2      => 115,  -- period after change
    eid2      => 120
  )
);
-- Diff report shows: what got faster, what got slower, new top events
```

---

## AWR Anti-Patterns and Common Mistakes

| Mistake | Problem | Fix |
|---|---|---|
| Comparing two AWR periods of different lengths | Rates look different but DB load is the same | Normalize by dividing by elapsed seconds |
| Only reading Top 5 Events | Missing the full picture (some issues show at #10) | Read all sections: SQL, memory, latch, segment stats |
| Ignoring "CPU time" as a wait event | CPU at top means DB is CPU-bound — tuning needed | Investigate high logical reads, full scans, hard parses |
| Not taking a snapshot before a change | No pre-change baseline for comparison | Always `CREATE_SNAPSHOT` before changes |
| Looking at only current V$ views for past incidents | V$ only shows current state | Use AWR/ASH for historical analysis |
| AWR retention too short (default 8 days) | Can't analyze incidents from 2 weeks ago | Increase retention to 30-90 days |

---

## Interview Tips

> **Tip 1:** "A slow period happened 3 days ago. How do you investigate it now?" — Use AWR/ASH historical data: (1) find the snap_ids covering that window in `dba_hist_snapshot`, (2) query `dba_hist_active_sess_history` for that window — look at wait events and top SQL, (3) check top SQL in `dba_hist_sqlstat` for the same snap range, (4) if you need more detail, generate an AWR HTML report for those snap_ids. For very recent incidents (< 30 minutes): use `v$active_session_history` (in-memory ASH).

> **Tip 2:** "Hard parse percentage is 15% this week vs 2% last week. What does that mean and what do you do?" — Hard parse = new SQL text that requires full parse + optimization. At 15%, something in the application stopped using bind variables or started generating literal-value SQL. Impact: CPU overhead (optimization is expensive), memory pressure (each unique SQL text takes shared pool space), cursor cache bloat. Investigation: query `v$sql` for SQL with `PARSE_CALLS = EXECUTIONS` (one parse per execution = no cursor reuse). Then trace which module/schema is generating the literal-value SQL.

> **Tip 3:** "How do you set up proactive monitoring so you're alerted before users complain?" — Use Oracle's AWR metrics thresholds with `DBMS_SERVER_ALERT.SET_THRESHOLD`. Set thresholds on: `DB_TIME_PER_SECOND` > X, `PHYSICAL_READ_BYTES_PER_SEC` > Y, `AVERAGE_SYNCHRONOUS_SINGLE_BLOCK_READ_LATENCY` > 20ms (slow disk), `LOGONS_PER_SEC` > Z (connection storm). When thresholds breach, Oracle writes to `DBA_OUTSTANDING_ALERTS` and can call your monitoring integration. Combined with EM Cloud Control or custom DBMS_SCHEDULER jobs polling these views, you get early warning before users notice.
