---
title: "Data Guard — Real World"
topic: oracle
subtopic: data-guard
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, data-guard, monitoring, dr-test, switchover, production]
---

# Data Guard — Real World Patterns

## Pattern 1: DR Test (Switchover and Back)

Quarterly DR tests validate that the standby can serve as primary:

```bash
# Step 1: Pre-flight checks (run BEFORE the DR test window)

# On PRIMARY:
sqlplus / as sysdba
SQL> SELECT protection_mode, protection_level FROM v$database;
SQL> SELECT dest_name, status, applied_seq#, archived_seq#, 
            (archived_seq# - applied_seq#) lag
     FROM v$archive_dest_status WHERE target = 'STANDBY';
-- lag should be 0 before starting

SQL> SELECT * FROM v$dataguard_stats WHERE name = 'apply lag';
-- apply lag should be < 5 seconds

# Step 2: Notify application teams of planned switchover window (15 min window)
# Step 3: Switchover via DG Broker
dgmgrl sys@PRIMARY
DGMGRL> SWITCHOVER TO MYDB_STANDBY;
-- Output: Performing switchover NOW, please wait...
-- New primary database "MYDB_STANDBY" is opening...
-- Oracle Data Guard switchover succeeded, new primary is "MYDB_STANDBY"
```

```sql
-- Step 4: Verify new primary is operational
-- On NEW PRIMARY (former standby):
SELECT instance_name, host_name, database_role, open_mode FROM v$database;
-- ROLE: PRIMARY, OPEN_MODE: READ WRITE

-- Check old primary is now a standby
SELECT instance_name, database_role FROM v$database;
-- On old primary: ROLE: PHYSICAL STANDBY

-- Step 5: Run application smoke tests against new primary (10 min)
-- Test critical transactions, confirm application connects to new primary via SCAN

-- Step 6: Switchover back to original primary
-- dgmgrl sys@NEW_PRIMARY
-- DGMGRL> SWITCHOVER TO MYDB;
```

---

## Pattern 2: Daily Data Guard Health Check Script

```sql
-- Run this as a daily monitoring job
CREATE OR REPLACE PROCEDURE dg_health_check IS
  v_role          VARCHAR2(30);
  v_lag_seconds   NUMBER;
  v_transport_lag NUMBER;
  v_status        VARCHAR2(30);
  
  PROCEDURE check_and_alert(p_metric IN VARCHAR2, p_value IN NUMBER, p_threshold IN NUMBER) IS
  BEGIN
    IF p_value > p_threshold THEN
      -- Send alert (integrate with your monitoring system)
      DBMS_OUTPUT.PUT_LINE('ALERT: ' || p_metric || ' = ' || p_value || 
                           ' exceeds threshold ' || p_threshold);
      -- In production: call alerting API via UTL_HTTP
    END IF;
  END;
BEGIN
  -- 1. Get current role
  SELECT database_role INTO v_role FROM v$database;
  DBMS_OUTPUT.PUT_LINE('Database role: ' || v_role);
  
  -- 2. Check transport and apply lag
  SELECT 
    MAX(CASE WHEN name = 'apply lag' 
        THEN EXTRACT(SECOND FROM TO_DSINTERVAL(value)) +
             EXTRACT(MINUTE FROM TO_DSINTERVAL(value)) * 60 +
             EXTRACT(HOUR FROM TO_DSINTERVAL(value)) * 3600 END),
    MAX(CASE WHEN name = 'transport lag'
        THEN EXTRACT(SECOND FROM TO_DSINTERVAL(value)) +
             EXTRACT(MINUTE FROM TO_DSINTERVAL(value)) * 60 +
             EXTRACT(HOUR FROM TO_DSINTERVAL(value)) * 3600 END)
  INTO v_lag_seconds, v_transport_lag
  FROM v$dataguard_stats
  WHERE name IN ('apply lag', 'transport lag');
  
  check_and_alert('apply_lag_seconds', NVL(v_lag_seconds, 999999), 300);  -- alert if > 5 min
  check_and_alert('transport_lag_seconds', NVL(v_transport_lag, 999999), 60);  -- alert if > 1 min
  
  -- 3. Check MRP status
  SELECT status INTO v_status
  FROM v$managed_standby
  WHERE process = 'MRP0'
    AND ROWNUM = 1;
  
  IF v_status NOT IN ('APPLYING_LOG', 'WAITING_FOR_LOG') THEN
    DBMS_OUTPUT.PUT_LINE('ALERT: MRP0 status = ' || v_status || ' (expected APPLYING_LOG)');
  END IF;
  
  -- 4. Check archive destination errors
  FOR d IN (SELECT dest_name, error FROM v$archive_dest WHERE status = 'ERROR') LOOP
    DBMS_OUTPUT.PUT_LINE('ALERT: Destination ' || d.dest_name || ' error: ' || d.error);
  END LOOP;
  
  DBMS_OUTPUT.PUT_LINE('DG health check complete: lag=' || v_lag_seconds || 's');
END dg_health_check;
/

-- Schedule the health check
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'DG_HEALTH_CHECK_JOB',
    job_type        => 'STORED_PROCEDURE',
    job_action      => 'DG_HEALTH_CHECK',
    repeat_interval => 'FREQ=MINUTELY; INTERVAL=15',  -- every 15 minutes
    enabled         => TRUE
  );
END;
/
```

---

## Pattern 3: Handling MRP Failure (Standby Not Applying)

```sql
-- Symptoms: apply lag keeps growing; MRP0 not in v$managed_standby

-- Step 1: Check MRP status
SELECT process, status, sequence#, block#, blocks
FROM v$managed_standby
WHERE process LIKE 'MRP%';
-- If no rows: MRP is not running

-- Step 2: Check alert log on STANDBY
-- tail -100 $ORACLE_BASE/diag/rdbms/mydb_standby/mydb/trace/alert_mydb.log

-- Step 3: Check for archive gaps
SELECT * FROM v$archive_gap;
-- If gap exists: see gap resolution pattern above

-- Step 4: Restart MRP
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE CANCEL;
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE 
  USING CURRENT LOGFILE DISCONNECT FROM SESSION;

-- Step 5: Verify MRP restarted and is applying
SELECT process, status, sequence#, SYSDATE apply_checked
FROM v$managed_standby
WHERE process = 'MRP0';

-- Step 6: If MRP keeps dying: check for corrupt archive or standby redo log
SELECT * FROM v$database_block_corruption;

-- Force register missing archived log
ALTER DATABASE REGISTER LOGFILE '/arch/MYDB_1_12345_1234567890.arc';

-- If corrupt archive is the problem: skip it (risks data divergence)
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE SKIP STANDBY LOGFILE
  USING CURRENT LOGFILE DISCONNECT;
-- WARNING: only use SKIP if the archive is truly unrecoverable and you accept data loss
```

---

## Data Guard Alerting Thresholds (Production Best Practices)

| Metric | Warning | Critical | Action |
|---|---|---|---|
| Apply lag | > 5 minutes | > 30 minutes | Check MRP, network, I/O on standby |
| Transport lag | > 1 minute | > 10 minutes | Check network bandwidth, primary redo volume |
| MRP status | Not APPLYING | MRP not running | Restart MRP; check alert log |
| Archive dest errors | Any error | Repeated error | Check standby reachability, disk space |
| Standby disk space (FRA) | > 80% full | > 95% full | Expand FRA or delete old archives |

---

## Interview Tips

> **Tip 1:** "MRP on the standby stopped applying redo. What do you do?" — Check: (1) `v$managed_standby` — is MRP0 present? If not, restart it with `RECOVER MANAGED STANDBY DATABASE`. (2) Alert log for ORA errors — common causes: archive gap, I/O error, insufficient FRA space. (3) `v$archive_gap` for missing sequences. (4) FRA usage via `v$recovery_area_usage`. Most issues are either a gap in archives or the FRA is full. Fix the root cause, then restart MRP.

> **Tip 2:** "How often should you test DR failover?" — At minimum quarterly; highly critical systems should test monthly. Each test validates: (1) standby is actually in sync, (2) failover procedure works as documented, (3) application connections reconnect correctly, (4) the RTO actually meets the SLA. Tests also keep the DBA team practiced — a failover at 3am is not the time to discover the runbook has a typo. Use switchover (not failover) for tests — it's graceful and reversible.

> **Tip 3:** "What's the difference between a switchover and a failover from an application perspective?" — Switchover: planned, graceful, no data loss. The brief outage is just the seconds for the new primary to open. Applications that use SCAN and TAF/AC will reconnect transparently. Failover: unplanned, primary may be unreachable. There may be committed transactions that were in the online redo log but not yet at the standby (Maximum Performance mode). Application sessions on the dead primary will error; new connections will go to the new primary via updated SCAN registration. Applications must be designed to handle brief errors and retry.
