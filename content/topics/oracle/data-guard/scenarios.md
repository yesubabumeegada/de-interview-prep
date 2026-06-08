---
title: "Data Guard — Scenarios"
topic: oracle
subtopic: data-guard
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [oracle, data-guard, interview, scenarios, dr, failover, rto-rpo]
---

# Data Guard — Interview Scenarios

## Scenario 1 (Junior): Set Up a Physical Standby

**Question:** You need to create a physical standby for a 500GB production database to enable DR. Walk through the high-level steps.

**Answer:**

```sql
-- PREREQUISITES (on PRIMARY):
-- 1. Enable ARCHIVELOG mode (required for Data Guard)
SELECT log_mode FROM v$database;
-- If NOARCHIVELOG: ALTER DATABASE ARCHIVELOG; (requires brief shutdown)

-- 2. Enable FORCE LOGGING (ensures all changes are logged, even with NOLOGGING hints)
ALTER DATABASE FORCE LOGGING;

-- 3. Add Standby Redo Logs (SRLs) — same size as online redo logs, one extra group
-- If primary has 4 redo log groups of 200MB:
-- Add 5 SRL groups (n+1 where n = redo log groups) of 200MB each
ALTER DATABASE ADD STANDBY LOGFILE GROUP 11 '/oradata/srl01.log' SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 12 '/oradata/srl02.log' SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 13 '/oradata/srl03.log' SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 14 '/oradata/srl04.log' SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 15 '/oradata/srl05.log' SIZE 200M;

-- 4. Set DB_UNIQUE_NAME (if not already)
ALTER SYSTEM SET DB_UNIQUE_NAME = 'MYDB' SCOPE=SPFILE;
-- Restart if needed

-- 5. Set log archive destinations
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=MYDB_STANDBY ASYNC 
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=MYDB_STANDBY';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_STATE_2 = ENABLE;
```

```bash
# INSTANTIATE STANDBY via RMAN Active Duplicate (while primary is online)
# Run on the STANDBY server:
rman auxiliary /
RMAN> DUPLICATE TARGET DATABASE FOR STANDBY FROM ACTIVE DATABASE
      DORECOVER
      SPFILE
        SET DB_UNIQUE_NAME='MYDB_STANDBY'
        SET LOG_ARCHIVE_DEST_2=''  -- clear transport from standby
        SET FAL_SERVER='MYDB'
        SET FAL_CLIENT='MYDB_STANDBY'
      NOFILENAMECHECK;
# RMAN copies all data files directly from primary over the network
# Automatically adds standby redo logs, creates standby control file
```

```sql
-- VERIFY (on STANDBY after RMAN duplication completes):
SELECT db_unique_name, database_role, open_mode FROM v$database;
-- Should show: MYDB_STANDBY | PHYSICAL STANDBY | MOUNTED

-- Start managed recovery
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE 
  USING CURRENT LOGFILE DISCONNECT;

-- Verify redo is being applied
SELECT process, status, sequence# FROM v$managed_standby WHERE process = 'MRP0';
-- Status: APPLYING_LOG = success
```

---

## Scenario 2 (Mid-level): Primary Database Has Crashed — Failover

**Question:** At 2am, your primary database server has a hardware failure. It's completely unresponsive. The standby is healthy with a 45-second apply lag (Maximum Performance mode). How do you failover?

**Answer:**

**Assessment (first 2 minutes):**
```sql
-- On STANDBY: verify it can't reach the primary
SELECT * FROM v$dataguard_stats WHERE name = 'transport lag';
-- 'transport lag' keeps growing = primary not sending redo (confirms primary is down)

-- Check most recent applied archive sequence
SELECT MAX(sequence#) AS last_applied, MAX(next_time) AS last_applied_time
FROM v$log_history;
-- This shows the last point in time the standby has data for
-- 45-second lag: approximately 45 seconds of data may be lost
```

**Failover (4-5 minutes total):**
```sql
-- Step 1: Flush any redo that may be in the standby redo logs but not applied
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE CANCEL;
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE FINISH;
-- FINISH: applies all available redo, then stops
-- If primary is truly dead (no redo available): will complete quickly
-- If some redo arrives: applies it all before stopping

-- Step 2: Activate the standby as primary
ALTER DATABASE ACTIVATE PHYSICAL STANDBY DATABASE;
ALTER DATABASE OPEN;

-- Step 3: Verify role change
SELECT database_role, open_mode, db_unique_name FROM v$database;
-- ROLE: PRIMARY, OPEN_MODE: READ WRITE
```

**Post-failover actions:**
```sql
-- 1. Notify application teams: update connection strings to point to new primary
-- 2. Confirm which transactions were lost (if any):
SELECT MAX(sequence#), MAX(next_time) AS data_through
FROM v$archived_log WHERE standby_dest = 'NO';

-- 3. Once old primary is recovered: rebuild it as a standby of the new primary
-- (Can't rejoin as primary — it has diverged)
-- rman> DUPLICATE TARGET DATABASE FOR STANDBY FROM ACTIVE DATABASE ...
```

---

## Scenario 3 (Senior): Design DR Architecture for 99.999% Uptime

**Question:** Design a Data Guard architecture for a financial trading system: RPO = 0 (zero data loss), RTO = 30 seconds (auto-failover), 5TB database, trading hours 8am-8pm EST, DB must support 50,000 TPS at peak.

**Answer:**

**Architecture:**
```
PRIMARY (DC1 - New York):
  4-node Oracle RAC cluster
  Exadata X8M (low-latency, 50K TPS)
  Protection Mode: MAXIMUM AVAILABILITY

         │ SYNC (15ms fiber to NJ)
         ▼

FAR SYNC (DC1.5 - New Jersey, 15ms):
  Lightweight instance, no data files
  Receives redo SYNCHRONOUSLY from primary
  Forwards ASYNCHRONOUSLY to DC2

         │ ASYNC (50ms WAN to Chicago)
         ▼

STANDBY (DC2 - Chicago):
  2-node Oracle RAC cluster (HA within standby)
  Active Data Guard: open READ ONLY for read offload
  Fast-Start Failover: configured with Observer in DC3

         │ Cascaded ASYNC
         ▼

STANDBY 2 (DC3 - Dallas):
  1-node instance, cold standby
  Emergency failover target if both DC1 and DC2 fail
```

**Configuration:**
```sql
-- Primary: SYNC to Far Sync (achieves zero data loss without WAN latency)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=FAR_SYNC_NJ SYNC AFFIRM
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=FARSYNC_NJ';

-- Far Sync: ASYNC forward to standby
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=MYDB_CHICAGO ASYNC
   VALID_FOR=(STANDBY_LOGFILES,STANDBY_ROLE)
   DB_UNIQUE_NAME=MYDB_CHICAGO';

-- Fast-Start Failover (automatic 30-second failover)
-- dgmgrl sys@PRIMARY
-- DGMGRL> ENABLE FAST_START FAILOVER;
-- DGMGRL> SET FAST_START FAILOVER THRESHOLD 30;
-- DGMGRL> SET FAST_START FAILOVER LAGAPPLY TARGET 0;  -- don't fail over if lag > 0 sec

-- Observer (runs in DC3 — independent location)
-- The Observer monitors primary + standby; triggers failover when FSFO threshold exceeded
-- dgmgrl sys@OBSERVER_DB
-- DGMGRL> START OBSERVER FILE=fsfo_observer.ora;
```

**RPO = 0 validation:**
```sql
-- Confirm SYNC transport is protecting commits
SELECT protection_mode, protection_level FROM v$database;
-- Must show MAXIMUM AVAILABILITY at both primary and Far Sync levels
-- If protection_level drops to MAXIMUM PERFORMANCE: alert immediately
-- (means Far Sync is unreachable and primary fell back to async)
```

**RTO = 30 seconds validation:**
```sql
-- Run quarterly failover test
-- dgmgrl> FAILOVER TO MYDB_CHICAGO;
-- Measure time from command to database OPEN in READ WRITE mode
-- Target: < 30 seconds total from primary failure detection to standby open
```

**Key decisions:**
- Far Sync in nearby DC: enables SYNC transport (zero RPO) without WAN latency penalty
- FSFO with Observer: achieves automatic 30-sec failover without human intervention
- RAC at both primary and standby: eliminates single-node failure as a failover trigger
- Active Data Guard on standby: reporting queries stay up during DR events (already open)
- 3rd standby (Dallas): protection against complete 2-site disaster (regulatory requirement)
