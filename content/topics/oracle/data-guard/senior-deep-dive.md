---
title: "Data Guard — Senior Deep Dive"
topic: oracle
subtopic: data-guard
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, data-guard, fsfo, far-sync, multi-standby, rolling-upgrades]
---

# Data Guard — Senior Deep Dive

## Far Sync Instance

A Far Sync instance is a lightweight Oracle instance with no data files — only control file and standby redo logs. It receives redo from the primary synchronously and forwards it asynchronously to a remote standby:

```
Primary DB (DC1) → SYNC → Far Sync Instance (DC2 regional)
                                        │
                                        └── ASYNC → Standby DB (DC3 remote)

Why: 
- Primary → Far Sync: low latency (same metro area) → SYNC is feasible
- Far Sync → Standby: high latency (cross-continent) → ASYNC is used
- Result: zero data loss with SYNC to Far Sync, near-zero loss to final standby
```

```sql
-- Create Far Sync instance (it's an Oracle instance, not a full DB)
-- Has controlfile (STANDBY type) and standby redo logs, NO data files

-- Primary configuration: SYNC to Far Sync
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=FAR_SYNC_TNS SYNC AFFIRM
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=FAR_SYNC_1';

-- Far Sync configuration: forward to remote standby
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=REMOTE_STANDBY ASYNC
   VALID_FOR=(STANDBY_LOGFILES,STANDBY_ROLE)
   DB_UNIQUE_NAME=MYDB_REMOTE';

-- Verify Far Sync role
SELECT db_unique_name, database_role, open_mode FROM v$database;
-- ROLE: FAR SYNC
-- OPEN_MODE: MOUNTED (never opens in READ ONLY or READ WRITE)
```

---

## Rolling Upgrades Using Data Guard

Data Guard enables patching Oracle Database with minimal downtime:

```sql
-- Database Rolling Upgrade (DRU) process:
-- Use Case: upgrade from 19.3 to 19.18 (patch set update)

-- Step 1: Apply patch to STANDBY first (while primary runs old version)
-- srvctl stop database -db MYDB_STANDBY
-- cd $ORACLE_HOME && opatch apply /patch/34765931
-- srvctl start database -db MYDB_STANDBY

-- Step 2: Convert standby to primary (switchover)
-- dgmgrl sys@PRIMARY
-- DGMGRL> SWITCHOVER TO MYDB_STANDBY;
-- (Now MYDB_STANDBY = patched version is PRIMARY; old PRIMARY is now standby)

-- Step 3: Apply patch to old primary (now standby)
-- (Apply opatch while it's in standby role — no production impact)

-- Step 4: Optional: switchover back if desired
-- DGMGRL> SWITCHOVER TO MYDB;

-- For major version upgrades (e.g., 19c → 21c) using Transient Logical Standby:
-- 1. Convert physical standby to logical standby
-- 2. Upgrade logical standby to 21c
-- 3. Logical standby applies SQL changes from 19c primary
-- 4. Switchover to upgraded logical standby as primary
-- 5. Convert old primary to standby of new version
```

---

## Multi-Standby Configurations

```sql
-- Enterprise: one primary, multiple standbys for different purposes
-- MYDB (Primary, DC1 Production)
--   └── MYDB_LOCAL (Physical Standby, DC1 same campus — ultra-low latency, SYNC)
--   └── MYDB_REMOTE (Physical Standby, DC2 DR site — ASYNC, Active Data Guard for reporting)
--   └── MYDB_FAR (Physical Standby, DC3 second DR — cascaded from MYDB_REMOTE)
--   └── MYDB_TEST (Snapshot Standby — dev/test environment)

-- Configure multiple destinations
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=MYDB_LOCAL SYNC AFFIRM VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME=MYDB_LOCAL';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_3 =
  'SERVICE=MYDB_REMOTE ASYNC VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME=MYDB_REMOTE';

-- View all destinations
SELECT dest_id, status, target, database_mode, destination, db_unique_name, error
FROM v$archive_dest
WHERE status != 'INACTIVE'
ORDER BY dest_id;

-- Monitor lag on all standbys
SELECT dest_name, archived_seq#, applied_seq#,
       (archived_seq# - applied_seq#) lag_archivelogs,
       recovery_mode
FROM v$archive_dest_status
WHERE target = 'STANDBY'
ORDER BY lag_archivelogs DESC;
```

---

## Automatic Block Repair (Active Data Guard)

ADG can automatically repair data file block corruptions on the primary using the standby copy:

```sql
-- ADG: when primary detects a corrupt block during read, it automatically
-- fetches the good copy from the standby

-- Check for block corruptions
SELECT * FROM v$database_block_corruption;

-- Configure automatic block repair (requires ADG license)
-- It's enabled by default when using ADG (Active Data Guard open READ ONLY)
-- No configuration needed — happens automatically

-- Monitor block repairs
SELECT * FROM v$block_change_tracking;  -- tracks changed blocks

-- Force a block media recovery test (for a non-critical table)
-- RMAN> RECOVER DATAFILE 5 BLOCK 1000;
-- With ADG: RMAN fetches the block from the standby automatically
```

---

## Data Guard Performance Tuning

```sql
-- Tuning redo transport: parallel ASYNC transport (19c+)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  'SERVICE=STANDBY ASYNC COMPRESSION=ENABLE
   REOPEN=15 MAX_FAILURE=10
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=MYDB_STANDBY';

-- Redo compression: reduces bandwidth to remote standby
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
  '... COMPRESSION=ENABLE ...';  -- typical 5-10× compression on redo

-- Tune redo apply (on standby):
-- Parallel redo apply: automatically parallel in 12c+
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE 
  USING CURRENT LOGFILE PARALLEL 8 DISCONNECT;

-- For I/O-bound apply: increase standby log buffer and apply buffer
ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 200G;
ALTER SYSTEM SET LOG_BUFFER = 256M;

-- Monitor apply rate
SELECT name, value, unit
FROM v$dataguard_stats
WHERE name IN ('apply rate', 'apply lag', 'transport lag');
-- apply rate: MB/s (how fast redo is being applied)
-- apply lag: how far behind in time (should be near 0)
```

---

## Interview Tips

> **Tip 1:** "What is a Far Sync instance and when do you need it?" — Far Sync is a lightweight Oracle instance (no data files) that acts as a redo relay station. The primary sends redo SYNCHRONOUSLY to the Far Sync (low latency, same metro area), which then forwards ASYNCHRONOUSLY to a geographically distant standby. This achieves zero data loss (SYNC) without the high latency penalty of synchronous commits across continents. Essential for Maximum Availability with remote disaster recovery sites.

> **Tip 2:** "How do you patch Oracle Database with near-zero downtime using Data Guard?" — Use Data Guard Rolling Upgrades (DRU): (1) apply the patch to the standby first, (2) switchover to the now-patched standby as primary (seconds of downtime), (3) apply patch to the old primary (now standby), (4) optionally switchover back. Application experiences only the switchover time (typically < 30 seconds). For major version upgrades, use the Transient Logical Standby method.

> **Tip 3:** "Design a Data Guard configuration that achieves zero RPO and < 1 minute RTO." — Zero RPO requires SYNC transport: primary only commits when standby confirms redo receipt. For low-latency SYNC: use a Far Sync instance in the same datacenter if the DR site is distant. < 1 minute RTO requires Fast-Start Failover (FSFO) with an Observer process: FSFO automatically promotes the standby when primary is unreachable for X seconds (configurable to 15-30 sec). FSFO + SYNC + Far Sync is the standard Oracle architecture for zero RPO + sub-minute RTO.
