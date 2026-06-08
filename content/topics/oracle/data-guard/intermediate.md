---
title: "Data Guard — Intermediate"
topic: oracle
subtopic: data-guard
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, data-guard, fast-start-failover, apply-lag, gap-resolution, observer]
---

# Data Guard — Intermediate

## Monitoring Data Guard Health

```sql
-- Comprehensive DG health check query
SELECT 
  d.name db_name,
  d.db_unique_name,
  d.open_mode,
  d.database_role,
  d.switchover_status,
  d.protection_mode,
  d.protection_level
FROM v$database d;

-- Check redo transport and apply status
SELECT name, value, unit, time_computed
FROM v$dataguard_stats
ORDER BY name;
-- Key stats:
-- 'apply lag': how far behind the standby is (target: 0 seconds)
-- 'transport lag': redo not yet transported (should be 0 for SYNC, <30s for ASYNC)
-- 'apply rate': MB/s being applied
-- 'estimated startup time': how long failover would take

-- Real-time lag monitoring (run on primary or standby)
SELECT ROUND((SYSDATE - APPLIED_TIME) * 24 * 60, 1) lag_minutes,
       APPLIED_TIME,
       SEQUENCE#
FROM (
  SELECT MAX(NEXT_TIME) APPLIED_TIME, MAX(SEQUENCE#) SEQUENCE#
  FROM v$log_history
)
JOIN v$database ON 1=1;
```

---

## Archive Gap Detection and Resolution

An archive gap occurs when the standby is missing some archived redo logs:

```sql
-- Detect archive gaps (run on STANDBY)
SELECT thread#, low_sequence#, high_sequence#
FROM v$archive_gap;
-- If rows returned → standby is missing archived logs in that range

-- Manually resolve a gap (transfer missing archives from primary)
-- Option 1: Active duplication (Data Guard will auto-detect and request gaps)
-- Data Guard resolves gaps automatically if the primary archive destination still has them

-- Option 2: Manually copy missing archives (if auto-resolve fails)
-- On primary: find the missing archive
SELECT name, sequence#, first_time, next_time
FROM v$archived_log
WHERE sequence# BETWEEN :low_gap AND :high_gap
  AND standby_dest = 'NO';  -- archives on primary not yet at standby

-- Check archive destination status for gaps
SELECT dest_id, dest_name, status, target, gap_status,
       error, archived_seq#, applied_seq#
FROM v$archive_dest_status
WHERE target = 'STANDBY';
-- gap_status = 'NO GAP': good
-- gap_status = 'RESOLVABLE GAP': archives exist on primary; will auto-sync
-- gap_status = 'UNRESOLVABLE GAP': archives deleted from primary; need rman restore
```

---

## Cascaded Standbys

A cascaded standby receives redo from another standby (not directly from primary) — reduces primary network load:

```sql
-- Architecture:
-- PRIMARY → (InfiniBand/network) → LOCAL STANDBY (same datacenter, sync)
--                                         │
--                                         └──► REMOTE STANDBY (different continent, async)
-- Primary only ships redo to LOCAL STANDBY
-- LOCAL STANDBY cascades to REMOTE STANDBY

-- Configure cascaded transport (on local standby)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_3 =
  'SERVICE=REMOTE_STANDBY ASYNC
   VALID_FOR=(STANDBY_LOGFILES,STANDBY_ROLE)  -- only active when this DB is a standby
   DB_UNIQUE_NAME=MYDB_REMOTE';

-- This allows offloading transcontinental redo shipping from the primary
```

---

## Snapshot Standby

Convert a standby to a fully writable test database, then convert back:

```sql
-- Convert to snapshot standby (standby stops applying redo — creates restore point)
ALTER DATABASE CONVERT TO SNAPSHOT STANDBY;
ALTER DATABASE OPEN;
-- Now the standby is fully writable — use for testing, UAT, performance testing

-- Run tests on snapshot standby...
-- (while primary continues running and redo accumulates in the standby's SRL)

-- Convert back to physical standby (drops all changes, resumes apply)
ALTER DATABASE CONVERT TO PHYSICAL STANDBY;
-- Oracle drops the Guaranteed Restore Point and resets to the physical copy
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT;
-- Standby now catches up applying all redo that accumulated during snapshot mode
```

---

## RMAN Backups from Standby

Take backups from the standby instead of the primary — zero impact on production:

```sql
-- Configure backup on standby (RMAN commands)
-- RMAN> CONNECT TARGET /  (connect to standby)
-- RMAN> CONNECT CATALOG rman_catalog@catdb  (connect to recovery catalog)

-- Full backup from standby
-- RMAN> BACKUP DATABASE PLUS ARCHIVELOG;

-- The backup is tagged as the primary database even though taken on standby
-- Files are backed up: standby has exact copy of primary data files

-- Configure backup job (on standby — run daily)
-- RMAN> RUN {
--   ALLOCATE CHANNEL c1 DEVICE TYPE DISK FORMAT '/backup/%U';
--   BACKUP INCREMENTAL LEVEL 1 DATABASE PLUS ARCHIVELOG DELETE INPUT;
--   BACKUP CURRENT CONTROLFILE FOR STANDBY;
--   RELEASE CHANNEL c1;
-- }

-- Register the backup in the primary's catalog:
-- RMAN> CONNECT TARGET sys@PRIMARY CATALOG rman_catalog@catdb
-- RMAN> CROSSCHECK BACKUP;  -- both primary and standby share the catalog
```

---

## Data Guard and Active Data Guard — License Comparison

| Feature | Physical Standby | Active Data Guard |
|---|---|---|
| Redo transport | ✓ | ✓ |
| Switchover/Failover | ✓ | ✓ |
| Fast-Start Failover | ✓ | ✓ |
| Standby open read-only (while applying redo) | ✗ | ✓ |
| Real-time query offload from primary | ✗ | ✓ |
| Snapshot standby | ✓ | ✓ |
| Automatic block repair | ✗ | ✓ |
| Global DB Services (cross-DB routing) | ✗ | ✓ |
| License | Standard DG (part of EE) | Separate ADG license |

---

## Interview Tips

> **Tip 1:** "What is an archive gap and how does Data Guard resolve it?" — An archive gap means the standby is missing one or more archived redo log files from the primary's redo sequence. Data Guard detects gaps through the FAL (Fetch Archive Log) mechanism: when the standby's MRP process detects a gap, it requests the missing archives from the primary (FAL_SERVER). If those archives still exist on primary, they're automatically fetched. If already deleted from primary, you need to restore them from backup or re-instantiate the standby.

> **Tip 2:** "How do you use a standby to offload backups from the primary?" — Configure RMAN to connect to the standby database (not primary) and run backup commands there. Since the standby is a block-for-block copy of the primary, the backup is equivalent. RMAN registers the backup in the recovery catalog so the primary also knows about it. This eliminates backup I/O, CPU, and tape/storage contention on the primary database during production hours.

> **Tip 3:** "What is Snapshot Standby and when would you use it?" — Snapshot Standby converts a physical standby to a fully writable database by: (1) creating a Guaranteed Restore Point, (2) opening the database in read-write mode. Redo from the primary continues to accumulate but isn't applied. After testing, you convert back — Oracle drops all changes and resumes redo application. Use cases: UAT testing against production-like data, performance testing, schema change validation. The only requirement: enough space in the standby's FRA to accumulate the redo received during snapshot mode.
