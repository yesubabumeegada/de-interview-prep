---
title: "Data Guard — Fundamentals"
topic: oracle
subtopic: data-guard
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, data-guard, standby, dr, redo-transport, switchover, failover]
---

# Data Guard — Fundamentals


## 🎯 Analogy

Think of Oracle Data Guard like a shadow copy of your database that's always in sync: the primary sends redo logs to standby databases in real time. If the primary fails, standby takes over in seconds — RPO near zero, RTO seconds to minutes.

---
## What Is Oracle Data Guard?

Oracle Data Guard maintains one or more standby databases as synchronized copies of the primary database. It automatically ships redo log data from the primary to standbys, keeping them current. Used for disaster recovery (DR) and high availability.

```
Primary DB (DC1, Production)
    │
    │  Redo Log Transport
    │  (SYNC or ASYNC)
    ▼
Standby DB (DC2, DR Site)
    │
    └── Physical Standby: block-for-block copy (most common)
    └── Logical Standby: SQL-applied changes (can run SQL on standby)
    └── Snapshot Standby: fully open for testing, re-syncs on conversion back
```

---

## Protection Modes

| Mode | Description | Data Loss Risk | Primary Impact |
|---|---|---|---|
| **Maximum Protection** | Primary waits for standby to confirm redo receipt before commit returns. Primary shuts down if standby is unreachable. | Zero | High (primary stops if standby fails) |
| **Maximum Availability** | Like Maximum Protection but primary continues if standby is unreachable (falls back to async). | Zero when sync; minimal if temporary fallback | Low |
| **Maximum Performance** | Redo sent asynchronously — primary doesn't wait for standby confirmation. | Some redo may be lost if primary fails | None |

```sql
-- View current protection mode
SELECT protection_mode, protection_level FROM v$database;

-- Change protection mode
ALTER DATABASE SET STANDBY DATABASE TO MAXIMIZE AVAILABILITY;
ALTER DATABASE SET STANDBY DATABASE TO MAXIMIZE PERFORMANCE;
```

---

## Redo Transport

```sql
-- Configure redo transport to standby (on PRIMARY)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 = 
  'SERVICE=STANDBY_TNSALIAS ASYNC                    -- ASYNC: lower latency impact
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=MYDB_STANDBY';

-- SYNC transport (for Maximum Availability / zero data loss)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 = 
  'SERVICE=STANDBY_TNSALIAS SYNC AFFIRM
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=MYDB_STANDBY';

-- SYNC: commit waits for standby to write to its standby redo log
-- ASYNC: commit returns immediately; redo shipped in background

-- Check transport status
SELECT dest_name, status, target, archiver, schedule, 
       destination, error
FROM v$archive_dest
WHERE target = 'STANDBY';

-- Check redo transport lag
SELECT dest_name, archived_seq#, applied_seq#,
       (archived_seq# - applied_seq#) AS lag_sequences
FROM v$archive_dest_status
WHERE target = 'STANDBY';
```

---

## Physical Standby

The most common standby type — exact block-for-block copy of primary:

```sql
-- On STANDBY: check redo apply status
SELECT process, status, sequence#, thread#
FROM v$managed_standby
ORDER BY process;
-- MRP0 process = Managed Recovery Process (applies redo)
-- Status: APPLYING_LOG = active recovery (good)

-- Start/stop managed recovery
-- Start:
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT;
-- Stop:
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE CANCEL;

-- Check apply lag
SELECT NAME, VALUE, DATUM_TIME
FROM v$dataguard_stats
WHERE NAME IN ('apply lag', 'transport lag', 'estimated startup time');

-- Open standby for READ ONLY (Active Data Guard — requires separate license)
ALTER DATABASE OPEN READ ONLY;
-- Now standby applies redo AND serves read queries simultaneously
```

---

## Switchover and Failover

**Switchover:** planned role reversal (no data loss, graceful)
**Failover:** unplanned — primary is down; standby takes over (possible data loss)

```sql
-- SWITCHOVER (graceful, no data loss)
-- Step 1: Convert primary to standby
-- On PRIMARY:
ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN;
-- Waits for all sessions to disconnect and all redo to be sent to standby

-- Step 2: Convert standby to primary
-- On STANDBY:
ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN;
ALTER DATABASE OPEN;
-- Old primary database now runs as standby

-- FAILOVER (emergency — primary is dead)
-- On STANDBY:
-- First apply any remaining received redo
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE FINISH;
-- Then failover
ALTER DATABASE ACTIVATE PHYSICAL STANDBY DATABASE;
ALTER DATABASE OPEN;
```

---

## Data Guard Broker

Data Guard Broker simplifies management and enables Fast-Start Failover:

```sql
-- Start DMON processes on both primary and standby
ALTER SYSTEM SET DG_BROKER_START = TRUE;

-- Connect to broker via DGMGRL
-- dgmgrl sys@PRIMARY
-- DGMGRL> CREATE CONFIGURATION 'myconfig' AS PRIMARY DATABASE IS 'MYDB' CONNECT IDENTIFIER IS MYDB;
-- DGMGRL> ADD DATABASE 'MYDB_STANDBY' AS CONNECT IDENTIFIER IS MYDB_STANDBY MAINTAINED AS PHYSICAL;
-- DGMGRL> ENABLE CONFIGURATION;

-- Check configuration health
-- DGMGRL> SHOW CONFIGURATION;
-- DGMGRL> SHOW DATABASE 'MYDB_STANDBY';

-- Switchover via broker (much simpler than manual)
-- DGMGRL> SWITCHOVER TO MYDB_STANDBY;

-- Enable Fast-Start Failover (automatic failover)
-- DGMGRL> ENABLE FAST_START FAILOVER;
-- DGMGRL> SET FAST_START FAILOVER THRESHOLD 30;  -- fail over after 30 seconds
```

---


## ▶️ Try It Yourself

```sql
-- On PRIMARY: check Data Guard status
SELECT db_unique_name, database_role, protection_mode, protection_level
FROM v$database;

-- Check log shipping lag to standby
SELECT dest_id, status, target, archiver,
       ROUND(applied_seq# - error_seq#) AS gap
FROM v$archive_dest_status
WHERE target = 'STANDBY';

-- On STANDBY: check apply lag
SELECT NAME, VALUE, TIME_COMPUTED
FROM v$dataguard_stats
WHERE name IN ('apply lag', 'transport lag');

-- Switchover (planned, zero data loss)
-- On primary:
ALTER DATABASE COMMIT TO SWITCHOVER TO STANDBY;
-- On standby:
ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY;
ALTER DATABASE OPEN;
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is the difference between switchover and failover?" — Switchover is a planned, graceful role reversal: primary drains sessions, sends all redo to standby, then both databases swap roles. No data loss. Used for planned maintenance, patching, or datacenter moves. Failover is unplanned: the primary has crashed and can't be reached. The standby is promoted to primary immediately. There may be data loss (redo that was in the primary's online redo log but not yet transported to the standby).

> **Tip 2:** "What are the three Data Guard protection modes and when do you use each?" — Maximum Protection: financial transactions requiring absolute zero data loss; primary halts if standby is unreachable — high availability risk. Maximum Availability (most common): zero data loss when standby is reachable; primary continues with async if temporarily unreachable — best balance. Maximum Performance: primary never waits for standby; use when DR is needed but latency is too high for sync transport (cross-continent).

> **Tip 3:** "What is Active Data Guard?" — Active Data Guard (ADG) is a separately licensed feature that allows the physical standby to be open READ ONLY while still applying redo logs from the primary. The standby serves read-only queries (reporting, backups, analytics) simultaneously. This offloads read workload from the primary and makes the standby productive, not just idle.
