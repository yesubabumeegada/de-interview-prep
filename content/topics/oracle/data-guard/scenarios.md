---
title: "Data Guard — Scenarios"
topic: oracle
subtopic: data-guard
content_type: scenario_question
tags: [oracle, data-guard, interview, scenarios, dr, failover, rto-rpo]
---

# Data Guard — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Set Up a Physical Standby

**Scenario:** You need to create a physical standby for a 500GB production database to enable DR. Walk through the high-level steps.

<details>
<summary>💡 Hint</summary>

Four prerequisites on the primary before you can create a standby: ARCHIVELOG mode must be ON (Data Guard ships archived logs), FORCE LOGGING must be enabled (so NOLOGGING operations are still captured), a password file must exist (used for redo transport authentication), and a standby redo log must be sized and created. Then use RMAN DUPLICATE to copy the database to the standby host — not a manual copy.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Primary Database Has Crashed — Failover

**Scenario:** At 2am, your primary database server has a hardware failure. It's completely unresponsive. The standby is healthy with a 45-second apply lag (Maximum Performance mode). How do you failover?

<details>
<summary>💡 Hint</summary>

Confirm the primary is truly down before failing over (not a network partition where both sides think they're primary). Check transport lag growth on the standby — if `v$dataguard_stats.transport lag` keeps increasing, the primary is not sending redo. With Maximum Performance and 45s lag, expect up to 45s of data loss. Use `DGMGRL FAILOVER TO standby` (not switchover — switchover requires the primary to participate). After failover, reinstate the old primary as a new standby when it comes back.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design DR Architecture for 99.999% Uptime

**Scenario:** Design a Data Guard architecture for a financial trading system: RPO = 0 (zero data loss), RTO = 30 seconds (auto-failover), 5TB database, trading hours 8am-8pm EST, DB must support 50,000 TPS at peak.

<details>
<summary>💡 Hint</summary>

RPO=0 forces Maximum Availability mode (synchronous redo shipping — primary waits for standby acknowledgement before returning). This adds write latency, so the standby must be on low-latency private interconnect, not public internet. RTO=30s means you need Fast-Start Failover (FSFO) with an Observer process that monitors both sides and triggers auto-failover. For 50K TPS, primary should be an Oracle RAC cluster. Consider a far-sync instance to relay redo asynchronously to a distant DR site without imposing inter-datacenter latency on every transaction.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is Oracle Data Guard and what problem does it solve?**
A: Data Guard is Oracle's high-availability and disaster recovery solution that maintains one or more synchronized standby databases. It eliminates single-point-of-failure risk by automatically shipping and applying redo logs from the primary to standbys, enabling failover in minutes with zero or near-zero data loss.

**Q: What are the three protection modes in Data Guard?**
A: Maximum Performance (async redo shipping—best throughput, potential data loss on failover), Maximum Availability (sync shipping, primary continues if standby unavailable—no data loss under normal conditions), and Maximum Protection (sync shipping, primary shuts down if standby unavailable—strictest no-data-loss guarantee).

**Q: What is the difference between a physical standby and a logical standby?**
A: A physical standby is a block-for-block copy maintained by applying archived redo logs via Media Recovery. A logical standby applies SQL-level changes reconstructed from LogMiner, allowing the standby to be open read-write with additional tables not on the primary—useful for reporting but with restrictions on supported data types.

**Q: What is Active Data Guard?**
A: Active Data Guard (a licensed feature) allows a physical standby to be open read-only while still applying redo logs in real time. This enables offloading read queries and backups to the standby without interrupting recovery, effectively providing a read-scale replica.

**Q: What is the difference between switchover and failover?**
A: Switchover is a planned, graceful role reversal—both primary and standby remain intact and no data is lost. Failover is an emergency operation triggered by primary failure—the standby becomes the new primary, potentially with some data loss depending on the protection mode and redo shipping lag.

**Q: What is a redo transport lag and how do you monitor it?**
A: Redo transport lag is the delay between when redo is generated on the primary and when it is applied on the standby. Monitor it via `V$DATAGUARD_STATS` (columns `name='apply lag'`) or `V$ARCHIVE_DEST_STATUS`. Sustained lag indicates network bandwidth or standby apply throughput issues.

**Q: What is a Fast-Start Failover (FSFO)?**
A: FSFO automates failover without DBA intervention when the primary becomes unavailable. It requires a Data Guard Observer process and the standby must meet configurable lag and connectivity thresholds. FSFO is essential for SLAs that require sub-minute RTO.

**Q: How does Data Guard integrate with RMAN for backup strategy?**
A: With Active Data Guard, RMAN backups can be offloaded entirely to the standby, reducing primary I/O. The standby's backups are usable for primary recovery because the files are physically identical. This is a best-practice for production environments to keep backup overhead off the primary.

---

## 💼 Interview Tips

- Lead with the three protection modes and the RPO/RTO trade-off each represents—this frames the architectural thinking interviewers want to see.
- Know the operational commands: `ALTER DATABASE SWITCHOVER TO standby_name VERIFY;` and `ALTER DATABASE FAILOVER TO standby_name;`. Vague answers about "clicking a button in OEM" won't land well.
- Senior interviewers often ask about post-failover steps: re-syncing the old primary as a new standby, updating connection strings/TNS aliases, and reinstating FSFO observer. Walk through the full sequence.
- Mention Active Data Guard for read offload—many teams don't use it optimally. Showing you know how to use the standby for reporting queries demonstrates cost-consciousness.
- Connect Data Guard to your DR testing discipline: scheduled switchover drills are the only way to verify RTO. Mention that untested failover procedures are a compliance risk.
