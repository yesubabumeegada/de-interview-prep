---
title: "RAC — Intermediate"
topic: oracle
subtopic: rac
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, rac, interconnect, gc-waits, services, instance-recovery, load-balancing]
---

# RAC — Intermediate

## Diagnosing Global Cache Wait Events

High `gc buffer busy acquire` or `gc cr block 2-way` wait times indicate interconnect issues:

```sql
-- Key RAC wait events and their meaning
-- gc cr block 2-way: got a consistent read block from another instance (normal, fast)
-- gc cr block 3-way: 3-node hop — slightly more expensive  
-- gc buffer busy acquire: waiting to lock a block to modify it (hot block contention)
-- gc cr multi block request: multiblock read via cache fusion

SELECT event, 
       total_waits, 
       ROUND(time_waited/100, 2) time_sec,
       ROUND(average_wait/100, 4) avg_wait_sec,
       CASE 
         WHEN ROUND(average_wait/100, 4) < 0.005 THEN 'GOOD'
         WHEN ROUND(average_wait/100, 4) < 0.020 THEN 'OK'
         ELSE 'INVESTIGATE'
       END health
FROM v$system_event
WHERE event LIKE 'gc%'
  AND total_waits > 0
ORDER BY time_waited DESC;

-- Interconnect throughput check
SELECT metric_name, value, metric_unit
FROM v$sysmetric
WHERE metric_name IN (
  'Interconnect Traffic', 
  'GC Throughput', 
  'Global Cache Average CR Get Time',
  'Global Cache Average Current Get Time'
)
ORDER BY metric_name;
```

---

## Hot Block Contention in RAC

When multiple instances compete for the same data block, `gc buffer busy acquire` spikes:

```sql
-- Find the hottest blocks causing RAC contention
SELECT inst_id, file#, block#, class, status,
       dirty, temp, ping, stale, direct
FROM gv$bh
WHERE class# = 1  -- data blocks
  AND ping > 100  -- block has been pinged (transferred between instances) frequently
ORDER BY ping DESC
FETCH FIRST 20 ROWS ONLY;

-- Map hot blocks back to objects
SELECT o.owner, o.object_name, o.object_type, o.subobject_name
FROM dba_objects o
WHERE o.data_object_id = (
  SELECT data_object_id
  FROM dba_extents
  WHERE file_id = :file_num
    AND :block_num BETWEEN block_id AND block_id + blocks - 1
    AND ROWNUM = 1
);
```

**Common causes and fixes:**

| Cause | Fix |
|---|---|
| Sequence cache too small | `ALTER SEQUENCE myseq CACHE 1000` (increase cache) |
| Hot index root block (monotonic inserts) | Use reverse key index or hash partitioning on index |
| Hot table header (segment header contention) | `ALTER TABLE t PCTFREE 30; MOVE;` to reduce block density |
| Application polling the same row | Move to notification-based pattern (DBMS_AQ) |

---

## RAC Services and Load Balancing

```sql
-- Connection Load Balancing Types:
-- 1. CLIENT-SIDE: client randomly picks a SCAN listener
-- 2. SERVER-SIDE (recommended): SCAN listener checks actual service load

-- Configure server-side load balancing goal
-- Goal 'SERVICE_TIME': route to instance with best response time
-- Goal 'THROUGHPUT': maximize transactions per second

-- Create/modify service load balancing (via DBMS_SERVICE or srvctl)
BEGIN
  DBMS_SERVICE.MODIFY_SERVICE(
    service_name    => 'OLTP_SVC',
    goal            => DBMS_SERVICE.GOAL_SERVICE_TIME,
    clb_goal        => DBMS_SERVICE.CLB_GOAL_LONG  -- for long-running: round-robin
    -- CLB_GOAL_SHORT: for short-lived connections (OLTP) = service time based
  );
END;
/

-- View load balancing advisory
SELECT inst_id, service_name, event, begin_time,
       intsize_csec, est_txn_count, qtime
FROM gv$servicemetric
ORDER BY inst_id, service_name;

-- Application Continuity: transparent replay of in-flight transactions on node failure
-- Requires using the service with Application Continuity enabled:
-- srvctl modify service -db MYDB -service OLTP_SVC -failovertype TRANSACTION -replay_init_time 300
```

---

## Instance Recovery in RAC

```sql
-- When a RAC node crashes, surviving nodes perform instance recovery
-- Monitor ongoing instance recovery
SELECT * FROM v$fast_start_transactions;  -- transactions being recovered
SELECT * FROM v$recovery_progress;        -- overall recovery progress

-- Tune instance recovery target (time to recovery after crash)
SHOW PARAMETER fast_start_mttr_target;
-- Default: 0 (no limit); set to 30-300 seconds for bounded recovery time
ALTER SYSTEM SET fast_start_mttr_target = 60;  -- target 60-second recovery

-- After instance recovery: check for unusable indexes
SELECT index_name, status FROM dba_indexes WHERE status = 'UNUSABLE';
-- Rebuild if necessary
ALTER INDEX idx_name REBUILD PARALLEL 8;
```

---

## RAC One Node — Single-Instance with RAC Benefits

```sql
-- RAC One Node: single active instance but RAC infrastructure in place
-- If node fails: online relocation to surviving node (no crash recovery needed)
-- Better than single-instance: zero-downtime patching via relocation

-- Check RAC One Node state
SELECT instance_number, instance_name, host_name, status
FROM gv$instance;
-- Only 1 instance will be OPEN; others in MOUNTED state waiting

-- Initiate online relocation (planned maintenance)
-- srvctl relocate database -db MYDB -currentnode node1 -targetnode node2
-- All sessions are transparently moved to the new instance
```

---

## Monitoring and Troubleshooting Tools

```sql
-- Cluster Health Monitor (CHM): OS-level metrics history
-- Access via OCLUMON utility (OS command):
-- oclumon dumpnodeview -allnodes -last 30m

-- RAC-specific wait analysis in ASH
SELECT inst_id, event, COUNT(*) samples
FROM gv$active_session_history
WHERE sample_time > SYSDATE - 1/24  -- last 1 hour
  AND event LIKE 'gc%'
GROUP BY inst_id, event
ORDER BY samples DESC;

-- Identify sessions with most interconnect activity
SELECT s.inst_id, s.sid, s.username, s.program,
       a.event, COUNT(*) ash_samples
FROM gv$active_session_history a
JOIN gv$session s ON a.session_id = s.sid AND a.inst_id = s.inst_id
WHERE a.sample_time > SYSDATE - 1/24
  AND a.event LIKE 'gc%'
GROUP BY s.inst_id, s.sid, s.username, s.program, a.event
ORDER BY ash_samples DESC
FETCH FIRST 20 ROWS ONLY;
```

---

## Interview Tips

> **Tip 1:** "What causes high `gc buffer busy acquire` waits in RAC?" — This wait means a session is trying to acquire a block that another session (possibly on a different instance) already holds in a mode that blocks the acquisition. Common causes: (1) sequence with small cache causing all instances to fight over the sequence header block, (2) monotonic primary key inserts all going to the same index leaf block, (3) application hot spots like a status update counter. Fix sequence contention with `CACHE 1000`; fix index hot blocks with reverse key or hash partitioning.

> **Tip 2:** "What is the difference between CLIENT-SIDE and SERVER-SIDE load balancing in RAC?" — Client-side: the client randomly picks from the SCAN IPs — no awareness of actual load. Server-side: the SCAN listener queries a lightweight load-balancing advisory that tracks each service's response time and connection count; new connections are routed to the least-loaded instance. Server-side with `CLB_GOAL_SHORT` is recommended for OLTP — it routes connections where the shortest response time is expected.

> **Tip 3:** "How does RAC provide high availability differently from Data Guard?" — RAC provides availability against node failures within a single datacenter — if one node fails, surviving nodes immediately serve traffic (no failover delay). But all nodes share the same storage — a storage failure or datacenter disaster affects all nodes. Data Guard provides a standby database in a separate datacenter with its own storage, providing disaster recovery (RPO/RTO measured in seconds to minutes). You often run both: RAC (multi-node) + Data Guard (standby site) for full HA/DR.
