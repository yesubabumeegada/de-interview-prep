---
title: "RAC — Real World"
topic: oracle
subtopic: rac
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, rac, production, monitoring, patching, node-failure]
---

# RAC — Real World Patterns

## Pattern 1: Node Failure Response Playbook

```sql
-- DETECTION: Alert from monitoring that node2 is down
-- Step 1: Confirm cluster state
SELECT inst_id, instance_name, host_name, status
FROM gv$instance
ORDER BY inst_id;
-- Only node1 shows (node2 is evicted/down)

-- Step 2: Check what happened to node2 (alert log + cluster log)
-- OS: tail -200 /oracle/diag/rdbms/mydb/mydb2/trace/alert_mydb2.log

-- Step 3: Confirm node1 is performing instance recovery
SELECT * FROM v$recovery_progress;
-- Should complete within 'fast_start_mttr_target' seconds (e.g., 60s)

-- Step 4: Verify all services are now running on node1
SELECT name, status, network_name
FROM v$active_services
WHERE network_name IS NOT NULL
ORDER BY name;
-- All business services should show status='ACTIVE' on node1

-- Step 5: Verify application connections are working
-- Check error logs; connections via SCAN should have reconnected automatically

-- Step 6: Monitor for unusual Global Cache waits (node1 now serving all load alone)
SELECT event, total_waits, ROUND(average_wait/100, 2) avg_ms
FROM v$system_event
WHERE event LIKE 'gc%' AND total_waits > 0
ORDER BY total_waits DESC
FETCH FIRST 5 ROWS ONLY;
-- gc waits should drop to near zero (no other instance to exchange with)
```

---

## Pattern 2: Adding a Third Node to an Existing RAC

```sql
-- PRE-WORK (on existing nodes):
-- 1. Confirm CRS/GI (Grid Infrastructure) version compatibility with new node
-- 2. Ensure shared storage can be seen from new node (same ASM diskgroups)
-- 3. Confirm OS settings match (kernel params, user limits, etc.)

-- OS INSTALL (on new node, run as oracle):
-- 1. Install Oracle Grid Infrastructure on new node
-- gridsetup.sh -addNode -nodeList node3.company.com,VIP-node3.company.com

-- 2. Add database instance on new node
-- dbca -silent -addInstance -nodelist node3 -gdbName MYDB -instanceName MYDB3

-- POST-INSTALL (verify in SQL):
SELECT inst_id, instance_name, host_name, status
FROM gv$instance
ORDER BY inst_id;
-- Should now show 3 instances

-- Step 3: Add new node to all services
-- srvctl modify service -db MYDB -service OLTP_SVC -preferred "MYDB1,MYDB2,MYDB3"
-- srvctl modify service -db MYDB -service REPORTING_SVC -preferred "MYDB3" -available "MYDB1"

-- Verify load redistribution
SELECT inst_id, COUNT(*) sessions
FROM gv$session
WHERE type = 'USER'
GROUP BY inst_id
ORDER BY inst_id;
-- Should start showing connections on all 3 nodes
```

---

## Pattern 3: Service-Based Workload Isolation

```sql
-- Problem: batch jobs running on all nodes degrade OLTP performance
-- Solution: dedicated services per workload type

-- Service design:
-- OLTP_SVC     → preferred: node1, node2 (no node3)
-- REPORTING_SVC → preferred: node3 (dedicated for analytics)
-- BATCH_SVC    → available on node1 only (during off-hours)

-- Create service configuration (srvctl commands):
-- srvctl add service -db MYDB -service OLTP_SVC -preferred "MYDB1,MYDB2"
-- srvctl add service -db MYDB -service REPORTING_SVC -preferred "MYDB3" -available "MYDB1,MYDB2"
-- srvctl add service -db MYDB -service BATCH_SVC -preferred "MYDB1" -available "MYDB2"
-- srvctl start service -db MYDB -service OLTP_SVC
-- srvctl start service -db MYDB -service REPORTING_SVC
-- srvctl start service -db MYDB -service BATCH_SVC

-- Verify service placement
SELECT inst_id, name, status
FROM gv$active_services
WHERE name NOT IN ('SYS$BACKGROUND','SYS$USERS')
ORDER BY name, inst_id;

-- Monitor per-service performance
SELECT service_name, 
       ROUND(elapsed_time/1000000/NULLIF(executions,0),3) avg_sec,
       executions, buffer_gets
FROM v$sqlarea
WHERE service_name IN ('OLTP_SVC', 'REPORTING_SVC', 'BATCH_SVC')
  AND executions > 0
ORDER BY elapsed_time DESC
FETCH FIRST 20 ROWS ONLY;
```

---

## RAC Gotchas

| Gotcha | Impact | Fix |
|---|---|---|
| Sequence with default CACHE 20 | `gc buffer busy acquire` spikes at peak load | `ALTER SEQUENCE s CACHE 10000` |
| Public network used as interconnect | Cache Fusion traffic over slow/shared network — terrible performance | Reconfigure to dedicated private network |
| All services on all nodes | Batch queries pollute OLTP buffer cache; cross-instance contention | Separate services per workload |
| Missing `srvctl` management | Services don't auto-restart after node recovery | Always use srvctl; never manually start instances |
| No load testing of failover | TAF works in theory but fails in practice | Load test with actual node kill during peak simulation |
| Timestamps not synchronized (NTP) | AWR reports show wrong times; ASH analysis unreliable | Ensure chrony/NTP configured identically on all nodes |

---

## Interview Tips

> **Tip 1:** "A RAC node goes down at 2pm. Walk me through what happens and what you do." — (1) Surviving nodes detect the failure via CSS heartbeat timeout (~10s). (2) Oracle performs instance recovery: rolling back uncommitted transactions from the failed instance's redo logs (~seconds to minutes based on MTTR target). (3) Services that were preferred on the failed node restart on available nodes. (4) SCAN listener stops routing to the failed node. (5) TAF/AC reconnects client sessions. As DBA: verify instance recovery completed, confirm all services active, check for unusual GC waits, investigate root cause in alert log and OS logs.

> **Tip 2:** "Why must the RAC interconnect be a dedicated private network?" — Cache Fusion traffic is continuous, high-bandwidth, and latency-sensitive. Block transfers must complete in microseconds for the cache fusion benefit to outweigh the overhead. On a shared public network: (1) bandwidth varies with other traffic, (2) latency can spike to milliseconds, (3) gc wait times explode, effectively making RAC slower than single-instance. The interconnect must be a dedicated 10/25 GbE or InfiniBand network with no other traffic.

> **Tip 3:** "Can you run Oracle Data Guard with a RAC primary?" — Yes, this is the recommended production HA/DR architecture. The primary is a 2-4 node RAC cluster (high availability within datacenter). The standby is a separate database (single-instance or RAC) in another datacenter receiving redo logs via Data Guard. RAC protects against node failures; Data Guard protects against site failures. The standby can also serve read-only queries (Active Data Guard) to offload the primary.
