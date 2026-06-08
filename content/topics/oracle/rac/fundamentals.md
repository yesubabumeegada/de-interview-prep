---
title: "RAC — Fundamentals"
topic: oracle
subtopic: rac
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, rac, cluster, cache-fusion, interconnect, high-availability]
---

# RAC — Fundamentals

## What Is Oracle RAC?

Oracle Real Application Clusters (RAC) allows multiple database instances to run on different servers while sharing a single database on shared storage. Each instance has its own SGA (memory) and processes, but all instances access the same physical data files.

```
┌─────────────────────────────────────────┐
│             Oracle RAC Cluster          │
│                                         │
│  ┌──────────┐      ┌──────────┐        │
│  │ Node 1   │      │ Node 2   │        │
│  │Instance 1│◄────►│Instance 2│        │
│  │  SGA     │      │  SGA     │        │
│  └────┬─────┘      └────┬─────┘        │
│       │    Private       │             │
│       │  Interconnect    │             │
│       └──────────────────┘             │
│                                         │
│         Shared Storage (ASM)            │
│  ┌──────────────────────────────────┐  │
│  │  Data Files  Redo Logs  Control  │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**Key benefits:**
- **Horizontal scalability**: add nodes to increase throughput
- **High availability**: if one node fails, surviving nodes take over connections
- **Workload distribution**: connections spread across all nodes via SCAN

---

## Cache Fusion

The heart of RAC: when a session on Node 2 needs a block currently in Node 1's buffer cache, Cache Fusion ships the block directly over the private interconnect — without going to disk.

```
Without RAC: Read from disk each time a different server needs the block
With Cache Fusion: Node 1 sends block over interconnect to Node 2 (memory-to-memory)
→ Much faster than disk I/O
```

**GCS (Global Cache Service)** manages Cache Fusion:
```sql
-- Monitor Cache Fusion performance
SELECT metric_name, value, metric_unit
FROM v$sysmetric
WHERE metric_name IN (
  'Global Cache Average CR Get Time',
  'Global Cache Average Current Get Time',
  'Global Cache Blocks Received',
  'Global Cache Blocks Served'
)
ORDER BY metric_name;

-- Cache fusion wait events (interconnect health indicator)
SELECT event, total_waits, 
       ROUND(time_waited/100, 2) time_sec,
       ROUND(average_wait/100, 2) avg_ms
FROM v$system_event
WHERE event LIKE 'gc%'  -- Global Cache waits
ORDER BY time_waited DESC
FETCH FIRST 10 ROWS ONLY;
```

---

## RAC Key Concepts

| Concept | Description |
|---|---|
| **SCAN** (Single Client Access Name) | One DNS name for all cluster nodes; clients connect to SCAN, Oracle routes to best node |
| **GCS** | Global Cache Service: manages buffer cache coherency across instances |
| **GES** | Global Enqueue Service: manages lock coherency across instances |
| **OCR** | Oracle Cluster Registry: stores cluster configuration |
| **Voting Disk** | Determines which nodes are alive during network partition; prevents split-brain |
| **Interconnect** | Private high-speed network between nodes (10/25 GbE or InfiniBand) — must be fast |
| **AWR/ASH** | Available cluster-wide: `gv$` views show all instances |

---

## GV$ Views — Cluster-Wide Statistics

In a RAC environment, use `gv$` views to see all instances simultaneously:

```sql
-- See all active sessions across all RAC nodes
SELECT inst_id, sid, username, status, event, machine
FROM gv$session
WHERE type = 'USER' AND status = 'ACTIVE'
ORDER BY inst_id, sid;

-- Top SQL across the cluster
SELECT inst_id, sql_id, executions, 
       ROUND(elapsed_time/1000000/executions, 2) avg_sec
FROM gv$sql
WHERE executions > 0
  AND parsing_schema_name NOT IN ('SYS', 'SYSTEM')
ORDER BY elapsed_time DESC
FETCH FIRST 20 ROWS ONLY;

-- Compare buffer cache hit ratio per instance
SELECT inst_id,
       ROUND(100 * (1 - phys_reads / (cons_gets + cur_gets)), 2) cache_hit_pct
FROM (
  SELECT inst_id,
         SUM(CASE WHEN name='physical reads' THEN value END) phys_reads,
         SUM(CASE WHEN name='consistent gets' THEN value END) cons_gets,
         SUM(CASE WHEN name='db block gets' THEN value END) cur_gets
  FROM gv$sysstat
  WHERE name IN ('physical reads', 'consistent gets', 'db block gets')
  GROUP BY inst_id
)
ORDER BY inst_id;
```

---

## Services — Workload Distribution

```sql
-- Services route different workload types to different instances
-- OLTP service: runs on all nodes
-- REPORTING service: runs only on Node 2 (avoids OLTP interference)
-- BATCH service: runs on Node 3

-- View configured services
SELECT name, network_name, enabled, preferred_instances, available_instances
FROM dba_services
ORDER BY name;

-- Check service availability per node
SELECT inst_id, name, status
FROM gv$active_services
ORDER BY inst_id, name;

-- Create a new service using SRVCTL (OS command, not SQL):
-- srvctl add service -db MYDB -service OLTP_SVC -preferred "MYDB1,MYDB2" -available "MYDB3"
-- srvctl start service -db MYDB -service OLTP_SVC
```

---

## RAC Startup and Shutdown

```sql
-- Check instance status
SELECT inst_id, instance_name, host_name, status, database_status
FROM gv$instance
ORDER BY inst_id;

-- Shutdown one node gracefully (other nodes remain up)
-- Typically done via OS: srvctl stop instance -db MYDB -instance MYDB2

-- Current node info
SELECT instance_number, instance_name, host_name
FROM v$instance;
```

---

## Interview Tips

> **Tip 1:** "What is Cache Fusion and why is it important for RAC?" — Cache Fusion allows RAC nodes to share data blocks from their buffer caches directly over the private interconnect, without going to disk. When Node 2 needs a block that Node 1 already has in memory, Cache Fusion ships the block in microseconds. This makes RAC's distributed memory almost as efficient as a single-instance buffer cache, and far faster than re-reading from shared storage.

> **Tip 2:** "What are SCAN addresses and why use them instead of VIPs?" — Before SCAN, clients had to be configured with each node's VIP — if nodes were added or removed, all client TNS configurations had to change. SCAN (Single Client Access Name) provides 3 IP addresses round-robined by DNS; Oracle's listener routes connections to the least-loaded instance. Clients only need the SCAN name — adding/removing nodes doesn't require client-side changes.

> **Tip 3:** "What is the purpose of the Voting Disk?" — The Voting Disk resolves "split brain" — a situation where nodes can't see each other but can still see storage. Without voting, both halves of a split cluster might try to access the same data files simultaneously, causing corruption. The voting disk is the tie-breaker: the node(s) that can write to the voting disk continue; the others evict themselves (node fencing). Typically 3 voting disks on separate storage for odd-count majority quorum.
