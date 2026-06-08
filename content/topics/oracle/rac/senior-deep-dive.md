---
title: "RAC — Senior Deep Dive"
topic: oracle
subtopic: rac
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, rac, application-continuity, global-data-services, sharding, split-brain]
---

# RAC — Senior Deep Dive

## Application Continuity and Transparent Application Failover (TAF)

When a RAC node fails, in-progress transactions need to be handled:

```sql
-- TAF (Transparent Application Failover): reconnect sessions after failure
-- Configure via TNS alias
-- tnsnames.ora:
-- MYDB_TAF =
--   (DESCRIPTION=
--     (FAILOVER=ON)
--     (ADDRESS_LIST=
--       (ADDRESS=(PROTOCOL=TCP)(HOST=scan-host)(PORT=1521)))
--     (CONNECT_DATA=
--       (SERVICE_NAME=OLTP_SVC)
--       (FAILOVER_MODE=
--         (TYPE=SELECT)     -- reconnect + replay SELECT cursors
--         (METHOD=BASIC)    -- reconnect to surviving node
--         (RETRIES=5)
--         (DELAY=5))))

-- Application Continuity (AC): full replay of in-flight requests
-- More powerful than TAF: replays entire call including DML
-- Requires: UCP connection pool, service with AC enabled

-- Enable AC on a service:
-- srvctl modify service -db MYDB -service OLTP_SVC \
--   -failovertype TRANSACTION -failoverdelay 0 -failoverretry 30 \
--   -replay_init_time 300 -commit_outcome TRUE

-- Check if AC is enabled for a service
SELECT s.name, s.failover_type, s.failover_retries, 
       s.commit_outcome, s.replay_initiation_timeout
FROM dba_services s
WHERE s.name = 'OLTP_SVC';
```

---

## Global Data Services (GDS)

GDS extends RAC to multiple databases across datacenters — provides global load balancing and failover:

```sql
-- GDS Architecture:
-- Global Service Manager (GSM): intelligent router
-- GDS Pool: group of databases serving the same application
-- Global Service: service available across the pool

-- Check GDS configuration from a database in the pool
SELECT name, gsmflags, network_name, enabled
FROM dba_services
WHERE network_name IS NOT NULL
ORDER BY name;

-- GDS enables:
-- 1. Read/Write → Active Standby (sends writes to primary, reads to standby)
-- 2. Active-Active geo-distribution (GoldenGate multi-master)
-- 3. Single endpoint for applications spanning multiple regions

-- Connection URL for GDS (application uses GSM endpoint, not database SCAN):
-- jdbc:oracle:thin:@//gsm-hostname:1521/MY_GLOBAL_SERVICE
```

---

## RAC and Sharding

Oracle Sharding combined with RAC provides extreme scale:

```sql
-- Sharding: horizontally partition data across multiple databases (shards)
-- Each shard is a RAC cluster: high availability within the shard
-- Shard Director routes queries to the correct shard

-- Sharded table (partition key = customer_id)
CREATE SHARDED TABLE customers (
  customer_id  NUMBER NOT NULL,
  region       VARCHAR2(20),
  email        VARCHAR2(100)
)
TABLESPACE SET ts_set_default
PARTITION BY CONSISTENT HASH (customer_id)  -- consistent hash sharding key
PARTITIONS AUTO;

-- Routing: ODP.NET, JDBC Thin, UCP automatically route to correct shard
-- For queries without shard key: coordinator query (hits all shards)
SELECT COUNT(*) FROM customers WHERE region = 'WEST';  -- cross-shard query
```

---

## Split Brain Prevention and Cluster Fencing

The most critical failure scenario in RAC:

```
Split-Brain Scenario:
  Node 1 and Node 2 lose sight of each other over the private interconnect
  Both think the other is dead
  Both try to access the same data files on shared storage
  → Data corruption

Prevention: Voting Disk (quorum)
  Node 1 checks voting disk: "Can I reach the majority of voting disks?"
  If YES: continue; if NO: evict (suicide/fencing)
  
Typical setup: 3 voting disks on separate ASM failure groups
  (2 of 3 must be accessible to continue)
```

```sql
-- Check voting disk location
SELECT voting_file, status FROM v$asm_disk
WHERE voting_file = 'TRUE';

-- Check cluster interconnect health
SELECT name, ip_address, is_public, source
FROM v$cluster_interconnects;

-- If interconnect is misconfigured (accidentally using public network):
-- v$cluster_interconnects should show the PRIVATE network IP
-- not the public IP — high bandwidth, low latency private network is critical
```

---

## RAC Patching Strategy (Zero Downtime)

```bash
# Rolling patch: patch one node at a time while cluster remains up

# Step 1: Relocate all services away from node1
srvctl relocate service -db MYDB -service OLTP_SVC -oldinst MYDB1 -newinst MYDB2

# Step 2: Stop node1 instance gracefully
srvctl stop instance -db MYDB -instance MYDB1 -stopoption IMMEDIATE

# Step 3: Apply patch on node1 OS (while it's down)
# opatch apply /tmp/patch/34765931

# Step 4: Start node1 instance
srvctl start instance -db MYDB -instance MYDB1

# Step 5: Repeat for node2
srvctl relocate service -db MYDB -service OLTP_SVC -oldinst MYDB2 -newinst MYDB1
# ... patch node2 ...
# ... restart node2 ...

# Step 6: Restore service distribution
srvctl relocate service -db MYDB -service OLTP_SVC -oldinst MYDB1 -newinst MYDB2
# Now balanced again across both nodes
```

---

## Advanced RAC Performance: Sequence Contention

```sql
-- The classic RAC sequence contention problem:
-- Default sequence: each NEXTVAL causes a round-trip to the sequence header block
-- In RAC: all instances fight for the same block → gc buffer busy acquire

-- Diagnose: find sequence-related hot blocks
SELECT s.sequence_name, s.cache_size, s.increment_by,
       b.inst_id, b.file#, b.block#, b.ping
FROM dba_sequences s
JOIN gv$bh b ON (b.ping > 50)  -- blocks with 50+ inter-instance transfers
WHERE s.sequence_name IN (
  SELECT UPPER(object_name) FROM dba_objects 
  WHERE object_type = 'SEQUENCE'
)
ORDER BY b.ping DESC;

-- Fix: increase cache size dramatically
ALTER SEQUENCE order_id_seq CACHE 10000;  -- default is 20 — increase 500×
-- Each instance pre-allocates 10000 values locally; no inter-instance contention
-- Downside: up to 10000 × num_instances gaps on restart (acceptable in most cases)

-- Alternative: NOORDER (default for RAC) — values are not in strict sequence order
-- ORDER: guaranteed strict order → forces single-instance allocation → heavy contention
ALTER SEQUENCE order_id_seq NOORDER;  -- unless strict order is truly required
```

---

## Interview Tips

> **Tip 1:** "Explain Application Continuity and when it's needed." — Application Continuity (AC) transparently replays in-flight application requests when a node failure occurs during a database call. Unlike TAF (which only re-establishes the connection), AC replays the entire request from the beginning — all DML, queries, and PL/SQL in the current call — so the application gets its result as if the failure never happened. Use AC when the application can't handle `ORA-3135` (connection lost) and retry logic would be complex to implement.

> **Tip 2:** "What is cluster fencing (node eviction) and why is it necessary?" — Cluster fencing is the forced eviction of a RAC node when it can't communicate with the cluster majority. Without fencing, a "partially alive" node might continue writing to shared storage even after the cluster considers it dead — causing data corruption when the surviving node also writes. Fencing ensures only one authoritative writer to each data block at any time. Oracle implements this via the Cluster Health Monitor and CSS (Cluster Synchronization Services) that evict nodes by cutting off their I/O to storage.

> **Tip 3:** "How do you design a RAC application to minimize global cache contention?" — Key strategies: (1) Avoid hot blocks — use sequence CACHE > 1000, avoid NOORDER sequences if strict ordering isn't needed, use hash partitioned indexes for monotonic keys. (2) Partition data by instance affinity — if possible, route customer A's requests always to instance 1 and customer B's to instance 2 (reduces cross-instance block transfers). (3) Design services — assign OLTP to nodes 1-2, reporting to node 3; reporting queries don't pollute OLTP caches. (4) Minimize lock contention — avoid hot rows (status update counts, running totals); use DBMS_AQ or periodic batch updates instead.
