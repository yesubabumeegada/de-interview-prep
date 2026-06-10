---
title: "RAC — Scenarios"
topic: oracle
subtopic: rac
content_type: scenario_question
tags: [oracle, rac, interview, scenarios, failover, troubleshooting]
---

# RAC — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: What Happens When a RAC Node Fails?

**Scenario:** Your 3-node RAC cluster has node2 fail suddenly at 3am due to a hardware crash. Walk through exactly what Oracle does and what the impact is on applications.

<details>
<summary>💡 Hint</summary>

RAC's failure detection is driven by CSS (Cluster Synchronization Services) heartbeats to voting disks. When a node misses heartbeats, CSS *fences* the node (revokes its storage access) and evicts it — this prevents a zombie node from corrupting data. Trace through the five steps: CSS detects failure → fencing → surviving node performs instance recovery (reads failed node's redo log from shared storage) → services failover → SCAN listeners stop routing to failed node. Think about where in-flight transactions go.

</details>

<details>
<summary>✅ Solution</summary>

**Automatic Oracle response (within seconds):**

1. **CSS detects failure (~10 sec):** Each node sends heartbeats to voting disks and other nodes. When node2 misses heartbeats, CSS declares it dead and evicts it from the cluster.

2. **Node fencing:** Node2's access to shared storage is revoked — prevents any lingering node2 process from corrupting data.

3. **Instance recovery (~30-120 sec):** Node1 or node3 reads node2's online redo logs from shared storage and:
   - Rolls forward committed transactions that aren't in data files yet
   - Rolls back uncommitted transactions from node2
   
4. **Service failover:** Services that were preferred on node2 restart on node1 or node3 (per service configuration).

5. **SCAN listener update:** SCAN listeners stop routing new connections to node2.

**Application impact:**
```
Active connections on node2: ORA-3135 (connection lost) — received by application
With TAF configured:     → Transparent reconnect to node1/node3 (application may see a brief pause)
With Application Continuity: → In-flight request replayed (appears seamless to application)
Without TAF/AC:          → Application must handle connection error and retry

New connections:         → SCAN routes to node1 and node3 only (no impact)
Committed transactions:  → All committed — fully durable (already in redo log on shared storage)
Uncommitted transactions → Rolled back during instance recovery
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Troubleshoot RAC Performance Degradation

**Scenario:** After adding node3 to a 2-node RAC, performance got WORSE instead of better. `gc buffer busy acquire` waits increased 10×. What's wrong?

<details>
<summary>💡 Hint</summary>

`gc buffer busy acquire` means nodes are contending over the same data blocks — Cache Fusion is working too hard. Adding a third node increases interconnect traffic because the same hot blocks now get passed between three nodes instead of two. The usual suspects: (1) the interconnect is accidentally routing through the public network instead of a private NIC, (2) hot sequences with low cache values cause every `nextval` to generate interconnect traffic, or (3) the application has a hotspot table/block that all nodes are updating (like a sequence-backed primary key). Check `v$cluster_interconnects` for public routing first — it's the fastest win.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Check the interconnect is private**
```sql
SELECT name, ip_address, is_public
FROM v$cluster_interconnects;
-- If is_public = 'TRUE' → PROBLEM! Cache Fusion traffic on public network
```

**Step 2: Check sequence cache sizes**
```sql
SELECT sequence_name, cache_size, increment_by, order_flag
FROM dba_sequences
WHERE cache_size < 100
ORDER BY cache_size;
-- If PRIMARY_KEY_SEQ has cache_size = 20 → every instance fights for the header block
-- Fix:
ALTER SEQUENCE primary_key_seq CACHE 10000 NOORDER;
```

**Step 3: Check for application routing issues**
```sql
-- Are all connections going to all 3 nodes (causing hot block sharing)?
SELECT inst_id, COUNT(*) sessions
FROM gv$session WHERE type = 'USER'
GROUP BY inst_id ORDER BY inst_id;

-- If application bypassed SCAN and hardcoded VIPs → all nodes serving same users
-- → same blocks needed on all instances → high Cache Fusion traffic
```

**Step 4: Check if node3 is causing new access patterns**
```sql
-- Find which SQL statements generate the most gc waits
SELECT s.sql_id, s.inst_id, 
       COUNT(*) ash_gc_waits,
       SUBSTR(q.sql_text, 1, 80) preview
FROM gv$active_session_history s
JOIN gv$sql q ON s.sql_id = q.sql_id AND s.inst_id = q.inst_id
WHERE s.sample_time > SYSDATE - 1/24
  AND s.event LIKE 'gc%'
GROUP BY s.sql_id, s.inst_id, q.sql_text
ORDER BY ash_gc_waits DESC
FETCH FIRST 10 ROWS ONLY;
```

**Most likely root causes for this scenario:**
1. Node3 using the public network for interconnect (misconfigured during installation)
2. Sequences with small cache — adding node3 tripled contention
3. Application not using services — all nodes serving the same workload with no affinity

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design RAC + Data Guard Architecture

**Scenario:** Design a production database architecture for a banking application: 99.99% availability (< 52 min downtime/year), < 1 second data loss, < 5 minute RTO (recovery time), multi-datacenter, 20TB database, 5,000 concurrent transactions/second peak.

<details>
<summary>💡 Hint</summary>

99.99% availability + <1s data loss + <5min RTO requires two technologies working together: RAC for node-level HA within a datacenter (transparent failover to surviving nodes) and Data Guard with synchronous transport for cross-datacenter protection (LGWR SYNC commits only after standby acknowledges — guarantees no data loss). For 5,000 TPS at low latency, the interconnect must be a dedicated 25GbE private network. Fast-Start Failover with an Observer provides the <5min RTO automatically. Think about the standby as 2-node RAC too (not single-node) so the DR site itself has HA.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**
```
Primary Site (DC1):
  RAC Cluster: 4 nodes × (16 core / 256GB RAM / NVMe flash)
  ASM: DATA diskgroup (mirrored, 20TB) + RECO diskgroup (FRA)
  Services: OLTP_SVC (preferred: node1-2), BATCH_SVC (node3-4)
  
Standby Site (DC2, 50km away):
  Data Guard Standby: 2-node RAC (for HA within standby site too)
  SYNC redo transport (LGWR SYNC) → 0 data loss on commit
  Active Data Guard: standby open READ ONLY for reporting
  
Connection:
  Dedicated fiber link: 10Gb/s × 2 (LACP bonded)
  Latency: <5ms (same metro area enables SYNC transport without performance hit)
```

**Data Guard configuration for 0 data loss:**
```sql
-- Primary: configure maximum availability protection
ALTER DATABASE SET STANDBY DATABASE TO MAXIMIZE AVAILABILITY;
-- (vs MAXIMIZE PROTECTION: blocks primary if standby unreachable)
-- (vs MAXIMIZE PERFORMANCE: async redo, potential data loss)

-- Redo transport: SYNC ensures standby confirms receipt before commit returns
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 = 
  'SERVICE=STANDBY_DB SYNC AFFIRM
   VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
   DB_UNIQUE_NAME=MYDB_STANDBY';

-- Verify sync transport lag
SELECT dest_name, status, archived_seq#, applied_seq#,
       (archived_seq# - applied_seq#) lag_logs
FROM v$archive_dest_status
WHERE target = 'STANDBY';
```

**RTO < 5 minutes — Data Guard Broker failover:**
```
Manual failover:  DGMGRL> FAILOVER TO MYDB_STANDBY;  (2-3 minutes)
Fast-Start Failover: automatic failover when primary unreachable (30-60 sec default)

dgmgrl> ENABLE FAST_START FAILOVER;
dgmgrl> SET FAST_START FAILOVER THRESHOLD 30;  -- fail over after 30s primary unreachable
```

**Availability calculation:**
```
RAC (4 nodes): single node failure = 0 downtime (surviving nodes continue)
Any 2 of 4 nodes fail = 0 downtime
All 4 nodes fail = fast-start failover to standby: ~30-60 sec
Standby switch back: manual ~ 10 minutes (planned)

99.99% = 52 min/year budget:
  - Monthly rolling patches: 4 × 5 min = 20 min (RAC rolling, zero downtime for app)
  - Unplanned site failover: 1-2 events × 1 min each = ~2 min
  Total estimated: ~22 min → well within 52 min budget
```

**Key design decisions:**
- SYNC transport (not ASYNC): 5ms fiber allows synchronous redo without significant latency hit on COMMIT
- 4-node RAC: provides N+2 redundancy — lose any 2 nodes and stay up
- Active Data Guard: standby also serves reporting (no separate reporting database needed)
- Fast-Start Failover: automatic promotion meets RTO < 5 min without 3am DBA page

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is Oracle RAC and what problem does it solve?**
A: Real Application Clusters (RAC) runs multiple Oracle instances against a single shared database on a shared-disk storage system. It eliminates single-instance as a failure point (active-active HA) and provides horizontal scale-out: you add nodes to handle more concurrent sessions and workload without a maintenance window.

**Q: What is Cache Fusion and why is it central to RAC?**
A: Cache Fusion is the mechanism by which RAC nodes share modified data blocks over the high-speed private interconnect instead of flushing them to disk first. When Node B needs a block held by Node A's buffer cache, Node A ships it directly over InfiniBand—eliminating disk I/O and providing a coherent, distributed buffer cache.

**Q: What is a GCS (Global Cache Service) and a GES (Global Enqueue Service)?**
A: GCS manages the distributed buffer cache coherency—tracking block ownership, shipping blocks between nodes, and managing block mode (exclusive/shared). GES manages global enqueues (locks and latches) that protect shared resources across all instances, ensuring only one session modifies a block at a time.

**Q: What is an interconnect and why is its performance critical?**
A: The private interconnect (typically InfiniBand) carries Cache Fusion block transfers and GES messaging between nodes. Slow or saturated interconnect directly increases wait times for `gc buffer busy acquire`, `gc cr block 2-way`, and `gc current block 2-way` waits, degrading RAC performance to worse than single-instance.

**Q: What are services in RAC and why should applications connect via services?**
A: A RAC Service is a named workload unit that can be configured to run on specific preferred/available instances, fail over, and load-balance connections via Oracle Client (TAF/FCF). Applications connecting via service name (not instance name) are automatically routed to available instances and reconnected on failure.

**Q: What is Transparent Application Failover (TAF) and Fast Connection Failover (FCF)?**
A: TAF is a client-side Oracle Net feature that transparently reconnects a session to another instance on failure and optionally re-executes the last SELECT. FCF (with ONS/FAN events) is faster—the client receives a failover notification proactively rather than waiting for a connection timeout, enabling sub-second failover for properly configured connection pools.

**Q: What AWR/ASH metrics indicate RAC interconnect problems?**
A: High values for `gc buffer busy acquire`, `gc cr block 2-way`, `gc current block 2-way`, and `gc current block congested` in the top wait events section. Also check the "Global Cache and Enqueue Statistics" section for high `average cr block receive time` (should be <1 ms on InfiniBand).

**Q: How does application design affect RAC performance?**
A: Poor affinity leads to excessive Cache Fusion traffic. Applications should use connection services tied to specific instances for localized workloads (e.g., OLTP on Node 1, reports on Node 2). Sequence caching (`CACHE 1000`), avoiding frequent commits with many small rows, and using partitioned tables with partition-wise operations reduce inter-node block shipping.

---

## 💼 Interview Tips

- Lead with the active-active HA + scale-out value proposition, then immediately go into Cache Fusion—it is what makes RAC technically unique and interviewers always probe it.
- Know the key RAC wait events and what they mean: `gc buffer busy` = another session on another node holds the block; `gc cr block 2-way` = read-consistent block shipped from remote node. Pattern-matching waits to root causes impresses senior interviewers.
- Services vs. instance-name connections is a critical operational point: direct instance connections bypass load balancing and TAF—this is a common misconfiguration in legacy applications.
- Senior interviewers often ask about anti-patterns: hot sequence values causing `SQ` enqueue contention (fix: high cache + NOORDER), row-chaining across blocks causing excessive Cache Fusion traffic (fix: proper row sizing and PCTFREE).
- Connect RAC to DR: RAC provides HA within a datacenter; Data Guard provides DR across datacenters. The two are complementary—MAX AVAILABILITY configuration combines both.
