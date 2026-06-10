---
title: "ZooKeeper - Senior Deep Dive"
topic: hadoop
subtopic: zookeeper
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hadoop, zookeeper, performance, observers, rolling-upgrade, etcd, consul, anti-patterns]
---

# ZooKeeper — Senior Deep Dive

## ZooKeeper Performance Limits

ZooKeeper is optimized for reads and small writes. Understanding its limits is critical for capacity planning:

| Metric | Typical Limit | Why |
|--------|--------------|-----|
| Write throughput | ~10,000 ops/sec | Leader bottleneck; ZAB serializes writes |
| Read throughput | ~100,000 ops/sec | Each server handles reads independently |
| ZNode data size | 1 MB (default) | jute.maxbuffer setting |
| Max ZNode children | Unlimited but ~10k practical | Listing causes network spike |
| Session timeout min | 2 * tickTime | Hard lower bound |
| Ensemble size | 3, 5, or 7 | More = slower writes |

```bash
# Monitor ZooKeeper performance
echo mntr | nc zk-host 2181 | grep -E "(latency|watch|count)"
# zk_avg_latency     2
# zk_max_latency     15
# zk_min_latency     0
# zk_watch_count     450
# zk_outstanding_requests  0
# zk_packets_received  192847
# zk_packets_sent    192845

# Check request latency histogram
echo stat | nc zk-host 2181 | grep -E "(Latency|Connections)"
# Latency min/avg/max: 0/1/15
# Connections: 23
```

## Watch Semantics and the Herd Effect

### Watch Semantics

```
Important watch rules:
1. One-shot: fires once, then automatically deregistered
2. Ordered: events are delivered in the order they occurred
3. Guaranteed: between a watch fire and re-registration, you might miss changes
4. Lightweight: watcher is a session-level registration (very cheap)

The "watch gap" problem:
  Time 0: Client gets data + registers watch
  Time 1: Data changes → watch fires → client notified
  Time 2: Data changes AGAIN (before client re-registers watch)
  Time 3: Client re-registers watch (misses Time 2 change!)

Solution: Always re-read data AND re-register watch atomically:
  getData("/config", watcher)  // Returns current value AND registers watch
  // The returned value is always consistent with the watch
```

### The Herd Effect

```
Problem: When a popular ZNode changes, ALL watchers are notified.

Example:
  - 1000 clients watching /election/leader
  - Leader crashes, ZNode deleted
  - All 1000 clients get NodeDeleted event simultaneously
  - All 1000 try to create /election/leader at once
  - ZooKeeper gets 1000 create requests in a burst
  - 999 fail with NodeExistsException
  - Resource waste, latency spike

Solution for leader election: Sequential ephemeral ZNodes
  - Client 1 creates /election/candidate-0000000001
  - Client 2 creates /election/candidate-0000000002
  - Client N creates /election/candidate-000000000N
  - Only Client 1 (smallest seq) is leader
  - Each non-leader watches ONLY its predecessor
  - When Client 1 crashes, only Client 2 is notified
  - Only ONE client reacts per failover event
```

## Observer Nodes

Observers are read-only ZooKeeper servers that don't participate in votes. They reduce leader write latency by not requiring ACKs in the quorum:

```
Regular ensemble (5 servers):
  Leader needs ACK from 3 of 5 to commit
  Each write waits for 3 network round trips

With observers (3 voters + 2 observers):
  Leader needs ACK from only 2 followers (quorum = 2 of 3 voters)
  Observers receive COMMIT after quorum but don't vote
  Reads scale horizontally with observers
  Write latency improves (fewer ACKs to wait for)

Use case: Cross-datacenter setups
  - DC1: 3 voting servers
  - DC2: 2 observer servers
  - No cross-DC write latency (observers don't vote)
  - DC2 clients can read locally
```

```bash
# zoo.cfg: configure observers
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
server.4=zk4:2888:3888:observer   # observer in DC2
server.5=zk5:2888:3888:observer   # observer in DC2
```

## Rolling Upgrades of ZooKeeper Ensemble

```
Rolling upgrade procedure (zero downtime):

1. Upgrade followers one at a time:
   a. Stop follower
   b. Install new ZooKeeper version
   c. Update zoo.cfg if needed
   d. Start follower
   e. Wait for follower to rejoin ensemble (check "stat" for Followers count)
   f. Wait 60 seconds to verify stability
   g. Repeat for next follower

2. Upgrade the leader last:
   a. Trigger leader resignation: zkServer.sh restart (forces re-election)
   b. Old leader becomes follower
   c. Upgrade old leader (now a follower)
   d. Restart and rejoin

Verification at each step:
  echo stat | nc zk-host 2181 | grep "Mode"  # Should show leader or follower
  echo ruok | nc zk-host 2181               # Should return "imok"
```

## ZooKeeper vs etcd vs Consul

| Dimension | ZooKeeper | etcd | Consul |
|-----------|-----------|------|--------|
| Protocol | ZAB | Raft | Raft |
| API | Custom ZK protocol | gRPC/HTTP | HTTP/DNS |
| Watch semantics | One-shot watches | Streaming watch (always on) | Blocking HTTP long-poll |
| Data model | Hierarchical ZNodes | Flat key-value | Key-value + service catalog |
| Max value size | 1 MB | 1.5 MB (configurable) | 512 KB |
| Write throughput | ~10K/sec | ~10K-50K/sec | ~5K/sec |
| Native service discovery | Via patterns | Via lease | Native feature |
| Kubernetes default | No | Yes (k8s uses etcd) | Via Helm |
| Hadoop/HBase support | Native | No | No |
| Health checks | Via ephemeral | Via lease TTL | Native HTTP/TCP/script |
| ACL/Security | Basic ACL | RBAC + TLS | ACL + mTLS |
| Operational complexity | High | Medium | Medium |

**When to use each:**
- **ZooKeeper**: Hadoop ecosystem, HBase, Kafka (legacy), deep integration with JVM-heavy infrastructure
- **etcd**: Kubernetes control plane, cloud-native apps, streaming watches needed
- **Consul**: Service mesh, multi-datacenter service discovery, health-check-driven discovery

## Common ZooKeeper Anti-Patterns

### Anti-Pattern 1: Using ZooKeeper as a Message Queue

```
WRONG: Storing 1000s of items in ZK as children of /queue/
- Listing /queue/ returns all children (network spike)
- ZK not designed for high-throughput queuing
- Use Kafka instead

RIGHT: ZK for coordination only
- Store queue metadata (head pointer, configuration) in ZK
- Store queue items in Kafka or HDFS
```

### Anti-Pattern 2: Storing Large Data in ZNodes

```
WRONG: store 10MB blob in a ZNode
  zk.setData("/config/bigfile", largeByte, -1)  // fails! 1MB limit

RIGHT: store a reference to the large data
  # Store pointer in ZK
  zk.setData("/config/data-location", "/hdfs/configs/app.json".encode(), -1)
  # Read actual data from HDFS/S3
```

### Anti-Pattern 3: Too Many Watches

```
WRONG: Each of 10,000 clients watches all 1,000 service ZNodes
  → 10M watch registrations
  → Server runs out of memory for watch metadata

RIGHT: Use a ZNode aggregator or hierarchical watching
  - Clients watch /services (1 child change event for any update)
  - Re-fetch the full list of children only when notified
```

### Anti-Pattern 4: Tight Connection Loops

```
WRONG: Reconnecting immediately on disconnect
  while (true):
      try: connect_to_zk()
      except: continue  # infinite retry without backoff

RIGHT: Exponential backoff
  wait = 1
  while not connected:
      try: connect_to_zk()
      except:
          time.sleep(wait)
          wait = min(wait * 2, 60)  # max 60 second backoff
```

## Distributed Barriers and Queues

### Barrier Pattern

```python
# Double barrier: wait for N processes to arrive, then all proceed together
from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier

zk = KazooClient(hosts='zk1:2181')
zk.start()

# All processes wait at the barrier
barrier = Barrier(zk, "/barriers/job-start")

# Process registers and waits for N participants
barrier.create()  # Register this process
print("Waiting for all processes...")
barrier.wait()    # Blocks until N processes have joined
print("All processes ready, proceeding!")
barrier.remove()  # Deregister
```

### Priority Queue Pattern

```python
# Sequential ZNodes automatically implement FIFO queue
# For priority queue, encode priority in the ZNode name

import struct
from kazoo.client import KazooClient

zk = KazooClient(hosts='zk1:2181')
zk.start()

def enqueue(priority, data):
    """priority: 1=high, 10=low"""
    # Zero-pad priority so string sort works
    name = f"/queue/item-{priority:010d}-"
    zk.create(name, data.encode(), ephemeral=True, sequence=True)

def dequeue():
    children = sorted(zk.get_children("/queue"))
    if not children:
        return None
    # First child has highest priority (lowest number)
    path = f"/queue/{children[0]}"
    data, _ = zk.get(path)
    zk.delete(path)
    return data.decode()

enqueue(1, "urgent job")
enqueue(5, "normal job")
enqueue(10, "low priority job")
print(dequeue())  # Returns "urgent job"
```

## Interview Tips

> **Tip 1:** The write throughput limit of ~10K ops/sec is the key ZooKeeper scalability constraint. Above this level, ZooKeeper itself becomes a bottleneck. The solution is either to reduce coordination frequency (batch updates), use observers to serve more reads, or switch to a more horizontally scalable system for that use case.

> **Tip 2:** The distinction between etcd and ZooKeeper is important for senior interviews. etcd's streaming watches (always-on, not one-shot) simplify client code significantly. ZooKeeper's one-shot watches require careful re-registration and can miss events in the gap. etcd is the industry direction for new systems.

> **Tip 3:** Observers are an often-overlooked scaling technique. If asked "how do you scale ZooKeeper reads without increasing write latency?", observers are the answer. They receive all updates but don't vote, so write latency depends only on the voting quorum size.

> **Tip 4:** When explaining ZAB vs Raft (used by etcd/Consul), the practical difference is: ZAB is designed as a primary-backup replication protocol (all reads can go through any server); Raft is a general consensus protocol. Both provide the same consistency guarantees for the coordinator use case.

> **Tip 5:** The most common ZooKeeper production incident is session expiration cascades — when the ZooKeeper cluster is overloaded, it can't respond to heartbeats fast enough, causing many client sessions to expire simultaneously. This triggers mass leader elections across all dependent services (HBase, Kafka, YARN). Monitor `zk_outstanding_requests` — if it's consistently > 0, you're overloading ZooKeeper.

## ⚡ Cheat Sheet

**HDFS architecture**
```
NameNode:   stores metadata (file → block mappings, permissions, namespace)
DataNode:   stores actual data blocks (default 128 MB per block)
Replication: default factor 3 (two local rack + one remote rack)
HA:         Active/Standby NameNode with JournalNodes for edit log sharing
```

**HDFS key commands**
```bash
hdfs dfs -ls /data/warehouse          # list files
hdfs dfs -put local.csv /data/raw/    # upload
hdfs dfs -get /data/output/ ./local/  # download
hdfs dfs -rm -r /data/tmp/            # delete
hdfs dfs -du -s -h /data/warehouse/   # disk usage
hdfs dfs -copyFromLocal -f src dst    # overwrite on upload
hdfs fsck /path -files -blocks        # check file health
```

**YARN resource model**
```
ResourceManager:  cluster master — allocates containers
NodeManager:      per-node agent — runs containers, reports health
ApplicationMaster: per-job — negotiates resources with RM
Container:        allocated unit (CPU cores + memory)

Scheduler types: FIFO, Capacity Scheduler (queues), Fair Scheduler
```

**Hive vs Spark SQL**
```
Hive:      MapReduce by default (slow); good for compatibility; HQL ≈ SQL
Hive LLAP: in-memory daemon; much faster (sub-minute queries)
Spark SQL:  Hive Metastore compatible but Spark execution — 10-100x faster
```

**Hive partitioning**
```sql
CREATE TABLE orders (order_id BIGINT, amount DOUBLE)
PARTITIONED BY (dt STRING, region STRING)
STORED AS PARQUET;
-- Dynamic partition insert
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO orders PARTITION (dt, region)
SELECT order_id, amount, dt, region FROM staging_orders;
```

**MapReduce pattern**
```
Map:    input splits → emit (key, value) pairs
Shuffle: sort + group by key across nodes
Reduce: aggregate values per key → output
Use case today: Hive compatibility, very large batch on older clusters
```

**ZooKeeper use cases in Hadoop**
```
HBase region assignment  — ZK tracks which RegionServer owns which region
HDFS NameNode HA         — ZK elects Active NameNode
YARN RM HA               — ZK elects Active ResourceManager
Kafka broker coordination — ZK stores broker/topic metadata (pre-KRaft)
```

**HBase data model**
```
Table → Row → Column Family → Column Qualifier → Value (versioned by timestamp)
Row key design is critical: avoid hot-spotting (don't use sequential IDs)
Strategies: salt prefix, reverse timestamp, MD5 hash of natural key
```

**Key interview points**
- HDFS is optimized for large files, sequential reads; terrible for many small files
- Sqoop: parallel JDBC import from RDBMS to HDFS/Hive (one mapper per table partition)
- Oozie: XML-based workflow scheduler (predecessor to Airflow in Hadoop ecosystem)
- Pig: dataflow language (Latin) — pre-dbt/Spark era; rarely used in modern stacks
- Ecosystem today: HDFS + YARN still used, but S3/GCS replacing HDFS in cloud-native stacks
