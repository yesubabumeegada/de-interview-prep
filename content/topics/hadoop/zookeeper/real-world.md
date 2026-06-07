---
title: "ZooKeeper - Real World"
topic: hadoop
subtopic: zookeeper
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, zookeeper, hbase, kafka, hdfs-ha, production, split-brain]
---

# ZooKeeper — Real-World Patterns

## HBase Region Server Failover

When an HBase region server crashes, ZooKeeper detects the failure and triggers region reassignment:

```
Failover Timeline:
t=0:    RegionServer RS3 experiences GC pause / network partition
t=30s:  ZooKeeper session timeout expires for RS3
t=30s:  /hbase/rs/RS3 ephemeral ZNode is deleted automatically
t=30s:  HBase Master is watching /hbase/rs (children watch)
t=30s:  Master receives NodeDeleted event for RS3
t=31s:  Master identifies which regions were assigned to RS3
t=31s:  Master reassigns RS3's regions to RS1 and RS2
t=45s:  Regions are OPEN on new servers and accepting requests
t=45s:  Clients auto-discover new region locations via HBase client
```

```bash
# Monitor HBase region servers via ZooKeeper
zkCli.sh -server zk1:2181 << 'EOF'
# Watch for region server changes
ls -w /hbase/rs
# You'll get notified when any RS joins or leaves

# Check dead servers (HBase tracks these separately)
ls /hbase/draining
ls /hbase/unassigned  # Regions waiting for assignment
EOF

# HBase-level: identify regions that need reassignment after failure
hbase shell << 'EOF'
# Show all regions and their servers
scan 'hbase:meta', {COLUMNS => ['info:regioninfo', 'info:server']}

# Force a manual region assignment if auto-assignment is stuck
assign '07ec1a24a6b56a83a5b00a5e2d4d5c1e.2342093a3ede9b3ba04b6dd6d5dcb3a9.'
EOF
```

## Kafka Broker Registration and Controller Election

```bash
# View Kafka ZooKeeper structure
zkCli.sh -server zk1:2181 << 'EOF'
ls /kafka/brokers/ids                       # Active broker IDs
ls /kafka/brokers/topics                    # All topics
ls /kafka/controller                        # Current controller
ls /kafka/admin/delete_topics               # Topics pending deletion
ls /kafka/isr_change_notification           # ISR change events
EOF

# Inspect broker details
zkCli.sh -server zk1:2181 << 'EOF'
get /kafka/brokers/ids/0
# Returns: {"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},
#           "endpoints":["PLAINTEXT://kafka-broker-0.corp:9092"],
#           "jmx_port":9999, "host":"kafka-broker-0.corp",
#           "timestamp":"1705276800000", "port":9092, "version":4}

get /kafka/controller
# Returns: {"version":2,"brokerid":0,"timestamp":"1705276800000"}
EOF
```

```
Kafka Controller Election Flow:
1. All brokers try to create /kafka/controller (ephemeral)
2. First broker to succeed becomes controller
3. Controller manages partition leadership, ISR tracking, broker membership
4. If controller broker dies:
   - ZK session expires
   - /kafka/controller deleted
   - All brokers watch /kafka/controller
   - Race to create → new controller elected
   - Takes ~10-30 seconds for ISR rebalancing to complete
```

## NameNode HA with QJM vs ZooKeeper

Hadoop NameNode HA has two complementary uses of ZooKeeper:

```
Component 1: Quorum Journal Manager (QJM)
  - NOT ZooKeeper — a separate cluster of 3+ JournalNodes
  - Stores HDFS edit logs
  - Ensures both NameNodes can read the shared edit log
  - 2n+1 JournalNodes for fault tolerance

Component 2: ZooKeeper (via ZKFC)
  - Used ONLY for automatic failover
  - Each NN has a co-located ZKFC process
  - ZKFC checks NN health via local RPC
  - ZKFC holds an ephemeral ZNode if NN is healthy and active
  - If active NN's ZKFC loses its ZNode → standby ZKFC creates its own → becomes active

Architecture:
  NameNode Active → ZKFC1 → ZooKeeper (/hadoop-ha/nn)
  NameNode Standby → ZKFC2 → ZooKeeper (watches /hadoop-ha/nn)
  Both NNs → JournalNode Cluster (for edit log sync)
```

```bash
# Check NameNode HA status
hdfs haadmin -getServiceState nn1   # Returns: active
hdfs haadmin -getServiceState nn2   # Returns: standby

# Verify ZooKeeper sees the active NN lock
zkCli.sh -server zk1:2181 <<< "get /hadoop-ha/ns1/ActiveStandbyElectorLock"

# Manual failover
hdfs haadmin -failover nn1 nn2      # Graceful: nn1 → standby, nn2 → active

# Force failover (if active NN is unresponsive)
hdfs haadmin -failover --forcefence --forceactive nn1 nn2
```

## ZooKeeper in Production: Sizing, Monitoring, Backup

### Sizing Guidelines

```
Memory:
  Each ZNode: ~300 bytes overhead (even if empty data)
  10,000 ZNodes ≈ 3 MB heap
  1M ZNodes ≈ 300 MB heap
  Typical production: 4-8 GB heap for ZK JVM

Storage:
  Transactions log: append-only (clean up via snapshots)
  Snapshots: periodic full state dump
  Recommended: dedicated SSD or fast disk (low latency writes critical)

Network:
  Ensemble members communicate on ports 2888 (data) and 3888 (election)
  Ensure low latency (<1ms) between ensemble members
  For cross-DC: use observers, not voters
```

### Monitoring

```bash
# Key metrics to monitor
echo mntr | nc zk-host 2181 | grep -E "(outstanding|latency|connections|watch)"

# Critical alerts:
# zk_outstanding_requests > 10 (overloaded)
# zk_avg_latency > 10ms (slow)
# zk_max_latency > 100ms (GC or disk issue)
# zk_open_file_descriptor_count > 80% of max

# Grafana/Prometheus metrics via ZK exporter
# Common exporters: zookeeper-exporter, prometheus-zookeeper-exporter
```

### Backup and Recovery

```bash
# ZooKeeper data backup (snapshot + transaction logs)
BACKUP_DIR="/backup/zookeeper/$(date +%Y-%m-%d)"
mkdir -p $BACKUP_DIR

# Copy snapshot and transaction logs
rsync -av /var/zookeeper/data/version-2/ ${BACKUP_DIR}/data/
rsync -av /var/zookeeper/datalog/version-2/ ${BACKUP_DIR}/datalog/

# Restore ZooKeeper from backup
# 1. Stop ZooKeeper
zkServer.sh stop
# 2. Clear current data
rm -rf /var/zookeeper/data/version-2/*
# 3. Copy backup
cp -r ${BACKUP_DIR}/data/* /var/zookeeper/data/version-2/
cp -r ${BACKUP_DIR}/datalog/* /var/zookeeper/datalog/version-2/
# 4. Start ZooKeeper
zkServer.sh start

# Automatic snapshot cleanup (prevent disk exhaustion)
# In zoo.cfg:
# autopurge.snapRetainCount=5
# autopurge.purgeInterval=24  # hours
```

## Split-Brain Prevention

Split-brain occurs when network partition makes two nodes believe they are each the sole leader:

```
Split-Brain Scenario:
  Network partitioned: [NameNode1 + ZK1] | [NameNode2 + ZK2 + ZK3]

  Partition 1: NN1 + ZK1 (minority: 1 of 3 ZK servers)
    - NN1's ZKFC tries to maintain ZK session
    - ZK1 alone cannot maintain quorum
    - NN1's ZK session expires
    - NN1's ZKFC cannot refresh the /leader ZNode
    - NN1 ZKFC fences NN1 (forces it to stop accepting writes)

  Partition 2: NN2 + ZK2 + ZK3 (majority: 2 of 3 ZK servers)
    - NN2's ZKFC successfully creates /leader ZNode
    - NN2 becomes active
    - NN2 starts accepting writes

Result: Only NN2 accepts writes. No split-brain!
ZooKeeper quorum requirement prevents split-brain automatically.
```

### Fencing Mechanisms

```bash
# HDFS fencing: prevent old active from writing after failover
# Configured in hdfs-site.xml:
# <property>
#   <name>dfs.ha.fencing.methods</name>
#   <value>sshfence(hdfs@nn1.corp)
#          shell(/usr/local/bin/fence_nn.sh)</value>
# </property>

# fence_nn.sh example
#!/bin/bash
# Last resort: force kill the NameNode process
TARGET_HOST=$1
ssh hdfs@${TARGET_HOST} "pkill -9 -u hdfs -f NameNode || true"
exit 0
```

## Interview Tips

> **Tip 1:** When explaining HBase region server failover, the key timeline is: ZK session timeout (30s default) → ZKFC/Master detects via NodeDeleted event → regions reassigned. The 30-second window of unavailability for affected regions is the SLA cost of region server failures. Reduce session timeout to decrease downtime (but risk false failovers on GC pauses).

> **Tip 2:** QJM (Quorum Journal Manager) vs ZooKeeper in HDFS HA confuses many candidates. They serve different purposes: QJM stores the shared edit log (data plane), ZooKeeper manages who is active (control plane). You need both for automatic HA.

> **Tip 3:** Split-brain prevention is elegant in ZooKeeper: because ZK requires a quorum for writes, the minority partition's ZK session expires and the ZKFC fences the old active. This is automatic and doesn't require manual intervention. The key insight: ZooKeeper itself doesn't prevent split-brain — the ZKFC using ZooKeeper does.

> **Tip 4:** ZooKeeper storage must be on a dedicated disk or fast SSD. ZK writes transaction logs synchronously (fsync before ACK). If ZK shares disk with other I/O-heavy processes (HDFS DataNode, HBase RegionServer), disk contention causes latency spikes, which can trigger false session expirations.

> **Tip 5:** For the Kafka question: ZooKeeper is being replaced by KRaft (Kafka Raft Metadata) mode starting in Kafka 2.8+, and ZooKeeper is fully deprecated in Kafka 3.5. If you're designing a new Kafka deployment, use KRaft mode. In interviews, show awareness of this transition.
