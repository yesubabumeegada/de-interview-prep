---
title: "AWS MSK - Senior Deep Dive"
topic: aws-services
subtopic: msk
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, msk, kafka, performance, multi-region, cost-optimization]
---

# AWS MSK — Senior-Level Deep Dive

## Cluster Sizing and Performance

### Throughput per Broker Instance

| Instance Type | vCPU | Memory | Network | Max Throughput (recommended) | Use Case |
|--------------|:---:|:---:|:---:|:---:|---|
| kafka.t3.small | 2 | 2 GB | Low | 5 MB/s | Dev/test |
| kafka.m5.large | 2 | 8 GB | Up to 10 Gbps | 30 MB/s | Small production |
| kafka.m5.2xlarge | 8 | 32 GB | Up to 10 Gbps | 80 MB/s | Medium production |
| kafka.m5.4xlarge | 16 | 64 GB | Up to 10 Gbps | 120 MB/s | Large production |
| kafka.m5.12xlarge | 48 | 192 GB | 12 Gbps | 250 MB/s | High-throughput |
| kafka.m7g.large (Graviton) | 2 | 8 GB | Up to 12.5 Gbps | 35 MB/s | Cost-optimized |

### Sizing Formula

```
Required aggregate throughput: T MB/s (write)
Replication factor: RF (typically 3)
Total cluster throughput needed: T × RF

Brokers needed = CEIL((T × RF) / throughput_per_broker)

Example: 100 MB/s write, RF=3, using m5.2xlarge (80 MB/s each)
Brokers = CEIL(100 × 3 / 80) = CEIL(3.75) = 4 brokers

Add headroom (30%): 4 × 1.3 = 6 brokers
Round to multiple of 3 (for AZ balance): 6 brokers ✓

Partition count: MAX(throughput MB/s × 1000 / target_partition_throughput, consumer_parallelism)
For 100 MB/s: ~100-200 partitions across key topics
```

---

## Multi-Region Replication

### MSK Replicator (Native)

```python
# Create cross-region replication
kafka.create_replicator(
    ReplicatorName='us-to-eu-replication',
    ServiceExecutionRoleArn='arn:aws:iam::123:role/MSKReplicatorRole',
    KafkaClusters=[
        {
            'AmazonMskCluster': {'MskClusterArn': 'arn:aws:kafka:us-east-1:...'},
            'VpcConfig': {'SubnetIds': [...], 'SecurityGroupIds': [...]}
        },
        {
            'AmazonMskCluster': {'MskClusterArn': 'arn:aws:kafka:eu-west-1:...'},
            'VpcConfig': {'SubnetIds': [...], 'SecurityGroupIds': [...]}
        }
    ],
    ReplicationInfoList=[{
        'SourceKafkaClusterArn': 'arn:aws:kafka:us-east-1:...',
        'TargetKafkaClusterArn': 'arn:aws:kafka:eu-west-1:...',
        'TopicReplication': {
            'TopicsToReplicate': ['order-events', 'user-events'],
            'CopyTopicConfigurations': True,
            'CopyAccessControlListsForTopics': True,
        },
        'ConsumerGroupReplication': {
            'ConsumerGroupsToReplicate': ['.*'],  # Replicate all consumer offsets
        },
        'TargetCompressionType': 'NONE',  # Preserve original compression
    }]
)
```

**Replication patterns:**

| Pattern | Use Case | Latency |
|---------|----------|---------|
| Active-Passive | DR (one region active, failover on disaster) | 100-500ms |
| Active-Active | Multi-region writes (conflict resolution needed) | 100-500ms |
| Hub-and-Spoke | Central aggregation from multiple regional clusters | 100-500ms |

---

## Performance Tuning

### Producer Optimization

```python
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'acks': 'all',                          # Durability
    'enable.idempotence': True,             # Exactly-once producing
    'compression.type': 'lz4',             # Best throughput/compression ratio
    'batch.size': 65536,                   # 64 KB batches
    'linger.ms': 10,                       # Wait 10ms to fill batch
    'buffer.memory': 134217728,            # 128 MB producer buffer
    'max.in.flight.requests.per.connection': 5,  # Parallel requests (safe with idempotence)
    'delivery.timeout.ms': 120000,         # 2 min total delivery timeout
}
# Expected: 50K+ records/sec per producer instance
```

### Consumer Optimization

```python
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'order-processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,           # Manual commit after processing
    'max.poll.records': 500,               # Records per poll batch
    'fetch.min.bytes': 1048576,            # 1 MB minimum fetch (reduces polls)
    'fetch.max.wait.ms': 500,             # Wait up to 500ms for fetch.min.bytes
    'session.timeout.ms': 45000,          # 45s before declared dead
    'heartbeat.interval.ms': 15000,       # Heartbeat every 15s
    'max.poll.interval.ms': 600000,       # 10 min max between polls
    'partition.assignment.strategy': 'cooperative-sticky',  # Incremental rebalance
}
```

### Broker-Level Tuning (via MSK Configuration)

```
# For high-throughput workloads:
num.io.threads=8
num.network.threads=5
num.replica.fetchers=2
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
log.flush.interval.messages=10000

# For low-latency workloads:
num.io.threads=16
replica.fetch.min.bytes=1
replica.fetch.wait.max.ms=100
```

---

## Cost Optimization Strategies

| Strategy | Savings | Trade-off |
|----------|---------|-----------|
| Graviton instances (m7g vs m5) | 15-20% | Same performance, newer generation |
| Tiered storage | 60-80% on storage | Slightly slower reads for cold data |
| Right-size instances (don't over-provision) | 20-40% | Monitor CPU/network utilization |
| MSK Serverless for variable workloads | 30-50% vs over-provisioned | 200 MB/s cap, less control |
| Reduce replication factor (RF=2 for non-critical) | 33% less storage+network | Lower durability (acceptable for logs) |
| Shorter retention + archive to S3 | 50%+ on storage | Need separate S3 consumer for long-term |

### Cost Comparison at Different Throughput Levels

| Throughput | MSK Provisioned | MSK Serverless | Kinesis |
|-----------|----------------|---------------|---------|
| 5 MB/s | $380/month (3×m5.large) | ~$340/month | ~$580/month |
| 50 MB/s | $770/month (3×m5.large) | ~$1,800/month | ~$2,370/month |
| 200 MB/s | $2,300/month (6×m5.2xlarge) | ~$7,200/month | ~$9,500/month |
| 500 MB/s | $5,500/month (6×m5.4xlarge) | N/A (over cap) | ~$23,700/month |

> **Key insight:** MSK Provisioned is cheapest for steady high throughput. MSK Serverless is cheapest for variable/low utilization. Kinesis is most expensive at scale but simplest to operate.

---

## Exactly-Once Semantics on MSK

```python
# Transactional producer (Kafka transactions)
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    transactional_id='etl-processor-001',  # Unique per producer instance
    enable_idempotence=True,
    acks='all',
)

producer.init_transactions()

# Read-process-write in a single transaction
consumer = KafkaConsumer('raw-events', group_id='etl-group', 
                         enable_auto_commit=False, isolation_level='read_committed')

for message in consumer:
    producer.begin_transaction()
    try:
        result = transform(message.value)
        producer.send('processed-events', value=result)
        producer.send_offsets_to_transaction(
            {TopicPartition(message.topic, message.partition): 
             OffsetAndMetadata(message.offset + 1)},
            'etl-group'
        )
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
```

---

## Operational Runbook

| Scenario | Action |
|----------|--------|
| Broker disk > 85% | Enable tiered storage or increase retention purging |
| Consumer lag growing | Add consumers (up to partition count) or optimize processing |
| Under-replicated partitions | Check broker health (CPU, network, disk I/O) |
| Need more throughput | Add brokers + reassign partitions (rolling operation) |
| Kafka version upgrade | MSK handles rolling upgrade (one broker at a time) |
| Topic partition increase | Use kafka-topics --alter (can't decrease!) |
| Cross-AZ data transfer cost | Keep producers/consumers in same AZ as leader replica |

---

## Interview Tips

> **Tip 1:** "How do you size an MSK cluster?" — "Calculate: write throughput × replication factor = total cluster throughput needed. Divide by per-broker recommended throughput. Add 30% headroom. Round to multiple of 3 (AZ balance). For 100 MB/s writes with RF=3: need 300 MB/s cluster capacity → 4-6 m5.2xlarge brokers."

> **Tip 2:** "How do you achieve exactly-once on MSK?" — "Kafka transactions: transactional producer wraps read+process+write in a single atomic transaction. Consumer reads with `isolation.level=read_committed` to only see committed messages. Combined with idempotent producer (prevents duplicates on retry). This gives end-to-end exactly-once for Kafka-to-Kafka processing."

> **Tip 3:** "MSK cost is growing — how do you optimize?" — "Five levers: (1) Tiered storage (80% storage savings — hot data on EBS, cold on S3). (2) Graviton instances (15-20% cheaper, same performance). (3) Right-size brokers (monitor CPU/network — if <30% utilized, downsize). (4) Reduce replication factor for non-critical topics (RF=2 for logs). (5) Shorter local retention + MSK Connect to S3 for long-term access."

## ⚡ Cheat Sheet

**Cluster Sizing Formula**
- Brokers = CEIL((write_MB_s × RF) / per_broker_throughput) × 1.3 headroom
- Always round to multiple of 3 (AZ balance: 3 brokers, one per AZ)
- Partitions ≥ max concurrent consumers; typical: 1–2 partitions per MB/s throughput
- Example: 100 MB/s write, RF=3 → 300 MB/s cluster → 4 m5.2xlarge × 1.3 → 6 brokers

**Cost Comparison at Scale**
| Throughput | MSK Provisioned | MSK Serverless | Kinesis |
|---|---|---|---|
| 5 MB/s | $380/mo | ~$340/mo | ~$580/mo |
| 50 MB/s | $770/mo | ~$1,800/mo | ~$2,370/mo |
| 500 MB/s | $5,500/mo | N/A (cap) | ~$23,700/mo |
- Provisioned cheapest for steady high throughput; Serverless for variable/dev; Kinesis most expensive at scale

**Cost Reduction Levers**
- Graviton (m7g vs m5): 15–20% cheaper, same performance
- Tiered storage: hot data on EBS, cold on S3 → 60–80% storage savings
- RF=2 for non-critical topics (logs): 33% less storage + replication network cost
- Shorter local retention + MSK Connect to S3 for long-term access

**Exactly-Once (Kafka Transactions)**
- Producer: `transactional_id=unique_per_instance`, `enable_idempotence=true`, `acks=all`
- `init_transactions()` → `begin_transaction()` → produce → `send_offsets_to_transaction()` → `commit_transaction()`
- Consumer: `isolation.level=read_committed` — only sees committed messages

**Producer Tuning**
- `compression.type=lz4` (best throughput/ratio); `batch.size=65536` (64 KB); `linger.ms=10`
- `max.in.flight.requests.per.connection=5` safe with idempotence enabled

**Operational Runbook**
- Disk > 85%: enable tiered storage or reduce retention
- Consumer lag growing: add consumers (up to partition count) or optimize processing
- Under-replicated partitions: check broker CPU/disk I/O
- Never decrease partition count (Kafka doesn't support it)
- Cross-AZ data transfer cost: keep producer/consumer in same AZ as partition leader
