---
title: "AWS MSK - Intermediate"
topic: aws-services
subtopic: msk
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, msk, kafka, connect, schema-registry, configuration, tiered-storage]
---

# AWS MSK — Intermediate Concepts

## MSK Configuration Management

MSK uses custom and default configurations applied at the cluster level:

```python
# Create a custom configuration
kafka = boto3.client('kafka')

config_response = kafka.create_configuration(
    Name='production-config',
    Description='Optimized for data platform workloads',
    KafkaVersions=['3.5.1'],
    ServerProperties=b"""
auto.create.topics.enable=false
default.replication.factor=3
min.insync.replicas=2
num.partitions=12
log.retention.hours=168
log.retention.bytes=-1
compression.type=producer
message.max.bytes=10485760
replica.fetch.max.bytes=10485760
log.segment.bytes=1073741824
log.cleanup.policy=delete
unclean.leader.election.enable=false
"""
)

# Apply to cluster
kafka.update_cluster_configuration(
    ClusterArn='arn:aws:kafka:...',
    ConfigurationInfo={
        'Arn': config_response['Arn'],
        'Revision': 1
    },
    CurrentVersion='K1234567'  # Current cluster version
)
```

**Critical production settings:**

| Setting | Recommended | Why |
|---------|-------------|-----|
| `auto.create.topics.enable` | `false` | Prevent accidental topic creation (typos) |
| `min.insync.replicas` | `2` | With RF=3: survives 1 broker failure without data loss |
| `unclean.leader.election.enable` | `false` | Never elect out-of-sync replica (prevents data loss) |
| `log.retention.hours` | `168` (7 days) | Balance between replay capability and storage cost |
| `compression.type` | `producer` | Let producer choose (usually lz4 or snappy) |

---

## Tiered Storage (MSK 2023+)

Offload cold log segments to S3 automatically — reduces broker storage costs by 80%+:

```python
# Enable tiered storage on a topic
admin.alter_configs(config_resources=[ConfigResource(
    ConfigResourceType.TOPIC, 'order-events',
    configs={
        'remote.storage.enable': 'true',
        'local.retention.ms': '86400000',      # Keep 1 day on local broker storage
        'retention.ms': '2592000000',           # Total retention: 30 days (S3 handles rest)
    }
)])

# Result:
# Day 0-1: data on local SSD (fast reads, recent data)
# Day 1-30: data on S3 (cheap, slightly slower reads)
# Day 30+: deleted (retention expired)
```

**Cost impact:**
```
Without tiered storage (30-day retention):
  3 brokers × 10 TB SSD each = $3,000/month storage

With tiered storage (1-day local, 29 days S3):
  3 brokers × 350 GB SSD = $105/month (hot)
  + S3: 9.65 TB × $0.023/GB = $222/month (cold)
  Total: $327/month (89% savings!)
```

---

## Schema Registry Integration

Use Glue Schema Registry or Confluent Schema Registry with MSK:

### AWS Glue Schema Registry (Native)

```python
from aws_schema_registry import SchemaRegistryClient, DataAndSchema
from aws_schema_registry.avro import AvroSchema

# Configure producer with Glue Schema Registry serializer
schema_registry = SchemaRegistryClient(
    registry_name='data-platform-schemas',
    region='us-east-1'
)

# Define Avro schema
order_schema = AvroSchema("""{
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.company.events",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
    ]
}""")

# Produce with schema validation
producer.send('order-events',
    value=DataAndSchema(data={'order_id': 'O-123', 'amount': 99.99, ...},
                        schema=order_schema)
)
# Schema registered in Glue Catalog — consumers use same registry to deserialize
```

**Benefits of Schema Registry:**
- Schema evolution control (backward/forward compatibility enforcement)
- Schema stored once, referenced by ID in messages (smaller payload)
- Consumer auto-discovers schema by ID (no hardcoded schema in consumer code)
- Audit trail: who changed the schema, when

---

## MSK Auto-Scaling (Provisioned Clusters)

### Storage Auto-Scaling

```python
# Storage auto-scales automatically when enabled (default: enabled)
# When broker storage reaches threshold → MSK adds more EBS capacity
# No downtime, no rebalancing needed

# Configure via cluster creation or update:
# StorageInfo.EbsStorageInfo.VolumeSize = initial size
# MSK automatically scales up (never down) when disk usage > 85%
```

### Broker Count Scaling

```bash
# Add brokers to an existing cluster (manual operation)
aws kafka update-broker-count \
    --cluster-arn arn:aws:kafka:... \
    --current-version K123 \
    --target-number-of-broker-nodes 6

# After adding brokers: must reassign partitions to new brokers!
# Use kafka-reassign-partitions tool or Cruise Control
```

> **Important:** Adding brokers doesn't automatically rebalance data. You must manually reassign topic partitions to the new brokers. This is a significant operational task.

---

## Networking and Security

### Private Access (VPC-Only)

```
MSK clusters are deployed inside your VPC:
- Brokers get private IP addresses (not publicly accessible)
- Clients must be in the same VPC or connected via:
  - VPC Peering
  - Transit Gateway
  - AWS PrivateLink
  - VPN/Direct Connect (for on-premises clients)
```

### Multi-VPC Access (PrivateLink)

```python
# Enable multi-VPC connectivity
kafka.update_connectivity(
    ClusterArn='arn:aws:kafka:...',
    CurrentVersion='K123',
    ConnectivityInfo={
        'VpcConnectivity': {
            'ClientAuthentication': {
                'Sasl': {'Iam': {'Enabled': True}},
                'Tls': {'Enabled': True}
            }
        }
    }
)
# Now other VPCs/accounts can connect via PrivateLink endpoints
```

---

## MSK + AWS Service Integration

| Integration | Pattern | Use Case |
|------------|---------|----------|
| Lambda | Event source mapping (poll-based) | Lightweight per-record processing |
| Glue Streaming | Spark Structured Streaming from MSK | Heavy ETL with Spark |
| Kinesis Data Analytics | Flink application reading from MSK | Stateful stream processing |
| MSK Connect | Managed Kafka Connect workers | CDC, S3 sink, Elasticsearch |
| EventBridge Pipes | MSK → transform → target | Simple routing without custom code |

```python
# Lambda consuming from MSK
lambda_client.create_event_source_mapping(
    EventSourceArn='arn:aws:kafka:...:cluster/my-cluster',
    FunctionName='process-orders',
    Topics=['order-events'],
    StartingPosition='TRIM_HORIZON',
    BatchSize=100,
    MaximumBatchingWindowInSeconds=5,
    SourceAccessConfigurations=[
        {'Type': 'SASL_SCRAM_512_AUTH', 'URI': 'arn:aws:secretsmanager:...:secret:msk-creds'}
    ]
)
```

---

## Monitoring Best Practices

```python
# Key CloudWatch alarms for MSK
alarms = [
    # Cluster health
    {'metric': 'ActiveControllerCount', 'threshold': 1, 'operator': 'LessThan',
     'description': 'No active controller — cluster is unhealthy'},
    {'metric': 'OfflinePartitionsCount', 'threshold': 0, 'operator': 'GreaterThan',
     'description': 'Partitions unavailable — data loss risk'},
    
    # Broker health
    {'metric': 'CpuUser', 'threshold': 80, 'operator': 'GreaterThan',
     'description': 'Broker CPU saturated — add brokers or scale up'},
    {'metric': 'KafkaDataLogsDiskUsed', 'threshold': 85, 'operator': 'GreaterThan',
     'description': 'Disk nearly full — data retention may be truncated'},
    
    # Consumer health
    {'metric': 'MaxOffsetLag', 'threshold': 100000, 'operator': 'GreaterThan',
     'description': 'Consumer falling behind — investigate processing lag'},
]
```

---

## Interview Tips

> **Tip 1:** "How do you manage schemas on MSK?" — "AWS Glue Schema Registry for AWS-native teams (integrates with Glue, no separate infrastructure). Confluent Schema Registry for teams already using Confluent ecosystem. Both enforce backward/forward compatibility, store schemas centrally, and embed schema IDs in messages so consumers auto-discover the correct schema."

> **Tip 2:** "How does tiered storage work on MSK?" — "Hot data (recent, configured by local.retention.ms) stays on broker EBS for fast access. Older data automatically moves to S3 (cheap, unlimited). Consumers read seamlessly from both tiers — MSK handles the routing transparently. This typically reduces storage costs by 80%+ for long-retention topics."

> **Tip 3:** "What's the operational difference between MSK and self-managed Kafka?" — "MSK handles: broker provisioning, OS patching, ZooKeeper/KRaft management, EBS volume management, minor version upgrades, and multi-AZ deployment. You still handle: topic creation/configuration, partition reassignment when scaling, consumer group management, schema registry, and monitoring alerts. It's about 70% less ops work than fully self-managed."
