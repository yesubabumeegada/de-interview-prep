---
title: "Scalability — Scenarios"
topic: system-design
subtopic: scalability
content_type: scenario_question
tags: [scalability, horizontal-scaling, scenarios]
---

# Scalability — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Horizontal vs Vertical Scaling for Data Pipelines

**Scenario:** Your Spark job processes 100GB daily and takes 2 hours. The data volume is doubling every 6 months. Your manager asks whether to buy a bigger server (vertical scaling) or add more servers (horizontal scaling). What do you recommend?

<details>
<summary>💡 Hint</summary>

Vertical scaling has hard limits (biggest EC2 instance). Horizontal scaling is elastic and can grow indefinitely. Spark is designed for horizontal scaling — adding nodes is straightforward. Vertical scaling is appropriate when the bottleneck is single-threaded (driver memory, metadata operations).

</details>

<details>
<summary>✅ Solution</summary>

**Recommendation: Horizontal Scaling for Spark**

Spark's architecture is inherently distributed — adding executor nodes scales linearly with data volume.

**Current state:**
- 100GB / 2 hours on 10 × r5.2xlarge nodes (8 vCPU, 64GB each)

**Projection:**
- 6 months: 200GB → 4 hours (or add 10 nodes → 2 hours)
- 12 months: 400GB → add more nodes

**When Vertical Scaling is Appropriate:**
- Spark driver OOM (single JVM, can't distribute) → bigger driver
- Pandas/single-node Python job → bigger machine
- PostgreSQL/OLTP databases → vertical (sharding is complex)

**Auto-scaling Configuration (AWS EMR):**

```python
emr_config = {
    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
    'Instances': {
        'InstanceFleets': [
            {
                'InstanceFleetType': 'MASTER',
                'TargetOnDemandCapacity': 1,
                'InstanceTypeConfigs': [{'InstanceType': 'r5.2xlarge'}]
            },
            {
                'InstanceFleetType': 'CORE',
                'TargetOnDemandCapacity': 4,
                'InstanceTypeConfigs': [{'InstanceType': 'r5.2xlarge'}]
            },
            {
                'InstanceFleetType': 'TASK',
                'TargetSpotCapacity': 10,  # Start with 10, auto-scale up
                'InstanceTypeConfigs': [
                    {'InstanceType': 'r5.2xlarge'},
                    {'InstanceType': 'r5.4xlarge'}
                ]
            }
        ]
    },
    'AutoScalingPolicy': {
        'Constraints': {'MinCapacity': 2, 'MaxCapacity': 50},
        'Rules': [{
            'Name': 'ScaleOutOnYARNMemory',
            'Action': {'SimpleScalingPolicyConfiguration': {
                'ScalingAdjustment': 5,
                'AdjustmentType': 'CHANGE_IN_CAPACITY'
            }},
            'Trigger': {'CloudWatchAlarmDefinition': {
                'MetricName': 'YARNMemoryAvailablePercentage',
                'ComparisonOperator': 'LESS_THAN',
                'Threshold': 15,
                'Period': 300
            }}
        }]
    }
}
```

**Result:** System scales automatically from 10 to 50 nodes as data grows, without code changes.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Scaling Kafka for High-Throughput Ingestion

**Scenario:** Your Kafka cluster ingests 500K events/second across 20 topics. A new use case requires adding another 1M events/second from IoT sensors. Diagnose current bottlenecks and scale the system.

<details>
<summary>💡 Hint</summary>

Kafka scaling involves: partition count (determines max consumer parallelism), broker count, replication factor, producer batch settings, and consumer group lag. Adding partitions to existing topics requires care (can't reduce partition count; affects keyed ordering).

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Diagnose Current Bottlenecks**

```bash
# Check per-broker throughput
kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check consumer group lag
kafka-consumer-groups.sh   --bootstrap-server kafka:9092   --describe   --group iot-consumer-group

# Check partition distribution
kafka-topics.sh   --bootstrap-server kafka:9092   --describe   --topic sensor-events
```

```python
# Python: monitor broker metrics via JMX/Prometheus
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Key Kafka metrics to watch:
metrics_to_monitor = {
    "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec": "< 80% of NIC capacity",
    "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions": "should be 0",
    "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce": "producer rate",
    "kafka.consumer:type=consumer-fetch-manager-metrics,attribute=records-lag-max": "< 10000",
}
```

**Step 2: Add Brokers**

```bash
# Current: 6 brokers handling 500K/s
# Target: 1.5M/s → add 12 more brokers (18 total)

# After adding brokers, rebalance partitions
kafka-reassign-partitions.sh   --bootstrap-server kafka:9092   --generate   --topics-to-move-json-file topics.json   --broker-list 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
```

**Step 3: Add Partitions for New IoT Topic**

```python
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

admin = KafkaAdminClient(bootstrap_servers='kafka:9092')

# New IoT topic: 180 partitions (1M events/s / ~5500 events/s per partition)
admin.create_topics([
    NewTopic(
        name='iot-sensor-events',
        num_partitions=180,
        replication_factor=3,
        topic_configs={
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'compression.type': 'lz4',  # Fast compression for IoT
            'min.insync.replicas': '2',
        }
    )
])

# Existing topics: increase partitions if needed
admin.create_partitions({
    'existing-topic': NewPartitions(total_count=120)
})
```

**Step 4: Tune Producer for High Throughput**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    # Batch settings for throughput
    batch_size=65536,              # 64KB batch (default 16KB)
    linger_ms=10,                  # Wait 10ms to fill batch
    compression_type='lz4',        # Fast compression
    buffer_memory=134217728,       # 128MB buffer
    max_block_ms=5000,
    # Reliability
    acks='1',                      # Leader ack only (not all replicas) for speed
    retries=3,
    # Serialization
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Multi-threaded producer: 10 threads × 100K events/s = 1M events/s
import concurrent.futures

def produce_batch(events):
    for event in events:
        producer.send('iot-sensor-events', value=event,
                      key=event['device_id'].encode())
    producer.flush()

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for batch in event_batches:
        executor.submit(produce_batch, batch)
```

**Step 5: Scale Consumers**

```python
# Consumer group with 180 consumers = 1 per partition (max parallelism)
# Deploy as Kubernetes deployment with HPA

# k8s deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iot-consumer
spec:
  replicas: 180  # Match partition count
  template:
    spec:
      containers:
      - name: consumer
        image: iot-processor:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: kafka:9092
        - name: KAFKA_TOPIC
          value: iot-sensor-events
        - name: KAFKA_GROUP_ID
          value: iot-consumer-group
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Petabyte-Scale Data Pipeline from Scratch

**Scenario:** You are founding data engineer at a fast-growing social media company. Current: 10M users, 100GB/day. Projected: 500M users, 5TB/day in 18 months. Design a data architecture that scales from day 1 to 5TB/day without a complete rewrite.

<details>
<summary>💡 Hint</summary>

Design for 10× scale from the start, but don't over-engineer day 1. Key decisions: managed services vs self-managed (managed scales faster), partitioning strategy (must work at petabyte scale), and avoiding architectural decisions that are hard to undo (tight coupling, wrong partition keys).

</details>

<details>
<summary>✅ Solution</summary>

**Guiding Principles:**
1. Use managed services (less operational burden as you scale)
2. Partition by time from day 1 (easiest dimension to query and prune)
3. Separate compute and storage (scale independently)
4. Keep the architecture simple at small scale — complexity grows with need

**Day 1 Architecture (10M users, 100GB/day):**

```
Mobile/Web Apps
      ↓
API Gateway → Kinesis Data Streams (5 shards)
      ↓
Lambda → S3 (Bronze: raw events, Parquet, date-partitioned)
      ↓
Glue ETL (serverless Spark) → S3 (Silver: cleaned)
      ↓
dbt on Snowflake → Gold tables
      ↓
Tableau / Looker
```

Cost at 100GB/day: ~$2K/month (mostly managed, no ops burden)

**18-Month Architecture (500M users, 5TB/day):**

The same logical architecture, just scaled:

```
Mobile/Web Apps (500M users)
      ↓
API Gateway → Kinesis Data Streams (250 shards) → Kafka (on MSK)
      ↓
Flink on EKS (streaming aggregations) → S3/Iceberg (Bronze)
      ↓
Spark on EMR (batch ETL) → S3/Iceberg (Silver)
      ↓
dbt on Snowflake → Snowflake Gold
      ↓
Tableau / Looker / internal dashboards
```

**Key Scaling Decisions Made at Day 1:**

**1. Partitioning Strategy:**
```python
# Day 1: partition by date (simple, works at any scale)
df.write     .partitionBy("event_date") \  # e.g. event_date=2024-01-15
    .format("parquet")     .save("s3://lake/bronze/events/")

# At 5TB/day: same strategy, just more data per partition
# Upgrade: add hour sub-partition when partition > 100GB
df.write     .partitionBy("event_date", "event_hour")     .format("iceberg") \  # Switched to Iceberg for ACID
    .save("s3://lake/bronze/events/")
```

**2. Event Schema with Envelope:**
```python
# Day 1: design envelope schema that survives schema evolution
event_schema = {
    "event_id": "uuid",          # globally unique
    "event_type": "string",      # page_view, click, purchase
    "user_id": "string",         # may be null for anonymous
    "session_id": "string",
    "timestamp": "long",         # epoch milliseconds — always use UTC
    "app_version": "string",
    "platform": "string",        # ios, android, web
    "payload": "map<string,string>"  # event-specific fields — flexible
}
# payload field lets you add new event properties without schema migration
```

**3. Migration Path — Kinesis → Kafka:**
```python
# When Kinesis shard limit hits (~200 shards, ~200K records/sec):
# Add Kafka MSK cluster, route new events there
# Keep Kinesis for old consumers during transition

ROUTING_CONFIG = {
    "new_users": "kafka",    # new traffic goes to Kafka
    "existing": "kinesis"    # existing consumers unchanged
}

# Dual-write during migration
def publish_event(event: dict):
    kinesis.put_record(...)   # existing consumers
    kafka_producer.send(...) # new consumers
    # Drop kinesis after all consumers migrated
```

**4. Cost Projection:**

| Scale | Monthly Cost | Key Change |
|-------|-------------|-----------|
| 100GB/day | $2K | Kinesis + Lambda + Glue |
| 500GB/day | $8K | Add EMR for heavy transforms |
| 2TB/day | $25K | Add Kafka MSK, Flink |
| 5TB/day | $80K | Scale-out Kafka, larger EMR fleet |

**5. Observability from Day 1:**

```python
# Instrument pipeline from start — metrics are cheap, missing them is expensive
from datadog import statsd

def process_events(events):
    statsd.gauge('pipeline.batch.size', len(events))
    start = time.time()

    result = transform(events)

    statsd.histogram('pipeline.processing.duration', time.time() - start)
    statsd.gauge('pipeline.output.rows', result.count())

    null_rate = result.filter("user_id IS NULL").count() / result.count()
    statsd.gauge('pipeline.null_user_rate', null_rate)
    if null_rate > 0.10:
        alert("High null user_id rate: pipeline quality issue")
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you know when to add more partitions to a Kafka topic?" — When consumer lag grows and you've maxed out consumer parallelism (consumers = partitions). Also when a single partition receives disproportionately high traffic (hot partition). Note: you can increase partitions but never decrease them.
> **Tip 2:** "What is the shuffle in Spark and why does it matter for scaling?" — A shuffle redistributes data across nodes (e.g., for joins, groupBy). It's expensive: serialization, network transfer, disk I/O. Minimizing shuffles (broadcast joins, pre-partitioning) is the primary Spark optimization at scale.
> **Tip 3:** "How do you handle hot partitions in a distributed system?" — Salting: add a random suffix to the partition key to spread load. For Kafka: use a custom partitioner that distributes hot keys. For Spark: use `repartition()` to redistribute skewed data before a join. For DynamoDB: add a shard prefix to the partition key.
