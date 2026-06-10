---
title: "Kinesis Advanced — Senior Deep Dive"
topic: real-time-streaming
subtopic: kinesis-advanced
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [kinesis, kcl, resharding, exactly-once, kda, flink, production, architecture]
---

# Kinesis Advanced — Senior Deep Dive

## KDS Internals and Ordering Guarantees

```
Shard storage model:
  Each shard = append-only log segment (similar to Kafka partition)
  Record = {PartitionKey, SequenceNumber, Data, ApproximateArrivalTimestamp}
  SequenceNumber: monotonically increasing per shard (not globally unique)
  
  Ordering guarantees:
    PER-SHARD: records with same partition key → same shard → ordered by arrival
    CROSS-SHARD: no ordering guarantee
  
  Producer retry and ordering:
    PutRecord: synchronous, one record at a time, SequenceNumberForOrdering param
      ensures new record appended after previous (prevents out-of-order on retry)
    PutRecords: batched, no ordering guarantee between records in batch
      (some records may succeed and others fail → reorder risk on retry)
    
    KPL: handles ordering via aggregation + per-shard sequencing

  Record aggregation (KPL):
    KPL packs multiple user records into one Kinesis record (up to 1 MB)
    KCL and Lambda automatically de-aggregate (transparent to application)
    Benefit: fewer PUT API calls (cost: billed per API call, not per record)
    
    WITHOUT aggregation: 10,000 small records = 10,000 PUT API calls
    WITH KPL aggregation: 10,000 records packed into ~100 Kinesis records = 100 calls
    Cost savings: ~99% reduction in PUT API call charges

Resharding deep dive:
  Split shard:
    Input: shard ID + starting hash key for the new shard
    Result: parent shard sealed, two child shards created
    Hash range: parent [0, 2^128) → child1 [0, midpoint), child2 [midpoint, 2^128)
    
  Merge shards:
    Input: two adjacent shards (hash ranges must be contiguous)
    Result: two parent shards sealed, one child shard created
    Restriction: can only merge adjacent shards (hash key ranges must be contiguous)
  
  Resharding algorithm for auto-scaling:
    Monitor: GetRecords.IteratorAgeMilliseconds (per shard CloudWatch metric)
    If lag > threshold AND shard count < limit: split hottest shard
    If all shards underloaded: merge coldest adjacent pair
    
  Partition key distribution after split:
    All records with hash(partitionKey) in [midpoint, 2^128) → new shard
    Records below midpoint: stay in original shard
    Application doesn't need to change — KDS handles routing
```

---

## Exactly-Once with Kinesis

```python
"""
Kinesis guarantees at-least-once delivery.
Achieving exactly-once requires application-level idempotency.

Two failure modes:
1. Producer retry: PUT fails → retry → duplicate record in KDS
2. Consumer crash: records processed but not checkpointed → re-read on restart

Handling producer duplicates (option A: sequence number dedup):
  Every Kinesis record has a unique SequenceNumber.
  Store last-seen SequenceNumber in DynamoDB per partition key.
  Skip if SequenceNumber already processed.
"""

import boto3
import hashlib

dynamodb = boto3.resource('dynamodb')
dedup_table = dynamodb.Table('kinesis-sequence-dedup')

def is_duplicate(partition_key: str, sequence_number: str) -> bool:
    """Check if this record has been processed before."""
    try:
        response = dedup_table.get_item(
            Key={'partition_key': partition_key}
        )
        item = response.get('Item')
        if not item:
            return False
        # Compare sequence numbers (lexicographically ordered within shard)
        return item['last_sequence'] >= sequence_number
    except Exception:
        return False  # fail open: process the record

def mark_processed(partition_key: str, sequence_number: str):
    """Record that this sequence number has been processed."""
    dedup_table.put_item(Item={
        'partition_key': partition_key,
        'last_sequence': sequence_number,
        'processed_at': datetime.utcnow().isoformat()
    })

def idempotent_process(record: dict):
    """Process record with exactly-once semantics."""
    pk  = record['kinesis']['partitionKey']
    seq = record['kinesis']['sequenceNumber']
    
    if is_duplicate(pk, seq):
        print(f"Skipping duplicate: {seq}")
        return
    
    # Process the record
    data = json.loads(base64.b64decode(record['kinesis']['data']))
    write_to_database(data)
    
    # Mark as processed (DynamoDB conditional write for atomicity)
    mark_processed(pk, seq)

"""
Option B: Business-key deduplication (simpler, more robust)
  Use a natural unique ID from your data (order_id, event_id, etc.)
  Store in DynamoDB or use database UPSERT:
  
  INSERT INTO orders (order_id, amount, ...) 
  VALUES (%s, %s, ...)
  ON CONFLICT (order_id) DO NOTHING;  -- PostgreSQL
  
  This handles both: Kinesis duplicate delivery AND producer retries
  Much simpler than sequence number tracking
"""
```

---

## Kinesis Data Analytics (Managed Flink)

```python
"""
KDA runs Apache Flink on AWS-managed infrastructure.
No cluster management: AWS handles scaling, patching, checkpointing.

KPU (Kinesis Processing Unit):
  1 KPU = 1 vCPU + 4 GB memory
  Parallelism: 1 KPU per parallel task (auto or manual)
  Scaling: based on CPU utilization; can set min/max KPUs

KDA application:
  Source: Kinesis Data Streams or MSK (Kafka)
  Processing: any Flink code (DataStream API, Flink SQL, Table API)
  Sink: KDS, Firehose, S3, RDS, DynamoDB

Deployment via CDK:
"""

import aws_cdk as cdk
from aws_cdk import aws_kinesisanalytics_flink as kda

class FlinkKDAStack(cdk.Stack):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        app = kda.Application(
            self, "FraudDetectionApp",
            code=kda.ApplicationCode.from_asset("target/fraud-detection-1.0.jar"),
            runtime=kda.Runtime.FLINK_1_15,
            
            # Auto-scaling
            property_groups={
                "FlinkApplicationProperties": {
                    "kafka.bootstrap.servers": "kafka:9092",
                    "kinesis.stream.name": "transactions",
                    "output.stream.name": "fraud-alerts"
                }
            },
            
            # Monitoring
            log=kda.LogLevel.INFO,
            
            # Snapshots (KDA term for Flink savepoints)
            snapshots_enabled=True,
        )

"""
KDA Flink job (Java/Scala): same code as self-managed Flink
KDA reads Kinesis via FlinkKinesisConsumer:
"""

# FlinkKinesisConsumer configuration:
Properties props = new Properties();
props.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
props.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
props.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "10000");

DataStream<String> stream = env.addSource(
    new FlinkKinesisConsumer<>(
        "transactions",
        new SimpleStringSchema(),
        props
    )
);
```

---

## Production Architecture: Kinesis Multi-Consumer

```
Multi-consumer Kinesis architecture (fan-out pattern):

  IoT Devices → KDS (transactions, 32 shards)
                    │
          ┌─────────┼──────────┬──────────┐
          │         │          │          │
    Lambda(EFO)  KDA-Flink  KCL App    Firehose
    (real-time   (analytics  (custom     (to S3)
     alerts)     aggregation) enrichment)

Consumer isolation:
  Lambda (EFO):    2 MB/sec/shard dedicated
  KDA Flink (EFO): 2 MB/sec/shard dedicated
  KCL App (EFO):   2 MB/sec/shard dedicated
  Firehose (EFO):  2 MB/sec/shard dedicated
  Total cost: 4× EFO charges (but each consumer gets full bandwidth)

Monitoring:
  Producer:     PutRecord.Success, WriteProvisionedThroughputExceeded → alert if > 0
  Consumer:     GetRecords.IteratorAgeMilliseconds → alert if > 60 seconds
  KDA:          numberOfLateRecordsDropped → watermark tuning needed
  Firehose:     DeliveryToS3.DataFreshness → alert if > 5 minutes

Cost optimization:
  Shards: right-size to actual throughput (not over-provisioned)
  EFO: only for consumers that need full bandwidth (use standard for low-volume consumers)
  Retention: 24 hours sufficient for most use cases (extended retention costs more)
  KPL aggregation: always use for high-volume producers (reduces PUT API charges ~90%)

Kinesis vs MSK for this architecture:
  MSK (Kafka):   unlimited retention, more consumers, better ecosystem
  KDS:           fully managed (no broker sizing), simpler auto-scaling
  Decision: KDS for AWS-native pipelines with < 10 consumers; MSK for complex topologies
```

---

## Interview Tips

> **Tip 1:** "How does Kinesis Data Analytics (managed Flink) compare to self-managed Flink on EMR or EKS?" — KDA: fully managed (no cluster ops), automatic checkpointing to S3, CloudWatch integration, scales by adding KPUs. Best for teams that want Flink without infrastructure management. Limitations: only Kinesis/MSK sources (no custom connectors), cannot use latest Flink version immediately (lags ~1 version behind), KPU pricing (~$0.11/KPU-hour) vs EMR/EKS where you pay for EC2 directly. Self-managed Flink on EKS: full flexibility (any source/sink, latest Flink), lower cost at scale, but requires operational expertise (Flink Operator, monitoring, upgrades).

> **Tip 2:** "What happens to Kinesis records if a consumer Lambda function is throttled?" — Lambda function concurrency is limited (account limit: 1000 concurrent Lambdas by default). For Kinesis event source mapping: each shard maps to 1 concurrent Lambda (or up to 10 with parallelization factor). If Lambda is throttled: the Kinesis iterator blocks — records accumulate in the shard (up to retention period). Lambda retries with exponential backoff. Iterator age grows → CloudWatch alarm fires. Fix: request Lambda concurrency limit increase, or use parallelization factor to process multiple batches per shard concurrently. Long-term: if throughput exceeds Lambda limits, migrate consumer to KCL on EC2/ECS or KDA.

> **Tip 3:** "What is KPL aggregation and when does it cause problems?" — KPL packs multiple small user records into one Kinesis record (up to 1 MB) to reduce API calls. The aggregated record is Base64+Protobuf encoded with a magic number prefix. KCL and Lambda automatically detect the magic number and de-aggregate transparently. Problems: (a) consumers that DON'T use KCL/Lambda (custom GetRecords consumers) receive opaque blob and must implement de-aggregation manually (use Kinesis Aggregation/Deaggregation library); (b) low-latency requirement: KPL may buffer records up to 100ms waiting to fill the 1 MB batch — set `RecordMaxBufferedTime=100` lower for latency-sensitive use cases.

## ⚡ Cheat Sheet

**Streaming fundamentals**
```
Event time:    when the event actually occurred (on the device)
Processing time: when the system processes it (can be much later)
Ingestion time: when it arrives at the message broker
Watermark:     max expected event time lag — defines when a window closes
Late data:     events arriving after the watermark → handled by allowedLateness or drop
```

**Apache Flink key concepts**
```java
// Keyed stream + window + aggregate
stream.keyBy(event -> event.userId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(new RevenueAggregator());

// Watermark strategy
WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.eventTimeMs);
```

**Spark Structured Streaming**
```python
# Read from Kafka
stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "orders") \
    .load()

# Window aggregation
from pyspark.sql.functions import window, col
agg = stream \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "5 minutes"), "region") \
    .sum("amount")

# Write to Delta (trigger: every 1 min or micro-batch)
agg.writeStream.format("delta").trigger(processingTime="1 minute") \
    .outputMode("append").option("checkpointLocation", "/chk/orders").start()
```

**Window types**
| Window | Description | Use case |
|---|---|---|
| Tumbling | Fixed non-overlapping | Hourly totals |
| Sliding | Fixed size, moves by slide interval | 5-min avg, every 1 min |
| Session | Gap-based (closes after inactivity) | User sessions |
| Global | Accumulates all events | Running total |

**Exactly-once semantics**
```
Source: idempotent read (Kafka offset tracking)
Processing: checkpointing (Flink) or write-ahead log (Spark)
Sink: idempotent write (Delta MERGE, upsert) or transactional sink
Kafka → Flink/Spark → Delta = exactly-once end-to-end (with checkpointing)
```

**CDC streaming (Debezium → Kafka → Lakehouse)**
```
1. Debezium captures MySQL/Postgres binlog → Kafka topic (op: c/u/d/r)
2. Flink/Spark reads Kafka topic
3. MERGE INTO Delta/Iceberg table:
   INSERT on c, UPDATE on u, DELETE on d
4. Result: real-time replicated lakehouse table
```

**Kinesis key operations**
```python
import boto3
kinesis = boto3.client('kinesis', region_name='us-east-1')
# Put record
kinesis.put_record(StreamName='orders', Data=json.dumps(event).encode(), PartitionKey=order_id)
# Get shard iterator
it = kinesis.get_shard_iterator(StreamName='orders', ShardId='shardId-000000000000',
                                 ShardIteratorType='LATEST')['ShardIterator']
# Read records
records = kinesis.get_records(ShardIterator=it, Limit=100)['Records']
```

**Stateful processing patterns**
```
Running total:    keyed state (ValueState[Double])
Sessionization:   keyed + timer-based (clear state after N seconds inactivity)
Pattern detection: CEP (Flink Complex Event Processing) — detect A then B within 5 min
Deduplication:    keyed state stores seen event IDs (with TTL for cleanup)
```

**Key interview points**
- Checkpointing: Flink snapshots operator state to S3/HDFS for fault tolerance
- Backpressure: slow downstream = upstream stops reading Kafka = natural flow control
- Parallelism = Kafka partitions: each Flink/Spark task reads one partition
- Streaming vs micro-batch: Flink = true streaming (event-by-event); Spark = micro-batch (more latency, simpler)
