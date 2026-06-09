---
title: "Kinesis Advanced — Fundamentals"
topic: real-time-streaming
subtopic: kinesis-advanced
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [kinesis, aws, streaming, kinesis-data-streams, kinesis-firehose, shards]
---

# Kinesis Advanced — Fundamentals

## Kinesis Ecosystem Overview

AWS Kinesis is a family of managed streaming services for real-time data ingestion and processing.

```
Kinesis family:

1. Kinesis Data Streams (KDS)
   - Raw streaming: producers put records, consumers read
   - You manage: shard count, consumer scaling, offset (sequence number)
   - Retention: 24 hours default, up to 365 days (extended retention)
   - Use: real-time analytics, custom consumers, Flink/Spark source

2. Kinesis Data Firehose (KDF)
   - Managed delivery: auto-batches and delivers to S3, Redshift, Elasticsearch, Splunk
   - No consumer code needed — fully managed ETL
   - Buffer: size (1-128 MB) or time (60-900 seconds), whichever first
   - Transformation: Lambda function for inline processing
   - Use: log delivery, IoT to S3, event archiving

3. Kinesis Data Analytics (KDA)
   - Managed Flink: run Apache Flink on fully managed infrastructure
   - Also supports SQL (legacy, deprecated)
   - Scales automatically based on KPU (Kinesis Processing Unit = 1 vCPU + 4 GB)
   - Use: streaming analytics without managing Flink cluster

4. Kinesis Video Streams
   - Video/audio streaming (specialized — not typically in DE interviews)

Comparison: Kinesis vs Kafka:
                 Kinesis Data Streams    Apache Kafka / MSK
  Managed        Fully managed           MSK managed; self-hosted = ops burden
  Retention      1-365 days              Unlimited (disk-bound)
  Throughput     1 MB/s or 1000 rec/s    Very high (limited by broker disk)
  Partitions     Shards (fixed, manual)  Partitions (elastic, configurable)
  Ordering       Per-shard              Per-partition
  Consumer        Kinesis SDK / KCL      Consumer group offset management
  Cost           Per shard/hour + PUT    Per broker/hour + storage
  Ecosystem       AWS-native             Broad (Confluent, Schema Registry, etc.)
```

---

## Kinesis Data Streams: Core Concepts

```
Shard: unit of capacity in KDS
  Write: 1 MB/sec OR 1,000 records/sec (whichever hit first → ThrottlingException)
  Read:  2 MB/sec per shard (shared across all consumers)
         OR 2 MB/sec per consumer per shard with Enhanced Fan-Out (EFO)

Partition key → shard routing:
  MD5 hash of partition key → maps to shard
  Good partition key: high cardinality → even distribution
  Bad partition key: user_type ("free"/"paid") → hot shard
  
Record structure:
  PartitionKey:     string (used for shard routing)
  Data:             blob up to 1 MB
  SequenceNumber:   assigned by KDS (ordered within shard)
  ApproximateArrivalTimestamp: when KDS received the record

Resharding:
  Split shard:    one shard → two shards (increase capacity)
  Merge shards:   two shards → one shard (reduce capacity, cost)
  Limit: 500 splits+merges per 24 hours per stream
  Cold start: resharding completes in ~30 seconds
  
  After resharding:
    Parent shard: read until exhausted (all existing records)
    Child shards: begin receiving new records
    Consumers must handle parent → child transition
  
  Auto-scaling: use Lambda + CloudWatch alarm on GetRecords.IteratorAgeMilliseconds
  (iterator age = how far behind consumers are — alarm at > 60 seconds → split shards)
```

---

## Producing to Kinesis

```python
import boto3
import json
import time

kinesis = boto3.client('kinesis', region_name='us-east-1')

# Single record put
def put_record(stream_name: str, data: dict, partition_key: str) -> dict:
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data).encode('utf-8'),
        PartitionKey=partition_key    # determines which shard
    )
    return response  # contains ShardId, SequenceNumber

# Batch put (up to 500 records, 5 MB total)
def put_records_batch(stream_name: str, records: list) -> None:
    """Batch producer: more efficient (fewer API calls, lower cost)."""
    
    kinesis_records = [
        {
            'Data': json.dumps(record).encode('utf-8'),
            'PartitionKey': record['device_id']   # partition by device
        }
        for record in records
    ]
    
    # PutRecords: up to 500 records per call
    response = kinesis.put_records(
        StreamName=stream_name,
        Records=kinesis_records
    )
    
    # Handle partial failures (some records may fail)
    failed = response.get('FailedRecordCount', 0)
    if failed > 0:
        print(f"WARNING: {failed} records failed — retry logic needed")
        failed_records = [
            kinesis_records[i]
            for i, r in enumerate(response['Records'])
            if 'ErrorCode' in r
        ]
        # Retry failed records with exponential backoff
        retry_with_backoff(stream_name, failed_records)

def retry_with_backoff(stream_name, records, max_retries=3):
    """Retry failed records with exponential backoff."""
    for attempt in range(max_retries):
        if not records:
            break
        time.sleep(2 ** attempt)  # 1s, 2s, 4s
        response = kinesis.put_records(StreamName=stream_name, Records=records)
        # filter out still-failed records for next retry
        records = [records[i] for i, r in enumerate(response['Records'])
                   if 'ErrorCode' in r]
    if records:
        print(f"FATAL: {len(records)} records failed after {max_retries} retries")

# Kinesis Producer Library (KPL) — Java, handles batching + retries + aggregation:
# KPL aggregation: pack multiple small records into one KDS record (up to 1 MB)
# Reduces PUT API calls and cost (billed per PUT API call + per shard)
```

---

## Consuming from Kinesis

```python
# Two consumer APIs:

# 1. GetRecords (polling) — shared throughput (2 MB/sec PER SHARD, all consumers share)
#    Max 5 GetRecords calls/sec per shard (limited by API rate)
#    Good for: 1-2 consumers per shard, cost-sensitive

# 2. Enhanced Fan-Out (SubscribeToShard) — dedicated throughput (2 MB/sec PER CONSUMER PER SHARD)
#    Push-based (HTTP/2 streaming — no polling)
#    Cost: $0.015/shard-hour + $0.013/GB data retrieved (additional on top of base)
#    Good for: multiple consumers, low-latency, real-time

# GetRecords (basic consumer):
def consume_shard(stream_name: str, shard_id: str):
    """Basic GetRecords consumer (polling)."""
    
    # Get shard iterator (starting position)
    response = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'   # from oldest available record
        # Options: TRIM_HORIZON (oldest), LATEST, AT_TIMESTAMP, AT_SEQUENCE_NUMBER
    )
    shard_iterator = response['ShardIterator']
    
    while True:
        records_response = kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=10000  # max records per call
        )
        
        records = records_response['Records']
        for record in records:
            data = json.loads(record['Data'])
            seq  = record['SequenceNumber']
            ts   = record['ApproximateArrivalTimestamp']
            print(f"seq={seq}: {data}")
        
        # Update iterator for next poll
        shard_iterator = records_response.get('NextShardIterator')
        if not shard_iterator:
            print("Shard closed (resharding occurred)")
            break
        
        # Rate limit: 5 calls/sec per shard (don't exceed)
        if not records:
            time.sleep(1.0)  # back off when no data
```

---

## Interview Tips

> **Tip 1:** "When would you use Kinesis Data Firehose instead of Kinesis Data Streams?" — Use Firehose when you want managed delivery to a specific destination (S3, Redshift, Elasticsearch, Splunk) without writing consumer code. Firehose handles buffering, batching, retries, and format conversion automatically. Use KDS when you need: multiple independent consumers of the same stream, custom processing logic (Flink, Lambda, KCL), sub-second latency (Firehose buffers for at least 60 seconds), or long retention (Firehose doesn't retain — it delivers and discards).

> **Tip 2:** "What is a hot shard and how do you fix it?" — A hot shard occurs when a partition key has very high cardinality imbalance — one key generates far more traffic than others (e.g., a popular user_id, or a low-cardinality key like "event_type" with 90% of events being "click"). Symptoms: ThrottlingException for that shard, while other shards are underutilized. Fix: (a) Use high-cardinality partition key (device_id, user_id, UUID); (b) Add a random suffix to partition key: `f"{user_id}-{random.randint(0,9)}"` (spreads load, breaks per-key ordering); (c) Use Kinesis Producer Library which handles partition key spreading automatically.

> **Tip 3:** "What is iterator age and why should you monitor it?" — Iterator age (GetRecords.IteratorAgeMilliseconds) is the gap between the timestamp of the last record returned and now — i.e., how far behind the consumer is from real-time. Age = 0: consumer is caught up. Age > 60 seconds: consumer is falling behind (processing slower than ingest). Alert threshold: age > 5 minutes → add shards or scale consumers. Age approaching retention period → data will be lost if consumer doesn't catch up before records expire. This is your primary SLA metric for streaming freshness.
