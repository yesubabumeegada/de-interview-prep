---
title: "AWS Kinesis - Intermediate"
topic: aws-services
subtopic: kinesis
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, kinesis, scaling, enhanced-fan-out, error-handling, kpl]
---

# AWS Kinesis — Intermediate Concepts

## Enhanced Fan-Out (Dedicated Throughput per Consumer)

Standard consumers share 2 MB/s read per shard. Enhanced Fan-Out gives each consumer its own dedicated 2 MB/s:

```python
# Standard (shared): 3 consumers share 2 MB/s per shard
# = 0.67 MB/s per consumer per shard (they compete)

# Enhanced Fan-Out: each consumer gets dedicated 2 MB/s per shard
# = 2 MB/s per consumer per shard (no competition)

# Register a consumer for Enhanced Fan-Out
kinesis.register_stream_consumer(
    StreamARN='arn:aws:kinesis:...:stream/my-stream',
    ConsumerName='analytics-consumer'
)

# Subscribe to shard with dedicated throughput (push-based, not polling)
# Records are pushed to consumer via HTTP/2 (lower latency than polling)
```

**When to use Enhanced Fan-Out:**
- Multiple consumers reading the same stream (3+)
- Low-latency requirements (<200ms end-to-end)
- Consumers falling behind due to shared throughput

**Cost:** $0.015/consumer/shard-hour + $0.013/GB data retrieved

---

## Kinesis Producer Library (KPL)

The KPL provides higher throughput than the basic `put_record` API by batching and aggregating:

```python
# Basic API: one PUT call per record (limited by API rate)
# KPL: aggregates many small records into one API call

# KPL features:
# 1. Record aggregation: packs multiple user records into one Kinesis record
# 2. Collection: batches multiple Kinesis records into one PutRecords API call
# 3. Automatic retries with backoff
# 4. CloudWatch metrics integration
# 5. Async non-blocking puts

# Result: 100x higher throughput than basic put_record()
# Basic API: ~1000 records/sec per shard
# KPL: up to 100,000 small records/sec per shard (via aggregation)
```

> **Trade-off:** KPL adds latency (buffers records for up to `RecordMaxBufferedTime`). If you need per-record low latency, use the basic API. If you need throughput, use KPL.

---

## Resharding (Scaling Shards)

```python
# Split a hot shard into two (scale up)
kinesis.split_shard(
    StreamName='my-stream',
    ShardToSplit='shardId-000000000001',
    NewStartingHashKey='170141183460469231731687303715884105728'
    # This hash key splits the shard's key range in half
)

# Merge two adjacent cold shards into one (scale down)
kinesis.merge_shards(
    StreamName='my-stream',
    ShardToMerge='shardId-000000000002',
    AdjacentShardToMerge='shardId-000000000003'
)

# Or use On-Demand mode (automatic scaling!)
kinesis.update_stream_mode(
    StreamARN='...',
    StreamModeDetails={'StreamMode': 'ON_DEMAND'}
)
# On-Demand: auto-scales shards based on throughput (no manual split/merge)
# Handles up to 200 MB/s write and 400 MB/s read automatically
```

---

## Error Handling with Lambda Consumer

```python
# Lambda event source mapping with error handling
lambda_client.create_event_source_mapping(
    EventSourceArn='arn:aws:kinesis:...:stream/orders',
    FunctionName='process-orders',
    StartingPosition='TRIM_HORIZON',
    BatchSize=100,
    MaximumBatchingWindowInSeconds=5,
    ParallelizationFactor=10,           # Process 10 batches per shard concurrently
    MaximumRetryAttempts=3,             # Retry failed batch 3 times
    BisectBatchOnFunctionError=True,    # Split failed batch in half to isolate poison record
    DestinationConfig={
        'OnFailure': {
            'Destination': 'arn:aws:sqs:...:kinesis-dlq'  # Failed records go to DLQ
        }
    },
    MaximumRecordAgeInSeconds=3600,     # Skip records older than 1 hour
)
```

**Error handling flow:**
1. Lambda processes batch of 100 records
2. If Lambda fails: retry up to 3 times
3. If still failing: bisect batch into two halves, retry each half (isolate poison record)
4. If a half still fails after retries: send the failed records to SQS DLQ
5. Continue processing (don't block the entire stream)

---

## Kinesis Data Analytics (Apache Flink)

Run SQL or Flink applications on streaming data:

```sql
-- Real-time anomaly detection with tumbling windows
CREATE TABLE order_stream (
    order_id VARCHAR,
    customer_id VARCHAR,
    amount DOUBLE,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'orders',
    'aws.region' = 'us-east-1',
    'format' = 'json'
);

-- Detect customers spending >$10K in a 5-minute window
SELECT 
    customer_id,
    TUMBLE_START(order_time, INTERVAL '5' MINUTE) AS window_start,
    SUM(amount) AS window_total,
    COUNT(*) AS order_count
FROM order_stream
GROUP BY customer_id, TUMBLE(order_time, INTERVAL '5' MINUTE)
HAVING SUM(amount) > 10000;
-- Results streamed to output Kinesis stream or S3 in real-time
```

---

## Kinesis vs MSK (Kafka) Decision Framework

| Factor | Choose Kinesis When | Choose MSK When |
|--------|-------------------|-----------------|
| Throughput | < 50 MB/s | > 50 MB/s (Kafka is cheaper at scale) |
| Operations | Want zero management | OK with some broker management |
| AWS integration | Need native Lambda/Firehose/Analytics | Need Kafka Connect/Streams ecosystem |
| Consumer model | Lambda or KCL (simple) | Consumer groups (mature, flexible) |
| Cost structure | Predictable (per shard/hour) | Variable (per broker instance) |
| Retention | Up to 365 days | Unlimited (configurable) |
| Ecosystem | AWS-only | Multi-cloud, open source |
| Team expertise | AWS-native team | Kafka-experienced team |

---

## Interview Tips

> **Tip 1:** "How do you handle a poison message in Kinesis?" — "Lambda event source mapping with `BisectBatchOnFunctionError=True` and `MaximumRetryAttempts=3`. After retries, the batch is bisected (split in half) to isolate the bad record. If it still fails, it goes to an SQS DLQ. The stream continues processing other records (not blocked by one bad message)."

> **Tip 2:** "How do you scale Kinesis?" — "Two options: (1) Provisioned mode: manually split/merge shards based on throughput needs. (2) On-Demand mode: automatic scaling up to 200 MB/s write without any shard management. On-Demand costs ~15% more per GB but eliminates capacity planning entirely."

> **Tip 3:** "Kinesis vs Kafka in your experience?" — "I choose Kinesis for: Lambda-based processing, Firehose auto-delivery to S3, and when I want zero infrastructure management. I choose Kafka (MSK) for: high-throughput workloads (>50 MB/s where Kafka is cheaper), when I need Kafka Connect or Kafka Streams, and when the team already has Kafka expertise."
