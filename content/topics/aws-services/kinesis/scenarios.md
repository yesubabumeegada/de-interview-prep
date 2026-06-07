---
title: "AWS Kinesis - Scenario Questions"
topic: aws-services
subtopic: kinesis
content_type: scenario_question
tags: [aws, kinesis, interview, scenarios, streaming]
---

# Scenario Questions — AWS Kinesis

<article data-difficulty="junior">

## 🟢 Junior: Choose Firehose vs Data Streams

**Scenario:** Your application generates 10,000 events/second (average 500 bytes each). You need to: (1) Store all events in S3 as Parquet for analytics, (2) No custom processing needed — just reliable delivery. Which Kinesis service should you use and why?

<details>
<summary>✅ Solution</summary>

**Answer: Kinesis Data Firehose**

**Why Firehose:**
- Automatic delivery to S3 (no custom consumer code needed)
- Built-in JSON → Parquet format conversion
- Automatic buffering (creates optimally-sized files, not tiny ones)
- Handles retries and error routing automatically
- Zero infrastructure management

**Why NOT Data Streams:**
- You don't need custom processing logic
- No multiple consumers needed (just S3 delivery)
- Data Streams would require writing and managing a consumer application

**Configuration:**
```python
# Throughput: 10K events × 500 bytes = 5 MB/sec
# Firehose handles this automatically (auto-scales)

# Buffer: 128 MB or 300 seconds (whichever comes first)
# At 5 MB/sec: buffer fills 128 MB in ~25 seconds → writes every 25 seconds
# Result: optimally-sized Parquet files on S3 with Hive partitioning

# Estimated cost:
# 5 MB/sec × 86400 sec/day = 432 GB/day ingested
# Firehose: $0.029/GB = $12.50/day = ~$375/month
# Plus format conversion: $0.018/GB = ~$233/month
# Total: ~$608/month for fully managed 5 MB/s → S3 Parquet pipeline
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Shard Capacity Planning

**Scenario:** Your e-commerce platform needs to ingest clickstream data with these requirements:
- Peak: 50,000 events/second
- Average event size: 1 KB
- 3 Lambda consumers need to read simultaneously
- Events must be ordered per user

How many shards do you need? What's the partition key strategy?

<details>
<summary>✅ Solution</summary>

**Shard calculation:**

```
Write requirements:
- 50,000 events/sec × 1 KB = 50 MB/sec write throughput
- Each shard: 1 MB/sec write OR 1,000 records/sec
- By throughput: 50 MB / 1 MB = 50 shards needed
- By record count: 50,000 / 1,000 = 50 shards needed
→ Need: 50 shards (both limits align)

Read requirements:
- Each shard: 2 MB/sec read (shared across all consumers)
- 3 consumers × 50 MB/sec each = would need 75 shards if shared mode
- With Enhanced Fan-Out: each consumer gets dedicated 2 MB/sec per shard
- 50 shards × 2 MB/sec = 100 MB/sec per consumer → sufficient for 3 consumers
→ Use Enhanced Fan-Out for 3 dedicated consumers
```

**Partition key strategy:**
```python
# Use user_id as partition key → per-user ordering guaranteed
kinesis.put_record(
    StreamName='clickstream',
    Data=event_json,
    PartitionKey=event['user_id']  # Same user always goes to same shard
)
# This guarantees: events for user-123 arrive in order to the consumer
# Different users may be on different shards (processed in parallel)
```

**Architecture:**
```
50 shards, Enhanced Fan-Out
├── Consumer 1: Lambda (real-time personalization)
├── Consumer 2: Lambda (anomaly detection)
└── Consumer 3: Firehose (S3 archive)
Each gets dedicated 2 MB/sec per shard (no contention)
```

**Cost estimate:**
- 50 shards × $0.015/hour = $0.75/hour = $540/month (shard hours)
- Enhanced Fan-Out: 3 consumers × 50 shards × $0.015/shard-hour = $2.25/hour = $1,620/month (plus $0.013/GB retrieved)
- Data ingestion: 50 MB/s × 86400 = 4.3 TB/day × $0.014/GB = $61/day = $1,830/month
- **Total: ~$4,000/month** for 50K events/sec fully managed streaming

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Handle Hot Shard Problem

**Scenario:** Your Kinesis stream has 20 shards. One shard consistently receives 80% of the traffic because most events have partition key "homepage" (80% of traffic is homepage clicks). This causes `ProvisionedThroughputExceededException` on that shard while others are nearly empty. Design a fix without increasing shard count.

<details>
<summary>✅ Solution</summary>

**Root cause:** Using `page_url` as partition key. Since "homepage" is 80% of traffic, one shard gets overwhelmed.

**Fix 1: Change partition key to distribute evenly**

```python
# BAD: page_url as key → homepage shard is hot
kinesis.put_record(PartitionKey=event['page_url'], ...)

# GOOD: Use user_id (much more evenly distributed)
kinesis.put_record(PartitionKey=event['user_id'], ...)
# 1M unique users spread across 20 shards = ~50K users per shard (balanced)

# If user_id not available: use random key for even distribution
import uuid
kinesis.put_record(PartitionKey=str(uuid.uuid4()), ...)
# Perfectly even distribution, but loses ordering guarantee
```

**Fix 2: Add random suffix to hot keys (salting)**

```python
import random

def get_partition_key(event):
    """Distribute hot keys across multiple shards."""
    base_key = event['page_url']
    
    # Hot keys get salted (spread across shards)
    if base_key in ['/', '/home', '/search']:
        salt = random.randint(0, 19)  # 20 possible suffixes
        return f"{base_key}_{salt}"
    
    # Cold keys use the base key (preserves ordering per page)
    return base_key

kinesis.put_record(PartitionKey=get_partition_key(event), ...)
# "/" traffic split across 20 sub-keys → 20 different shards
```

**Fix 3: Use two streams (hot/cold separation)**

```python
# Stream 1: High-volume generic events (homepage, search)
# 15 shards, random partition key (no ordering needed)

# Stream 2: User-specific events (checkout, purchase)  
# 5 shards, user_id partition key (ordering preserved)

def route_event(event):
    if event['page_url'] in ['/', '/home', '/search']:
        kinesis.put_record(StreamName='high-volume-stream', 
                          PartitionKey=str(uuid.uuid4()), Data=...)
    else:
        kinesis.put_record(StreamName='user-events-stream',
                          PartitionKey=event['user_id'], Data=...)
```

**Fix 4: Kinesis On-Demand mode (automatic scaling)**

```python
# Switch from Provisioned to On-Demand mode
kinesis.update_stream_mode(
    StreamARN='arn:aws:kinesis:...:stream/clickstream',
    StreamModeDetails={'StreamMode': 'ON_DEMAND'}
)
# Kinesis automatically scales shards to handle traffic
# No more manual shard management
# Cost: per-GB pricing instead of per-shard (more expensive per GB, but no over-provisioning)
```

**Recommendation:** Fix 1 (change to user_id key) is simplest and best for most clickstream use cases. You get per-user ordering AND even distribution.

</details>

</article>
