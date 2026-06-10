---
title: "AWS DynamoDB - Scenario Questions"
topic: aws-services
subtopic: dynamodb
content_type: scenario_question
tags: [aws, dynamodb, interview, scenarios, nosql]
---

# Scenario Questions — AWS DynamoDB

<article data-difficulty="junior">

## 🟢 Junior: Design a Pipeline State Table

**Scenario:** You need to track which dates have been processed by your ETL pipeline. Design a DynamoDB table for this: what's the partition key, what attributes do you store, and how do you query it?

<details>
<summary>✅ Solution</summary>

```python
# Table design
# Partition Key: pipeline_name (String)
# Sort Key: process_date (String, format: YYYY-MM-DD)
# Attributes: status, row_count, started_at, completed_at, error_message

import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('pipeline_state')

# Mark a date as processing
table.put_item(Item={
    'pipeline_name': 'daily-orders-etl',
    'process_date': '2024-01-15',
    'status': 'RUNNING',
    'started_at': datetime.now().isoformat(),
    'row_count': 0
})

# Mark as completed
table.update_item(
    Key={'pipeline_name': 'daily-orders-etl', 'process_date': '2024-01-15'},
    UpdateExpression='SET #s = :status, row_count = :rows, completed_at = :ts',
    ExpressionAttributeNames={'#s': 'status'},
    ExpressionAttributeValues={
        ':status': 'COMPLETED',
        ':rows': 150000,
        ':ts': datetime.now().isoformat()
    }
)

# Check if a date was already processed (for idempotency)
response = table.get_item(Key={'pipeline_name': 'daily-orders-etl', 'process_date': '2024-01-15'})
if response.get('Item', {}).get('status') == 'COMPLETED':
    print("Already processed — skipping")

# Query all dates for a pipeline (sort key range)
response = table.query(
    KeyConditionExpression='pipeline_name = :pn AND process_date BETWEEN :start AND :end',
    ExpressionAttributeValues={':pn': 'daily-orders-etl', ':start': '2024-01-01', ':end': '2024-01-31'}
)
```

**Why DynamoDB for this:** Low latency lookups, automatic scaling, serverless, and the partition key + sort key design naturally supports "find all dates for a pipeline" queries.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Idempotent Processing with DynamoDB

**Scenario:** Your Lambda processes Kinesis records but sometimes receives duplicates (at-least-once delivery). Use DynamoDB to ensure each event is processed exactly once.

<details>
<summary>✅ Solution</summary>

```python
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
dedup_table = dynamodb.Table('processed_events')

def handler(event, context):
    for record in event['Records']:
        event_id = record['kinesis']['sequenceNumber']
        
        # Conditional put: only succeeds if event_id doesn't exist
        try:
            dedup_table.put_item(
                Item={
                    'event_id': event_id,
                    'processed_at': datetime.now().isoformat(),
                    'ttl': int(time.time()) + 86400 * 7  # Auto-delete after 7 days
                },
                ConditionExpression='attribute_not_exists(event_id)'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"Duplicate event {event_id} — skipping")
                continue  # Already processed
            raise
        
        # First time seeing this event — process it
        process_event(record)
```

**Key design:**
- `ConditionExpression='attribute_not_exists(event_id)'` — atomic check-and-write
- TTL attribute auto-deletes old entries (keeps table small)
- Thread-safe: DynamoDB conditional writes are atomic (no race conditions)
- Cost: tiny (~$0.25/month for 1M dedup checks/day)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Amazon DynamoDB and when is it the right choice for data engineers?**
A: DynamoDB is a fully managed, serverless NoSQL key-value and document database that delivers single-digit millisecond performance at any scale. It's the right choice for high-throughput operational data stores — pipeline state tracking, deduplication tables, configuration stores, and real-time lookup tables that don't require complex joins.

**Q: What is the difference between partition key and sort key in DynamoDB?**
A: The partition key determines which physical partition stores the item (it must be unique for simple primary keys). A composite primary key combines a partition key with a sort key, allowing multiple items per partition key — enabling range queries and sorted retrieval within a partition.

**Q: What are Global Secondary Indexes (GSIs) and Local Secondary Indexes (LSIs)?**
A: GSIs create an alternate partition key + optional sort key index on any attributes, enabling queries on non-primary-key attributes; they have their own read/write capacity. LSIs share the same partition key as the base table but have a different sort key; they must be defined at table creation and share the table's capacity.

**Q: What is DynamoDB Streams and how is it used in data pipelines?**
A: DynamoDB Streams captures a time-ordered sequence of item-level changes (INSERT, MODIFY, REMOVE) and retains them for 24 hours. It's used to trigger Lambda functions for real-time downstream processing — syncing changes to S3, Elasticsearch, or Redshift — enabling CDC (Change Data Capture) patterns.

**Q: What is the difference between eventually consistent and strongly consistent reads?**
A: Eventually consistent reads (default) may return slightly stale data immediately after a write, but cost half the read capacity of strongly consistent reads. Strongly consistent reads always return the latest committed data, consuming one full read capacity unit per 4KB.

**Q: What is DynamoDB on-demand capacity mode vs. provisioned mode?**
A: On-demand mode automatically scales to any request rate and charges per request — ideal for unpredictable or spiky workloads. Provisioned mode requires specifying read/write capacity units upfront (with optional Auto Scaling) and is more cost-effective for predictable, steady-state workloads.

**Q: How do you handle hot partitions in DynamoDB?**
A: Hot partitions occur when too many requests target the same partition key. Solutions include: choosing high-cardinality partition keys, adding a random suffix to distribute writes across multiple logical partitions (write sharding), or using DAX (DynamoDB Accelerator) to cache hot read items.

**Q: What is DynamoDB Accelerator (DAX)?**
A: DAX is a fully managed, in-memory cache for DynamoDB that delivers microsecond response times for read-heavy workloads. It's API-compatible with DynamoDB, so applications need minimal code changes. Use it when your read-to-write ratio is high and sub-millisecond latency matters.

---

## 💼 Interview Tips

- Always lead with access pattern design: DynamoDB's schema must be designed around queries, not the other way around. Interviewers assess whether you understand that you model data to fit access patterns, unlike relational databases.
- Mention the single-table design pattern for advanced discussions — storing multiple entity types in one table using composite sort keys reduces table count and enables efficient joins via query, which senior interviewers recognize as deep DynamoDB expertise.
- Avoid the mistake of suggesting DynamoDB for complex analytical queries — it's not a replacement for Redshift or Athena. Be clear about its strengths: high-throughput, low-latency operational access.
- Senior interviewers want to hear about cost control: choosing the right capacity mode, setting TTL (Time to Live) to automatically expire stale items and reduce storage costs, and using sparse indexes.
- Know the hard limits that matter: item size max 400KB, partition throughput limit 3,000 RCUs or 1,000 WCUs — mention these when discussing scale.
- Demonstrate operational thinking by describing how you'd use DynamoDB Streams with Lambda for CDC-based replication into S3/Redshift, a pattern commonly used in real-time data lake architectures.
