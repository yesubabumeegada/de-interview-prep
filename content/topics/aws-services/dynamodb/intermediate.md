---
title: "AWS DynamoDB - Intermediate"
topic: aws-services
subtopic: dynamodb
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, dynamodb, gsi, lsi, streams, ttl, transactions, capacity-modes]
---

# AWS DynamoDB — Intermediate Concepts

## Global Secondary Indexes (GSI)

GSIs let you query data by non-primary-key attributes. They maintain a separate copy of your data with a different partition key:

```python
import boto3

dynamodb = boto3.resource('dynamodb')

# Table: pipeline_runs
# PK: pipeline_id, SK: run_timestamp
# GSI: status-index (PK: status, SK: run_timestamp)
# GSI: date-index (PK: run_date, SK: pipeline_id)

table = dynamodb.Table('pipeline_runs')

# Query by primary key (fast, always works)
response = table.query(
    KeyConditionExpression='pipeline_id = :pid AND run_timestamp > :ts',
    ExpressionAttributeValues={
        ':pid': 'etl-orders-daily',
        ':ts': '2024-01-01T00:00:00Z'
    }
)

# Query by status using GSI (find all failed runs)
response = table.query(
    IndexName='status-index',
    KeyConditionExpression='#s = :status AND run_timestamp > :ts',
    ExpressionAttributeValues={
        ':status': 'FAILED',
        ':ts': '2024-01-01T00:00:00Z'
    },
    ExpressionAttributeNames={'#s': 'status'}  # status is a reserved word
)

# Query by date using GSI (all pipelines that ran today)
response = table.query(
    IndexName='date-index',
    KeyConditionExpression='run_date = :date',
    ExpressionAttributeValues={':date': '2024-01-15'}
)
```

**GSI characteristics:**

| Property | Value |
|----------|-------|
| Max per table | 20 |
| Consistency | Eventually consistent only |
| Projected attributes | ALL, KEYS_ONLY, or INCLUDE (specify) |
| Capacity | Separate from base table (own RCU/WCU) |
| Size | No limit (scales independently) |
| Backfill | Automatic (new GSI populates from existing data) |

---

## Local Secondary Indexes (LSI)

LSIs share the same partition key but allow a different sort key:

```python
# Table: sensor_readings
# PK: device_id, SK: timestamp
# LSI: temperature-index (PK: device_id, SK: temperature)

# Must be created at table creation time (cannot add later!)

# Find highest temperature readings for a device
response = table.query(
    IndexName='temperature-index',
    KeyConditionExpression='device_id = :did',
    ExpressionAttributeValues={':did': 'sensor-001'},
    ScanIndexForward=False,  # Descending (highest first)
    Limit=10
)
```

**GSI vs LSI comparison:**

| Aspect | GSI | LSI |
|--------|-----|-----|
| Partition key | Different from table | Same as table |
| Sort key | Different from table | Different from table |
| Create timing | Anytime | Table creation only |
| Read consistency | Eventually consistent | Strong or eventual |
| Storage limit | Unlimited | 10 GB per partition key |
| Capacity | Separate RCU/WCU | Shares table capacity |
| Typical use | Query by alternate key | Sort by alternate attribute |

---

## DynamoDB Streams for CDC

```python
# DynamoDB Streams: ordered, time-based log of item-level changes
# Use cases: CDC to data lake, cross-region replication, event sourcing

import json

def stream_handler(event, context):
    """Lambda triggered by DynamoDB Stream - process item changes."""
    
    for record in event['Records']:
        event_name = record['eventName']  # INSERT, MODIFY, REMOVE
        
        if event_name == 'INSERT':
            new_item = record['dynamodb']['NewImage']
            # Process new pipeline run
            print(f"New run: {deserialize(new_item)}")
            
        elif event_name == 'MODIFY':
            old_item = record['dynamodb']['OldImage']
            new_item = record['dynamodb']['NewImage']
            # Detect status change
            old_status = old_item.get('status', {}).get('S')
            new_status = new_item.get('status', {}).get('S')
            if old_status != new_status:
                print(f"Status changed: {old_status} -> {new_status}")
                if new_status == 'FAILED':
                    send_alert(deserialize(new_item))
                    
        elif event_name == 'REMOVE':
            old_item = record['dynamodb']['OldImage']
            print(f"Item deleted: {deserialize(old_item)}")

def deserialize(dynamodb_item):
    """Convert DynamoDB JSON format to plain dict."""
    from boto3.dynamodb.types import TypeDeserializer
    deserializer = TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in dynamodb_item.items()}
```

**Stream view types:**

| View Type | Content | Use Case |
|-----------|---------|----------|
| KEYS_ONLY | Only partition/sort key | Trigger processing, fetch item separately |
| NEW_IMAGE | Full item after change | Replication, downstream sync |
| OLD_IMAGE | Full item before change | Audit, change detection |
| NEW_AND_OLD_IMAGES | Both before and after | CDC, diff computation |

---

## TTL for Auto-Expiry

```python
# TTL automatically deletes items after a timestamp (free, no WCU consumed)
# Useful for: session data, temp state, idempotency tokens, audit logs

import time

def store_idempotency_token(token_id, result):
    """Store idempotency token with 24-hour TTL."""
    table.put_item(Item={
        'token_id': token_id,
        'result': result,
        'created_at': int(time.time()),
        'ttl_expire': int(time.time()) + 86400  # Expires in 24 hours
    })

# TTL attribute must be a Number type containing Unix epoch timestamp
# DynamoDB checks periodically (within 48 hours of expiry - not exact!)
# Expired items still appear in queries until actually deleted
# Deletions appear in DynamoDB Streams (can archive before permanent deletion)
```

> **Important:** TTL deletion is not instant. Items may persist up to 48 hours past the TTL timestamp. Do not rely on TTL for security-sensitive expiration (filter in your query instead).

---

## Capacity Modes

```python
# On-Demand: pay per request, no capacity planning
# Provisioned: set RCU/WCU, cheaper at predictable load

# On-demand pricing (us-east-1):
# Write: $1.25 per million WRU (write request units)
# Read: $0.25 per million RRU (read request units)

# Provisioned pricing:
# Write: $0.00065 per WCU/hour (~$0.47/month per WCU)
# Read: $0.00013 per RCU/hour (~$0.09/month per RCU)

# Break-even calculation:
# On-demand write: 1M writes/month = $1.25
# Provisioned equivalent: ~0.4 WCU = $0.19/month (76% cheaper!)
# But provisioned throttles if exceeded

# When to use each:
use_on_demand = [
    'New tables with unknown traffic patterns',
    'Spiky, unpredictable workloads',
    'Dev/test environments',
    'Tables with infrequent but burst access',
]

use_provisioned = [
    'Predictable, steady traffic (>30% utilization)',
    'Cost optimization at scale',
    'Combined with auto-scaling for some variability',
    'Reserved capacity for additional 20% savings',
]
```

---

## Batch Operations

```python
# BatchWriteItem: up to 25 items per call (16 MB max)
# BatchGetItem: up to 100 items per call (16 MB max)

def batch_write_pipeline_metadata(items):
    """Write multiple items efficiently using batch operations."""
    
    with table.batch_writer() as batch:
        # boto3 handles chunking into 25-item batches automatically
        for item in items:
            batch.put_item(Item=item)
    # Also handles retries for unprocessed items

def batch_get_pipeline_status(pipeline_ids):
    """Get status for multiple pipelines in one call."""
    
    keys = [{'pipeline_id': pid, 'run_date': 'latest'} for pid in pipeline_ids]
    
    response = dynamodb.meta.client.batch_get_item(
        RequestItems={
            'pipeline_runs': {
                'Keys': keys,
                'ProjectionExpression': 'pipeline_id, #s, last_run_time',
                'ExpressionAttributeNames': {'#s': 'status'}
            }
        }
    )
    
    results = response['Responses']['pipeline_runs']
    # Handle UnprocessedKeys (retry if throttled)
    unprocessed = response.get('UnprocessedKeys', {})
    if unprocessed:
        # Exponential backoff and retry
        pass
    
    return results
```

---

## DynamoDB Transactions

```python
# ACID transactions across up to 100 items per transaction (TransactWriteItems / TransactGetItems)
# Use case: update multiple items atomically

def complete_pipeline_run(pipeline_id, run_id, metrics):
    """Atomically update run status and pipeline summary."""
    
    dynamodb.meta.client.transact_write_items(
        TransactItems=[
            {
                # Update the specific run record
                'Update': {
                    'TableName': 'pipeline_runs',
                    'Key': {'pipeline_id': {'S': pipeline_id}, 'run_id': {'S': run_id}},
                    'UpdateExpression': 'SET #s = :status, completed_at = :ts, metrics = :m',
                    'ExpressionAttributeNames': {'#s': 'status'},
                    'ExpressionAttributeValues': {
                        ':status': {'S': 'COMPLETED'},
                        ':ts': {'S': datetime.utcnow().isoformat()},
                        ':m': {'M': metrics}
                    }
                }
            },
            {
                # Update pipeline summary (last successful run)
                'Update': {
                    'TableName': 'pipeline_summary',
                    'Key': {'pipeline_id': {'S': pipeline_id}},
                    'UpdateExpression': 'SET last_success = :ts, consecutive_failures = :zero',
                    'ExpressionAttributeValues': {
                        ':ts': {'S': datetime.utcnow().isoformat()},
                        ':zero': {'N': '0'}
                    }
                }
            },
            {
                # Conditional: only if run is currently IN_PROGRESS
                'ConditionCheck': {
                    'TableName': 'pipeline_runs',
                    'Key': {'pipeline_id': {'S': pipeline_id}, 'run_id': {'S': run_id}},
                    'ConditionExpression': '#s = :expected',
                    'ExpressionAttributeNames': {'#s': 'status'},
                    'ExpressionAttributeValues': {':expected': {'S': 'IN_PROGRESS'}}
                }
            }
        ]
    )
```

---

## Conditional Writes

```python
# Prevent race conditions with conditional expressions
# Only write if a condition on the existing item is true

def claim_pipeline_lock(pipeline_id, worker_id):
    """Acquire distributed lock with conditional write."""
    try:
        table.put_item(
            Item={
                'pipeline_id': pipeline_id,
                'lock_holder': worker_id,
                'acquired_at': int(time.time()),
                'ttl_expire': int(time.time()) + 300  # 5-minute lock
            },
            ConditionExpression='attribute_not_exists(pipeline_id) OR ttl_expire < :now',
            ExpressionAttributeValues={':now': int(time.time())}
        )
        return True  # Lock acquired
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False  # Lock held by another worker
```

---

## Interview Tips

> **Tip 1:** "When do you use GSI vs LSI in DynamoDB?" — "GSI when you need to query by a completely different attribute (e.g., query orders by customer instead of order_id). LSI when you want an alternate sort order within the same partition (e.g., sort sensor readings by temperature instead of timestamp). GSI is more flexible (add anytime, eventual consistent) while LSI must be defined at creation but supports strongly consistent reads."

> **Tip 2:** "How does DynamoDB Streams enable CDC for data lakes?" — "Enable Streams with NEW_AND_OLD_IMAGES view type. A Lambda function processes the stream records, transforms them, and writes to S3 in Parquet format (or sends to Kinesis for buffering). This gives you a real-time CDC pipeline from DynamoDB to your data lake without impacting table read capacity."

> **Tip 3:** "On-Demand vs Provisioned capacity — how do you decide?" — "Start on-demand for new tables (no capacity planning needed). Once traffic stabilizes and you can predict patterns, switch to provisioned with auto-scaling for 4-5x cost savings. The break-even is roughly 30% utilization of provisioned capacity. For tables with extreme spikes (0 to 10K WCU in seconds), on-demand handles it without throttling."
