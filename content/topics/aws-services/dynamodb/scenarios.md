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
