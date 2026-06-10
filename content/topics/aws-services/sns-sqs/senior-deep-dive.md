---
title: "AWS SNS/SQS - Senior Deep Dive"
topic: aws-services
subtopic: sns-sqs
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, sns, sqs, architecture, patterns]
---

# AWS SNS/SQS — Senior-Level Deep Dive

## Exactly-Once Processing with FIFO + Deduplication

```python
import boto3
import hashlib
import json

sqs = boto3.client('sqs')

# Strategy 1: Content-based deduplication (queue-level setting)
# SHA-256 of message body — same body within 5 min is deduplicated automatically

# Strategy 2: Explicit deduplication ID (for when body might differ slightly)
def send_idempotent_event(queue_url, table_name, partition, event_id):
    """Send exactly-once event with explicit dedup"""
    body = json.dumps({
        'table': table_name,
        'partition': partition,
        'timestamp': '2024-01-15T06:00:00Z',
        'event_id': event_id
    })
    
    # DeduplicationId: same value within 5 min = message is rejected (not duplicated)
    dedup_id = f"{table_name}-{partition}-{event_id}"
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageGroupId=table_name,  # Order per table
        MessageDeduplicationId=dedup_id
    )

# Consumer-side idempotency (defense in depth)
def process_with_idempotency(message):
    """Idempotent processing even if SQS delivers twice"""
    dynamodb = boto3.resource('dynamodb')
    idempotency_table = dynamodb.Table('processing-idempotency')
    
    message_id = message['MessageId']
    
    # Check if already processed
    try:
        idempotency_table.put_item(
            Item={'message_id': message_id, 'processed_at': int(time.time()), 'ttl': int(time.time()) + 86400},
            ConditionExpression='attribute_not_exists(message_id)'
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        print(f"Already processed {message_id}, skipping")
        return
    
    # Process the message (guaranteed first time)
    do_actual_work(message)
```

---

## Large Message Handling (S3 + SQS Extended Client)

```python
# SQS message size limit: 256 KB
# For larger payloads: store in S3, send reference via SQS

import json

def send_large_message(queue_url, payload, bucket='sqs-large-messages'):
    """Store large payload in S3, send reference via SQS"""
    s3 = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    
    payload_bytes = json.dumps(payload).encode()
    
    if len(payload_bytes) > 200_000:  # 200 KB threshold (leave room for metadata)
        # Store in S3
        key = f"messages/{datetime.utcnow().strftime('%Y/%m/%d')}/{uuid4()}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=payload_bytes)
        
        # Send S3 reference via SQS
        reference = {
            '_large_message': True,
            'bucket': bucket,
            'key': key,
            'size_bytes': len(payload_bytes)
        }
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(reference))
    else:
        # Small enough to send directly
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))

def receive_large_message(message_body):
    """Resolve large message reference from S3"""
    body = json.loads(message_body)
    
    if body.get('_large_message'):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=body['bucket'], Key=body['key'])
        return json.loads(response['Body'].read())
    else:
        return body
```

---

## Cross-Account Messaging

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCrossAccountPublish",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::987654321:role/ProducerRole"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:us-east-1:123456789:shared-events-queue",
            "Condition": {
                "ArnLike": {
                    "aws:SourceArn": "arn:aws:sns:us-east-1:987654321:data-events"
                }
            }
        }
    ]
}
```

```python
# Cross-account SNS → SQS pattern
# Account A (987654321): publishes to SNS topic
# Account B (123456789): subscribes SQS queue to Account A's SNS

# Account A: allow Account B to subscribe
sns.add_permission(
    TopicArn='arn:aws:sns:us-east-1:987654321:data-events',
    Label='CrossAccountSubscribe',
    AWSAccountId=['123456789'],
    ActionName=['Subscribe']
)

# Account B: subscribe their queue to Account A's topic
sns.subscribe(
    TopicArn='arn:aws:sns:us-east-1:987654321:data-events',
    Protocol='sqs',
    Endpoint='arn:aws:sqs:us-east-1:123456789:incoming-events'
)
# Also: SQS queue policy must allow SNS to send messages
```

---

## Batching and Throughput Optimization

```python
# Batch send: up to 10 messages per API call (reduces API costs)
entries = []
for i, record in enumerate(records_batch):
    entries.append({
        'Id': str(i),
        'MessageBody': json.dumps(record),
        'MessageAttributes': {
            'source': {'DataType': 'String', 'StringValue': 'etl-pipeline'}
        }
    })
    
    if len(entries) == 10:  # SQS batch limit
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        failed = response.get('Failed', [])
        if failed:
            print(f"Failed to send {len(failed)} messages: {failed}")
        entries = []

# Batch receive: up to 10 messages
messages = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,  # Always request max for throughput
    WaitTimeSeconds=20       # Long polling
)

# Batch delete after processing
if messages.get('Messages'):
    sqs.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {'Id': str(i), 'ReceiptHandle': msg['ReceiptHandle']}
            for i, msg in enumerate(messages['Messages'])
        ]
    )
```

**Throughput comparison:**

| Approach | Messages/sec | Cost (1M messages) |
|----------|-------------|-------------------|
| Individual send/receive | ~200 | $0.40 (1M requests) |
| Batch (10 per call) | ~2000 | $0.04 (100K requests) |
| FIFO individual | ~300 | $0.40 |
| FIFO with high throughput | ~3000 | $0.04 |

---

## Cost Analysis at Scale

```
SQS Pricing:
  Standard: $0.40 per 1M requests (first 1M free/month)
  FIFO: $0.50 per 1M requests
  Request = SendMessage, ReceiveMessage, DeleteMessage, etc.
  64 KB chunk = 1 request (256 KB message = 4 requests)

SNS Pricing:
  $0.50 per 1M publishes
  SQS delivery: free
  HTTP/HTTPS delivery: $0.60 per 1M
  
Example: Data platform with 10M events/day
  - Using batching (10 per call): 1M SQS requests/day = $0.40/day = $12/month
  - Without batching: 30M requests/day (send + receive + delete) = $12/day = $360/month
  Savings from batching: 97%
```

---

## SQS vs Kinesis Decision Framework

| Dimension | SQS | Kinesis Data Streams |
|-----------|-----|---------------------|
| Message consumption | Delete after processing | Replay within retention period |
| Ordering | FIFO only (per group) | Per shard (partition key) |
| Consumer model | Single consumer per message | Multiple consumers (fan-out) |
| Throughput | Unlimited (Standard) | 1 MB/s write, 2 MB/s read per shard |
| Retention | 4 days (max 14) | 24h to 365 days |
| Replay | No (once consumed, gone) | Yes (seek to any point in time) |
| Scaling | Automatic | Manual (add shards) |
| Pricing | Per request | Per shard-hour + data |
| Best for | Task queues, decoupling | Event streaming, analytics |

**Decision:**
- Use SQS when: messages are tasks to be completed once (ETL triggers, notifications)
- Use Kinesis when: events are records to be replayed/analyzed (clickstreams, IoT, logs)

---

## Message-Driven Architecture Patterns

```python
# Pattern: Saga (distributed transaction using SQS)
# Order processing: each step is a separate service with its own queue

saga_steps = {
    'validate-order': {
        'queue': 'order-validation-queue',
        'success_next': 'reserve-inventory',
        'failure_action': 'cancel-order'
    },
    'reserve-inventory': {
        'queue': 'inventory-reservation-queue',
        'success_next': 'process-payment',
        'failure_action': 'release-reservation'
    },
    'process-payment': {
        'queue': 'payment-processing-queue',
        'success_next': 'complete-order',
        'failure_action': 'release-reservation'
    }
}

# Each service:
# 1. Reads from its queue
# 2. Does its work
# 3. Sends success/failure to next step's queue
# 4. On failure: sends compensating events to previous steps

# Benefits for data pipelines:
# - Each stage is independently scalable
# - Failures don't cascade (DLQ per stage)
# - Easy to add new consumers (fan-out)
# - Built-in backpressure (queue depth = congestion signal)
```

---

## Interview Tips

> **Tip 1:** "How do you guarantee exactly-once processing with SQS?" — "Three layers: (1) FIFO queue with deduplication (rejects duplicate sends within 5-minute window). (2) Consumer-side idempotency using DynamoDB conditional writes (check message_id before processing). (3) Idempotent downstream operations (INSERT ON CONFLICT DO NOTHING, S3 PUT is naturally idempotent). No single mechanism is sufficient — defense in depth is required because SQS guarantees at-least-once, not exactly-once at the consumer level."

> **Tip 2:** "SQS vs Kinesis for a real-time data pipeline?" — "Kinesis for event streaming where you need replay, multiple consumers reading the same events, and ordering by partition key (clickstreams, IoT). SQS for task-based workloads where each message is processed once and deleted (ETL triggers, job notifications). Key differentiator: Kinesis retains events for replay; SQS deletes after consumption. Many platforms use both: Kinesis for ingestion, SQS for internal orchestration."

> **Tip 3:** "How do you handle a message that's too large for SQS?" — "SQS limit is 256 KB. For larger payloads (data samples, file manifests, large JSON): store the payload in S3, send a reference (bucket + key) via SQS. Consumer reads the reference, fetches from S3. Use the SQS Extended Client Library (Java) or implement the pattern manually. Set S3 lifecycle to delete referenced objects after the retention period."

## ⚡ Cheat Sheet

**SNS vs SQS vs EventBridge**
| Feature | SNS | SQS | EventBridge |
|---|---|---|---|
| Pattern | Pub/sub (push) | Queue (pull) | Event bus (routing) |
| Persistence | No (fire-and-forget) | Yes (up to 14 days) | Yes (archive) |
| Fan-out | Yes (multiple subscribers) | No (one consumer group) | Yes (multiple targets) |
| Filtering | Attribute-based | No | Content-based (JSON) |
| Best for | Notifications | Work queues, decoupling | Event routing, SaaS integration |

**SQS key settings**
| Setting | Default | Rule |
|---|---|---|
| Visibility timeout | 30s | Set to 6× max processing time |
| Message retention | 4 days | Max 14 days |
| Delivery delay | 0s | Up to 15 minutes |
| Max message size | 256 KB | Use S3 pointer for larger |
| Long polling | Off | Always enable (`WaitTimeSeconds=20`) |

**Dead-letter queue (DLQ)**
- Receives messages after `maxReceiveCount` failures (1–1000)
- Set alarm on `ApproximateNumberOfMessagesVisible > 0` for immediate alert
- DLQ message retention ≥ source queue retention

**FIFO queues**
- Exactly-once processing: deduplication ID (5-min window)
- Ordering: per `MessageGroupId`; use customer_id or session_id as group
- Throughput: 300 TPS standard; 3000 TPS with batching (10 msg/batch)

**SNS fan-out to SQS**
```
SNS Topic → SQS Queue A (analytics)
          → SQS Queue B (email service)
          → Lambda (real-time alert)
# Filter policy on subscription: {"event_type": ["order_placed"]}
```

**Lambda trigger from SQS**
- `batchSize`: 1–10000 messages per Lambda invocation
- `functionResponseTypes: ReportBatchItemFailures`: partial batch success
- Concurrency: 1 Lambda per shard (FIFO) or up to 1000 concurrent (Standard)
