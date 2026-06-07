---
title: "AWS SNS/SQS - Intermediate"
topic: aws-services
subtopic: sns-sqs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, sns, sqs, messaging, decoupling, fan-out]
---

# AWS SNS/SQS — Intermediate Concepts

## FIFO Queues — Ordering and Deduplication

```python
import boto3

sqs = boto3.client('sqs')

# Create FIFO queue (name must end with .fifo)
queue = sqs.create_queue(
    QueueName='etl-events.fifo',
    Attributes={
        'FifoQueue': 'true',
        'ContentBasedDeduplication': 'true',  # SHA-256 hash of body for dedup
        'DeduplicationScope': 'messageGroup',  # Dedup per group, not entire queue
        'FifoThroughputLimit': 'perMessageGroupId'  # 300 msg/s per group
    }
)

# Send message with ordering guarantee
sqs.send_message(
    QueueUrl=queue['QueueUrl'],
    MessageBody='{"table": "orders", "partition": "2024-01-15", "status": "ready"}',
    MessageGroupId='orders-pipeline',  # Messages in same group are ordered
    MessageDeduplicationId='orders-2024-01-15-v1'  # Prevents duplicate processing
)

# Different group = independent ordering (parallel processing)
sqs.send_message(
    QueueUrl=queue['QueueUrl'],
    MessageBody='{"table": "customers", "partition": "2024-01-15", "status": "ready"}',
    MessageGroupId='customers-pipeline',  # Independent from orders group
    MessageDeduplicationId='customers-2024-01-15-v1'
)
```

| Feature | Standard Queue | FIFO Queue |
|---------|---------------|------------|
| Throughput | Unlimited | 300 msg/s (3000 with batching) |
| Ordering | Best-effort | Strict (per MessageGroupId) |
| Delivery | At-least-once | Exactly-once |
| Deduplication | None | 5-minute window |
| Use case | High-volume events | Ordered pipeline events |

---

## SNS Message Filtering

```python
sns = boto3.client('sns')

# Create topic for pipeline events
topic = sns.create_topic(TopicName='pipeline-events')

# Subscribe with filter policy (only receive matching messages)
# Team A: only cares about failures
sns.subscribe(
    TopicArn=topic['TopicArn'],
    Protocol='sqs',
    Endpoint='arn:aws:sqs:us-east-1:123456789:alerts-queue',
    Attributes={
        'FilterPolicy': '{"status": ["FAILED"], "severity": ["critical", "high"]}',
        'FilterPolicyScope': 'MessageAttributes'
    }
)

# Team B: only cares about their domain
sns.subscribe(
    TopicArn=topic['TopicArn'],
    Protocol='sqs',
    Endpoint='arn:aws:sqs:us-east-1:123456789:orders-team-queue',
    Attributes={
        'FilterPolicy': '{"domain": ["orders"], "event_type": ["completion", "failure"]}',
        'FilterPolicyScope': 'MessageAttributes'
    }
)

# Publish with attributes (filtering applies here)
sns.publish(
    TopicArn=topic['TopicArn'],
    Message='{"job": "daily-orders-etl", "records": 150000}',
    MessageAttributes={
        'status': {'DataType': 'String', 'StringValue': 'FAILED'},
        'severity': {'DataType': 'String', 'StringValue': 'critical'},
        'domain': {'DataType': 'String', 'StringValue': 'orders'},
        'event_type': {'DataType': 'String', 'StringValue': 'failure'}
    }
)
# Both subscriptions receive this message (matches both filters)
```

---

## Fan-Out Pattern (SNS to Multiple SQS)

```
S3 Event → SNS Topic
               │
    ┌──────────┼──────────┐
    │          │          │
┌───┴───┐ ┌───┴───┐ ┌───┴───┐
│SQS-ETL│ │SQS-ML │ │SQS-Arch│
│Process │ │Train  │ │Archive │
└───────┘ └───────┘ └───────┘
```

```python
# Single S3 event triggers multiple independent consumers
# Each consumer has its own queue (independent processing rate, retries)

# SNS topic receives S3 notifications
# Subscription 1: ETL processing queue
# Subscription 2: ML feature extraction queue
# Subscription 3: Archival/compliance queue

# Each queue has its own Lambda consumer
# If ETL fails, ML and archival are unaffected
```

---

## Dead-Letter Queues (DLQ) and Redrive Policy

```python
# Create DLQ first
dlq = sqs.create_queue(QueueName='etl-events-dlq')
dlq_arn = sqs.get_queue_attributes(
    QueueUrl=dlq['QueueUrl'],
    AttributeNames=['QueueArn']
)['Attributes']['QueueArn']

# Create main queue with redrive policy
main_queue = sqs.create_queue(
    QueueName='etl-events',
    Attributes={
        'RedrivePolicy': json.dumps({
            'deadLetterTargetArn': dlq_arn,
            'maxReceiveCount': '3'  # After 3 failed attempts → DLQ
        }),
        'VisibilityTimeout': '300',  # 5 minutes to process
        'MessageRetentionPeriod': '1209600'  # 14 days max
    }
)

# Monitor DLQ depth (alarm if messages accumulate)
cloudwatch = boto3.client('cloudwatch')
cloudwatch.put_metric_alarm(
    AlarmName='ETL-DLQ-Messages',
    Namespace='AWS/SQS',
    MetricName='ApproximateNumberOfMessagesVisible',
    Dimensions=[{'Name': 'QueueName', 'Value': 'etl-events-dlq'}],
    Statistic='Sum',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:oncall-alerts']
)

# Redrive from DLQ back to main queue (after fixing the issue)
sqs.start_message_move_task(
    SourceArn=dlq_arn,
    DestinationArn='arn:aws:sqs:us-east-1:123456789:etl-events'
)
```

---

## SQS Visibility Timeout and Long Polling

```python
# Visibility Timeout: time a message is hidden after being received
# If consumer doesn't delete it within this time → message reappears

# Long Polling: reduces empty responses and cost
messages = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=20,  # Long polling (wait up to 20s for messages)
    VisibilityTimeout=300,  # 5 min to process
    MessageAttributeNames=['All']
)

# Extend visibility if processing takes longer than expected
for msg in messages.get('Messages', []):
    try:
        process(msg)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
    except SlowProcessingError:
        # Extend timeout by another 5 minutes
        sqs.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=msg['ReceiptHandle'],
            VisibilityTimeout=600
        )
```

**Visibility timeout guidelines:**
- Set to 6x your average processing time
- Too short → duplicate processing (message reappears)
- Too long → failed messages take too long to retry

---

## Message Attributes

```python
# Message attributes carry metadata without parsing the body
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps({'partition': '2024-01-15', 'record_count': 50000}),
    MessageAttributes={
        'source_table': {'DataType': 'String', 'StringValue': 'raw.orders'},
        'priority': {'DataType': 'Number', 'StringValue': '1'},
        'file_size_mb': {'DataType': 'Number', 'StringValue': '250'},
        'compression': {'DataType': 'String', 'StringValue': 'snappy'}
    }
)

# Consumer can filter or route based on attributes without parsing body
```

---

## SNS + SQS + Lambda Pattern

```python
# Event-driven ETL: S3 upload → SNS → SQS → Lambda → Glue

# Lambda processes SQS messages (triggered automatically)
def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        # SNS wraps the message
        sns_message = json.loads(body['Message'])
        
        # Extract S3 event details
        s3_event = sns_message['Records'][0]['s3']
        bucket = s3_event['bucket']['name']
        key = s3_event['object']['key']
        
        # Start Glue job for new data
        glue = boto3.client('glue')
        glue.start_job_run(
            JobName='process-incoming-data',
            Arguments={
                '--source_path': f's3://{bucket}/{key}',
                '--target_path': 's3://curated/processed/'
            }
        )
```

---

## Interview Tips

> **Tip 1:** "When would you use FIFO vs Standard SQS?" — "Standard for high-throughput, order-independent processing (log ingestion, metrics collection). FIFO when message order matters (sequential pipeline stages, financial transactions) or when you need exactly-once delivery (prevent duplicate processing). Trade-off: FIFO is limited to 300 msg/sec (3000 with batching) vs unlimited for Standard. Use MessageGroupId to parallelize within FIFO (each group is independently ordered)."

> **Tip 2:** "Explain the fan-out pattern" — "SNS topic with multiple SQS subscribers. One event triggers multiple independent consumers. Example: new data in S3 → SNS → three queues (ETL processing, ML feature extraction, compliance archival). Each consumer processes at its own rate, has its own DLQ, and failures are isolated. Filter policies on subscriptions reduce noise — each consumer only receives events they care about."

> **Tip 3:** "How do you handle poison messages (messages that always fail)?" — "Dead-letter queue with maxReceiveCount=3. After 3 failed processing attempts, the message moves to the DLQ instead of blocking the queue forever. Monitor DLQ depth with CloudWatch alarm. Investigate root cause, fix the consumer, then use StartMessageMoveTask to redrive DLQ messages back to the main queue for reprocessing. Never delete DLQ messages without investigation."
