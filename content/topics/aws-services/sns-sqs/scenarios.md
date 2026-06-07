---
title: "AWS SNS/SQS - Scenario Questions"
topic: aws-services
subtopic: sns-sqs
content_type: scenario_question
tags: [aws, sns, sqs, interview, scenarios, messaging]
---

# Scenario Questions — AWS SNS/SQS

<article data-difficulty="junior">

## 🟢 Junior: SNS vs SQS — When to Use Which

**Scenario:** Your data pipeline needs to: (1) Notify 3 teams (email, Slack, PagerDuty) when a job fails, and (2) Queue failed records for retry processing by a single consumer. Which service for each?

<details>
<summary>✅ Solution</summary>

| Requirement | Service | Why |
|------------|---------|-----|
| Notify 3 teams simultaneously | **SNS** | Fan-out: one message → multiple subscribers (email, Slack webhook, PagerDuty) |
| Queue failed records for retry | **SQS** | Point-to-point: one consumer processes each message exactly once |

**SNS for fan-out notifications:**
```python
sns = boto3.client('sns')
# Create topic + subscribe multiple endpoints
topic = sns.create_topic(Name='pipeline-alerts')
sns.subscribe(TopicArn=topic['TopicArn'], Protocol='email', Endpoint='team@co.com')
sns.subscribe(TopicArn=topic['TopicArn'], Protocol='https', Endpoint='https://slack.webhook/...')
sns.subscribe(TopicArn=topic['TopicArn'], Protocol='https', Endpoint='https://pagerduty.com/...')

# One publish → all 3 receive
sns.publish(TopicArn=topic['TopicArn'], Subject='Job Failed', Message='daily-etl failed at 6:05 AM')
```

**SQS for retry queue:**
```python
sqs = boto3.client('sqs')
# Failed records go into a queue for later reprocessing
sqs.send_message(
    QueueUrl='https://sqs.../failed-records-queue',
    MessageBody=json.dumps({'record_id': '123', 'error': 'validation_failed'}),
    MessageAttributes={'RetryCount': {'DataType': 'Number', 'StringValue': '1'}}
)
# A Lambda or consumer polls the queue and retries each record
```

**Key difference:** SNS = pub/sub (one-to-many, push). SQS = queue (many-to-one, pull). Use both together: SNS publishes → SQS queues subscribe (fan-out to multiple queues for different consumers).

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Dead Letter Queue for Pipeline Errors

**Scenario:** Your Lambda processes records from an SQS queue. Some records are "poison messages" that always fail (bad data format). After 3 failed attempts, they should be routed to a dead-letter queue (DLQ) for manual investigation. Design this.

<details>
<summary>✅ Solution</summary>

```python
sqs = boto3.client('sqs')

# Step 1: Create the Dead Letter Queue
dlq = sqs.create_queue(
    QueueName='orders-processing-dlq',
    Attributes={'MessageRetentionPeriod': '1209600'}  # Keep for 14 days
)

# Step 2: Create the main queue WITH DLQ redrive policy
main_queue = sqs.create_queue(
    QueueName='orders-processing',
    Attributes={
        'RedrivePolicy': json.dumps({
            'deadLetterTargetArn': 'arn:aws:sqs:us-east-1:123:orders-processing-dlq',
            'maxReceiveCount': '3'  # After 3 failed attempts → move to DLQ
        }),
        'VisibilityTimeout': '300'  # 5 min for Lambda to process
    }
)

# Step 3: Lambda processes messages (failures automatically retry)
def handler(event, context):
    for record in event['Records']:
        try:
            data = json.loads(record['body'])
            process_order(data)  # If this throws, message becomes visible again after timeout
        except PermanentError as e:
            # Don't retry permanent errors — let it go to DLQ after 3 attempts
            print(f"Permanent error: {e}")
            raise  # Lambda reports failure → SQS retries → after 3x → DLQ
        except TransientError as e:
            # Transient: will succeed on retry
            raise  # SQS will retry automatically

# Step 4: Monitor DLQ (alert if messages accumulate)
# CloudWatch alarm: ApproximateNumberOfMessagesVisible > 0 on DLQ
```

**Flow:** Message received → Lambda processes → if fails, message reappears after visibility timeout → Lambda retries → if fails 3 times total → message moves to DLQ → alert team.

**DLQ investigation:**
```python
# Read DLQ messages for debugging (without deleting them)
response = sqs.receive_message(
    QueueUrl=dlq_url, MaxNumberOfMessages=10,
    MessageAttributeNames=['All'], AttributeNames=['All']
)
for msg in response['Messages']:
    print(f"Failed message: {msg['Body']}")
    print(f"Receive count: {msg['Attributes']['ApproximateReceiveCount']}")
    # Fix the data issue, then reprocess or delete
```

</details>

</article>
