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

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between Amazon SNS and Amazon SQS?**
A: SNS (Simple Notification Service) is a pub/sub messaging service that pushes messages to multiple subscribers simultaneously (fan-out). SQS (Simple Queue Service) is a point-to-point queuing service that holds messages until a single consumer processes and deletes them. Use SNS for fan-out to multiple systems; use SQS for reliable, ordered, decoupled processing between two services.

**Q: What is the SNS + SQS fan-out pattern and why is it used?**
A: The fan-out pattern involves publishing a message to an SNS topic that has multiple SQS queues subscribed to it. Each queue receives an independent copy of the message, allowing multiple downstream consumers (different microservices or pipelines) to process the same event independently and at their own pace.

**Q: What is the difference between SQS Standard and SQS FIFO queues?**
A: Standard queues offer unlimited throughput, at-least-once delivery, and best-effort ordering. FIFO queues guarantee exactly-once processing and strict first-in-first-out ordering within message groups, with a throughput limit of 300 msg/s (or 3,000 with batching). Use FIFO when order and deduplication are critical.

**Q: What is SQS visibility timeout and how does it prevent duplicate processing?**
A: When a consumer reads a message, it becomes invisible to other consumers for the visibility timeout duration. If the consumer successfully processes it and deletes it within that time, it's gone. If the consumer fails, the message becomes visible again after the timeout expires and another consumer can retry. Set timeout longer than your maximum processing time.

**Q: What is a Dead Letter Queue (DLQ) in SQS?**
A: A DLQ is a separate SQS queue that receives messages that failed to process after a configured number of retries (`maxReceiveCount`). It isolates poison pill messages from blocking the main queue, enabling debugging and reprocessing without disrupting normal message flow.

**Q: How does SQS integrate with Lambda for event-driven pipelines?**
A: Lambda has a native SQS event source mapping: Lambda polls the queue, batches messages (up to 10,000 per batch), and invokes the function. Failed batches return to the queue and retry. Configure `bisect-on-error` to split failing batches to isolate bad messages, and route exhausted messages to a DLQ.

**Q: What is SQS long polling and why should you use it?**
A: Long polling holds the `ReceiveMessage` request open for up to 20 seconds, returning immediately when a message arrives. It reduces empty API responses, lowers cost (fewer API calls), and reduces latency compared to short polling (which returns immediately even if the queue is empty). Always enable long polling in production.

**Q: What is the SQS message retention period and size limit?**
A: Messages are retained for 1 minute to 14 days (default 4 days). Maximum message size is 256KB; for larger payloads, use the Extended Client Library to store the body in S3 and send a reference in the SQS message.

---

## 💼 Interview Tips

- Master the fan-out pattern: SNS → multiple SQS queues is the standard answer for "how do you notify multiple downstream systems from a single event source." Interviewers expect you to draw this architecture confidently.
- Always mention DLQs when discussing SQS — production pipelines without DLQs lose messages silently on repeated failures. Describing DLQs + CloudWatch alarms on `ApproximateNumberOfMessagesNotVisible` shows operational maturity.
- Know when to use FIFO vs. Standard: FIFO for financial transactions, order processing, or any sequence-sensitive workflow; Standard for high-throughput, order-insensitive event processing. Confusing these is a common interview mistake.
- Senior interviewers probe visibility timeout sizing: if your processing takes 5 minutes and the visibility timeout is 30 seconds, messages will appear to duplicate. Describe how you'd size timeout to max expected processing time + buffer.
- Demonstrate the SQS-Lambda backpressure benefit: SQS buffers events during traffic spikes, preventing Lambda from being overwhelmed. Contrast this with Kinesis (where consumers must keep up) to show architectural nuance.
- Mention SQS message deduplication for FIFO: within a 5-minute deduplication window, identical messages (same MessageDeduplicationId) are not delivered twice — explain this is how exactly-once delivery is achieved.
