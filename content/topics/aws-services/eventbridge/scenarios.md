---
title: "AWS EventBridge - Scenario Questions"
topic: aws-services
subtopic: eventbridge
content_type: scenario_question
tags: [aws, eventbridge, interview, scenarios, event-driven]
---

# Scenario Questions — AWS EventBridge

<article data-difficulty="junior">

## 🟢 Junior: Schedule a Pipeline with EventBridge

**Scenario:** You need to trigger a Glue job every day at 6 AM UTC and a Lambda function every hour for data quality checks. Set up both using EventBridge.

<details>
<summary>✅ Solution</summary>

```python
import boto3
events = boto3.client('events')

# Rule 1: Daily Glue job at 6 AM UTC
events.put_rule(
    Name='daily-etl-trigger',
    ScheduleExpression='cron(0 6 * * ? *)',  # 6 AM UTC every day
    State='ENABLED'
)
events.put_targets(
    Rule='daily-etl-trigger',
    Targets=[{
        'Id': 'glue-job',
        'Arn': 'arn:aws:glue:us-east-1:123:job/daily-orders-etl',
        'RoleArn': 'arn:aws:iam::123:role/EventBridgeGlueRole',
        'Input': json.dumps({'Arguments': {'--process_date': '{{$.time}}'}})
    }]
)

# Rule 2: Hourly Lambda for quality checks
events.put_rule(
    Name='hourly-quality-check',
    ScheduleExpression='rate(1 hour)',  # Every hour
    State='ENABLED'
)
events.put_targets(
    Rule='hourly-quality-check',
    Targets=[{
        'Id': 'quality-lambda',
        'Arn': 'arn:aws:lambda:us-east-1:123:function:data-quality-checker'
    }]
)
```

**Why EventBridge over cron/Airflow for simple schedules:**
- Zero infrastructure (serverless)
- Native AWS integration (directly triggers Glue, Lambda, Step Functions)
- Fine-grained IAM control
- Built-in retry and dead-letter queue for failed deliveries

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Event-Driven Pipeline on Glue Completion

**Scenario:** Design a pipeline where: Glue job A finishes → triggers quality check Lambda → if quality passes → triggers Glue job B → when done → sends SNS notification. All event-driven (no polling, no scheduler).

<details>
<summary>✅ Solution</summary>

```python
# Rule 1: When Glue Job A succeeds → trigger quality Lambda
events.put_rule(
    Name='after-job-a-success',
    EventPattern=json.dumps({
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {
            "jobName": ["extract-orders"],
            "state": ["SUCCEEDED"]
        }
    }),
    State='ENABLED'
)
events.put_targets(Rule='after-job-a-success', Targets=[{
    'Id': 'quality-check',
    'Arn': 'arn:aws:lambda:...:function:quality-gate'
}])

# Rule 2: Quality Lambda publishes custom event when passed
# (Inside the Lambda):
def quality_lambda_handler(event, context):
    # ... run quality checks ...
    if all_checks_passed:
        events_client = boto3.client('events')
        events_client.put_events(Entries=[{
            'Source': 'custom.datapipeline',
            'DetailType': 'QualityCheckPassed',
            'Detail': json.dumps({'table': 'orders', 'date': '2024-01-15'})
        }])

# Rule 3: Quality passed → trigger Glue Job B
events.put_rule(
    Name='after-quality-pass',
    EventPattern=json.dumps({
        "source": ["custom.datapipeline"],
        "detail-type": ["QualityCheckPassed"]
    })
)
events.put_targets(Rule='after-quality-pass', Targets=[{
    'Id': 'job-b',
    'Arn': 'arn:aws:glue:...:job/transform-orders',
    'RoleArn': 'arn:aws:iam::123:role/EventBridgeGlueRole'
}])

# Rule 4: Glue Job B succeeds → SNS notification
events.put_rule(
    Name='after-job-b-success',
    EventPattern=json.dumps({
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {"jobName": ["transform-orders"], "state": ["SUCCEEDED"]}
    })
)
events.put_targets(Rule='after-job-b-success', Targets=[{
    'Id': 'notify',
    'Arn': 'arn:aws:sns:...:pipeline-notifications',
    'InputTransformer': {
        'InputTemplate': '"Pipeline completed successfully for <detail.jobName>"'
    }
}])
```

**Flow:** Glue A succeeds → EventBridge → Lambda (quality) → custom event → EventBridge → Glue B → EventBridge → SNS notification.

**Benefits over polling:** Zero wasted compute waiting, instant reaction to events, fully serverless, automatic retry on failed delivery.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Amazon EventBridge and how does it differ from SNS?**
A: EventBridge is a serverless event bus that routes events from AWS services, SaaS applications, and custom sources to targets using content-based filtering rules. SNS is a pub/sub messaging service for fan-out delivery to multiple subscribers. EventBridge offers richer filtering on event content, a schema registry, and cross-account/cross-region routing that SNS lacks.

**Q: What is an EventBridge Rule and how does event filtering work?**
A: A Rule matches incoming events against an event pattern (a JSON filter on event fields like source, detail-type, and nested detail attributes) and routes matching events to one or more targets. Filtering is content-based — you can match on specific field values, prefixes, numeric ranges, or the existence of fields.

**Q: What are EventBridge Pipes?**
A: Pipes create point-to-point integrations between a source (SQS, DynamoDB Streams, Kinesis) and a target (Lambda, Step Functions, SQS) with optional filtering and enrichment in between. They simplify event-driven pipeline patterns without custom glue code.

**Q: What is the EventBridge Schema Registry?**
A: The Schema Registry automatically discovers and stores event schemas from your event bus. It generates code bindings (Python, Java, TypeScript) for event schemas, enabling type-safe event handling in downstream consumers.

**Q: How does EventBridge handle event delivery failures?**
A: EventBridge retries delivery for up to 24 hours using exponential backoff. You can configure a Dead Letter Queue (DLQ) on targets to capture events that exhaust retries, and use CloudWatch metrics to monitor `FailedInvocations` and set alarms.

**Q: What is a custom event bus and when would you create one?**
A: Custom event buses receive events from your own applications or cross-account sources. Creating a separate bus for custom application events keeps them isolated from the default AWS service event bus, enabling cleaner event routing and access control via resource-based policies.

**Q: How do you trigger a Glue job or Step Functions workflow from an S3 file upload using EventBridge?**
A: Enable S3 Event Notifications to EventBridge on the bucket, create a rule with an event pattern matching `aws.s3` source and `Object Created` detail-type filtered to your prefix, and set the target to a Step Functions state machine or Glue job trigger via Lambda.

**Q: What is EventBridge Scheduler?**
A: EventBridge Scheduler is a fully managed scheduler that invokes targets on a one-time or recurring schedule (cron or rate expressions). It replaces CloudWatch Events scheduled rules with higher scale limits, timezone support, and a flexible time window for delivery.

---

## 💼 Interview Tips

- Frame EventBridge as the glue of event-driven architectures: explain how it decouples producers from consumers, allowing services to evolve independently — this is the key architectural benefit interviewers want to hear.
- Distinguish EventBridge from SQS and SNS clearly: EventBridge for content-based routing and orchestration triggers, SNS for fan-out pub/sub, SQS for reliable point-to-point queuing with backpressure.
- Senior interviewers appreciate hearing about cross-account event routing: sending events from a production account to a centralized observability or audit account using event bus policies — a common enterprise pattern.
- Mention Schema Registry as a governance and developer productivity win — auto-discovery of event schemas and generated code bindings reduce integration errors.
- Avoid treating EventBridge as a replacement for Kafka/Kinesis for high-throughput streaming: EventBridge handles thousands of events per second, not millions. Know the throughput boundaries.
- Demonstrate end-to-end pipeline thinking: describe how S3 → EventBridge → Step Functions creates a fully event-driven ingestion pipeline with zero polling and automatic triggering on file arrival.
