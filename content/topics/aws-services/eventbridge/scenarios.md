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
