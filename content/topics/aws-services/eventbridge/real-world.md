---
title: "AWS EventBridge - Real-World Production Examples"
topic: aws-services
subtopic: eventbridge
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, eventbridge, production, orchestration]
---

# AWS EventBridge — Real-World Production Examples

## Pattern 1: Event-Driven Pipeline (Glue Completion → Quality Check → Next Stage)

```python
import boto3
import json

events = boto3.client('events')

# Stage 1: Glue job completes → triggers quality check
events.put_rule(
    Name='etl-stage1-completed',
    EventPattern=json.dumps({
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {
            "jobName": ["extract-orders-raw"],
            "state": ["SUCCEEDED"]
        }
    }),
    State='ENABLED'
)

events.put_targets(
    Rule='etl-stage1-completed',
    Targets=[{
        'Id': 'trigger-quality-check',
        'Arn': 'arn:aws:lambda:us-east-1:123456789:function:data-quality-checker',
        'InputTransformer': {
            'InputPathsMap': {
                'job_name': '$.detail.jobName',
                'run_id': '$.detail.jobRunId'
            },
            'InputTemplate': '{"table": "raw.orders", "triggered_by": "<job_name>", "run_id": "<run_id>"}'
        }
    }]
)

# Stage 2: Quality check publishes custom event → triggers transform
# (Lambda publishes this after validation)
def quality_check_handler(event, context):
    """Validate data quality and emit event for next stage"""
    results = run_quality_checks(event['table'])
    
    events_client = boto3.client('events')
    events_client.put_events(Entries=[{
        'Source': 'data-platform.quality',
        'DetailType': 'Quality Check Result',
        'Detail': json.dumps({
            'table': event['table'],
            'status': 'PASSED' if results['passed'] else 'FAILED',
            'checks': results,
            'triggered_by': event['triggered_by']
        }),
        'EventBusName': 'data-platform-events'
    }])

# Stage 3: Quality passed → trigger transform
events.put_rule(
    Name='quality-passed-trigger-transform',
    EventBusName='data-platform-events',
    EventPattern=json.dumps({
        "source": ["data-platform.quality"],
        "detail-type": ["Quality Check Result"],
        "detail": {
            "status": ["PASSED"],
            "table": ["raw.orders"]
        }
    }),
    State='ENABLED'
)

events.put_targets(
    Rule='quality-passed-trigger-transform',
    EventBusName='data-platform-events',
    Targets=[{
        'Id': 'start-transform',
        'Arn': 'arn:aws:glue:us-east-1:123456789:job/transform-orders-curated',
        'RoleArn': 'arn:aws:iam::123456789:role/EventBridgeGlueRole'
    }]
)

# Stage 4: Quality failed → alert team
events.put_rule(
    Name='quality-failed-alert',
    EventBusName='data-platform-events',
    EventPattern=json.dumps({
        "source": ["data-platform.quality"],
        "detail-type": ["Quality Check Result"],
        "detail": {"status": ["FAILED"]}
    }),
    State='ENABLED'
)

events.put_targets(
    Rule='quality-failed-alert',
    EventBusName='data-platform-events',
    Targets=[{
        'Id': 'alert-sns',
        'Arn': 'arn:aws:sns:us-east-1:123456789:pipeline-alerts',
        'InputTransformer': {
            'InputPathsMap': {'table': '$.detail.table', 'checks': '$.detail.checks'},
            'InputTemplate': '"Quality FAILED for table <table>. Details: <checks>"'
        }
    }]
)
```

---

## Pattern 2: Cross-Account Data Platform Orchestration

```python
# Architecture: 
# Account A (Orders Domain): produces order data
# Account B (Customers Domain): produces customer data
# Account C (Analytics): consumes both, builds aggregates

# Account A: publish "data ready" event when ETL completes
def orders_etl_complete_handler(event, context):
    """Called after orders ETL job succeeds"""
    events_client = boto3.client('events')
    
    # Publish to local bus (forwarded to central via rule)
    events_client.put_events(Entries=[{
        'Source': 'orders-domain.etl',
        'DetailType': 'Data Product Ready',
        'Detail': json.dumps({
            'domain': 'orders',
            'table': 'curated.fact_orders',
            'partition': '2024-01-15',
            'record_count': 150000,
            'schema_version': '2.1',
            'freshness_timestamp': datetime.utcnow().isoformat()
        })
    }])

# Central Account: rule matches "Data Product Ready" from both domains
# Only triggers analytics when BOTH orders AND customers are ready
# (Uses Step Functions for coordination)

# Step Function: wait for both events
sfn_definition = {
    "StartAt": "WaitForBoth",
    "States": {
        "WaitForBoth": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "WaitForOrders",
                    "States": {
                        "WaitForOrders": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::events:putEvents.waitForTaskToken",
                            "Parameters": {
                                "Entries": [{
                                    "Source": "analytics.coordinator",
                                    "DetailType": "Waiting for Orders",
                                    "Detail": {"taskToken.$": "$$.Task.Token"}
                                }]
                            },
                            "TimeoutSeconds": 43200,
                            "End": True
                        }
                    }
                },
                {
                    "StartAt": "WaitForCustomers",
                    "States": {
                        "WaitForCustomers": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::events:putEvents.waitForTaskToken",
                            "Parameters": {
                                "Entries": [{
                                    "Source": "analytics.coordinator",
                                    "DetailType": "Waiting for Customers",
                                    "Detail": {"taskToken.$": "$$.Task.Token"}
                                }]
                            },
                            "TimeoutSeconds": 43200,
                            "End": True
                        }
                    }
                }
            ],
            "Next": "RunAggregation"
        },
        "RunAggregation": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": "build-customer-orders-aggregate"},
            "End": True
        }
    }
}
```

---

## Pattern 3: Scheduler Replacing CloudWatch Events Cron

```python
scheduler = boto3.client('scheduler')

# Migration: CloudWatch Events cron → EventBridge Scheduler
# Scheduler advantages: one-time schedules, timezone support, flexible windows

# Daily ETL trigger (6 AM Eastern, with 15-min flex window)
scheduler.create_schedule(
    Name='daily-etl-orders',
    ScheduleExpression='cron(0 6 * * ? *)',
    ScheduleExpressionTimezone='America/New_York',
    FlexibleTimeWindow={'Mode': 'FLEXIBLE', 'MaximumWindowInMinutes': 15},
    Target={
        'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:daily-orders-pipeline',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole',
        'Input': json.dumps({
            'date': '{{ .ScheduleTime | date "2006-01-02" }}',
            'triggered_by': 'scheduler'
        })
    },
    State='ENABLED'
)

# Hourly micro-batch ingestion
scheduler.create_schedule(
    Name='hourly-ingestion',
    ScheduleExpression='rate(1 hour)',
    FlexibleTimeWindow={'Mode': 'OFF'},
    Target={
        'Arn': 'arn:aws:lambda:us-east-1:123456789:function:trigger-ingestion',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole'
    }
)

# One-time backfill schedule (run once, then auto-delete)
scheduler.create_schedule(
    Name='backfill-q4-2023',
    ScheduleExpression='at(2024-02-01T02:00:00)',
    FlexibleTimeWindow={'Mode': 'OFF'},
    Target={
        'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:backfill-pipeline',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole',
        'Input': json.dumps({
            'start_date': '2023-10-01',
            'end_date': '2023-12-31',
            'parallelism': 10
        })
    },
    ActionAfterCompletion='DELETE'  # Auto-cleanup after execution
)

# Schedule group for organizing related schedules
scheduler.create_schedule_group(Name='data-platform-schedules')

# All platform schedules in one group (easy to pause/resume together)
scheduler.create_schedule(
    Name='weekly-compaction',
    GroupName='data-platform-schedules',
    ScheduleExpression='cron(0 2 ? * SUN *)',  # Sundays 2 AM
    FlexibleTimeWindow={'Mode': 'FLEXIBLE', 'MaximumWindowInMinutes': 60},
    Target={
        'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:compaction-pipeline',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole'
    }
)
```

---

## Pattern 4: Event Replay for Reprocessing Failures

```python
# Scenario: Bug in transform logic corrupted data for Jan 10-12
# Solution: Replay archived events to reprocess those days

events = boto3.client('events')

# Step 1: Archive is already configured (all custom events retained 90 days)
# Verify archive exists
archives = events.list_archives(EventSourceArn='arn:aws:events:us-east-1:123456789:event-bus/data-platform-events')

# Step 2: Fix the bug and deploy new transform code
# ... deploy fixed Glue job ...

# Step 3: Replay events from the affected period
replay = events.start_replay(
    ReplayName='reprocess-jan-10-12-bugfix',
    Description='Reprocessing after transform bug fix deployed',
    EventSourceArn='arn:aws:events:us-east-1:123456789:event-bus/data-platform-events',
    Destination={
        'Arn': 'arn:aws:events:us-east-1:123456789:event-bus/data-platform-events',
        'FilterArns': [
            # Only replay events that trigger the transform pipeline
            'arn:aws:events:us-east-1:123456789:rule/data-platform-events/quality-passed-trigger-transform'
        ]
    },
    EventStartTime=datetime(2024, 1, 10, 0, 0, 0),
    EventEndTime=datetime(2024, 1, 13, 0, 0, 0)  # Exclusive end
)

# Step 4: Monitor replay progress
status = events.describe_replay(ReplayName='reprocess-jan-10-12-bugfix')
print(f"State: {status['State']}")
print(f"Events replayed: {status.get('EventLastReplayedTime')}")

# Best practices for replay:
# - Test replay in staging first (replay to a test bus/rule)
# - Ensure consumers are idempotent (replayed events shouldn't duplicate data)
# - Use FilterArns to target only specific rules (don't replay notifications)
# - Monitor downstream for unexpected load during replay
```

---

## Production Monitoring

```python
# Monitor EventBridge health and event flow

cloudwatch = boto3.client('cloudwatch')

# Alarm: events failing to deliver to targets
cloudwatch.put_metric_alarm(
    AlarmName='EventBridge-FailedInvocations',
    Namespace='AWS/Events',
    MetricName='FailedInvocations',
    Dimensions=[{'Name': 'RuleName', 'Value': 'quality-passed-trigger-transform'}],
    Statistic='Sum',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:pipeline-alerts']
)

# Alarm: no events received (pipeline stalled)
cloudwatch.put_metric_alarm(
    AlarmName='EventBridge-NoEvents',
    Namespace='AWS/Events',
    MetricName='TriggeredRules',
    Dimensions=[{'Name': 'RuleName', 'Value': 'etl-stage1-completed'}],
    Statistic='Sum',
    Period=7200,  # 2 hours
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='LessThanThreshold',
    TreatMissingData='breaching',  # Missing data = no events = problem
    AlarmActions=['arn:aws:sns:us-east-1:123456789:pipeline-alerts']
)

# Dashboard: event flow visualization
dashboard_widgets = [
    {
        "type": "metric",
        "properties": {
            "title": "Events Matched vs Failed",
            "metrics": [
                ["AWS/Events", "MatchedEvents", "RuleName", "etl-stage1-completed"],
                ["AWS/Events", "FailedInvocations", "RuleName", "etl-stage1-completed", {"color": "#d62728"}]
            ],
            "period": 300,
            "stat": "Sum"
        }
    },
    {
        "type": "metric",
        "properties": {
            "title": "Event Latency (time to target)",
            "metrics": [
                ["AWS/Events", "IngestionToInvocationStartLatency", "RuleName", "etl-stage1-completed"]
            ],
            "period": 60,
            "stat": "Average"
        }
    }
]
```

---

## Interview Tips

> **Tip 1:** "Design an event-driven data pipeline" — "Chain stages via events: Glue completion emits AWS event → EventBridge rule triggers quality Lambda → Lambda publishes custom quality event → another rule triggers the next Glue job. Benefits over cron: processes immediately when data is ready (lower latency), naturally handles variable-length jobs, adding new consumers doesn't modify existing pipeline, archive enables replay for reprocessing. Monitor MatchedEvents and FailedInvocations per rule."

> **Tip 2:** "How do you handle reprocessing in an event-driven architecture?" — "EventBridge Archive + Replay. Archive all events to the custom bus (90-day retention). When a bug is fixed, replay the affected time window targeting only the specific rules that need reprocessing. Key requirement: consumers must be idempotent (writing to partitioned tables with overwrite semantics, or using upsert logic). This is significantly simpler than rebuilding state from scratch."

> **Tip 3:** "How do you coordinate multiple data sources before running aggregation?" — "Two approaches: (1) Step Functions with Parallel state waiting for callback tokens — each domain sends task success when their data is ready. (2) EventBridge rules that publish to a Lambda 'coordinator' which tracks state in DynamoDB (received orders? received customers? → if both, trigger aggregation). The Step Functions approach is cleaner for fixed dependencies; the coordinator pattern handles dynamic/unknown sources better."
