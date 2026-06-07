---
title: "AWS EventBridge - Intermediate"
topic: aws-services
subtopic: eventbridge
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, eventbridge, event-driven, rules, scheduling]
---

# AWS EventBridge — Intermediate Concepts

## Event Patterns (Matching Syntax)

EventBridge rules use pattern matching to filter events:

```python
import boto3
import json

events = boto3.client('events')

# Pattern 1: Match specific Glue job state changes
events.put_rule(
    Name='glue-job-failure-rule',
    EventPattern=json.dumps({
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {
            "jobName": ["daily-orders-etl", "daily-customers-etl"],
            "state": ["FAILED", "TIMEOUT"]
        }
    }),
    State='ENABLED'
)

# Pattern 2: Match S3 object creation in specific prefix
events.put_rule(
    Name='new-data-arrival',
    EventPattern=json.dumps({
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {"name": ["data-lake-raw"]},
            "object": {"key": [{"prefix": "incoming/orders/"}]}
        }
    }),
    State='ENABLED'
)

# Pattern 3: Numeric matching (file size > 1GB)
events.put_rule(
    Name='large-file-alert',
    EventPattern=json.dumps({
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "object": {
                "size": [{"numeric": [">", 1073741824]}]
            }
        }
    }),
    State='ENABLED'
)

# Pattern 4: Exists/not-exists matching
events.put_rule(
    Name='events-with-error-field',
    EventPattern=json.dumps({
        "detail": {
            "error": [{"exists": True}]
        }
    }),
    State='ENABLED'
)
```

**Pattern operators:**
| Operator | Syntax | Example |
|----------|--------|---------|
| Exact match | `["value"]` | `"state": ["FAILED"]` |
| Prefix | `[{"prefix": "val"}]` | `"key": [{"prefix": "incoming/"}]` |
| Suffix | `[{"suffix": ".parquet"}]` | `"key": [{"suffix": ".parquet"}]` |
| Numeric | `[{"numeric": [">", 100]}]` | `"size": [{"numeric": [">", 1000000]}]` |
| Exists | `[{"exists": true}]` | `"error": [{"exists": true}]` |
| Anything-but | `[{"anything-but": "test"}]` | `"env": [{"anything-but": "dev"}]` |

---

## Input Transformers

Transform event data before sending to targets:

```python
# Transform Glue event into a clean notification message
events.put_targets(
    Rule='glue-job-failure-rule',
    Targets=[{
        'Id': 'notify-slack',
        'Arn': 'arn:aws:lambda:us-east-1:123456789:function:slack-notifier',
        'InputTransformer': {
            'InputPathsMap': {
                'job_name': '$.detail.jobName',
                'state': '$.detail.state',
                'error': '$.detail.message',
                'timestamp': '$.time'
            },
            'InputTemplate': '"Pipeline Alert: Job <job_name> entered state <state> at <timestamp>. Error: <error>"'
        }
    }]
)

# Transform S3 event into Glue job parameters
events.put_targets(
    Rule='new-data-arrival',
    Targets=[{
        'Id': 'start-glue-job',
        'Arn': 'arn:aws:glue:us-east-1:123456789:job/process-incoming',
        'RoleArn': 'arn:aws:iam::123456789:role/EventBridgeGlueRole',
        'InputTransformer': {
            'InputPathsMap': {
                'bucket': '$.detail.bucket.name',
                'key': '$.detail.object.key'
            },
            'InputTemplate': '{"--source_path": "s3://<bucket>/<key>", "--target_path": "s3://curated/processed/"}'
        }
    }]
)
```

---

## Event Bus Types

| Bus Type | Source | Use Case |
|----------|--------|----------|
| Default | AWS services | React to AWS events (Glue, S3, etc.) |
| Custom | Your applications | Internal events (pipeline status, data quality) |
| Partner | SaaS providers | Salesforce, Zendesk, PagerDuty events |

```python
# Create custom event bus for data platform
events.create_event_bus(Name='data-platform-events')

# Publish custom events
events.put_events(Entries=[{
    'Source': 'data-platform.etl',
    'DetailType': 'ETL Job Completed',
    'Detail': json.dumps({
        'job_name': 'daily-orders-transform',
        'status': 'SUCCESS',
        'records_processed': 150000,
        'duration_seconds': 245,
        'output_path': 's3://curated/orders/dt=2024-01-15/'
    }),
    'EventBusName': 'data-platform-events'
}])
```

---

## Archive and Replay

```python
# Archive events for replay (debugging, reprocessing)
events.create_archive(
    ArchiveName='pipeline-events-archive',
    EventSourceArn='arn:aws:events:us-east-1:123456789:event-bus/data-platform-events',
    EventPattern=json.dumps({
        "source": ["data-platform.etl"]
    }),
    RetentionDays=90
)

# Replay events from a time window (reprocess after fixing a bug)
events.start_replay(
    ReplayName='reprocess-jan-15',
    EventSourceArn='arn:aws:events:us-east-1:123456789:event-bus/data-platform-events',
    Destination={
        'Arn': 'arn:aws:events:us-east-1:123456789:event-bus/data-platform-events',
        'FilterArns': ['arn:aws:events:us-east-1:123456789:rule/data-platform-events/process-orders']
    },
    EventStartTime=datetime(2024, 1, 15, 0, 0, 0),
    EventEndTime=datetime(2024, 1, 15, 23, 59, 59)
)
```

---

## Schema Discovery

```python
# EventBridge automatically discovers event schemas
schemas = boto3.client('schemas')

# List discovered schemas
response = schemas.list_schemas(RegistryName='discovered-schemas')
for schema in response['Schemas']:
    print(f"Schema: {schema['SchemaName']}")

# Get schema definition (JSON Schema format)
schema_def = schemas.describe_schema(
    RegistryName='discovered-schemas',
    SchemaName='aws.glue@GlueJobStateChange'
)
# Use this to validate events or generate code bindings
```

---

## EventBridge Pipes (Source → Enrichment → Target)

```python
pipes = boto3.client('pipes')

# Pipe: SQS → Lambda enrichment → Step Functions
pipes.create_pipe(
    Name='data-ingestion-pipe',
    Source='arn:aws:sqs:us-east-1:123456789:incoming-files',
    SourceParameters={
        'SqsQueueParameters': {
            'BatchSize': 10,
            'MaximumBatchingWindowInSeconds': 30
        }
    },
    Enrichment='arn:aws:lambda:us-east-1:123456789:function:enrich-metadata',
    EnrichmentParameters={
        'InputTemplate': '{"file_key": <$.body.key>, "source": <$.body.source>}'
    },
    Target='arn:aws:states:us-east-1:123456789:stateMachine:process-file',
    TargetParameters={
        'StepFunctionStateMachineParameters': {
            'InvocationType': 'FIRE_AND_FORGET'
        }
    },
    RoleArn='arn:aws:iam::123456789:role/EventBridgePipesRole'
)
```

**Pipes vs Rules:**
- Pipes: point-to-point, with enrichment, filtering, and batching
- Rules: event-driven, fan-out to multiple targets, pattern matching
- Use Pipes when you need to transform/enrich before delivery
- Use Rules when you need pattern matching and multiple targets

---

## EventBridge Scheduler

```python
scheduler = boto3.client('scheduler')

# One-time schedule
scheduler.create_schedule(
    Name='backfill-2024-q1',
    ScheduleExpression='at(2024-04-01T06:00:00)',
    FlexibleTimeWindow={'Mode': 'OFF'},
    Target={
        'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:backfill-pipeline',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole',
        'Input': '{"start_date": "2024-01-01", "end_date": "2024-03-31"}'
    }
)

# Recurring schedule (cron)
scheduler.create_schedule(
    Name='daily-etl-trigger',
    ScheduleExpression='cron(0 6 * * ? *)',  # 6 AM UTC daily
    FlexibleTimeWindow={'Mode': 'FLEXIBLE', 'MaximumWindowInMinutes': 15},
    Target={
        'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:daily-pipeline',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole',
        'Input': '{"date": "<aws.scheduler.execution-id>"}'
    },
    State='ENABLED'
)

# Rate-based schedule
scheduler.create_schedule(
    Name='metrics-collection',
    ScheduleExpression='rate(5 minutes)',
    FlexibleTimeWindow={'Mode': 'OFF'},
    Target={
        'Arn': 'arn:aws:lambda:us-east-1:123456789:function:collect-metrics',
        'RoleArn': 'arn:aws:iam::123456789:role/SchedulerRole'
    }
)
```

---

## Interview Tips

> **Tip 1:** "How does EventBridge compare to CloudWatch Events?" — "EventBridge IS the successor to CloudWatch Events (same underlying service). EventBridge adds: custom event buses (application events), partner integrations (SaaS), schema discovery, archive and replay, Pipes (enrichment), and richer pattern matching (prefix, suffix, numeric). All new development should use EventBridge. Existing CloudWatch Events rules continue to work."

> **Tip 2:** "How do you trigger a pipeline when new data arrives in S3?" — "Enable EventBridge notifications on the S3 bucket. Create a rule matching `source: aws.s3, detail-type: Object Created` with a key prefix filter. Target can be Step Functions (for orchestration), Lambda (for processing), or Glue (direct job start). Use input transformers to pass the bucket/key as parameters. This replaces S3 event notifications to Lambda (more flexible, supports multiple targets)."

> **Tip 3:** "What's the difference between EventBridge Pipes and Rules?" — "Rules: pattern-based routing with fan-out (one event → multiple targets). Best for event-driven reactions to AWS or custom events. Pipes: point-to-point integration with optional enrichment and batching (source → enrich → target). Best for connecting a queue/stream to a target with transformation. Use Rules for 'when X happens, do Y and Z'. Use Pipes for 'pull from source, transform, deliver to target'."
