---
title: "AWS EventBridge - Senior Deep Dive"
topic: aws-services
subtopic: eventbridge
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, eventbridge, architecture, event-driven, cross-account]
---

# AWS EventBridge — Senior-Level Deep Dive

## Cross-Account Event Routing

```python
import boto3
import json

events = boto3.client('events')

# Architecture: Domain accounts publish events → Central event bus → Fan-out to consumers

# Central account: create event bus and allow cross-account put
events.create_event_bus(Name='central-data-events')

# Resource policy: allow other accounts to put events
events.put_permission(
    EventBusName='central-data-events',
    Action='events:PutEvents',
    Principal='*',
    StatementId='AllowOrgAccounts',
    Condition={
        'Type': 'StringEquals',
        'Key': 'aws:PrincipalOrgID',
        'Value': 'o-organization123'
    }
)

# Domain account: forward events to central bus
# Rule in domain account's default bus
events.put_rule(
    Name='forward-to-central',
    EventPattern=json.dumps({
        "source": ["data-platform.orders"]
    }),
    State='ENABLED'
)

events.put_targets(
    Rule='forward-to-central',
    Targets=[{
        'Id': 'central-bus',
        'Arn': 'arn:aws:events:us-east-1:000000000000:event-bus/central-data-events',
        'RoleArn': 'arn:aws:iam::111111111111:role/EventBridgeCrossAccountRole'
    }]
)

# Central account: route events to consumers
events.put_rule(
    Name='route-orders-events',
    EventBusName='central-data-events',
    EventPattern=json.dumps({
        "source": ["data-platform.orders"],
        "detail-type": ["Order Data Ready"]
    }),
    State='ENABLED'
)

events.put_targets(
    Rule='route-orders-events',
    EventBusName='central-data-events',
    Targets=[
        {
            'Id': 'analytics-account',
            'Arn': 'arn:aws:events:us-east-1:222222222222:event-bus/default',
            'RoleArn': 'arn:aws:iam::000000000000:role/EventBridgeForwardRole'
        },
        {
            'Id': 'ml-account',
            'Arn': 'arn:aws:events:us-east-1:333333333333:event-bus/default',
            'RoleArn': 'arn:aws:iam::000000000000:role/EventBridgeForwardRole'
        }
    ]
)
```

---

## Event-Driven Data Lake Architecture

```python
# Complete event-driven pipeline: no schedulers, pure event flow

# Layer 1: Data arrives → event
# S3 → EventBridge → triggers processing
arrival_rule = {
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
        "bucket": {"name": ["data-lake-raw"]},
        "object": {"key": [{"prefix": "incoming/"}]}
    }
}

# Layer 2: Processing complete → event
# Glue job completes → EventBridge → triggers next stage
completion_rule = {
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {
        "jobName": [{"prefix": "transform-"}],
        "state": ["SUCCEEDED"]
    }
}

# Layer 3: Custom quality event → triggers notification or next pipeline
# Custom event published after quality checks pass
events.put_events(Entries=[{
    'Source': 'data-platform.quality',
    'DetailType': 'Data Quality Check Passed',
    'Detail': json.dumps({
        'table': 'curated.orders',
        'partition': '2024-01-15',
        'checks_passed': 5,
        'checks_total': 5,
        'ready_for_consumption': True
    }),
    'EventBusName': 'data-platform-events'
}])

# Layer 4: Consumption-ready event → notify analytics teams
consumption_rule = {
    "source": ["data-platform.quality"],
    "detail-type": ["Data Quality Check Passed"],
    "detail": {
        "ready_for_consumption": [True]
    }
}
```

**Benefits of event-driven over scheduler-based:**
- No wasted runs (processes only when data arrives)
- Natural backpressure (events queue if downstream is slow)
- Easy to add new consumers without modifying producers
- Audit trail (every event is logged)
- Lower latency (react in seconds vs wait for next cron window)

---

## EventBridge vs SNS/SQS Decision Framework

| Dimension | EventBridge | SNS/SQS |
|-----------|-------------|---------|
| Pattern matching | Rich (prefix, numeric, exists) | Basic filter policies |
| Sources | 100+ AWS services + custom | Custom only |
| Throughput | Soft limit (customizable) | SNS: near unlimited, SQS: unlimited |
| Message size | 256 KB | 256 KB (SQS), 256 KB (SNS) |
| Replay | Archive + Replay (built-in) | No (SQS: messages deleted after consumption) |
| Schema | Schema Registry + Discovery | No schema management |
| Cross-account | Native event bus forwarding | Queue/topic policies |
| Scheduling | Built-in Scheduler | No (use EventBridge for scheduling) |
| Cost | $1.00/1M events | $0.40/1M (SQS), $0.50/1M (SNS) |
| Best for | Event routing, orchestration | Queue-based processing, fan-out |

**Decision:**
- Use EventBridge when: reacting to AWS service events, need rich filtering, cross-account routing, archive/replay
- Use SNS when: high-volume fan-out, simple pub/sub, need SQS as consumer (SNS→SQS pattern)
- Use SQS when: task queue, buffering, need visibility timeout and DLQ
- Common combo: EventBridge for routing decisions → SNS for fan-out → SQS for consumption

---

## Global Endpoints (Multi-Region Failover)

```python
# EventBridge Global Endpoints: automatic failover between regions
events.create_endpoint(
    Name='data-platform-global',
    RoutingConfig={
        'FailoverConfig': {
            'Primary': {
                'HealthCheck': 'arn:aws:route53:::healthcheck/abc123'
            },
            'Secondary': {
                'Route': 'us-west-2'
            }
        }
    },
    ReplicationConfig={
        'State': 'ENABLED'  # Events replicated to secondary region
    },
    EventBuses=[
        {'EventBusArn': 'arn:aws:events:us-east-1:123456789:event-bus/data-platform-events'},
        {'EventBusArn': 'arn:aws:events:us-west-2:123456789:event-bus/data-platform-events'}
    ]
)

# Publish via the global endpoint (automatic region routing)
# If us-east-1 health check fails → events route to us-west-2
events.put_events(
    EndpointId='data-platform-global.abc123',
    Entries=[{
        'Source': 'data-platform.etl',
        'DetailType': 'Pipeline Completed',
        'Detail': json.dumps({'pipeline': 'daily-orders'})
    }]
)
```

---

## API Destinations (Webhook Integration)

```python
# Send events to external HTTP endpoints (Slack, Jira, custom APIs)

# Create API connection (credentials)
events.create_connection(
    Name='slack-webhook-connection',
    AuthorizationType='API_KEY',
    AuthParameters={
        'ApiKeyAuthParameters': {
            'ApiKeyName': 'Authorization',
            'ApiKeyValue': 'Bearer xoxb-slack-token'
        }
    }
)

# Create API destination
events.create_api_destination(
    Name='slack-pipeline-alerts',
    ConnectionArn='arn:aws:events:us-east-1:123456789:connection/slack-webhook-connection',
    HttpMethod='POST',
    InvocationEndpoint='https://slack.com/api/chat.postMessage',
    InvocationRateLimitPerSecond=10
)

# Target: transform event → Slack message format
events.put_targets(
    Rule='pipeline-failures',
    Targets=[{
        'Id': 'slack-alert',
        'Arn': 'arn:aws:events:us-east-1:123456789:api-destination/slack-pipeline-alerts',
        'RoleArn': 'arn:aws:iam::123456789:role/EventBridgeApiRole',
        'InputTransformer': {
            'InputPathsMap': {
                'job': '$.detail.jobName',
                'error': '$.detail.message',
                'time': '$.time'
            },
            'InputTemplate': '{"channel": "#data-alerts", "text": "Pipeline <job> failed at <time>: <error>"}'
        }
    }]
)
```

---

## Content-Based Filtering Performance

```python
# EventBridge evaluates rules in parallel — adding rules doesn't slow processing
# But complex patterns with many conditions may affect evaluation cost

# Efficient pattern (specific, narrow match)
efficient_pattern = {
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {"state": ["FAILED"]}
}

# Less efficient (broad with nested matching)
broad_pattern = {
    "detail": {
        "error": [{"exists": True}],
        "message": [{"prefix": "Error"}],
        "metadata": {
            "tags": [{"anything-but": ["test"]}]
        }
    }
}

# Best practices:
# 1. Filter on "source" and "detail-type" first (most selective)
# 2. Avoid pattern-only rules without source filter (matches all events)
# 3. Use specific string matches over prefix/suffix when possible
# 4. Limit nested matching depth (performance degrades with depth)
```

---

## Cost Optimization

```
EventBridge Pricing:
  Custom events: $1.00 per 1M events published
  AWS service events: FREE (Glue, S3, EC2 state changes)
  Scheduler invocations: $1.00 per 1M invocations
  Pipes: $0.40 per 1M requests + enrichment compute
  Schema Registry: Free (discovery) + $0.10 per schema update
  Archive: $0.023/GB stored + $0.02/GB replayed

Cost optimization:
  - Leverage free AWS service events (Glue/S3 state changes)
  - Batch custom events when possible (reduces publish count)
  - Set archive retention to minimum needed (storage cost)
  - Use filter policies on rules (don't trigger targets unnecessarily)
  - Scheduler: use FlexibleTimeWindow to batch nearby invocations
```

---

## Interview Tips

> **Tip 1:** "How do you implement cross-account event routing for a data mesh?" — "Central event bus in a governance account with resource policy allowing org accounts to publish. Domain accounts forward their events (data-ready, quality-passed) to the central bus. Central bus has rules that route events to consumer accounts based on content (domain, event type). This decouples producers from consumers — new consumers subscribe without producer changes. Archive all events for replay/audit."

> **Tip 2:** "EventBridge vs SNS/SQS for event-driven architecture?" — "Different layers: EventBridge for smart routing (rich pattern matching, 100+ AWS service integrations, archive/replay). SNS for high-volume fan-out (simpler, cheaper at scale). SQS for consumption buffering (DLQ, visibility timeout). In practice, combine them: EventBridge routes events based on content → SNS fans out to multiple SQS queues → consumers process from SQS with retry logic."

> **Tip 3:** "How do you make an event-driven pipeline resilient?" — "Four mechanisms: (1) Archive + Replay for reprocessing after bugs (replay the exact events from the failed time window). (2) DLQ on targets (failed invocations captured). (3) Global endpoints for regional failover (health check triggers automatic routing). (4) Idempotent consumers (same event processed twice produces same result). The archive is the killer feature — you can literally replay yesterday's events through a fixed pipeline."

## ⚡ Cheat Sheet

**Pricing**
- Custom events: $1.00/1M; AWS service events (Glue/S3/EC2 state changes): FREE
- Scheduler invocations: $1.00/1M; Pipes: $0.40/1M + enrichment compute
- Archive storage: $0.023/GB; Replay: $0.02/GB replayed

**Pattern Matching Performance**
- Always filter on `source` + `detail-type` first (most selective fields)
- Avoid rules with no `source` filter (matches every event in the bus)
- Rules evaluated in parallel — adding more rules doesn't slow individual rule evaluation
- Deep nested matching (`detail.metadata.tags`) degrades performance vs flat matches

**EventBridge vs SNS vs SQS**
| | EventBridge | SNS | SQS |
|---|---|---|---|
| Filtering | Rich (prefix, numeric, exists) | Basic | None (filter in consumer) |
| Sources | 100+ AWS services + custom | Custom only | Custom only |
| Replay | Yes (archive) | No | No |
| Best for | Routing, orchestration | Fan-out | Task queue |
- Common combo: EventBridge routes → SNS fans out → SQS buffers for consumption

**Cross-Account Architecture**
- Central event bus resource policy: allow `events:PutEvents` scoped to org ID (`aws:PrincipalOrgID`)
- Domain accounts forward events via rules targeting the central bus ARN with an IAM role
- Central bus routes events to consumer account buses — producers never know about consumers

**Resilience Patterns**
- Archive + Replay: reprocess exact events from a failed time window (killer feature)
- DLQ on targets: failed invocations captured for inspection/retry
- Global Endpoints: Route 53 health check triggers automatic failover to secondary region
- Idempotent consumers: processing the same event twice must produce the same result

**API Destinations**
- POST to any HTTPS endpoint (Slack, Jira, custom webhooks) with managed connection auth
- Rate limit: `InvocationRateLimitPerSecond` (max 300/sec)
- Use `InputTransformer` to reshape EventBridge event JSON into target API's expected format
