---
title: "AWS CloudWatch - Senior Deep Dive"
topic: aws-services
subtopic: cloudwatch
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, cloudwatch, observability, cost-optimization, architecture]
---

# AWS CloudWatch — Senior-Level Deep Dive

## Embedded Metric Format (EMF) — High-Cardinality Metrics

EMF lets you embed custom metrics directly in structured log output. CloudWatch automatically extracts them — no PutMetricData API calls needed:

```python
import json
import sys
from datetime import datetime

def emit_emf_metric(job_name, table_name, records, duration_ms, error_count):
    """Emit structured log that CloudWatch parses as metrics"""
    emf_log = {
        "_aws": {
            "Timestamp": int(datetime.utcnow().timestamp() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "DataPipeline/ETL",
                "Dimensions": [["JobName", "TableName"]],
                "Metrics": [
                    {"Name": "RecordsProcessed", "Unit": "Count"},
                    {"Name": "ProcessingDuration", "Unit": "Milliseconds"},
                    {"Name": "ErrorCount", "Unit": "Count"}
                ]
            }]
        },
        "JobName": job_name,
        "TableName": table_name,
        "RecordsProcessed": records,
        "ProcessingDuration": duration_ms,
        "ErrorCount": error_count,
        "message": f"Processed {records} records from {table_name}"
    }
    # Print to stdout (CloudWatch Logs agent picks this up)
    print(json.dumps(emf_log))
    sys.stdout.flush()

# Usage in Glue job
emit_emf_metric('daily-transform', 'orders', 150000, 45000, 0)
emit_emf_metric('daily-transform', 'customers', 50000, 12000, 2)
```

**EMF advantages over PutMetricData:**
- No API throttle limits (metrics extracted from logs)
- High-cardinality dimensions (table name, partition, customer_id)
- Metrics + logs in one output (correlate metric spike with log context)
- No additional cost beyond log ingestion

---

## Log Analytics Pipeline: Subscriptions to Kinesis to S3

```python
import boto3

logs = boto3.client('logs')

# Stream logs to Kinesis for real-time processing
logs.put_subscription_filter(
    logGroupName='/aws/glue/jobs/output',
    filterName='AllLogs',
    filterPattern='',  # All log events
    destinationArn='arn:aws:kinesis:us-east-1:123456789:stream/log-analytics',
    roleArn='arn:aws:iam::123456789:role/CWLogsToKinesis'
)

# Architecture:
# CloudWatch Logs → Subscription Filter → Kinesis Data Stream
#   → Kinesis Firehose → S3 (Parquet, partitioned by date)
#   → Athena queries for log analytics
#   → QuickSight dashboards

# Alternative: direct to Firehose (simpler, no real-time processing)
logs.put_subscription_filter(
    logGroupName='/aws/lambda',
    filterName='LambdaLogs',
    filterPattern='{ $.level = "ERROR" }',
    destinationArn='arn:aws:firehose:us-east-1:123456789:deliverystream/error-logs',
    roleArn='arn:aws:iam::123456789:role/CWLogsToFirehose'
)
```

**When to use this pattern:**
- Log retention > 30 days (CloudWatch Logs is expensive for long-term storage)
- Cross-service log correlation (join logs from different services in Athena)
- Complex analytics not possible in Log Insights (multi-day trends, ML anomaly detection)
- Cost: S3 storage is ~90% cheaper than CloudWatch Logs for archival

---

## Cross-Account Observability Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Monitoring Account                       │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Central Dashboard                               │    │
│  │  - Metrics from all accounts                     │    │
│  │  - Cross-account Log Insights                    │    │
│  │  - Unified alarms                                │    │
│  └─────────────────────────────────────────────────┘    │
│                         ▲                                │
│                    OAM Sink                               │
└─────────────────────────┼───────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
   OAM Link         OAM Link         OAM Link
        │                 │                 │
┌───────┴──────┐  ┌───────┴──────┐  ┌───────┴──────┐
│ Account A    │  │ Account B    │  │ Account C    │
│ (ETL/Glue)   │  │ (Analytics)  │  │ (Streaming)  │
└──────────────┘  └──────────────┘  └──────────────┘
```

```python
# Monitoring account: query metrics from all source accounts
cloudwatch = boto3.client('cloudwatch')

# Get ETL metrics from Account A (visible via OAM link)
response = cloudwatch.get_metric_data(
    MetricDataQueries=[
        {
            'Id': 'glue_errors',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Glue',
                    'MetricName': 'glue.driver.aggregate.numFailedTasks',
                    'Dimensions': [{'Name': 'AccountId', 'Value': '111111111111'}]
                },
                'Period': 300,
                'Stat': 'Sum'
            }
        }
    ],
    StartTime=datetime(2024, 1, 1),
    EndTime=datetime(2024, 1, 2)
)
```

---

## Cost Management — Metrics and Logs

### Metric Cost Optimization

| Resolution | Cost per Metric | Use Case |
|-----------|----------------|----------|
| Standard (60s) | $0.30/month | Most monitoring |
| High-resolution (1s) | $0.30/month + higher API costs | Real-time dashboards |
| Custom namespace | $0.30/month per metric | Business KPIs |

**Cost traps and solutions:**
- High-cardinality dimensions (user_id as dimension → millions of metrics) → Use EMF instead
- Unused alarms ($0.10/alarm/month) → Audit and delete quarterly
- Log retention (default: never expire) → Set 30-day retention, archive to S3

### Log Cost Optimization

```python
# Set retention to reduce costs (default is NEVER expire)
logs.put_retention_policy(logGroupName='/aws/glue/jobs/output', retentionInDays=30)
logs.put_retention_policy(logGroupName='/aws/lambda', retentionInDays=14)

# Use log class for infrequently accessed logs (50% cheaper ingestion)
logs.create_log_group(
    logGroupName='/archive/etl-history',
    logGroupClass='INFREQUENT_ACCESS'  # $0.25/GB vs $0.50/GB ingestion
)
```

---

## CloudWatch vs Datadog vs Prometheus

| Feature | CloudWatch | Datadog | Prometheus + Grafana |
|---------|-----------|---------|---------------------|
| Hosting | Managed (AWS) | SaaS | Self-hosted (or managed) |
| AWS integration | Native (zero config) | Agent-based | Exporters needed |
| Custom metrics | EMF, PutMetricData | DogStatsD, API | Pushgateway, pull model |
| Log analytics | Log Insights (basic) | Powerful (full-text) | Loki (label-based) |
| APM/Tracing | X-Ray (separate) | Built-in | Jaeger/Tempo |
| Cost (medium scale) | $500-2000/mo | $3000-10000/mo | Infrastructure only |
| Multi-cloud | No | Yes | Yes |
| Alerting | Basic (composite) | Advanced (ML-based) | Alertmanager (flexible) |

**Decision framework:**
- AWS-only, cost-sensitive → CloudWatch
- Multi-cloud, enterprise features → Datadog
- Full control, open-source, Kubernetes → Prometheus + Grafana

---

## Custom Dashboards for Data Platforms

```python
cloudwatch.put_dashboard(
    DashboardName='DataPlatform-Overview',
    DashboardBody=json.dumps({
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "title": "Pipeline Records Processed",
                    "metrics": [
                        ["DataPipeline/ETL", "RecordsProcessed", "JobName", "daily-orders"],
                        ["DataPipeline/ETL", "RecordsProcessed", "JobName", "daily-customers"]
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "view": "timeSeries"
                }
            },
            {
                "type": "log",
                "properties": {
                    "title": "Recent Errors",
                    "query": "fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20",
                    "region": "us-east-1",
                    "stacked": False
                }
            },
            {
                "type": "metric",
                "properties": {
                    "title": "Kinesis Iterator Age",
                    "metrics": [["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", "events-stream"]],
                    "period": 60,
                    "stat": "Maximum",
                    "annotations": {"horizontal": [{"value": 60000, "label": "SLA Threshold"}]}
                }
            }
        ]
    })
)
```

---

## Interview Tips

> **Tip 1:** "How do you handle high-cardinality metrics in CloudWatch?" — "Use Embedded Metric Format (EMF). Instead of calling PutMetricData with high-cardinality dimensions (which creates millions of metric streams at $0.30/each), emit structured JSON logs with the EMF schema. CloudWatch extracts metrics automatically. You get both the log context and the metrics without the cardinality cost explosion. Limit PutMetricData to low-cardinality dimensions (job_name, environment)."

> **Tip 2:** "How do you architect observability across multiple AWS accounts?" — "CloudWatch Observability Access Manager (OAM). Create a sink in the central monitoring account. Each source account creates a link sharing metrics, logs, and traces. The monitoring account sees all data in a single dashboard. Combine with cross-account Log Insights for unified troubleshooting. For cost: each account pays for their own log ingestion, the monitoring account pays for cross-account queries."

> **Tip 3:** "How do you optimize CloudWatch costs for a data platform?" — "Three levers: (1) Log retention — set 14-30 days instead of infinite, archive to S3 via subscription filters for long-term. (2) Use INFREQUENT_ACCESS log class for historical logs (50% cheaper). (3) Replace PutMetricData with EMF for high-volume metrics. Typical savings: 60-80% reduction from default settings. Monitor with Cost Explorer filtering on CloudWatch service."
