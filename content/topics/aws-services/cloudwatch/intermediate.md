---
title: "AWS CloudWatch - Intermediate"
topic: aws-services
subtopic: cloudwatch
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, cloudwatch, monitoring, logs, metrics, alarms]
---

# AWS CloudWatch — Intermediate Concepts

## Custom Metrics — Publishing from ETL Jobs

```python
import boto3
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

# Publish custom metrics from a Glue/ETL job
def publish_pipeline_metrics(job_name, records_processed, errors, duration_seconds):
    cloudwatch.put_metric_data(
        Namespace='DataPipeline/ETL',
        MetricData=[
            {
                'MetricName': 'RecordsProcessed',
                'Dimensions': [{'Name': 'JobName', 'Value': job_name}],
                'Timestamp': datetime.utcnow(),
                'Value': records_processed,
                'Unit': 'Count'
            },
            {
                'MetricName': 'ErrorCount',
                'Dimensions': [{'Name': 'JobName', 'Value': job_name}],
                'Timestamp': datetime.utcnow(),
                'Value': errors,
                'Unit': 'Count'
            },
            {
                'MetricName': 'JobDuration',
                'Dimensions': [{'Name': 'JobName', 'Value': job_name}],
                'Timestamp': datetime.utcnow(),
                'Value': duration_seconds,
                'Unit': 'Seconds'
            }
        ]
    )

# Call at end of ETL job
publish_pipeline_metrics('daily-orders-transform', records_processed=150000, errors=3, duration_seconds=245)
```

---

## Log Insights Query Language

CloudWatch Logs Insights uses a pipe-based query language for log analysis:

```sql
-- Find top errors in last hour
fields @timestamp, @message
| filter @message like /ERROR/
| stats count(*) as error_count by @message
| sort error_count desc
| limit 20

-- Pipeline latency percentiles
fields @timestamp, duration_ms
| filter job_name = 'daily-transform'
| stats avg(duration_ms) as avg_duration,
        pct(duration_ms, 95) as p95,
        pct(duration_ms, 99) as p99
  by bin(1h)

-- Find slow Glue job runs
fields @timestamp, @message
| filter @message like /Job run/
| parse @message "Job run * completed in * seconds" as job_id, duration
| filter duration > 300
| sort duration desc

-- Lambda cold starts detection
fields @timestamp, @initDuration, @duration
| filter ispresent(@initDuration)
| stats count(*) as cold_starts, avg(@initDuration) as avg_init_ms by bin(5m)
```

---

## Metric Filters — Extract Metrics from Logs

```python
# Create a metric filter that counts pipeline failures from logs
logs = boto3.client('logs')

logs.put_metric_filter(
    logGroupName='/aws/glue/jobs/output',
    filterName='ETLFailures',
    filterPattern='{ $.status = "FAILED" }',
    metricTransformations=[
        {
            'metricName': 'ETLJobFailures',
            'metricNamespace': 'DataPipeline/Monitoring',
            'metricValue': '1',
            'defaultValue': 0
        }
    ]
)

# Pattern matching examples:
# JSON logs: { $.level = "ERROR" }
# Text logs: [timestamp, request_id, level = ERROR, ...]
# Keyword: "OutOfMemoryError"
# Multiple conditions: { $.statusCode >= 500 && $.latency > 5000 }
```

---

## Composite Alarms

Combine multiple alarms into a single decision point:

```python
cloudwatch.put_composite_alarm(
    AlarmName='Pipeline-Critical-Composite',
    AlarmRule='ALARM("HighErrorRate") AND ALARM("LowThroughput")',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:oncall-pagerduty'],
    AlarmDescription='Fires only when BOTH error rate is high AND throughput drops'
)

# More complex rules
cloudwatch.put_composite_alarm(
    AlarmName='DataPlatform-Unhealthy',
    AlarmRule='ALARM("GlueJobsFailing") OR (ALARM("S3HighLatency") AND ALARM("AthenaSlowQueries"))',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:data-platform-alerts']
)
```

**Benefit:** Reduces alert fatigue. Single metric spikes don't page on-call; only compound failures trigger alerts.

---

## Anomaly Detection

```python
# Create anomaly detection band (learns normal patterns over 2 weeks)
cloudwatch.put_anomaly_detector(
    Namespace='DataPipeline/ETL',
    MetricName='RecordsProcessed',
    Dimensions=[{'Name': 'JobName', 'Value': 'daily-orders-transform'}],
    Stat='Average',
    Configuration={
        'ExcludedTimeRanges': [
            {'StartTime': '2024-12-24T00:00:00', 'EndTime': '2024-12-26T00:00:00'}  # Holiday
        ]
    }
)

# Alarm on anomaly (outside 2 standard deviations)
cloudwatch.put_metric_alarm(
    AlarmName='RecordsProcessed-Anomaly',
    MetricName='RecordsProcessed',
    Namespace='DataPipeline/ETL',
    Dimensions=[{'Name': 'JobName', 'Value': 'daily-orders-transform'}],
    ComparisonOperator='LessThanLowerOrGreaterThanUpperThreshold',
    ThresholdMetricId='ad1',
    EvaluationPeriods=3,
    Metrics=[
        {
            'Id': 'm1',
            'MetricStat': {
                'Metric': {'Namespace': 'DataPipeline/ETL', 'MetricName': 'RecordsProcessed', 'Dimensions': [{'Name': 'JobName', 'Value': 'daily-orders-transform'}]},
                'Period': 3600,
                'Stat': 'Average'
            }
        },
        {'Id': 'ad1', 'Expression': 'ANOMALY_DETECTION_BAND(m1, 2)'}
    ],
    AlarmActions=['arn:aws:sns:us-east-1:123456789:anomaly-alerts']
)
```

---

## Cross-Account Monitoring

```python
# Set up cross-account observability (Organization-level)
# Source accounts send metrics/logs to a monitoring account

# In monitoring account: create a sink
oam = boto3.client('oam')  # CloudWatch Observability Access Manager

oam.create_sink(
    Name='central-monitoring-sink',
    Tags={'Environment': 'production'}
)

# In source accounts: create links to the sink
oam.create_link(
    LabelTemplate='$AccountName',
    ResourceTypes=['AWS::CloudWatch::Metric', 'AWS::Logs::LogGroup', 'AWS::XRay::Trace'],
    SinkIdentifier='arn:aws:oam:us-east-1:000000000000:sink/sink-id'
)
```

---

## CloudWatch Synthetics — Canary Monitoring

```python
# Monitor data API availability with synthetic checks
synthetics = boto3.client('synthetics')

# Canary that verifies data freshness endpoint
canary_code = """
const synthetics = require('Synthetics');
const https = require('https');

exports.handler = async () => {
    const response = await synthetics.executeHttpStep(
        'Check Data Freshness API',
        {hostname: 'api.internal.com', path: '/data/freshness', method: 'GET'}
    );
    const body = JSON.parse(response.body);
    if (body.latestPartition < Date.now() - 86400000) {
        throw new Error('Data is stale: latest partition > 24h old');
    }
};
"""
```

---

## Key Metric Types for Data Platforms

| Service | Key Metrics | Alert Threshold |
|---------|-------------|-----------------|
| Glue Jobs | `glue.driver.aggregate.bytesRead`, `glue.driver.aggregate.recordsRead` | Job duration > 2x baseline |
| Kinesis | `IncomingRecords`, `IteratorAgeMilliseconds` | Iterator age > 60,000 ms |
| Redshift | `CPUUtilization`, `PercentageDiskSpaceUsed`, `QueryDuration` | Disk > 80%, CPU > 90% |
| Lambda | `Duration`, `Errors`, `ConcurrentExecutions` | Error rate > 5% |
| S3 | `BucketSizeBytes`, `NumberOfObjects`, `4xxErrors` | 4xx spike |

---

## Interview Tips

> **Tip 1:** "How do you monitor a data pipeline end-to-end?" — "Custom metrics at each stage: records extracted, transformed, loaded. Publish via CloudWatch PutMetricData from within Glue/Lambda. Set up anomaly detection on record counts (catches both drops and unexpected spikes). Composite alarms reduce noise — only page when multiple signals indicate a real problem. Log Insights for root-cause investigation."

> **Tip 2:** "How do you extract useful metrics from unstructured logs?" — "Metric filters. Define a pattern (JSON: `{ $.status = 'FAILED' }` or text: keyword match) and CloudWatch automatically creates a metric every time the pattern appears. Then alarm on that metric. Example: filter for 'OutOfMemoryError' in Glue logs → metric → alarm → PagerDuty."

> **Tip 3:** "What's the difference between anomaly detection and threshold alarms?" — "Threshold alarms are static (alert if > 100 errors). Anomaly detection learns the normal pattern (weekday vs weekend, hourly patterns) and alerts when behavior deviates from the expected band. Better for metrics with natural variation like record counts — a Monday with 10K records is normal, but a Wednesday with 10K might be a 50% drop."
