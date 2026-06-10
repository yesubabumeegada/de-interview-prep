---
title: "AWS CloudWatch - Scenario Questions"
topic: aws-services
subtopic: cloudwatch
content_type: scenario_question
tags: [aws, cloudwatch, interview, scenarios, monitoring]
---

# Scenario Questions — AWS CloudWatch

<article data-difficulty="junior">

## 🟢 Junior: Alert on Pipeline Failure

**Scenario:** Your Glue ETL job runs nightly. Set up alerting so the team gets an email when the job fails.

<details>
<summary>✅ Solution</summary>

```python
import boto3

cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

# Step 1: Create SNS topic for alerts
topic = sns.create_topic(Name='pipeline-alerts')
sns.subscribe(TopicArn=topic['TopicArn'], Protocol='email', Endpoint='team@company.com')

# Step 2: Create CloudWatch alarm on Glue job metric
cloudwatch.put_metric_alarm(
    AlarmName='glue-job-daily-etl-failure',
    MetricName='glue.driver.aggregate.numFailedTasks',
    Namespace='Glue',
    Dimensions=[{'Name': 'JobName', 'Value': 'daily-orders-etl'}],
    Statistic='Sum',
    Period=300,  # 5 minutes
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=[topic['TopicArn']],
    AlarmDescription='Alert when daily ETL job has failed tasks'
)
```

**Alternative (EventBridge — simpler):**
```json
{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {"jobName": ["daily-orders-etl"], "state": ["FAILED"]}
}
```
Route this EventBridge rule to SNS → email. This is actually the recommended modern approach.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design Pipeline Monitoring Dashboard

**Scenario:** You have 10 daily Glue jobs, 3 Kinesis streams, and a Redshift cluster. Design a CloudWatch dashboard that shows pipeline health at a glance.

<details>
<summary>✅ Solution</summary>

**Dashboard layout:**

| Section | Metrics | Alert Threshold |
|---------|---------|-----------------|
| **ETL Jobs** | Success/fail count, duration trend, DPU usage | Failure count > 0 |
| **Streaming** | IncomingRecords/sec, IteratorAge (consumer lag), WriteProvisionedThroughputExceeded | IteratorAge > 5 min |
| **Warehouse** | CPU utilization, query queue length, disk usage | CPU > 80%, queue > 10 |
| **Data Quality** | Custom metric: rows processed, null rate, duplicate rate | Null rate > 5% |

**Custom metric for data quality (published from ETL job):**
```python
# Inside Glue job: publish quality metrics to CloudWatch
cloudwatch.put_metric_data(
    Namespace='DataPipeline/Quality',
    MetricData=[
        {'MetricName': 'RowsProcessed', 'Value': row_count, 'Unit': 'Count',
         'Dimensions': [{'Name': 'Table', 'Value': 'fact_orders'}]},
        {'MetricName': 'NullRate', 'Value': null_pct, 'Unit': 'Percent',
         'Dimensions': [{'Name': 'Table', 'Value': 'fact_orders'}]},
    ]
)
```

**Composite alarm (fires if ANY sub-alarm triggers):**
```python
cloudwatch.put_composite_alarm(
    AlarmName='pipeline-health-critical',
    AlarmRule='ALARM(glue-failure) OR ALARM(kinesis-lag) OR ALARM(redshift-cpu)',
    AlarmActions=['arn:aws:sns:...:pagerduty-topic']
)
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Amazon CloudWatch and what are its main components?**
A: CloudWatch is AWS's monitoring and observability service. Its main components are: Metrics (time-series data from AWS services and custom sources), Logs (centralized log storage and querying), Alarms (threshold-based or anomaly-based alerts), Dashboards (visualization), and CloudWatch Events/EventBridge (event-driven automation).

**Q: What is the difference between CloudWatch Metrics and CloudWatch Logs?**
A: Metrics are numeric, time-series data points (CPU utilization, bytes written) aggregated at set intervals — ideal for dashboards and alarms. Logs are raw text records (application output, access logs) stored for search, filtering, and analysis via Logs Insights.

**Q: How do you create custom metrics in CloudWatch?**
A: Use the `PutMetricData` API or the AWS CLI to publish custom numeric data points with a namespace, metric name, dimensions, and value. This is used to track business metrics like records processed, ETL job duration, or pipeline error counts.

**Q: What is CloudWatch Logs Insights?**
A: Logs Insights is an interactive query engine for CloudWatch Logs that supports a SQL-like query language. It lets you filter, aggregate, and visualize log data across multiple log groups — useful for debugging ETL failures or analyzing Glue/Athena job logs.

**Q: What is the difference between a CloudWatch Alarm and a Composite Alarm?**
A: A standard alarm monitors a single metric against a threshold. A Composite Alarm combines multiple alarms using boolean logic (AND/OR) to reduce alert noise — for example, alerting only when both error rate is high AND throughput is low simultaneously.

**Q: How do you use CloudWatch for data pipeline monitoring?**
A: Publish custom metrics for key pipeline KPIs (records ingested, processing latency, error count), create alarms on anomalous values, use Logs Insights for root-cause analysis on failures, and build dashboards for real-time pipeline health visibility.

**Q: What are CloudWatch Metric Filters?**
A: Metric Filters extract numeric values from log events and turn them into CloudWatch Metrics. For example, you can count occurrences of "ERROR" in application logs and create an alarm when the count exceeds a threshold — bridging logs and metric-based alerting.

**Q: What is the CloudWatch retention policy for logs and metrics?**
A: Metrics are retained for 15 months (with decreasing granularity over time). Log retention is configurable per log group — from 1 day to indefinitely. By default, log groups never expire, so setting an explicit retention policy is a cost-control best practice.

---

## 💼 Interview Tips

- Show you think beyond basic alarms: discuss anomaly detection alarms (`ANOMALY_DETECTION_BAND`) for metrics without fixed thresholds, like daily record counts that vary by day of week — this signals senior-level operational thinking.
- Interviewers expect you to discuss cost management: high-resolution metrics (1-second granularity) cost more; custom metrics cost per metric per month. Mention aggregating related metrics into fewer dimensions to control costs.
- Avoid the mistake of logging everything at DEBUG level in production — describe how you'd use structured logging with log levels, then use Metric Filters to extract actionable signals from structured logs.
- Senior interviewers want to hear about the full observability stack: CloudWatch Metrics and Alarms for alerting, Logs Insights for investigation, and dashboards for stakeholder-facing SLA reporting.
- Mention the EventBridge (formerly CloudWatch Events) integration: pipeline alerts can auto-trigger Lambda remediation actions, SNS notifications, or Step Functions workflows.
- Know that CloudWatch Agent is needed for OS-level metrics (memory, disk) since EC2 does not publish these by default — a common gap interviewers probe.
