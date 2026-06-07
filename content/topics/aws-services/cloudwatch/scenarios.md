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
