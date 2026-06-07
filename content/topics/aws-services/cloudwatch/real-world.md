---
title: "AWS CloudWatch - Real-World Production Examples"
topic: aws-services
subtopic: cloudwatch
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, cloudwatch, production, monitoring, alerting]
---

# AWS CloudWatch — Real-World Production Examples

## Pattern 1: Data Pipeline Health Dashboard

```python
import boto3
import json

cloudwatch = boto3.client('cloudwatch')

# Comprehensive dashboard: Glue + Kinesis + Redshift + custom metrics
dashboard_body = {
    "widgets": [
        {
            "type": "metric",
            "properties": {
                "title": "Glue Job Status (Last 24h)",
                "metrics": [
                    ["AWS/Glue", "glue.driver.aggregate.numCompletedStages", "JobName", "daily-etl", {"stat": "Sum"}],
                    ["AWS/Glue", "glue.driver.aggregate.numFailedTasks", "JobName", "daily-etl", {"stat": "Sum", "color": "#d62728"}]
                ],
                "period": 3600,
                "view": "timeSeries"
            }
        },
        {
            "type": "metric",
            "properties": {
                "title": "Kinesis Stream Health",
                "metrics": [
                    ["AWS/Kinesis", "IncomingRecords", "StreamName", "clickstream", {"stat": "Sum"}],
                    ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", "clickstream", {"stat": "Maximum", "yAxis": "right"}]
                ],
                "period": 60,
                "annotations": {"horizontal": [{"value": 60000, "label": "Lag SLA", "yAxis": "right"}]}
            }
        },
        {
            "type": "metric",
            "properties": {
                "title": "Redshift Cluster Performance",
                "metrics": [
                    ["AWS/Redshift", "CPUUtilization", "ClusterIdentifier", "analytics-cluster"],
                    ["AWS/Redshift", "PercentageDiskSpaceUsed", "ClusterIdentifier", "analytics-cluster"],
                    ["AWS/Redshift", "DatabaseConnections", "ClusterIdentifier", "analytics-cluster", {"yAxis": "right"}]
                ],
                "period": 300
            }
        },
        {
            "type": "metric",
            "properties": {
                "title": "Pipeline Throughput (Custom Metrics)",
                "metrics": [
                    ["DataPipeline/ETL", "RecordsProcessed", "JobName", "orders-pipeline"],
                    ["DataPipeline/ETL", "RecordsProcessed", "JobName", "customers-pipeline"],
                    ["DataPipeline/ETL", "RecordsProcessed", "JobName", "events-pipeline"]
                ],
                "period": 3600,
                "stat": "Sum",
                "view": "bar"
            }
        }
    ]
}

cloudwatch.put_dashboard(DashboardName='DataPlatform-Production', DashboardBody=json.dumps(dashboard_body))
```

---

## Pattern 2: Custom ETL Metrics with EMF

```python
import json
import sys
import time
from datetime import datetime

class PipelineMetricsEmitter:
    """Emit ETL metrics using Embedded Metric Format"""
    
    def __init__(self, job_name, environment='production'):
        self.job_name = job_name
        self.environment = environment
        self.start_time = time.time()
        self.records_read = 0
        self.records_written = 0
        self.errors = 0
        self.stage_durations = {}
    
    def record_stage(self, stage_name, records, duration_ms, errors=0):
        """Record metrics for a pipeline stage"""
        self.records_written += records
        self.errors += errors
        self.stage_durations[stage_name] = duration_ms
        
        emf = {
            "_aws": {
                "Timestamp": int(datetime.utcnow().timestamp() * 1000),
                "CloudWatchMetrics": [{
                    "Namespace": "DataPipeline/ETL",
                    "Dimensions": [["JobName", "Stage", "Environment"]],
                    "Metrics": [
                        {"Name": "RecordsProcessed", "Unit": "Count"},
                        {"Name": "StageDuration", "Unit": "Milliseconds"},
                        {"Name": "StageErrors", "Unit": "Count"}
                    ]
                }]
            },
            "JobName": self.job_name,
            "Stage": stage_name,
            "Environment": self.environment,
            "RecordsProcessed": records,
            "StageDuration": duration_ms,
            "StageErrors": errors,
            "message": f"[{self.job_name}] {stage_name}: {records} records in {duration_ms}ms"
        }
        print(json.dumps(emf))
        sys.stdout.flush()
    
    def emit_summary(self):
        """Emit job-level summary metrics"""
        total_duration = int((time.time() - self.start_time) * 1000)
        
        emf = {
            "_aws": {
                "Timestamp": int(datetime.utcnow().timestamp() * 1000),
                "CloudWatchMetrics": [{
                    "Namespace": "DataPipeline/ETL",
                    "Dimensions": [["JobName", "Environment"]],
                    "Metrics": [
                        {"Name": "TotalRecords", "Unit": "Count"},
                        {"Name": "TotalDuration", "Unit": "Milliseconds"},
                        {"Name": "TotalErrors", "Unit": "Count"},
                        {"Name": "RecordsPerSecond", "Unit": "Count/Second"}
                    ]
                }]
            },
            "JobName": self.job_name,
            "Environment": self.environment,
            "TotalRecords": self.records_written,
            "TotalDuration": total_duration,
            "TotalErrors": self.errors,
            "RecordsPerSecond": self.records_written / max(total_duration / 1000, 1)
        }
        print(json.dumps(emf))
        sys.stdout.flush()

# Usage in Glue job
metrics = PipelineMetricsEmitter('daily-orders-pipeline')
metrics.record_stage('extract', records=200000, duration_ms=15000)
metrics.record_stage('transform', records=195000, duration_ms=45000, errors=5)
metrics.record_stage('load', records=195000, duration_ms=8000)
metrics.emit_summary()
```

---

## Pattern 3: Log-Based Alerting for Pipeline Failures

```python
import boto3

logs = boto3.client('logs')
cloudwatch = boto3.client('cloudwatch')

# Metric filter: detect Glue job failures
logs.put_metric_filter(
    logGroupName='/aws/glue/jobs/error',
    filterName='GlueJobFailures',
    filterPattern='{ $.jobRunState = "FAILED" }',
    metricTransformations=[{
        'metricName': 'GlueJobFailureCount',
        'metricNamespace': 'DataPipeline/Alerts',
        'metricValue': '1',
        'defaultValue': 0
    }]
)

# Metric filter: detect data quality violations
logs.put_metric_filter(
    logGroupName='/aws/lambda/data-quality-checker',
    filterName='QualityViolations',
    filterPattern='{ $.check_status = "FAILED" && $.severity = "critical" }',
    metricTransformations=[{
        'metricName': 'CriticalQualityViolations',
        'metricNamespace': 'DataPipeline/Alerts',
        'metricValue': '1',
        'defaultValue': 0
    }]
)

# Alarm: page on-call for any Glue failure
cloudwatch.put_metric_alarm(
    AlarmName='GlueJob-Failure-Alert',
    Namespace='DataPipeline/Alerts',
    MetricName='GlueJobFailureCount',
    Statistic='Sum',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:oncall-pagerduty'],
    TreatMissingData='notBreaching'
)

# Alarm: alert on data freshness (no records processed in 2 hours)
cloudwatch.put_metric_alarm(
    AlarmName='Pipeline-Stale-Data',
    Namespace='DataPipeline/ETL',
    MetricName='RecordsProcessed',
    Dimensions=[{'Name': 'JobName', 'Value': 'hourly-ingest'}],
    Statistic='Sum',
    Period=3600,
    EvaluationPeriods=2,
    Threshold=1,
    ComparisonOperator='LessThanThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:data-platform-alerts'],
    TreatMissingData='breaching'  # Missing data = pipeline not running
)
```

---

## Pattern 4: Cost Monitoring Automation

```python
import boto3
from datetime import datetime, timedelta

ce = boto3.client('ce')
cloudwatch = boto3.client('cloudwatch')

def publish_daily_cost_metrics():
    """Publish daily AWS costs as custom CloudWatch metrics"""
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    # Get cost by service
    response = ce.get_cost_and_usage(
        TimePeriod={'Start': yesterday, 'End': today},
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )
    
    metric_data = []
    for group in response['ResultsByTime'][0]['Groups']:
        service = group['Keys'][0]
        cost = float(group['Metrics']['UnblendedCost']['Amount'])
        
        # Filter to data platform services
        if service in ['Amazon Athena', 'AWS Glue', 'Amazon Kinesis', 'Amazon Redshift', 'Amazon S3']:
            metric_data.append({
                'MetricName': 'DailyCost',
                'Dimensions': [{'Name': 'Service', 'Value': service}],
                'Timestamp': datetime.utcnow(),
                'Value': cost,
                'Unit': 'None'
            })
    
    cloudwatch.put_metric_data(Namespace='DataPlatform/Costs', MetricData=metric_data)

# Alarm: cost spike detection
cloudwatch.put_metric_alarm(
    AlarmName='Athena-Cost-Spike',
    Namespace='DataPlatform/Costs',
    MetricName='DailyCost',
    Dimensions=[{'Name': 'Service', 'Value': 'Amazon Athena'}],
    Statistic='Average',
    Period=86400,
    EvaluationPeriods=1,
    Threshold=50,  # Alert if Athena costs exceed $50/day
    ComparisonOperator='GreaterThanThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:cost-alerts']
)
```

---

## SLA Tracking Setup

```python
# Track data freshness SLA: data available within 2 hours of source update

def check_sla_compliance(table_name, sla_hours=2):
    """Publish SLA compliance metric"""
    athena = boto3.client('athena')
    
    query = f"""
    SELECT MAX(ingestion_timestamp) as latest_record,
           CURRENT_TIMESTAMP - MAX(ingestion_timestamp) as lag
    FROM curated.{table_name}
    WHERE partition_date = CURRENT_DATE
    """
    # Execute and parse result...
    lag_hours = 1.5  # parsed from query result
    
    cloudwatch.put_metric_data(
        Namespace='DataPlatform/SLA',
        MetricData=[
            {
                'MetricName': 'DataLagHours',
                'Dimensions': [{'Name': 'Table', 'Value': table_name}],
                'Value': lag_hours,
                'Unit': 'None'
            },
            {
                'MetricName': 'SLACompliant',
                'Dimensions': [{'Name': 'Table', 'Value': table_name}],
                'Value': 1 if lag_hours <= sla_hours else 0,
                'Unit': 'None'
            }
        ]
    )

# Alarm on SLA breach
cloudwatch.put_metric_alarm(
    AlarmName='SLA-Breach-Orders',
    Namespace='DataPlatform/SLA',
    MetricName='SLACompliant',
    Dimensions=[{'Name': 'Table', 'Value': 'fact_orders'}],
    Statistic='Minimum',
    Period=3600,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='LessThanThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:sla-breach-alerts']
)
```

---

## Interview Tips

> **Tip 1:** "How would you build observability for a data platform?" — "Four layers: (1) AWS-native metrics (Glue, Kinesis, Redshift built-in). (2) Custom business metrics via EMF (records processed, data freshness, error rates). (3) Log-based alerts via metric filters (detect specific failure patterns). (4) Composite alarms and anomaly detection to reduce noise. Unified dashboard showing pipeline health, throughput, latency, and cost — all in one view."

> **Tip 2:** "How do you track SLAs for data delivery?" — "Publish a 'DataLagHours' metric per table: current_time minus max(ingestion_timestamp). Alarm when lag exceeds SLA threshold. Also track a binary 'SLACompliant' metric for dashboard visualization. Historical SLA compliance percentage calculated from CloudWatch metric math. This creates accountability and visibility into which pipelines are meeting their contracts."

> **Tip 3:** "How do you handle alert fatigue in a data platform?" — "Three techniques: (1) Composite alarms — don't page for a single metric spike; require multiple signals (high errors AND low throughput). (2) Anomaly detection instead of static thresholds — adapts to natural variation. (3) Severity tiers: P1 (pages on-call) for SLA breaches and complete failures, P2 (Slack notification) for degradation, P3 (dashboard only) for informational. Review and tune alarm thresholds monthly."
