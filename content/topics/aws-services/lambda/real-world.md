---
title: "AWS Lambda - Real-World Production Examples"
topic: aws-services
subtopic: lambda
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, lambda, production, s3-events, kinesis, api-gateway, data-quality, monitoring]
---

# AWS Lambda — Real-World Production Examples

## Pattern 1: S3 Event to Validate and Trigger Glue

```python
# Architecture: S3 upload → Lambda (validate) → Glue job (process)
# Use case: Partners upload CSV files, Lambda validates schema/quality
#           before triggering expensive Glue ETL

import json
import boto3
import csv
import io

s3 = boto3.client('s3')
glue = boto3.client('glue')
sns = boto3.client('sns')

EXPECTED_COLUMNS = ['order_id', 'customer_id', 'amount', 'date', 'product']
ALERT_TOPIC = 'arn:aws:sns:us-east-1:123456:data-alerts'

def handler(event, context):
    """Validate uploaded file and trigger Glue if valid."""
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Only process files in the incoming/ prefix
    if not key.startswith('incoming/'):
        return {'status': 'skipped', 'reason': 'not in incoming/ prefix'}
    
    # Download and validate
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    
    validation_result = validate_file(content, key)
    
    if validation_result['valid']:
        # Move to validated/ prefix and trigger Glue
        validated_key = key.replace('incoming/', 'validated/')
        s3.copy_object(Bucket=bucket, CopySource=f'{bucket}/{key}', Key=validated_key)
        s3.delete_object(Bucket=bucket, Key=key)
        
        # Trigger Glue job with file path
        glue.start_job_run(
            JobName='partner-data-etl',
            Arguments={
                '--input_path': f's3://{bucket}/{validated_key}',
                '--source': key.split('/')[1],  # partner name from path
                '--date': context.function_name
            }
        )
        return {'status': 'valid', 'glue_triggered': True}
    else:
        # Move to quarantine/ and alert
        quarantine_key = key.replace('incoming/', 'quarantine/')
        s3.copy_object(Bucket=bucket, CopySource=f'{bucket}/{key}', Key=quarantine_key)
        s3.delete_object(Bucket=bucket, Key=key)
        
        sns.publish(
            TopicArn=ALERT_TOPIC,
            Subject=f'Data Validation Failed: {key}',
            Message=json.dumps(validation_result, indent=2)
        )
        return {'status': 'invalid', 'errors': validation_result['errors']}


def validate_file(content, key):
    """Run validation checks on uploaded CSV."""
    errors = []
    reader = csv.DictReader(io.StringIO(content))
    
    # Check columns
    if reader.fieldnames != EXPECTED_COLUMNS:
        errors.append(f"Column mismatch. Expected: {EXPECTED_COLUMNS}, Got: {reader.fieldnames}")
        return {'valid': False, 'errors': errors}
    
    rows = list(reader)
    
    # Check row count
    if len(rows) == 0:
        errors.append("File is empty")
    
    # Check for nulls in required fields
    null_orders = sum(1 for r in rows if not r.get('order_id'))
    if null_orders > 0:
        errors.append(f"{null_orders} rows with null order_id")
    
    # Check amount is numeric and positive
    bad_amounts = sum(1 for r in rows if not is_positive_number(r.get('amount', '')))
    if bad_amounts > len(rows) * 0.01:  # >1% bad amounts
        errors.append(f"{bad_amounts} rows with invalid amounts ({bad_amounts/len(rows):.1%})")
    
    return {'valid': len(errors) == 0, 'errors': errors, 'row_count': len(rows)}


def is_positive_number(val):
    try:
        return float(val) > 0
    except (ValueError, TypeError):
        return False
```

---

## Pattern 2: Kinesis Stream Processor with DLQ

```python
# Architecture: Kinesis Data Stream → Lambda → S3 (micro-batch) + DynamoDB (state)
# Use case: Real-time clickstream processing, aggregate and land to S3 every minute

import json
import base64
import boto3
from datetime import datetime
from collections import defaultdict

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stream-processing-state')

OUTPUT_BUCKET = 'data-lake-streaming'

def handler(event, context):
    """Process Kinesis batch: aggregate events and write to S3."""
    
    aggregations = defaultdict(lambda: {'count': 0, 'total_amount': 0.0})
    failures = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(payload)
            
            # Aggregate by event type and minute
            event_minute = data['timestamp'][:16]  # 2024-01-15T10:30
            key = f"{data['event_type']}|{event_minute}"
            aggregations[key]['count'] += 1
            aggregations[key]['total_amount'] += data.get('amount', 0)
            
        except (json.JSONDecodeError, KeyError) as e:
            # Report individual record failure (partial batch failure)
            failures.append({
                'itemIdentifier': record['kinesis']['sequenceNumber']
            })
    
    # Write aggregations to S3 (partitioned by hour)
    if aggregations:
        now = datetime.utcnow()
        output_key = (
            f"streaming/clickstream/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/{context.aws_request_id}.json"
        )
        
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=output_key,
            Body=json.dumps(dict(aggregations)),
            ContentType='application/json'
        )
        
        # Update processing state (for monitoring)
        table.put_item(Item={
            'shard_id': event['Records'][0]['kinesis']['partitionKey'],
            'last_sequence': event['Records'][-1]['kinesis']['sequenceNumber'],
            'records_processed': len(event['Records']) - len(failures),
            'updated_at': datetime.utcnow().isoformat()
        })
    
    # Return partial failures for retry
    return {'batchItemFailures': failures}
```

**Event source mapping configuration:**
```json
{
    "EventSourceArn": "arn:aws:kinesis:us-east-1:123:stream/clickstream",
    "FunctionName": "kinesis-processor",
    "BatchSize": 500,
    "MaximumBatchingWindowInSeconds": 60,
    "ParallelizationFactor": 5,
    "BisectBatchOnFunctionError": true,
    "MaximumRetryAttempts": 3,
    "FunctionResponseTypes": ["ReportBatchItemFailures"],
    "DestinationConfig": {
        "OnFailure": {"Destination": "arn:aws:sqs:us-east-1:123:kinesis-dlq"}
    }
}
```

---

## Pattern 3: API Gateway Data Ingestion Endpoint

```python
# Architecture: API Gateway → Lambda → Kinesis/S3
# Use case: REST endpoint for partners to POST data events in real-time

import json
import boto3
import hashlib
import time

kinesis = boto3.client('kinesis')
dynamodb = boto3.resource('dynamodb')
idempotency_table = dynamodb.Table('idempotency-tokens')

STREAM_NAME = 'ingestion-stream'

def handler(event, context):
    """API Gateway endpoint for data ingestion with idempotency."""
    
    # Parse request
    try:
        body = json.loads(event['body'])
        api_key = event['requestContext']['identity']['apiKey']
    except (json.JSONDecodeError, KeyError):
        return response(400, {'error': 'Invalid request body'})
    
    # Validate required fields
    required = ['event_type', 'timestamp', 'payload']
    missing = [f for f in required if f not in body]
    if missing:
        return response(400, {'error': f'Missing fields: {missing}'})
    
    # Idempotency check (prevent duplicate processing)
    idempotency_key = body.get('idempotency_key') or generate_key(body)
    if is_duplicate(idempotency_key):
        return response(200, {'status': 'already_processed', 'idempotency_key': idempotency_key})
    
    # Enrich and forward to Kinesis
    enriched = {
        **body,
        'ingested_at': int(time.time() * 1000),
        'source_api_key': hashlib.sha256(api_key.encode()).hexdigest()[:12],
        'idempotency_key': idempotency_key
    }
    
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(enriched),
        PartitionKey=body.get('entity_id', idempotency_key)
    )
    
    # Record idempotency token (TTL: 24 hours)
    idempotency_table.put_item(Item={
        'token': idempotency_key,
        'created_at': int(time.time()),
        'ttl': int(time.time()) + 86400
    })
    
    return response(202, {'status': 'accepted', 'idempotency_key': idempotency_key})


def is_duplicate(key):
    resp = idempotency_table.get_item(Key={'token': key})
    return 'Item' in resp


def generate_key(body):
    content = json.dumps(body, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body)
    }
```

---

## Pattern 4: Scheduled Data Quality Monitor

```python
# Architecture: EventBridge (cron) → Lambda → Athena queries → SNS alerts
# Use case: Daily checks that data landed correctly and meets quality thresholds

import boto3
import time
import json
from datetime import datetime, timedelta

athena = boto3.client('athena')
sns = boto3.client('sns')

WORKGROUP = 'data-quality'
OUTPUT_LOCATION = 's3://athena-results/data-quality/'
ALERT_TOPIC = 'arn:aws:sns:us-east-1:123456:data-quality-alerts'

CHECKS = [
    {
        'name': 'orders_row_count',
        'query': """
            SELECT COUNT(*) as cnt FROM curated.fact_orders 
            WHERE order_date = DATE '{check_date}'
        """,
        'threshold': lambda val: int(val) > 1000,
        'message': 'Order count below minimum threshold (1000)'
    },
    {
        'name': 'orders_null_check',
        'query': """
            SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as nulls
            FROM curated.fact_orders WHERE order_date = DATE '{check_date}'
        """,
        'threshold': lambda val: int(val) == 0,
        'message': 'NULL customer_ids found in orders'
    },
    {
        'name': 'revenue_anomaly',
        'query': """
            WITH today AS (SELECT SUM(amount) as rev FROM curated.fact_orders WHERE order_date = DATE '{check_date}'),
            baseline AS (SELECT AVG(daily_rev) as avg_rev, STDDEV(daily_rev) as std_rev 
                FROM (SELECT order_date, SUM(amount) as daily_rev FROM curated.fact_orders 
                      WHERE order_date BETWEEN DATE '{check_date}' - INTERVAL '30' DAY AND DATE '{check_date}' - INTERVAL '1' DAY
                      GROUP BY order_date))
            SELECT CASE WHEN t.rev < b.avg_rev - 2*b.std_rev THEN 'ANOMALY' ELSE 'OK' END
            FROM today t, baseline b
        """,
        'threshold': lambda val: val == 'OK',
        'message': 'Revenue anomaly detected (>2 std deviations below mean)'
    }
]

def handler(event, context):
    """Run all data quality checks and alert on failures."""
    check_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    results = []
    failures = []
    
    for check in CHECKS:
        query = check['query'].format(check_date=check_date)
        result_value = run_athena_query(query)
        
        passed = check['threshold'](result_value)
        results.append({
            'check': check['name'],
            'value': result_value,
            'passed': passed
        })
        
        if not passed:
            failures.append(f"{check['name']}: {check['message']} (value={result_value})")
    
    # Alert on failures
    if failures:
        sns.publish(
            TopicArn=ALERT_TOPIC,
            Subject=f'Data Quality FAILED ({len(failures)} checks) - {check_date}',
            Message=json.dumps({
                'date': check_date,
                'failures': failures,
                'all_results': results
            }, indent=2)
        )
    
    return {'date': check_date, 'total_checks': len(CHECKS), 'failures': len(failures), 'results': results}


def run_athena_query(query):
    """Execute Athena query and return single scalar result."""
    execution = athena.start_query_execution(
        QueryString=query,
        WorkGroup=WORKGROUP,
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    execution_id = execution['QueryExecutionId']
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(2)
    
    if state != 'SUCCEEDED':
        raise Exception(f"Query failed: {status['QueryExecution']['Status'].get('StateChangeReason')}")
    
    # Get result
    results = athena.get_query_results(QueryExecutionId=execution_id)
    return results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
```

---

## Production Monitoring Checklist

| Metric | CloudWatch Alarm | Threshold |
|--------|-----------------|-----------|
| Errors | Function errors > 0 | Any error in 5 min |
| Throttles | Throttles > 0 | Any throttle (capacity issue) |
| Duration | P99 > 80% of timeout | Approaching timeout |
| Concurrent executions | > 80% of reserved | Capacity pressure |
| Iterator age (Kinesis) | > 5 minutes | Falling behind |
| DLQ messages | ApproximateNumberOfMessages > 0 | Failed events accumulating |
| Cost | Daily estimated cost | > 120% of baseline |

```python
# CloudWatch dashboard JSON for Lambda monitoring:
# Key metrics to track per function:
# - Invocations (count, to spot traffic changes)
# - Errors (absolute and error rate percentage)
# - Duration (p50, p95, p99)
# - ConcurrentExecutions (capacity usage)
# - Throttles (hitting concurrency limit)
# - IteratorAge (Kinesis/DynamoDB stream lag)
```

---

## Interview Tips

> **Tip 1:** "Walk me through a production Lambda data pipeline" — "S3 upload triggers Lambda for schema validation. Valid files are moved to a validated prefix and a Glue job is triggered. Invalid files go to quarantine with SNS alerting. The Glue job transforms and loads to the curated layer. A separate scheduled Lambda runs Athena-based data quality checks. DLQ captures any Lambda failures for investigation."

> **Tip 2:** "How do you prevent duplicate processing in an event-driven pipeline?" — "Idempotency tokens stored in DynamoDB with TTL. Each incoming event gets a deterministic key (hash of content). Before processing, check if the key exists. If yes, return success without reprocessing. TTL auto-cleans old tokens after 24 hours. This handles API Gateway retries, duplicate S3 events, and at-least-once delivery from Kinesis."

> **Tip 3:** "How do you monitor Lambda-based data pipelines in production?" — "Three layers: (1) Function-level: CloudWatch alarms on errors, throttles, duration p99, and iterator age for streams. (2) Pipeline-level: Scheduled data quality Lambda checks row counts and anomalies against baselines. (3) Cost-level: Budget alerts and per-function cost tracking. DLQ depth alarm catches failed events before they age out."
