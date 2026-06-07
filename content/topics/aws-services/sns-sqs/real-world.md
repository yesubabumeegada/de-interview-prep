---
title: "AWS SNS/SQS - Real-World Production Examples"
topic: aws-services
subtopic: sns-sqs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, sns, sqs, production, event-driven]
---

# AWS SNS/SQS — Real-World Production Examples

## Pattern 1: Pipeline Alerting System (SNS Fan-Out)

```python
import boto3
import json

sns = boto3.client('sns')

# Central alerting topic with multiple subscribers
topic_arn = 'arn:aws:sns:us-east-1:123456789:pipeline-alerts'

# Subscriber 1: Email for non-critical (filtered)
sns.subscribe(
    TopicArn=topic_arn,
    Protocol='email',
    Endpoint='data-team@company.com',
    Attributes={
        'FilterPolicy': json.dumps({
            'severity': ['medium', 'high', 'critical']
        })
    }
)

# Subscriber 2: PagerDuty for critical only
sns.subscribe(
    TopicArn=topic_arn,
    Protocol='https',
    Endpoint='https://events.pagerduty.com/integration/KEY/enqueue',
    Attributes={
        'FilterPolicy': json.dumps({
            'severity': ['critical'],
            'environment': ['production']
        })
    }
)

# Subscriber 3: Slack via Lambda (all alerts)
sns.subscribe(
    TopicArn=topic_arn,
    Protocol='lambda',
    Endpoint='arn:aws:lambda:us-east-1:123456789:function:slack-notifier'
)

# Subscriber 4: SQS for audit trail (all messages, no filter)
sns.subscribe(
    TopicArn=topic_arn,
    Protocol='sqs',
    Endpoint='arn:aws:sqs:us-east-1:123456789:alert-audit-queue'
)

# Publishing alerts from pipeline code
def alert_pipeline_failure(job_name, error_message, environment='production'):
    sns.publish(
        TopicArn=topic_arn,
        Subject=f'Pipeline Failure: {job_name}',
        Message=json.dumps({
            'job_name': job_name,
            'error': error_message,
            'environment': environment,
            'timestamp': datetime.utcnow().isoformat(),
            'runbook': f'https://wiki.internal/runbooks/{job_name}'
        }),
        MessageAttributes={
            'severity': {'DataType': 'String', 'StringValue': 'critical'},
            'environment': {'DataType': 'String', 'StringValue': environment},
            'domain': {'DataType': 'String', 'StringValue': 'data-platform'}
        }
    )

# Usage
alert_pipeline_failure('daily-orders-etl', 'OutOfMemoryError in transform stage')
```

---

## Pattern 2: Decoupled ETL with SQS Buffer

```python
# Architecture: Extract → SQS Buffer → Load
# SQS decouples the extract and load stages
# Benefits: extract can run faster than load, load retries independently

# Producer: Extract stage dumps work items to SQS
def extract_and_enqueue(source_tables, queue_url):
    """Extract data and enqueue load tasks"""
    sqs = boto3.client('sqs')
    
    for table in source_tables:
        # Extract to S3
        output_path = extract_table_to_s3(table)
        
        # Enqueue load task
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                'source_table': table,
                'extracted_path': output_path,
                'record_count': get_record_count(output_path),
                'extracted_at': datetime.utcnow().isoformat()
            }),
            MessageAttributes={
                'priority': {
                    'DataType': 'Number',
                    'StringValue': str(get_table_priority(table))
                }
            }
        )

# Consumer: Load stage processes at its own pace
def load_worker(queue_url):
    """Continuously process load tasks from queue"""
    sqs = boto3.client('sqs')
    
    while True:
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            VisibilityTimeout=600  # 10 min for loading
        )
        
        for msg in messages.get('Messages', []):
            task = json.loads(msg['Body'])
            try:
                # Load into target (Redshift, Iceberg, etc.)
                load_to_target(
                    source_path=task['extracted_path'],
                    target_table=task['source_table']
                )
                # Success: delete message
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
            except Exception as e:
                # Failure: message returns to queue after visibility timeout
                # After 3 failures → DLQ
                print(f"Load failed for {task['source_table']}: {e}")

# Benefits:
# - Extract runs at full speed (doesn't wait for slow loads)
# - Load failures don't block other tables
# - Natural backpressure (queue depth indicates congestion)
# - Easy to scale: add more load workers when queue depth grows
```

---

## Pattern 3: S3 Event → SNS → Multiple SQS → Parallel Processors

```python
# Real-world: new files land in S3 → trigger multiple independent workflows

# S3 notification → SNS topic (configured in bucket settings)
s3_notification_config = {
    'TopicConfigurations': [{
        'TopicArn': 'arn:aws:sns:us-east-1:123456789:new-data-notifications',
        'Events': ['s3:ObjectCreated:*'],
        'Filter': {'Key': {'FilterRules': [{'Name': 'prefix', 'Value': 'incoming/'}]}}
    }]
}

# Consumer 1: Data quality validation (fast, Lambda-based)
# Queue → Lambda → validates schema, checks for nulls
quality_lambda = """
def handler(event, context):
    for record in event['Records']:
        s3_event = json.loads(json.loads(record['body'])['Message'])['Records'][0]
        bucket = s3_event['s3']['bucket']['name']
        key = s3_event['s3']['object']['key']
        
        # Run quality checks
        result = validate_file(bucket, key)
        if not result['passed']:
            alert_quality_failure(key, result['violations'])
"""

# Consumer 2: ETL processing (slow, Glue-based)
# Queue → Lambda trigger → starts Glue job
etl_trigger = """
def handler(event, context):
    glue = boto3.client('glue')
    for record in event['Records']:
        s3_event = json.loads(json.loads(record['body'])['Message'])['Records'][0]
        key = s3_event['s3']['object']['key']
        
        glue.start_job_run(
            JobName='incoming-data-transform',
            Arguments={'--source_key': key}
        )
"""

# Consumer 3: Metadata cataloging
# Queue → Lambda → updates DynamoDB file registry
catalog_lambda = """
def handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('file-registry')
    
    for record in event['Records']:
        s3_event = json.loads(json.loads(record['body'])['Message'])['Records'][0]
        table.put_item(Item={
            'file_key': s3_event['s3']['object']['key'],
            'size_bytes': s3_event['s3']['object']['size'],
            'arrived_at': datetime.utcnow().isoformat(),
            'status': 'received'
        })
"""
```

---

## Pattern 4: DLQ Monitoring and Reprocessing Workflow

```python
import boto3
import json
from datetime import datetime

sqs = boto3.client('sqs')

# Automated DLQ monitoring and reprocessing
class DLQManager:
    def __init__(self, dlq_url, main_queue_url):
        self.dlq_url = dlq_url
        self.main_queue_url = main_queue_url
        self.sqs = boto3.client('sqs')
        self.sns = boto3.client('sns')
    
    def get_dlq_depth(self):
        """Get number of messages in DLQ"""
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.dlq_url,
            AttributeNames=['ApproximateNumberOfMessagesVisible']
        )
        return int(attrs['Attributes']['ApproximateNumberOfMessagesVisible'])
    
    def analyze_failures(self, sample_size=10):
        """Sample DLQ messages to understand failure patterns"""
        messages = self.sqs.receive_message(
            QueueUrl=self.dlq_url,
            MaxNumberOfMessages=min(sample_size, 10),
            VisibilityTimeout=30,
            AttributeNames=['All']
        )
        
        analysis = {'total_sampled': 0, 'patterns': {}}
        for msg in messages.get('Messages', []):
            analysis['total_sampled'] += 1
            body = json.loads(msg['Body'])
            
            # Categorize failure
            receive_count = int(msg['Attributes'].get('ApproximateReceiveCount', 0))
            first_received = msg['Attributes'].get('ApproximateFirstReceiveTimestamp')
            
            analysis['patterns'].setdefault(body.get('error_type', 'unknown'), []).append({
                'receive_count': receive_count,
                'age_hours': (datetime.utcnow().timestamp() * 1000 - int(first_received)) / 3600000
            })
            
            # Return message to DLQ (don't delete during analysis)
            self.sqs.change_message_visibility(
                QueueUrl=self.dlq_url,
                ReceiptHandle=msg['ReceiptHandle'],
                VisibilityTimeout=0
            )
        
        return analysis
    
    def redrive_all(self):
        """Move all DLQ messages back to main queue"""
        dlq_arn = self.sqs.get_queue_attributes(
            QueueUrl=self.dlq_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']
        
        main_arn = self.sqs.get_queue_attributes(
            QueueUrl=self.main_queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']
        
        response = self.sqs.start_message_move_task(
            SourceArn=dlq_arn,
            DestinationArn=main_arn,
            MaxNumberOfMessagesPerSecond=50  # Rate limit to avoid overwhelming consumer
        )
        return response['TaskHandle']
    
    def alert_if_growing(self, threshold=10):
        """Alert if DLQ depth exceeds threshold"""
        depth = self.get_dlq_depth()
        if depth >= threshold:
            self.sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789:oncall-alerts',
                Subject=f'DLQ Alert: {depth} messages accumulated',
                Message=json.dumps({
                    'dlq_url': self.dlq_url,
                    'depth': depth,
                    'action_required': 'Investigate failures and redrive',
                    'runbook': 'https://wiki.internal/runbooks/dlq-procedures'
                }),
                MessageAttributes={
                    'severity': {'DataType': 'String', 'StringValue': 'high'}
                }
            )

# Scheduled Lambda runs every 5 minutes
def lambda_handler(event, context):
    manager = DLQManager(
        dlq_url='https://sqs.us-east-1.amazonaws.com/123456789/etl-events-dlq',
        main_queue_url='https://sqs.us-east-1.amazonaws.com/123456789/etl-events'
    )
    manager.alert_if_growing(threshold=10)
```

---

## Interview Tips

> **Tip 1:** "Design an alerting system for a data platform" — "SNS topic with filter-based routing: critical alerts → PagerDuty (HTTPS subscription), all alerts → Slack (Lambda subscriber), medium+ → email, everything → SQS for audit trail. MessageAttributes enable filtering without parsing the body. Each subscriber receives only what matters to them. SNS handles fan-out and delivery retries independently per subscriber."

> **Tip 2:** "How do you decouple pipeline stages with SQS?" — "SQS as a buffer between extract and load. Extract writes work items (S3 paths, table names) to the queue. Load workers consume at their own pace. Benefits: (1) Extract isn't blocked by slow loads. (2) Failed loads retry independently via visibility timeout + DLQ. (3) Scale load workers based on queue depth. (4) Natural backpressure. This pattern handles load spikes gracefully — queue absorbs burst and consumers drain it steadily."

> **Tip 3:** "Walk me through DLQ operations" — "Three-phase approach: (1) Monitor — CloudWatch alarm on DLQ depth > 0. (2) Investigate — sample messages, categorize failure patterns (permission error? schema mismatch? timeout?). (3) Remediate — fix the root cause, then StartMessageMoveTask to redrive from DLQ to main queue at a controlled rate (50 msg/s to avoid overwhelming consumers). Never delete DLQ messages without understanding why they failed. Track DLQ reprocessing success rate."
