---
title: "AWS DynamoDB - Real-World Production Examples"
topic: aws-services
subtopic: dynamodb
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, dynamodb, production, pipeline-state, idempotency, cdc, feature-flags, monitoring]
---

# AWS DynamoDB — Real-World Production Examples

## Pattern 1: Pipeline State and Metadata Store

```python
# Architecture: Airflow/Step Functions → DynamoDB (state store) → Dashboard
# Use case: Track pipeline runs, task status, SLA compliance across 500+ pipelines

import boto3
from datetime import datetime, timedelta
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('pipeline-state')

class PipelineStateStore:
    """DynamoDB-backed pipeline state management."""
    
    def start_run(self, pipeline_id: str, run_id: str, config: dict):
        """Record pipeline run start."""
        table.put_item(Item={
            'PK': f'PIPELINE#{pipeline_id}',
            'SK': f'RUN#{run_id}',
            'status': 'RUNNING',
            'started_at': datetime.utcnow().isoformat(),
            'config': config,
            'tasks_total': config.get('task_count', 0),
            'tasks_completed': 0,
            'run_date': datetime.utcnow().strftime('%Y-%m-%d'),
            'ttl_expire': int((datetime.utcnow() + timedelta(days=90)).timestamp())
        })
        
        # Update pipeline latest status
        table.update_item(
            Key={'PK': f'PIPELINE#{pipeline_id}', 'SK': 'LATEST'},
            UpdateExpression='SET current_run = :rid, #s = :status, started_at = :ts',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':rid': run_id,
                ':status': 'RUNNING',
                ':ts': datetime.utcnow().isoformat()
            }
        )
    
    def complete_task(self, pipeline_id: str, run_id: str, task_name: str, metrics: dict):
        """Record individual task completion within a run."""
        # Atomic counter increment
        table.update_item(
            Key={'PK': f'PIPELINE#{pipeline_id}', 'SK': f'RUN#{run_id}'},
            UpdateExpression='SET tasks_completed = tasks_completed + :one, last_task = :task',
            ExpressionAttributeValues={':one': 1, ':task': task_name}
        )
        
        # Store task details
        table.put_item(Item={
            'PK': f'RUN#{pipeline_id}#{run_id}',
            'SK': f'TASK#{task_name}',
            'status': 'COMPLETED',
            'duration_sec': Decimal(str(metrics.get('duration_sec', 0))),
            'records_processed': metrics.get('records_processed', 0),
            'completed_at': datetime.utcnow().isoformat()
        })
    
    def complete_run(self, pipeline_id: str, run_id: str, status: str, metrics: dict):
        """Mark run as completed (SUCCESS or FAILED)."""
        table.update_item(
            Key={'PK': f'PIPELINE#{pipeline_id}', 'SK': f'RUN#{run_id}'},
            UpdateExpression='SET #s = :status, completed_at = :ts, metrics = :m, duration_sec = :d',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':status': status,
                ':ts': datetime.utcnow().isoformat(),
                ':m': metrics,
                ':d': Decimal(str(metrics.get('duration_sec', 0)))
            }
        )
        
        # Update pipeline latest
        table.update_item(
            Key={'PK': f'PIPELINE#{pipeline_id}', 'SK': 'LATEST'},
            UpdateExpression='SET #s = :status, last_completed = :ts',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':status': status, ':ts': datetime.utcnow().isoformat()}
        )
    
    def get_pipeline_history(self, pipeline_id: str, limit: int = 20):
        """Get recent runs for a pipeline (newest first)."""
        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :prefix)',
            ExpressionAttributeValues={
                ':pk': f'PIPELINE#{pipeline_id}',
                ':prefix': 'RUN#'
            },
            ScanIndexForward=False,
            Limit=limit
        )
        return response['Items']
    
    def get_all_failing_pipelines(self):
        """Query GSI to find all currently failing pipelines."""
        response = table.query(
            IndexName='status-index',
            KeyConditionExpression='#s = :status',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':status': 'FAILED'}
        )
        return response['Items']
```

---

## Pattern 2: Idempotency Token Table

```python
# Architecture: API/Lambda → Check DynamoDB → Process → Store token
# Use case: Prevent duplicate processing in at-least-once delivery systems

import hashlib
import json
import time
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('idempotency-tokens')

class IdempotencyGuard:
    """Prevent duplicate processing using DynamoDB conditional writes."""
    
    def __init__(self, ttl_hours: int = 24):
        self.ttl_hours = ttl_hours
    
    def execute_once(self, idempotency_key: str, processor_fn, event: dict):
        """Execute processor_fn exactly once for the given key."""
        
        # Try to claim the idempotency key
        try:
            table.put_item(
                Item={
                    'idempotency_key': idempotency_key,
                    'status': 'IN_PROGRESS',
                    'started_at': int(time.time()),
                    'ttl_expire': int(time.time()) + (self.ttl_hours * 3600)
                },
                ConditionExpression='attribute_not_exists(idempotency_key) OR #s = :failed',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':failed': 'FAILED'}
            )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Key exists and is not failed — already processed or in progress
            existing = table.get_item(Key={'idempotency_key': idempotency_key})['Item']
            if existing['status'] == 'COMPLETED':
                return {'status': 'already_processed', 'result': existing.get('result')}
            elif existing['status'] == 'IN_PROGRESS':
                # Check if stale (processor crashed)
                if time.time() - existing['started_at'] > 300:  # 5 min timeout
                    pass  # Allow retry (fall through)
                else:
                    return {'status': 'in_progress'}
        
        # Execute the actual processing
        try:
            result = processor_fn(event)
            
            # Mark as completed with result
            table.update_item(
                Key={'idempotency_key': idempotency_key},
                UpdateExpression='SET #s = :status, #r = :result, completed_at = :ts',
                ExpressionAttributeNames={'#s': 'status', '#r': 'result'},
                ExpressionAttributeValues={
                    ':status': 'COMPLETED',
                    ':result': json.dumps(result),
                    ':ts': int(time.time())
                }
            )
            return {'status': 'processed', 'result': result}
            
        except Exception as e:
            # Mark as failed (allows retry)
            table.update_item(
                Key={'idempotency_key': idempotency_key},
                UpdateExpression='SET #s = :status, error = :err',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':status': 'FAILED', ':err': str(e)}
            )
            raise
    
    @staticmethod
    def generate_key(event: dict) -> str:
        """Generate deterministic key from event content."""
        content = json.dumps(event, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


# Usage in Lambda handler:
guard = IdempotencyGuard(ttl_hours=24)

def handler(event, context):
    key = IdempotencyGuard.generate_key(event)
    return guard.execute_once(key, process_event, event)
```

---

## Pattern 3: DynamoDB Streams to Lambda to S3 (CDC to Data Lake)

```python
# Architecture: DynamoDB → Stream → Lambda → Kinesis Firehose → S3 (Parquet)
# Use case: Replicate DynamoDB state changes to data lake for analytics

import json
import boto3
from datetime import datetime

firehose = boto3.client('firehose')
DELIVERY_STREAM = 'dynamodb-cdc-to-s3'

def cdc_stream_handler(event, context):
    """Transform DynamoDB Stream records and send to Firehose for S3 delivery."""
    
    records_for_firehose = []
    
    for record in event['Records']:
        # Build CDC record (similar to Debezium format)
        cdc_record = {
            'event_id': record['eventID'],
            'event_type': record['eventName'],  # INSERT, MODIFY, REMOVE
            'event_time': datetime.utcfromtimestamp(
                record['dynamodb']['ApproximateCreationDateTime']
            ).isoformat() + 'Z',
            'table_name': record['eventSourceARN'].split('/')[1],
            'keys': deserialize_item(record['dynamodb']['Keys']),
        }
        
        # Include before/after images
        if 'NewImage' in record['dynamodb']:
            cdc_record['after'] = deserialize_item(record['dynamodb']['NewImage'])
        if 'OldImage' in record['dynamodb']:
            cdc_record['before'] = deserialize_item(record['dynamodb']['OldImage'])
        
        # Compute changed fields (for MODIFY events)
        if record['eventName'] == 'MODIFY':
            before = cdc_record.get('before', {})
            after = cdc_record.get('after', {})
            cdc_record['changed_fields'] = [
                k for k in after if after.get(k) != before.get(k)
            ]
        
        records_for_firehose.append({
            'Data': json.dumps(cdc_record).encode('utf-8') + b'\n'
        })
    
    # Send batch to Firehose (buffers and writes Parquet to S3)
    if records_for_firehose:
        # Firehose handles batching, compression, and Parquet conversion
        firehose.put_record_batch(
            DeliveryStreamName=DELIVERY_STREAM,
            Records=records_for_firehose
        )
    
    return {'processed': len(records_for_firehose)}


def deserialize_item(dynamodb_item):
    """Convert DynamoDB wire format to plain Python dict."""
    from boto3.dynamodb.types import TypeDeserializer
    deserializer = TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in dynamodb_item.items()}


# Firehose configuration for Parquet output:
# - Buffer: 128 MB or 5 minutes
# - Format conversion: JSON → Parquet (via Glue table schema)
# - S3 prefix: cdc/table=pipeline-state/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/
# - Compression: Snappy
```

---

## Pattern 4: Feature Flags for Data Pipelines

```python
# Architecture: DynamoDB table → Lambda/Airflow reads flags → conditional logic
# Use case: Toggle pipeline behavior without redeployment

import boto3
import json
from functools import lru_cache
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('feature-flags')

class FeatureFlags:
    """Feature flag system backed by DynamoDB."""
    
    def __init__(self, cache_ttl_sec: int = 60):
        self.cache_ttl_sec = cache_ttl_sec
        self._cache = {}
        self._cache_time = 0
    
    def get_flag(self, flag_name: str, default=False):
        """Get feature flag value with local caching."""
        self._refresh_cache_if_stale()
        flag = self._cache.get(flag_name)
        if flag is None:
            return default
        return flag.get('enabled', default)
    
    def get_flag_config(self, flag_name: str) -> dict:
        """Get flag with full configuration (rollout %, variant, etc.)."""
        self._refresh_cache_if_stale()
        return self._cache.get(flag_name, {})
    
    def _refresh_cache_if_stale(self):
        if time.time() - self._cache_time > self.cache_ttl_sec:
            response = table.scan()  # Small table, scan is fine
            self._cache = {item['flag_name']: item for item in response['Items']}
            self._cache_time = time.time()


# Feature flag items in DynamoDB:
flags = [
    {
        'flag_name': 'use_iceberg_merge',
        'enabled': True,
        'description': 'Use Iceberg MERGE instead of overwrite for incremental loads',
        'rollout_percentage': 100,
        'updated_by': 'data-team',
        'updated_at': '2024-01-15T10:00:00Z'
    },
    {
        'flag_name': 'enable_data_quality_v2',
        'enabled': True,
        'description': 'Use Great Expectations instead of custom SQL checks',
        'rollout_percentage': 50,  # 50% of pipelines
        'pipeline_allowlist': ['etl-orders', 'etl-customers'],
        'updated_by': 'platform-team'
    },
    {
        'flag_name': 'backfill_mode',
        'enabled': False,
        'description': 'Process historical data instead of incremental',
        'config': {'start_date': '2023-01-01', 'end_date': '2023-12-31'},
        'updated_by': 'on-call-engineer'
    }
]

# Usage in pipeline code:
flags = FeatureFlags(cache_ttl_sec=30)

def etl_pipeline(date: str):
    """ETL with feature flag controlled behavior."""
    data = extract(date)
    transformed = transform(data)
    
    if flags.get_flag('use_iceberg_merge'):
        # New path: Iceberg MERGE (upsert)
        iceberg_merge(transformed, target_table='curated.orders')
    else:
        # Old path: full partition overwrite
        overwrite_partition(transformed, date)
    
    if flags.get_flag('enable_data_quality_v2'):
        run_great_expectations(transformed)
    else:
        run_legacy_quality_checks(transformed)
```

---

## Monitoring and Operations

```python
# Key DynamoDB metrics for data platform tables:

monitoring = {
    'capacity': {
        'ConsumedReadCapacityUnits': 'Should be < 80% of provisioned',
        'ConsumedWriteCapacityUnits': 'Should be < 80% of provisioned',
        'ReadThrottleEvents': 'Must be 0 (data loss risk)',
        'WriteThrottleEvents': 'Must be 0 (pipeline failures)',
    },
    'latency': {
        'SuccessfulRequestLatency': 'P99 should be < 10ms',
        'SystemErrors': 'DynamoDB-side failures (rare, auto-retry)',
        'UserErrors': 'Client-side issues (validation, conditional check)',
    },
    'streams': {
        'IteratorAge': 'Stream processing lag (should be < 60s)',
        # High iterator age = Lambda not keeping up with changes
    }
}

# Alarm configuration:
alarms = [
    {'metric': 'ReadThrottleEvents', 'threshold': 1, 'action': 'Increase RCU or switch to on-demand'},
    {'metric': 'WriteThrottleEvents', 'threshold': 1, 'action': 'Increase WCU or switch to on-demand'},
    {'metric': 'SystemErrors', 'threshold': 5, 'action': 'Check AWS Health Dashboard'},
    {'metric': 'IteratorAge', 'threshold': 60000, 'action': 'Scale stream Lambda or check for errors'},
    {'metric': 'AccountProvisionedReadCapacityUtilization', 'threshold': 80, 'action': 'Approaching account limits'},
]
```

---

## Interview Tips

> **Tip 1:** "How would you use DynamoDB in a data pipeline?" — "Three primary use cases: (1) Pipeline state store — track run status, task progress, and SLA compliance with single-digit-millisecond reads for dashboards. (2) Idempotency tokens — prevent duplicate processing using conditional writes with TTL auto-cleanup. (3) CDC to data lake — DynamoDB Streams to Lambda to Firehose/S3, giving you a real-time copy of operational data in your analytics layer."

> **Tip 2:** "How do you implement feature flags for data pipelines?" — "Small DynamoDB table with flag name, enabled boolean, and optional config. Pipeline code reads flags with a local cache (60s TTL). This lets you toggle between processing strategies, enable/disable quality checks, or activate backfill mode without redeploying. Changes take effect within the cache TTL. For safety, flags support allowlists and gradual rollout percentages."

> **Tip 3:** "How do you prevent DynamoDB throttling in production?" — "Three defenses: (1) Design keys to distribute evenly (avoid hot partitions). (2) Use auto-scaling with a target utilization of 70% (buffer for spikes). (3) Monitor ConsumedCapacity in every request and set CloudWatch alarms on ThrottleEvents. For burst scenarios, on-demand mode handles 2x previous peak automatically. DAX absorbs repeated read patterns."
