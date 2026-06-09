---
title: "Kinesis Advanced — Real World"
topic: real-time-streaming
subtopic: kinesis-advanced
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [kinesis, aws, production, firehose, lambda, kcl, iot, real-time]
---

# Kinesis Advanced — Real World

## Pattern 1: IoT Fleet Telemetry Pipeline

```python
"""
Pattern: 50,000 IoT devices → Kinesis → real-time anomaly detection + S3 archival

Architecture:
  Devices → Kinesis Data Streams (50 shards, 50 MB/sec capacity)
             ├── Lambda (EFO): real-time anomaly detection → SNS alerts
             └── Firehose:  buffered delivery → S3 (Parquet, partitioned by date/hour)
"""

import boto3
import json
import base64
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')
firehose = boto3.client('firehose', region_name='us-east-1')
sns = boto3.client('sns', region_name='us-east-1')

# Producer: IoT device telemetry batch sender
class IoTProducer:
    """Batch producer with retry and partition key spreading."""
    
    def __init__(self, stream_name: str, batch_size: int = 500):
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.buffer = []
    
    def add_reading(self, device_id: str, reading: dict):
        """Buffer a reading for batch send."""
        self.buffer.append({
            'Data': json.dumps({
                'device_id': device_id,
                'timestamp': datetime.utcnow().isoformat(),
                **reading
            }).encode('utf-8'),
            'PartitionKey': device_id   # ensures device readings → same shard (ordered)
        })
        
        if len(self.buffer) >= self.batch_size:
            self.flush()
    
    def flush(self):
        """Send buffered records to Kinesis."""
        if not self.buffer:
            return
        
        response = kinesis.put_records(
            StreamName=self.stream_name,
            Records=self.buffer
        )
        
        failed = response.get('FailedRecordCount', 0)
        if failed > 0:
            # Retry failed records
            retry_batch = [
                self.buffer[i]
                for i, r in enumerate(response['Records'])
                if 'ErrorCode' in r
            ]
            self._retry(retry_batch)
        
        self.buffer = []
    
    def _retry(self, records, attempt=0, max_attempts=3):
        if attempt >= max_attempts or not records:
            return
        time.sleep(2 ** attempt)
        response = kinesis.put_records(StreamName=self.stream_name, Records=records)
        still_failed = [records[i] for i, r in enumerate(response['Records'])
                       if 'ErrorCode' in r]
        self._retry(still_failed, attempt + 1, max_attempts)

# Lambda consumer: anomaly detection
def anomaly_detector(event, context):
    """Real-time anomaly detection on device readings."""
    
    THRESHOLDS = {
        'temperature': {'min': -10, 'max': 85},
        'pressure':    {'min': 900, 'max': 1100},
        'vibration':   {'max': 5.0}
    }
    
    alerts = []
    for record in event['Records']:
        data = json.loads(base64.b64decode(record['kinesis']['data']))
        device_id = data['device_id']
        
        for metric, thresholds in THRESHOLDS.items():
            value = data.get(metric)
            if value is None:
                continue
            
            if 'min' in thresholds and value < thresholds['min']:
                alerts.append({
                    'device_id': device_id,
                    'metric': metric,
                    'value': value,
                    'threshold': f"< {thresholds['min']}"
                })
            elif 'max' in thresholds and value > thresholds['max']:
                alerts.append({
                    'device_id': device_id,
                    'metric': metric,
                    'value': value,
                    'threshold': f"> {thresholds['max']}"
                })
    
    if alerts:
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789:iot-alerts',
            Message=json.dumps(alerts),
            Subject=f"IoT Anomaly: {len(alerts)} alerts"
        )
    
    return {'statusCode': 200, 'alertsEmitted': len(alerts)}
```

---

## Pattern 2: Click Stream to Redshift via Firehose

```python
"""
Pattern: Web clickstream → Kinesis Firehose → S3 (Parquet) → Glue Catalog → Athena/Redshift

Benefits:
  - No consumer code needed
  - Automatic S3 partitioning by date/hour
  - Parquet format for cost-efficient querying
  - Glue Catalog integration for schema discovery
"""

import boto3
import json
import random

firehose_client = boto3.client('firehose', region_name='us-east-1')

# Firehose delivery stream configuration (via CDK/CloudFormation):
"""
DeliveryStreamName: clickstream-to-s3
DeliveryStreamType: DirectPut
ExtendedS3DestinationConfiguration:
  BucketARN: arn:aws:s3:::my-data-lake
  Prefix: clickstream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/
  ErrorOutputPrefix: errors/clickstream/!{firehose:error-output-type}/year=!{timestamp:yyyy}/
  BufferingHints:
    SizeInMBs: 128        # buffer until 128 MB filled
    IntervalInSeconds: 300  # or 5 minutes, whichever first
  CompressionFormat: UNCOMPRESSED  # Parquet handles compression internally
  DataFormatConversionConfiguration:
    Enabled: true
    InputFormatConfiguration:
      Deserializer:
        OpenXJsonSerDe: {}   # input is JSON
    OutputFormatConfiguration:
      Serializer:
        ParquetSerDe:
          Compression: SNAPPY
    SchemaConfiguration:
      DatabaseName: clickstream_db
      TableName: click_events
      RoleARN: arn:aws:iam::123456789:role/firehose-glue-access
  ProcessingConfiguration:
    Enabled: true
    Processors:
      - Type: Lambda
        Parameters:
          - ParameterName: LambdaArn
            ParameterValue: arn:aws:lambda:us-east-1:123456789:function:enrich-click-events
"""

# Producer: send click events to Firehose
def send_click_event(event: dict):
    """Send click event to Firehose (direct put)."""
    firehose_client.put_record(
        DeliveryStreamName='clickstream-to-s3',
        Record={
            'Data': json.dumps(event).encode('utf-8')
        }
    )

def send_click_events_batch(events: list):
    """Batch send (up to 500 records, 4 MB total)."""
    records = [
        {'Data': json.dumps(event).encode('utf-8')}
        for event in events
    ]
    
    response = firehose_client.put_record_batch(
        DeliveryStreamName='clickstream-to-s3',
        Records=records
    )
    
    failed = response.get('FailedPutCount', 0)
    if failed > 0:
        print(f"Firehose: {failed} records failed delivery")

# Lambda enrichment function (called by Firehose before S3):
def enrich_click_events(event, context):
    """Add derived fields to click events in Firehose processing."""
    output = []
    
    for record in event['records']:
        data = json.loads(base64.b64decode(record['data']))
        
        # Enrich: derive device type from user agent
        ua = data.get('user_agent', '')
        if 'Mobile' in ua:
            data['device_type'] = 'mobile'
        elif 'Tablet' in ua:
            data['device_type'] = 'tablet'
        else:
            data['device_type'] = 'desktop'
        
        # Enrich: parse URL path
        url = data.get('page_url', '')
        data['url_path'] = url.split('?')[0] if '?' in url else url
        
        encoded = base64.b64encode(
            (json.dumps(data) + '\n').encode('utf-8')
        ).decode('utf-8')
        
        output.append({
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': encoded
        })
    
    return {'records': output}
```

---

## Pattern 3: Kinesis Auto-Scaling

```python
"""
Auto-scale Kinesis shards based on iterator age (consumer lag).
Runs as a scheduled Lambda (every 5 minutes).
"""

import boto3
from datetime import datetime, timedelta

kinesis = boto3.client('kinesis', region_name='us-east-1')
cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')

def autoscale_kinesis(stream_name: str, 
                      min_shards: int = 4, 
                      max_shards: int = 100,
                      lag_threshold_seconds: int = 60):
    """
    Auto-scale Kinesis shards based on consumer lag.
    Scale up if lag > threshold; scale down if all shards underutilized.
    """
    
    # Get current shard count
    response = kinesis.describe_stream_summary(StreamName=stream_name)
    current_shards = response['StreamDescriptionSummary']['OpenShardCount']
    
    # Get iterator age (consumer lag) per shard
    stats = cloudwatch.get_metric_statistics(
        Namespace='AWS/Kinesis',
        MetricName='GetRecords.IteratorAgeMilliseconds',
        Dimensions=[{'Name': 'StreamName', 'Value': stream_name}],
        StartTime=datetime.utcnow() - timedelta(minutes=5),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=['Maximum']
    )
    
    max_age_ms = max((p['Maximum'] for p in stats.get('Datapoints', [])), default=0)
    max_age_sec = max_age_ms / 1000
    
    print(f"Stream: {stream_name}, Shards: {current_shards}, Max lag: {max_age_sec:.1f}s")
    
    # Scale UP: consumer is falling behind
    if max_age_sec > lag_threshold_seconds and current_shards < max_shards:
        target_shards = min(current_shards * 2, max_shards)
        print(f"Scaling UP: {current_shards} → {target_shards} shards (lag: {max_age_sec:.1f}s)")
        kinesis.update_shard_count(
            StreamName=stream_name,
            TargetShardCount=target_shards,
            ScalingType='UNIFORM_SCALING'
        )
        return
    
    # Scale DOWN: if all shards consistently underloaded (write < 50% capacity)
    write_stats = cloudwatch.get_metric_statistics(
        Namespace='AWS/Kinesis',
        MetricName='IncomingBytes',
        Dimensions=[{'Name': 'StreamName', 'Value': stream_name}],
        StartTime=datetime.utcnow() - timedelta(hours=1),
        EndTime=datetime.utcnow(),
        Period=3600,
        Statistics=['Sum']
    )
    
    total_bytes = sum(p['Sum'] for p in write_stats.get('Datapoints', []))
    mb_per_sec = total_bytes / (3600 * 1024 * 1024)
    shard_capacity_mb_sec = current_shards * 1.0  # 1 MB/sec per shard
    utilization = mb_per_sec / shard_capacity_mb_sec
    
    if utilization < 0.4 and current_shards > min_shards:  # < 40% utilized
        target_shards = max(current_shards // 2, min_shards)
        print(f"Scaling DOWN: {current_shards} → {target_shards} shards ({utilization:.0%} utilized)")
        kinesis.update_shard_count(
            StreamName=stream_name,
            TargetShardCount=target_shards,
            ScalingType='UNIFORM_SCALING'
        )

def lambda_handler(event, context):
    """Scheduled Lambda: auto-scale all Kinesis streams."""
    streams = ['transactions', 'clickstream', 'iot-telemetry']
    for stream in streams:
        try:
            autoscale_kinesis(stream)
        except Exception as e:
            print(f"Error scaling {stream}: {e}")
```

---

## Interview Tips

> **Tip 1:** "How do you handle PII in a Kinesis pipeline that archives to S3?" — Use Firehose with a Lambda transformation function to scrub PII before writing to S3. In the Lambda: parse each record, replace PII fields (email, SSN, phone) with hashed values or null, re-encode and return. The transformation runs inline before delivery to S3. For audit purposes: send unredacted records to a separate, access-controlled S3 bucket with strict IAM policies, S3 Object Lock (WORM), and VPC endpoint access only. In KDS itself: records are encrypted at rest (SSE-KMS), and in transit (HTTPS). Enhanced Fan-Out also uses TLS.

> **Tip 2:** "What is the throughput math for Kinesis, and how does it affect Lambda scaling?" — KDS: 1 MB/sec or 1,000 records/sec per shard (write). For Lambda as consumer: 1 Lambda invocation per shard (without parallelization factor). If each Lambda invocation takes 200ms and receives 10,000 records, throughput per Lambda = 50K records/sec per shard. With parallelization factor 10: 10 Lambda invocations per shard concurrently → 500K records/sec. Total concurrent Lambdas = shard_count × parallelization_factor. Ensure Lambda concurrency quota covers peak load. Latency: Lambda cold start (100-500ms) adds to processing latency — use provisioned concurrency for latency-sensitive consumers.

> **Tip 3:** "When would you choose Kinesis Firehose over writing directly to S3?" — Firehose when: (a) you want buffering/batching automatically (avoid millions of small S3 files); (b) you need format conversion (JSON → Parquet with Glue schema); (c) you want dynamic partitioning based on record content; (d) you need inline transformation via Lambda; (e) you want automatic retry and error handling to S3 error prefix. Write directly to S3 when: (a) you need < 60 second latency (Firehose minimum buffer is 60 seconds); (b) you need custom partitioning logic that exceeds Firehose's JQ expression capabilities; (c) you're already using KCL/Lambda with complex processing and adding S3 write is trivial.
