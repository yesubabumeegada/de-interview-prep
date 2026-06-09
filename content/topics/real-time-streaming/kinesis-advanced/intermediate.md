---
title: "Kinesis Advanced — Intermediate"
topic: real-time-streaming
subtopic: kinesis-advanced
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [kinesis, kcl, lambda, firehose, enhanced-fan-out, resharding, aws]
---

# Kinesis Advanced — Intermediate

## Kinesis Client Library (KCL)

```java
/*
 KCL (Kinesis Client Library): production-grade consumer framework
 Handles: shard enumeration, checkpointing, resharding, load balancing across worker nodes
 
 KCL concepts:
   Worker:      one running instance of your consumer application
   Processor:   code you write to handle records from one shard
   Lease:       a worker "owns" a shard lease (DynamoDB-backed)
   Checkpoint:  last processed sequence number (stored in DynamoDB)
 
 Multi-worker: KCL distributes shards across workers automatically
   4 shards, 2 workers → each worker gets 2 shards
   Worker fails → other worker picks up its leases within 30 seconds
*/

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class OrderProcessor implements ShardRecordProcessor {
    
    private String shardId;
    
    @Override
    public void initialize(InitializationInput input) {
        // Called when this processor takes ownership of a shard
        this.shardId = input.shardId();
        System.out.println("Initialized processor for shard: " + shardId);
    }
    
    @Override
    public void processRecords(ProcessRecordsInput input) {
        // Called with each batch of records from the shard
        List<KinesisClientRecord> records = input.records();
        
        for (KinesisClientRecord record : records) {
            try {
                // Parse and process
                String data = StandardCharsets.UTF_8.decode(record.data()).toString();
                Order order = objectMapper.readValue(data, Order.class);
                processOrder(order);
            } catch (Exception e) {
                log.error("Failed to process record: " + record.sequenceNumber(), e);
                // Don't throw — let KCL continue processing remaining records
                // Route to DLQ separately
            }
        }
        
        // Checkpoint after processing batch
        // Checkpoint tells KCL: "I've processed up to this sequence number"
        // On restart: KCL starts from last checkpoint (at-least-once)
        try {
            input.checkpointer().checkpoint();
        } catch (ThrottlingException | ShutdownException e) {
            log.warn("Checkpoint failed: " + e.getMessage());
            // Non-fatal: KCL will retry checkpointing
        }
    }
    
    @Override
    public void leaseLost(LeaseLostInput input) {
        // Called when another worker takes this shard (normal during scaling)
        // Cleanup any shard-local state
        log.info("Lease lost for shard: " + shardId);
    }
    
    @Override
    public void shardEnded(ShardEndedInput input) {
        // Shard split or merge — must checkpoint at shard end
        try {
            input.checkpointer().checkpoint();
        } catch (Exception e) { /* handle */ }
    }
    
    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        // Graceful shutdown: checkpoint current position
        try {
            input.checkpointer().checkpoint();
        } catch (Exception e) { /* handle */ }
    }
}

// KCL worker setup:
Scheduler scheduler = new Scheduler(
    new ConfigsBuilder(
        streamName,
        applicationName,        // DynamoDB table name for leases
        kinesisClient,
        dynamoDbClient,
        cloudWatchClient,
        workerId,               // unique ID per process
        new OrderProcessorFactory()
    )
    .retrievalConfig()
        .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
    .build()
);
scheduler.run();  // blocking
```

---

## Kinesis + Lambda: Serverless Consumer

```python
# Lambda trigger for Kinesis (event source mapping)
# Lambda invoked per batch of records per shard
# Scaling: 1 Lambda invocation per shard (concurrent = shard count)

import json
import base64
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """
    Process Kinesis records from event source mapping.
    Each invocation handles records from ONE shard.
    
    Event structure:
    {
      "Records": [
        {
          "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "device-123",
            "sequenceNumber": "49590338271490256608559692540925702759324208523137515522",
            "data": "base64-encoded-data",
            "approximateArrivalTimestamp": 1545084650.987
          },
          "eventSource": "aws:kinesis",
          "eventID": "shardId-000000000006:49590338...",
          "invokeIdentityArn": "arn:aws:iam::...",
          ...
        }
      ]
    }
    """
    
    processed = 0
    errors = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis data (base64 encoded)
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(raw_data)
            
            partition_key = record['kinesis']['partitionKey']
            sequence_num  = record['kinesis']['sequenceNumber']
            arrival_ts    = record['kinesis']['approximateArrivalTimestamp']
            
            # Process the event
            process_event(data, partition_key)
            processed += 1
            
        except Exception as e:
            print(f"Error processing record {record['kinesis']['sequenceNumber']}: {e}")
            errors.append({
                'sequenceNumber': record['kinesis']['sequenceNumber'],
                'error': str(e)
            })
    
    print(f"Processed: {processed}, Errors: {len(errors)}")
    
    # If any record fails, Lambda will retry the ENTIRE batch (at-least-once)
    # To skip failed records without blocking: configure bisect-on-error
    # (Lambda retries only the failing half of the batch recursively)
    
    if errors:
        # Return partial success: only report unrecoverable errors
        # This prevents blocking the shard on transient failures
        raise Exception(f"{len(errors)} records failed: {errors}")

# Lambda event source mapping configuration (CloudFormation/CDK):
# BatchSize: up to 10,000 records per invocation
# StartingPosition: TRIM_HORIZON or LATEST
# BisectBatchOnFunctionError: true (split batch on error → find bad record)
# MaximumRetryAttempts: 3 (then send to DLQ)
# DestinationConfig.OnFailure: SQS DLQ ARN (failed records sent here)
# ParallelizationFactor: 1-10 (concurrent Lambda invocations per shard)
```

---

## Enhanced Fan-Out

```python
# Enhanced Fan-Out: dedicated 2 MB/sec per consumer per shard
# vs. shared GetRecords: 2 MB/sec per shard total

import boto3
import time

kinesis = boto3.client('kinesis', region_name='us-east-1')

# Register as Enhanced Fan-Out consumer
def register_efo_consumer(stream_arn: str, consumer_name: str) -> str:
    """Register a consumer for Enhanced Fan-Out."""
    response = kinesis.register_stream_consumer(
        StreamARN=stream_arn,
        ConsumerName=consumer_name
    )
    consumer_arn = response['Consumer']['ConsumerARN']
    
    # Wait for consumer to become ACTIVE
    while True:
        resp = kinesis.describe_stream_consumer(ConsumerARN=consumer_arn)
        status = resp['ConsumerDescription']['ConsumerStatus']
        if status == 'ACTIVE':
            break
        print(f"Consumer status: {status}, waiting...")
        time.sleep(2)
    
    return consumer_arn

# Subscribe to shard (EFO - push-based, HTTP/2)
def subscribe_to_shard(consumer_arn: str, shard_id: str):
    """
    EFO: records pushed to subscriber as soon as available (no polling).
    Each subscribed consumer gets its own 2 MB/sec per shard.
    """
    response = kinesis.subscribe_to_shard(
        ConsumerARN=consumer_arn,
        ShardId=shard_id,
        StartingPosition={'Type': 'LATEST'}
    )
    
    event_stream = response['EventStream']
    
    for event in event_stream:
        if 'SubscribeToShardEvent' in event:
            records = event['SubscribeToShardEvent']['Records']
            for record in records:
                data = json.loads(record['Data'])
                print(f"EFO received: {data}")
            
            # Checkpoint periodically
            continuation_seq = event['SubscribeToShardEvent']['ContinuationSequenceNumber']
        
        elif 'SubscribeToShardEventStreamInitializationEvent' in event:
            print("EFO stream initialized")

# When to use EFO:
# Multiple consumer applications (each needs full throughput)
# Standard consumers: A (analytics) + B (alerts) share 2 MB/sec → each gets 1 MB/sec
# EFO consumers: A gets 2 MB/sec, B gets 2 MB/sec (independent)
# Cost: $0.015/shard-hour extra + $0.013/GB
```

---

## Kinesis Firehose Advanced Patterns

```python
# Firehose: producer → Firehose → S3/Redshift/ES (no consumer code)
# Advanced: Lambda transformation + S3 dynamic partitioning

import boto3
import json
import base64

# Firehose Lambda transformation function
def firehose_transform(event, context):
    """
    Transform records in Firehose before delivery to S3.
    Each record: {"recordId": "...", "data": "base64-encoded"}
    Return: {"recordId": "...", "result": "Ok|Dropped|ProcessingFailed", "data": "base64"}
    """
    output_records = []
    
    for record in event['records']:
        try:
            # Decode
            raw = base64.b64decode(record['data']).decode('utf-8')
            data = json.loads(raw)
            
            # Transform: add processing timestamp, normalize fields
            data['processed_at'] = datetime.utcnow().isoformat()
            data['source'] = 'firehose'
            
            # Filter: drop health check events
            if data.get('event_type') == 'health_check':
                output_records.append({
                    'recordId': record['recordId'],
                    'result': 'Dropped'  # discard this record
                })
                continue
            
            # Re-encode
            transformed = json.dumps(data) + '\n'  # newline for S3 line-delimited JSON
            encoded = base64.b64encode(transformed.encode('utf-8')).decode('utf-8')
            
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': encoded
            })
        
        except Exception as e:
            print(f"Transform failed for record {record['recordId']}: {e}")
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed'
                # ProcessingFailed records go to S3 error prefix
            })
    
    return {'records': output_records}

# Dynamic Partitioning in Firehose (S3 prefix based on record content):
# Prefix: "data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/event=!{partitionKeyFromQuery:event_type}/"
# Requires: enable dynamic partitioning + JQ query to extract partition key from record
# Result: S3 files organized as s3://bucket/data/year=2024/month=01/event=purchase/
# Benefit: Athena/Glue can use partition pruning → cheaper queries
```

---

## Interview Tips

> **Tip 1:** "How does KCL handle shard splitting during active consumption?" — When a shard is split, the parent shard is sealed (no new records). KCL consumers must finish reading all records from the parent shard before reading from the child shards. KCL handles this automatically: it detects the split, lets the current processor finish the parent shard, calls `shardEnded()` (which must checkpoint), then creates new processors for the child shards. If you're running KCL manually, ensure your processor calls `checkpointer.checkpoint()` in `shardEnded()` — otherwise KCL won't advance to the children.

> **Tip 2:** "What is the difference between `TRIM_HORIZON`, `LATEST`, and `AT_TIMESTAMP` starting positions?" — `TRIM_HORIZON`: read from the oldest available record in the shard (within retention period). Use for catching up from the beginning. `LATEST`: read only new records from now (skip existing). Use for new consumers that don't need historical data. `AT_TIMESTAMP`: start from records at a specific timestamp. Use for disaster recovery (replay from a specific time). `AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER`: resume from a specific sequence number (used internally by KCL for checkpoint-based resume).

> **Tip 3:** "How do you size Kinesis Data Streams for a given workload?" — Calculate required shards: `ceil(max(ingress_MB_per_sec / 1, records_per_sec / 1000))`. Example: 500 MB/min ingest = 8.3 MB/sec → 9 shards (round up). For consumers: if using GetRecords (shared 2 MB/sec/shard): check if consumers can process within the 2 MB/sec limit. If multiple consumers need full throughput → use Enhanced Fan-Out. Cost: ~$0.015/shard-hour + $0.014/million PUT records. Monitor: GetRecords.IteratorAgeMilliseconds for consumer lag and WriteProvisionedThroughputExceeded for producer throttling.
