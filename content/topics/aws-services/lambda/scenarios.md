---
title: "AWS Lambda - Scenario Questions"
topic: aws-services
subtopic: lambda
content_type: scenario_question
tags: [aws, lambda, interview, scenarios, serverless, event-driven]
---

# Scenario Questions — AWS Lambda

<article data-difficulty="junior">

## 🟢 Junior: Design an Event-Driven File Processor

**Scenario:** Partners upload CSV files to an S3 bucket (`s3://partner-uploads/`). When a file arrives, you need to: validate it has the correct columns, convert it to Parquet, and move it to the data lake (`s3://data-lake/raw/partner/`). If validation fails, move to quarantine. Design using Lambda.

<details>
<summary>✅ Solution</summary>

```python
import boto3
import pandas as pd
import io
import json

s3 = boto3.client('s3')
EXPECTED_COLUMNS = ['order_id', 'customer_id', 'amount', 'order_date']
TARGET_BUCKET = 'data-lake'
QUARANTINE_BUCKET = 'quarantine'

def handler(event, context):
    # Get file info from S3 event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"Processing: s3://{source_bucket}/{key}")
    
    try:
        # Download file
        obj = s3.get_object(Bucket=source_bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        # Validate columns
        missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")
        
        # Validate not empty
        if len(df) == 0:
            raise ValueError("File is empty")
        
        # Convert to Parquet and upload to data lake
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_key = key.replace('.csv', '.parquet')
        
        s3.put_object(
            Bucket=TARGET_BUCKET,
            Key=f"raw/partner/{parquet_key}",
            Body=parquet_buffer.getvalue()
        )
        
        print(f"Success: {len(df)} rows → s3://{TARGET_BUCKET}/raw/partner/{parquet_key}")
        return {'status': 'success', 'rows': len(df)}
        
    except Exception as e:
        # Move to quarantine on any failure
        s3.copy_object(
            Bucket=QUARANTINE_BUCKET,
            Key=f"partner/{key}",
            CopySource={'Bucket': source_bucket, 'Key': key}
        )
        
        # Send alert
        sns = boto3.client('sns')
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123:data-alerts',
            Subject=f'Partner file validation failed: {key}',
            Message=f'Error: {str(e)}\nFile moved to quarantine.'
        )
        
        print(f"Failed: {str(e)}")
        raise  # Re-raise so Lambda marks invocation as failed
```

**S3 Event Configuration:**
```json
{
    "Events": ["s3:ObjectCreated:*"],
    "Filter": {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "uploads/"},
                                {"Name": "suffix", "Value": ".csv"}]}
    }
}
```

**Lambda configuration:**
- Memory: 512 MB (enough for pandas + 100 MB files)
- Timeout: 5 minutes (plenty for file processing)
- Layer: pandas + pyarrow layer attached
- Trigger: S3 event notification on `partner-uploads` bucket

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Lambda vs Step Functions for Pipeline Orchestration

**Scenario:** Your pipeline has 5 steps that must run in sequence: extract (5 min), validate (30 sec), transform (20 min via Glue), load (2 min), notify (1 sec). Currently it's a single Lambda that calls each step synchronously — but it times out at 15 minutes. Redesign the architecture.

<details>
<summary>✅ Solution</summary>

**Problem:** Lambda max timeout is 15 minutes. The pipeline takes 28 minutes total — exceeds the limit.

**Solution: Use Step Functions to orchestrate Lambda + Glue**

```json
{
    "StartAt": "Extract",
    "States": {
        "Extract": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:function:extract-data",
            "TimeoutSeconds": 600,
            "Retry": [{"ErrorEquals": ["States.TaskFailed"], "MaxAttempts": 2}],
            "Next": "Validate"
        },
        "Validate": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:function:validate-data",
            "Next": "Transform"
        },
        "Transform": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "transform-orders",
                "Arguments": {"--process_date.$": "$.process_date"}
            },
            "TimeoutSeconds": 1800,
            "Next": "Load"
        },
        "Load": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:function:load-to-redshift",
            "Next": "Notify"
        },
        "Notify": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:function:send-notification",
            "End": true
        }
    }
}
```

**Key design decisions:**
- Lambda handles short steps (extract: 5 min, validate: 30 sec, load: 2 min, notify: 1 sec)
- Glue handles the long step (transform: 20 min) — Step Functions waits for it with `.sync` integration
- Step Functions orchestrates the sequence (no 15-min Lambda timeout)
- Built-in retry on failure (no custom retry code needed)
- Each step is independently testable and deployable

**Architecture:**

```mermaid
flowchart LR
    A["EventBridge Schedule"] --> B["Step Function"]
    B --> C["Lambda: Extract (5min)"]
    C --> D["Lambda: Validate (30s)"]
    D --> E["Glue: Transform (20min)"]
    E --> F["Lambda: Load (2min)"]
    F --> G["Lambda: Notify (1s)"]
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: High-Volume Real-Time Processing

**Scenario:** You receive 100,000 events/second from Kinesis. Each event needs: (1) enrichment from DynamoDB lookup, (2) fraud check (compare to user's 30-day average), (3) routing to different SQS queues based on event type. Current Lambda processing can't keep up — consumer lag is growing. Diagnose and fix.

<details>
<summary>✅ Solution</summary>

**Diagnosis: Why Lambda can't keep up**

```
100K events/sec × 50ms per event = 5,000 concurrent Lambda instances needed
But: DynamoDB lookup adds 10ms, fraud check adds 30ms = 90ms total per event
Actual need: 100K × 0.09s = 9,000 concurrent Lambdas!
Default concurrency limit: 1,000 → THROTTLED
```

**Fix 1: Increase batch size (reduce invocations)**

```python
# Instead of 1 event per Lambda invocation, process 500 at once
# Event Source Mapping config:
{
    "BatchSize": 500,              # Process 500 records per invocation
    "MaximumBatchingWindowInSeconds": 5,  # Or wait 5 seconds for batch
    "ParallelizationFactor": 10    # Process 10 batches per shard concurrently
}

# With 50 shards × 10 parallelization = 500 concurrent Lambdas
# Each processes 500 records = 250K records/second capacity
# Well above 100K/sec requirement!
```

**Fix 2: Optimize per-record processing time**

```python
def handler(event, context):
    records = event['Records']
    
    # BATCH DynamoDB lookups (not one-by-one!)
    user_ids = list(set(r['user_id'] for r in decoded_records))
    
    # BatchGetItem: 100 items per call (vs 100 individual GetItem calls)
    user_data = dynamodb_batch_get(user_ids)  # 1 call instead of 100
    
    # Pre-load fraud thresholds in bulk
    fraud_thresholds = get_fraud_thresholds_batch(user_ids)
    
    # Process records using pre-loaded data
    results = []
    for record in decoded_records:
        user = user_data.get(record['user_id'], {})
        threshold = fraud_thresholds.get(record['user_id'], DEFAULT_THRESHOLD)
        
        # Fraud check (in-memory, no external call)
        is_fraud = record['amount'] > threshold * 3
        
        # Route to appropriate queue
        queue_url = FRAUD_QUEUE if is_fraud else NORMAL_QUEUE
        results.append({'queue': queue_url, 'body': record})
    
    # BATCH SQS sends (not one-by-one!)
    batch_send_to_sqs(results)  # SendMessageBatch: 10 per call
    
    return {'batchItemFailures': []}  # Report partial failures
```

**Fix 3: Architecture optimization (if Lambda still insufficient)**

```
Option A: Use Kinesis Data Analytics (Flink) for the fraud check
  - Stateful processing (maintains 30-day averages in state)
  - No external DynamoDB lookups (state is local)
  - Handles 100K+/sec natively

Option B: Pre-compute and cache
  - Nightly batch job computes 30-day averages per user → Redis
  - Lambda reads from Redis (1ms) instead of computing on the fly
  - Reduces per-record time from 90ms to 5ms

Option C: Move to ECS/Fargate containers
  - No concurrency limit
  - Keep connections to DynamoDB open (connection pooling)
  - Better for sustained high throughput
```

**Final optimized architecture:**

```
Kinesis (50 shards)
  → Lambda (batch=500, parallelization=10)
    → Batch DynamoDB lookups (BatchGetItem)
    → In-memory fraud check (pre-loaded thresholds from Redis)
    → Batch SQS routing (SendMessageBatch)

Throughput: 500 concurrent Lambdas × 500 records/batch × 2 batches/sec = 500K records/sec
Well above the 100K/sec requirement with headroom.
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is AWS Lambda and what are its key constraints for data engineering?**
A: Lambda is a serverless compute service that runs code in response to events without provisioning servers. Key constraints for data engineering: 15-minute max execution time, 10GB max memory, 512MB–10GB ephemeral /tmp storage, and 1,000 concurrent executions per region by default. These limits make Lambda suitable for lightweight ETL triggers and event processing, not heavy batch transformations.

**Q: What is the Lambda execution model — cold starts vs. warm starts?**
A: On a cold start, Lambda provisions a new execution environment, downloads your code, and runs initialization code outside the handler. On a warm start, a previously used environment is reused, skipping provisioning. Cold starts add 100ms–several seconds of latency; mitigations include Provisioned Concurrency, smaller deployment packages, and keeping initialization code outside the handler.

**Q: How does Lambda scaling work?**
A: Lambda scales automatically by creating new execution environments for concurrent invocations. The default burst limit is 3,000 simultaneous executions (region-dependent), scaling by 500 per minute. Reserved concurrency caps a function's maximum concurrent executions; Provisioned Concurrency pre-warms environments for latency-sensitive functions.

**Q: What is the difference between synchronous and asynchronous Lambda invocations?**
A: Synchronous invocations (API Gateway, direct invoke) wait for the function to complete and return the result; the caller handles errors. Asynchronous invocations (S3, SNS, EventBridge) place the event in an internal queue; Lambda retries failed invocations twice automatically and can route failures to a Dead Letter Queue or an on-failure destination.

**Q: How do you handle Lambda function errors and retries in data pipelines?**
A: For async invocations, configure a Dead Letter Queue (SQS or SNS) to capture failed events after retries. For event source mappings (SQS, Kinesis), configure `bisect-on-error` to split failing batches and `maximum retry attempts` to limit retries. Use structured logging and CloudWatch alarms on error metrics for operational visibility.

**Q: How do you pass secrets and configuration to Lambda functions?**
A: Use environment variables for non-sensitive config. For secrets (database passwords, API keys), use Secrets Manager or SSM Parameter Store with the Lambda execution role having `secretsmanager:GetSecretValue` access. Use the Parameters and Secrets Lambda Extension to cache secrets in-memory and avoid per-invocation API calls.

**Q: What is Lambda Layers and when should you use them?**
A: Layers are ZIP archives containing shared libraries or dependencies that can be attached to multiple functions. Use layers for large shared packages (pandas, NumPy, PySpark) to keep your function deployment package small, enable sharing across functions, and reduce iteration time during development.

**Q: How does Lambda integrate with SQS for reliable event processing?**
A: SQS as an event source mapping lets Lambda poll the queue and process batches. Messages are not deleted until Lambda successfully processes them (at-least-once delivery). Configure batch size, batch window, and maximum concurrency. Failed batches go back to the queue and retry until the visibility timeout expires or they reach the DLQ.

---

## 💼 Interview Tips

- Always discuss Lambda's fit vs. limitations for data engineering: excellent for event-driven triggers (S3 file arrival → Glue job kick-off), lightweight transformation, and orchestration glue code; not suitable for heavy batch processing due to the 15-minute limit.
- Senior interviewers probe cold start mitigation: know that Provisioned Concurrency is the definitive solution for latency-sensitive workloads, but it incurs costs even when idle — frame it as a cost-vs-latency tradeoff.
- Mention the power of Lambda's event source mapping integrations: SQS, Kinesis, DynamoDB Streams, MSK, and Kafka all have native triggers — this enables sophisticated event-driven pipelines without custom polling infrastructure.
- Avoid vague answers about "Lambda will handle it automatically" — always specify the concurrency model, retry behavior, and error destination relevant to the invocation type.
- Demonstrate operational maturity: describe how you'd use Lambda destinations (on-success and on-failure) for async invocations to route results and errors to different downstream systems.
- Know the /tmp storage limit (up to 10GB) and ephemeral nature — never rely on /tmp across invocations. For state sharing between invocations, use S3, DynamoDB, or ElastiCache.
