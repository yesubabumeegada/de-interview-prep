---
title: "AWS Lambda - Intermediate"
topic: aws-services
subtopic: lambda
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, lambda, serverless, step-functions, vpc, layers, concurrency]
---

# AWS Lambda — Intermediate Concepts

## Lambda Layers — Shared Code and Dependencies

Layers let you package libraries, custom runtimes, or shared code separately from your function code:

```python
# Layer structure on disk:
# python/lib/python3.11/site-packages/pandas/...
# python/lib/python3.11/site-packages/numpy/...

# Deploy a layer with shared utilities
# aws lambda publish-layer-version \
#   --layer-name data-utils \
#   --zip-file fileb://layer.zip \
#   --compatible-runtimes python3.11

# In your Lambda function, just import as normal:
import pandas as pd
import numpy as np
from shared_utils import validate_schema, send_alert

def handler(event, context):
    df = pd.read_json(event['body'])
    validated = validate_schema(df, schema='orders_v2')
    return {'statusCode': 200, 'body': f'Processed {len(df)} rows'}
```

**Layer limits and best practices:**

| Constraint | Limit |
|-----------|-------|
| Layers per function | 5 |
| Total unzipped size (code + layers) | 250 MB |
| Layer versions | Immutable (publish new version) |
| Sharing | Cross-account via resource policy |

> **When to use layers:** Shared libraries across multiple functions (pandas, numpy), common utility code, or custom runtimes. Avoid for function-specific code.

---

## Environment Variables and Configuration

```python
import os
import json
import boto3

# Environment variables set in Lambda configuration
ENVIRONMENT = os.environ['ENVIRONMENT']          # dev, staging, prod
TABLE_NAME = os.environ['DYNAMODB_TABLE']        # pipeline-state-prod
S3_BUCKET = os.environ['OUTPUT_BUCKET']          # data-lake-prod
SECRET_ARN = os.environ['DB_SECRET_ARN']         # arn:aws:secretsmanager:...

def get_db_credentials():
    """Fetch secrets at runtime (not in env vars for security)."""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=SECRET_ARN)
    return json.loads(response['SecretString'])

# Cache expensive lookups outside handler (reused across invocations)
_db_creds = None

def handler(event, context):
    global _db_creds
    if _db_creds is None:
        _db_creds = get_db_credentials()
    
    # Use cached credentials
    process_data(event, _db_creds)
```

**Best practice:** Store non-sensitive config (bucket names, table names, feature flags) in environment variables. Store secrets in Secrets Manager or Parameter Store and fetch at runtime.

---

## VPC Access — Connecting to Private Resources

```python
# Lambda in VPC can access:
# - RDS instances in private subnets
# - ElastiCache clusters
# - Redshift clusters
# - Any resource in the VPC

# BUT: Lambda in VPC loses internet access by default!
# Solution: NAT Gateway or VPC Endpoints

# Architecture:
# Lambda (private subnet) → NAT Gateway (public subnet) → Internet
# Lambda (private subnet) → VPC Endpoint → S3/DynamoDB (no internet needed)

import psycopg2

def handler(event, context):
    # Connect to RDS in the same VPC
    conn = psycopg2.connect(
        host='mydb.cluster-abc123.us-east-1.rds.amazonaws.com',
        dbname='analytics',
        user='lambda_user',
        password=get_secret('rds-password')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders WHERE date = %s", (event['date'],))
    count = cursor.fetchone()[0]
    conn.close()
    return {'row_count': count}
```

**VPC configuration impact:**

| Aspect | Without VPC | With VPC |
|--------|-------------|----------|
| Cold start | ~200ms | ~200ms (ENI pre-created since 2019) |
| Internet access | Yes (default) | No (need NAT or endpoints) |
| S3 access | Direct | VPC Endpoint (free) or NAT |
| RDS access | No | Yes |
| Cost | Base only | + NAT Gateway ($0.045/hr + data) |

---

## Step Functions Integration

```python
# Step Functions orchestrate multiple Lambdas into workflows
# Common pattern: ETL pipeline with validation

# Lambda 1: Extract
def extract_handler(event, context):
    """Pull data from source and stage in S3."""
    records = fetch_from_api(event['source_url'])
    s3_path = f"s3://staging/{event['date']}/raw.parquet"
    write_parquet(records, s3_path)
    return {'s3_path': s3_path, 'record_count': len(records)}

# Lambda 2: Validate
def validate_handler(event, context):
    """Check data quality before loading."""
    df = read_parquet(event['s3_path'])
    null_pct = df.isnull().sum().max() / len(df)
    if null_pct > 0.05:
        raise ValueError(f"Null rate {null_pct:.1%} exceeds 5% threshold")
    return {'s3_path': event['s3_path'], 'valid': True}

# Lambda 3: Load
def load_handler(event, context):
    """Move validated data to curated zone."""
    copy_to_curated(event['s3_path'])
    trigger_glue_crawler('curated-orders')
    return {'status': 'complete'}
```

---

## Dead-Letter Queues (DLQ) and Failure Handling

```python
# DLQ captures events that fail all retry attempts
# Configure: Lambda → Configuration → Asynchronous invocation → DLQ

# SQS-based DLQ processor (reprocess failed events)
import json
import boto3

sqs = boto3.client('sqs')
DLQ_URL = os.environ['DLQ_URL']

def dlq_processor(event, context):
    """Process failed events from DLQ with enhanced error handling."""
    for record in event['Records']:
        original_event = json.loads(record['body'])
        failure_reason = record['messageAttributes'].get('ErrorMessage', {}).get('stringValue')
        
        # Log failure details
        print(f"Reprocessing failed event: {failure_reason}")
        
        try:
            # Attempt reprocessing with relaxed validation
            process_with_fallback(original_event)
        except Exception as e:
            # Send to permanent failure store after DLQ retry
            send_to_permanent_failure(original_event, str(e))
```

**Retry behavior by invocation type:**

| Invocation Type | Retries | DLQ Support |
|----------------|---------|-------------|
| Synchronous (API Gateway) | 0 (caller retries) | No |
| Asynchronous (S3, SNS) | 2 retries | Yes (SQS or SNS) |
| Event source (SQS, Kinesis) | Until expiry | SQS DLQ on source |

---

## Provisioned Concurrency — Eliminating Cold Starts

```python
# Cold start: first invocation initializes runtime (~1-3s for Python + libs)
# Provisioned concurrency: keep N instances pre-warmed

# aws lambda put-provisioned-concurrency-config \
#   --function-name data-processor \
#   --qualifier prod \
#   --provisioned-concurrent-executions 10

# Auto-scaling provisioned concurrency (scale with schedule):
# - 10 instances during business hours (8 AM - 6 PM)
# - 2 instances overnight (batch jobs only)
# - 50 instances during peak ETL window (2 AM - 4 AM)

# Cost comparison:
# On-demand: $0.20/1M requests + $0.0000166667/GB-second
# Provisioned: $0.0000041667/GB-second (always on) + execution cost
# Break-even: ~50-70% utilization of provisioned capacity
```

---

## Lambda Destinations — Routing Success and Failure

```python
# Destinations route async invocation results WITHOUT code changes
# Success → SQS queue (for downstream processing)
# Failure → SNS topic (for alerting)

# Configuration (via AWS CLI or IaC):
# aws lambda put-function-event-invoke-config \
#   --function-name data-processor \
#   --destination-config '{
#     "OnSuccess": {"Destination": "arn:aws:sqs:us-east-1:123:success-queue"},
#     "OnFailure": {"Destination": "arn:aws:sns:us-east-1:123:failure-alerts"}
#   }'

# The destination receives the full invocation record:
# {
#   "version": "1.0",
#   "requestContext": {"functionArn": "...", "condition": "Success"},
#   "requestPayload": {original event},
#   "responsePayload": {function return value}
# }
```

> **Destinations vs DLQ:** Destinations handle both success AND failure routing with richer metadata. DLQ only handles failures. Use destinations for event-driven architectures; use DLQ for simple retry patterns.

---

## Lambda Power Tuning

```python
# AWS Lambda Power Tuning (open-source Step Functions tool)
# Tests your function at different memory sizes and finds the optimal setting

# Memory affects CPU proportionally:
# 128 MB  = 1/8 vCPU
# 1024 MB = ~0.6 vCPU
# 1769 MB = 1 full vCPU
# 3008 MB = ~1.7 vCPU
# 10240 MB = 6 vCPUs

# Example result for a pandas-heavy function:
# 128 MB:  Duration 15000ms, Cost $0.000031
# 512 MB:  Duration 4200ms,  Cost $0.000035
# 1024 MB: Duration 2100ms,  Cost $0.000035
# 2048 MB: Duration 1100ms,  Cost $0.000037
# 3008 MB: Duration 980ms,   Cost $0.000049

# Sweet spot: 1024 MB (fast enough, cheapest per-invocation)
# For latency-sensitive: 2048 MB (2x faster, similar cost)
```

---

## Interview Tips

> **Tip 1:** "How do you manage shared dependencies across Lambda functions?" — "Lambda Layers. Package common libraries (pandas, shared utilities) into a layer, publish it with versioning, and attach to multiple functions. Each function can use up to 5 layers. This reduces deployment package size and ensures consistency across functions."

> **Tip 2:** "What happens when Lambda is in a VPC?" — "Lambda gets ENIs in the specified subnets. It can access private resources (RDS, ElastiCache) but loses internet access. To reach the internet or AWS services, you need a NAT Gateway (costly) or VPC Endpoints (free for S3/DynamoDB). Since 2019, VPC-attached Lambdas no longer have additional cold start penalty."

> **Tip 3:** "How do you choose the right memory setting for Lambda?" — "Use Lambda Power Tuning. Memory scales CPU proportionally (1769 MB = 1 vCPU). More memory often means faster execution, so cost stays flat while latency drops. Run your function at 128, 512, 1024, 2048, and 3008 MB — compare duration times cost. The sweet spot is usually where cost plateaus but duration is acceptable."
