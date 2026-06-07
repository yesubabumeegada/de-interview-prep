---
title: "AWS Lambda - Senior Deep Dive"
topic: aws-services
subtopic: lambda
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, lambda, cold-start, snapstart, concurrency, custom-runtime, cost-optimization]
---

# AWS Lambda — Senior-Level Deep Dive

## Cold Start Optimization

Cold starts happen when Lambda must create a new execution environment. Understanding the phases:

```
Cold Start Phases:
1. Download code (50-200ms) — from S3/ECR
2. Start runtime (100-500ms) — Python/Java/Node initialization
3. Init code execution (variable) — imports, connections, SDK clients
4. Handler execution — your actual logic

Warm invocation: only phase 4 runs (reuses existing environment)
```

**Optimization strategies by impact:**

| Strategy | Cold Start Reduction | Effort |
|----------|---------------------|--------|
| Smaller deployment package | 50-150ms | Low |
| Fewer imports (lazy loading) | 100-500ms | Medium |
| Provisioned concurrency | Eliminates entirely | Cost |
| SnapStart (Java) | 90%+ reduction | Low |
| ARM64 (Graviton2) | 10-20% faster init | Low |
| Connection reuse (global scope) | 200-800ms per warm call | Low |

```python
# BAD: imports everything at module level
import pandas as pd
import numpy as np
import boto3
import sqlalchemy
from heavy_library import everything  # Adds 2s to cold start

def handler(event, context):
    # Uses only boto3 for this specific invocation
    s3 = boto3.client('s3')
    return s3.get_object(Bucket='b', Key='k')

# GOOD: lazy imports for heavy libraries
import boto3  # Light, always needed

def handler(event, context):
    if event.get('needs_pandas'):
        import pandas as pd  # Only loaded when needed
        return process_dataframe(event)
    
    # Most invocations only need boto3
    s3 = boto3.client('s3')
    return s3.get_object(Bucket='b', Key='k')
```

---

## Custom Runtimes — Running Anything on Lambda

```python
# Custom runtime: implement the Lambda Runtime API
# Use case: Rust, C++, or specific Python version not yet supported

# bootstrap file (executable that Lambda runs):
#!/bin/sh
set -euo pipefail

# Runtime API endpoint provided by Lambda
RUNTIME_API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"

# Initialization (runs once per cold start)
python3 -c "from my_app import init; init()"

# Processing loop (handles multiple invocations)
while true; do
    # Get next event
    RESPONSE=$(curl -sS "${RUNTIME_API}/invocation/next" -D /tmp/headers)
    REQUEST_ID=$(grep -i Lambda-Runtime-Aws-Request-Id /tmp/headers | tr -d '\r' | cut -d: -f2 | xargs)
    
    # Process event
    RESULT=$(echo "$RESPONSE" | python3 -c "from my_app import handler; import sys, json; print(json.dumps(handler(json.load(sys.stdin), None)))")
    
    # Send response
    curl -sS -X POST "${RUNTIME_API}/invocation/${REQUEST_ID}/response" -d "$RESULT"
done
```

**Custom runtime use cases in data engineering:**
- Compiled Rust for high-throughput stream processing (10x faster than Python)
- Custom Python with C extensions not available in AWS-managed runtime
- Embedded database engines (DuckDB, SQLite) for serverless analytics

---

## Lambda SnapStart (Java)

```java
// SnapStart: takes a Firecracker microVM snapshot after init
// Cold start: 5-10s → 200-400ms (90%+ reduction)

// How it works:
// 1. Deploy function → Lambda runs init code
// 2. Lambda takes memory snapshot (Firecracker snapshot)
// 3. On cold start: restore from snapshot (fast!) instead of re-initializing

// Hooks for snapshot lifecycle:
public class Handler implements RequestHandler<Event, Response> {
    
    // CRaC (Coordinated Restore at Checkpoint) hooks
    @Override
    public void beforeCheckpoint(Context<? extends Resource> context) {
        // Close connections before snapshot (they'll be stale after restore)
        dbConnection.close();
        httpClient.close();
    }
    
    @Override
    public void afterRestore(Context<? extends Resource> context) {
        // Re-establish connections after restore
        dbConnection = createNewConnection();
        httpClient = HttpClient.newHttpClient();
    }
}
```

> **Key limitation:** SnapStart only works with Java 11+ and Corretto. Not available for Python/Node. For Python cold start optimization, use provisioned concurrency or smaller packages.

---

## Memory and CPU Tuning

```python
# Lambda allocates CPU proportionally to memory:
# 128 MB  → 0.08 vCPU (throttled)
# 1769 MB → 1.0 vCPU (single core)
# 3538 MB → 2.0 vCPU (multi-core begins)
# 10240 MB → 6.0 vCPU (maximum)

# IMPORTANT: Multi-core only helps if your code is parallelized!
# Python GIL limits: threading won't help for CPU-bound work
# Use multiprocessing for CPU-bound parallel work above 1769 MB

import multiprocessing
from concurrent.futures import ProcessPoolExecutor

def process_chunk(chunk):
    """CPU-intensive transformation on a data chunk."""
    import pandas as pd
    df = pd.DataFrame(chunk)
    return df.apply(complex_transform).to_dict('records')

def handler(event, context):
    data = load_data(event['s3_path'])
    
    # At 3538+ MB, we have 2+ vCPUs — use them!
    num_cores = multiprocessing.cpu_count()  # Returns available cores
    chunk_size = len(data) // num_cores
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        results = list(executor.map(process_chunk, chunks))
    
    return {'processed': sum(len(r) for r in results)}
```

**Memory tuning decision matrix:**

| Workload Type | Recommended Memory | Reason |
|--------------|-------------------|--------|
| I/O bound (API calls, S3) | 256-512 MB | CPU not the bottleneck |
| Light processing (JSON transform) | 512-1024 MB | Balanced cost/speed |
| Data processing (pandas) | 1024-3008 MB | CPU-bound benefits |
| Heavy compute (ML inference) | 3008-10240 MB | Multi-core parallel |
| Memory-heavy (large DataFrames) | Set by data size | Prevent OOM |

---

## Event Source Mapping Internals

```python
# Event source mappings (ESM) poll sources and batch invoke Lambda
# Sources: SQS, Kinesis, DynamoDB Streams, Kafka, MQ

# Kinesis ESM internals:
# - Lambda service polls each shard
# - Batches records (up to 10,000 or 6 MB or batch window)
# - Invokes your function synchronously
# - Checkpoints on success (advances shard iterator)
# - On failure: retries entire batch OR bisects

# Critical settings for Kinesis/DynamoDB Streams:
config = {
    'BatchSize': 1000,               # Records per invocation (max 10000)
    'MaximumBatchingWindowInSeconds': 30,  # Wait up to 30s to fill batch
    'ParallelizationFactor': 10,     # Process 10 batches per shard simultaneously
    'BisectBatchOnFunctionError': True,    # Split batch on failure to find bad record
    'MaximumRetryAttempts': 3,       # Then send to DLQ
    'DestinationConfig': {
        'OnFailure': {'Destination': 'arn:aws:sqs:...'}  # Failed batch DLQ
    },
    'FunctionResponseTypes': ['ReportBatchItemFailures'],  # Partial batch failure
}

# Partial batch failure response (avoid reprocessing entire batch):
def handler(event, context):
    failures = []
    for record in event['Records']:
        try:
            process_record(record)
        except Exception as e:
            failures.append({'itemIdentifier': record['kinesis']['sequenceNumber']})
    
    return {'batchItemFailures': failures}  # Only failed records retry
```

---

## Concurrency Controls

```
Account-level concurrency limit: 1000 (default, can request increase)
                    |
    ┌───────────────┼───────────────┐
    |               |               |
Reserved: 100    Reserved: 50    Unreserved: 850
(func-A)         (func-B)        (all other functions share)

Reserved concurrency: guarantees capacity AND caps maximum
Provisioned concurrency: pre-warms instances (subset of reserved)
```

```python
# Scenario: Kinesis stream with 100 shards, parallelization factor = 5
# Maximum concurrency for this function: 100 shards * 5 = 500 concurrent executions

# If account limit is 1000 and this function uses 500:
# Only 500 left for ALL other functions in the account!

# Solution: Set reserved concurrency
# aws lambda put-function-concurrency \
#   --function-name kinesis-processor \
#   --reserved-concurrent-executions 500

# This guarantees 500 AND prevents exceeding 500 (protects other functions)
```

**Reserved vs Provisioned Concurrency:**

| Aspect | Reserved | Provisioned |
|--------|----------|-------------|
| Purpose | Capacity guarantee + throttle cap | Eliminate cold starts |
| Cold starts | Still happen | None (pre-warmed) |
| Cost | Free | ~$13/month per instance (1 GB) |
| Scales to zero | Yes | No (minimum stays warm) |
| Use case | Protect capacity, limit blast radius | Latency-sensitive APIs |

---

## Cost at Scale Analysis

```python
# Cost model for Lambda data processing:
# Assumptions: 1 GB memory, 10s average duration, ARM64

# Per invocation cost:
request_cost = 0.20 / 1_000_000           # $0.0000002 per request
compute_cost = 0.0000133334 * 1 * 10      # GB-seconds price * GB * seconds
total_per_invocation = request_cost + compute_cost  # $0.000133534

# Scale scenarios:
scenarios = {
    '10K invocations/day': 10_000 * 30 * total_per_invocation,      # ~$40/month
    '100K invocations/day': 100_000 * 30 * total_per_invocation,    # ~$400/month
    '1M invocations/day': 1_000_000 * 30 * total_per_invocation,    # ~$4,000/month
    '10M invocations/day': 10_000_000 * 30 * total_per_invocation,  # ~$40,000/month
}

# Lambda vs Fargate vs EC2 break-even:
# Lambda wins: sporadic, bursty workloads (<30% average utilization)
# Fargate wins: steady 30-70% utilization
# EC2 wins: steady >70% utilization or GPU/custom hardware needed

# Rule of thumb: if a Lambda runs >50% of the time continuously,
# consider Fargate or EC2 for 2-5x cost savings
```

**Cost optimization checklist:**
1. Use ARM64 (Graviton2) — 20% cheaper, often faster
2. Right-size memory with Power Tuning
3. Batch records (fewer invocations = fewer request charges)
4. Set max concurrency to prevent runaway costs
5. Use provisioned concurrency only where latency SLA requires it

---

## Interview Tips

> **Tip 1:** "How do you optimize Lambda cold starts for a Python data pipeline?" — "Four strategies in order of impact: (1) Minimize deployment package (remove unused libraries, use Lambda layers for shared deps), (2) Lazy-import heavy libraries like pandas (only when needed), (3) Use ARM64 for 10-20% faster init, (4) For strict latency SLAs, use provisioned concurrency. SnapStart is Java-only. For Python, going from 3s cold start to 500ms is achievable with lean packaging alone."

> **Tip 2:** "How do you handle partial failures in Kinesis-triggered Lambda?" — "Enable ReportBatchItemFailures in the event source mapping. The handler returns a list of failed sequence numbers. Lambda only retries those specific records instead of the entire batch. Combined with BisectBatchOnFunctionError, this prevents one poison message from blocking an entire shard."

> **Tip 3:** "When does Lambda become more expensive than containers?" — "Lambda cost scales linearly with invocations and duration. At roughly 50% continuous utilization or 1M+ daily invocations with long durations (>5s), Fargate becomes cheaper. The crossover depends on memory and duration — run the math for your specific workload. Lambda's value is zero cost at idle and instant scaling, not raw compute efficiency."
