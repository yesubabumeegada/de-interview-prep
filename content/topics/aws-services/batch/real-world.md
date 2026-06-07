---
title: "AWS Batch - Real-World Production Examples"
topic: aws-services
subtopic: batch
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, batch, production, parallel-processing, ml-inference, etl]
---

# AWS Batch — Real-World Production Examples

## Pattern 1: Nightly File Processing Pipeline

```python
# Process 5,000 partner files nightly (varying sizes: 10 MB to 5 GB each)
# Workflow: S3 event → Step Functions → Batch array job → validate → load to Redshift

# Job definition: processes one file
batch.register_job_definition(
    jobDefinitionName='partner-file-processor',
    type='container',
    containerProperties={
        'image': '123.dkr.ecr.us-east-1.amazonaws.com/file-processor:v3.2',
        'resourceRequirements': [
            {'type': 'VCPU', 'value': '2'},
            {'type': 'MEMORY', 'value': '4096'}
        ],
        'command': ['python', 'process_file.py'],
        'environment': [
            {'name': 'OUTPUT_BUCKET', 'value': 's3://data-lake/curated/partner/'},
            {'name': 'QUALITY_TABLE', 'value': 'quality_checks.partner_files'}
        ],
        'jobRoleArn': 'arn:aws:iam::123:role/BatchJobRole',
        'logConfiguration': {
            'logDriver': 'awslogs',
            'options': {'awslogs-group': '/batch/partner-processing'}
        }
    },
    retryStrategy={
        'attempts': 3,
        'evaluateOnExit': [
            {'onStatusReason': 'Host EC2*terminated', 'action': 'RETRY'},
            {'onExitCode': '137', 'action': 'RETRY'},  # OOM killed — retry on bigger instance
            {'onExitCode': '0', 'action': 'EXIT'},
            {'onExitCode': '*', 'action': 'EXIT'}
        ]
    },
    timeout={'attemptDurationSeconds': 1800}  # 30 min max per file
)

# Container script: process_file.py
"""
import os, boto3, pandas as pd

index = int(os.environ.get('AWS_BATCH_JOB_ARRAY_INDEX', 0))
files = json.loads(os.environ['FILE_LIST'])  # Passed via job parameters
my_file = files[index]

# Download from S3
s3.download_file(my_file['bucket'], my_file['key'], '/tmp/input.csv')

# Validate and transform
df = pd.read_csv('/tmp/input.csv')
assert len(df) > 0, "Empty file"
assert df['amount'].notna().all(), "Null amounts found"

# Transform
df['processed_at'] = datetime.now().isoformat()
df['source_file'] = my_file['key']

# Write to curated zone
output_key = f"curated/partner/{my_file['date']}/{my_file['name']}.parquet"
df.to_parquet(f'/tmp/output.parquet')
s3.upload_file('/tmp/output.parquet', os.environ['OUTPUT_BUCKET'], output_key)
"""
```

---

## Pattern 2: ML Batch Inference at Scale

```python
# Process 1M images for classification (100K per day, backlog catch-up)
# Each image: 3 seconds on CPU (2 vCPU), 0.5 seconds on GPU

# GPU compute environment (g4dn instances — best price/performance for inference)
batch.create_compute_environment(
    computeEnvironmentName='gpu-inference',
    computeResources={
        'type': 'SPOT',
        'instanceTypes': ['g4dn.xlarge', 'g4dn.2xlarge', 'g5.xlarge'],
        'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED',
        'minvCpus': 0,
        'maxvCpus': 64,  # Up to 16 GPU instances (4 vCPU each)
    }
)

# GPU job definition
batch.register_job_definition(
    jobDefinitionName='image-classifier',
    containerProperties={
        'image': '123.dkr.ecr.us-east-1.amazonaws.com/classifier:gpu-v2',
        'resourceRequirements': [
            {'type': 'VCPU', 'value': '4'},
            {'type': 'MEMORY', 'value': '16384'},
            {'type': 'GPU', 'value': '1'}
        ],
        'command': ['python', 'classify_batch.py',
                    '--start-index', 'Ref::start',
                    '--end-index', 'Ref::end'],
    }
)

# Submit: 1M images / 1000 per job = 1000 array jobs
batch.submit_job(
    jobName='classify-million-images',
    jobQueue='gpu-queue',
    jobDefinition='image-classifier',
    arrayProperties={'size': 1000},
    parameters={'start': '0', 'end': '999999'}
)

# Each container processes 1000 images:
# index * 1000 to (index + 1) * 1000
# 1000 images × 0.5s each = 500s per container = 8.3 minutes
# 16 GPUs running simultaneously → 1000 / 16 = 63 batches → ~525 min = ~9 hours total
# Cost: 16 GPU Spot × 9 hrs × $0.16/hr = $23 for 1M images!
```

---

## Pattern 3: Data Validation Framework

```python
# Validate 200 tables nightly before allowing downstream consumers access
# Each validation: check row counts, null rates, schema conformity, freshness

# Validation container
"""
# validate_table.py (inside Docker container)
import os, boto3, json
from datetime import datetime, timedelta

table_config = json.loads(os.environ['TABLE_CONFIG'])
table_name = table_config['name']
s3_path = table_config['path']
expected_min_rows = table_config['min_rows']
max_null_rate = table_config.get('max_null_rate', 0.05)

# Run validations
import pyarrow.parquet as pq

dataset = pq.ParquetDataset(s3_path)
table = dataset.read()
row_count = len(table)

# Checks
results = {
    'table': table_name,
    'timestamp': datetime.now().isoformat(),
    'checks': []
}

# Check 1: Row count
results['checks'].append({
    'name': 'row_count',
    'passed': row_count >= expected_min_rows,
    'actual': row_count,
    'expected': f'>= {expected_min_rows}'
})

# Check 2: Freshness (data from last 24 hours exists)
# Check 3: Null rates on critical columns
# Check 4: Schema matches expected

# Report results
s3.put_object(
    Bucket='quality-reports',
    Key=f'validations/{datetime.now().strftime("%Y-%m-%d")}/{table_name}.json',
    Body=json.dumps(results)
)

# Exit code signals pass/fail to Batch
all_passed = all(c['passed'] for c in results['checks'])
exit(0 if all_passed else 1)
"""

# Submit 200 parallel validation jobs
batch.submit_job(
    jobName='nightly-validation',
    jobQueue='fargate-queue',
    jobDefinition='table-validator',
    arrayProperties={'size': 200},
    containerOverrides={
        'environment': [{'name': 'TABLE_CONFIGS_PATH', 'value': 's3://config/tables.json'}]
    }
)
```

---

## Pattern 4: Step Functions + Batch Pipeline

```python
# Complete production pipeline: EventBridge → Step Functions → Batch → Redshift

# Step Functions state machine (simplified):
pipeline = {
    "StartAt": "ListPartnerFiles",
    "States": {
        "ListPartnerFiles": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:list-s3-files",
            "Parameters": {"bucket": "partner-uploads", "date.$": "$.date"},
            "Next": "ProcessFiles"
        },
        "ProcessFiles": {
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
                "JobDefinition": "partner-file-processor",
                "JobQueue": "spot-queue",
                "JobName.$": "States.Format('process-{}', $.date)",
                "ArrayProperties": {"Size.$": "$.file_count"},
            },
            "Retry": [{"ErrorEquals": ["Batch.JobFailed"], "MaxAttempts": 1}],
            "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "AlertOnFailure"}],
            "Next": "ValidateOutput"
        },
        "ValidateOutput": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:validate-batch-output",
            "Next": "LoadToRedshift"
        },
        "LoadToRedshift": {
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
                "JobDefinition": "redshift-loader",
                "JobQueue": "fargate-queue",
                "JobName": "load-to-redshift"
            },
            "Next": "NotifySuccess"
        },
        "NotifySuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:...:pipeline-success",
                "Message.$": "States.Format('Pipeline completed for {}', $.date)"
            },
            "End": true
        },
        "AlertOnFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {"TopicArn": "arn:aws:sns:...:pipeline-failures"},
            "Next": "FailPipeline"
        },
        "FailPipeline": {"Type": "Fail"}
    }
}
```

---

## Production Operations

| Metric | Monitor | Alert Threshold |
|--------|---------|-----------------|
| Job success rate | CloudWatch: JobsSucceeded / (Succeeded + Failed) | < 95% |
| Job queue depth | CloudWatch: PendingJobs | > 1000 for 30+ minutes |
| Average duration | Custom metric from jobs | > 2x baseline |
| Spot interruption rate | CloudWatch: SpotInterruptions | > 10% of jobs |
| Compute utilization | CloudWatch: CPUUtilization on compute env | < 20% (over-provisioned) |

---

## Interview Tips

> **Tip 1:** "Design a system to process 100K files daily on AWS" — "AWS Batch with array jobs on Spot. One job definition (Docker container with processing logic). Submit one array job with size=100K. Each container uses its AWS_BATCH_JOB_ARRAY_INDEX to pick its file. Spot instances for 70% cost savings. Retry strategy handles interruptions. Step Functions orchestrates the overall pipeline (list files → Batch → validate → load)."

> **Tip 2:** "How do you ensure all files are processed even with Spot interruptions?" — "Three mechanisms: (1) Retry strategy auto-resubmits interrupted jobs. (2) After array job completes: check for failed children and resubmit only those indices. (3) For long jobs: checkpoint to S3 every N records so retries resume from last checkpoint, not from zero. (4) Fargate fallback compute environment for jobs that repeatedly fail on Spot."

> **Tip 3:** "Batch vs Glue for non-Spark ETL?" — "Batch: any language/runtime in Docker (Python, R, Go, Java), Spot instances for 70% savings, array jobs for parallelism, GPU support, unlimited runtime. Glue: specifically PySpark, managed Spark infrastructure, catalog integration, bookmarks for incremental. If your ETL is pure Python (not Spark) and needs parallelism or long runtime: Batch is the better fit."
