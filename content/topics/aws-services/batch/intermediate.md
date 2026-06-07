---
title: "AWS Batch - Intermediate"
topic: aws-services
subtopic: batch
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, batch, array-jobs, dependencies, fargate, spot, scheduling]
---

# AWS Batch — Intermediate Concepts

## Array Jobs — Massive Parallelism

Array jobs create N identical containers, each with a unique index:

```python
# Submit 10,000 parallel jobs with one API call
batch.submit_job(
    jobName='process-daily-files',
    jobQueue='spot-queue',
    jobDefinition='file-processor',
    arrayProperties={'size': 10000},  # Creates jobs with index 0-9999
    parameters={'date': '2024-01-15', 'bucket': 's3://raw-data/'}
)

# Inside each container:
import os, boto3, json

index = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])  # 0 to 9999
total = int(os.environ.get('AWS_BATCH_JOB_ARRAY_SIZE', 10000))

# Strategy 1: Each job processes one file
files = list_all_files(f"s3://raw-data/dt=2024-01-15/")
my_file = files[index]
process(my_file)

# Strategy 2: Each job processes a range (for variable file counts)
chunk_size = len(files) // total
start = index * chunk_size
end = start + chunk_size if index < total - 1 else len(files)
for f in files[start:end]:
    process(f)
```

---

## Multi-Node Parallel Jobs

For workloads requiring communication between containers (MPI, distributed training):

```python
batch.register_job_definition(
    jobDefinitionName='distributed-training',
    type='multinode',
    nodeProperties={
        'numNodes': 4,
        'mainNode': 0,
        'nodeRangeProperties': [{
            'targetNodes': '0:3',
            'container': {
                'image': '123.dkr.ecr.us-east-1.amazonaws.com/training:latest',
                'vcpus': 8,
                'memory': 32768,
                'instanceType': 'p3.2xlarge',  # GPU instance
            }
        }]
    }
)
# 4 nodes communicate via MPI/NCCL for distributed ML training
```

---

## Job Dependencies and Workflows

```python
# Chain: extract → transform → load (sequential)
extract = batch.submit_job(jobName='extract', jobQueue='q', jobDefinition='extract-job')

transform = batch.submit_job(
    jobName='transform', jobQueue='q', jobDefinition='transform-job',
    dependsOn=[{'jobId': extract['jobId'], 'type': 'SEQUENTIAL'}]
    # Waits for extract to SUCCEED before starting
)

load = batch.submit_job(
    jobName='load', jobQueue='q', jobDefinition='load-job',
    dependsOn=[{'jobId': transform['jobId']}]
)

# Array dependency: N_TO_N
# Array job B[i] depends on Array job A[i] (child-to-child mapping)
array_a = batch.submit_job(
    jobName='validate', arrayProperties={'size': 100}, ...
)
array_b = batch.submit_job(
    jobName='transform', arrayProperties={'size': 100},
    dependsOn=[{'jobId': array_a['jobId'], 'type': 'N_TO_N'}]
    # transform[0] starts after validate[0] succeeds
    # transform[1] starts after validate[1] succeeds
    # ...independently
)
```

---

## Fair-Share Scheduling

Allocate compute proportionally between teams/priorities:

```python
batch.create_scheduling_policy(
    name='data-platform-fairshare',
    fairsharePolicy={
        'shareDecaySeconds': 600,  # 10-minute decay window
        'computeReservation': 10,  # Reserve 10% for emergency jobs
        'shareDistribution': [
            {'shareIdentifier': 'etl-team', 'weightFactor': 0.6},       # 60% of compute
            {'shareIdentifier': 'ml-team', 'weightFactor': 0.3},        # 30% of compute
            {'shareIdentifier': 'ad-hoc', 'weightFactor': 0.1},         # 10% of compute
        ]
    }
)

# Submit with share identifier
batch.submit_job(
    jobName='ml-inference',
    jobQueue='shared-queue',
    jobDefinition='inference-job',
    shareIdentifier='ml-team',  # Counts against ML team's share
)
```

---

## Spot Instance Best Practices

```python
# Allocation strategy: SPOT_CAPACITY_OPTIMIZED (recommended)
# Selects instance types with the most available Spot capacity (fewest interruptions)

compute_env = {
    'type': 'SPOT',
    'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED',
    'instanceTypes': [
        # Diversify: more instance types = better Spot availability
        'c5.large', 'c5.xlarge', 'c5.2xlarge',
        'c5a.large', 'c5a.xlarge',
        'm5.large', 'm5.xlarge',
        'r5.large', 'r5.xlarge',
    ],
    'bidPercentage': 100,  # Pay up to On-Demand price (let AWS optimize)
}

# Handle Spot interruptions gracefully:
# 1. Set retry strategy (automatically resubmit interrupted jobs)
retryStrategy = {
    'attempts': 3,
    'evaluateOnExit': [
        {'onStatusReason': 'Host EC2*terminated', 'action': 'RETRY'},
        {'onExitCode': '0', 'action': 'EXIT'},      # Success
        {'onExitCode': '*', 'action': 'EXIT'},       # Any other failure: don't retry
    ]
}

# 2. Checkpoint progress (for long jobs)
# In your container: save progress to S3 every N records
# On restart: resume from last checkpoint instead of starting over
```

---

## Container Image Management

```dockerfile
# Dockerfile for a Batch job
FROM python:3.11-slim

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/
WORKDIR /app

# Entry point (Batch overrides the command via job definition)
ENTRYPOINT ["python"]
CMD ["process.py"]
```

```python
# CI/CD: build and push to ECR on every merge
# GitHub Actions example:
# 1. Build image
# 2. Push to ECR
# 3. Update Batch job definition to reference new image tag
# 4. New jobs automatically use the updated image

batch.register_job_definition(
    jobDefinitionName='file-processor',
    type='container',
    containerProperties={
        'image': '123.dkr.ecr.us-east-1.amazonaws.com/file-processor:v2.1.0',
        # Pin to specific tag (not :latest) for reproducibility
        ...
    }
)
```

---

## Monitoring and Observability

```python
# Job states flow: SUBMITTED → PENDING → RUNNABLE → STARTING → RUNNING → SUCCEEDED/FAILED

# Monitor via CloudWatch:
# - JobsSubmittedCount, JobsRunningCount, JobsFailedCount
# - CPUUtilization, MemoryUtilization per compute environment

# Custom metrics from inside your container:
import boto3

cloudwatch = boto3.client('cloudwatch')
cloudwatch.put_metric_data(
    Namespace='BatchJobs/ETL',
    MetricData=[{
        'MetricName': 'RecordsProcessed',
        'Value': records_count,
        'Unit': 'Count',
        'Dimensions': [
            {'Name': 'JobName', 'Value': os.environ['AWS_BATCH_JOB_ID']},
            {'Name': 'ArrayIndex', 'Value': os.environ.get('AWS_BATCH_JOB_ARRAY_INDEX', '0')}
        ]
    }]
)
```

---

## Integration with Step Functions

```json
{
    "Type": "Task",
    "Resource": "arn:aws:states:::batch:submitJob.sync",
    "Parameters": {
        "JobDefinition": "my-job",
        "JobQueue": "my-queue",
        "JobName": "step-functions-triggered-job",
        "ArrayProperties": {"Size": 100},
        "Parameters": {
            "date.$": "$.process_date"
        }
    },
    "Retry": [{"ErrorEquals": ["Batch.JobFailed"], "MaxAttempts": 2}],
    "Next": "NextStep"
}
```

> **`.sync` integration:** Step Functions waits for the Batch job to complete (SUCCEEDED or FAILED) before continuing. Perfect for orchestrating multi-step pipelines.

---

## Interview Tips

> **Tip 1:** "How do array jobs handle failures?" — "Each array child job is independent. If child[42] fails, others continue. You can set retry strategy per child. After all children complete, check array job status: SUCCEEDED only if ALL children succeeded. For partial failure handling: check individual child statuses and resubmit only the failed indices."

> **Tip 2:** "How do you handle Spot interruptions in Batch?" — "Three layers: (1) Retry strategy with `evaluateOnExit` matching EC2 termination reason → auto-resubmit. (2) Instance type diversification (10+ types) reduces interruption probability. (3) For long-running jobs: checkpoint progress to S3 every N records, resume from checkpoint on retry instead of restarting."

> **Tip 3:** "Batch vs ECS for containerized workloads?" — "Batch for: batch/scheduled jobs with queuing, array parallelism, Spot optimization built-in, automatic scaling to zero. ECS for: long-running services (APIs, web servers), precise control over task placement, service discovery, load balancing. Batch IS built on ECS/Fargate — it adds job scheduling and array job semantics on top."
