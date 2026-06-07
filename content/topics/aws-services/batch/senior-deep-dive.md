---
title: "AWS Batch - Senior Deep Dive"
topic: aws-services
subtopic: batch
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, batch, cost-optimization, spot-strategies, gpu, architecture]
---

# AWS Batch — Senior-Level Deep Dive

## Advanced Spot Strategies

### Capacity-Optimized Allocation

```python
# SPOT_CAPACITY_OPTIMIZED: AWS places jobs on instance types with
# the highest current Spot capacity (fewest interruptions)
compute_resources = {
    'type': 'SPOT',
    'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED',
    'instanceTypes': [
        # Provide 10+ instance types for maximum flexibility
        'c5.large', 'c5.xlarge', 'c5.2xlarge', 'c5.4xlarge',
        'c5a.large', 'c5a.xlarge', 'c5a.2xlarge',
        'c5n.large', 'c5n.xlarge',
        'm5.large', 'm5.xlarge', 'm5.2xlarge',
        'm5a.large', 'm5a.xlarge',
        'r5.large', 'r5.xlarge',
    ],
    # Don't set bidPercentage below 100% — let AWS optimize price
    'bidPercentage': 100,
}
# Result: 80-90% less interruptions vs BEST_FIT strategy
# AWS selects from the deepest Spot pools automatically
```

### Interruption Rate by Strategy

| Strategy | Avg Interruption Rate | Cost | Best For |
|----------|:---:|:---:|---|
| BEST_FIT | 10-30% | Cheapest single instance | Non-critical, short jobs |
| SPOT_CAPACITY_OPTIMIZED | 2-5% | Slightly higher | Production batch workloads |
| BEST_FIT_PROGRESSIVE | 5-15% | Medium | Balance of cost and reliability |

---

## GPU Workloads

```python
# GPU compute environment for ML inference/training
batch.create_compute_environment(
    computeEnvironmentName='gpu-env',
    computeResources={
        'type': 'SPOT',
        'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED',
        'instanceTypes': ['p3.2xlarge', 'g4dn.xlarge', 'g4dn.2xlarge', 'g5.xlarge'],
        'minvCpus': 0,
        'maxvCpus': 128,
    }
)

# GPU job definition
batch.register_job_definition(
    jobDefinitionName='ml-inference-gpu',
    type='container',
    containerProperties={
        'image': '123.dkr.ecr.us-east-1.amazonaws.com/ml-inference:gpu',
        'vcpus': 4,
        'memory': 16384,
        'resourceRequirements': [
            {'type': 'GPU', 'value': '1'}  # Request 1 GPU
        ],
        'command': ['python', 'inference.py', '--batch-size', '256'],
    }
)
```

---

## Cost Modeling at Scale

```python
def estimate_batch_cost(num_jobs, duration_seconds, vcpus, memory_gb, compute_type='FARGATE'):
    """Estimate AWS Batch cost for a workload."""
    hours = duration_seconds / 3600
    
    if compute_type == 'FARGATE':
        vcpu_cost = vcpus * 0.04048 * hours  # Per vCPU-hour
        mem_cost = memory_gb * 0.004445 * hours  # Per GB-hour
        per_job = vcpu_cost + mem_cost
    elif compute_type == 'EC2_SPOT':
        # Approximate: Spot is ~30% of On-Demand for compute-optimized
        per_job = vcpus * 0.017 * hours  # ~$0.017/vCPU-hr for c5 Spot
    elif compute_type == 'EC2_ON_DEMAND':
        per_job = vcpus * 0.085 * hours  # ~$0.085/vCPU-hr for c5
    
    total = per_job * num_jobs
    return {
        'per_job': f'${per_job:.4f}',
        'total': f'${total:.2f}',
        'monthly_30_runs': f'${total * 30:.2f}'
    }

# Compare:
print(estimate_batch_cost(10000, 120, 2, 4, 'FARGATE'))    # $33/run
print(estimate_batch_cost(10000, 120, 2, 4, 'EC2_SPOT'))    # $9.44/run
print(estimate_batch_cost(10000, 120, 2, 4, 'EC2_ON_DEMAND'))  # $47/run
```

---

## Checkpointing for Long-Running Jobs

```python
# For jobs that run >1 hour on Spot: save progress to survive interruptions
import os, json, boto3, signal

s3 = boto3.client('s3')
CHECKPOINT_BUCKET = 'batch-checkpoints'
JOB_ID = os.environ['AWS_BATCH_JOB_ID']

def save_checkpoint(state):
    """Save progress to S3 before interruption."""
    s3.put_object(
        Bucket=CHECKPOINT_BUCKET,
        Key=f'checkpoints/{JOB_ID}.json',
        Body=json.dumps(state)
    )

def load_checkpoint():
    """Resume from last checkpoint if exists."""
    try:
        obj = s3.get_object(Bucket=CHECKPOINT_BUCKET, Key=f'checkpoints/{JOB_ID}.json')
        return json.loads(obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None  # No checkpoint — start from beginning

# Handle Spot interruption signal (2-minute warning)
def handle_termination(signum, frame):
    save_checkpoint({'last_processed_index': current_index, 'partial_results': results})
    print("Checkpoint saved before Spot termination")
    exit(0)

signal.signal(signal.SIGTERM, handle_termination)

# Main processing logic with checkpointing
checkpoint = load_checkpoint()
start_index = checkpoint['last_processed_index'] + 1 if checkpoint else 0

for i in range(start_index, total_items):
    process_item(items[i])
    current_index = i
    
    if i % 1000 == 0:  # Checkpoint every 1000 items
        save_checkpoint({'last_processed_index': i})
```

---

## Multi-Queue Architecture

```python
# Priority-based queue routing:
# Queue 1 (high priority): critical ETL jobs → On-Demand compute
# Queue 2 (normal): standard batch jobs → Spot compute  
# Queue 3 (low priority): ML experiments → Spot (lowest bid)

batch.create_job_queue(
    jobQueueName='critical-queue',
    priority=100,  # Highest priority
    computeEnvironmentOrder=[
        {'computeEnvironment': 'on-demand-env', 'order': 1}
    ]
)

batch.create_job_queue(
    jobQueueName='standard-queue',
    priority=50,
    computeEnvironmentOrder=[
        {'computeEnvironment': 'spot-primary', 'order': 1},
        {'computeEnvironment': 'on-demand-fallback', 'order': 2}
    ]
)

batch.create_job_queue(
    jobQueueName='experiment-queue',
    priority=1,  # Lowest priority — gets remaining capacity
    computeEnvironmentOrder=[
        {'computeEnvironment': 'spot-primary', 'order': 1}
    ],
    schedulingPolicyArn='arn:aws:batch:...:scheduling-policy/fairshare'
)
```

---

## Batch + Step Functions — Production Pattern

```python
# Step Functions orchestrates multi-stage pipeline with Batch for heavy compute

workflow = {
    "StartAt": "PrepareInput",
    "States": {
        "PrepareInput": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:prepare-input",
            "Next": "ParallelProcessing"
        },
        "ParallelProcessing": {
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
                "JobDefinition": "heavy-compute",
                "JobQueue": "spot-queue",
                "ArrayProperties": {"Size.$": "$.file_count"},
                "Parameters": {"date.$": "$.date"}
            },
            "Retry": [
                {"ErrorEquals": ["Batch.JobFailed"], "MaxAttempts": 2, "BackoffRate": 2}
            ],
            "Catch": [
                {"ErrorEquals": ["States.ALL"], "Next": "HandleFailure"}
            ],
            "Next": "Consolidate"
        },
        "Consolidate": {
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
                "JobDefinition": "consolidator",
                "JobQueue": "fargate-queue"
            },
            "Next": "Success"
        },
        "HandleFailure": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:...:alert-on-failure",
            "Next": "Failed"
        },
        "Success": {"Type": "Succeed"},
        "Failed": {"Type": "Fail"}
    }
}
```

---

## Interview Tips

> **Tip 1:** "How do you handle Spot interruptions in production Batch jobs?" — "Four layers: (1) SPOT_CAPACITY_OPTIMIZED allocation with 10+ instance types (minimizes interruptions). (2) Retry strategy with evaluateOnExit matching EC2 termination — auto-resubmits to a different instance. (3) Checkpointing every N records to S3 — resume from last checkpoint, not restart from zero. (4) Fargate fallback compute environment — if ALL Spot capacity is gone, jobs still run on Fargate."

> **Tip 2:** "How do you size compute environments for cost optimization?" — "Calculate: max concurrent vCPUs needed = total jobs × vCPUs per job / (parallelism × duration ratio). Set maxvCpus to this. Use Spot for 60-80% savings. Diversify instance types (10+) for Spot availability. Set minvCpus=0 (scale to zero when idle). Monitor actual utilization and right-size monthly."

> **Tip 3:** "Batch vs Kubernetes (EKS) for batch workloads?" — "Batch: simpler (no K8s expertise needed), built-in Spot handling, array jobs, job queues/scheduling, auto-provisions compute. EKS: more control (custom schedulers like Volcano), existing K8s ecosystem, better for teams already on K8s, supports Spark natively. Choose Batch for pure batch processing; choose EKS if you already have a K8s platform and want consistency."
