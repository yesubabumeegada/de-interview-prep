---
title: "AWS EMR - Senior Deep Dive"
topic: aws-services
subtopic: emr
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, emr, spot-instances, yarn, spark-tuning, emr-on-eks, serverless, cost-optimization]
---

# AWS EMR — Senior-Level Deep Dive

## Spot Instance Strategies

### Diversification and Capacity-Optimized Allocation

```python
# Key principle: diversify across instance types AND availability zones
# Spot interruption rate varies by instance type and AZ

# BAD: Single instance type (high interruption risk)
bad_config = {'InstanceType': 'r5.4xlarge', 'TargetSpotCapacity': 20}

# GOOD: Diversified fleet with capacity-optimized allocation
good_config = {
    'InstanceFleetType': 'TASK',
    'TargetSpotCapacity': 80,  # 80 capacity units
    'InstanceTypeConfigs': [
        # Mix generations, families, and sizes
        {'InstanceType': 'r5.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r5a.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r5d.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r5n.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r4.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'm5.4xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r6i.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
        {'InstanceType': 'r6a.2xlarge', 'WeightedCapacity': 4, 'BidPriceAsPercentageOfOnDemandPrice': 100},
    ],
    'LaunchSpecifications': {
        'SpotSpecification': {
            'TimeoutDurationMinutes': 15,
            'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
            # capacity-optimized picks pools with most available capacity
            'AllocationStrategy': 'capacity-optimized'
        }
    }
}
```

**Allocation strategy comparison:**

| Strategy | Behavior | Best For |
|----------|----------|----------|
| lowest-price | Picks cheapest spot pool | Short jobs (< 1 hour) |
| capacity-optimized | Picks pool with most capacity | Long jobs (less interruption) |
| diversified | Spreads across all pools equally | Maximum availability |

---

## YARN Tuning

```bash
# YARN manages cluster resources (memory + vCPUs)
# Key parameters for EMR Spark workloads:

# yarn-site.xml configuration
yarn.nodemanager.resource.memory-mb = 57344    # 56 GB (leave 1-2 GB for OS)
yarn.nodemanager.resource.cpu-vcores = 16       # All cores to YARN
yarn.scheduler.maximum-allocation-mb = 57344    # Max container size
yarn.scheduler.minimum-allocation-mb = 1024     # Min container size

# Common problem: YARN kills containers for exceeding memory
# Solution: account for off-heap memory
spark.executor.memoryOverhead = max(384MB, 0.10 * executor-memory)
# For PySpark with pandas: set overhead to 20-30% (Python processes use off-heap)

# Monitor YARN health:
# yarn application -list              # Running applications
# yarn node -list                     # Node status
# yarn application -status app_id    # Application details
```

**Memory calculation for a r5.2xlarge (64 GB RAM, 8 vCPUs):**

```
Total node memory: 64 GB
OS/system reserved: -4 GB
YARN allocatable: 60 GB

Option A: Fewer large executors (better for shuffles)
  2 executors × 28 GB memory × 4 cores each
  Overhead: 28 GB × 0.10 = 2.8 GB
  YARN container: 28 + 2.8 = 30.8 GB each

Option B: More small executors (better for parallelism)  
  4 executors × 13 GB memory × 2 cores each
  Overhead: 13 GB × 0.10 = 1.3 GB
  YARN container: 13 + 1.3 = 14.3 GB each
```

---

## Spark Executor Sizing on EMR

```python
# Rule of thumb for EMR Spark sizing:

def calculate_spark_config(instance_type_memory_gb, instance_vcpus, num_nodes):
    """Calculate optimal Spark executor configuration."""
    
    # Reserve for OS and node manager
    usable_memory = instance_type_memory_gb - 4  # GB
    usable_cores = instance_vcpus - 1            # Reserve 1 for YARN NM
    
    # 5 cores per executor (sweet spot for I/O parallelism)
    cores_per_executor = min(5, usable_cores)
    executors_per_node = usable_cores // cores_per_executor
    
    # Memory per executor
    memory_per_executor = int(usable_memory / executors_per_node)
    overhead = max(int(memory_per_executor * 0.10), 1)  # 10% overhead
    
    # Total executors (minus 1 for driver)
    total_executors = (executors_per_node * num_nodes) - 1
    
    return {
        'spark.executor.cores': cores_per_executor,
        'spark.executor.memory': f'{memory_per_executor - overhead}g',
        'spark.executor.memoryOverhead': f'{overhead}g',
        'spark.executor.instances': total_executors,
        'spark.driver.memory': f'{memory_per_executor - overhead}g',
        'spark.sql.shuffle.partitions': total_executors * cores_per_executor * 2,
    }

# Example: 10x r5.2xlarge (64 GB, 8 vCPUs)
config = calculate_spark_config(64, 8, 10)
# Result:
# executor.cores = 5 (actually 7 usable, so 1 executor per node with 7 cores)
# executor.memory = 50g
# executor.instances = 9
# shuffle.partitions = 126
```

---

## Transient vs Persistent Clusters

| Aspect | Transient | Persistent |
|--------|-----------|-----------|
| Lifecycle | Launch → process → terminate | Always running |
| Cost | Pay only during processing | 24/7 cost |
| Use case | Scheduled ETL (nightly batch) | Interactive analytics, notebooks |
| State | Stateless (S3 for everything) | HDFS for temp data, long sessions |
| Failure recovery | Re-launch fresh cluster | Node replacement, graceful decommission |
| Typical savings | 60-80% vs persistent | N/A (baseline) |
| Spot suitability | Excellent (retry entire job) | Risky (interrupts active sessions) |

```python
# Transient cluster pattern with Step Functions:
# 1. Create cluster → 2. Add steps → 3. Wait for steps → 4. Terminate

# Auto-termination settings:
cluster_config = {
    'AutoTerminationPolicy': {'IdleTimeout': 300},  # Terminate after 5 min idle
    'KeepJobFlowAliveWhenNoSteps': False,  # Terminate when steps complete
}
```

---

## EMR on EKS Architecture

```python
# EMR on EKS: run Spark jobs on existing Kubernetes cluster
# No need to manage separate EMR clusters

import boto3

emr_containers = boto3.client('emr-containers')

# Submit Spark job to EKS
response = emr_containers.start_job_run(
    virtualClusterId='vc-abc123',  # Mapped to EKS namespace
    name='daily-etl-2024-01-15',
    executionRoleArn='arn:aws:iam::123:role/emr-on-eks-role',
    releaseLabel='emr-7.0.0-latest',
    jobDriver={
        'sparkSubmitJobDriver': {
            'entryPoint': 's3://bucket/jobs/daily_etl.py',
            'entryPointArguments': ['--date', '2024-01-15'],
            'sparkSubmitParameters': (
                '--conf spark.executor.instances=20 '
                '--conf spark.executor.memory=8g '
                '--conf spark.executor.cores=4 '
                '--conf spark.kubernetes.container.image=123.dkr.ecr.us-east-1.amazonaws.com/spark:3.5'
            )
        }
    },
    configurationOverrides={
        'monitoringConfiguration': {
            's3MonitoringConfiguration': {
                'logUri': 's3://bucket/emr-on-eks-logs/'
            }
        }
    }
)
```

**EMR on EKS vs EMR on EC2:**

| Aspect | EMR on EC2 | EMR on EKS |
|--------|-----------|-------------|
| Infrastructure | Dedicated EC2 cluster | Shared Kubernetes cluster |
| Startup time | 5-10 minutes | 30-60 seconds |
| Resource sharing | Separate from other workloads | Share with microservices |
| Scaling | EMR managed scaling | Kubernetes autoscaler |
| Cost | Per-cluster overhead | Shared cluster efficiency |
| Complexity | EMR manages everything | Need Kubernetes expertise |
| Best for | Large dedicated workloads | Multi-tenant, fast iteration |

---

## Graviton Instances (ARM64)

```python
# Graviton2/3 instances: 20-40% better price-performance for Spark

# Graviton instance types for EMR:
graviton_instances = [
    'm6g.2xlarge',   # General purpose (Graviton2)
    'r6g.2xlarge',   # Memory optimized (Graviton2)
    'm7g.2xlarge',   # General purpose (Graviton3)
    'r7g.2xlarge',   # Memory optimized (Graviton3)
    'c6g.4xlarge',   # Compute optimized (Graviton2)
]

# EMR automatically uses ARM64 Spark/Java binaries
# Python/PySpark works transparently (no code changes)
# Some native libraries may need ARM64 builds (check bootstrap)

# Cost comparison (us-east-1, approximate):
# r5.2xlarge  (x86): $0.504/hr
# r6g.2xlarge (ARM): $0.403/hr → 20% cheaper
# r7g.2xlarge (ARM): $0.425/hr → 16% cheaper, faster

# For Spark workloads: typically 20-30% cost reduction with same performance
```

---

## Cost Comparison: Provisioned vs Serverless

```python
# Scenario: Nightly ETL processing 500 GB, takes 2 hours

# Option 1: EMR on EC2 (provisioned, transient)
emr_ec2_cost = {
    'master': 1 * 0.504 * 2,      # 1x r5.2xlarge × 2 hours = $1.01
    'core': 4 * 0.504 * 2,        # 4x r5.2xlarge × 2 hours = $4.03
    'task_spot': 10 * 0.504 * 0.3 * 2,  # 10x spot (70% off) × 2 hours = $3.02
    'emr_fee': 15 * 0.126 * 2,    # EMR surcharge per instance = $3.78
    'total_monthly': (1.01 + 4.03 + 3.02 + 3.78) * 30  # ~$355/month
}

# Option 2: EMR Serverless
emr_serverless_cost = {
    # Pricing: $0.052624/vCPU-hour + $0.0057785/GB-hour
    'vcpu_hours': 80 * 2 * 0.052624,    # 80 vCPUs × 2 hours = $8.42
    'memory_hours': 320 * 2 * 0.0057785, # 320 GB × 2 hours = $3.70
    'storage': 200 * 2 * 0.000111,       # 200 GB ephemeral = $0.04
    'total_monthly': (8.42 + 3.70 + 0.04) * 30  # ~$365/month
}

# Option 3: Glue (for comparison)
glue_cost = {
    # Pricing: $0.44/DPU-hour (1 DPU = 4 vCPU + 16 GB)
    'dpus': 20 * 2 * 0.44,  # 20 DPUs × 2 hours = $17.60
    'total_monthly': 17.60 * 30  # ~$528/month
}
```

| Option | Monthly Cost | Startup Time | Operational Overhead |
|--------|-------------|--------------|---------------------|
| EMR EC2 (spot) | ~$355 | 5-10 min | High (cluster management) |
| EMR Serverless | ~$365 | 30-60 sec | Low (serverless) |
| Glue | ~$528 | 1-2 min | Lowest (fully managed) |
| EMR on EKS | ~$300 | 30-60 sec | Medium (need K8s) |

---

## Interview Tips

> **Tip 1:** "How do you minimize EMR costs for batch ETL?" — "Three strategies: (1) Transient clusters that terminate after job completion (no idle cost). (2) Instance fleet diversification with capacity-optimized spot allocation (60-70% savings on compute). (3) Right-size executors using the 5-core rule and memory calculation based on instance type. For large clusters, Graviton instances add another 20% savings."

> **Tip 2:** "EMR on EC2 vs EMR on EKS vs EMR Serverless — when do you use each?" — "EMR on EC2 for large, dedicated workloads needing full control (custom AMIs, HDFS). EMR on EKS when you already have Kubernetes and want fast startup plus resource sharing. EMR Serverless for simplest operations — no cluster management, just submit jobs. Cost is similar; choose based on operational maturity and startup time requirements."

> **Tip 3:** "How do you handle spot interruptions in EMR?" — "Instance fleet diversification across 6-8 instance types in multiple AZs using capacity-optimized allocation. Keep core nodes on-demand (they hold HDFS). Task nodes on spot (compute-only, safe to lose). Enable graceful decommissioning so interrupted nodes finish active tasks. For critical jobs, set TimeoutAction to SWITCH_TO_ON_DEMAND as fallback."
