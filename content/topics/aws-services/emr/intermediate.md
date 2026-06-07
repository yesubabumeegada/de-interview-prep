---
title: "AWS EMR - Intermediate"
topic: aws-services
subtopic: emr
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, emr, spark, hadoop, cluster-management, emrfs, scaling]
---

# AWS EMR — Intermediate Concepts

## EMR Cluster Configuration

### Bootstrap Actions

Bootstrap actions run scripts on every node before applications start:

```bash
#!/bin/bash
# bootstrap_action.sh — runs on every node at cluster launch

# Install custom Python packages
sudo pip3 install pandas==2.0.3 pyarrow==13.0.0 great-expectations==0.17.0

# Configure Spark defaults
sudo tee /etc/spark/conf/spark-defaults.conf.append <<EOF
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
EOF

# Download shared utilities from S3
aws s3 cp s3://my-bucket/libs/etl_utils.py /home/hadoop/libs/
```

### Instance Fleets (Flexible Provisioning)

```python
# Instance fleets let you specify multiple instance types per fleet
# EMR picks the best available combination for capacity and spot pricing

import boto3

emr = boto3.client('emr')

cluster = emr.run_job_flow(
    Name='nightly-etl-cluster',
    ReleaseLabel='emr-7.0.0',
    Instances={
        'InstanceFleets': [
            {
                'Name': 'Master',
                'InstanceFleetType': 'MASTER',
                'TargetOnDemandCapacity': 1,
                'InstanceTypeConfigs': [
                    {'InstanceType': 'm5.xlarge', 'WeightedCapacity': 1},
                    {'InstanceType': 'm5a.xlarge', 'WeightedCapacity': 1},
                ]
            },
            {
                'Name': 'Core',
                'InstanceFleetType': 'CORE',
                'TargetOnDemandCapacity': 2,
                'TargetSpotCapacity': 8,
                'InstanceTypeConfigs': [
                    {'InstanceType': 'r5.2xlarge', 'WeightedCapacity': 4},
                    {'InstanceType': 'r5a.2xlarge', 'WeightedCapacity': 4},
                    {'InstanceType': 'r5d.2xlarge', 'WeightedCapacity': 4},
                    {'InstanceType': 'r4.2xlarge', 'WeightedCapacity': 4},
                ],
                'LaunchSpecifications': {
                    'SpotSpecification': {
                        'TimeoutDurationMinutes': 10,
                        'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                        'AllocationStrategy': 'capacity-optimized'
                    }
                }
            }
        ]
    },
    Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}],
    BootstrapActions=[{
        'Name': 'install-deps',
        'ScriptBootstrapAction': {'Path': 's3://my-bucket/bootstrap/install.sh'}
    }]
)
```

---

## Spark Submit Options on EMR

```bash
# Submit a PySpark job to EMR cluster
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8g \
    --executor-cores 4 \
    --driver-memory 4g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --py-files s3://bucket/libs/utils.zip \
    s3://bucket/jobs/daily_etl.py \
    --date 2024-01-15 \
    --output s3://bucket/output/
```

**Deploy mode comparison:**

| Mode | Driver Location | Use Case |
|------|----------------|----------|
| client | Runs on master node | Interactive (notebooks, debugging) |
| cluster | Runs on a worker node | Production batch jobs (master stays free) |

---

## EMRFS Consistent View

```python
# EMRFS provides S3 as the storage layer with consistency guarantees
# S3 is now strongly consistent (since Dec 2020), but EMRFS adds:
# - Retry logic for eventual S3 operations
# - S3 server-side encryption integration
# - Optimized S3 file listing (committer protocols)

# Key configuration for data lakes:
spark_conf = {
    # Use EMRFS S3-optimized committer (avoid _temporary folder issues)
    'spark.sql.sources.commitProtocolClass': 
        'com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter',
    
    # Enable S3 multipart upload for large files
    'spark.hadoop.fs.s3a.multipart.size': '128m',
    'spark.hadoop.fs.s3a.fast.upload': 'true',
    
    # Retry configuration
    'spark.hadoop.fs.s3a.retry.limit': '10',
    'spark.hadoop.fs.s3a.retry.interval': '500ms',
}
```

> **Critical:** Always use the EMRFS S3-optimized committer for Spark writes to S3. The default Hadoop committer creates `_temporary` directories that cause failures in concurrent writes and leave orphan files.

---

## Managed Scaling

```python
# EMR Managed Scaling automatically adds/removes nodes based on workload

managed_scaling_policy = {
    'ComputeLimits': {
        'UnitType': 'InstanceFleetUnits',  # or Instances, VCPU
        'MinimumCapacityUnits': 4,          # Minimum cluster size
        'MaximumCapacityUnits': 100,        # Maximum scale-out
        'MaximumOnDemandCapacityUnits': 20, # Cap on-demand spend
        'MaximumCoreCapacityUnits': 20      # Core nodes (with HDFS)
    }
}

# Scaling metrics EMR uses:
# - YARNMemoryAvailablePercentage (scale up when <15%)
# - ContainerPendingRatio (pending containers vs running)
# - HDFSUtilization (for core nodes with HDFS)

# Best practice: use mostly TASK nodes for scaling (no HDFS, cheaper to add/remove)
# Core nodes: fixed count, hold HDFS replicas
# Task nodes: elastic, compute-only, use spot instances
```

**Node type roles:**

| Node Type | HDFS | Can Scale Down | Spot Safe | Role |
|-----------|------|---------------|-----------|------|
| Master | Yes (NameNode) | No | No | Cluster coordination |
| Core | Yes (DataNode) | Risky (data loss) | Risky | Storage + compute |
| Task | No | Yes (safe) | Yes | Compute only |

---

## Security Configuration

```python
# Kerberos authentication for EMR
security_config = {
    'AuthenticationConfiguration': {
        'KerberosConfiguration': {
            'Provider': 'ClusterDedicatedKdc',
            'ClusterDedicatedKdcConfiguration': {
                'TicketLifetimeInHours': 24
            }
        }
    },
    'EncryptionConfiguration': {
        'EnableInTransitEncryption': True,
        'EnableAtRestEncryption': True,
        'AtRestEncryptionConfiguration': {
            'S3EncryptionConfiguration': {
                'EncryptionMode': 'SSE-KMS',
                'AwsKmsKey': 'arn:aws:kms:us-east-1:123:key/abc-123'
            },
            'LocalDiskEncryptionConfiguration': {
                'EncryptionKeyProviderType': 'AwsKms',
                'AwsKmsKey': 'arn:aws:kms:us-east-1:123:key/abc-123'
            }
        }
    }
}

# Lake Formation integration (fine-grained access)
# - Column-level access control on S3 data
# - No need to manage S3 bucket policies per table
# - Users see only columns they have permissions for
```

---

## Jupyter and Zeppelin Notebooks on EMR

```python
# EMR Studio / JupyterHub — interactive analytics on the cluster

# Launch cluster with JupyterHub:
# Applications: ['Spark', 'JupyterHub', 'Livy']

# Access: https://<master-public-dns>:9443/hub
# Each user gets isolated Jupyter environment with Spark session

# PySpark notebook example (runs on the cluster):
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("interactive-analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Query data lake directly
df = spark.read.parquet("s3://data-lake/curated/fact_orders/")
df.filter("order_date >= '2024-01-01'") \
  .groupBy("product_category") \
  .agg({"amount": "sum", "order_id": "count"}) \
  .orderBy("sum(amount)", ascending=False) \
  .show(10)

# Profile large dataset (billions of rows, too big for pandas)
df.describe().show()
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
```

---

## Step Execution (Job Pipeline)

```python
# EMR Steps: queue jobs to run sequentially on the cluster
# Useful for multi-stage ETL without external orchestration

steps = [
    {
        'Name': 'Stage 1 - Extract and Clean',
        'ActionOnFailure': 'CONTINUE',  # or TERMINATE_CLUSTER
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.shuffle.partitions=500',
                's3://bucket/jobs/01_extract.py',
                '--date', '2024-01-15'
            ]
        }
    },
    {
        'Name': 'Stage 2 - Transform',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', '--deploy-mode', 'cluster',
                's3://bucket/jobs/02_transform.py',
                '--date', '2024-01-15'
            ]
        }
    },
    {
        'Name': 'Stage 3 - Load to Curated',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', '--deploy-mode', 'cluster',
                's3://bucket/jobs/03_load.py',
                '--date', '2024-01-15'
            ]
        }
    }
]

emr.add_job_flow_steps(JobFlowId='j-ABC123', Steps=steps)
```

---

## Interview Tips

> **Tip 1:** "How do you configure an EMR cluster for production ETL?" — "Instance fleets with multiple instance types for spot availability. Core nodes on-demand for HDFS stability, task nodes on spot for elastic compute. Bootstrap actions install dependencies. Managed scaling for auto-sizing. The S3-optimized committer for reliable writes. Deploy mode cluster so the driver doesn't overload the master node."

> **Tip 2:** "What is the difference between core and task nodes?" — "Core nodes run HDFS DataNodes and compute tasks. Task nodes are compute-only with no local HDFS. Task nodes are safe to scale down (no data loss) and ideal for spot instances. Core nodes should be on-demand because losing one loses HDFS blocks (replication helps but recovery is slow). For S3-based workloads, minimize core nodes and scale with task nodes."

> **Tip 3:** "How does EMR managed scaling work?" — "EMR monitors YARN metrics (pending containers, available memory) and automatically adds or removes task nodes. You set min/max capacity limits and a cap on on-demand spend. It prefers spot instances for scale-out. Scaling decisions happen in 1-2 minutes. Best used with transient workloads that have varying stages (small extract, large shuffle, small write)."
