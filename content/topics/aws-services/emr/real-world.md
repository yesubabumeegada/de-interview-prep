---
title: "AWS EMR - Real-World Production Examples"
topic: aws-services
subtopic: emr
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, emr, production, transient-clusters, emr-serverless, migration, monitoring]
---

# AWS EMR — Real-World Production Examples

## Pattern 1: Transient Cluster for Nightly ETL

```python
# Architecture: Airflow → Launch EMR → Run Steps → Terminate
# Use case: Process 500 GB of daily data, output to curated S3 layer

import boto3
from datetime import datetime

emr = boto3.client('emr')

def launch_nightly_etl(execution_date: str):
    """Launch transient EMR cluster for nightly batch processing."""
    
    response = emr.run_job_flow(
        Name=f'nightly-etl-{execution_date}',
        ReleaseLabel='emr-7.0.0',
        LogUri='s3://emr-logs/nightly-etl/',
        
        Instances={
            'InstanceFleets': [
                {
                    'Name': 'Master',
                    'InstanceFleetType': 'MASTER',
                    'TargetOnDemandCapacity': 1,
                    'InstanceTypeConfigs': [
                        {'InstanceType': 'm6g.xlarge', 'WeightedCapacity': 1},
                    ]
                },
                {
                    'Name': 'Core',
                    'InstanceFleetType': 'CORE',
                    'TargetOnDemandCapacity': 4,
                    'InstanceTypeConfigs': [
                        {'InstanceType': 'r6g.2xlarge', 'WeightedCapacity': 4},
                        {'InstanceType': 'r5.2xlarge', 'WeightedCapacity': 4},
                    ]
                },
                {
                    'Name': 'Task',
                    'InstanceFleetType': 'TASK',
                    'TargetSpotCapacity': 40,
                    'InstanceTypeConfigs': [
                        {'InstanceType': 'r6g.2xlarge', 'WeightedCapacity': 4},
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
            ],
            'KeepJobFlowAliveWhenNoSteps': False,  # Auto-terminate
            'Ec2SubnetIds': ['subnet-abc', 'subnet-def', 'subnet-ghi'],
        },
        
        Steps=[
            {
                'Name': 'Stage 1 - Ingest and Deduplicate',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', '--deploy-mode', 'cluster',
                        '--conf', 'spark.sql.adaptive.enabled=true',
                        '--conf', 'spark.sql.shuffle.partitions=400',
                        's3://etl-code/jobs/01_ingest_dedup.py',
                        '--date', execution_date
                    ]
                }
            },
            {
                'Name': 'Stage 2 - Transform and Enrich',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', '--deploy-mode', 'cluster',
                        '--conf', 'spark.sql.adaptive.enabled=true',
                        's3://etl-code/jobs/02_transform.py',
                        '--date', execution_date
                    ]
                }
            },
            {
                'Name': 'Stage 3 - Write to Curated Iceberg Tables',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', '--deploy-mode', 'cluster',
                        '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                        's3://etl-code/jobs/03_write_iceberg.py',
                        '--date', execution_date
                    ]
                }
            }
        ],
        
        Applications=[{'Name': 'Spark'}],
        BootstrapActions=[{
            'Name': 'install-deps',
            'ScriptBootstrapAction': {'Path': 's3://etl-code/bootstrap/install.sh'}
        }],
        
        Configurations=[
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    'spark.executor.memory': '24g',
                    'spark.executor.cores': '5',
                    'spark.driver.memory': '8g',
                    'spark.sql.adaptive.enabled': 'true',
                    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                    'spark.dynamicAllocation.enabled': 'true',
                }
            }
        ],
        
        AutoTerminationPolicy={'IdleTimeout': 300},
        Tags=[
            {'Key': 'Environment', 'Value': 'production'},
            {'Key': 'CostCenter', 'Value': 'data-platform'},
            {'Key': 'Pipeline', 'Value': 'nightly-etl'},
        ]
    )
    
    return response['JobFlowId']
```

---

## Pattern 2: Interactive Analytics Cluster with JupyterHub

```python
# Architecture: Persistent EMR cluster with JupyterHub for data science team
# Use case: Ad-hoc exploration of data lake (petabytes of data)

cluster_config = {
    'Name': 'analytics-cluster-prod',
    'ReleaseLabel': 'emr-7.0.0',
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'JupyterHub'},
        {'Name': 'Livy'},        # REST API for Spark (remote submission)
        {'Name': 'Hive'},        # SQL interface
        {'Name': 'Presto'},      # Fast interactive queries
    ],
    
    'Instances': {
        'InstanceFleets': [
            {
                'Name': 'Master',
                'InstanceFleetType': 'MASTER',
                'TargetOnDemandCapacity': 1,
                'InstanceTypeConfigs': [
                    {'InstanceType': 'r6g.2xlarge', 'WeightedCapacity': 1}
                ]
            },
            {
                'Name': 'Core',
                'InstanceFleetType': 'CORE',
                'TargetOnDemandCapacity': 8,  # Fixed core for stability
                'InstanceTypeConfigs': [
                    {'InstanceType': 'r6g.4xlarge', 'WeightedCapacity': 8}
                ]
            },
            {
                'Name': 'Task',
                'InstanceFleetType': 'TASK',
                'TargetSpotCapacity': 32,  # Elastic compute
                'InstanceTypeConfigs': [
                    {'InstanceType': 'r6g.4xlarge', 'WeightedCapacity': 8},
                    {'InstanceType': 'r5.4xlarge', 'WeightedCapacity': 8},
                    {'InstanceType': 'r5a.4xlarge', 'WeightedCapacity': 8},
                ],
                'LaunchSpecifications': {
                    'SpotSpecification': {
                        'TimeoutDurationMinutes': 15,
                        'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                        'AllocationStrategy': 'capacity-optimized'
                    }
                }
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,  # Persistent!
    },
    
    'ManagedScalingPolicy': {
        'ComputeLimits': {
            'UnitType': 'InstanceFleetUnits',
            'MinimumCapacityUnits': 8,
            'MaximumCapacityUnits': 80,
            'MaximumOnDemandCapacityUnits': 16,
        }
    },
    
    'Configurations': [
        {
            'Classification': 'jupyter-s3-conf',
            'Properties': {
                's3.persistence.enabled': 'true',
                's3.persistence.bucket': 'jupyter-notebooks-prod'
            }
        },
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 
                    'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        }
    ]
}

# Notebook persistence: notebooks saved to S3 automatically
# Multi-user: each data scientist gets isolated Spark session
# Access control: IAM + Lake Formation for column-level security
```

---

## Pattern 3: Migrating from EMR to EMR Serverless

```python
# Before: EMR on EC2 (managed cluster)
# After: EMR Serverless (no cluster management)

# EMR Serverless application setup:
import boto3

emr_serverless = boto3.client('emr-serverless')

# Create serverless application (one-time)
app = emr_serverless.create_application(
    name='data-platform-spark',
    releaseLabel='emr-7.0.0',
    type='SPARK',
    initialCapacity={
        'DRIVER': {
            'workerCount': 2,
            'workerConfiguration': {
                'cpu': '4vCPU',
                'memory': '16GB',
                'disk': '120GB'
            }
        },
        'EXECUTOR': {
            'workerCount': 10,
            'workerConfiguration': {
                'cpu': '4vCPU',
                'memory': '16GB',
                'disk': '120GB'
            }
        }
    },
    maximumCapacity={
        'cpu': '400vCPU',
        'memory': '1600GB',
        'disk': '20000GB'
    },
    autoStartConfiguration={'enabled': True},
    autoStopConfiguration={'enabled': True, 'idleTimeoutMinutes': 5}
)

# Submit job (replaces spark-submit on cluster)
job = emr_serverless.start_job_run(
    applicationId=app['applicationId'],
    executionRoleArn='arn:aws:iam::123:role/emr-serverless-role',
    jobDriver={
        'sparkSubmit': {
            'entryPoint': 's3://etl-code/jobs/daily_etl.py',
            'entryPointArguments': ['--date', '2024-01-15'],
            'sparkSubmitParameters': (
                '--conf spark.sql.adaptive.enabled=true '
                '--conf spark.executor.memory=14g '
                '--conf spark.executor.cores=4 '
                '--conf spark.dynamicAllocation.enabled=true '
                '--conf spark.dynamicAllocation.minExecutors=5 '
                '--conf spark.dynamicAllocation.maxExecutors=50'
            )
        }
    },
    configurationOverrides={
        'monitoringConfiguration': {
            's3MonitoringConfiguration': {
                'logUri': 's3://emr-logs/serverless/'
            }
        }
    }
)
```

**Migration checklist:**

| Aspect | EMR on EC2 | EMR Serverless | Migration Notes |
|--------|-----------|----------------|-----------------|
| Bootstrap actions | Shell scripts | Not supported | Move to Docker image or job init |
| HDFS | Available | Not available | Use S3 for all storage |
| spark-submit | On cluster | Via API/CLI | Change submission method |
| Custom JARs | Classpath | S3 reference | Upload to S3, reference in config |
| Monitoring | YARN UI, Ganglia | CloudWatch, Spark UI | Different dashboards |
| VPC access | Default | Configure VPC | Add network config |

---

## Production Operations and Monitoring

```python
# Key CloudWatch metrics for EMR monitoring:

monitoring_config = {
    'cluster_health': [
        'IsIdle',                    # Cluster has no running steps
        'CoreNodesRunning',          # Should equal target
        'TaskNodesRunning',          # Varies with scaling
        'MissingBlocks',             # HDFS health (should be 0)
    ],
    'spark_metrics': [
        'AppsRunning',               # Active Spark applications
        'AppsPending',               # Waiting for resources
        'ContainerAllocated',        # YARN container usage
        'ContainerPending',          # Resource starvation indicator
        'MemoryAvailableMB',         # Remaining YARN memory
    ],
    'cost_tracking': [
        # Custom metric: emit cluster cost per hour
        # instance_count * instance_price + emr_fee
        # Track actual vs budgeted monthly spend
    ]
}

# Alerting rules:
alerts = [
    {'metric': 'IsIdle', 'threshold': '1 for 30 min', 'action': 'Check if stuck or terminate'},
    {'metric': 'CoreNodesRunning', 'threshold': '< target', 'action': 'Node failure - investigate'},
    {'metric': 'ContainerPending', 'threshold': '> 0 for 10 min', 'action': 'Scale up or check config'},
    {'metric': 'Step Failed', 'threshold': 'any', 'action': 'Alert on-call, check logs'},
    {'metric': 'MemoryAvailableMB', 'threshold': '< 5%', 'action': 'Memory pressure - increase nodes'},
]
```

**Operational runbook:**
1. Job failure: Check step logs in S3 → Check Spark History Server → Identify OOM/shuffle/data issue
2. Slow performance: Check YARN containers pending → Check executor metrics → Look for data skew
3. Cost spike: Check instance count → Check job duration → Look for stuck clusters
4. Spot interruption: Verify task nodes were replaced → Check job progress → Confirm no data loss

---

## Interview Tips

> **Tip 1:** "Walk me through a production EMR ETL pipeline" — "Airflow triggers a transient EMR cluster nightly. Instance fleet with diversified spot instances for task nodes, on-demand for core. Three Spark steps: ingest/deduplicate, transform/enrich, write to Iceberg tables. Cluster auto-terminates on step completion. Cost is $12-15/night for 500 GB processing. Monitoring via CloudWatch alarms on step failures and cluster idle time."

> **Tip 2:** "Why would you migrate from EMR on EC2 to EMR Serverless?" — "Operational simplicity. No cluster management, no bootstrap scripts to maintain, no capacity planning. 30-second startup vs 5-10 minutes. Auto-scales to zero when idle. Trade-offs: no HDFS (must use S3), no SSH access for debugging, slightly higher per-compute cost. Best for teams that want to focus on Spark code, not infrastructure."

> **Tip 3:** "How do you debug a slow EMR Spark job?" — "Start with Spark History Server UI: check the DAG for skewed stages (one task taking 10x longer). Look at shuffle read/write sizes for data skew. Check YARN for pending containers (resource starvation). Check for small file problem (thousands of tiny S3 reads). Common fixes: repartition skewed keys with salting, increase shuffle partitions, compact input files, enable AQE (adaptive query execution)."
