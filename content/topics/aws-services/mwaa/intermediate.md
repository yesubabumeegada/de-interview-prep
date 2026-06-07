---
title: "AWS MWAA - Intermediate"
topic: aws-services
subtopic: mwaa
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [aws, mwaa, airflow, plugins, networking, integration, scaling]
---

# AWS MWAA — Intermediate Concepts

## Custom Plugins and Operators

Package custom operators, hooks, and sensors as Airflow plugins:

```python
# plugins/custom_operators/glue_quality_operator.py
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook

class GlueJobWithQualityCheckOperator(BaseOperator):
    """Run a Glue job and validate output quality."""
    
    def __init__(self, job_name, quality_checks, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.quality_checks = quality_checks
    
    def execute(self, context):
        # Run Glue job
        hook = GlueJobHook(job_name=self.job_name)
        job_run = hook.initialize_job(script_arguments={'--date': context['ds']})
        hook.job_completion(self.job_name, job_run['JobRunId'])
        
        # Run quality checks on output
        for check in self.quality_checks:
            result = self._run_check(check)
            if not result['passed']:
                raise ValueError(f"Quality check failed: {check['name']} - {result['details']}")
        
        return {'job_run_id': job_run['JobRunId'], 'quality': 'passed'}
```

**Plugin structure for MWAA:**
```
plugins/
├── __init__.py
├── custom_operators/
│   ├── __init__.py
│   └── glue_quality_operator.py
├── custom_hooks/
│   ├── __init__.py
│   └── slack_webhook_hook.py
└── custom_sensors/
    ├── __init__.py
    └── data_quality_sensor.py
```

Upload as zip: `aws s3 cp plugins.zip s3://mwaa-bucket/plugins.zip`

---

## VPC Networking

MWAA requires a VPC with specific networking:

```
VPC Requirements:
├── 2 Private Subnets (different AZs) — for scheduler + workers
├── NAT Gateway (for outbound internet access)
│   └── Workers need internet for: pip installs, external APIs
├── VPC Endpoints (optional, for AWS service access without NAT):
│   ├── com.amazonaws.region.s3 (Gateway endpoint — free)
│   ├── com.amazonaws.region.sqs (for Celery queue)
│   ├── com.amazonaws.region.kms (for encryption)
│   ├── com.amazonaws.region.logs (for CloudWatch)
│   └── com.amazonaws.region.monitoring (for CloudWatch metrics)
└── Security Group:
    ├── Self-referencing inbound (workers communicate with each other)
    └── Outbound: 443 (HTTPS), 5432 (Aurora DB), all for self-reference
```

> **Common mistake:** Not adding self-referencing security group rule → workers can't communicate with the scheduler/database → DAG parsing fails.

---

## MWAA + AWS Service Integration Patterns

### Pattern 1: MWAA → Glue → Redshift

```python
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

with DAG('mwaa_glue_redshift', ...):
    transform = GlueJobOperator(
        task_id='transform_orders',
        job_name='orders-transform',
        script_args={'--date': '{{ ds }}'},
        wait_for_completion=True,
    )
    
    load = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='curated',
        table='fact_orders',
        s3_bucket='data-lake',
        s3_key='curated/orders/dt={{ ds }}/',
        copy_options=['FORMAT AS PARQUET'],
        redshift_conn_id='redshift_default',
    )
    
    transform >> load
```

### Pattern 2: MWAA → EMR → Quality Check

```python
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrAddStepsOperator, 
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

with DAG('mwaa_emr_pipeline', ...):
    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr',
        job_flow_overrides=EMR_CONFIG,
    )
    
    add_step = EmrAddStepsOperator(
        task_id='spark_etl',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr') }}",
        steps=[{'Name': 'Spark ETL', 'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', 's3://scripts/etl.py', '--date', '{{ ds }}']}}],
    )
    
    wait_step = EmrStepSensor(
        task_id='wait_for_spark',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='spark_etl')[0] }}",
    )
    
    terminate = EmrTerminateJobFlowOperator(
        task_id='terminate_emr',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr') }}",
        trigger_rule='all_done',  # Terminate even if step fails
    )
    
    create_cluster >> add_step >> wait_step >> terminate
```

---

## Worker Auto-Scaling

MWAA automatically scales workers based on queued tasks:

```python
# Configuration options affecting scaling:
mwaa.create_environment(
    MinWorkers=1,          # Minimum workers (always running, even idle)
    MaxWorkers=25,         # Maximum workers during peak
    AirflowConfigurationOptions={
        'celery.worker_autoscale': '16,4',  # Max 16 concurrent tasks per worker, min 4
    }
)

# Scaling behavior:
# Tasks queued → MWAA adds workers (takes 2-3 minutes per new worker)
# Queue empty for 5 minutes → workers scale back to MinWorkers
# Each worker handles celery.worker_autoscale[0] concurrent tasks
```

**Scaling math:**
```
Max concurrent tasks = MaxWorkers × celery.worker_autoscale (max)
Example: 10 workers × 16 tasks/worker = 160 concurrent tasks

If you need 50 concurrent tasks:
  Option A: 5 workers × 10 tasks/worker (fewer, larger workers)
  Option B: 10 workers × 5 tasks/worker (more, smaller workers)
  Option A is cheaper (fewer worker instances to pay for)
```

---

## Airflow Configuration Options

```python
# Key MWAA configuration options (set via AirflowConfigurationOptions)
config = {
    # Core
    'core.parallelism': '64',                # Max tasks running across all DAGs
    'core.max_active_runs_per_dag': '3',     # Max concurrent DAG runs per DAG
    'core.dag_file_processor_timeout': '150', # Seconds before DAG file parsing times out
    
    # Scheduler
    'scheduler.min_file_process_interval': '60',  # Seconds between re-parsing DAG files
    'scheduler.dag_dir_list_interval': '30',      # Seconds between scanning dags/ folder
    
    # Celery (task execution)
    'celery.worker_autoscale': '16,4',       # Max,min tasks per worker
    
    # Email (via SES)
    'smtp.smtp_host': 'email-smtp.us-east-1.amazonaws.com',
    'smtp.smtp_port': '587',
    'smtp.smtp_starttls': 'True',
    'smtp.smtp_mail_from': 'airflow@company.com',
    
    # Webserver
    'webserver.default_ui_timezone': 'America/New_York',
}
```

---

## Secrets Management

MWAA integrates with AWS Secrets Manager and Systems Manager Parameter Store for connections and variables:

```python
# Store Airflow connection in Secrets Manager
# Secret name: airflow/connections/redshift_default
# Secret value:
{
    "conn_type": "redshift",
    "host": "cluster.xxx.us-east-1.redshift.amazonaws.com",
    "schema": "warehouse",
    "login": "airflow_user",
    "password": "secret_password",
    "port": 5439
}

# MWAA automatically discovers this connection — no manual UI setup needed!
# In your DAG:
hook = PostgresHook(postgres_conn_id='redshift_default')  # Found from Secrets Manager

# For Variables:
# Secret name: airflow/variables/etl_config
# Secret value: {"batch_size": 10000, "parallelism": 5}
# In DAG: Variable.get("etl_config", deserialize_json=True)
```

**Configuration to enable Secrets Manager backend:**
```python
AirflowConfigurationOptions={
    'secrets.backend': 'airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend',
    'secrets.backend_kwargs': '{"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}'
}
```

---

## Monitoring MWAA

| What to Monitor | How | Alert On |
|----------------|-----|----------|
| DAG failures | CloudWatch metric: `DagProcessingImportErrors` | > 0 |
| Task failures | Airflow UI or `TaskInstanceFailures` metric | Anomaly detection |
| Scheduler heartbeat | `SchedulerHeartbeat` metric | Missing for > 60s |
| Worker count | `RunningTasks` metric | Consistently at max (need more workers) |
| Queue depth | `QueuedTasks` metric | > 50 for > 5 minutes |
| Environment health | `EnvironmentHealth` metric | Not "HEALTHY" |

---

## Interview Tips

> **Tip 1:** "How do you deploy DAGs to MWAA?" — "Upload Python files to the S3 dags/ folder. MWAA picks them up within 30-60 seconds. For dependencies: update requirements.txt in S3 and trigger an environment update (takes 10-20 minutes). For CI/CD: use GitHub Actions or CodePipeline to sync the dags/ folder on every merge to main."

> **Tip 2:** "MWAA is expensive for small workloads — alternatives?" — "MWAA minimum is ~$500/month (environment always on). For small/occasional pipelines: use Step Functions (pay per execution, $0 idle). For cost-optimized Airflow: self-host on EKS with KubernetesExecutor (pods scale to zero when idle). MWAA is best when you have 10+ daily DAGs and want zero operational burden."

> **Tip 3:** "How do you handle MWAA environment updates without downtime?" — "Requirements.txt changes trigger a rolling update (workers replaced one at a time — minimal disruption). DAG file changes are picked up live (no environment update needed). For major version upgrades: create a new environment, test DAGs, then switch DNS/update triggers. MWAA supports blue-green with parallel environments."
