---
title: "AWS MWAA - Real-World Production Examples"
topic: aws-services
subtopic: mwaa
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [aws, mwaa, airflow, production, orchestration, cicd]
---

# AWS MWAA — Real-World Production Examples

## Pattern 1: Multi-Service ETL Orchestration

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

with DAG('production_daily_pipeline',
         schedule_interval='0 6 * * *',
         start_date=datetime(2024, 1, 1),
         catchup=False,
         max_active_runs=1,
         default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
         tags=['production', 'daily']) as dag:

    # Phase 1: Wait for upstream data
    with TaskGroup('wait_for_sources') as wait_group:
        wait_crm = S3KeySensor(task_id='wait_crm', bucket_name='sources',
            bucket_key='crm/{{ ds }}/_SUCCESS', timeout=7200, mode='reschedule')
        wait_erp = S3KeySensor(task_id='wait_erp', bucket_name='sources',
            bucket_key='erp/{{ ds }}/_SUCCESS', timeout=7200, mode='reschedule')

    # Phase 2: Extract and transform (Glue)
    with TaskGroup('transform') as transform_group:
        transform_crm = GlueJobOperator(task_id='transform_crm',
            job_name='crm-to-curated', script_args={'--date': '{{ ds }}'},
            wait_for_completion=True)
        transform_erp = GlueJobOperator(task_id='transform_erp',
            job_name='erp-to-curated', script_args={'--date': '{{ ds }}'},
            wait_for_completion=True)

    # Phase 3: Heavy processing (EMR Serverless)
    compute_metrics = EmrServerlessStartJobOperator(
        task_id='compute_daily_metrics',
        application_id='emr-app-id',
        execution_role_arn='arn:aws:iam::123:role/EMRServerlessRole',
        job_driver={'sparkSubmit': {
            'entryPoint': 's3://scripts/compute_metrics.py',
            'entryPointArguments': ['--date', '{{ ds }}'],
        }},
        configuration_overrides={'monitoringConfiguration': {
            's3MonitoringConfiguration': {'logUri': 's3://logs/emr/'}
        }},
        wait_for_completion=True,
    )

    # Phase 4: Data quality (Athena SQL checks)
    quality_check = AthenaOperator(
        task_id='quality_check',
        query="""
            SELECT CASE WHEN 
                (SELECT COUNT(*) FROM curated.fact_orders WHERE dt='{{ ds }}') > 0
                AND (SELECT COUNT(*) FROM curated.fact_orders WHERE dt='{{ ds }}' AND amount IS NULL) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
        """,
        database='curated',
        output_location='s3://athena-results/quality/',
    )

    # Phase 5: Notify
    notify = SnsPublishOperator(
        task_id='notify_success',
        topic_arn='arn:aws:sns:...:pipeline-success',
        message='Daily pipeline completed for {{ ds }}',
    )

    wait_group >> transform_group >> compute_metrics >> quality_check >> notify
```

---

## Pattern 2: Dynamic DAG Generation from Config

```python
# dags/dynamic_table_sync.py
import yaml
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

# Load config from S3 (included in dags/ folder)
with open('/opt/airflow/dags/config/tables.yaml') as f:
    config = yaml.safe_load(f)

# Generate one DAG per table group
for group in config['table_groups']:
    dag_id = f"sync_{group['name']}"
    
    with DAG(dag_id,
             schedule_interval=group.get('schedule', '@daily'),
             start_date=datetime(2024, 1, 1),
             catchup=False,
             tags=['auto-generated', group['name']]) as dag:
        
        previous_task = None
        for table in group['tables']:
            task = GlueJobOperator(
                task_id=f"sync_{table['name']}",
                job_name='generic-table-sync',
                script_args={
                    '--source': table['source'],
                    '--target': table['target'],
                    '--date': '{{ ds }}',
                },
                wait_for_completion=True,
            )
            if previous_task:
                previous_task >> task
            previous_task = task
        
        globals()[dag_id] = dag  # Register DAG
```

**Config file (dags/config/tables.yaml):**
```yaml
table_groups:
  - name: crm
    schedule: "0 6 * * *"
    tables:
      - {name: customers, source: "rds://crm/customers", target: "s3://lake/crm/customers/"}
      - {name: contacts, source: "rds://crm/contacts", target: "s3://lake/crm/contacts/"}
  - name: erp
    schedule: "0 7 * * *"
    tables:
      - {name: orders, source: "rds://erp/orders", target: "s3://lake/erp/orders/"}
      - {name: inventory, source: "rds://erp/inventory", target: "s3://lake/erp/inventory/"}
```

**Result:** Adding a new table = one YAML line. No Python code changes. DAG auto-generates.

---

## Pattern 3: MWAA + Step Functions Hybrid for Complex Workflows

```python
# MWAA orchestrates overall pipeline
# Step Functions handles parallel file processing (Distributed Map)

from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator
)
from airflow.providers.amazon.aws.sensors.step_function import (
    StepFunctionExecutionSensor
)

with DAG('hybrid_pipeline', ...):
    # MWAA: schedule and orchestrate
    prepare = PythonOperator(task_id='prepare_file_list',
        python_callable=list_files_to_process)
    
    # Step Functions: parallel processing (1000 files simultaneously)
    start_sfn = StepFunctionStartExecutionOperator(
        task_id='start_parallel_processing',
        state_machine_arn='arn:aws:states:...:file-processor',
        input=json.dumps({
            'bucket': 'source-data',
            'prefix': 'raw/{{ ds }}/',
            'output_bucket': 'data-lake',
        }),
    )
    
    wait_sfn = StepFunctionExecutionSensor(
        task_id='wait_for_processing',
        execution_arn="{{ task_instance.xcom_pull(task_ids='start_parallel_processing') }}",
        timeout=3600,
    )
    
    # MWAA: post-processing
    validate = PythonOperator(task_id='validate_output', ...)
    
    prepare >> start_sfn >> wait_sfn >> validate
```

**Why hybrid:** MWAA handles scheduling/dependencies/monitoring. Step Functions handles massive parallelism (Map state can run 10,000 concurrent Lambdas processing files). Best of both worlds.

---

## Pattern 4: Observability and Alerting

```python
# dags/utils/alerting.py
import boto3
from airflow.models import Variable

def on_failure_callback(context):
    """Send rich alert to Slack + PagerDuty on task failure."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    log_url = task_instance.log_url
    
    message = {
        'text': f':red_circle: *Pipeline Failure*',
        'blocks': [
            {'type': 'section', 'text': {'type': 'mrkdwn', 
             'text': f'*DAG:* `{dag_id}`\n*Task:* `{task_id}`\n*Date:* {execution_date}\n*Error:* {str(exception)[:200]}'}},
            {'type': 'actions', 'elements': [
                {'type': 'button', 'text': {'type': 'plain_text', 'text': 'View Logs'},
                 'url': log_url}
            ]}
        ]
    }
    
    # Send to Slack via webhook
    slack_url = Variable.get('slack_webhook_url')
    requests.post(slack_url, json=message)
    
    # If critical DAG: also page on-call
    critical_dags = Variable.get('critical_dags', deserialize_json=True, default_var=[])
    if dag_id in critical_dags:
        sns = boto3.client('sns')
        sns.publish(
            TopicArn=Variable.get('pagerduty_topic_arn'),
            Subject=f'CRITICAL: {dag_id}.{task_id} failed',
            Message=f'Error: {exception}\nLogs: {log_url}'
        )

# Apply to all DAGs via default_args:
default_args = {
    'on_failure_callback': on_failure_callback,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

---

## Production Operations Checklist

| Task | Frequency | How |
|------|-----------|-----|
| DAG deployment | Per merge (CI/CD) | S3 sync via GitHub Actions |
| Requirements update | As needed (test first!) | Update S3 + environment update |
| Monitor environment health | Continuous | CloudWatch alarm on EnvironmentHealth |
| Review failed DAG runs | Daily | Airflow UI or CloudWatch metrics |
| Capacity review | Monthly | Check MaxWorkers vs actual peak workers |
| Airflow version upgrade | Quarterly | Create new environment, test, swap |
| Cost review | Monthly | CloudWatch MWAA metrics + billing |
| Secret rotation | Quarterly | Update in Secrets Manager (auto-discovered) |
| Backup DAG code | Always | It's in Git! S3 is just the deployment target |
| DR readiness test | Quarterly | Create environment in DR region from replicated S3 |

---

## Interview Tips

> **Tip 1:** "Describe your MWAA production setup" — "DAGs in Git, CI/CD syncs to S3 on merge (with DAG import tests as gate). mw1.medium environment with 1-10 auto-scaling workers. Connections stored in Secrets Manager (not Airflow UI). Slack alerts on failure via on_failure_callback. Separate dev/staging/prod environments with the same DAG code but different configs. ~30 DAGs running daily, mostly triggering Glue and EMR Serverless jobs."

> **Tip 2:** "How do you handle MWAA environment updates?" — "Two types: (1) DAG code changes → just sync to S3 (picked up in 60s, zero downtime). (2) Requirements/plugins changes → environment update (20 min rolling update, workers replaced one at a time, minimal disruption). For major Airflow version upgrades: create a parallel new environment, run both in parallel for a week, then switch traffic."

> **Tip 3:** "How do you test DAGs before deploying to production?" — "Three levels: (1) Local: run `python dags/my_dag.py` to verify imports work. (2) CI: pytest runs DAG structure tests (correct dependencies, no cycles, required tags). (3) Staging MWAA: trigger DAGs against test data and validate output. Only after all three pass does the DAG deploy to production via the CI/CD pipeline."
