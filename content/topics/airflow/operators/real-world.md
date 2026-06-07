---
title: "Airflow Operators - Real-World Scenarios"
topic: airflow
subtopic: operators
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, operators, production, snowflake, dbt, kubernetes, data-pipeline]
---

# Airflow Operators — Real-World Scenarios

## Scenario 1: Multi-Step ELT Pipeline with Provider Operators

A fintech company runs a nightly ELT pipeline: extract from PostgreSQL (transactional DB), stage to S3, copy to Snowflake, run dbt transformations, validate row counts, then notify on Slack.

```python
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'on_failure_callback': lambda ctx: send_pagerduty_alert(ctx),
}

@dag(
    dag_id='nightly_fintech_elt',
    default_args=default_args,
    schedule='0 2 * * *',        # 2 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fintech', 'nightly', 'elt'],
)
def nightly_elt():

    # Step 1: Extract from Postgres to S3
    extract_transactions = SqlToS3Operator(
        task_id='extract_transactions_to_s3',
        sql="""
            SELECT tx_id, account_id, amount, currency, tx_type, created_at
            FROM transactions
            WHERE DATE(created_at) = '{{ ds }}'
              AND status = 'settled'
        """,
        s3_bucket='fintech-data-lake',
        s3_key='raw/transactions/{{ ds }}/transactions.parquet',
        file_format='parquet',
        sql_conn_id='postgres_prod',
        aws_conn_id='aws_default',
        replace=True,
    )

    # Step 2: Copy from S3 to Snowflake staging
    load_to_snowflake_staging = S3ToSnowflakeOperator(
        task_id='load_staging_transactions',
        s3_keys=['raw/transactions/{{ ds }}/transactions.parquet'],
        table='transactions_staging',
        schema='STAGING',
        stage='fintech_s3_stage',
        file_format='(TYPE=PARQUET)',
        truncate_table=True,
        snowflake_conn_id='snowflake_default',
    )

    # Step 3: Merge staging into production table
    merge_to_production = SnowflakeOperator(
        task_id='merge_transactions_to_prod',
        snowflake_conn_id='snowflake_default',
        sql="""
            MERGE INTO warehouse.transactions AS t
            USING staging.transactions_staging AS s
            ON t.tx_id = s.tx_id
            WHEN MATCHED AND t.updated_at < s.created_at THEN UPDATE SET
                t.amount   = s.amount,
                t.status   = 'settled',
                t.updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (tx_id, account_id, amount, currency, tx_type, created_at)
            VALUES
                (s.tx_id, s.account_id, s.amount, s.currency, s.tx_type, s.created_at);
        """,
        warehouse='TRANSFORM_WH',
        database='ANALYTICS',
    )

    # Step 4: Row count validation
    @task
    def validate_row_counts(ds=None):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        result = hook.get_first(
            f"SELECT COUNT(*) FROM staging.transactions_staging WHERE DATE(created_at) = '{ds}'"
        )
        count = result[0]
        if count == 0:
            raise ValueError(f"No transactions loaded for {ds} — aborting pipeline")
        logging.info(f"Validated {count} transactions for {ds}")
        return count

    # Step 5: Notify success
    notify_slack = SlackWebhookOperator(
        task_id='notify_success',
        slack_webhook_conn_id='slack_data_alerts',
        message='✅ Nightly ELT complete for {{ ds }} — {{ ti.xcom_pull(task_ids="validate_row_counts") }} transactions loaded',
    )

    (
        extract_transactions
        >> load_to_snowflake_staging
        >> merge_to_production
        >> validate_row_counts()
        >> notify_slack
    )

dag = nightly_elt()
```

---

## Scenario 2: Kubernetes-Based Spark Pipeline with Dynamic Configuration

A media company runs Spark jobs on Kubernetes for large-scale log processing. Different job sizes need different resource profiles based on the day's data volume.

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from kubernetes.client import models as k8s
from datetime import datetime

def estimate_data_volume(**context):
    """Check S3 to estimate today's log volume and choose job size."""
    import boto3
    s3 = boto3.client('s3')
    prefix = f"logs/raw/{context['ds']}/"
    response = s3.list_objects_v2(Bucket='media-datalake', Prefix=prefix)
    total_bytes = sum(obj['Size'] for obj in response.get('Contents', []))
    gb = total_bytes / (1024**3)
    context['ti'].xcom_push(key='data_gb', value=gb)
    if gb > 100:
        return 'spark_job_large'
    elif gb > 20:
        return 'spark_job_medium'
    return 'spark_job_small'

def make_spark_task(task_id, cpu, memory, driver_memory):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"spark-logs-{{{{ ds_nodash }}}}-{task_id}",
        namespace='data-engineering',
        image='media-registry/spark-log-processor:3.5.1',
        cmds=['spark-submit'],
        arguments=[
            '--master', 'k8s://https://kubernetes.default',
            '--deploy-mode', 'cluster',
            '--conf', f'spark.executor.instances={cpu // 2}',
            '--conf', f'spark.executor.memory={memory}',
            '--conf', f'spark.driver.memory={driver_memory}',
            '/app/jobs/process_logs.py',
            '--date', '{{ ds }}',
            '--output', 's3://media-datalake/processed/logs/{{ ds }}/',
        ],
        resources=k8s.V1ResourceRequirements(
            requests={'cpu': str(cpu), 'memory': memory},
            limits={'cpu': str(cpu * 2), 'memory': memory},
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        retries=1,
    )

with DAG(
    dag_id='media_log_processing',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    estimate = BranchPythonOperator(
        task_id='estimate_volume',
        python_callable=estimate_data_volume,
    )

    spark_small  = make_spark_task('spark_job_small',  cpu=4,  memory='8Gi',  driver_memory='2Gi')
    spark_medium = make_spark_task('spark_job_medium', cpu=16, memory='32Gi', driver_memory='4Gi')
    spark_large  = make_spark_task('spark_job_large',  cpu=64, memory='128Gi', driver_memory='8Gi')

    catalog_update = SnowflakeOperator(
        task_id='update_data_catalog',
        snowflake_conn_id='snowflake_default',
        sql="CALL update_log_partition_metadata('{{ ds }}')",
        trigger_rule='one_success',  # Run after whichever Spark task ran
    )

    estimate >> [spark_small, spark_medium, spark_large]
    [spark_small, spark_medium, spark_large] >> catalog_update
```

---

## Scenario 3: dbt Orchestration with Granular Task-Level Visibility

Rather than running dbt as a single BashOperator (one opaque task), this pattern creates one Airflow task per dbt model for full observability.

```python
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import json, subprocess

def get_dbt_manifest():
    """Parse dbt manifest.json to discover model dependencies."""
    with open('/opt/dbt/target/manifest.json') as f:
        return json.load(f)

def build_dbt_dag(dag):
    manifest = get_dbt_manifest()
    tasks = {}

    # Create one BashOperator per dbt model
    for node_name, node in manifest['nodes'].items():
        if node['resource_type'] != 'model':
            continue
        model = node['name']
        tasks[node_name] = BashOperator(
            task_id=f"dbt_{model}",
            bash_command=(
                f"cd /opt/dbt && dbt run "
                f"--models {model} "
                f"--profiles-dir /opt/dbt "
                f"--vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
            ),
            dag=dag,
        )

    # Wire dependencies from manifest
    for node_name, node in manifest['nodes'].items():
        if node['resource_type'] != 'model':
            continue
        for dep in node.get('depends_on', {}).get('nodes', []):
            if dep in tasks and node_name in tasks:
                tasks[dep] >> tasks[node_name]

    return tasks

with DAG(
    dag_id='dbt_full_refresh',
    schedule='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    task_map = build_dbt_dag(dag)
    # Attach all root models (no upstream dbt deps) to start
    roots = [t for name, t in task_map.items()
             if not manifest['nodes'][name].get('depends_on', {}).get('nodes')]
    start >> roots
```

This gives you full per-model task state, per-model retry, and per-model logs in the Airflow UI — far better observability than a single `dbt run` BashOperator.
