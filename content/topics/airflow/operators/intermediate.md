---
title: "Airflow Operators - Intermediate"
topic: airflow
subtopic: operators
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, operators, provider-operators, snowflake, bigquery, s3, kubernetes, taskflow]
---

# Airflow Operators — Intermediate

## Provider Operators — The Real Power

Airflow's core ships with basic operators. The **provider packages** (`apache-airflow-providers-*`) add hundreds of purpose-built operators for every major data system.

```bash
pip install apache-airflow-providers-snowflake
pip install apache-airflow-providers-google
pip install apache-airflow-providers-amazon
pip install apache-airflow-providers-databricks
```

---

## Snowflake Operators

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

# Run any SQL in Snowflake
run_merge = SnowflakeOperator(
    task_id='merge_orders',
    snowflake_conn_id='snowflake_default',   # Connection defined in Airflow UI
    sql="""
        MERGE INTO warehouse.orders AS target
        USING staging.orders_staging AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET
            target.status = source.status,
            target.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount, status)
        VALUES (source.order_id, source.customer_id, source.amount, source.status);
    """,
    warehouse='TRANSFORM_WH',
    database='ANALYTICS',
    schema='WAREHOUSE',
)

# Load from S3 to Snowflake via COPY INTO
load_from_s3 = S3ToSnowflakeOperator(
    task_id='load_raw_orders',
    s3_keys=['raw/orders/{{ ds }}/orders.parquet'],
    table='raw_orders',
    schema='STAGING',
    stage='my_s3_stage',
    file_format='(TYPE=PARQUET)',
    snowflake_conn_id='snowflake_default',
)
```

---

## BigQuery Operators

```python
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryCheckOperator,
)

# Run a SQL job in BigQuery
run_query = BigQueryInsertJobOperator(
    task_id='transform_events',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": """
                INSERT INTO `project.dataset.events_clean`
                SELECT
                    event_id,
                    user_id,
                    event_type,
                    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS event_hour
                FROM `project.dataset.events_raw`
                WHERE DATE(event_timestamp) = '{{ ds }}'
                  AND event_type IS NOT NULL
            """,
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
        }
    },
    location='US',
)

# Quality check — fails if query returns 0 rows
check_data = BigQueryCheckOperator(
    task_id='check_events_loaded',
    sql="SELECT COUNT(*) FROM `project.dataset.events_clean` WHERE DATE(event_hour) = '{{ ds }}'",
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default',
)
```

---

## AWS Operators (Amazon Provider)

```python
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Run a Glue ETL job
run_glue = GlueJobOperator(
    task_id='run_glue_etl',
    job_name='my_etl_job',
    aws_conn_id='aws_default',
    script_args={'--execution_date': '{{ ds }}'},
    num_of_dpus=10,
    wait_for_completion=True,
)

# Load from S3 to Redshift
load_redshift = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema='public',
    table='orders',
    s3_bucket='my-data-lake',
    s3_key='processed/orders/{{ ds }}/',
    copy_options=['FORMAT AS PARQUET'],
    aws_conn_id='aws_default',
    redshift_conn_id='redshift_default',
)
```

---

## KubernetesPodOperator — Run Containerised Work

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

run_spark = KubernetesPodOperator(
    task_id='run_spark_job',
    name='spark-orders-{{ ds_nodash }}',
    namespace='data-engineering',
    image='my-registry/spark-etl:latest',
    image_pull_policy='Always',
    cmds=['spark-submit'],
    arguments=[
        '--master', 'k8s://https://kubernetes.default',
        '--py-files', '/app/jobs/orders.py',
        '--execution-date', '{{ ds }}',
    ],
    env_vars={
        'EXECUTION_DATE': '{{ ds }}',
        'S3_BUCKET': 'my-data-lake',
    },
    resources=k8s.V1ResourceRequirements(
        requests={'cpu': '2', 'memory': '4Gi'},
        limits={'cpu': '4', 'memory': '8Gi'},
    ),
    is_delete_operator_pod=True,    # Clean up pod after completion
    get_logs=True,                  # Stream pod logs to Airflow
    in_cluster=True,                # Use the cluster Airflow runs in
)
```

---

## TaskFlow API (@task decorator) — Cleaner PythonOperator

Airflow 2.0+ introduced the `@task` decorator as a cleaner alternative to `PythonOperator`:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='taskflow_example',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def sales_pipeline():

    @task
    def extract(execution_date=None):
        print(f"Extracting for {execution_date}")
        return {"rows": 15000, "source": "api"}

    @task
    def transform(raw_data: dict):
        print(f"Transforming {raw_data['rows']} rows")
        return {"rows_loaded": raw_data["rows"]}

    @task
    def load(transformed: dict):
        print(f"Loading {transformed['rows_loaded']} rows to warehouse")

    # XCom passing is automatic — return values flow between tasks
    raw   = extract()
    clean = transform(raw)
    load(clean)

dag = sales_pipeline()
```

**Traditional vs TaskFlow:**

| Aspect | Traditional | TaskFlow (@task) |
|--------|-------------|-----------------|
| Boilerplate | More (operator instantiation) | Less (just decorate a function) |
| XCom passing | Manual (`xcom_push`/`xcom_pull`) | Automatic (return values) |
| Type hints | Not enforced | Supported |
| Readability | Medium | High |
| Mixing with other operators | Easy | Needs `.operator` for non-Python tasks |

---

## Operator Retries and Timeouts

```python
from datetime import timedelta

PythonOperator(
    task_id='flaky_api_call',
    python_callable=call_external_api,
    retries=3,                          # Retry up to 3 times
    retry_delay=timedelta(minutes=5),   # Wait 5 min between retries
    retry_exponential_backoff=True,     # 5m, 10m, 20m (doubles each time)
    max_retry_delay=timedelta(hours=1), # Cap at 1 hour
    execution_timeout=timedelta(hours=2), # Kill task if it runs > 2h
    on_failure_callback=alert_slack,    # Custom failure hook
    on_retry_callback=log_retry,        # Custom retry hook
)
```

---

## Interview Tips

> **Tip 1:** For any production Airflow setup, almost every task should use a provider operator rather than `BashOperator` calling a CLI. Provider operators handle authentication via Connections, structured logging, and proper error handling automatically.

> **Tip 2:** `KubernetesPodOperator` is the preferred pattern for heavy compute in cloud-native Airflow (MWAA, Cloud Composer, Astronomer). Each task gets its own isolated container with its own resource limits — no shared worker memory contention.

> **Tip 3:** The TaskFlow API is the modern Airflow 2.x way for Python tasks. Use it for new DAGs. But for tasks talking to external systems (Snowflake, BigQuery, S3), prefer the purpose-built provider operators — they're more battle-tested and have better observability.
