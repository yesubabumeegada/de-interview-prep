---
title: "Airflow DAG Design - Real-World Production Examples"
topic: airflow
subtopic: dag-design
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, dag, production, patterns, etl, data-pipeline]
---

# Airflow DAG Design — Real-World Production Examples

## Pattern 1: Multi-Source Data Warehouse Load

A production DAG that loads from 3 sources, validates, loads to warehouse, and alerts on issues.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': alert_slack,
}

with DAG(
    dag_id='daily_warehouse_load',
    schedule_interval='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['warehouse', 'daily', 'critical'],
) as dag:

    # Phase 1: Wait for source data
    with TaskGroup('wait_for_sources') as wait_group:
        wait_crm = S3KeySensor(
            task_id='wait_crm_export',
            bucket_key='exports/crm/{{ ds }}/customers.parquet',
            bucket_name='source-data',
            timeout=3600, mode='reschedule',
        )
        wait_erp = S3KeySensor(
            task_id='wait_erp_export',
            bucket_key='exports/erp/{{ ds }}/orders.parquet',
            bucket_name='source-data',
            timeout=3600, mode='reschedule',
        )
        wait_web = S3KeySensor(
            task_id='wait_web_events',
            bucket_key='events/web/{{ ds }}/_SUCCESS',
            bucket_name='source-data',
            timeout=7200, mode='reschedule',
        )

    # Phase 2: Extract and stage
    with TaskGroup('extract_and_stage') as extract_group:
        stage_crm = PythonOperator(task_id='stage_crm', python_callable=stage_crm_data)
        stage_erp = PythonOperator(task_id='stage_erp', python_callable=stage_erp_data)
        stage_web = PythonOperator(task_id='stage_web', python_callable=stage_web_data)

    # Phase 3: Validate staged data
    with TaskGroup('validate') as validate_group:
        check_row_counts = PythonOperator(task_id='check_counts', python_callable=validate_counts)
        check_nulls = PythonOperator(task_id='check_nulls', python_callable=validate_nulls)
        check_dupes = PythonOperator(task_id='check_dupes', python_callable=validate_no_duplicates)

    # Phase 4: Load dimensions (must complete before facts)
    with TaskGroup('load_dimensions') as dim_group:
        load_dim_customer = PythonOperator(task_id='dim_customer', python_callable=load_scd2_customer)
        load_dim_product = PythonOperator(task_id='dim_product', python_callable=load_scd1_product)
        load_dim_date = PythonOperator(task_id='dim_date', python_callable=ensure_date_dimension)

    # Phase 5: Load facts (dimensions must be ready)
    with TaskGroup('load_facts') as fact_group:
        load_fact_sales = PythonOperator(task_id='fact_sales', python_callable=load_fact_sales_fn)
        load_fact_events = PythonOperator(task_id='fact_events', python_callable=load_fact_events_fn)

    # Phase 6: Post-load quality checks
    validate_warehouse = PythonOperator(task_id='validate_warehouse', python_callable=run_dbt_tests)

    # Phase 7: Notify success
    notify = PythonOperator(task_id='notify_success', python_callable=send_completion_report)

    # Dependencies
    wait_group >> extract_group >> validate_group >> dim_group >> fact_group >> validate_warehouse >> notify
```

---

## Pattern 2: Retry-Safe Incremental Load with State Tracking

```python
def incremental_load(**context):
    """
    Load data incrementally using a high-water mark.
    Stores state in Airflow Variables (or a control table).
    Safe to retry — re-reads from the same watermark.
    """
    from airflow.models import Variable
    
    table_name = context['params']['table']
    ds = context['ds']
    
    # Get last processed watermark
    var_key = f"hwm_{table_name}"
    last_hwm = Variable.get(var_key, default_var="1970-01-01T00:00:00")
    
    # Extract new data since watermark
    new_data = source_db.query(f"""
        SELECT * FROM {table_name}
        WHERE updated_at > '{last_hwm}'
        AND updated_at <= '{ds}T23:59:59'
    """)
    
    if new_data.empty:
        print(f"No new data for {table_name} since {last_hwm}")
        return
    
    # Load (idempotent: MERGE pattern)
    warehouse.merge(
        target=f"raw.{table_name}",
        source=new_data,
        merge_keys=['id'],
        update_columns=['*'],
    )
    
    # Update watermark ONLY after successful load
    new_hwm = new_data['updated_at'].max().isoformat()
    Variable.set(var_key, new_hwm)
    
    # Push metrics for downstream validation
    context['ti'].xcom_push(key='row_count', value=len(new_data))
    context['ti'].xcom_push(key='new_hwm', value=new_hwm)
```

---

## Pattern 3: dbt Integration

```python
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG('dbt_transformation', schedule_interval='0 8 * * *', ...) as dag:

    # Wait for raw data load to complete
    wait_for_raw = ExternalTaskSensor(
        task_id='wait_for_raw_load',
        external_dag_id='daily_warehouse_load',
        timeout=7200,
        mode='reschedule',
    )
    
    # Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt && dbt run --target prod --select tag:daily',
        env={'DBT_PROFILES_DIR': '/opt/dbt'},
    )
    
    # Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt && dbt test --target prod --select tag:daily',
    )
    
    # Generate dbt docs
    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command='cd /opt/dbt && dbt docs generate --target prod',
        trigger_rule='all_done',  # Generate docs even if tests fail
    )
    
    wait_for_raw >> dbt_run >> dbt_test >> dbt_docs
```

---

## Pattern 4: Data Quality Gate (Stop Pipeline on Bad Data)

```python
def quality_gate(**context):
    """
    Hard gate: stop pipeline if critical quality thresholds are breached.
    Soft gate: alert but continue if non-critical thresholds are breached.
    """
    row_count = context['ti'].xcom_pull(task_ids='extract', key='row_count')
    
    # Hard gate: no data = critical failure (don't load empty results)
    if row_count == 0:
        raise ValueError("CRITICAL: Zero rows extracted. Aborting to prevent data loss.")
    
    # Hard gate: anomaly detection (> 3 standard deviations from average)
    avg_daily_rows = Variable.get("avg_daily_row_count", deserialize_json=True)
    if row_count < avg_daily_rows * 0.1:
        raise ValueError(
            f"CRITICAL: Only {row_count} rows (expected ~{avg_daily_rows}). "
            "Possible upstream outage. Aborting."
        )
    
    # Soft gate: warning but continue
    if row_count < avg_daily_rows * 0.5:
        send_slack_alert(
            f"WARNING: {row_count} rows (50% below average {avg_daily_rows}). "
            "Pipeline continuing but investigate."
        )
    
    # Update running average
    new_avg = int(avg_daily_rows * 0.9 + row_count * 0.1)  # Exponential moving average
    Variable.set("avg_daily_row_count", new_avg)
    
    return "quality_passed"

quality_check = PythonOperator(
    task_id='quality_gate',
    python_callable=quality_gate,
)

extract >> quality_check >> transform >> load
```

---

## Pattern 5: Cleanup and Maintenance DAG

```python
with DAG(
    'maintenance_cleanup',
    schedule_interval='0 3 * * 0',  # Every Sunday at 3 AM
    tags=['maintenance'],
) as dag:

    # Clean up old staging data (keep last 7 days)
    cleanup_staging = PythonOperator(
        task_id='cleanup_staging_tables',
        python_callable=lambda: warehouse.execute("""
            DELETE FROM staging.raw_events WHERE load_date < CURRENT_DATE - 7;
            DELETE FROM staging.raw_orders WHERE load_date < CURRENT_DATE - 7;
        """),
    )
    
    # Clean up old Airflow metadata (task instances, logs)
    cleanup_airflow_meta = BashOperator(
        task_id='cleanup_old_task_instances',
        bash_command='airflow db clean --clean-before-timestamp "$(date -d "90 days ago" +%Y-%m-%d)" --yes',
    )
    
    # Vacuum/analyze warehouse tables for optimal query performance
    vacuum_tables = PythonOperator(
        task_id='vacuum_analyze',
        python_callable=lambda: warehouse.execute("""
            ANALYZE fact_sales;
            ANALYZE fact_events;
            ANALYZE dim_customer;
        """),
    )
    
    # Report on DAG health metrics
    health_report = PythonOperator(
        task_id='generate_health_report',
        python_callable=generate_weekly_pipeline_health_report,
    )
    
    [cleanup_staging, cleanup_airflow_meta, vacuum_tables] >> health_report
```

---

## Production Deployment Checklist

| Category | Check |
|----------|-------|
| Idempotency | Every task safe to re-run (DELETE+INSERT or MERGE) |
| Alerting | on_failure_callback configured for Slack/PagerDuty |
| Timeouts | execution_timeout set per task, dagrun_timeout set per DAG |
| Retries | retries=2-3 with exponential backoff |
| Scheduling | catchup=False, max_active_runs=1 |
| Dependencies | Sensors for external data with timeout |
| Quality | Validation task BEFORE loading to production tables |
| Monitoring | SLA configured for critical paths |
| Secrets | Connections stored in Airflow (not hardcoded) |
| Testing | DAG loads without error, dependencies are correct |

---

## Interview Tips

> **Tip 1:** "Describe a production DAG you've built" — Walk through: sensor waits → extract → validate (quality gate) → dimension load → fact load → post-load tests → notify. Mention idempotency, retries, and alerting. Show you think about failure modes.

> **Tip 2:** "How do you prevent loading corrupt data?" — "Quality gate pattern: after extraction, verify row count (not zero, not anomalously low), check for NULL rates in critical columns, and validate against the previous day's data. If checks fail, the pipeline raises an exception and downstream tasks don't execute."

> **Tip 3:** "How do you test an Airflow DAG?" — "Three levels: (1) Unit tests verify DAG parses without errors and dependencies are correct, (2) Integration tests run individual tasks with test data, (3) End-to-end tests run the full DAG against a staging environment with known inputs and validate outputs."
