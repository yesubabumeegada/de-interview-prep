---
title: "Airflow DAG Design - Scenario Questions"
topic: airflow
subtopic: dag-design
content_type: scenario_question
tags: [airflow, dag, interview, scenarios, orchestration]
---

# Scenario Questions — Airflow DAG Design

<article data-difficulty="junior">

## 🟢 Junior: Fix the Duplicate Data Problem

**Scenario:** Your team deployed a daily DAG that loads sales data. After a network error caused Task 2 (load) to fail, someone manually retried it. Now the target table has duplicate records for that day. What went wrong and how do you fix the DAG design?

<details>
<summary>✅ Solution</summary>

**What went wrong:** The load task uses `INSERT INTO` (append). When retried, it inserts the same data again without checking if it already exists.

**Fix — Make it idempotent:**

```python
def load_sales(**context):
    ds = context['ds']
    
    # Option 1: DELETE + INSERT (simple, works everywhere)
    warehouse.execute(f"DELETE FROM fact_sales WHERE sale_date = '{ds}'")
    warehouse.execute(f"""
        INSERT INTO fact_sales
        SELECT * FROM staging.raw_sales WHERE sale_date = '{ds}'
    """)
    
    # Option 2: MERGE/Upsert (better for partial failures)
    warehouse.execute(f"""
        MERGE INTO fact_sales t
        USING staging.raw_sales s ON t.sale_id = s.sale_id
        WHEN MATCHED THEN UPDATE SET amount = s.amount, updated_at = NOW()
        WHEN NOT MATCHED THEN INSERT (sale_id, sale_date, amount) 
            VALUES (s.sale_id, s.sale_date, s.amount)
    """)
```

**Key principle:** Every task must produce the same result regardless of how many times it's run for the same date. This is idempotency.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Why Is My DAG Not Running?

**Scenario:** You deployed a new DAG with `start_date=datetime(2024, 6, 1)` and `schedule_interval='@daily'`. Today is June 2, but the DAG hasn't triggered. What are possible causes?

<details>
<summary>✅ Solution</summary>

**Possible causes (check in this order):**

1. **DAG is paused:** New DAGs are paused by default. Toggle it ON in the Airflow UI.

2. **Catchup behavior:** With `schedule_interval='@daily'` and start_date=June 1, the first run triggers AFTER the first interval completes (June 2 at midnight). If it's still early on June 2, it hasn't fired yet.

3. **DAG file not parsed:** Check if the file is in the correct `dags/` folder and the scheduler has picked it up (check "Last Parsed" in the UI).

4. **Python syntax error:** If the DAG file has an import error or syntax issue, it won't appear in the UI. Check scheduler logs.

5. **`catchup=False` with past start_date:** With catchup disabled, only the MOST RECENT interval triggers. If start_date is far in the past, it only runs for the latest eligible interval.

**Debugging commands:**
```bash
# Check if DAG is recognized
airflow dags list | grep my_dag_id

# Test parsing
airflow dags test my_dag_id 2024-06-01

# Check for import errors
airflow dags list-import-errors
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a DAG with External Dependencies

**Scenario:** Your pipeline needs to:
1. Wait for a partner's file drop to S3 (arrives between 2-6 AM, unpredictable)
2. Validate the file format and row count
3. Process and load to warehouse
4. If the file doesn't arrive by 8 AM, send an alert and skip this day's run

Design the DAG structure with appropriate timeout handling.

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime, timedelta

def handle_sensor_timeout(context):
    """Called when sensor times out — alert and skip."""
    send_alert(f"Partner file not received by 8 AM for {context['ds']}")

with DAG(
    'partner_file_ingestion',
    schedule_interval='0 2 * * *',  # Start checking at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Wait for file (timeout at 8 AM = 6 hours after 2 AM start)
    wait_for_file = S3KeySensor(
        task_id='wait_for_partner_file',
        bucket_name='partner-drops',
        bucket_key='daily/{{ ds }}/data.csv',
        timeout=6 * 3600,           # 6 hours (until 8 AM)
        poke_interval=300,          # Check every 5 minutes
        mode='reschedule',
        soft_fail=True,             # Don't mark DAG as failed on timeout — mark as skipped
        on_failure_callback=handle_sensor_timeout,
    )
    
    # Validate file structure
    validate = PythonOperator(
        task_id='validate_file',
        python_callable=validate_partner_file,
    )
    
    # Process and load
    process = PythonOperator(
        task_id='process_and_load',
        python_callable=process_file,
    )
    
    # Success notification
    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=lambda: send_slack("Partner file loaded successfully"),
    )
    
    wait_for_file >> validate >> process >> notify_success
```

**Key design decisions:**
- `soft_fail=True`: sensor timeout marks task as SKIPPED (not FAILED) — downstream tasks skip gracefully
- `mode='reschedule'`: releases worker slot between checks (important for 6-hour wait)
- `on_failure_callback`: sends alert so team knows the file is missing
- `poke_interval=300`: check every 5 minutes (not too aggressive)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Parallelize a Slow Sequential DAG

**Scenario:** Your DAG loads 20 tables sequentially (extract_table_1 → extract_table_2 → ... → extract_table_20 → validate → load). It takes 4 hours. The tables are independent — they don't depend on each other. Redesign for maximum parallelism while respecting a database connection limit of 5 concurrent queries.

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

TABLES = [
    'customers', 'orders', 'products', 'payments', 'shipments',
    'returns', 'reviews', 'inventory', 'suppliers', 'categories',
    'promotions', 'warehouses', 'employees', 'regions', 'campaigns',
    'subscriptions', 'invoices', 'credit_notes', 'addresses', 'preferences',
]

with DAG(
    'parallel_table_extract',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=10,  # Max parallel tasks in this DAG
) as dag:

    # Create a pool to limit concurrent DB connections
    # (Set up in Admin > Pools: 'source_db_pool' with 5 slots)
    
    # Phase 1: Extract all tables in parallel (limited by pool)
    with TaskGroup('extract_tables') as extract_group:
        extract_tasks = []
        for table in TABLES:
            task = PythonOperator(
                task_id=f'extract_{table}',
                python_callable=extract_table,
                op_kwargs={'table': table},
                pool='source_db_pool',  # Limits to 5 concurrent extracts
            )
            extract_tasks.append(task)
    
    # Phase 2: Validate all extracts completed successfully
    validate = PythonOperator(
        task_id='validate_all_extracts',
        python_callable=validate_all_tables,
        op_kwargs={'tables': TABLES},
    )
    
    # Phase 3: Load to warehouse (can also be parallelized)
    with TaskGroup('load_tables') as load_group:
        for table in TABLES:
            PythonOperator(
                task_id=f'load_{table}',
                python_callable=load_table,
                op_kwargs={'table': table},
                pool='warehouse_pool',  # Separate pool for warehouse connections
            )
    
    extract_group >> validate >> load_group
```

**Key optimizations:**
- **Pools** (`source_db_pool` with 5 slots): Airflow limits concurrent extract tasks to 5, preventing source DB overload
- **TaskGroup:** 20 parallel tasks collapse into one visual box in the UI
- **Separate pools** for source and target: extract and load can use different connection limits

**Expected improvement:** 20 tables × 12 min each sequential = 4 hours. With 5 parallel slots: ~48 minutes (5x faster).

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Self-Healing Pipeline

**Scenario:** Your critical daily pipeline processes 50 source tables. Occasionally 1-2 tables fail due to transient source system issues. Currently, the entire pipeline fails and requires manual intervention. Design a DAG that:
1. Continues processing other tables even if some fail
2. Automatically retries failed tables after 30 minutes
3. Alerts only if a table fails 3 times consecutively
4. Generates a daily report of which tables succeeded/failed

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import json

TABLES = [...]  # 50 tables

def process_table_with_tracking(table, **context):
    """Process a single table with failure tracking."""
    failure_key = f"consecutive_failures_{table}"
    
    try:
        extract_transform_load(table, context['ds'])
        
        # Reset failure counter on success
        Variable.set(failure_key, "0")
        context['ti'].xcom_push(key='status', value='success')
        
    except Exception as e:
        # Increment failure counter
        failures = int(Variable.get(failure_key, default_var="0")) + 1
        Variable.set(failure_key, str(failures))
        
        # Alert only if 3+ consecutive failures
        if failures >= 3:
            send_pagerduty(
                f"CRITICAL: {table} has failed {failures} consecutive runs. "
                f"Error: {str(e)[:200]}"
            )
        
        context['ti'].xcom_push(key='status', value='failed')
        context['ti'].xcom_push(key='error', value=str(e)[:500])
        
        # Don't raise — let other tasks continue
        # The task succeeds (from Airflow's perspective) but records the failure
        # Alternatively, use soft_fail or custom trigger rules

def generate_daily_report(**context):
    """Summarize success/failure across all tables."""
    report = {"date": context['ds'], "succeeded": [], "failed": []}
    
    for table in TABLES:
        status = context['ti'].xcom_pull(
            task_ids=f'process_tables.process_{table}', key='status'
        )
        if status == 'success':
            report['succeeded'].append(table)
        else:
            error = context['ti'].xcom_pull(
                task_ids=f'process_tables.process_{table}', key='error'
            )
            report['failed'].append({'table': table, 'error': error})
    
    # Send report
    total = len(TABLES)
    success_count = len(report['succeeded'])
    fail_count = len(report['failed'])
    
    send_slack(
        f"Daily Pipeline Report ({context['ds']}): "
        f"{success_count}/{total} succeeded, {fail_count} failed.\n"
        f"Failed tables: {[f['table'] for f in report['failed']]}"
    )
    
    # Store for trend analysis
    Variable.set(f"pipeline_report_{context['ds']}", json.dumps(report))

with DAG(
    'self_healing_multi_table',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=15),
        'retry_exponential_backoff': True,
    },
) as dag:

    with TaskGroup('process_tables') as process_group:
        for table in TABLES:
            PythonOperator(
                task_id=f'process_{table}',
                python_callable=process_table_with_tracking,
                op_kwargs={'table': table},
                pool='source_db_pool',
            )
    
    report = PythonOperator(
        task_id='daily_report',
        python_callable=generate_daily_report,
        trigger_rule='all_done',  # Run even if some tasks failed
    )
    
    process_group >> report
```

**Key design decisions:**
- Tasks don't raise exceptions (they record failures internally) — other tasks continue unblocked
- `trigger_rule='all_done'` on report task — generates report regardless of individual failures
- Consecutive failure tracking via Airflow Variables — alerts escalate only after repeated failures (avoids alert fatigue)
- Each task has its own retry logic (2 retries with backoff) before recording as failed

</details>

</article>
