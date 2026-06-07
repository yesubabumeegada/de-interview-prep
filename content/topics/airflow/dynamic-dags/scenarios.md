---
title: "Airflow Dynamic DAGs - Scenario Questions"
topic: airflow
subtopic: dynamic-dags
content_type: scenario_question
tags: [airflow, dynamic-dags, task-mapping, expand, config-driven, parse-performance]
---

# Airflow Dynamic DAGs — Scenario Questions

<article data-difficulty="junior">

## 🟢 Question 1: Create a Dynamic DAG That Processes Multiple Tables in Parallel

You have a list of 5 tables: `['orders', 'customers', 'products', 'inventory', 'returns']`. Write a DAG that creates one parallel task per table, all running after a `start` task and all converging at an `end` task.

<details>
<summary>💡 Hint</summary>

Use a Python loop to generate tasks dynamically, and use list syntax with the `>>` operator to express fan-out and fan-in dependencies.

</details>

<details>
<summary>✅ Solution</summary>

### Loop-Based Dynamic Task Generation

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

TABLES = ['orders', 'customers', 'products', 'inventory', 'returns']

def process_table(table_name: str, **context):
    print(f"Processing {table_name} for {context['ds']}")

with DAG(
    dag_id='parallel_table_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Generate one task per table
    table_tasks = []
    for table in TABLES:
        task = PythonOperator(
            task_id=f'process_{table}',        # unique ID per table
            python_callable=process_table,
            op_kwargs={'table_name': table},   # pass table name as argument
        )
        table_tasks.append(task)

    # Fan-out: start → all table tasks (in parallel)
    start >> table_tasks

    # Fan-in: all table tasks → end
    table_tasks >> end
```

**Key rules for dynamic task generation:**
1. Task IDs must be unique — use an f-string with the table name
2. Build a list of task objects, then use `>>` with the whole list
3. Tasks in a list run in parallel by default
4. All tasks in `table_tasks` must complete before `end` starts (default `all_success`)

**To add a new table:** Add it to `TABLES`. The next DAG parse will add the task automatically.

</details>
</article>

---

<article data-difficulty="junior">

## 🟢 Question 2: What Is the Risk of Reading from a Database Inside a DAG File?

A colleague writes this DAG. What's wrong with it, and how would you fix it?

```python
# my_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

# Fetch list of tables from a database
conn = psycopg2.connect(host='db.company.com', database='config', user='airflow', password='...')
cursor = conn.cursor()
cursor.execute("SELECT table_name FROM pipeline_config WHERE enabled = true")
TABLES = [row[0] for row in cursor.fetchall()]
conn.close()

with DAG('dynamic_dag', ...) as dag:
    for table in TABLES:
        PythonOperator(task_id=f'process_{table}', ...)
```

<details>
<summary>💡 Hint</summary>

Think about how often Airflow parses DAG files, and what happens to the top-level code (outside of functions and the `with DAG` block) during each parse.

</details>

<details>
<summary>✅ Solution</summary>

### The Problem: Top-Level DB Calls Are Executed on Every Parse

Airflow parses DAG files **every 30 seconds** (configurable via `min_file_process_interval`). Any code at the module level (outside functions) runs on every parse.

**Impact:**
- 1 parse per 30s × 120 min/hr × 24 hr/day = **2,880 database connections per day**
- Under 30-DAG deployment: 2,880 × 30 = **86,400 queries per day** to the config DB
- Parse time includes the DB query — slow DB = slow parse = scheduler lag

**Fix 1: Use a JSON/YAML config file (recommended)**

```python
import json

# Filesystem read is ~1000× faster than a DB query
with open('/opt/airflow/dags/config/tables.json') as f:
    config = json.load(f)

TABLES = config['tables']
```

**Fix 2: Use Airflow Variables (cached by scheduler)**

```python
from airflow.models import Variable

# Airflow caches Variable reads — much lighter than a DB query
TABLES = Variable.get('enabled_tables', deserialize_json=True, default_var=[])
```

**Fix 3: Move the DB call into a task function (best for frequently changing data)**

```python
def discover_tables(**context) -> list[str]:
    """This runs at task EXECUTION time, not parse time."""
    conn = psycopg2.connect(...)
    cursor.execute("SELECT table_name FROM pipeline_config WHERE enabled = true")
    return [row[0] for row in cursor.fetchall()]

with DAG('dynamic_dag', ...) as dag:
    discover = PythonOperator(task_id='discover_tables', python_callable=discover_tables)
    
    process = PythonOperator.partial(
        task_id='process_table',
        python_callable=process_fn,
    ).expand(op_kwargs=discover.output.map(lambda t: {'table': t}))
    
    discover >> process
```

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 3: Use Dynamic Task Mapping to Process a Variable-Length List

You have an upstream task that returns the list of S3 files to process — the count varies by day (could be 5 or 500). Write a DAG using `expand()` that processes each file with a separate task instance, limiting concurrency to 20 at a time.

<details>
<summary>💡 Hint</summary>

Use `PythonOperator.partial(...).expand(...)` where the expand argument comes from the upstream task's XCom output. Use `max_active_tis_per_dag` to limit concurrency.

</details>

<details>
<summary>✅ Solution</summary>

### Dynamic Task Mapping with Concurrency Limit

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def list_s3_files(**context) -> list[dict]:
    """
    Returns list of files to process — count varies by day.
    Each dict is the kwargs for one process_file instance.
    """
    import boto3
    s3 = boto3.client('s3')
    
    ds = context['ds']
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket='my-data-bucket', Prefix=f'raw/{ds}/'):
        for obj in page.get('Contents', []):
            files.append({
                'bucket': 'my-data-bucket',
                'key': obj['Key'],
                'size_bytes': obj['Size'],
            })
    
    print(f"Found {len(files)} files to process")
    return files  # ← this list is passed to expand()

def process_s3_file(bucket: str, key: str, size_bytes: int, **context):
    """Process one S3 file — runs once per item from list_s3_files."""
    print(f"Processing s3://{bucket}/{key} ({size_bytes:,} bytes)")
    # ... actual processing logic

with DAG(
    dag_id='dynamic_s3_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Step 1: Discover files — returns a list of dicts
    list_files = PythonOperator(
        task_id='list_s3_files',
        python_callable=list_s3_files,
    )

    # Step 2: Process each file — one task instance per dict in the list
    process = PythonOperator.partial(
        task_id='process_s3_file',
        python_callable=process_s3_file,
        max_active_tis_per_dag=20,    # ← at most 20 instances run at once
        pool='s3_processing_pool',    # ← also cap via pool for cross-DAG control
        pool_slots=1,
        retries=2,
        retry_delay=timedelta(minutes=2),
    ).expand(
        op_kwargs=list_files.output   # ← XCom from list_files — one dict per instance
    )

    list_files >> process
```

**How `expand()` works here:**
- `list_files` returns `[{'bucket': '...', 'key': '...', 'size_bytes': ...}, ...]`
- `expand(op_kwargs=list_files.output)` creates one task instance per item
- Each instance receives its dict as `**kwargs` in `process_s3_file`
- `max_active_tis_per_dag=20` ensures at most 20 instances run simultaneously

**Day 1:** list_files returns 5 files → 5 process instances
**Day 2:** list_files returns 500 files → 500 process instances (at most 20 concurrent)

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 4: Organize Dynamic Tasks with TaskGroups

You're generating 30 tasks (10 tables × 3 operations: extract, transform, load). Without grouping, the Graph view is overwhelming. Redesign using TaskGroups so each table's 3 steps are visually grouped together.

<details>
<summary>💡 Hint</summary>

Use `TaskGroup` as a context manager inside the table loop. Task IDs inside a group are prefixed with the group ID — ensure they remain unique.

</details>

<details>
<summary>✅ Solution</summary>

### TaskGroup Inside a Dynamic Loop

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

TABLES = ['orders', 'customers', 'products', 'inventory', 'returns',
          'refunds', 'campaigns', 'leads', 'sessions', 'events']

def extract_fn(table: str, **ctx): print(f"Extracting {table}")
def transform_fn(table: str, **ctx): print(f"Transforming {table}")
def load_fn(table: str, **ctx): print(f"Loading {table}")

with DAG('grouped_table_pipeline', start_date=datetime(2024, 1, 1), catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    all_table_groups = []

    for table in TABLES:
        # One TaskGroup per table — collapsible in the Graph view
        with TaskGroup(group_id=f'table_{table}',
                       tooltip=f'ETL pipeline for {table}') as table_group:

            extract = PythonOperator(
                task_id='extract',          # full ID: table_orders.extract
                python_callable=extract_fn,
                op_kwargs={'table': table},
            )
            transform = PythonOperator(
                task_id='transform',        # full ID: table_orders.transform
                python_callable=transform_fn,
                op_kwargs={'table': table},
            )
            load = PythonOperator(
                task_id='load',             # full ID: table_orders.load
                python_callable=load_fn,
                op_kwargs={'table': table},
            )

            # Sequential within each table group
            extract >> transform >> load

        all_table_groups.append(table_group)

    # All table groups run in parallel
    start >> all_table_groups >> end
```

**Result in the UI:**
- Graph view shows 10 TaskGroup nodes (not 30 individual tasks)
- Each group is collapsible — click to expand and see extract→transform→load
- Task IDs: `table_orders.extract`, `table_orders.transform`, `table_orders.load`
- No task ID conflicts because the group prefix is unique per table

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Question 5: Debug a Dynamic DAG Parse Performance Problem

Your Airflow scheduler is degraded — DAG parse time for `config_driven_pipeline.py` is 45 seconds (normal is < 1 second). Other DAGs are impacted because the parser is blocked. Diagnose the root cause and fix it without breaking the pipeline's dynamic behavior.

```python
# config_driven_pipeline.py — SLOW VERSION
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Fetch pipeline config from internal config service
response = requests.get('http://config-service.internal/api/pipeline-tables',
                        timeout=30)
config = response.json()
TABLES = config['tables']

for table in TABLES:
    PythonOperator(task_id=f'process_{table}', ...)
```

<details>
<summary>💡 Hint</summary>

Identify all operations that run at module parse time (not inside functions). Consider what happens when the config service is slow or temporarily unavailable. Think about both the performance fix and the reliability fix.

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause Analysis

**Problem 1: HTTP call at parse time**
The `requests.get()` runs at module load time — every 30 seconds. If the config service takes 45 seconds, the DAG file parser times out (default: 50 seconds) or blocks for 45 seconds on every parse cycle.

**Problem 2: Single point of failure**
If the config service is down, the DAG fails to parse → Airflow removes it from the scheduler → existing scheduled runs stop triggering.

**Problem 3: Network latency cascades**
The scheduler processes DAG files serially in each worker. A 45-second parse blocks all other DAG file parsing during that time.

### Fix: Multi-Layer Solution

**Layer 1: Move HTTP call to a task function**

```python
# FIXED: HTTP call is inside a task function — only runs when the task executes
def fetch_config_and_process(table_name: str, **context):
    response = requests.get('http://config-service.internal/api/pipeline-tables')
    config = response.json()
    # ... process table_name
```

**Layer 2: Use a file-based cache with scheduled refresh**

```python
# config_driven_pipeline.py — FIXED VERSION
import json
import os
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Read from local file — filesystem read is <1ms
# File is refreshed by a separate DAG (config_refresh_dag.py)
CONFIG_PATH = Path('/opt/airflow/dags/config/pipeline_tables.json')

def load_config_with_fallback():
    """Load config from file, return empty list if file missing."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {'tables': []}   # safe fallback — parse succeeds with empty DAG

config = load_config_with_fallback()
TABLES = config.get('tables', [])

with DAG(
    dag_id='config_driven_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    for table in TABLES:
        PythonOperator(
            task_id=f'process_{table}',
            python_callable=process_table_fn,
            op_kwargs={'table': table},
        )
```

**Layer 3: Separate config refresh DAG**

```python
# config_refresh_dag.py — runs every 15 minutes to refresh the config file
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

def refresh_config():
    """Fetch latest config from service and write to local file."""
    response = requests.get('http://config-service.internal/api/pipeline-tables',
                           timeout=10)
    response.raise_for_status()
    
    # Atomic write: write to temp file then rename (prevents partial reads)
    tmp_path = '/opt/airflow/dags/config/pipeline_tables.json.tmp'
    final_path = '/opt/airflow/dags/config/pipeline_tables.json'
    
    with open(tmp_path, 'w') as f:
        json.dump(response.json(), f)
    
    os.rename(tmp_path, final_path)    # atomic on most filesystems
    print("Config refreshed successfully")

with DAG(
    dag_id='config_refresh',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/15 * * * *',   # every 15 minutes
    catchup=False,
) as dag:
    PythonOperator(task_id='refresh_pipeline_config', python_callable=refresh_config)
```

**Impact:**

| Metric | Before | After |
|--------|--------|-------|
| Parse time | 45 seconds | < 10ms |
| Config staleness | 0s (real-time) | Up to 15 minutes |
| Failure on config service outage | DAG disappears from scheduler | DAG uses last known config |
| Config service requests/day | 2,880 per DAG file | 96 (once per 15 minutes) |

**Trade-off:** Config is up to 15 minutes stale. For most use cases, this is acceptable. If real-time config is required, use dynamic task mapping where the config is fetched inside a task (runs at execution time, not parse time).

</details>
</article>
