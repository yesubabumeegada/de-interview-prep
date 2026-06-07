---
title: "Airflow Scheduler Tuning - Intermediate"
topic: airflow
subtopic: scheduler-tuning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, scheduler, performance, parsing, executor, high-availability]
---

# Airflow Scheduler Tuning — Intermediate

## Profiling Scheduler Performance

Before tuning, measure what's actually slow:

```bash
# Check how long each DAG file takes to parse
airflow dags report

# View scheduler logs for parsing durations
grep "DAG File Processing Stats" /opt/airflow/logs/scheduler/latest/*.log

# Check task scheduling lag via metadata DB
SELECT
    dag_id,
    AVG(EXTRACT(EPOCH FROM (queued_dttm - scheduled_dttm))) AS avg_scheduling_lag_sec,
    MAX(EXTRACT(EPOCH FROM (queued_dttm - scheduled_dttm))) AS max_scheduling_lag_sec
FROM task_instance
WHERE scheduled_dttm > NOW() - INTERVAL '1 day'
  AND queued_dttm IS NOT NULL
GROUP BY dag_id
ORDER BY avg_scheduling_lag_sec DESC;
```

---

## Optimising DAG File Structure

### Use `.airflowignore`

```text
# .airflowignore — placed in your DAGs folder
# Uses .gitignore syntax
archive/
temp_*.py
__pycache__/
*.pyc
experimental/
```

Excluded files are never parsed — direct reduction in parsing load.

### Separate utility code from DAG files

```
dags/
├── utils/          # helper functions, NOT imported at module level in DAGs
│   ├── __init__.py
│   └── db_helpers.py
├── daily_elt.py    # DAG file — minimal imports
└── weekly_report.py
```

```python
# ✅ DAG file imports only what Airflow needs at parse time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_etl(**context):
    # Heavy imports inside the function — not at module level
    from utils.db_helpers import get_connection
    import pandas as pd
    conn = get_connection()
    # ...

with DAG('daily_elt', schedule='@daily', start_date=datetime(2024,1,1)) as dag:
    PythonOperator(task_id='etl', python_callable=run_etl)
```

---

## Executor-Specific Tuning

### LocalExecutor

```ini
[core]
executor = LocalExecutor

# How many tasks can run in parallel (local processes)
parallelism = 16
```

Good for single-machine setups. Does not scale beyond one node.

### CeleryExecutor

```ini
[celery]
worker_concurrency = 16          # Tasks per worker process
worker_prefetch_multiplier = 1   # Don't prefetch (avoids starvation)
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow

[core]
parallelism = 256                # Higher — many workers across many machines
```

```bash
# Scale workers independently of scheduler
airflow celery worker --concurrency 32
```

### KubernetesExecutor

```ini
[kubernetes]
worker_pods_creation_batch_size = 16   # Pods created per scheduler loop
worker_container_repository = my-registry/airflow
worker_container_tag = 2.8.1
delete_worker_pods = True
delete_worker_pods_on_failure = False   # Keep failed pods for debugging
```

---

## High-Availability Schedulers (Airflow 2.0+)

Airflow 2.0+ supports **multiple schedulers** running simultaneously using optimistic locking in the metadata DB:

```bash
# Run 2 schedulers pointing at same metadata DB
# Scheduler 1
airflow scheduler

# Scheduler 2 (different machine or container)
airflow scheduler
```

```mermaid
flowchart TD
    S1["Scheduler 1"] --> DB[("Metadata DB")]
    S2["Scheduler 2"] --> DB
    S3["Scheduler 3"] --> DB
    DB --> EX["Executor / Workers"]
```

**How it avoids duplicates:** schedulers use `SELECT ... FOR UPDATE SKIP LOCKED` on task rows. Only one scheduler "wins" a given task.

**When to use HA schedulers:**
- > 500 DAGs
- Task scheduling lag > 30 seconds despite tuning
- Scheduler becomes a single point of failure in prod

---

## DAG Serialization (Airflow 2.0+)

DAG serialization stores parsed DAG structure in the metadata DB so the webserver doesn't need to parse DAG files:

```ini
[core]
store_dag_code = True
min_serialized_dag_update_interval = 30    # seconds between re-serialization
min_serialized_dag_fetch_interval = 10     # webserver fetch interval
```

**Benefits:**
- Webserver no longer needs access to DAG files
- DAG file parsing only needed by scheduler processes
- Faster UI load times

---

## Metadata Database Tuning

The scheduler queries the metadata DB constantly. A slow DB = slow scheduler.

```sql
-- Critical indexes (usually auto-created, but verify)
CREATE INDEX IF NOT EXISTS idx_ti_dag_state
    ON task_instance (dag_id, state);

CREATE INDEX IF NOT EXISTS idx_dagrun_dag_state
    ON dag_run (dag_id, state);

-- Check for slow scheduler queries
SELECT query, calls, mean_exec_time
FROM pg_stat_statements
WHERE query ILIKE '%task_instance%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

```ini
# PostgreSQL connection pooling via pgBouncer (recommended for production)
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 10
sql_alchemy_pool_recycle = 1800
```

---

## Monitoring Scheduler Health

```python
# StatsD metrics emitted by the scheduler
# scheduler.dag_processing.total_parse_time    — total parse time per cycle
# scheduler.dag_processing.processes           — number of parse processes
# scheduler.critical_section_duration          — time holding scheduler lock
# scheduler.tasks.killed_externally            — tasks killed by external signal
# scheduler.tasks.running                      — currently running tasks

# Set up StatsD export
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

---

## Interview Tips

> **Tip 1:** The most common scheduler bottleneck in large deployments is DAG parsing, not scheduling logic. Reducing DAG file parse time (by moving heavy imports inside functions and splitting large DAG files) has more impact than any configuration change.

> **Tip 2:** `worker_prefetch_multiplier = 1` for CeleryExecutor is important. The default allows workers to prefetch multiple tasks, which can cause task starvation — fast tasks get prefetched by a slow worker while other workers sit idle.

> **Tip 3:** Know the HA scheduler architecture for senior interviews. Airflow 2.x uses database-level row locking to allow multiple schedulers without conflicts. This is a significant reliability improvement over Airflow 1.x where the scheduler was a single point of failure.
