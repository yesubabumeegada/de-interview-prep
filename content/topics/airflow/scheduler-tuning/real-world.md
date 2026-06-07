---
title: "Airflow Scheduler Tuning - Real-World Scenarios"
topic: airflow
subtopic: scheduler-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, scheduler, production, performance, dag-parsing, high-availability]
---

# Airflow Scheduler Tuning — Real-World Scenarios

## Scenario 1: 500+ DAG Environment with High Scheduling Lag

A data platform team has 650 DAGs across teams. The scheduler is single-instance, tasks frequently sit in `scheduled` state for 3–5 minutes before being queued, and the UI is slow to load.

**Diagnosis:**

```bash
# Step 1: Measure per-DAG parse time
airflow dags report | sort -t'|' -k5 -rn | head -20

# Step 2: Find slow DAG files
grep "Loaded DAG" /opt/airflow/logs/scheduler/latest/*.log \
  | awk '{print $NF, $(NF-1)}' | sort -rn | head -10

# Step 3: Check metadata DB connection time
time psql $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN -c "SELECT COUNT(*) FROM task_instance WHERE state='running'"
```

**Resolution applied:**

```ini
# airflow.cfg changes
[scheduler]
# Increase parse parallelism (was 2)
parsing_processes = 8

# Reduce parse frequency for stable DAGs (was 30)
min_file_process_interval = 120

# Scan for new files less often (was 60)
dag_dir_list_interval = 300

# More tasks per scheduling loop (was 512)
max_tis_per_query = 1024
max_dagruns_per_loop_to_schedule = 50

[core]
# Total concurrent tasks across cluster (was 32)
parallelism = 256
```

```ini
# Enable DAG serialization so webserver doesn't parse files
[core]
store_dag_code = True

[webserver]
# Webserver reads from DB, not files
dag_orientation = LR
```

```bash
# Run second scheduler for HA (zero-downtime improvement)
# Scheduler 2 on separate k8s pod
airflow scheduler
```

**Result:** scheduling lag dropped from 4 minutes to 15 seconds. UI load time dropped from 8 seconds to 1 second.

---

## Scenario 2: Memory Explosion from Heavy DAG Imports

A machine learning team writes DAGs that import `torch`, `transformers`, and `sklearn` at module level. The scheduler processes use 12GB RAM per process, causing OOM kills and scheduler restarts.

```python
# ❌ Original DAG — imports at module level
import torch
import transformers
from sklearn.preprocessing import StandardScaler
from airflow import DAG
from airflow.operators.python import PythonOperator

def train_model(**ctx):
    model = transformers.AutoModel.from_pretrained('bert-base-uncased')
    # ...

with DAG('ml_training', ...) as dag:
    PythonOperator(task_id='train', python_callable=train_model)
```

```python
# ✅ Fixed — all heavy imports inside functions
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def train_model(**ctx):
    # Imported only when the task EXECUTES, not when DAG is PARSED
    import torch
    import transformers
    from sklearn.preprocessing import StandardScaler

    model = transformers.AutoModel.from_pretrained('bert-base-uncased')
    # ...

with DAG(
    'ml_training',
    start_date=datetime(2024, 1, 1),
    schedule='@weekly',
    catchup=False,
) as dag:
    PythonOperator(task_id='train', python_callable=train_model)
```

**Result:** Scheduler process memory dropped from 12GB to 180MB per process. Parse time per DAG file dropped from 45s to 0.3s.

**Additional safeguard — set parse timeout:**

```ini
[scheduler]
# Kill parser process if it takes > 30s (prevents runaway imports)
dag_file_processor_timeout = 30
```

---

## Scenario 3: Burst Scheduling — 1000 Tasks at Midnight

A financial firm runs all their batch jobs at midnight. The scheduler receives 1000+ tasks simultaneously. Workers get overwhelmed, and the DB connection pool exhausts.

**Staggered scheduling solution:**

```python
# Spread DAGs across a 2-hour window to avoid the thundering herd
import random
from datetime import datetime
from airflow import DAG

# Assign each DAG a random minute offset: 00:00 to 01:59
def make_midnight_dag(dag_id, offset_minutes):
    return DAG(
        dag_id=dag_id,
        schedule=f'{offset_minutes % 60} {offset_minutes // 60} * * *',
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )

# 100 DAGs spread across 00:00–01:59
for i in range(100):
    dag_id = f'batch_job_{i:03d}'
    offset = (i * 1.2) % 120   # evenly spread across 120 minutes
    globals()[dag_id] = make_midnight_dag(dag_id, int(offset))
```

**Pool-based rate limiting:**

```python
# Create a pool that limits midnight concurrency
# Via Airflow UI or CLI:
# airflow pools set midnight_batch 50 "Rate limit midnight batch jobs"

from airflow.operators.python import PythonOperator

task = PythonOperator(
    task_id='batch_load',
    python_callable=load_fn,
    pool='midnight_batch',      # Only 50 slots = max 50 concurrent tasks
    pool_slots=1,
)
```

**pgBouncer for connection pooling:**

```yaml
# docker-compose.yml — add pgBouncer between Airflow and PostgreSQL
pgbouncer:
  image: bitnami/pgbouncer:latest
  environment:
    POSTGRESQL_HOST: postgres
    POSTGRESQL_PORT: 5432
    POSTGRESQL_DATABASE: airflow
    PGBOUNCER_DATABASE: airflow
    PGBOUNCER_POOL_MODE: transaction   # transaction pooling = most efficient
    PGBOUNCER_MAX_CLIENT_CONN: 1000
    PGBOUNCER_DEFAULT_POOL_SIZE: 20
```

```ini
# Airflow points to pgBouncer, not directly to PostgreSQL
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@pgbouncer:6432/airflow
```

---

## Scenario 4: Scheduler Keeps Restarting in Kubernetes

The Airflow scheduler pod restarts every 30–60 minutes due to Kubernetes OOMKill.

**Investigation:**

```bash
kubectl describe pod airflow-scheduler-xxx -n airflow
# Events:
#   OOMKilled (exit code 137) — memory limit exceeded

kubectl top pod -n airflow --containers
# airflow-scheduler: CPU 0.2/1, Memory 3.8Gi/4Gi  ← near limit
```

**Root causes and fixes:**

```yaml
# 1. Increase memory limit
resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "6Gi"    # Was 4Gi
    cpu: "2000m"

# 2. Reduce parsing processes to reduce memory usage
env:
  - name: AIRFLOW__SCHEDULER__PARSING_PROCESSES
    value: "2"       # Was 4

# 3. Enable memory profiling to find the leak
  - name: AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE
    value: "True"
```

```python
# 3. Identify memory-leaking DAG files
# Add memory tracking to CI
import tracemalloc
import importlib.util

def measure_dag_memory(dag_file):
    tracemalloc.start()
    spec = importlib.util.spec_from_file_location("dag", dag_file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / 1024 / 1024   # MB

# Run in CI for all DAG files
for dag_file in Path('dags').glob('*.py'):
    mb = measure_dag_memory(dag_file)
    if mb > 100:
        print(f"WARNING: {dag_file} uses {mb:.0f}MB at parse time")
```
