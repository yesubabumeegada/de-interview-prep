---
title: "Airflow Monitoring and Alerting - Fundamentals"
topic: airflow
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, monitoring, alerting, sla, callbacks, email]
---

# Airflow Monitoring and Alerting — Fundamentals

## 🎯 Analogy

Think of Airflow monitoring like air traffic control: the UI gives you the radar view (where every flight is), SLA misses are the "flight is overdue" alerts, and callbacks are the automated calls to the airline when something goes wrong.

---

## DAG Run States and Task Instance States

Every unit of work in Airflow has a state. Understanding states is the foundation of monitoring.

**DAG Run States:**

| State | Meaning |
|---|---|
| `running` | Currently executing |
| `success` | All tasks completed successfully |
| `failed` | At least one task failed and has no more retries |
| `queued` | Scheduled but not yet started |

**Task Instance States:**

| State | Meaning |
|---|---|
| `success` | Task completed without error |
| `failed` | Task raised an exception |
| `running` | Task is currently executing |
| `up_for_retry` | Failed but has retries remaining |
| `up_for_reschedule` | Sensor waiting to re-poke |
| `skipped` | Skipped by BranchOperator or ShortCircuit |
| `queued` | Waiting for a worker slot |
| `scheduled` | Scheduler has scheduled it, not yet queued |
| `deferred` | Using deferrable operator, waiting for trigger |
| `zombie` | Process died without updating state |

---

## UI Views for Monitoring

**Grid View (formerly Tree View):**
- Shows all DAG runs as columns, tasks as rows
- Color-coded by state — instantly see which runs/tasks failed
- Click any cell to see task logs, re-run, or mark success

**Graph View:**
- Shows task dependencies as a DAG diagram
- Color-coded — see which tasks in the current run are done/running/failed

**Gantt View:**
- Timeline of task durations for a specific DAG run
- Identifies bottlenecks — which task took the longest?
- Spot scheduling lag (large gaps between tasks going from `scheduled` to `running`)

---

## Email Alerts: email_on_failure and email_on_retry

The simplest alerting: configure SMTP and add email addresses to `default_args`.

```python
# airflow.cfg — SMTP configuration
[email]
email_backend = airflow.utils.email.send_email_smtp
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
smtp_user = airflow@example.com
smtp_password = your_smtp_password
smtp_mail_from = airflow@example.com
```

```python
# DAG definition
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    "owner": "data-team",
    "email": ["data-oncall@example.com", "team-lead@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,   # Don't spam on retries
    "email_on_success": False, # Only alert on problems
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_etl",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
)
```

The built-in failure email contains: DAG ID, task ID, execution date, log URL, exception message.

---

## SLA Misses

An SLA (Service Level Agreement) in Airflow defines the maximum time a task or DAG run should take to complete after the schedule time.

```python
from datetime import timedelta
from airflow import DAG

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when any task misses its SLA."""
    print(f"SLA MISS on DAG: {dag.dag_id}")
    print(f"Tasks that missed SLA: {[str(sla) for sla in slas]}")
    # Send alert here

dag = DAG(
    dag_id="daily_report",
    schedule_interval="0 8 * * *",  # 8am daily
    sla_miss_callback=sla_miss_callback,
    default_args={"sla": timedelta(hours=2)},  # Each task must finish within 2 hours of DAG start
)
```

> **Important:** SLA is measured from the DAG's scheduled `data_interval_start`, not from when the task actually started. A task that starts late because of upstream delays still misses its SLA.

**SLA miss callback parameters:**

| Parameter | Description |
|---|---|
| `dag` | The DAG object |
| `task_list` | List of task_ids that missed their SLA |
| `blocking_task_list` | Upstream tasks blocking SLA tasks |
| `slas` | SlaMiss model objects (contains `execution_date`, `task_id`) |
| `blocking_tis` | TaskInstance objects that are blocking |

---

## Callbacks: on_failure, on_success, on_retry

Callbacks are Python functions called when task state changes. They receive the Airflow **context dictionary**.

```python
def on_failure_callback(context):
    """Called when a task fails (after all retries exhausted)."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")
    log_url = context["task_instance"].log_url
    
    print(f"FAILURE: {dag_id}.{task_id} at {execution_date}")
    print(f"Exception: {exception}")
    print(f"Logs: {log_url}")
    # → Send Slack/PagerDuty alert

def on_retry_callback(context):
    """Called on each retry attempt."""
    task_instance = context["task_instance"]
    print(f"Retry #{task_instance.try_number}: {task_instance.task_id}")

def on_success_callback(context):
    """Called on successful completion."""
    duration = context["task_instance"].duration
    print(f"Task completed in {duration:.1f}s")
```

```python
# Apply at DAG level (all tasks inherit)
dag = DAG(
    dag_id="monitored_dag",
    default_args={
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
    },
)

# Or override on a specific task
from airflow.operators.python import PythonOperator

critical_task = PythonOperator(
    task_id="critical_load",
    python_callable=load_data,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    dag=dag,
)
```

**Context dict key fields:**

```python
context = {
    "dag": dag,                    # DAG object
    "task": task,                  # Task/Operator object
    "task_instance": ti,           # TaskInstance object
    "execution_date": pendulum,    # Execution date (pendulum datetime)
    "ds": "2024-01-15",            # execution_date as YYYY-MM-DD string
    "ts": "2024-01-15T00:00:00+00:00",  # ISO timestamp
    "run_id": "scheduled__2024-01-15T00:00:00+00:00",
    "exception": Exception,        # Only in on_failure_callback
    "prev_execution_date": pendulum,
    "next_execution_date": pendulum,
}
```

---

## Key Takeaways

- Task states (success, failed, running, zombie, up_for_retry) are the foundation of monitoring
- Grid/Graph/Gantt views in the UI give different perspectives on DAG health
- `email_on_failure=True` in `default_args` provides basic alerting with zero code
- SLA misses fire when a task doesn't complete within a defined duration after the schedule time
- Callbacks (`on_failure_callback`, `on_retry_callback`, `on_success_callback`) receive a context dict with full task metadata and are the building block for Slack/PagerDuty integrations
