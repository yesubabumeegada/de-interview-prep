---
title: "Airflow Monitoring and Alerting - Intermediate"
topic: airflow
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, monitoring, slack, pagerduty, statsd, prometheus, grafana, metrics]
---

# Airflow Monitoring and Alerting — Intermediate

## Custom Callbacks with Slack

Slack integration is the most common monitoring upgrade from email. Use the `SlackWebhookOperator` pattern or direct HTTP calls inside callbacks.

```python
import requests
from airflow.hooks.base import BaseHook

def slack_failure_callback(context):
    """Rich Slack alert with full context."""
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = ti.task_id
    execution_date = context["ds"]
    exception = str(context.get("exception", "Unknown error"))[:200]
    log_url = ti.log_url

    slack_msg = {
        "attachments": [{
            "color": "#FF0000",
            "title": f":red_circle: Task Failed: `{dag_id}.{task_id}`",
            "fields": [
                {"title": "DAG", "value": dag_id, "short": True},
                {"title": "Task", "value": task_id, "short": True},
                {"title": "Execution Date", "value": execution_date, "short": True},
                {"title": "Retries", "value": str(ti.try_number - 1), "short": True},
                {"title": "Error", "value": f"```{exception}```", "short": False},
                {"title": "Logs", "value": f"<{log_url}|View Logs>", "short": False},
            ],
        }]
    }

    webhook_url = BaseHook.get_connection("slack_webhook").password
    requests.post(webhook_url, json=slack_msg, timeout=10)

def slack_sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA miss alert."""
    webhook_url = BaseHook.get_connection("slack_webhook").password
    msg = {
        "text": f":warning: *SLA Miss* on `{dag.dag_id}`\n"
                f"Tasks: {', '.join(task_list)}\n"
                f"Blocking: {', '.join(blocking_task_list)}"
    }
    requests.post(webhook_url, json=msg, timeout=10)
```

---

## PagerDuty Integration

For critical pipelines that require on-call response:

```python
import requests

def pagerduty_failure_callback(context):
    """Trigger PagerDuty incident on task failure."""
    ti = context["task_instance"]
    
    payload = {
        "routing_key": Variable.get("pagerduty_routing_key"),
        "event_action": "trigger",
        "dedup_key": f"airflow-{context['dag'].dag_id}-{ti.task_id}-{context['ds']}",
        "payload": {
            "summary": f"Airflow task failed: {context['dag'].dag_id}.{ti.task_id}",
            "severity": "critical",
            "source": "airflow",
            "custom_details": {
                "dag_id": context["dag"].dag_id,
                "task_id": ti.task_id,
                "execution_date": context["ds"],
                "log_url": ti.log_url,
                "exception": str(context.get("exception", "")),
            },
        },
    }
    
    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json=payload,
        timeout=10,
    )
```

The `dedup_key` prevents duplicate incidents — if the task is retried and fails again, PagerDuty updates the existing incident rather than creating a new one.

---

## Writing Callback Functions — Context Dict Deep Dive

The context dict is your primary source of information in callbacks:

```python
def comprehensive_callback(context):
    ti = context["task_instance"]
    
    # TaskInstance properties
    ti.dag_id             # "daily_etl"
    ti.task_id            # "load_warehouse"
    ti.execution_date     # pendulum datetime
    ti.start_date         # When task started running
    ti.end_date           # When task ended
    ti.duration           # Float seconds (None if still running)
    ti.try_number         # Current try (1-indexed)
    ti.max_tries          # Max retries allowed
    ti.log_url            # Direct URL to task logs
    ti.xcom_pull("upstream_task")  # Access XCom from callbacks
    
    # DAG properties
    context["dag"].dag_id
    context["dag"].schedule_interval
    context["dag"].tags
    
    # Time references
    context["ds"]         # "2024-01-15"
    context["ts"]         # "2024-01-15T00:00:00+00:00"
    context["data_interval_start"]
    context["data_interval_end"]
    context["run_id"]     # "scheduled__2024-01-15T00:00:00+00:00"
    
    # Only in on_failure_callback
    context.get("exception")  # The actual exception object
```

---

## StatsD Metrics Emission

Airflow emits metrics via StatsD. Configure in `airflow.cfg`:

```ini
[metrics]
statsd_on = True
statsd_host = statsd-host.example.com
statsd_port = 8125
statsd_prefix = airflow
statsd_allow_list = scheduler,executor,dagrun,task
```

Airflow emits dozens of metrics. The most important:

| Metric | What It Measures |
|---|---|
| `airflow.scheduler.heartbeat` | Scheduler is alive (rate should be ~1/s) |
| `airflow.dag.loading_duration` | Time to import DAG files |
| `airflow.dagbag.import_errors` | Number of DAGs with import errors |
| `airflow.dagrun.duration.success.<dag_id>` | DAG run duration for successful runs |
| `airflow.dagrun.duration.failed.<dag_id>` | DAG run duration for failed runs |
| `airflow.task.duration.<dag_id>.<task_id>` | Individual task duration |
| `airflow.executor.open_slots` | Available worker slots |
| `airflow.executor.queued_tasks` | Tasks waiting for a slot |
| `airflow.zombies_killed` | Tasks killed as zombies |

---

## Prometheus via statsd_exporter

Prometheus scrapes metrics via HTTP. Bridge StatsD to Prometheus using `prom/statsd-exporter`:

```yaml
# docker-compose.yml snippet
statsd-exporter:
  image: prom/statsd-exporter:latest
  ports:
    - "9102:9102"   # Prometheus scrape endpoint
    - "9125:9125/udp"  # StatsD receiver
  command:
    - "--statsd.mapping-config=/tmp/statsd-mappings.yaml"
  volumes:
    - ./statsd-mappings.yaml:/tmp/statsd-mappings.yaml
```

```yaml
# statsd-mappings.yaml — convert StatsD dots to Prometheus labels
mappings:
  - match: "airflow.task.duration.*.*"
    name: "airflow_task_duration_seconds"
    labels:
      dag_id: "$1"
      task_id: "$2"
  - match: "airflow.dagrun.duration.success.*"
    name: "airflow_dagrun_duration_success_seconds"
    labels:
      dag_id: "$1"
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: airflow
    static_configs:
      - targets: ["statsd-exporter:9102"]
```

---

## Key Airflow Metrics to Alert On

```yaml
# Grafana alerts (PromQL)

# 1. Scheduler is dead
- alert: AirflowSchedulerDown
  expr: rate(airflow_scheduler_heartbeat_total[5m]) == 0
  for: 2m
  severity: critical

# 2. DAG import errors — someone broke a DAG file
- alert: AirflowDagImportErrors
  expr: airflow_dagbag_import_errors > 0
  severity: warning

# 3. Task queue backing up — not enough workers
- alert: AirflowQueuedTasksHigh
  expr: airflow_executor_queued_tasks > 20
  for: 10m
  severity: warning

# 4. Zombie tasks
- alert: AirflowZombiesDetected
  expr: increase(airflow_zombies_killed_total[1h]) > 3
  severity: warning

# 5. Scheduler lag — tasks not being scheduled
- alert: AirflowSchedulerLag
  expr: airflow_scheduler_critical_section_busy > 0.8
  for: 5m
  severity: warning
```

---

## Grafana Dashboard Setup for Airflow

Essential panels for an Airflow Grafana dashboard:

```
Row 1: System Health
  ├── Scheduler heartbeat rate (green if > 0.9/s)
  ├── Active workers count
  └── Queued tasks / Open slots ratio

Row 2: DAG Health
  ├── DAG import errors count (should be 0)
  ├── DAG parse time (p95) — alert if > 30s
  └── DAGs processed per minute

Row 3: Task Performance
  ├── Task duration by DAG (heatmap)
  ├── Task failure rate by DAG (bar chart)
  └── Retry rate by task

Row 4: Infrastructure
  ├── Metadata DB query duration (p95)
  ├── Zombie tasks killed per hour
  └── Executor slot utilization
```

---

## Key Takeaways

- Slack callbacks use the full context dict to build rich, actionable alerts with log URLs and error details
- PagerDuty callbacks should include a `dedup_key` based on `dag_id + task_id + ds` to prevent duplicate incidents
- StatsD is enabled with 4 config lines; the most critical metrics to alert on are `scheduler.heartbeat`, `dagbag.import_errors`, and `executor.queued_tasks`
- `prom/statsd-exporter` bridges Airflow StatsD metrics to Prometheus with label extraction via regex mappings
- Grafana dashboards should cover: scheduler health, DAG import errors, task duration trends, and worker slot utilization
