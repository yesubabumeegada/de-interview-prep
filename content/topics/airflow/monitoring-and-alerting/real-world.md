---
title: "Airflow Monitoring and Alerting - Real World"
topic: airflow
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, monitoring, production, slack, sla, zombie, grafana, incident-runbook]
---

# Airflow Monitoring and Alerting — Real World

## Production Pattern: Rich Slack Alert Template

A production-grade Slack alert provides enough context to diagnose the issue without opening Airflow:

```python
from airflow.hooks.base import BaseHook
import requests

def build_slack_failure_alert(context) -> dict:
    """Build a rich Slack Block Kit message for task failures."""
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = ti.task_id
    execution_date = context["ds"]
    try_number = ti.try_number
    max_tries = ti.max_tries + 1
    exception = str(context.get("exception", "No exception captured"))[:300]
    
    is_final_failure = try_number >= max_tries
    color = "#FF0000" if is_final_failure else "#FFA500"
    status = ":red_circle: FINAL FAILURE" if is_final_failure else ":orange_circle: RETRY FAILURE"
    
    return {
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{status}: {dag_id}"}
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                        {"type": "mrkdwn", "text": f"*Date:*\n`{execution_date}`"},
                        {"type": "mrkdwn", "text": f"*Attempt:*\n{try_number}/{max_tries}"},
                        {"type": "mrkdwn", "text": f"*Owner:*\n{context['dag'].owner}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Error:*\n```{exception}```"}
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View Logs"},
                            "url": ti.log_url,
                            "style": "primary",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open DAG"},
                            "url": f"http://airflow.example.com/dags/{dag_id}/grid",
                        }
                    ]
                }
            ]
        }]
    }

def slack_failure_callback(context):
    webhook_url = BaseHook.get_connection("slack_webhook_data_alerts").password
    payload = build_slack_failure_alert(context)
    requests.post(webhook_url, json=payload, timeout=10)
```

---

## SLA Miss Cascade: Upstream Delay Propagating to 5 Downstream DAGs

**Scenario:** The `raw_events_ingestion` DAG has an SLA of 6am. It runs at 5am and normally completes by 5:45am. On one day, a source system was slow and it didn't complete until 7:30am. This caused 5 downstream DAGs (all waiting on an ExternalTaskSensor) to also miss their SLAs and trigger alerts.

**Root causes:**
1. ExternalTaskSensors with `timeout=None` just waited indefinitely
2. No timeout on the source system connection, so the extraction ran for 2.5 hours
3. All 5 downstream DAGs had SLA set to 7am but no alternative logic for late data

**Resolution:**

```python
# 1. Add timeouts to ExternalTaskSensors
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_raw_events = ExternalTaskSensor(
    task_id="wait_for_raw_events",
    external_dag_id="raw_events_ingestion",
    external_task_id="validate_complete",
    execution_date_fn=lambda dt: dt,
    timeout=3600,           # Fail after 1 hour of waiting
    poke_interval=60,
    mode="reschedule",      # Don't hold a slot while waiting
    on_failure_callback=slack_failure_callback,
)

# 2. Add source system timeout
def extract_events(**context):
    conn = BaseHook.get_connection("source_system")
    # Always set explicit timeouts
    response = requests.get(
        f"{conn.host}/events",
        timeout=(10, 300),   # (connect timeout, read timeout) in seconds
    )
```

```python
# 3. Propagate SLA miss alerts to downstream DAG owners
def cascade_aware_sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert both the upstream DAG owner AND downstream DAG owners."""
    slack_notify(
        channel="#data-alerts",
        message=f"SLA miss on `{dag.dag_id}` — "
                f"downstream DAGs affected: reporting_daily, analytics_refresh, ..."
    )
    # Trigger downstream incident workflow
```

---

## Zombie Task Detection and Auto-Clearing

Zombie tasks occur when a worker process dies (OOM kill, node failure) without updating the task state in the metadata DB. The scheduler detects and kills them, but in high-throughput environments you want proactive monitoring.

```python
# Detect potential zombies via metadata DB
SELECT
    ti.dag_id,
    ti.task_id,
    ti.execution_date,
    ti.state,
    ti.start_date,
    NOW() - ti.start_date AS running_duration,
    ti.hostname
FROM task_instance ti
WHERE ti.state = 'running'
    AND ti.start_date < NOW() - INTERVAL '2 hours'
    AND ti.heartbeat_timeout IS NULL
ORDER BY running_duration DESC;
```

```python
# Airflow DAG to auto-detect and clear zombie tasks
from airflow.models import TaskInstance
from airflow import settings

def detect_and_clear_zombies(**context):
    """Find tasks stuck in 'running' state for > 2 hours and clear them."""
    from datetime import datetime, timedelta, timezone
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    
    with settings.Session() as session:
        stuck_tasks = session.query(TaskInstance).filter(
            TaskInstance.state == "running",
            TaskInstance.start_date < cutoff,
        ).all()
        
        cleared = []
        for ti in stuck_tasks:
            ti.state = "failed"
            ti.end_date = datetime.now(timezone.utc)
            cleared.append(f"{ti.dag_id}.{ti.task_id}")
            
        session.commit()
    
    if cleared:
        slack_notify(f":zombie: Auto-cleared {len(cleared)} zombie tasks: {cleared}")
    
    return len(cleared)
```

---

## Grafana Dashboard for 200-DAG Production Environment

A real Grafana setup for 200 DAGs needs dynamic filtering, not hard-coded DAG names.

**Key dashboard structure:**

```
[Variable: team] → filter by DAG tag
[Variable: dag_id] → drill into specific DAG

Row 1: Fleet Overview
  ├── Total running tasks (gauge)
  ├── Failed tasks in last 1hr (stat, red if > 0)
  ├── Scheduler heartbeat (gauge, green if < 5s)
  └── Import errors (stat, must be 0)

Row 2: Task Success Rate (filtered by $team)
  ├── Success rate by DAG (bar chart, last 7 days)
  ├── P95 task duration trend by DAG (time series)
  └── Retry rate heatmap (dag_id × task_id)

Row 3: Infrastructure
  ├── Worker slot utilization (stacked area: used/free)
  ├── Queue depth over time
  └── DB connection pool utilization
```

PromQL examples:

```promql
# Success rate for a specific team's DAGs (last 24h)
rate(airflow_dagrun_duration_success_seconds_count{dag_id=~"$team_.*"}[24h])
/
(rate(airflow_dagrun_duration_success_seconds_count{dag_id=~"$team_.*"}[24h])
 + rate(airflow_dagrun_duration_failed_seconds_count{dag_id=~"$team_.*"}[24h]))

# P95 task duration for a specific DAG
histogram_quantile(0.95, rate(airflow_task_duration_seconds_bucket{dag_id="$dag_id"}[1h]))
```

---

## Incident Runbook for Common Failure Modes

```
=== RUNBOOK: Airflow Production Incidents ===

INCIDENT: Scheduler stopped scheduling tasks
Symptoms: Tasks stuck in "scheduled" state, scheduler heartbeat gap > 5 min
Steps:
  1. Check scheduler logs: kubectl logs -l component=scheduler --tail=100
  2. Check DB connection: airflow db check
  3. Check for OOM: kubectl describe pod <scheduler-pod> | grep -A5 "OOM"
  4. If OOM: increase scheduler memory limits, restart scheduler pod
  5. If DB issue: check RDS/pg metrics, check connection pool exhaustion
  6. Restart scheduler: kubectl rollout restart deployment/airflow-scheduler

INCIDENT: Flood of failure alerts (many DAGs failing simultaneously)
Symptoms: >5 different DAGs alerting within 1 minute
Likely cause: Infrastructure issue (DB, network), not a code issue
Steps:
  1. Check /health endpoint: curl airflow:8080/health
  2. Check metadata DB health
  3. Check worker node health: kubectl get nodes
  4. If infrastructure OK: look for a common dependency (shared connection, S3 bucket)
  5. Alert data platform team, NOT individual DAG owners

INCIDENT: Tasks running but taking 3x longer than usual
Symptoms: No failures, but SLA misses increasing
Steps:
  1. Check Grafana task duration trends — which specific tasks slowed down?
  2. Check external system (Snowflake, BigQuery, RDS) for slowness
  3. Check if worker nodes are resource-constrained (CPU/memory)
  4. Check if task is doing more work (data volume spike)
  5. Scale workers if data volume spike, else escalate to platform team

INCIDENT: Import errors > 0
Symptoms: airflow_dagbag_import_errors metric > 0, some DAGs missing from UI
Steps:
  1. Run: airflow dags report  (shows which DAG files have errors)
  2. Check git log for recent DAG changes
  3. Run: python /path/to/broken_dag.py  (syntax check)
  4. Fix in version control, deploy fix
  5. Monitor until import_errors returns to 0
```

---

## Key Takeaways

- Production Slack alerts need Block Kit with log URLs, retry counts, and direct action buttons for fast incident response
- SLA cascades happen when ExternalTaskSensors have no timeout — always set `timeout` on sensors
- Zombie tasks should be monitored via a dedicated DAG that queries the metadata DB and clears stuck tasks
- 200-DAG Grafana dashboards need team/DAG variables for filtering and focus on fleet-wide health first
- A documented incident runbook for common failure modes dramatically reduces MTTR
