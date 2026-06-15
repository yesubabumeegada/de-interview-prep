---
title: "Airflow Monitoring and Alerting - Scenario Questions"
topic: airflow
subtopic: monitoring-and-alerting
content_type: scenario_question
tags: [airflow, monitoring, alerting, sla, observability, interview, scenarios]
---

# Scenario Questions — Airflow Monitoring and Alerting

<article data-difficulty="junior">

## 🟢 Junior: Set Up Email + Slack Alert on DAG Failure

**Scenario:** Your team's `daily_revenue_report` DAG runs at 6am. Currently there are no alerts — the team discovers failures by manually checking the UI each morning. Set up alerts so the team is notified immediately on failure via both email and Slack.

<details>
<summary>✅ Solution</summary>

**Step 1: Configure SMTP in airflow.cfg**

```ini
[email]
email_backend = airflow.utils.email.send_email_smtp
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
smtp_user = airflow-alerts@example.com
smtp_password = your_app_password
smtp_mail_from = Airflow Alerts <airflow-alerts@example.com>
```

**Step 2: Create a Slack connection**

```bash
# In the Airflow UI: Admin → Connections → Add
# Conn ID: slack_webhook_data
# Conn Type: HTTP
# Password: https://hooks.slack.com/services/T.../B.../...
```

**Step 3: Write the Slack callback**

```python
# dags/callbacks.py
import requests
from airflow.hooks.base import BaseHook

def slack_failure_callback(context):
    """Send Slack notification with task failure details."""
    ti = context["task_instance"]
    
    message = {
        "text": (
            f":red_circle: *DAG Failure Alert*\n"
            f"*DAG:* `{context['dag'].dag_id}`\n"
            f"*Task:* `{ti.task_id}`\n"
            f"*Date:* `{context['ds']}`\n"
            f"*Attempt:* {ti.try_number}/{ti.max_tries + 1}\n"
            f"*Error:* ```{str(context.get('exception', 'Unknown'))[:200]}```\n"
            f"*Logs:* <{ti.log_url}|Click here>"
        )
    }
    
    webhook_url = BaseHook.get_connection("slack_webhook_data").password
    requests.post(webhook_url, json=message, timeout=10)
```

**Step 4: Apply callbacks and email to the DAG**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.callbacks import slack_failure_callback

default_args = {
    "owner": "data-team",
    "email": ["data-team@example.com"],      # Email list
    "email_on_failure": True,                # Built-in email on failure
    "email_on_retry": False,                 # No email on intermediate retries
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback,  # Slack on every failure
}

dag = DAG(
    dag_id="daily_revenue_report",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
)
```

**Result:** On failure, the team gets:
- Email with exception details (after all retries exhausted)
- Slack message on every retry failure (for awareness) AND on final failure

**Pro tip:** Set `email_on_retry=False` but `on_failure_callback` fires on every retry — use the try_number to only Slack on final failure:

```python
def smart_slack_callback(context):
    ti = context["task_instance"]
    is_final = ti.try_number >= ti.max_tries + 1
    if is_final:  # Only Slack when retries are exhausted
        slack_failure_callback(context)
```

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid: Design SLA Monitoring for a 4-Hour Freshness Requirement

**Scenario:** The business requires that the `customer_360` table is refreshed by 8am every day. The DAG starts at 4am and must complete within 4 hours. Design the full SLA monitoring and escalation path.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
4:00am - DAG starts
7:30am - SLA warning (30min buffer remaining) → Slack #data-alerts
8:00am - SLA deadline → Slack + PagerDuty page
8:30am - Escalation → Page data engineering manager
9:00am - Executive notification
```

**Implementation:**

```python
from datetime import datetime, timedelta, timezone
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Multi-tier escalation on SLA miss."""
    import boto3
    
    for sla in slas:
        # Calculate overdue duration
        overdue = datetime.now(timezone.utc) - sla.execution_date.replace(tzinfo=timezone.utc)
        overdue_minutes = overdue.total_seconds() / 60
        
        # Always: Slack warning to team channel
        slack_msg = (
            f":warning: *SLA Miss: customer_360 pipeline*\n"
            f"Task `{sla.task_id}` missed its 8am deadline\n"
            f"Overdue by: {overdue_minutes:.0f} minutes\n"
            f"Blocking tasks: {', '.join(blocking_task_list) or 'None'}"
        )
        webhook = BaseHook.get_connection("slack_webhook_data").password
        requests.post(webhook, json={"text": slack_msg}, timeout=10)
        
        # After 30min: PagerDuty
        if overdue_minutes >= 30:
            _trigger_pagerduty(
                summary=f"customer_360 SLA miss: {overdue_minutes:.0f}min overdue",
                severity="warning" if overdue_minutes < 60 else "critical",
                dag_id=dag.dag_id,
                task_id=sla.task_id,
            )
        
        # After 60min: Escalate to manager
        if overdue_minutes >= 60:
            requests.post(webhook, json={
                "text": f":rotating_light: *ESCALATION: customer_360 {overdue_minutes:.0f}min overdue*\n"
                        f"<@data-eng-manager> please acknowledge."
            }, timeout=10)

def _trigger_pagerduty(summary, severity, dag_id, task_id):
    key = Variable.get("pagerduty_routing_key")
    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json={
            "routing_key": key,
            "event_action": "trigger",
            "dedup_key": f"airflow-sla-{dag_id}-{task_id}",
            "payload": {"summary": summary, "severity": severity, "source": "airflow"},
        },
        timeout=10,
    )

# Apply SLA to the DAG
with DAG(
    dag_id="customer_360_refresh",
    schedule_interval="0 4 * * *",       # Start at 4am
    default_args={
        "sla": timedelta(hours=4),        # Each task must complete within 4 hours
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    sla_miss_callback=sla_miss_callback,
    start_date=datetime(2024, 1, 1),
) as dag:
    # Tasks...
    pass
```

**SLA monitoring dashboard addition:**

```sql
-- Add to Grafana dashboard: "SLA Miss history for customer_360"
SELECT
    dag_id,
    task_id,
    execution_date,
    timestamp,
    description
FROM sla_miss
WHERE dag_id = 'customer_360_refresh'
ORDER BY execution_date DESC
LIMIT 30;
```

**Runbook section for this DAG:**

```markdown
# customer_360 SLA Miss Runbook
1. Check if source tables (raw_events, user_profiles) are updated: 
   SELECT MAX(updated_at) FROM raw.events;
2. Check if a specific task is bottleneck (Gantt view in Airflow UI)
3. Check Snowflake query performance for the transform tasks
4. If source tables late: notify upstream team, inform stakeholders of delay
5. If Snowflake slow: contact DBA, consider scaling warehouse up temporarily
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build Observability System for 500-DAG Airflow with Distributed Tracing and Anomaly Detection

**Scenario:** Your organization runs 500 DAGs across 12 data teams. The current state: email alerts for failures, no SLA tracking, no metrics. The VP of Data says "we find out about problems from business users, not from monitoring." Design a complete observability system.

<details>
<summary>✅ Solution</summary>

**Target architecture:**

```
Airflow Tasks
    │
    ├─ Structured Logs ──────────────► Datadog Logs
    ├─ StatsD Metrics ──────────────► statsd_exporter → Prometheus → Grafana
    ├─ OpenTelemetry Traces ────────► Jaeger / Tempo
    └─ Audit Events ────────────────► CloudWatch → Splunk SIEM

Alert Routing:
    Prometheus Alertmanager
        ├─ Warning → #data-alerts Slack
        ├─ Critical → PagerDuty (on-call rotation per team)
        └─ Anomaly → #data-platform-team Slack
```

**1. Instrument all tasks with a shared decorator (applied in a base DAG factory):**

```python
# platform/observability.py
from functools import wraps
from opentelemetry import trace
import structlog

tracer = trace.get_tracer("airflow")
log = structlog.get_logger()

def instrument_task(func):
    @wraps(func)
    def wrapper(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        team = context["dag"].tags[0] if context["dag"].tags else "unknown"
        
        with tracer.start_as_current_span(
            f"{dag_id}.{task_id}",
            attributes={"team": team, "dag_id": dag_id, "task_id": task_id, "run_id": run_id}
        ) as span:
            bound_log = log.bind(dag_id=dag_id, task_id=task_id, run_id=run_id, team=team)
            bound_log.info("task_start")
            try:
                result = func(**context)
                span.set_status(trace.StatusCode.OK)
                bound_log.info("task_success", duration=context["task_instance"].duration)
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR)
                bound_log.error("task_failure", error=str(e))
                raise
    return wrapper
```

**2. Anomaly detection on task duration (ML-based alerting):**

```python
# platform/anomaly_detector.py — runs as a DAG itself
import numpy as np
from scipy import stats

def detect_duration_anomalies(**context):
    """Compare today's task durations against rolling 30-day baseline."""
    from airflow.models import TaskInstance
    from airflow import settings
    from datetime import timedelta
    
    with settings.Session() as session:
        # Get last 30 days of durations per task
        cutoff = context["execution_date"] - timedelta(days=30)
        
        rows = session.execute("""
            SELECT dag_id, task_id, duration
            FROM task_instance
            WHERE state = 'success'
              AND execution_date >= :cutoff
              AND execution_date < :today
        """, {"cutoff": cutoff, "today": context["execution_date"]}).fetchall()
        
        # Build baseline distributions
        from collections import defaultdict
        baselines = defaultdict(list)
        for dag_id, task_id, duration in rows:
            if duration:
                baselines[(dag_id, task_id)].append(duration)
        
        # Get today's durations
        today_rows = session.execute("""
            SELECT dag_id, task_id, duration
            FROM task_instance
            WHERE state = 'success'
              AND DATE(execution_date) = :today
        """, {"today": context["ds"]}).fetchall()
        
        anomalies = []
        for dag_id, task_id, duration in today_rows:
            if duration is None:
                continue
            baseline = baselines.get((dag_id, task_id), [])
            if len(baseline) < 7:
                continue  # Not enough history
            
            mean = np.mean(baseline)
            std = np.std(baseline)
            z_score = (duration - mean) / std if std > 0 else 0
            
            if abs(z_score) > 3:  # 3-sigma anomaly
                anomalies.append({
                    "dag_id": dag_id, "task_id": task_id,
                    "duration": duration, "expected": mean, "z_score": z_score
                })
        
        if anomalies:
            slack_notify(
                channel="#data-platform-team",
                message=f":chart_with_upwards_trend: *Duration anomalies detected ({len(anomalies)} tasks)*\n"
                        + "\n".join(
                            f"• `{a['dag_id']}.{a['task_id']}`: {a['duration']:.0f}s "
                            f"(expected {a['expected']:.0f}s, z={a['z_score']:.1f})"
                            for a in anomalies[:10]
                        )
            )

anomaly_dag = DAG(
    dag_id="_platform_anomaly_detection",
    schedule_interval="0 10 * * *",  # Run after morning pipelines complete
    tags=["platform"],
)
```

**3. Per-team alert routing in Alertmanager:**

```yaml
# alertmanager.yml
route:
  group_by: [team, dag_id]
  receiver: default-slack
  routes:
    - match:
        team: payments
      receiver: payments-pagerduty
    - match:
        team: marketing
      receiver: marketing-slack
    - match:
        severity: critical
      receiver: platform-pagerduty

receivers:
  - name: payments-pagerduty
    pagerduty_configs:
      - routing_key: <payments_routing_key>
        description: "Airflow alert: {{ .CommonLabels.dag_id }}"
  - name: marketing-slack
    slack_configs:
      - api_url: <marketing_slack_webhook>
        channel: "#marketing-data-alerts"
```

**4. Team-scoped Grafana dashboards via tags:**

```promql
# Each team sees only their own DAG metrics
# Airflow DAG tags become Prometheus labels after StatsD mapping

# Dashboard variable: team = payments
# Query: all metrics where dag_id matches payments_* tag
airflow_dagrun_duration_success_seconds{dag_id=~"payments_.*"}

# SLA compliance rate (should be > 99%)
count(airflow_sla_miss_total{dag_id=~"$team_.*"} == 0)
/ count(airflow_dagrun_duration_success_seconds_count{dag_id=~"$team_.*"})
```

**Rollout strategy for 500 DAGs:**
1. Week 1: Deploy StatsD + Prometheus + basic Grafana (no code changes, infrastructure only)
2. Week 2: Add `instrument_task` decorator to 10 highest-impact DAGs as pilot
3. Week 3: Enable anomaly detection DAG
4. Week 4: Roll out decorator to all DAGs via DAG factory refactor
5. Week 5: Add per-team PagerDuty routing, decommission email alerts

</details>

</article>
