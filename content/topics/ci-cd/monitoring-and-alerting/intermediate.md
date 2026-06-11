---
title: "Monitoring and Alerting - Intermediate"
topic: ci-cd
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, monitoring,alerting,observability,prometheus]
---

# Monitoring and Alerting — Intermediate

See fundamentals for core concepts. This section covers intermediate patterns and real-world implementation.

## Data Quality Monitoring in Production

```python
from great_expectations.checkpoint import Checkpoint
import great_expectations as gx

context = gx.get_context()

def run_quality_gate(table_name: str, date: str) -> bool:
    """Run GE checkpoint; return True if passed."""
    result = context.run_checkpoint(
        checkpoint_name=f"{table_name}_checkpoint",
        batch_request={"table_name": table_name, "partition": date},
    )
    if not result["success"]:
        send_slack_alert(
            channel="#data-quality",
            message=f"Quality gate FAILED for {table_name} on {date}\n{result['statistics']}",
        )
    return result["success"]
```

## Airflow Task Duration Monitoring

```python
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl

class DurationMonitorListener:
    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session):
        duration = task_instance.duration
        baseline = get_historical_p95_duration(task_instance.task_id)
        if duration > baseline * 2:
            alert_slack(f"Task {task_instance.task_id} took {duration:.0f}s (baseline p95: {baseline:.0f}s)")
```

## SLO Definition and Monitoring

```yaml
# SLO: revenue pipeline must complete by 8 AM
SLO:
  pipeline: revenue_daily
  metric: completion_time
  target: "< 08:00 UTC"
  error_budget: "99.5%"  # can miss 0.5% of runs

# Alerting: page if SLO at risk
alert_policy:
  - if: error_budget_remaining < 20%  # burn rate alert
    action: page_on_call
  - if: completion_time > 08:00
    action: slack_data_team
```
