---
title: "Monitoring and Alerting - Fundamentals"
topic: ci-cd
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, monitoring, alerting, observability, slo]
---

# Monitoring and Alerting — Fundamentals


## 🎯 Analogy

Think of CI/CD monitoring like a smoke alarm system: you set threshold alerts (pipeline took >30 min -> yellow, >60 min -> red), route them to the right channel (Slack for warnings, PagerDuty for critical), and review trends weekly to catch degradation before it becomes an incident.

---
## The Smoke Detector Analogy

You don't wait for a building to be on fire before reacting — smoke detectors alert you early. Pipeline monitoring works the same way: sensors detect anomalies early (data volume drops, latency spikes), alerts notify the right people before users feel pain, and automated responses kick in where possible.

---

## What to Monitor

| Layer | What | Alert When |
|---|---|---|
| Infrastructure | CPU, memory, disk | > 80% sustained |
| Pipeline | Task success rate | < 99.5% |
| Pipeline | Duration | > 2× baseline |
| Data | Row count | < 90% of yesterday |
| Data | Null rate | Increases by > 5% |
| Data | Freshness | Not updated within SLA |

---

## Airflow SLA Monitoring

```python
from datetime import timedelta

default_args = {
    "sla": timedelta(hours=2),       # alert if task > 2 hours
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@company.com"],
}
```

---

## Prometheus Metrics for Pipelines

```python
from prometheus_client import Counter, Histogram, start_http_server

ROWS = Counter("pipeline_rows_total", "Rows processed", ["pipeline", "status"])
DURATION = Histogram("pipeline_duration_seconds", "Duration", ["pipeline"])

start_http_server(9090)  # Prometheus scrapes :9090/metrics

with DURATION.labels(pipeline="revenue").time():
    rows = run_pipeline()
    ROWS.labels(pipeline="revenue", status="success").inc(rows)
```

---

## Alert Rule Example

```yaml
# Prometheus alerting rule
groups:
  - name: pipelines
    rules:
      - alert: PipelineHighErrorRate
        expr: rate(pipeline_rows_total{status="error"}[5m]) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} error rate > 1%"

      - alert: TableStale
        expr: table_freshness_hours > 4
        labels:
          severity: warning
        annotations:
          summary: "Table {{ $labels.table }} not refreshed in > 4 hours"
```

---

## Key Monitoring Principle

Alert on **symptoms** (users are impacted), not causes. A slow query is a cause; a dashboard showing wrong numbers is a symptom. Page on symptoms; log causes for investigation.

## ▶️ Try It Yourself

```yaml
# CloudWatch alarm for Airflow DAG SLA breach
# (Terraform/CDK equivalent)

# .github/workflows with Slack notification on failure
name: ETL Pipeline
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6am

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - name: Run dbt
        run: dbt run --target prod
      - name: Run dbt tests
        run: dbt test --target prod

  notify-on-failure:
    runs-on: ubuntu-latest
    needs: run-pipeline
    if: failure()
    steps:
      - name: Slack alert
        uses: slackapi/slack-github-action@v1
        with:
          payload: |
            {
              "text": ":red_circle: Pipeline FAILED on ${{ github.ref }}. Run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
            }
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_DATA_ALERTS_WEBHOOK }}
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
