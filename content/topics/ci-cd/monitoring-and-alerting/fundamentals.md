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
