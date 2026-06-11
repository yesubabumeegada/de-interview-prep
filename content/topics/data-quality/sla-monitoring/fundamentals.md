---
title: "SLA Monitoring — Fundamentals"
topic: data-quality
subtopic: sla-monitoring
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sla, monitoring, freshness, latency, availability]
---

# SLA Monitoring — Fundamentals


## 🎯 Analogy

Think of SLA monitoring like a train schedule board: each pipeline has a committed arrival time, and a red light appears if the train (data) hasn't arrived by the deadline — enabling on-call teams to investigate before analysts notice stale dashboards.

---
## What Is a Data SLA?

A Data Service Level Agreement (SLA) is a formal commitment that data will meet quality, availability, and timeliness standards. Breaking an SLA has business consequences: wrong decisions, trust erosion, contractual penalties.

```mermaid
flowchart LR
    A[Data SLA Definition] --> B[Monitoring System]
    B --> C{SLA Met?}
    C -- Yes --> D[Green Status]
    C -- No --> E[SLA Breach Alert]
    E --> F[PagerDuty / Slack]
    E --> G[Incident Created]
    G --> H[Resolution + RCA]
    H --> I[SLA Report]
```

---

## The 4 Types of Data SLAs

| SLA Type | Definition | Example |
|----------|-----------|---------|
| **Freshness** | Data must be updated within N minutes/hours | "Orders table updated by 8 AM UTC daily" |
| **Latency** | End-to-end pipeline must complete within N minutes | "Raw → Gold within 30 minutes of source event" |
| **Availability** | System uptime percentage | "Data API available 99.9% of the time" |
| **Quality** | DQ score must exceed a threshold | "≥99.5% completeness on required fields" |

---

## Freshness SLA — Implementation

```python
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional
import sqlalchemy as sa

@dataclass
class FreshnessSLA:
    table_name: str
    max_age_hours: float
    check_column: str = "updated_at"
    severity: str = "critical"  # critical or warning
    owner: str = ""

class FreshnessMonitor:
    def __init__(self, engine):
        self.engine = engine
    
    def check(self, sla: FreshnessSLA) -> dict:
        now = datetime.now(timezone.utc)
        
        with self.engine.connect() as conn:
            max_ts = conn.execute(sa.text(
                f"SELECT MAX({sla.check_column}) FROM {sla.table_name}"
            )).scalar()
        
        if max_ts is None:
            return {
                "table": sla.table_name,
                "status": "BREACH",
                "reason": "Table is empty",
                "severity": sla.severity,
            }
        
        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        
        age_hours = (now - max_ts).total_seconds() / 3600
        is_fresh = age_hours <= sla.max_age_hours
        
        return {
            "table": sla.table_name,
            "status": "OK" if is_fresh else "BREACH",
            "max_timestamp": max_ts.isoformat(),
            "age_hours": round(age_hours, 2),
            "sla_hours": sla.max_age_hours,
            "lag_hours": round(max(0, age_hours - sla.max_age_hours), 2),
            "severity": sla.severity if not is_fresh else "none",
            "owner": sla.owner,
        }
    
    def check_all(self, slas: list[FreshnessSLA]) -> list[dict]:
        results = []
        for sla in slas:
            try:
                results.append(self.check(sla))
            except Exception as e:
                results.append({
                    "table": sla.table_name,
                    "status": "ERROR",
                    "reason": str(e),
                    "severity": "critical",
                })
        return results


# Define SLAs
slas = [
    FreshnessSLA("orders", max_age_hours=1, owner="data-eng@company.com"),
    FreshnessSLA("customers", max_age_hours=24, severity="warning"),
    FreshnessSLA("inventory", max_age_hours=0.5, check_column="last_updated_at", owner="ops@company.com"),
]

engine = sa.create_engine("postgresql://user:pass@host/db")
monitor = FreshnessMonitor(engine)
results = monitor.check_all(slas)

for r in results:
    status_icon = "✓" if r["status"] == "OK" else "✗"
    print(f"{status_icon} {r['table']}: {r['status']} (age={r.get('age_hours', 'N/A')}h)")
```

---

## Airflow SLA Monitoring

Airflow has built-in SLA miss detection:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def send_sla_miss_email(context):
    """Called automatically when a task misses its SLA."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    
    print(f"SLA MISSED: {dag_id}/{task_id} for {execution_date}")
    # In production: send Slack/PagerDuty alert


with DAG(
    "orders_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    sla_miss_callback=send_sla_miss_email,
    dagrun_timeout=timedelta(hours=2),  # Entire DAG must complete within 2h
) as dag:
    
    load_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
        sla=timedelta(minutes=30),  # This task must complete within 30 min
    )
    
    transform_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
        sla=timedelta(hours=1),     # Must complete within 1 hour of scheduled time
    )
    
    load_task >> transform_task
```

---

## SLA Dashboard Queries

```sql
-- SLA breach history (last 30 days)
SELECT
    table_name,
    DATE(checked_at) AS check_date,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'BREACH' THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS availability_pct,
    MAX(lag_hours) AS max_lag_hours
FROM sla_check_results
WHERE checked_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Tables breaching SLA right now
SELECT * FROM sla_check_results
WHERE status = 'BREACH'
  AND checked_at >= NOW() - INTERVAL '10 minutes'
ORDER BY lag_hours DESC;
```

---

## Key Concepts Cheat Sheet

| Term | Definition |
|------|-----------|
| SLA | Committed performance standard |
| SLO | Service Level Objective — internal target (stricter than SLA) |
| SLI | Service Level Indicator — the metric being measured |
| Error budget | How much SLA slack you have left this month |
| Breach | SLA threshold violated |
| Lag | How far behind the data is from its SLA |

---


## ▶️ Try It Yourself

```python
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional

@dataclass
class PipelineSLA:
    pipeline_name: str
    expected_completion: datetime
    tolerance_minutes: int = 30

def check_slas(slas: list[PipelineSLA], actual_completions: dict) -> list[dict]:
    breaches = []
    now = datetime.now()
    for sla in slas:
        deadline = sla.expected_completion + timedelta(minutes=sla.tolerance_minutes)
        completed_at = actual_completions.get(sla.pipeline_name)
        if completed_at is None and now > deadline:
            breaches.append({
                "pipeline": sla.pipeline_name,
                "status": "MISSING",
                "overdue_minutes": int((now - deadline).total_seconds() / 60),
            })
        elif completed_at and completed_at > deadline:
            breaches.append({
                "pipeline": sla.pipeline_name,
                "status": "LATE",
                "overdue_minutes": int((completed_at - deadline).total_seconds() / 60),
            })
    return breaches

slas = [PipelineSLA("orders_daily", datetime(2024, 1, 16, 6, 0))]
actual = {"orders_daily": datetime(2024, 1, 16, 7, 45)}  # 1h45m late
print(check_slas(slas, actual))
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between SLA, SLO, and SLI?" — SLI is the metric (data age). SLO is your internal target (data < 55 min old). SLA is the external commitment (data < 1 hour old). Set SLOs stricter than SLAs so you catch problems before they breach the customer SLA.

> **Tip 2:** "How do you define a freshness SLA for a table?" — Ask: who consumes this data and when? What's the worst acceptable lag? Work backward from the business need (e.g., "operations reviews inventory at 9 AM → inventory must be updated by 8:55 AM → SLA = 24h, measured at 8:55 AM").

> **Tip 3:** "What causes SLA breaches?" — Upstream source delays, pipeline failures or retries, infrastructure issues (throttling, OOM), heavy queries locking tables, schema changes breaking jobs. Monitor each of these independently, not just the final freshness.
