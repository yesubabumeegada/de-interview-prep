---
title: "Incident Management — Real World"
topic: data-quality
subtopic: incident-management
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [incident-management, production, airflow, rollback, communication]
---

# Incident Management — Real World Patterns

## Pattern 1: Airflow SLA Miss + Auto-Remediation

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def notify_sla_miss(context):
    """Airflow SLA miss callback — called by framework."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    
    # 1. Calculate how late we are
    sla = context["task"].sla
    scheduled_time = execution_date + sla
    now = datetime.utcnow()
    late_by = (now - scheduled_time.replace(tzinfo=None)).total_seconds() / 60
    
    # 2. Post to Slack
    requests.post(
        "https://hooks.slack.com/services/xxx/yyy/zzz",
        json={
            "text": (
                f":warning: *SLA MISS*: `{dag_id}/{task_id}`\n"
                f"Execution: {execution_date}\n"
                f"Late by: {late_by:.0f} minutes\n"
                f"<https://airflow.company.com/task?dag_id={dag_id}|View in Airflow>"
            )
        }
    )
    
    # 3. Auto-retry if within grace period (< 30 min late)
    if late_by < 30:
        context["task_instance"].clear()  # Triggers retry
        print(f"Auto-retry triggered for {task_id}")


def on_failure_callback(context):
    """Task failure callback — immediate alert + attempt remediation."""
    exception = context.get("exception")
    task_instance = context["task_instance"]
    
    # Check if OOM (common recoverable failure)
    if exception and "OutOfMemoryError" in str(exception):
        # Trigger high-memory rerun via separate DAG
        from airflow.api.client.local_client import Client
        client = Client(None, None)
        client.trigger_dag(
            dag_id="orders_pipeline_large_cluster",
            conf={"original_run_id": task_instance.run_id},
        )
        print("OOM detected: triggered large cluster rerun")


with DAG(
    "orders_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    sla_miss_callback=notify_sla_miss,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    }
) as dag:
    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform,
        sla=timedelta(hours=2),
    )
```

---

## Pattern 2: Rollback Strategy

```python
from datetime import date, timedelta
import sqlalchemy as sa

class TableRollback:
    """Roll back a Delta/Iceberg table to a previous version."""
    
    def __init__(self, spark, engine):
        self.spark = spark
        self.engine = engine
    
    def get_versions(self, table_path: str, n: int = 10) -> list:
        """List available table versions."""
        history = self.spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
        return history.select("version", "timestamp", "operation", "operationParameters").limit(n).collect()
    
    def rollback_delta(self, table_path: str, to_version: int):
        """Roll back a Delta table to a specific version."""
        print(f"Rolling back {table_path} to version {to_version}")
        
        # Create backup of current version
        current_version = self.spark.sql(
            f"DESCRIBE HISTORY delta.`{table_path}` LIMIT 1"
        ).collect()[0]["version"]
        
        backup_path = f"{table_path}_backup_v{current_version}"
        self.spark.read.format("delta").load(table_path).write.format("delta").save(backup_path)
        print(f"Backup saved to {backup_path}")
        
        # Restore
        self.spark.sql(f"""
            RESTORE TABLE delta.`{table_path}` TO VERSION AS OF {to_version}
        """)
        
        print(f"Rollback complete: {table_path} → version {to_version}")
    
    def rollback_by_date(self, table_path: str, target_date: date):
        """Roll back to the most recent version before a given date."""
        versions = self.get_versions(table_path, n=30)
        
        target_version = None
        for v in versions:
            if v["timestamp"].date() < target_date:
                target_version = v["version"]
                break
        
        if target_version is None:
            raise ValueError(f"No version found before {target_date}")
        
        self.rollback_delta(table_path, target_version)
    
    def verify_rollback(self, table_path: str, expected_row_count: int) -> bool:
        actual = self.spark.read.format("delta").load(table_path).count()
        ok = abs(actual - expected_row_count) / max(expected_row_count, 1) < 0.05
        print(f"Rollback verification: {'PASS' if ok else 'FAIL'} (expected: {expected_row_count:,}, actual: {actual:,})")
        return ok


# Usage
rollback = TableRollback(spark, engine)

# List versions to find the good one
versions = rollback.get_versions("s3://bucket/gold/orders")
for v in versions:
    print(f"v{v['version']}: {v['timestamp']} ({v['operation']})")

# Roll back to yesterday's version
rollback.rollback_by_date("s3://bucket/gold/orders", target_date=date.today() - timedelta(days=1))
rollback.verify_rollback("s3://bucket/gold/orders", expected_row_count=500_000)
```

---

## Pattern 3: Incident Communication Templates

```python
class IncidentCommunicator:
    """Standardized communication for data incidents."""
    
    @staticmethod
    def opening_message(incident_id: str, summary: str, affected: list, owner: str) -> str:
        return (
            f":red_circle: *DATA INCIDENT STARTED* | `{incident_id}`\n\n"
            f"*Summary:* {summary}\n"
            f"*Affected:* {', '.join(affected)}\n"
            f"*Responder:* {owner}\n"
            f"*Status:* Investigating\n\n"
            f"Updates will be posted every 15 minutes in this thread."
        )
    
    @staticmethod
    def update_message(incident_id: str, status: str, details: str, eta: str = None) -> str:
        eta_line = f"\n*ETA for resolution:* {eta}" if eta else ""
        return (
            f":information_source: *UPDATE* | `{incident_id}`\n"
            f"*Status:* {status}\n"
            f"*Details:* {details}{eta_line}"
        )
    
    @staticmethod
    def resolution_message(incident_id: str, root_cause: str, duration_min: float) -> str:
        return (
            f":white_check_mark: *INCIDENT RESOLVED* | `{incident_id}`\n\n"
            f"*Duration:* {duration_min:.0f} minutes\n"
            f"*Root cause:* {root_cause}\n"
            f"*Postmortem:* Will be published within 24 hours\n\n"
            f"Thank you for your patience."
        )

# Usage flow during real incident
comm = IncidentCommunicator()

# T+0: Incident starts
slack.post("#data-incidents", comm.opening_message(
    "INC-2024-042", "Orders pipeline failing, dashboard stale",
    ["gold.orders", "revenue_dashboard"], "@jane-smith"
))

# T+15: Update
slack.post("#data-incidents", comm.update_message(
    "INC-2024-042", "Root cause identified",
    "OOM error during flash sale volume spike. Relaunching with 2x cluster.",
    eta="~45 minutes from now"
))

# T+90: Resolved
slack.post("#data-incidents", comm.resolution_message(
    "INC-2024-042", "Spark cluster OOM — static cluster not sized for peak load", 90
))
```

---

## Incident Anti-Patterns

| Anti-Pattern | Impact | Better Approach |
|---|---|---|
| No runbooks for known issues | Slow MTTR, senior engineer always needed | Document every recurring failure type |
| Blame-focused postmortems | People hide mistakes | Blameless postmortems, focus on systems |
| No postmortem action follow-through | Same incidents repeat | Track action items in Jira, review in sprint |
| Alert during first occurrence only | Flapping issues missed | Alert on sustained firing (N consecutive checks) |
| Single point of contact on-call | Bus factor of 1 | Rotating on-call, detailed runbooks |
| Fix without root cause | Same issue recurs in 2 weeks | Always complete 5 Whys before closing |
