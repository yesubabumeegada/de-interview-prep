---
title: "Anomaly Detection — Real World"
topic: data-quality
subtopic: anomaly-detection
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [anomaly-detection, production, airflow, monitoring, alerting]
---

# Anomaly Detection — Real World Patterns

## Pattern 1: Daily Anomaly Check in Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import sqlalchemy as sa

def compute_daily_metrics(**context):
    """Compute metrics for yesterday's data."""
    ds = context["ds"]
    engine = sa.create_engine("postgresql://user:pass@host/db")
    
    with engine.connect() as conn:
        metrics = conn.execute(sa.text(f"""
            SELECT
                COUNT(*) as row_count,
                AVG(amount) as mean_amount,
                SUM(amount) as total_revenue,
                COUNT(CASE WHEN amount IS NULL THEN 1 END) * 100.0 / COUNT(*) as null_pct,
                MAX(created_at) as latest_record
            FROM orders
            WHERE DATE(created_at) = '{ds}'
        """)).fetchone()._asdict()
    
    context["ti"].xcom_push(key="metrics", value=metrics)
    return metrics

def check_anomalies(**context):
    """Compare today's metrics against rolling 28-day baseline."""
    ds = context["ds"]
    today_metrics = context["ti"].xcom_pull(key="metrics")
    engine = sa.create_engine("postgresql://user:pass@host/db")
    
    with engine.connect() as conn:
        history = conn.execute(sa.text(f"""
            SELECT row_count, mean_amount, total_revenue
            FROM dq_metrics_daily
            WHERE metric_date >= '{ds}'::date - INTERVAL '35 days'
              AND metric_date < '{ds}'::date
            ORDER BY metric_date
        """)).fetchall()
    
    if len(history) < 7:
        print("Insufficient history for anomaly detection, skipping")
        return
    
    hist_df = pd.DataFrame(history, columns=["row_count", "mean_amount", "total_revenue"])
    
    alerts = []
    for metric in ["row_count", "total_revenue"]:
        historical_vals = hist_df[metric].dropna().values
        current_val = today_metrics[metric]
        
        mean, std = np.mean(historical_vals), np.std(historical_vals)
        if std == 0:
            continue
        
        z = abs(current_val - mean) / std
        if z > 3.0:
            pct_change = (current_val - mean) / mean * 100
            alerts.append({
                "metric": metric,
                "current": current_val,
                "baseline": round(mean, 2),
                "z_score": round(z, 2),
                "pct_change": round(pct_change, 1),
            })
    
    if alerts:
        message = f"DQ Anomaly Alert for {ds}:\n"
        for a in alerts:
            message += f"  - {a['metric']}: {a['current']:,.0f} (baseline: {a['baseline']:,.0f}, z={a['z_score']}, change={a['pct_change']:+.1f}%)\n"
        
        # In production: send to Slack/PagerDuty
        print(message)
        raise ValueError(f"Anomalies detected: {[a['metric'] for a in alerts]}")

with DAG("daily_anomaly_check", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    compute = PythonOperator(task_id="compute_metrics", python_callable=compute_daily_metrics)
    check = PythonOperator(task_id="check_anomalies", python_callable=check_anomalies)
    compute >> check
```

---

## Pattern 2: Real-Time Streaming Anomaly Detection

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time

class OrderVolumeAnomalyDetector(ProcessWindowFunction):
    """Flink: detect order volume anomalies in 5-minute windows."""
    
    def process(self, key, context, elements, out):
        orders = list(elements)
        current_count = len(orders)
        
        # In production: fetch baseline from state backend
        baseline_mean = self.get_baseline_mean(key)
        baseline_std = self.get_baseline_std(key)
        
        if baseline_std > 0:
            z_score = abs(current_count - baseline_mean) / baseline_std
            if z_score > 3.0:
                out.collect({
                    "window_start": context.window().start,
                    "window_end": context.window().end,
                    "category": key,
                    "order_count": current_count,
                    "z_score": z_score,
                    "anomaly": True,
                })

env = StreamExecutionEnvironment.get_execution_environment()
```

---

## Pattern 3: Monte Carlo-Style Automated Monitoring

```python
class AutoMonitor:
    """Automatically discover and monitor all tables in a catalog."""
    
    def __init__(self, catalog_client, metrics_store, alert_client):
        self.catalog = catalog_client
        self.metrics = metrics_store
        self.alerts = alert_client
    
    def run_for_all_tables(self):
        tables = self.catalog.list_tables()
        
        for table in tables:
            try:
                self.monitor_table(table)
            except Exception as e:
                print(f"Failed to monitor {table}: {e}")
    
    def monitor_table(self, table_name: str):
        # Auto-detect columns to monitor based on type
        schema = self.catalog.get_schema(table_name)
        
        for col in schema["columns"]:
            if col["type"] in ("timestamp", "datetime"):
                self._check_freshness(table_name, col["name"])
            elif col["type"] in ("integer", "float", "decimal"):
                self._check_numeric_distribution(table_name, col["name"])
            elif col["type"] == "string":
                self._check_null_rate(table_name, col["name"])
        
        self._check_row_count(table_name)
    
    def _check_row_count(self, table_name: str):
        current_count = self.metrics.get_latest_row_count(table_name)
        history = self.metrics.get_row_count_history(table_name, days=30)
        
        if len(history) < 7:
            return
        
        mean, std = np.mean(history), np.std(history)
        z = abs(current_count - mean) / max(std, 1)
        
        if z > 3.0:
            self.alerts.send(
                severity="critical",
                table=table_name,
                message=f"Row count anomaly: {current_count:,} vs baseline {mean:,.0f} (z={z:.1f})",
            )
```

---

## Common Anomaly Detection Patterns and Results

| Scenario | Detection Method | Threshold | Action |
|---|---|---|---|
| Daily row count drop | Z-score, day-of-week adjusted | z > 3.0 | PagerDuty |
| NULL rate spike | Z-score on null_pct | z > 2.5 | Slack alert |
| Revenue out of range | IQR-based bounds | Outside 1.5×IQR | Slack + Jira |
| Table not updated | Max timestamp age | > SLA hours | PagerDuty |
| New unexpected column | Schema comparison | Any new column | Slack alert |
| Mean shift in amount | T-test vs baseline | p < 0.05 | Slack alert |
