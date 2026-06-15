---
title: "Data Observability Tools - Intermediate"
topic: data-quality
subtopic: observability-tools
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-quality, observability, anomaly-detection, monitoring, dbt, airflow]
---

# Data Observability Tools — Intermediate

## Implementing Observability Without Commercial Tools

Many teams can't justify $50K+/year for Monte Carlo. Here's how to build a solid observability layer with dbt, Airflow, and custom monitors.

### Architecture

```
Airflow DAG
  ├── dbt source freshness
  ├── dbt build (run + test)
  ├── custom_volume_monitor (Python operator)
  ├── custom_distribution_monitor (Python operator)
  └── alert_on_anomalies (Python operator)
              ↓
    monitoring.pipeline_metrics (BigQuery table)
              ↓
    Looker dashboard + Slack alerts
```

---

## Volume Monitoring: Z-Score Based

```python
# plugins/monitors/volume_monitor.py
from google.cloud import bigquery
import statistics
import math

client = bigquery.Client()

def check_volume_anomaly(
    table_id: str,
    lookback_days: int = 30,
    z_score_threshold: float = 3.0,
    partition_col: str = "DATE(_partitiontime)"
) -> dict:
    """
    Compare today's row count to rolling 30-day average.
    Returns anomaly report dict.
    """
    history_query = f"""
    SELECT
        {partition_col} AS partition_date,
        COUNT(*) AS row_count
    FROM `{table_id}`
    WHERE {partition_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
      AND {partition_col} < CURRENT_DATE()
    GROUP BY 1
    ORDER BY 1
    """
    
    today_query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{table_id}`
    WHERE {partition_col} = CURRENT_DATE()
    """
    
    history = [r["row_count"] for r in client.query(history_query).result()]
    today_count = list(client.query(today_query).result())[0]["row_count"]
    
    if len(history) < 7:
        return {"status": "insufficient_data", "table": table_id}
    
    mean = statistics.mean(history)
    stdev = statistics.stdev(history)
    
    if stdev == 0:
        return {"status": "no_variance", "table": table_id}
    
    z_score = (today_count - mean) / stdev
    is_anomaly = abs(z_score) > z_score_threshold
    
    return {
        "table": table_id,
        "today_count": today_count,
        "historical_mean": round(mean, 0),
        "z_score": round(z_score, 2),
        "is_anomaly": is_anomaly,
        "direction": "low" if z_score < 0 else "high",
    }
```

---

## Distribution Monitoring: Null Rate and Cardinality

```python
# plugins/monitors/distribution_monitor.py
from google.cloud import bigquery
from dataclasses import dataclass
from typing import Optional

@dataclass
class ColumnMetrics:
    column_name: str
    null_rate: float
    cardinality: int
    min_val: Optional[str]
    max_val: Optional[str]
    mean_val: Optional[float]

def profile_column(table_id: str, column_name: str, data_type: str) -> ColumnMetrics:
    client = bigquery.Client()
    
    base_stats = f"""
    SELECT
        COUNTIF({column_name} IS NULL) / COUNT(*) AS null_rate,
        COUNT(DISTINCT {column_name}) AS cardinality
    FROM `{table_id}`
    WHERE DATE(_partitiontime) = CURRENT_DATE()
    """
    
    numeric_stats = ""
    if data_type in ("INT64", "FLOAT64", "NUMERIC"):
        numeric_stats = f", MIN({column_name}) AS min_val, MAX({column_name}) AS max_val, AVG({column_name}) AS mean_val"
    
    query = f"SELECT * {numeric_stats} FROM ({base_stats})"
    row = list(client.query(query).result())[0]
    
    return ColumnMetrics(
        column_name=column_name,
        null_rate=float(row["null_rate"]),
        cardinality=int(row["cardinality"]),
        min_val=str(row.get("min_val", "")),
        max_val=str(row.get("max_val", "")),
        mean_val=float(row["mean_val"]) if row.get("mean_val") else None,
    )

def detect_null_rate_spike(
    current: ColumnMetrics,
    baseline_null_rate: float,
    threshold: float = 0.10
) -> bool:
    """Alert if null rate increased by more than threshold (absolute)."""
    return (current.null_rate - baseline_null_rate) > threshold
```

---

## Storing Metrics for Trend Analysis

```sql
-- Create the monitoring table
CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    metric_id STRING,
    table_id STRING,
    column_name STRING,
    metric_name STRING,    -- 'row_count', 'null_rate', 'cardinality', 'z_score'
    metric_value FLOAT64,
    is_anomaly BOOL,
    anomaly_direction STRING,
    measured_at TIMESTAMP,
    run_id STRING
)
PARTITION BY DATE(measured_at)
OPTIONS (partition_expiration_days = 365);
```

```python
# Save metrics after each pipeline run
def save_metrics(metrics: list[dict], run_id: str):
    import uuid
    from datetime import datetime, timezone
    from google.cloud import bigquery
    
    client = bigquery.Client()
    rows = [
        {
            "metric_id": str(uuid.uuid4()),
            "measured_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            **m
        }
        for m in metrics
    ]
    client.insert_rows_json("project.monitoring.pipeline_metrics", rows)
```

---

## Schema Change Detection

```python
# plugins/monitors/schema_monitor.py
from google.cloud import bigquery
import json
from datetime import date

def get_schema_snapshot(table_id: str) -> list[dict]:
    client = bigquery.Client()
    table = client.get_table(table_id)
    return [
        {
            "name": field.name,
            "field_type": field.field_type,
            "mode": field.mode,
        }
        for field in table.schema
    ]

def detect_schema_changes(current: list[dict], previous: list[dict]) -> dict:
    current_cols = {c["name"]: c for c in current}
    previous_cols = {c["name"]: c for c in previous}
    
    added = [c for c in current_cols if c not in previous_cols]
    removed = [c for c in previous_cols if c not in current_cols]
    type_changed = [
        c for c in current_cols
        if c in previous_cols
        and current_cols[c]["field_type"] != previous_cols[c]["field_type"]
    ]
    
    return {
        "has_changes": bool(added or removed or type_changed),
        "added_columns": added,
        "removed_columns": removed,
        "type_changes": type_changed,
    }
```

```sql
-- Store schema snapshots for comparison
CREATE TABLE IF NOT EXISTS monitoring.schema_snapshots (
    table_id STRING,
    snapshot_date DATE,
    schema_json STRING,  -- JSON array of column definitions
    captured_at TIMESTAMP
);

-- Query to find schema changes
SELECT
    curr.table_id,
    curr.snapshot_date AS current_date,
    prev.snapshot_date AS previous_date,
    curr.schema_json AS current_schema,
    prev.schema_json AS previous_schema
FROM monitoring.schema_snapshots curr
JOIN monitoring.schema_snapshots prev
    ON curr.table_id = prev.table_id
    AND prev.snapshot_date = curr.snapshot_date - 1
WHERE curr.schema_json != prev.schema_json
ORDER BY curr.captured_at DESC;
```

---

## Seasonal Anomaly Detection (IQR Method)

Z-scores assume normality. For data with weekly seasonality, use day-of-week bucketed IQR:

```python
def check_volume_with_seasonality(
    table_id: str,
    day_of_week: int,  # 0=Mon, 6=Sun
    lookback_weeks: int = 8
) -> dict:
    """Compare today's count only to same day of week historically."""
    client = bigquery.Client()
    
    query = f"""
    WITH daily_counts AS (
        SELECT
            DATE(_partitiontime) AS partition_date,
            EXTRACT(DAYOFWEEK FROM DATE(_partitiontime)) AS dow,
            COUNT(*) AS row_count
        FROM `{table_id}`
        WHERE DATE(_partitiontime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_weeks * 7} DAY)
          AND DATE(_partitiontime) < CURRENT_DATE()
        GROUP BY 1, 2
    )
    SELECT row_count
    FROM daily_counts
    WHERE dow = {day_of_week + 1}  -- BigQuery: 1=Sun, 7=Sat
    ORDER BY partition_date DESC
    LIMIT {lookback_weeks}
    """
    
    import statistics
    historical = [r["row_count"] for r in client.query(query).result()]
    
    if len(historical) < 4:
        return {"status": "insufficient_data"}
    
    q1 = statistics.quantiles(historical, n=4)[0]
    q3 = statistics.quantiles(historical, n=4)[2]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    return {
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "historical_median": statistics.median(historical),
    }
```

---

## Build vs Buy Decision Framework

| Factor | Build | Buy (Monte Carlo/Bigeye) |
|---|---|---|
| **Budget** | < $20K/year | $50K–$200K/year |
| **Team size** | 1–3 engineers | 5+ engineers / dedicated DE team |
| **Time to value** | 4–8 weeks | 1–2 weeks |
| **Customization** | Full control | Limited to platform features |
| **Lineage** | Manual/dbt only | Automated from query logs |
| **Anomaly detection** | Manual z-score/IQR | ML-based, auto-learns |
| **Maintenance** | You own it | Vendor manages |

**Rule of thumb:** If you have fewer than 50 tables and one DE, build it. If you have 500+ tables, multiple teams, and SLA commitments, buy it — the ROI justifies the cost within months.

---

## Key Interview Points

- Z-score anomaly detection assumes normality; IQR is more robust for skewed data
- Day-of-week bucketing prevents false alerts on legitimate weekly seasonality
- Schema snapshots stored in a monitoring table enable automated change detection
- The build vs buy decision hinges on table count, team size, and time-to-value requirements
- Commercial tools (Monte Carlo, Bigeye) auto-parse query logs for lineage — building lineage manually is the hardest part of DIY observability
