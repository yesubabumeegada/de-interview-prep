---
title: "Data Observability Tools - Senior Deep Dive"
topic: data-quality
subtopic: observability-tools
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-quality, observability, architecture, monte-carlo, incident-management, senior]
---

# Data Observability Tools — Senior Deep Dive

## Designing an Observability Platform from Scratch

For teams choosing to build rather than buy, the components needed for production-grade observability:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Observability Platform                        │
├──────────────┬──────────────┬───────────────┬───────────────────┤
│   Collector  │   Storage    │   Detection   │   Notification    │
│              │              │               │                   │
│ - dbt hooks  │ pipeline_    │ - Z-score     │ - PagerDuty       │
│ - Airflow    │ metrics      │ - IQR         │ - Slack           │
│   callbacks  │ schema_      │ - Seasonal    │ - Email           │
│ - Custom     │ snapshots    │   decomp      │ - Incident mgmt   │
│   profilers  │ incidents    │ - ML models   │                   │
└──────────────┴──────────────┴───────────────┴───────────────────┘
```

---

## Automated Anomaly Detection with Seasonal Decomposition

For tables with strong weekly seasonality, STL decomposition outperforms simple z-scores:

```python
# plugins/monitors/seasonal_detector.py
from statsmodels.tsa.seasonal import STL
import numpy as np
import pandas as pd
from google.cloud import bigquery

def detect_seasonal_anomaly(
    table_id: str,
    lookback_days: int = 90,
    anomaly_threshold: float = 3.0
) -> dict:
    """
    Use STL decomposition to detect anomalies accounting for trend + seasonality.
    Requires statsmodels: pip install statsmodels
    """
    client = bigquery.Client()
    
    query = f"""
    SELECT
        DATE(_partitiontime) AS partition_date,
        COUNT(*) AS row_count
    FROM `{table_id}`
    WHERE DATE(_partitiontime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    GROUP BY 1
    ORDER BY 1
    """
    
    df = client.query(query).to_dataframe()
    df = df.set_index("partition_date").asfreq("D").fillna(method="ffill")
    
    if len(df) < 14:  # Need at least 2 weeks for seasonal decomposition
        return {"status": "insufficient_data"}
    
    stl = STL(df["row_count"], period=7, robust=True)
    result = stl.fit()
    
    # Residuals represent unexplained variation after trend + seasonality
    residuals = result.resid
    mad = np.median(np.abs(residuals - np.median(residuals)))
    modified_z_scores = 0.6745 * (residuals - np.median(residuals)) / (mad + 1e-10)
    
    today_residual = residuals.iloc[-1]
    today_z = modified_z_scores.iloc[-1]
    
    return {
        "table": table_id,
        "today_row_count": int(df["row_count"].iloc[-1]),
        "expected_row_count": int(result.trend.iloc[-1] + result.seasonal.iloc[-1]),
        "residual": float(today_residual),
        "modified_z_score": float(today_z),
        "is_anomaly": abs(today_z) > anomaly_threshold,
        "trend": float(result.trend.iloc[-1]),
        "seasonal_component": float(result.seasonal.iloc[-1]),
    }
```

---

## Incident Management Workflow

### Incident Data Model

```sql
CREATE TABLE monitoring.data_incidents (
    incident_id STRING DEFAULT GENERATE_UUID(),
    title STRING NOT NULL,
    severity STRING,              -- 'P1', 'P2', 'P3', 'P4'
    status STRING,                -- 'open', 'investigating', 'resolved', 'closed'
    affected_tables ARRAY<STRING>,
    affected_dashboards ARRAY<STRING>,
    root_cause_category STRING,   -- 'source_data', 'pipeline_failure', 'schema_change', 'infra'
    root_cause_description STRING,
    detected_at TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    time_to_acknowledge_minutes FLOAT64
        AS (TIMESTAMP_DIFF(acknowledged_at, detected_at, MINUTE)),
    time_to_resolve_minutes FLOAT64
        AS (TIMESTAMP_DIFF(resolved_at, detected_at, MINUTE)),
    incident_commander STRING,
    postmortem_url STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP
)
PARTITION BY DATE(detected_at);
```

### Automated Incident Creation

```python
# plugins/incident_manager.py
import uuid
from datetime import datetime, timezone
from typing import Optional
from google.cloud import bigquery
import requests

class DataIncidentManager:
    
    SEVERITY_MAP = {
        "fct_payments": "P1",
        "fct_orders": "P1",
        "fct_revenue": "P1",
        "dim_customers": "P2",
    }
    
    def __init__(self, bq_client: bigquery.Client, pagerduty_key: str, slack_webhook: str):
        self.bq = bq_client
        self.pd_key = pagerduty_key
        self.slack_webhook = slack_webhook
    
    def open_incident(
        self,
        title: str,
        affected_tables: list[str],
        anomaly_details: dict,
        auto_assign: Optional[str] = None,
    ) -> str:
        incident_id = str(uuid.uuid4())
        severity = self._determine_severity(affected_tables)
        
        row = {
            "incident_id": incident_id,
            "title": title,
            "severity": severity,
            "status": "open",
            "affected_tables": affected_tables,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        
        self.bq.insert_rows_json("project.monitoring.data_incidents", [row])
        
        if severity == "P1":
            self._page_oncall(incident_id, title, affected_tables, severity)
        
        self._notify_slack(incident_id, title, severity, affected_tables, anomaly_details)
        
        return incident_id
    
    def _determine_severity(self, affected_tables: list[str]) -> str:
        for table in affected_tables:
            short_name = table.split(".")[-1]
            if short_name in self.SEVERITY_MAP:
                return self.SEVERITY_MAP[short_name]
        return "P3"
    
    def _page_oncall(self, incident_id, title, tables, severity):
        requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json={
                "routing_key": self.pd_key,
                "event_action": "trigger",
                "dedup_key": incident_id,
                "payload": {
                    "summary": f"[{severity}] Data Incident: {title}",
                    "severity": "critical",
                    "source": "data-observability",
                    "custom_details": {
                        "incident_id": incident_id,
                        "affected_tables": tables,
                        "runbook": "https://wiki/runbooks/data-incidents",
                    }
                }
            }
        )
    
    def resolve_incident(self, incident_id: str, root_cause: str, resolution: str):
        self.bq.query(f"""
            UPDATE monitoring.data_incidents
            SET
                status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP(),
                root_cause_description = '{root_cause}'
            WHERE incident_id = '{incident_id}'
        """).result()
```

---

## Lineage-Based Impact Analysis

When an anomaly fires, the most urgent question is: what's the blast radius?

### Building a Lineage Graph from dbt Manifest

```python
# plugins/lineage/impact_analyzer.py
import json
from pathlib import Path
from collections import defaultdict, deque

class LineageGraph:
    
    def __init__(self, manifest_path: str):
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        self.nodes = manifest["nodes"]
        self.sources = manifest["sources"]
        
        # Build adjacency: node -> list of downstream nodes
        self.downstream: dict[str, list[str]] = defaultdict(list)
        for node_id, node in self.nodes.items():
            for dep in node.get("depends_on", {}).get("nodes", []):
                self.downstream[dep].append(node_id)
    
    def get_all_downstream(self, table_name: str) -> list[str]:
        """BFS to find all downstream models affected by a table."""
        # Find matching node IDs
        start_nodes = [
            nid for nid in {**self.nodes, **self.sources}
            if table_name in nid
        ]
        
        if not start_nodes:
            return []
        
        visited = set()
        queue = deque(start_nodes)
        downstream_models = []
        
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for downstream in self.downstream.get(current, []):
                downstream_models.append(downstream)
                queue.append(downstream)
        
        return downstream_models
    
    def get_impact_summary(self, table_name: str) -> dict:
        downstream = self.get_all_downstream(table_name)
        marts = [n for n in downstream if "marts" in n]
        return {
            "source_table": table_name,
            "total_downstream_models": len(downstream),
            "affected_marts": [n.split(".")[-1] for n in marts],
        }

# Usage
graph = LineageGraph("target/manifest.json")
impact = graph.get_impact_summary("stg_stripe_charges")
# → {"source_table": "stg_stripe_charges", "total_downstream_models": 12, "affected_marts": ["fct_revenue", "fct_payments"]}
```

---

## SLA Monitoring with Observability Data

```sql
-- models/monitoring/observability_sla_report.sql
WITH daily_anomalies AS (
    SELECT
        DATE(measured_at) AS metric_date,
        table_id,
        metric_name,
        COUNTIF(is_anomaly) AS anomaly_count,
        COUNT(*) AS total_checks,
        COUNTIF(is_anomaly) / COUNT(*) AS anomaly_rate
    FROM monitoring.pipeline_metrics
    WHERE measured_at >= CURRENT_DATE() - 30
    GROUP BY 1, 2, 3
),
table_availability AS (
    SELECT
        table_id,
        metric_date,
        -- "available" = no volume anomaly or freshness failure that day
        NOT BOOL_OR(is_anomaly AND metric_name IN ('row_count', 'freshness_minutes')) AS was_available
    FROM daily_anomalies
    GROUP BY 1, 2
)
SELECT
    table_id,
    COUNT(*) AS days_monitored,
    COUNTIF(was_available) AS days_available,
    ROUND(COUNTIF(was_available) / COUNT(*) * 100, 2) AS availability_pct,
    30 - COUNTIF(was_available) AS days_with_issues
FROM table_availability
GROUP BY 1
ORDER BY availability_pct ASC;
```

---

## Quantifying the Cost of Bad Data

Interviewers at senior levels ask: "How do you make the case for data observability investment?"

**Framework:**

1. **Identify high-cost incidents:** Pull post-mortems from the last 12 months. Estimate engineer-hours per incident × hourly cost.

2. **Measure MTTD and MTTR:** Monte Carlo publishes industry benchmarks: average MTTD (mean time to detect) without observability = 14 hours; with observability = 1.2 hours.

3. **Quantify downstream harm:**
   - Bad data in finance reports → delayed close → $X compliance risk
   - Bad data in ML features → model degradation → revenue impact
   - Bad data in customer-facing systems → chargebacks or SLA penalties

4. **Formula:**
   ```
   Annual savings = (incidents_per_year × avg_hours_per_incident × hourly_eng_cost) × MTTD_reduction_factor
   
   Example:
   24 incidents/year × 8 hours × $150/hour × (14 - 1.2) / 14 = $165,000
   
   vs. Monte Carlo cost: $80,000/year → ROI in < 6 months
   ```

---

## Key Interview Points

- STL (Seasonal-Trend decomposition using LOESS) outperforms z-scores for weekly-seasonal data
- Incident management needs a data model: detected_at, acknowledged_at, resolved_at → enables MTTD/MTTR tracking
- Lineage-based blast radius analysis (BFS on dbt manifest) is the critical "so what" after detecting an anomaly
- Quantify observability ROI: MTTD reduction × incident frequency × engineer cost
- Schema change detection: store daily snapshots, compare JSON representations, alert on diffs
- Open incidents automatically via PagerDuty for P1/P2; route P3/P4 to Slack for next-business-day review
