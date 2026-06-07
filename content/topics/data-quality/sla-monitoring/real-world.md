---
title: "SLA Monitoring — Real World"
topic: data-quality
subtopic: sla-monitoring
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [sla, monitoring, production, airflow, pagerduty]
---

# SLA Monitoring — Real World Patterns

## Pattern 1: Centralized SLA Monitor Service

```python
import schedule
import time
import requests
import sqlalchemy as sa
from datetime import datetime
from typing import List

class SLAMonitorService:
    """Runs on a schedule, checks all SLAs, routes alerts."""
    
    def __init__(self, engine, slack_webhook: str, pagerduty_key: str):
        self.engine = engine
        self.slack_webhook = slack_webhook
        self.pagerduty_key = pagerduty_key
        self.slas = self._load_slas_from_db()
    
    def _load_slas_from_db(self) -> list:
        """Load SLA definitions from a catalog table."""
        with self.engine.connect() as conn:
            rows = conn.execute(sa.text("""
                SELECT table_name, check_column, max_age_hours, severity, owner
                FROM data_sla_catalog
                WHERE is_active = TRUE
            """)).fetchall()
        return [dict(row._mapping) for row in rows]
    
    def check_freshness(self, sla: dict) -> dict:
        now = datetime.utcnow()
        with self.engine.connect() as conn:
            max_ts = conn.execute(sa.text(
                f"SELECT MAX({sla['check_column']}) FROM {sla['table_name']}"
            )).scalar()
        
        if not max_ts:
            return {"table": sla["table_name"], "status": "BREACH", "lag_hours": 9999}
        
        age_hours = (now - max_ts).total_seconds() / 3600
        status = "OK" if age_hours <= sla["max_age_hours"] else "BREACH"
        
        result = {**sla, "status": status, "age_hours": round(age_hours, 2),
                  "lag_hours": round(max(0, age_hours - sla["max_age_hours"]), 2)}
        
        # Store result
        self._store_result(result)
        
        return result
    
    def _store_result(self, result: dict):
        with self.engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO sla_check_results
                (table_name, status, age_hours, lag_hours, checked_at)
                VALUES (:table_name, :status, :age_hours, :lag_hours, NOW())
            """), result)
    
    def _send_slack(self, message: str, severity: str):
        color = "#FF0000" if severity == "critical" else "#FFA500"
        requests.post(self.slack_webhook, json={
            "attachments": [{"color": color, "text": message, "mrkdwn_in": ["text"]}]
        })
    
    def _send_pagerduty(self, title: str, details: dict):
        requests.post("https://events.pagerduty.com/v2/enqueue", json={
            "routing_key": self.pagerduty_key,
            "event_action": "trigger",
            "payload": {
                "summary": title,
                "severity": "critical",
                "source": "data-platform",
                "custom_details": details,
            }
        })
    
    def run(self):
        for sla in self.slas:
            result = self.check_freshness(sla)
            
            if result["status"] == "BREACH":
                msg = (
                    f":red_circle: SLA BREACH: `{result['table_name']}`\n"
                    f"Lag: {result['lag_hours']:.1f}h over SLA | "
                    f"Owner: {result.get('owner', 'unknown')}"
                )
                
                if result.get("severity") == "critical":
                    self._send_pagerduty(f"Data SLA Breach: {result['table_name']}", result)
                    self._send_slack(msg, "critical")
                else:
                    self._send_slack(msg, "warning")

# Schedule: run every 5 minutes
service = SLAMonitorService(engine, SLACK_WEBHOOK, PD_KEY)
schedule.every(5).minutes.do(service.run)

while True:
    schedule.run_pending()
    time.sleep(30)
```

---

## Pattern 2: SLA Dashboard SQL

```sql
-- Monthly SLA compliance report
WITH daily_checks AS (
    SELECT
        table_name,
        DATE(checked_at) AS check_date,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) AS passed_checks,
        MAX(lag_hours) AS max_lag_hours,
        AVG(age_hours) AS avg_age_hours
    FROM sla_check_results
    WHERE DATE(checked_at) >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2
),
monthly_summary AS (
    SELECT
        table_name,
        COUNT(*) AS total_days,
        SUM(passed_checks) AS total_passes,
        SUM(total_checks) AS total_check_count,
        ROUND(SUM(passed_checks) * 100.0 / SUM(total_checks), 3) AS availability_pct,
        MAX(max_lag_hours) AS worst_lag_hours,
        SUM(CASE WHEN max_lag_hours > 0 THEN 1 ELSE 0 END) AS days_with_breach
    FROM daily_checks
    GROUP BY 1
)
SELECT
    table_name,
    availability_pct,
    CASE
        WHEN availability_pct >= 99.9 THEN 'GREEN'
        WHEN availability_pct >= 99.0 THEN 'YELLOW'
        ELSE 'RED'
    END AS health_status,
    days_with_breach,
    ROUND(worst_lag_hours, 2) AS worst_lag_hours,
    ROUND(100 - availability_pct, 3) AS downtime_pct
FROM monthly_summary
ORDER BY availability_pct ASC;
```

---

## Pattern 3: SLA Alert Suppression During Maintenance

```python
from datetime import datetime
from typing import Optional

class MaintenanceWindow:
    """Suppress SLA alerts during planned maintenance."""
    
    def __init__(self):
        self._windows = []
    
    def register(self, start: datetime, end: datetime, tables: list, reason: str):
        self._windows.append({
            "start": start, "end": end,
            "tables": set(tables), "reason": reason,
        })
    
    def is_in_maintenance(self, table: str, at: datetime = None) -> Optional[str]:
        if at is None:
            at = datetime.utcnow()
        
        for window in self._windows:
            if (window["start"] <= at <= window["end"] and
                    (not window["tables"] or table in window["tables"])):
                return window["reason"]
        
        return None

# Usage
maintenance = MaintenanceWindow()
maintenance.register(
    start=datetime(2024, 1, 20, 2, 0),
    end=datetime(2024, 1, 20, 4, 0),
    tables=["silver.orders", "gold.revenue"],
    reason="Planned Snowflake maintenance window",
)

# In monitor service
def should_alert(table: str, result: dict, maintenance: MaintenanceWindow) -> bool:
    reason = maintenance.is_in_maintenance(table)
    if reason:
        print(f"Suppressing alert for {table}: {reason}")
        return False
    return result["status"] == "BREACH"
```

---

## SLA Monitoring Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Check SLA only at one time | Misses intra-day breaches | Check every 5-15 min continuously |
| Alert on every check failure | Alert fatigue | Use consecutive failure count threshold |
| No maintenance window suppression | False alerts during planned outages | Register maintenance windows pre-event |
| SLA = pipeline completion time | Doesn't capture freshness | Measure MAX(updated_at) at check time |
| No escalation policy | Alerts go unacknowledged | Auto-escalate after 30 min without ack |
| Measuring wrong table | Gold SLA checked but Silver is stale | Measure at the consumer-facing layer |
