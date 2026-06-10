---
title: "SLA Monitoring — Senior Deep Dive"
topic: data-quality
subtopic: sla-monitoring
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sla, observability, burn-rate, multi-window, governance]
---

# SLA Monitoring — Senior Deep Dive

## Multi-Window Error Budget Burn Rate

Google SRE's approach: alert on burn rate, not just threshold breaches.

```python
from dataclasses import dataclass
from typing import List
import numpy as np

@dataclass
class BurnRateAlert:
    """
    Multi-window burn rate alerting (Google SRE "Chapter 5" approach).
    Alert when error budget is being consumed faster than acceptable.
    """
    sla_target_pct: float = 99.9
    windows: list = None  # [(window_hours, burn_rate_threshold, severity)]
    
    def __post_init__(self):
        if self.windows is None:
            # Standard: 1h window burns fast → critical; 6h → warning
            self.windows = [
                (1, 14.4, "critical"),    # 1h at 14.4x burn → exhausts monthly budget in ~2 days
                (6, 6.0, "warning"),      # 6h at 6x burn → exhausts in ~5 days
                (24, 3.0, "info"),        # 24h at 3x → inform
            ]
    
    def compute_burn_rate(
        self,
        breach_minutes: float,
        window_hours: float,
        period_days: int = 30,
    ) -> float:
        """
        Burn rate = (breach_minutes in window / budget per window).
        Burn rate > 1 = consuming budget faster than it accrues.
        """
        budget_per_day_minutes = period_days * 24 * 60 * (1 - self.sla_target_pct / 100) / period_days
        budget_for_window = budget_per_day_minutes * (window_hours / 24)
        
        if budget_for_window == 0:
            return float("inf") if breach_minutes > 0 else 0
        
        return breach_minutes / budget_for_window
    
    def evaluate(self, breach_minutes_by_window: dict) -> List[dict]:
        """
        breach_minutes_by_window: {window_hours: breach_minutes_in_that_window}
        Returns list of active alerts.
        """
        alerts = []
        for window_h, threshold, severity in self.windows:
            breach_min = breach_minutes_by_window.get(window_h, 0)
            burn_rate = self.compute_burn_rate(breach_min, window_h)
            
            if burn_rate >= threshold:
                alerts.append({
                    "severity": severity,
                    "window_hours": window_h,
                    "burn_rate": round(burn_rate, 1),
                    "threshold": threshold,
                    "message": f"{severity.upper()}: {burn_rate:.1f}x burn rate in {window_h}h window (threshold: {threshold}x)",
                })
        
        return alerts


# Usage
alerter = BurnRateAlert(sla_target_pct=99.9)
recent_breaches = {
    1: 8.0,    # 8 min of breach in last 1 hour → burn rate = 8/0.43 ≈ 18.6x
    6: 12.0,   # 12 min in last 6 hours
    24: 18.0,  # 18 min in last 24 hours
}
active_alerts = alerter.evaluate(recent_breaches)
for a in active_alerts:
    print(a["message"])
```

---

## Dependency-Aware SLA Monitoring

When upstream misses SLA, auto-extend downstream SLA window:

```python
from typing import Dict, Optional
from datetime import datetime, timedelta

class SLADependencyGraph:
    """Track SLA dependencies and compute adjusted SLAs."""
    
    def __init__(self, sla_catalog: list):
        self.slas = {s.table: s for s in sla_catalog}
        self.dependency_map = {
            s.table: s.depends_on for s in sla_catalog
        }
    
    def get_upstream_delay(
        self,
        table: str,
        actual_run_times: Dict[str, datetime],
        scheduled_run_times: Dict[str, datetime],
    ) -> float:
        """
        Compute delay caused by upstream dependencies.
        Returns the maximum upstream delay in minutes.
        """
        depends_on = self.dependency_map.get(table, [])
        max_upstream_delay_minutes = 0
        
        for upstream in depends_on:
            if upstream in actual_run_times and upstream in scheduled_run_times:
                delay = (actual_run_times[upstream] - scheduled_run_times[upstream]).total_seconds() / 60
                max_upstream_delay_minutes = max(max_upstream_delay_minutes, delay)
        
        return max_upstream_delay_minutes
    
    def is_own_sla_breach(
        self,
        table: str,
        actual_completion: datetime,
        scheduled_completion: datetime,
        actual_run_times: Dict[str, datetime],
        scheduled_run_times: Dict[str, datetime],
    ) -> tuple:
        """
        Distinguish between own-cause vs upstream-cause SLA breaches.
        Returns (is_breach, is_own_fault, upstream_delay_minutes)
        """
        sla = self.slas[table]
        total_delay = (actual_completion - scheduled_completion).total_seconds() / 60
        upstream_delay = self.get_upstream_delay(table, actual_run_times, scheduled_run_times)
        
        own_delay = total_delay - upstream_delay
        
        return (
            total_delay > sla.max_lag_minutes,   # SLA breached
            own_delay > 5,                        # Own fault if own delay > 5 min
            upstream_delay,
        )
```

---

## SLA Contract with Business — Template

```yaml
# docs/sla/orders_pipeline.yaml
pipeline: orders_pipeline
version: "1.2"
effective_date: "2024-01-15"
reviewed_by: [data-engineering, analytics, finance]

sla:
  freshness:
    gold_orders:
      description: "gold.orders updated by 8:30 AM UTC daily"
      commitment: "99.5% of business days"
      measurement_window: "monthly"
      check_time: "08:30 UTC"
      max_age_hours: 24
      
    silver_orders_streaming:
      description: "silver.orders updated within 5 min of source event"
      commitment: "99.9% of events"
      max_latency_minutes: 5

  latency:
    e2e_batch:
      description: "Raw to Gold within 90 minutes of scheduled start"
      commitment: "95% of daily runs"
      max_minutes: 90
    
  quality:
    completeness:
      description: "≥99.9% of required fields populated"
      commitment: "daily"
      threshold_pct: 99.9

  availability:
    description: "Pipeline available for querying 99.5% of time"
    commitment: "monthly"
    target_pct: 99.5

breach_escalation:
  - severity: warning
    action: Slack to #data-quality
    response_time: 30 minutes
  - severity: critical
    action: PagerDuty on-call
    response_time: 15 minutes

reporting:
  cadence: monthly
  recipients: [data-engineering, analytics-leads, finance-ops]
  includes: [availability, breach_history, error_budget_remaining]

exclusions:
  - "Source system maintenance windows (pre-announced 24h+)"
  - "AWS region outages"
```

---

## Interview Tips

> **Tip 1:** "How do you report SLA compliance to business stakeholders?" — Monthly report with: (1) availability % for each table, (2) breach count + root causes, (3) error budget remaining, (4) trend vs prior month. Use RAG (Red/Amber/Green) status for quick visual. Never wait for stakeholders to discover breaches — proactive reporting builds trust.

> **Tip 2:** "What is burn rate alerting and why is it better than threshold alerting?" — Threshold alerting fires only after breach. Burn rate fires while you're on track to breach. E.g., if you're consuming 3x your budget in the last hour, you'll exhaust the monthly budget in 10 days — alert now, fix before month-end breach.

> **Tip 3:** "How do you handle an SLA breach caused by a third-party system?" — Track upstream SLA performance separately. Report to business: "Our pipeline delivered within 12 minutes of data arrival. Source system was 4 hours late. Our SLA commitment was met." Document dependency SLAs in your SLA contract so exclusions are pre-agreed.

## ⚡ Cheat Sheet

**Great Expectations core objects**
```python
import great_expectations as gx
context = gx.get_context()

# Expectation suite
suite = context.add_expectation_suite("orders_suite")
validator = context.get_validator(batch_request=batch_req, expectation_suite_name="orders_suite")

# Common expectations
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_between("amount", 0, 100000)
validator.expect_column_pair_values_a_to_be_greater_than_b("ship_date", "order_date")
validator.expect_column_values_to_match_regex("email", r"^[\w._%+-]+@[\w.-]+\.[a-z]{2,}$")

# Run checkpoint
result = context.run_checkpoint("orders_checkpoint")
assert result["success"], f"DQ failure: {result}"
```

**Anomaly detection patterns**
```python
# Z-score for numeric columns
def zscore_anomaly(series, threshold=3.0):
    z = (series - series.mean()) / series.std()
    return z.abs() > threshold

# Rolling mean comparison (for time series)
df["rolling_avg"] = df["revenue"].rolling(7).mean()
df["anomaly"] = abs(df["revenue"] - df["rolling_avg"]) > 2 * df["revenue"].rolling(7).std()
```

**Data contract (dbt schema.yml)**
```yaml
models:
  - name: orders
    description: "Gold orders table — SLA: updated within 1 hour of source"
    config: {contract: {enforced: true}}
    columns:
      - name: order_id
        data_type: bigint
        constraints: [{type: not_null}, {type: unique}]
      - name: amount
        data_type: double
        constraints: [{type: not_null}]
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: updated_at
          interval: 2
```

**SLA monitoring**
```sql
-- Alert if table hasn't been updated within SLA window
SELECT table_name,
       MAX(updated_at) AS last_updated,
       DATEDIFF('hour', MAX(updated_at), NOW()) AS hours_since_update,
       CASE WHEN DATEDIFF('hour', MAX(updated_at), NOW()) > sla_hours THEN 'BREACHED' ELSE 'OK' END AS status
FROM table_sla_registry
JOIN gold_tables USING (table_name)
GROUP BY table_name, sla_hours;
```

**DQ dimensions**
```
Completeness:  % non-null values
Accuracy:      matches source of truth
Consistency:   same value across systems
Timeliness:    data arrives within SLA
Uniqueness:    no duplicates on PK
Validity:      conforms to expected format/range
```

**Incident response flow**
```
1. Alert fires (DQ check fails, SLA breached)
2. Triage: severity — who's impacted? (BI dashboard, ML model, external SLA?)
3. Notify: page on-call DE + inform data consumers
4. Contain: quarantine bad data (move to _quarantine schema; don't serve bad data)
5. Fix: patch pipeline or source data
6. Backfill: reprocess affected time range
7. Post-mortem: root cause + prevention (add check that would have caught this earlier)
```
