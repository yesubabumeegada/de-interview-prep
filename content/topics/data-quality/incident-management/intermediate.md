---
title: "Incident Management — Intermediate"
topic: data-quality
subtopic: incident-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [incident-management, rca, postmortem, data-corruption, rollback]
---

# Incident Management — Intermediate

## Root Cause Analysis (RCA) Framework

The 5 Whys applied to a data incident:

```
Incident: Revenue dashboard shows $0 for last 3 hours

Why 1: gold.revenue table has no records for last 3 hours
Why 2: silver.orders → gold.revenue job failed
Why 3: The job failed with OOM error
Why 4: Today's batch had 10x more rows than usual (flash sale event)
Why 5: Pipeline cluster size is static — not auto-scaled for peak events

Root Cause: Static cluster size not sized for peak load
Contributing Factor: No alerting on cluster memory usage

Corrective Actions:
  - Immediate: Rerun with larger cluster
  - Short-term: Add cluster memory monitoring
  - Long-term: Enable Spark auto-scaling or pre-size for known events
```

---

## Incident Timeline Template

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

@dataclass
class TimelineEvent:
    timestamp: datetime
    actor: str
    action: str
    details: str = ""

@dataclass
class DataIncident:
    incident_id: str
    title: str
    severity: str
    detected_at: datetime
    resolved_at: datetime = None
    timeline: List[TimelineEvent] = field(default_factory=list)
    root_cause: str = ""
    contributing_factors: List[str] = field(default_factory=list)
    corrective_actions: List[str] = field(default_factory=list)
    
    def add_event(self, actor: str, action: str, details: str = ""):
        self.timeline.append(TimelineEvent(
            timestamp=datetime.utcnow(),
            actor=actor,
            action=action,
            details=details,
        ))
    
    @property
    def time_to_detect_minutes(self) -> float:
        return 0  # Set when actual detection time is known
    
    @property
    def time_to_resolve_minutes(self) -> float:
        if not self.resolved_at:
            return None
        return (self.resolved_at - self.detected_at).total_seconds() / 60
    
    def to_markdown(self) -> str:
        lines = [
            f"# Incident Report: {self.incident_id}",
            f"**Title:** {self.title}",
            f"**Severity:** {self.severity}",
            f"**Detected:** {self.detected_at.isoformat()}",
            f"**Resolved:** {self.resolved_at.isoformat() if self.resolved_at else 'Ongoing'}",
            f"**Duration:** {self.time_to_resolve_minutes:.0f} min" if self.resolved_at else "",
            "",
            "## Timeline",
        ]
        for event in sorted(self.timeline, key=lambda e: e.timestamp):
            lines.append(f"- `{event.timestamp.strftime('%H:%M UTC')}` [{event.actor}] {event.action}: {event.details}")
        
        lines.extend([
            "", "## Root Cause", self.root_cause,
            "", "## Corrective Actions",
        ])
        for action in self.corrective_actions:
            lines.append(f"- {action}")
        
        return "\n".join(lines)
```

---

## Data Corruption Recovery

Data corruption requires careful, staged recovery:

```python
import pandas as pd
from datetime import date

class DataCorruptionRecovery:
    """Staged recovery process for corrupted data."""
    
    def __init__(self, table_name: str, engine):
        self.table_name = table_name
        self.engine = engine
    
    def assess_corruption(self, run_date: date) -> dict:
        """Quantify the extent of corruption before attempting fix."""
        import sqlalchemy as sa
        
        with self.engine.connect() as conn:
            stats = conn.execute(sa.text(f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN amount < 0 THEN 1 END) as negative_amounts,
                    COUNT(CASE WHEN order_id IS NULL THEN 1 END) as null_pks,
                    COUNT(DISTINCT order_id) as distinct_orders,
                    COUNT(*) - COUNT(DISTINCT order_id) as duplicate_count
                FROM {self.table_name}
                WHERE DATE(order_date) = '{run_date}'
            """)).fetchone()._asdict()
        
        print(f"Corruption assessment for {run_date}:")
        for key, val in stats.items():
            print(f"  {key}: {val:,}")
        
        return stats
    
    def create_recovery_backup(self, run_date: date):
        """Save current (corrupted) state before attempting recovery."""
        backup_table = f"{self.table_name}_incident_backup_{run_date.strftime('%Y%m%d')}"
        
        with self.engine.begin() as conn:
            conn.execute(sa.text(f"""
                CREATE TABLE {backup_table} AS
                SELECT * FROM {self.table_name}
                WHERE DATE(order_date) = '{run_date}'
            """))
        
        print(f"Backup created: {backup_table}")
    
    def apply_dedup_fix(self, run_date: date):
        """Remove duplicate rows, keeping the most recent version."""
        with self.engine.begin() as conn:
            conn.execute(sa.text(f"""
                DELETE FROM {self.table_name}
                WHERE ctid NOT IN (
                    SELECT MIN(ctid)
                    FROM {self.table_name}
                    WHERE DATE(order_date) = '{run_date}'
                    GROUP BY order_id
                )
                AND DATE(order_date) = '{run_date}'
            """))
    
    def verify_recovery(self, run_date: date, expected_row_count: int) -> bool:
        """Verify recovery was successful."""
        with self.engine.connect() as conn:
            actual = conn.execute(sa.text(f"""
                SELECT COUNT(*) FROM {self.table_name}
                WHERE DATE(order_date) = '{run_date}'
            """)).scalar()
        
        ok = abs(actual - expected_row_count) / expected_row_count < 0.01
        print(f"Recovery verification: {'PASS' if ok else 'FAIL'} (expected: {expected_row_count:,}, actual: {actual:,})")
        return ok
```

---

## Postmortem Template

```markdown
# Postmortem: Data Incident INC-2024-042

**Date:** 2024-01-15
**Severity:** P1
**Duration:** 4 hours 23 minutes
**Author:** Jane Smith
**Reviewers:** Data Engineering Team

## Executive Summary
Orders pipeline failed due to an OOM error caused by a 10x volume spike during flash sale.
Finance dashboard showed stale data from 6 AM to 10:23 AM UTC.
No revenue was lost. Finance team used backup report for morning review.

## Impact
- 3 dashboards unavailable: Revenue, Orders, Customer Activity
- ~40 finance team members affected
- No data was corrupted or lost

## Timeline
| Time (UTC) | Event |
|------------|-------|
| 05:30 | Orders pipeline starts (scheduled) |
| 05:47 | Spark job fails with OOM on transform_silver task |
| 05:47 | Airflow marks task as failed — no automatic retry configured |
| 07:30 | PagerDuty alert fires (SLA breach — 2hr delay in alerting) |
| 07:45 | On-call engineer acknowledges, begins investigation |
| 08:15 | Root cause identified: cluster OOM during flash sale spike |
| 08:20 | Rerun started with 2x cluster size |
| 10:23 | Pipeline completes, data fresh |

## Root Cause
Static Spark cluster (8 executors, 16GB RAM) was unable to handle 10x volume during Black Friday flash sale. No auto-scaling configured. OOM caused shuffle failure.

## Contributing Factors
- No retry configured on transform_silver task
- SLA alert had 2-hour delay (check runs every 2h, not 5min)
- Flash sale volume not communicated to data engineering team in advance

## What Went Well
- Runbook existed for this failure type
- Mitigation (rerun) was straightforward
- Communication to finance team was timely

## Action Items
| Action | Owner | Due | Status |
|--------|-------|-----|--------|
| Configure Spark auto-scaling | Platform Eng | Jan 22 | In Progress |
| Add 5-min SLA freshness check | Data Eng | Jan 17 | Done |
| Add Airflow retry on all critical tasks | Data Eng | Jan 17 | Done |
| Process for capacity planning around sales events | Data Eng + Marketing | Jan 31 | Pending |

## Lessons Learned
1. SLA checks must run frequently (every 5 min, not every 2h)
2. Retry configuration is mandatory for all production tasks
3. Business events (flash sales) need to be communicated to Data Engineering 48h in advance
```

---

## Interview Tips

> **Tip 1:** "How do you write a good postmortem?" — Blameless: focus on systems, not people. Factual timeline with exact timestamps. Clear root cause (5 Whys). Specific action items with owners and due dates. Executive summary for leadership. Share broadly — other teams learn from your incidents.

> **Tip 2:** "What's the difference between MTTD, MTTA, and MTTR?" — MTTD: Mean Time to Detect (when did we know there was a problem?). MTTA: Mean Time to Acknowledge (when did the on-call respond?). MTTR: Mean Time to Resolve (when was service restored?). Track all three — each points to different improvements.

> **Tip 3:** "How do you safely fix corrupted production data?" — Never modify in place without a backup. Steps: (1) Assess scope, (2) Create backup table, (3) Apply fix in transaction, (4) Verify fix, (5) Remove backup after 30 days. Always have a rollback plan before starting.
