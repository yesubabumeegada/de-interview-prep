---
title: "SLA Monitoring — Scenarios"
topic: data-quality
subtopic: sla-monitoring
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [sla, monitoring, interview, scenarios]
---

# SLA Monitoring — Interview Scenarios

## Scenario 1 (Junior): Defining a Freshness SLA

**Question:** The finance team says they need orders data to be "fresh." How do you translate this into a concrete SLA?

**Answer:**

**Discovery conversation:**
- "When do you use the orders data?" → "Every morning at 9 AM and ad-hoc during the day"
- "What's the cost if data is 2 hours stale?" → "We might approve wrong credit limits"
- "What about 30 minutes stale?" → "Probably fine"
- "What time does your nightly processing start?" → "8:30 AM"

**SLA definition:**
```yaml
table: gold.orders
freshness_sla:
  check_time: "08:55 UTC"  # Before 9 AM usage
  max_age_hours: 1         # Must be updated by 7:55 AM at minimum
  commitment: "99.5% of business days"
  severity: critical
  owner: data-engineering
  consumer: finance-team
```

**Implementation:**
```python
# Airflow: check freshness at 8:55 AM
with DAG("freshness_check", schedule="55 8 * * 1-5"):
    PythonOperator(
        task_id="check_orders_freshness",
        python_callable=lambda: check_freshness("gold.orders", max_age_hours=1),
    )
```

---

## Scenario 2 (Mid-level): SLA Breach Investigation

**Question:** Finance reports at 9 AM that the orders dashboard shows data from yesterday afternoon. The SLA says data must be updated by 8:30 AM. Root-cause and fix?

**Answer:**

**Investigation steps:**
```python
# 1. Check when the pipeline actually completed
import sqlalchemy as sa
engine = sa.create_engine("postgresql://...")
with engine.connect() as conn:
    runs = conn.execute(sa.text("""
        SELECT task_id, start_date, end_date, state, duration
        FROM airflow.task_instance
        WHERE dag_id = 'orders_pipeline'
          AND execution_date = '2024-01-15'
        ORDER BY start_date
    """)).fetchall()
    print(runs)

# 2. Check MAX(updated_at) in gold.orders
with engine.connect() as conn:
    max_ts = conn.execute(sa.text("SELECT MAX(updated_at) FROM gold.orders")).scalar()
    print(f"Last update: {max_ts}")

# 3. Check upstream source
# Was the source system delayed?
with engine.connect() as conn:
    source_max = conn.execute(sa.text(
        "SELECT MAX(created_at) FROM bronze.orders_raw WHERE DATE(created_at) = '2024-01-15'"
    )).scalar()
    print(f"Source last record: {source_max}")
```

**Common root causes and fixes:**

| Root Cause | Evidence | Fix |
|---|---|---|
| Pipeline ran but failed silently | Airflow shows success but incomplete | Add row count validation |
| Source system delayed | Bronze has no new records | Check source SLA, add dependency monitoring |
| Transformation failed | Silver task failed | Fix transformation, rerun |
| Job ran slow (OOM) | Long duration, restart | Scale cluster, add memory |
| Wrong schedule | Job scheduled at 9 AM not 7 AM | Fix cron expression |

---

## Scenario 3 (Senior): SLA Governance Framework

**Question:** You're joining a company where data teams have no formal SLAs. Leadership is frustrated because dashboards are often stale. Design a governance framework.

**Answer:**

**Phase 1: Inventory and measure (Month 1)**
- Profile all tables: row count, MAX(updated_at), consumer teams
- Measure actual freshness over 30 days without SLAs
- Map consumer needs: who reads what, when, for what purpose

**Phase 2: Define SLAs collaboratively (Month 2)**
```
- Workshop with each consumer team: "What's your minimum freshness need?"
- Set initial SLAs at observed p70 performance (achievable, but stretches)
- Define escalation path: who gets alerted, when
- Publish SLA catalog in data catalog/wiki
```

**Phase 3: Monitor and enforce (Month 3+)**
```python
# Automated SLA monitoring running every 5 minutes
# Breach → Slack → PagerDuty (for critical) → Auto-create Jira ticket

# Monthly SLA report to leadership
# Include: breach count, root causes, trend, top offenders
# Show improvement trajectory
```

**SLA governance model:**

| Role | Responsibility |
|---|---|
| Pipeline Owner | Meet SLA, respond to breaches within SLA response time |
| Data Platform | Provide monitoring tooling, alert routing, reporting |
| Consumer Team | Define freshness needs, acknowledge alerts |
| Data Governance | Approve SLA commitments, escalation path |

**Key insight for interview:** Don't start by promising aggressive SLAs. Measure first, promise what you can consistently deliver, then tighten over time as reliability improves. Overpromising and consistently breaching is worse than a realistic SLA that's always met.
