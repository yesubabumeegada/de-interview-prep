---
title: "SLA Monitoring — Scenarios"
topic: data-quality
subtopic: sla-monitoring
content_type: scenario_question
tags: [sla, monitoring, interview, scenarios]
---

# SLA Monitoring — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Defining a Freshness SLA

**Scenario:** The finance team says they need orders data to be "fresh." How do you translate this into a concrete SLA?

<details>
<summary>💡 Hint</summary>

**Discovery conversation:** - "When do you use the orders data?" → "Every morning at 9 AM and ad-hoc during the day" - "What's the cost if data is 2 hours stale?" → "We might approve wrong credit limits" - "What about 30 minutes stale?" → "Probably fine" - "What time does your nightly processing...

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: SLA Breach Investigation

**Scenario:** Finance reports at 9 AM that the orders dashboard shows data from yesterday afternoon. The SLA says data must be updated by 8:30 AM. Root-cause and fix?

<details>
<summary>💡 Hint</summary>

Work backwards from the SLA deadline: check Airflow task instance logs to see when each stage completed. The bottleneck is usually one of three things — the upstream source was late (check source extraction time), a slow transformation task (check duration vs historical baseline), or a queue/resource contention issue (cluster was busy). Once you find the slow stage, check what changed: new data volume, a schema change that broke an index, or a resource constraint.

</details>

<details>
<summary>✅ Solution</summary>

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
|

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: SLA Governance Framework

**Scenario:** You're joining a company where data teams have no formal SLAs. Leadership is frustrated because dashboards are often stale. Design a governance framework.

<details>
<summary>💡 Hint</summary>

**Phase 1: Inventory and measure (Month 1)** - Profile all tables: row count, MAX(updated_at), consumer teams - Measure actual freshness over 30 days without SLAs - Map consumer needs: who reads what, when, for what purpose

</details>

<details>
<summary>✅ Solution</summary>

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
|

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is a data SLA and what does it typically cover?**
A: A data SLA (Service Level Agreement) is a formal commitment about data availability, freshness, quality, and completeness. It typically specifies when data will be available (e.g., "orders table updated by 8 AM"), acceptable error rates, and the consequences or escalation path when the SLA is missed.

**Q: What is the difference between an SLA, SLO, and SLI in data engineering?**
A: An SLI (Service Level Indicator) is a measured metric (e.g., pipeline completion time). An SLO (Service Level Objective) is an internal target for that metric (e.g., pipeline completes within 2 hours 99% of the time). An SLA is the external agreement with consequences for missing the SLO.

**Q: How do you monitor data freshness as part of SLA monitoring?**
A: Track the maximum timestamp or load time of the latest record in a table and compare it to the expected freshness window. Tools like dbt's `freshness` blocks, Monte Carlo, or custom SQL checks can alert when a table hasn't been updated within the SLA-defined window.

**Q: What metrics should a data SLA monitoring dashboard include?**
A: Pipeline completion time vs. SLA target, data freshness lag, row count anomalies, quality check pass rates, historical SLA compliance percentage, number of SLA breaches by severity, and mean time to detection and resolution for incidents.

**Q: How do you handle a recurring SLA breach?**
A: Investigate root cause (upstream latency, resource contention, data volume growth), implement fixes (pipeline optimization, resource scaling, priority adjustment), and re-negotiate the SLA if the target is structurally unachievable. Document the breach pattern and add proactive monitoring to detect it earlier.

**Q: How do you set realistic data SLAs when first establishing them?**
A: Baseline current pipeline performance over 30-60 days, identify the P95 completion time, set the initial SLA at P90 or P95 to be achievable while still meaningful, and plan to tighten it as reliability improves. Avoid committing to targets without historical evidence.

**Q: What is the role of alerting in SLA monitoring?**
A: Alerting should trigger before SLA breach (predictive — "pipeline is running 30% slower than usual and may miss the 8 AM SLA") and at breach time (reactive). Pre-breach alerts give engineers time to intervene; breach alerts trigger incident response and consumer notification.

**Q: How do you communicate SLA breaches to business stakeholders?**
A: Use a tiered communication plan: immediate notification to data consumers at breach time with estimated resolution, status updates at defined intervals, and a postmortem summary after resolution. Plain business language (not technical jargon) and clear impact statements maintain trust.

---

## 💼 Interview Tips

- Use the SLI/SLO/SLA framework when discussing reliability — it shows you apply site reliability engineering principles to data platforms.
- Be ready to discuss how you would implement SLA monitoring from scratch, including what metrics to track, what tooling to use, and how to alert.
- Senior interviewers want to hear about stakeholder communication — technical excellence alone isn't enough if business users don't know when and how data is affected.
- Distinguish between monitoring for SLA compliance (are we meeting commitments?) and optimization (how do we improve reliability?) — both matter.
- Common mistake: setting overly aggressive SLAs based on best-case performance rather than realistic P95 baselines — discuss how to negotiate achievable commitments.
- Connect SLA monitoring to cost: tighter SLAs require more infrastructure, faster pipelines, and more engineering effort — show that you understand the business tradeoff.
