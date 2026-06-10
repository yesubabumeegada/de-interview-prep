---
title: "Incident Management — Scenarios"
topic: data-quality
subtopic: incident-management
content_type: scenario_question
tags: [incident-management, interview, scenarios, on-call]
---

# Incident Management — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: First On-Call Incident

**Scenario:** You're on-call and get a PagerDuty alert at 2 AM: "orders_freshness_sla_breach — orders table not updated in 3 hours." Walk through your response.

<details>
<summary>💡 Hint</summary>

**Step 1: Acknowledge (< 5 min)** - Acknowledge PagerDuty alert to stop escalation - Post in #data-incidents: "Acknowledging orders SLA breach. Investigating."

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Acknowledge (< 5 min)**
- Acknowledge PagerDuty alert to stop escalation
- Post in #data-incidents: "Acknowledging orders SLA breach. Investigating."

**Step 2: Quick checks (< 15 min)**
```bash
# Check Airflow for pipeline status
# Open: https://airflow.company.com/dags/orders_pipeline

# Check from command line
airflow dags list-runs --dag-id orders_pipeline --state failed --limit 5

# Check the actual data
psql -c "SELECT MAX(updated_at) FROM gold.orders"
psql -c "SELECT COUNT(*) FROM bronze.orders_raw WHERE DATE(ingested_at) = CURRENT_DATE"
```

**Step 3: Common first actions**
```bash
# If source data is present and Silver is stale → rerun transform
airflow tasks run orders_pipeline transform_silver 2024-01-15

# If Bronze is also empty → source system issue
# Check source system monitoring, notify source team
```

**Step 4: Escalate if needed**
- If no source data → escalate to source team (not a data engineering issue)
- If 30 min and still failing → escalate to senior DE

**Step 5: Communicate resolution**
- Post resolution time, root cause, and summary in #data-incidents

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Silent Data Corruption

**Scenario:** The data looks fresh and row counts are normal, but the sales team reports revenue figures seem wrong. Nobody received an alert. How do you investigate?

<details>
<summary>💡 Hint</summary>

**If Silver ≠ Gold:** transformation bug (wrong aggregation, wrong join cardinality) **If Silver = Gold but both wrong:** ingestion bug (missing records, wrong amount) **If duplicates found:** dedup logic failed after a change

</details>

<details>
<summary>✅ Solution</summary>

```python
# Step 1: Quantify the discrepancy
# Compare DW numbers to expected (from source or finance estimates)

import sqlalchemy as sa
engine = sa.create_engine("postgresql://...")

with engine.connect() as conn:
    dw_revenue = conn.execute(sa.text("""
        SELECT DATE(order_date) AS date, SUM(amount) AS revenue
        FROM gold.orders
        WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1
        ORDER BY 1
    """)).fetchall()

# Step 2: Check for silent corruption indicators
checks = conn.execute(sa.text("""
    SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT order_id) AS distinct_orders,
        COUNT(*) - COUNT(DISTINCT order_id) AS duplicates,
        SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
        SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_amounts,
        AVG(amount) AS mean_amount,
        MAX(amount) AS max_amount
    FROM gold.orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
""")).fetchone()._asdict()
print(checks)

# Step 3: Compare Silver vs Gold
silver_vs_gold = conn.execute(sa.text("""
    SELECT
        'silver' AS layer, SUM(amount) AS total_revenue
    FROM silver.orders WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
    UNION ALL
    SELECT
        'gold' AS layer, SUM(amount) AS total_revenue
    FROM gold.orders WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
""")).fetchall()
print(silver_vs_gold)
```

**If Silver ≠ Gold:** transformation bug (wrong aggregation, wrong join cardinality)
**If Silver = Gold but both wrong:** ingestion bug (missing records, wrong amount)
**If duplicates found:** dedup logic failed after a change

**Prevention:** Add business-logic DQ check: SUM(gold.revenue) BETWEEN (rolling_avg * 0.8) AND (rolling_avg * 1.2). This would have caught it.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing Incident Prevention

**Scenario:** Your team is having 3 P1 incidents per week. Leadership asks you to reduce this by 75% in 90 days. What's your plan?

<details>
<summary>💡 Hint</summary>

**Week 1-2: Analysis** - Pull incident history: group by root cause category - Top categories typically: OOM, source delays, schema drift, bad deployments - Identify which incidents are recurring (same root cause > 1x)

</details>

<details>
<summary>✅ Solution</summary>

**Week 1-2: Analysis**
- Pull incident history: group by root cause category
- Top categories typically: OOM, source delays, schema drift, bad deployments
- Identify which incidents are recurring (same root cause > 1x)

**Week 3-6: Quick wins**
```
Root cause: OOM → Enable Spark auto-scaling or right-size clusters per job
Root cause: Retry not configured → Add retries to all critical tasks
Root cause: No runbooks → Write runbooks for top 5 incident types
Root cause: Slow detection → Reduce SLA check interval 2h → 5min
Root cause: Bad deploy → Add deploy validation tests, rollback process
```

**Week 7-12: Systematic improvements**
```
1. Alert deduplication: correlated alerts → one incident
2. Auto-remediation for known recovery patterns (OOM → retry large cluster)
3. Game day: validate all runbooks work under pressure
4. Pre-commit checks: block schema changes that would break downstream
5. Error budget tracking: stop new features when budget is exhausted
```

**Measurement:**
```sql
-- Track improvement month-over-month
SELECT
    DATE_TRUNC('month', opened_at) AS month,
    COUNT(*) AS total_incidents,
    SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) AS p1_incidents,
    AVG(duration_minutes) AS avg_mttr_minutes,
    SUM(CASE WHEN is_recurring THEN 1 ELSE 0 END) AS recurring_incidents
FROM incident_history
WHERE opened_at >= '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

**Target:** P1 incidents: 3/week → 0.75/week in 90 days (75% reduction). Achieved through: auto-remediation (-40%), faster detection (-20%), runbooks (-15%).

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is a data quality incident and how does it differ from a pipeline failure?**
A: A pipeline failure is an operational error (job crashes, dependency unavailable). A data quality incident is when the pipeline completes successfully but produces incorrect, incomplete, or misleading data — often more dangerous because it is harder to detect automatically.

**Q: What are the phases of a data quality incident response?**
A: Detection (alerts or consumer reports), triage (assess severity and blast radius), containment (flag or quarantine affected data, notify consumers), root cause analysis, remediation (fix source and reprocess), verification (confirm fix), and postmortem (document and add prevention measures).

**Q: How do you measure the blast radius of a data quality incident?**
A: Use data lineage to identify all downstream datasets, reports, dashboards, and ML models that consume the affected data. Assess whether the issue affects current reporting, historical data, or both, and quantify the number of consumers and business decisions at risk.

**Q: What is a data quality SLA and how does it relate to incident management?**
A: A data quality SLA defines acceptable bounds for freshness, accuracy, and availability for a dataset. Incident severity is determined by how severely and for how long an SLA is violated — this drives escalation priority and the urgency of remediation.

**Q: How do you handle retroactive data corrections after a data quality incident?**
A: Options include reprocessing the affected pipeline from the last known good state, applying a targeted SQL correction (with audit trail), or publishing a correction dataset. All approaches require notifying consumers, documenting the correction, and validating that downstream effects are resolved.

**Q: What is a data quality postmortem and what should it include?**
A: A postmortem documents what happened, the timeline, root cause, impact, remediation steps taken, and action items to prevent recurrence. It should be blameless, focus on systemic fixes (new tests, better monitoring, process changes), and be shared with all stakeholders.

**Q: How do you prevent repeat data quality incidents?**
A: Add targeted data quality checks to catch the specific failure mode that occurred, improve upstream monitoring (source profiling, freshness checks), enhance documentation (data contracts), implement data lineage for faster blast radius assessment, and conduct regular data quality reviews.

**Q: What is the role of an on-call rotation in data quality incident management?**
A: An on-call rotation ensures a designated engineer is always responsible for responding to data quality alerts within the SLA response window. It distributes the operational burden, builds team-wide familiarity with production systems, and ensures incidents are not silently ignored.

---

## 💼 Interview Tips

- Show that you have a structured incident response process, not just "fix it and move on" — interviewers at senior levels expect operational maturity.
- Always mention postmortems and blameless culture — this signals engineering maturity and shows you prioritize learning over blame.
- Be ready to discuss a real (or hypothetical) data quality incident you handled: what was wrong, how you found it, what you did, and what you changed afterward.
- Blast radius assessment using lineage is a differentiator — most junior engineers think only about the immediate table, not all downstream consumers.
- Senior interviewers want to hear about proactive measures: how do you prevent incidents before they happen, not just respond after?
- Distinguish between P1 incidents (critical business decisions at risk, executive visibility) and lower-severity issues — show that you prioritize based on impact, not just technical severity.
