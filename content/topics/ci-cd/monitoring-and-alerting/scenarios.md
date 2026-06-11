---
title: "Monitoring and Alerting — Scenarios"
topic: ci-cd
subtopic: monitoring-and-alerting
content_type: scenario_question
tags: [ci-cd, monitoring,alerting,observability,prometheus, interview, scenarios]
---

# Monitoring and Alerting — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Basic Scenario

**Scenario:** Your data pipeline runs successfully every day but nobody knows when it finishes or if the output looks correct. How do you add basic monitoring?

<details>
<summary>💡 Hint</summary>

Start with what you already have: Airflow email_on_failure, a row count check at the end of the pipeline, and freshness alert if the output table isn't updated. Add Slack alerting for visibility.

</details>

<details>
<summary>✅ Solution</summary>

```python
# Add to end of pipeline:

# 1. Row count assertion
def validate_output(df: pd.DataFrame, min_rows: int = 1000):
    if len(df) < min_rows:
        raise ValueError(f"Output has only {len(df)} rows (expected >= {min_rows})")
    print(f"✓ Output validated: {len(df):,} rows")

# 2. Freshness tracking  
import boto3
ssm = boto3.client("ssm")
ssm.put_parameter(
    Name="/pipeline/revenue/last_success",
    Value=datetime.utcnow().isoformat(),
    Overwrite=True,
    Type="String"
)

# 3. Slack notification on complete
def notify_slack(message: str):
    import requests
    requests.post(os.environ["SLACK_WEBHOOK"], json={"text": message})

notify_slack("✅ Revenue pipeline complete: 48,291 rows processed")
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Intermediate Challenge

**Scenario:** You need to set up an alert that detects when today's revenue total is more than 20% different from last week's same weekday. How do you implement this?

<details>
<summary>💡 Hint</summary>

Compare today's metric to the same-weekday last week in a scheduled alert. Store daily metrics in a monitoring table. Alert when the ratio is outside acceptable bounds.

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Store daily revenue in monitoring table
CREATE TABLE pipeline_metrics (
    date DATE,
    pipeline VARCHAR,
    total_revenue FLOAT,
    row_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert today's metrics at end of pipeline
INSERT INTO pipeline_metrics (date, pipeline, total_revenue, row_count)
SELECT CURRENT_DATE, 'revenue_daily', SUM(revenue), COUNT(*)
FROM fct_revenue_daily
WHERE date = CURRENT_DATE;
```

```python
# Alert: compare today vs same weekday last week
def check_revenue_anomaly():
    today = get_metric("revenue_daily", date.today())
    last_week = get_metric("revenue_daily", date.today() - timedelta(days=7))
    
    if last_week and today:
        ratio = today / last_week
        if ratio < 0.8 or ratio > 1.2:  # > 20% deviation
            send_alert(
                f"Revenue anomaly: today={today:,.0f}, last week={last_week:,.0f}, "
                f"ratio={ratio:.1%}"
            )
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Challenge

**Scenario:** Design a comprehensive observability strategy for a DE platform processing 10TB/day across 100 pipelines. Define SLOs, alerting tiers, and escalation policy.

<details>
<summary>💡 Hint</summary>

Three-tier alerting: critical (page on-call: SLA breach), warning (Slack: degraded performance), info (dashboard: normal anomalies). SLOs defined per pipeline criticality. Automated runbooks for common alerts.

</details>

<details>
<summary>✅ Solution</summary>

```
SLO Tiers:

Tier 1 (Revenue-critical pipelines):
  - Availability: 99.9% (< 44 min downtime/month)
  - Freshness: completed by 7 AM UTC
  - Data quality: < 0.01% error rows
  - Alert: page on-call immediately if breached

Tier 2 (Analytics pipelines):
  - Availability: 99.5%
  - Freshness: completed by 10 AM UTC
  - Alert: Slack #data-alerts, SLA breach

Tier 3 (Experimental/ML pipelines):
  - Availability: 95%
  - Freshness: best effort
  - Alert: Slack only, no paging

Escalation policy:
  1. Alert fires → Slack #data-alerts (all tiers)
  2. Tier 1 + unacknowledged 5 min → PagerDuty page
  3. Unacknowledged 15 min → escalate to engineering manager
  4. Data quality breach → notify data consumers automatically
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between monitoring and observability?**
A: Monitoring answers pre-defined questions: "Is the pipeline healthy?" with known metrics. Observability lets you ask arbitrary questions about system state from its outputs (metrics, logs, traces). You monitor known failure modes; observability lets you investigate unknown issues.

**Q: What are SLOs and SLAs, and how do they differ?**
A: SLO (Service Level Objective) is an internal target: "Revenue pipeline completes by 7 AM 99.9% of the time." SLA (Service Level Agreement) is a contractual commitment to customers with penalties for breach. SLOs should be stricter than SLAs — you want internal alerts before SLA breach.

**Q: What is an error budget and how is it used?**
A: An error budget is 100% minus the SLO: for 99.9% availability, you have 0.1% error budget (~44 minutes/month). When the budget is exhausted, stop feature work and fix reliability. Burn rate alerts warn when you're consuming the budget too fast.

**Q: What is structured logging and why is it better for pipelines?**
A: Structured logging outputs JSON instead of plaintext — each field (timestamp, level, pipeline, row_count) is a queryable key. Unstructured logs require regex parsing. Structured logs enable: `logs.filter(pipeline="revenue", status="error")` — essential for debugging at scale.

**Q: What should trigger a PagerDuty page vs. a Slack alert for data pipelines?**
A: Page: revenue/financial pipeline SLA breach, data corruption affecting production reporting, complete pipeline system outage. Slack: pipeline slower than baseline, data quality warning (not failure), non-critical pipeline delayed. Never page for anything a human can't action immediately.

**Q: What is a runbook and why should alerts link to one?**
A: A runbook is a documented procedure for responding to a specific alert: what it means, how to investigate, common causes, and resolution steps. Alerts should link to the runbook so on-call engineers (including those who didn't build the pipeline) can respond effectively at 3 AM.

**Q: What data quality checks should run as part of pipeline monitoring?**
A: Row count (within expected range), null rate on key columns (not increasing), deduplication (no unexpected duplicates), referential integrity (foreign keys valid), freshness (table updated within SLA), and statistical checks (distribution of values hasn't shifted significantly).

---

## 💼 Interview Tips

- Lead with SLOs as the framing — monitoring without SLOs is just dashboards. SLOs connect monitoring to business impact.
- Distinguish alert severity tiers explicitly (page vs Slack vs info) — interviewers want to know you've thought about alert fatigue, not just alert coverage.
- Data-specific monitors (row count deviation, freshness, null rate) are what separate DE-specialized monitoring answers from generic DevOps answers.
- For senior roles, mention error budget burn rate alerts — they're the modern approach to SLO-based alerting and show you know beyond basic thresholds.
- Connect monitoring to the incident response cycle — alert → runbook → resolution → post-mortem → prevention. Show the operational flywheel.
- Avoid claiming you'd alert on everything — alert fatigue is a real problem. Discuss what you would NOT alert on (expected daily variance) as much as what you would.
