---
title: "Data SLA Enforcement - Scenario Questions"
topic: data-quality
subtopic: data-sla-enforcement
content_type: scenario_question
tags: [data-quality, sla, scenarios, interview, data-contracts, freshness]
---

# Data SLA Enforcement — Scenario Questions

<article data-difficulty="junior">

## Scenario (Junior): Adding Basic Freshness Monitoring

Your manager asks you to ensure the `fct_sales` table — used by the sales team for their morning pipeline review at 9am — is always fresh by 8:30am. Currently there's no monitoring. The table is built by a dbt pipeline that runs at 7am via Airflow.

**Question:** How do you add a basic freshness SLA check? What does "breach" mean here, and how would you alert the team?

<details>
<summary>✅ Solution</summary>

**Step 1: Define what "fresh" means**

The table should be updated by 8:30am. The dbt pipeline completes around 7:45am normally. So:
- Warn: data older than 90 minutes (8:30am check: last update before 7:00am)
- Error: data older than 120 minutes (8:30am check: last update before 6:30am)

**Step 2: Add dbt source freshness config**

```yaml
# models/sources.yml
sources:
  - name: app_db
    tables:
      - name: orders
        loaded_at_field: _updated_at
        freshness:
          warn_after: {count: 90, period: minute}
          error_after: {count: 120, period: minute}
```

Run after the dbt build:
```bash
dbt source freshness --select source:app_db.orders
```

**Step 3: Add a post-pipeline check in Airflow**

```python
# dags/sales_pipeline.py
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum, requests

@dag(schedule="0 7 * * 1-5", start_date=pendulum.datetime(2024, 1, 1))  # Weekdays 7am
def sales_morning_pipeline():
    
    @task
    def dbt_run():
        # ... run dbt
        pass
    
    @task
    def check_fct_sales_freshness():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
        result = hook.get_first("""
            SELECT
                MAX(updated_at) AS last_update,
                DATEDIFF('minute', MAX(updated_at), CURRENT_TIMESTAMP()) AS lag_minutes
            FROM analytics.fct_sales
            WHERE DATE(updated_at) = CURRENT_DATE()
        """)
        
        last_update, lag_minutes = result
        
        if lag_minutes is None or lag_minutes > 90:
            # Alert the sales ops channel
            requests.post(SLACK_WEBHOOK, json={
                "text": (
                    f":warning: *Sales data freshness alert*\n"
                    f"fct_sales last updated: {last_update} ({lag_minutes} min ago)\n"
                    f"Expected: < 90 minutes stale by 8:30am\n"
                    f"Action: Check if the 7am Airflow pipeline completed successfully"
                )
            })
            
            if lag_minutes > 120:
                raise ValueError(f"SLA BREACH: fct_sales is {lag_minutes} minutes stale")
        
        return {"lag_minutes": lag_minutes, "status": "ok"}
    
    build = dbt_run()
    build >> check_fct_sales_freshness()
```

**What a "breach" means:**
- Warn (90 min): pipeline ran slowly or had a retry — investigate but don't panic
- Error (120 min): pipeline failed entirely — alert required before 8:30am so someone can investigate before the sales meeting

**Key decisions:**
- Check runs as part of the pipeline, not separately, so a pipeline failure automatically means the check fails
- Slack alert goes to the team owning the pipeline, not to the sales team (they don't need technical noise)
- Only page/escalate at the error threshold, not the warn threshold
</details>

</article>

---

<article data-difficulty="mid">

## Scenario (Mid-Level): Handling an SLA Breach Under Pressure

It's 8:45am. You get a Slack message from the CFO: "The revenue dashboard is showing numbers from 2 days ago. Our board meeting is at 10am and we need current data." You check and see the `fct_revenue` pipeline failed at 6am. The Airflow task shows a Python import error in a dependencies upgrade someone deployed last night.

**Question:** Walk through your incident response. How do you fix it, communicate, and prevent recurrence?

<details>
<summary>✅ Solution</summary>

**Immediate Response (8:45am — first 5 minutes)**

Do not start debugging silently. Communicate first:

```
To: #data-oncall + #revenue-analytics
"I'm investigating the revenue dashboard issue. Root cause identified: dependency 
upgrade last night broke the 6am pipeline. Working on fix now. ETA: 15-20 minutes 
for data to be current. Will update at 9:15am."
```

**Identify the exact fix (8:50am)**

```bash
# Look at the Airflow task log
# Error: ImportError: cannot import name 'DataFrame' from 'pandas' (version mismatch)

# Check what was deployed last night
git log --since="yesterday" -- requirements.txt
# Commit: "bump pandas 1.5 → 2.0"

# Verify the import issue
python -c "from pandas import DataFrame"
# Works locally — environment mismatch
```

**Apply the fix (8:55am)**

Option A (fastest — pin the old version to unblock):
```bash
# In requirements.txt, pin to last known good version
pandas==1.5.3

# Rebuild the Docker image / Airflow environment
docker build -t dbt-runner:hotfix .

# Trigger a manual backfill run
airflow dags trigger fct_revenue_pipeline \
  --conf '{"execution_date": "2024-11-15"}'
```

Option B (if Docker rebuild takes too long — direct query fix):
```bash
# Run dbt directly on the production environment
dbt build --select fct_revenue --target prod --vars '{"run_date": "2024-11-15"}'
```

**Monitor and confirm (9:05am)**

```bash
# Watch the task in real time
airflow tasks logs fct_revenue_pipeline dbt_build <run_id>

# Query the table to verify recency
SELECT MAX(updated_at) FROM analytics.fct_revenue;
# → 2024-11-15 09:03:22 UTC  ✓
```

**Communicate resolution (9:10am)**

```
To: CFO + #revenue-analytics
"Revenue data is now current as of 9:03am UTC. The dashboard should show 
correct numbers. Root cause: a dependency upgrade deployed last night broke 
the 6am pipeline. We've pinned the version to restore service. 
Post-mortem will follow by EOD."
```

**Prevent Recurrence**

1. **Add CI test for imports:**
```yaml
# .github/workflows/test.yml
- name: Test all imports
  run: python -c "import dbt, pandas, numpy, google.cloud.bigquery"
```

2. **Add staging validation before production deploys:**
```bash
# Test in staging first: dbt build --target staging
# Only promote to prod after staging passes
```

3. **Add pipeline failure alert (don't wait for CFO to notice):**
```python
# Airflow on_failure_callback
def pipeline_failure_callback(context):
    notify_slack(
        channel="#data-oncall",
        text=f":red_circle: Pipeline FAILED: {context['dag'].dag_id}"
    )
```

**The gap that hurt:** No alerting on pipeline failure. The 6am failure went unnoticed for 2.75 hours. A simple failure alert to #data-oncall would have given the team 2+ hours to fix it before the board meeting.
</details>

</article>

---

<article data-difficulty="senior">

## Scenario (Senior): Designing Multi-Tier SLAs for a Growing Data Platform

You are the head of data engineering at a Series B company. You have 800 dbt models, 12 downstream teams, and a data team of 10. The CEO has asked you to "put real SLAs on data like we do for the product." Currently there are no formal SLAs, and the data team gets ad-hoc complaints when things break.

**Question:** Design the end-to-end SLA framework: how you tier data products, define SLIs/SLOs/SLAs, enforce them technically, and govern them organizationally.

<details>
<summary>✅ Solution</summary>

### Phase 1: Discovery and Tiering (Weeks 1-2)

**Identify which tables have SLA implications through interviews:**

```python
# discovery_survey.py — send to each downstream team
SURVEY_QUESTIONS = [
    "Which tables/datasets does your team depend on?",
    "What is the maximum acceptable data lag before your work is impacted?",
    "What happens to your team/customers if this data is unavailable?",
    "What are your peak dependency hours?",
]

# Map responses to tiers:
# P1/Platinum: real-time customer impact or regulatory exposure
# P2/Gold: business-critical reporting (finance, operations)
# P3/Silver: analytics, exploration, non-time-sensitive
```

**Example tiering output from discovery:**

```yaml
platinum:  # <5% of tables, ~40 models
  examples: [fct_payments, fct_orders, dim_customers]
  stakeholders: [Finance, Risk, CEO dashboard]
  max_lag_minutes: 15
  max_breach_minutes_per_month: 44  # 99.9% SLA
  
gold:  # ~20% of tables, ~160 models
  examples: [fct_revenue, fct_mrr, fct_churn]
  stakeholders: [Revenue Analytics, Sales Operations]
  max_lag_minutes: 60
  max_breach_minutes_per_month: 216  # 99.5% SLA

silver:  # ~75% of tables, ~600 models
  examples: [marketing_attribution, experiment_results, content_engagement]
  stakeholders: [Marketing, Product]
  max_lag_minutes: 240
  max_breach_minutes_per_month: 2160  # 95% SLA
  no_pagerduty: true
```

### Phase 2: Technical Implementation (Weeks 3-6)

**SLI measurement for each tier:**

```sql
-- Store SLI measurements every 5 minutes
INSERT INTO monitoring.sli_measurements
SELECT
    CURRENT_TIMESTAMP() AS measured_at,
    table_id,
    tier,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), MINUTE) AS freshness_lag_minutes,
    COUNTIF(updated_at IS NULL) / COUNT(*) AS completeness_null_rate
FROM monitoring.sla_registry
CROSS JOIN analytics.{table_id}  -- parameterized per table
WHERE DATE(updated_at) >= CURRENT_DATE()
GROUP BY 1, 2, 3;
```

**Automated SLA breach detection:**

```python
# plugins/sla_evaluator.py
from dataclasses import dataclass

TIER_THRESHOLDS = {
    "platinum": {"freshness_minutes": 15, "warn_buffer": 5, "pagerduty": True},
    "gold": {"freshness_minutes": 60, "warn_buffer": 15, "pagerduty": True},
    "silver": {"freshness_minutes": 240, "warn_buffer": 30, "pagerduty": False},
}

def evaluate_sli(table_id: str, tier: str, lag_minutes: int) -> str:
    config = TIER_THRESHOLDS[tier]
    sla = config["freshness_minutes"]
    warn_at = sla - config["warn_buffer"]
    
    if lag_minutes > sla:
        return "breach"
    elif lag_minutes > warn_at:
        return "warning"
    return "ok"
```

### Phase 3: Governance Model (Weeks 7-8)

**SLA Ownership Matrix:**

| Role | Responsibility |
|---|---|
| Data Engineering | Define SLIs, build monitoring, respond to breaches, run post-mortems |
| Data Product Owner | Define SLA tier and business requirements |
| Downstream Team Lead | Acknowledge SLAs, flag new dependencies |
| VP of Engineering | Review monthly compliance; approve tier changes |

**Monthly SLA Review Meeting Agenda:**
1. Compliance report: which tables met SLA, which breached
2. Error budget status: who has budget remaining, who is exhausted
3. Incident review: post-mortems from major breaches
4. Tier change requests: teams requesting tier upgrades
5. Error budget freeze decisions: freeze risky changes for exhausted pipelines

**SLA Escalation Ladder:**

```
Platinum breach:
  T+0 min:  PagerDuty pages DE on-call
  T+15 min: Auto-page DE manager if not acknowledged
  T+30 min: Manual escalation to VP Engineering + stakeholder team lead
  T+60 min: CEO/CFO briefed if breach > 1 hour

Gold breach:
  T+0 min:  Slack alert to #data-oncall
  T+30 min: PagerDuty page if not acknowledged
  T+60 min: Manager escalation

Silver breach:
  T+0 min:  Slack alert to #data-alerts
  T+4 hr:   Email summary if unresolved
  No PagerDuty
```

**The key organizational insight:** SLAs only work if they are co-owned by the consuming teams. When the finance team signed off on the platinum SLA for `fct_payments`, they also committed to: (1) not making schema requests without 2 weeks notice, and (2) having an on-call contact available for incident communication. This shared ownership changes the dynamic from "data team vs. everyone" to "data team and finance team both own this."
</details>

</article>
