---
title: "Data SLA Enforcement - Real World"
topic: data-quality
subtopic: data-sla-enforcement
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [data-quality, sla, production, case-study, fintech, data-contracts]
---

# Data SLA Enforcement — Real World

## Case Study: Fintech Payment Pipeline — 99.9% Freshness SLA

### Context

A payments company processes $500M/day in transactions. The finance team and risk operations team both depend on a `fct_payments` table being fresh within 15 minutes of a Stripe update. A breach means:
- Risk team can't run real-time fraud scoring
- Finance daily P&L report is delayed
- Potential regulatory reporting exposure

The company committed to this SLA in their enterprise customer contracts.

### The SLA Monitoring System

**SLA Definition registered in a config table:**

```sql
INSERT INTO monitoring.sla_registry VALUES
(
    'fct_payments',
    'platinum',
    15,    -- freshness_minutes
    99.9,  -- completeness_pct
    0.001, -- max_accuracy_variance
    CURRENT_TIMESTAMP()
);
```

**Monitoring DAG runs every 2 minutes:**

```python
# dags/payment_sla_monitor.py
@dag(schedule="*/2 * * * *", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def payment_freshness_monitor():
    
    @task
    def measure_freshness() -> dict:
        from google.cloud import bigquery
        client = bigquery.Client()
        
        result = list(client.query("""
            SELECT
                MAX(updated_at) AS last_update,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), MINUTE) AS lag_minutes,
                COUNT(*) AS row_count_today,
                COUNTIF(amount_usd IS NULL) / COUNT(*) AS null_amount_rate
            FROM `analytics.fct_payments`
            WHERE DATE(updated_at) >= CURRENT_DATE()
        """).result())[0]
        
        return {
            "last_update": result["last_update"].isoformat(),
            "lag_minutes": result["lag_minutes"],
            "row_count": result["row_count_today"],
            "null_amount_rate": float(result["null_amount_rate"]),
        }
    
    @task
    def evaluate_sla(metrics: dict) -> dict:
        is_freshness_breach = metrics["lag_minutes"] > 15
        is_accuracy_breach = metrics["null_amount_rate"] > 0.001
        
        return {
            **metrics,
            "is_breached": is_freshness_breach or is_accuracy_breach,
            "breach_types": (
                (["freshness"] if is_freshness_breach else []) +
                (["accuracy"] if is_accuracy_breach else [])
            ),
        }
    
    @task
    def handle_breach(sla_result: dict):
        if not sla_result["is_breached"]:
            return
        
        breach_types = sla_result["breach_types"]
        lag = sla_result["lag_minutes"]
        
        # Only page if breach is real and worsening
        # (avoid alert storms for transient blips)
        if lag > 20:  # 5-minute grace period beyond SLA
            trigger_pagerduty(
                summary=f"Payment SLA breach: {breach_types} — {lag}min stale",
                severity="critical",
            )
        
        notify_slack(
            channel="#payments-data-oncall",
            text=f":red_circle: Payment freshness: {lag}min (SLA: 15min) | Breach: {breach_types}"
        )
    
    metrics = measure_freshness()
    sla_result = evaluate_sla(metrics)
    handle_breach(sla_result)
```

---

## Incident Report: The 47-Minute Breach

### What Happened

At 08:12 UTC on a Tuesday morning, the Stripe Fivetran connector stopped syncing. The monitoring DAG detected a freshness breach at 08:14 (first check after the 15-minute window passed).

**Condensed post-mortem:**

```
ROOT CAUSE: Fivetran → Stripe API rate limit hit after Stripe released 
a new API version that changed pagination behavior. Fivetran's connector 
made more requests per sync than the previous version, hitting the 
100 requests/min limit.

TIMELINE:
  08:00 Stripe released API v2024-11-01
  08:12 Fivetran sync began using new API, hit rate limit at page 3
  08:14 Monitoring detected breach (lag crossed 15 min threshold)
  08:16 PagerDuty alert fired (20 min grace period passed)
  08:22 On-call engineer acknowledged
  08:35 Root cause identified: API rate limit errors in Fivetran logs
  08:40 Fivetran sync frequency reduced from 5min to 15min intervals
  08:59 Lag returned to < 15 minutes. SLA restored.

BREACH DURATION: 47 minutes
BUDGET CONSUMED: 47 / 44 allowed = budget exhausted for November

DOWNSTREAM IMPACT:
  - Finance P&L report delayed to 09:30 (vs 08:30 SLA)
  - Risk team operated on 47-minute-old data for fraud scoring
  - 0 customer-facing SLA penalties triggered (contractual SLA is 1hr)
```

**Key lesson:** The monitoring detected the issue in 2 minutes. Without monitoring, the finance team would have noticed at 08:30 when they checked the dashboard — a 30-minute additional delay.

---

## Data Contracts in Practice

### Producer-Consumer Agreement for the Payments Team

The payments engineering team (producer) and finance team (consumer) signed a data contract in Q3 2024.

```yaml
# contracts/fct_payments_v2.yaml
apiVersion: v2
kind: DataContract
metadata:
  name: fct_payments
  version: "2.1.0"
  effective_date: "2024-10-01"
  producer: payments-data-team@company.com
  consumers:
    - finance@company.com
    - risk-ops@company.com
  review_date: "2025-04-01"

spec:
  sla:
    freshness:
      max_lag_minutes: 15
      measurement: "TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), MINUTE)"
      breach_consequence: "Incident opened, escalation within 30 minutes"
    completeness:
      min_pct: 99.9
      measurement: "COUNT(transaction_id) / expected_count_from_stripe_balance_api"
    availability:
      min_uptime_pct: 99.9
      window_days: 30
  
  breaking_changes:
    notice_required: true
    notice_days: 14
    definition_of_breaking:
      - Column removal
      - Column type change
      - Column rename
      - Adding NOT NULL constraint to existing nullable column
    
  non_breaking_changes:
    notice_required: false
    definition_of_non_breaking:
      - Adding new nullable column
      - Widening numeric precision
      - Adding new accepted value to existing categorical column
```

**Contract validation in CI:**

```bash
# .github/workflows/contract_validation.yml
- name: Validate data contract
  run: |
    python scripts/validate_contract.py \
      --contract contracts/fct_payments_v2.yaml \
      --manifest target/manifest.json \
      --fail-on-breaking-change
```

```python
# scripts/validate_contract.py
def detect_breaking_changes(contract: dict, current_schema: list, previous_schema: list) -> list:
    breaking = []
    
    prev_cols = {c["name"]: c for c in previous_schema}
    curr_cols = {c["name"]: c for c in current_schema}
    
    # Check for removals
    for col in prev_cols:
        if col not in curr_cols:
            breaking.append(f"BREAKING: Column '{col}' was removed")
    
    # Check for type changes
    for col in curr_cols:
        if col in prev_cols:
            if curr_cols[col]["type"] != prev_cols[col]["type"]:
                breaking.append(
                    f"BREAKING: Column '{col}' type changed "
                    f"{prev_cols[col]['type']} → {curr_cols[col]['type']}"
                )
    
    return breaking
```

---

## SLA Reporting Dashboard: Quarterly Review

At the end of each quarter, the data team presents SLA compliance to stakeholders.

```sql
-- Quarterly SLA Compliance Report
WITH quarterly_summary AS (
    SELECT
        pipeline_name,
        DATE_TRUNC(run_started_at, QUARTER) AS quarter,
        COUNT(*) AS total_runs,
        COUNTIF(is_sla_breached) AS breach_count,
        ROUND((1 - COUNTIF(is_sla_breached) / COUNT(*)) * 100, 3) AS compliance_pct,
        AVG(freshness_lag_minutes) AS avg_lag_minutes,
        MAX(freshness_lag_minutes) AS worst_case_lag,
        SUM(
            CASE WHEN is_sla_breached
                THEN GREATEST(freshness_lag_minutes - sla_freshness_minutes, 0)
                ELSE 0
            END
        ) AS total_breach_minutes
    FROM monitoring.pipeline_runs
    WHERE run_started_at >= '2024-01-01'
    GROUP BY 1, 2
)
SELECT
    pipeline_name,
    FORMAT_TIMESTAMP('%Y Q%Q', CAST(quarter AS TIMESTAMP)) AS period,
    total_runs,
    breach_count,
    compliance_pct,
    CONCAT(compliance_pct, '%') AS compliance_display,
    CASE
        WHEN compliance_pct >= 99.9 THEN '✅ Met SLA'
        WHEN compliance_pct >= 99.0 THEN '⚠️ Near SLA'
        ELSE '❌ Missed SLA'
    END AS status
FROM quarterly_summary
ORDER BY pipeline_name, quarter;
```

---

## Lessons from 18 Months of SLA Enforcement

1. **The hardest part is defining SLAs before having data.** Start with a 30-day observation period to understand realistic lag distributions before committing to SLA numbers.

2. **99.9% is easier than 99.99%.** 99.9% allows 44 minutes of breach per month — enough for one medium incident. 99.99% allows 4.3 minutes — effectively zero tolerance. Be very deliberate about which tier each product needs.

3. **Grace periods prevent alert storms.** Adding a 5-minute grace period beyond the SLA threshold (page at 20 min instead of 15 min) eliminates transient alerts without meaningfully reducing protection.

4. **Data contracts changed the relationship with stakeholders.** Before contracts: ad-hoc Slack messages about data issues. After contracts: formal SLA reviews, defined breach consequences, and consumer acknowledgment that non-breaking changes won't cause incidents.

5. **Error budgets made prioritization easier.** When the budget is exhausted, the team can point to a number — "we've used all 44 allowed minutes this month" — to justify freezing risky changes. It depersonalizes the conversation.
