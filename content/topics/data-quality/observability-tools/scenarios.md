---
title: "Data Observability Tools - Scenario Questions"
topic: data-quality
subtopic: observability-tools
content_type: scenario_question
tags: [data-quality, observability, monitoring, scenarios, interview]
---

# Data Observability Tools — Scenario Questions

<article data-difficulty="junior">

## Scenario (Junior): First Data Quality Incident

You're a junior data engineer and an analyst comes to you saying "the revenue number in the dashboard dropped 50% overnight — it was $2M yesterday and shows $1M today." You have no observability tooling in place. The pipeline ran successfully with no errors.

**Question:** How do you investigate? What checks would you run, and what would you put in place to catch this faster next time?

<details>
<summary>✅ Solution</summary>

**Immediate Investigation (Triage)**

Start by checking the most likely causes in order of probability:

```sql
-- 1. Check if today's data actually loaded
SELECT
    DATE(_updated_at) AS load_date,
    COUNT(*) AS row_count,
    SUM(revenue) AS total_revenue
FROM analytics.fct_orders
WHERE DATE(_updated_at) >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 1 DESC;
-- If today's row count is half of yesterday → data didn't fully load

-- 2. Check source freshness
SELECT MAX(_fivetran_synced) AS last_sync
FROM raw.stripe.charges;
-- If this is >2 hours old → Fivetran sync failed

-- 3. Check for unexpected NULLs introduced today
SELECT
    DATE(created_at) AS date,
    COUNT(*) AS total_rows,
    COUNTIF(revenue IS NULL) AS null_revenue_rows,
    COUNTIF(revenue IS NULL) / COUNT(*) AS null_rate
FROM analytics.fct_orders
WHERE DATE(created_at) >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 1 DESC;
-- If null_rate jumped today → upstream schema change or NULL propagation

-- 4. Check if a filter changed in a dbt model
-- Pull the git log for the model file
```

**Quick Prevention for Next Time**

Add three checks to the Airflow DAG that runs after the pipeline:

```python
@task
def check_revenue_sanity():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
    
    result = hook.get_first("""
        WITH today AS (
            SELECT SUM(revenue) AS rev
            FROM analytics.fct_orders
            WHERE DATE(created_at) = CURRENT_DATE()
        ),
        yesterday AS (
            SELECT SUM(revenue) AS rev
            FROM analytics.fct_orders
            WHERE DATE(created_at) = CURRENT_DATE() - 1
        )
        SELECT today.rev, yesterday.rev,
               today.rev / NULLIF(yesterday.rev, 0) AS ratio
        FROM today, yesterday
    """)
    
    today_rev, yesterday_rev, ratio = result
    if ratio is not None and ratio < 0.7:
        raise ValueError(
            f"Revenue dropped >30%: today=${today_rev:,.0f} vs yesterday=${yesterday_rev:,.0f}"
        )
```

This gives you an alert within 30 minutes of pipeline completion instead of waiting for an analyst to notice.

**Root Cause Hypothesis to Share with Your Manager:**
1. Fivetran sync partially failed (most common)
2. A dbt model change introduced a narrower filter
3. Source system bug sent partial data
4. NULL propagation from upstream schema change
</details>

</article>

---

<article data-difficulty="mid">

## Scenario (Mid-Level): Designing a Freshness Monitor for Multi-Source Pipelines

Your company has 3 data sources: Salesforce (synced hourly via Fivetran), a PostgreSQL app DB (CDC via Debezium, near real-time), and a vendor SFTP drop (daily at 3am). You need to monitor freshness for all three, with different expectations, and alert appropriately.

**Question:** Design the freshness monitoring system. Include: how you define SLAs per source, the implementation, and the alert routing.

<details>
<summary>✅ Solution</summary>

### SLA Definition Per Source

| Source | Expected Arrival | Warn After | Error After | On-Call Trigger |
|---|---|---|---|---|
| Salesforce (Fivetran) | Every 60 min | 90 min | 3 hours | Business hours only |
| PostgreSQL CDC | Near real-time | 15 min | 45 min | 24/7 PagerDuty |
| SFTP Vendor Drop | Daily by 4am UTC | 1 hour late (5am) | 3 hours late (7am) | Business hours only |

### Implementation

```python
# plugins/monitors/freshness_monitor.py
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dataclass
class FreshnessConfig:
    source_name: str
    table: str
    timestamp_col: str
    warn_after_minutes: int
    error_after_minutes: int
    pagerduty_always: bool  # if False, only pages during business hours

FRESHNESS_CONFIGS = [
    FreshnessConfig("salesforce", "raw.salesforce.opportunity", "_fivetran_synced", 90, 180, False),
    FreshnessConfig("postgres_cdc", "raw.app_db.orders", "_cdc_ingested_at", 15, 45, True),
    FreshnessConfig("vendor_sftp", "raw.vendor.daily_feed", "file_date", 60, 180, False),
]

def check_freshness(config: FreshnessConfig) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
    result = hook.get_first(f"""
        SELECT
            MAX({config.timestamp_col}) AS last_updated,
            DATEDIFF('minute', MAX({config.timestamp_col}), CURRENT_TIMESTAMP()) AS minutes_stale
        FROM {config.table}
    """)
    
    last_updated, minutes_stale = result
    
    status = "ok"
    if minutes_stale > config.error_after_minutes:
        status = "error"
    elif minutes_stale > config.warn_after_minutes:
        status = "warn"
    
    return {
        "source": config.source_name,
        "table": config.table,
        "last_updated": last_updated,
        "minutes_stale": minutes_stale,
        "status": status,
        "should_page": config.pagerduty_always or _is_business_hours(),
    }

def _is_business_hours() -> bool:
    now = datetime.now(timezone.utc)
    # Business hours: Mon-Fri 9am-6pm UTC
    return now.weekday() < 5 and 9 <= now.hour < 18
```

### Alert Routing

```python
def route_freshness_alert(check_result: dict):
    status = check_result["status"]
    source = check_result["source"]
    minutes = check_result["minutes_stale"]
    
    if status == "ok":
        return
    
    message = (
        f":clock1: *Freshness {status.upper()}*: `{check_result['table']}`\n"
        f"Last updated: {minutes} minutes ago (SLA: warn={...})"
    )
    
    # Always notify Slack
    post_slack(SLACK_DATA_ALERTS_CHANNEL, message)
    
    # Page only if appropriate
    if status == "error" and check_result["should_page"]:
        trigger_pagerduty(
            summary=f"Data freshness error: {source} is {minutes}m stale",
            severity="critical" if minutes > 120 else "warning",
        )
```

**Key Design Decisions:**
- Business-hours-only paging prevents 3am alerts for vendor SFTP (stakeholders aren't impacted until morning)
- PostgreSQL CDC pages 24/7 because near-real-time latency means customer-facing features may be broken
- Separate warn and error thresholds allow graceful degradation (warn = investigate, error = page)
</details>

</article>

---

<article data-difficulty="senior">

## Scenario (Senior): Evaluating and Deploying a Commercial Observability Tool

Your VP of Data Engineering says: "I keep hearing about data observability tools. Evaluate Monte Carlo vs building our own and give me a recommendation." Your stack: Databricks + dbt + Airflow on AWS, 600 tables, 8-person DE team, $3M annual data infrastructure budget.

**Question:** Walk through your evaluation framework, the key criteria, and how you would present the recommendation.

<details>
<summary>✅ Solution</summary>

### Evaluation Framework

**Step 1: Quantify the Problem**

Before evaluating tools, measure the current cost of data quality issues:

```python
# Pull from your incident tracking system
incidents_2024 = query_incidents(year=2024, category="data_quality")

total_engineer_hours = sum(i.resolution_hours for i in incidents_2024)
avg_hourly_cost = 150  # loaded cost including overhead
annual_incident_cost = total_engineer_hours * avg_hourly_cost

# Also measure analyst time wasted
analyst_investigation_hours = 3 * 52  # ~3 hrs/week × 52 weeks (from team survey)
analyst_cost = analyst_investigation_hours * 120  # analyst hourly rate

print(f"Annual incident cost: ${annual_incident_cost:,.0f}")
print(f"Annual analyst productivity loss: ${analyst_cost:,.0f}")
# Example output: $180,000 + $18,720 = ~$200,000/year
```

**Step 2: Define Requirements**

| Requirement | Priority | Monte Carlo | DIY |
|---|---|---|---|
| Auto-detect schema changes | P0 | ✅ | ⚠️ (3 weeks to build) |
| Databricks integration | P0 | ✅ | ✅ |
| Column-level lineage | P0 | ✅ | ❌ (very hard to build) |
| dbt test integration | P1 | ✅ | ✅ |
| Distribution anomaly detection | P1 | ✅ | ⚠️ (6 weeks to build) |
| Cost | P1 | $90K/yr est. | $40K/yr eng time |
| Time to value | P1 | 2 weeks | 3–4 months |
| Custom business rules | P2 | ✅ | ✅ |

**Step 3: Run a Structured POC**

```
Week 1: Connect Monte Carlo to Databricks + dbt
Week 2: Enable monitors on top 50 tables
Week 3: Run parallel with existing custom checks
Week 4: Measure: false positive rate, MTTD vs baseline, engineer time spent

Success criteria:
- MTTD < 30 minutes for volume anomalies
- < 5 false positives per week (otherwise alert fatigue)
- Schema change detection within 15 minutes
```

**Step 4: TCO Comparison**

| | Monte Carlo | DIY |
|---|---|---|
| Tool license | $90,000/yr | $0 |
| Engineer time (build) | $0 | $120,000 (3 eng × 6 weeks × $40/hr loaded) |
| Engineer time (maintain) | $15,000/yr | $60,000/yr (0.5 FTE) |
| **Year 1 total** | **$105,000** | **$180,000** |
| **Year 2+ total** | **$105,000/yr** | **$60,000/yr** |

**Step 5: Recommendation Structure for VP**

Present as a decision memo, not a slide deck:

```
RECOMMENDATION: Purchase Monte Carlo for Year 1 with a Year 2 review.

Rationale:
1. Column-level lineage is the decisive factor. Building automated lineage
   from Databricks query logs requires significant infrastructure (Spark
   listener + graph DB + UI). Estimated 8–12 weeks of senior engineer time.
   Monte Carlo provides this on day 1.

2. Year 1 ROI: We spend $105K on Monte Carlo vs $180K to build equivalent
   coverage. We break even in ~7 months if we prevent just 2 major incidents.

3. Risk: Year 2+ DIY is cheaper ($60K vs $105K). If Monte Carlo doesn't
   deliver or raises prices, we can migrate with the institutional knowledge
   gained from their approach.

4. Condition: Negotiate for a data export guarantee (all metrics + lineage
   data exportable via API) to avoid lock-in.

Action items:
- Legal: review vendor contract, verify data residency (must stay in AWS us-east-1)
- Security: review Monte Carlo's access model (read-only warehouse credentials)
- Finance: $90K approved under observability budget line item
```

**Key differentiator in your answer:** Interviewers look for the "Year 2 review" — showing you understand the build vs. buy calculus changes as the product matures and your team grows expertise.
</details>

</article>
