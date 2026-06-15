---
title: "Data Observability Tools - Real World"
topic: data-quality
subtopic: observability-tools
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [data-quality, observability, production, case-study, monte-carlo, incident-management]
---

# Data Observability Tools — Real World

## Case Study: B2B SaaS — From Zero Observability to Production Platform

### Context

A B2B SaaS company with 200 customers ran a 400-model dbt project on Snowflake. The data team (3 engineers) was fielding 3–5 Slack messages per week from analysts asking "why does this number look wrong?" Each investigation took 2–4 hours of engineering time.

### Phase 1: Emergency Fixes (Week 1–2)

Before any tooling, they added the most impactful monitoring manually:

```python
# dags/monitoring/quick_wins.py
"""
Airflow DAG: 4 checks that catch 80% of issues.
Runs 30 minutes after the main pipeline completes.
"""
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum

CRITICAL_TABLES = [
    ("analytics", "fct_mrr"),
    ("analytics", "fct_usage_events"),
    ("analytics", "dim_accounts"),
]

@dag(schedule="30 7 * * *", start_date=pendulum.datetime(2024, 1, 1))
def data_quick_wins():
    
    @task
    def check_freshness():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
        issues = []
        for db, table in CRITICAL_TABLES:
            result = hook.get_first(f"""
                SELECT DATEDIFF('hour', MAX(_updated_at), CURRENT_TIMESTAMP())
                FROM {db}.{table}
            """)
            hours_stale = result[0]
            if hours_stale > 4:
                issues.append(f"{db}.{table} is {hours_stale}h stale")
        return issues
    
    @task
    def check_volume(freshness_issues: list):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
        issues = []
        for db, table in CRITICAL_TABLES:
            result = hook.get_first(f"""
                WITH today AS (
                    SELECT COUNT(*) AS n
                    FROM {db}.{table}
                    WHERE DATE(_updated_at) = CURRENT_DATE()
                ),
                yesterday AS (
                    SELECT COUNT(*) AS n
                    FROM {db}.{table}
                    WHERE DATE(_updated_at) = CURRENT_DATE() - 1
                )
                SELECT today.n, yesterday.n,
                    CASE WHEN yesterday.n = 0 THEN NULL
                         ELSE today.n / yesterday.n::FLOAT END AS ratio
                FROM today, yesterday
            """)
            today_n, yesterday_n, ratio = result
            if ratio is not None and ratio < 0.5:
                issues.append(
                    f"{db}.{table}: today={today_n:,} vs yesterday={yesterday_n:,} (ratio={ratio:.2f})"
                )
        return issues
    
    @task
    def alert_if_issues(freshness_issues, volume_issues):
        import requests
        all_issues = freshness_issues + volume_issues
        if all_issues:
            text = ":red_circle: *Data Quality Alert*\n" + "\n".join(f"• {i}" for i in all_issues)
            requests.post(SLACK_WEBHOOK, json={"text": text})
    
    fresh = check_freshness()
    vol = check_volume(fresh)
    alert_if_issues(fresh, vol)

data_quick_wins()
```

**Result:** The 4 checks ran for 2 weeks. They caught 7 real incidents that would have gone undetected for hours.

---

## Phase 2: Monte Carlo Evaluation (Week 3–6)

The team ran a free 30-day Monte Carlo trial alongside their custom monitors.

### What Monte Carlo Caught That Custom Checks Missed

1. **Silent schema change:** A Fivetran connector added a new nullable column `campaign_id` to the `usage_events` source table. Custom checks didn't look at schema. Monte Carlo alerted within 12 minutes.

2. **Distribution drift:** The average `session_duration` in usage events dropped from 340 seconds to 12 seconds after a front-end deployment sent malformed events. The volume didn't drop — just the values. Custom volume checks missed it entirely.

3. **Lineage clarity:** When an alert fired on `fct_mrr`, Monte Carlo's lineage view immediately showed which of the 23 upstream models was the root cause — without the team manually tracing the dbt DAG.

### Monte Carlo Rollout Configuration

```python
# Monte Carlo is configured via its API for custom monitors
import monte_carlo_client as mc

client = mc.MonteCarloClient(api_key=MC_API_KEY, api_secret=MC_API_SECRET)

# Add a custom volume rule for a critical table
client.create_custom_rule(
    comparisons=[
        {
            "type": "THRESHOLD",
            "metric": "VOLUME",
            "operator": "LESS_THAN",
            "threshold": 100,
            "is_threshold_relative": False,
        }
    ],
    description="MRR table must have > 100 rows daily",
    table_id="analytics:fct_mrr",
)

# Add a freshness rule
client.create_freshness_rule(
    table_id="analytics:fct_mrr",
    sla_hours=4,
    breach_action="PAGE",
)
```

---

## Real Incident: The Silent NULL Propagation

### Timeline

```
T+0:00  Fivetran sync completes for Stripe source
T+0:15  dbt run completes — no test failures
T+0:20  Monte Carlo alerts: null_rate for fct_mrr.mrr_amount jumped from 0.1% to 31%
T+0:22  PagerDuty incident opened, DE on-call paged
T+0:35  Root cause identified: Stripe changed API response format — "amount" field
        became nested object instead of integer in new API version
        Fivetran parsed it as NULL for the new-format records
T+1:10  Hotfix deployed: Fivetran schema mapping updated, historical records backfilled
T+1:30  Incident resolved. MRR report delayed by 1.5 hours (within SLA)
```

### Why Tests Didn't Catch It

The dbt `not_null` test on `mrr_amount` had been removed 3 months earlier during a "cleanup" because some historical records were legitimately null (trial customers). The test removal was low-risk at the time but left a gap.

**Lesson:** Monte Carlo's ML-based null rate monitoring caught a 30x spike in nulls that a static `not_null` test couldn't detect (because nulls were allowed). This is the core value proposition: detecting *changes* rather than asserting *static rules*.

---

## Build vs Buy: Final Decision

After the trial, the team made the following analysis:

| Factor | Custom Monitors | Monte Carlo |
|---|---|---|
| Time to detect schema changes | Hours (next manual check) | Minutes (automated) |
| Distribution anomalies | Not implemented | Automated |
| Lineage | Manual dbt graph traversal | Automatic, visual |
| Maintenance burden | ~4 hrs/week | ~1 hr/week |
| Annual cost | $15K (engineer time) | $72K |
| Tables monitored | 12 (manual selection) | 400 (all, automatic) |

**Decision:** Buy. The coverage gap (12 vs 400 tables) was the deciding factor. With 400 models, manual maintenance of custom monitors would require a dedicated engineer.

---

## OpenMetadata: Open-Source Alternative in Production

A larger team at a Series B company chose OpenMetadata instead of Monte Carlo to avoid vendor lock-in.

### Setup

```yaml
# docker-compose.yml (simplified)
services:
  openmetadata-server:
    image: openmetadata/server:1.3.0
    ports:
      - "8585:8585"
    environment:
      DB_HOST: postgres
      AIRFLOW_HOST: http://airflow:8080
  
  ingestion:
    image: openmetadata/ingestion:1.3.0
    # Runs Airflow internally for metadata ingestion
```

### Connecting dbt Artifacts

```yaml
# ingestion/dbt_ingestion.yaml
source:
  type: dbt
  serviceName: prod_dbt
  sourceConfig:
    config:
      type: DBT
      dbtConfigSource:
        dbtCatalogFilePath: /artifacts/catalog.json
        dbtManifestFilePath: /artifacts/manifest.json
        dbtRunResultsFilePath: /artifacts/run_results.json
      dbtUpdateDescriptions: true
      includeTags: true
      dbtClassificationName: dbt_tags
```

This imports dbt model descriptions, lineage, and test results directly into OpenMetadata's UI — giving analysts a searchable catalog with quality indicators.

---

## Key Lessons from Production

1. **Start with freshness and volume.** These two pillars catch 70% of real incidents. Everything else is incremental.

2. **Distribution monitoring is where commercial tools earn their keep.** It's the hardest pillar to build yourself and the most valuable — it catches gradual drift that static tests miss.

3. **Lineage is 10x more valuable during an incident.** Knowing "which 5 dashboards are showing wrong numbers right now" focuses remediation effort.

4. **ML thresholds need 2–4 weeks of data to be reliable.** During Monte Carlo onboarding, expect false positives for the first month.

5. **Observability is not a replacement for dbt tests.** Observability catches anomalies in production; dbt tests enforce invariants at deploy time. Both are needed.
