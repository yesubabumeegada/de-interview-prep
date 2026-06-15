---
title: "Data Freshness Patterns: Real-World Case Studies and Production Tools"
description: "Real case studies of freshness SLA breaches, remediation playbooks, dbt source freshness, and Great Expectations freshness validation."
content_type: study_material
topic: etl-concepts
subtopic: data-freshness-patterns
layer: real-world
difficulty_level: senior
tags: [case-studies, dbt-freshness, great-expectations, freshness-breach, incident-response, production]
---

# Data Freshness Patterns: Real-World

## Case Study 1: Silent Freshness Degradation at an E-Commerce Company

### Background

A mid-size e-commerce company ran a daily ETL job loading orders into their data warehouse. The pipeline had no freshness monitoring. Analysts used a Looker dashboard for daily revenue reporting.

### What Happened

Over six months, the ETL job's runtime grew from 2 hours to 6 hours as data volume tripled. Nobody noticed because the job still completed successfully — just later. By month 6, the pipeline was completing at 08:30 UTC instead of 03:30 UTC. The Finance team was reviewing "yesterday's" revenue at 09:00 UTC, but the dashboard was actually showing data from two days ago.

A quarterly board meeting used projections based on stale data. The error was caught after the meeting when an analyst cross-checked with the source system.

### Root Causes

1. No freshness SLO defined
2. Pipeline monitoring only checked for job success/failure, not data recency
3. Runtime growth was never alerted
4. Dashboard consumers assumed freshness without verifying

### Remediation

```sql
-- Added freshness metadata table
CREATE TABLE pipeline_audit (
  pipeline_name    VARCHAR(100),
  run_date         DATE,
  started_at       TIMESTAMP,
  completed_at     TIMESTAMP,
  max_event_date   DATE,
  freshness_lag_hr DECIMAL(5,2),
  row_count        BIGINT
);

-- Daily check: alert if dashboard data is > 4 hours stale at 09:00 UTC
CREATE OR REPLACE VIEW v_dashboard_freshness AS
SELECT
  max_event_date,
  completed_at,
  TIMESTAMPDIFF(HOUR, max_event_date, NOW()) AS hours_stale,
  CASE
    WHEN TIMESTAMPDIFF(HOUR, completed_at, NOW()) > 4 THEN 'STALE - CHECK PIPELINE'
    ELSE 'FRESH'
  END AS status
FROM pipeline_audit
WHERE pipeline_name = 'orders_daily'
ORDER BY run_date DESC
LIMIT 1;
```

**Outcome:** Freshness SLO set to "data available by 04:00 UTC, max 1 hour old". Alerting added. Pipeline was refactored to use incremental loads, cutting runtime to 45 minutes.

---

## Case Study 2: Streaming Watermark Misconfig at a Fintech

### Background

A fintech company processed payment transactions using Spark Structured Streaming, writing to a Delta Lake table used for real-time fraud scoring.

### What Happened

A developer deployed a configuration change that increased the watermark from 5 minutes to 2 hours to "reduce late event drops." In testing, this appeared to work correctly. In production, the watermark delay meant Spark held state for 2 hours before emitting aggregated windows.

The fraud model's feature store showed payment velocity aggregates as 2 hours stale. A coordinated fraud ring exploited this window by submitting high volumes of small transactions that exceeded velocity limits — but the features weren't yet aggregated, so the model didn't flag them.

**Business impact:** $340,000 in fraudulent transactions over a 6-hour period.

### Root Causes

1. Watermark change was not reviewed for freshness impact
2. No freshness SLO on the feature store
3. The fraud model assumed near-real-time features but had no input freshness assertion

### Remediation

```python
# Feature freshness assertion added to fraud model inference
def get_payment_velocity_features(customer_id: str) -> dict:
    features = feature_store.get(customer_id, ["velocity_1h", "velocity_24h"])
    
    # Assert feature freshness before scoring
    feature_age_minutes = get_feature_age_minutes(customer_id)
    if feature_age_minutes > 10:
        # Log and use fallback conservative threshold
        log_warning(f"Stale features for {customer_id}: {feature_age_minutes}min old")
        return {"velocity_1h": 0, "velocity_24h": 0, "is_stale": True}
    
    return features

# Watermark reduced back to 5 minutes, late events handled via side output
orders_stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(F.window("event_time", "1 hour"), "customer_id") \
    .agg(F.count("*").alias("velocity_1h"))
```

**Outcome:** Watermark configuration changes now require freshness impact review. Feature freshness is checked before every model inference call.

---

## Case Study 3: Partition Completeness Gap at a Media Company

### Background

A media streaming company tracked content views in a daily partitioned BigQuery table. Data pipelines loaded each day's data from cloud storage the following morning.

### What Happened

A region-specific storage incident caused files from the APAC region to be missing for 3 days. The pipeline completed successfully because it treated missing files as "no data," not as an error. The `MAX(event_date)` check showed the table as fresh. Content recommendation models were trained daily on this table and showed degraded performance for APAC users for 10 days (3 days of incident + 7 days for model quality to recover).

### Root Causes

1. Completeness was not validated — only freshness (max event date)
2. Source file counts were not compared against expected counts
3. Model quality degradation wasn't linked back to data completeness

### Remediation

```python
# Source completeness validation before loading
class SourceCompletenessValidator:
    def __init__(self, gcs_client, bq_client):
        self.gcs = gcs_client
        self.bq = bq_client
    
    def validate_partition(self, date: str, region: str) -> bool:
        """
        Validate that the number of source files matches expected count
        before loading to BigQuery.
        """
        expected_file_count = self.get_expected_file_count(date, region)
        actual_files = self.gcs.list_objects(
            bucket="events-raw",
            prefix=f"views/{region}/{date}/"
        )
        actual_count = len(list(actual_files))
        
        if actual_count < expected_file_count * 0.95:  # Allow 5% variance
            raise ValueError(
                f"Completeness check failed for {region}/{date}: "
                f"expected {expected_file_count} files, got {actual_count}"
            )
        return True
    
    def get_expected_file_count(self, date: str, region: str) -> int:
        """Look up expected file count from historical baseline."""
        result = self.bq.query("""
            SELECT APPROX_QUANTILES(file_count, 100)[OFFSET(10)] AS p10_count
            FROM source_file_registry
            WHERE region = @region
              AND event_date BETWEEN DATE_SUB(@date, INTERVAL 28 DAY) AND @date
        """, params={"date": date, "region": region})
        return result[0]["p10_count"]
```

---

## dbt Source Freshness

dbt provides native freshness checking for source tables. When you define a `loaded_at_field` on a source, dbt can measure and alert on staleness.

### Configuring dbt Source Freshness

```yaml
# models/sources.yml
version: 2

sources:
  - name: raw_events
    database: production
    schema: raw

    freshness:
      warn_after:
        count: 6
        period: hour
      error_after:
        count: 24
        period: hour

    tables:
      - name: orders
        loaded_at_field: _airbyte_emitted_at  # Timestamp when record was loaded

      - name: payments
        loaded_at_field: _loaded_at
        freshness:
          # Table-level override: payments must be fresher than orders
          warn_after:
            count: 1
            period: hour
          error_after:
            count: 3
            period: hour

      - name: events_partitioned
        loaded_at_field: event_time
        freshness:
          warn_after:
            count: 2
            period: hour
          error_after:
            count: 6
            period: hour
```

### Running dbt Freshness Checks

```bash
# Check all source freshness
dbt source freshness

# Check specific source
dbt source freshness --select source:raw_events

# Output to JSON for downstream processing
dbt source freshness --output json > freshness_results.json
```

### Parsing dbt Freshness Results

```python
import json
from datetime import datetime

def parse_dbt_freshness(results_path: str) -> list:
    with open(results_path) as f:
        results = json.load(f)
    
    freshness_summary = []
    for source in results["results"]:
        status = source["status"]  # "pass", "warn", "error"
        age_seconds = source.get("max_loaded_at_time_ago_in_s", 0)
        
        freshness_summary.append({
            "source": source["unique_id"],
            "status": status,
            "age_hours": round(age_seconds / 3600, 2),
            "max_loaded_at": source.get("max_loaded_at"),
            "is_healthy": status == "pass"
        })
    
    return freshness_summary

def post_freshness_to_slack(results: list, webhook_url: str):
    stale = [r for r in results if not r["is_healthy"]]
    if not stale:
        return
    
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "dbt Source Freshness Alerts"}},
    ]
    for s in stale:
        emoji = "⚠️" if s["status"] == "warn" else "🔴"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"{emoji} *{s['source']}*: {s['age_hours']}h old (status: {s['status']})"}
        })
    
    requests.post(webhook_url, json={"blocks": blocks})
```

### dbt Freshness in CI/CD

```yaml
# .github/workflows/dbt-freshness.yml
name: dbt Source Freshness Check
on:
  schedule:
    - cron: '0 * * * *'  # Every hour

jobs:
  freshness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install dbt
        run: pip install dbt-bigquery==1.7.0
      - name: Run freshness check
        run: dbt source freshness --output json 2>&1 | tee freshness.json
        env:
          DBT_PROFILES_DIR: ./profiles
      - name: Post to Slack on failure
        if: failure()
        run: python scripts/post_freshness_alert.py freshness.json
```

---

## Great Expectations: Freshness Validations

Great Expectations (GX) provides expectations that can enforce freshness as part of data quality validation suites.

### Freshness Expectations

```python
import great_expectations as gx
from datetime import datetime, timedelta

context = gx.get_context()

# Define a freshness expectation suite
suite = context.add_expectation_suite("orders_freshness_suite")

# Connect to datasource
datasource = context.sources.add_or_update_pandas_filesystem(
    name="orders_datasource",
    base_directory="./data"
)

asset = datasource.add_csv_asset("orders", "orders.csv")
batch_request = asset.build_batch_request()
validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

# Expect the max event_time to be recent (within last 60 minutes)
sixty_min_ago = (datetime.utcnow() - timedelta(minutes=60)).isoformat()

validator.expect_column_max_to_be_between(
    column="event_time",
    min_value=sixty_min_ago,
    max_value=None,
    meta={
        "notes": "Orders must contain events from the last 60 minutes",
        "freshness_slo": "60 minutes"
    }
)

# Expect no rows older than 48 hours (catches old test data in prod)
two_days_ago = (datetime.utcnow() - timedelta(hours=48)).isoformat()
validator.expect_column_values_to_be_between(
    column="event_time",
    min_value=two_days_ago,
    max_value=None,
    mostly=0.99  # 99% of rows must meet this — allows small % of late events
)

validator.save_expectation_suite(discard_failed_expectations=False)
```

### Custom GX Freshness Expectation

```python
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.execution_engine import (
    PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
)

class ExpectColumnMaxToBeWithinMinutes(ColumnAggregateExpectation):
    """
    Custom expectation: the max value of a timestamp column must be within
    N minutes of the current time.
    """
    
    examples = [
        {
            "data": {"event_time": ["2024-01-15 10:00:00", "2024-01-15 10:30:00"]},
            "tests": [
                {"title": "Fresh data", "exact_match_out": False, "in": {"column": "event_time", "max_age_minutes": 90}, "out": {"success": True}}
            ]
        }
    ]
    
    metric_dependencies = ("column.max",)
    success_keys = ("max_age_minutes",)
    
    def _validate(self, metrics, runtime_configuration=None, execution_engine=None):
        max_age_minutes = self.get_success_kwargs()["max_age_minutes"]
        column_max = metrics["column.max"]
        
        if column_max is None:
            return {"success": False, "result": {"observed_value": None}}
        
        age_minutes = (datetime.utcnow() - column_max).total_seconds() / 60
        return {
            "success": age_minutes <= max_age_minutes,
            "result": {
                "observed_value": f"{age_minutes:.1f} minutes",
                "max_age_minutes": max_age_minutes
            }
        }
```

---

## Production Freshness Runbook

### Incident Response for Freshness Breach

```
FRESHNESS BREACH RUNBOOK
=========================

1. TRIAGE (0-5 minutes)
   □ Identify which pipelines are stale (check freshness dashboard)
   □ Check if breach is isolated (one table) or systemic (multiple tables)
   □ Determine downstream impact (which dashboards/models are affected)
   □ Notify stakeholders via #data-incidents Slack channel

2. DIAGNOSE (5-15 minutes)
   □ Check pipeline run history: did the job run? Did it complete?
   □ Check pipeline logs for errors
   □ Check source system availability (is the upstream database/API up?)
   □ Check infrastructure (are workers healthy? Any resource limits hit?)
   □ If streaming: check Kafka consumer lag metrics

3. REMEDIATE (15-60 minutes)
   Scenario A — Job failed:
     □ Fix the root cause (schema change, resource issue, credential expiry)
     □ Trigger manual backfill for missed interval
     □ Verify freshness restored via monitoring dashboard
   
   Scenario B — Job slow:
     □ Check for data volume spike
     □ Scale compute if needed
     □ Consider incremental load optimization
   
   Scenario C — Source unavailable:
     □ Escalate to source system owner
     □ Consider publishing estimated freshness to stakeholders
     □ Implement retry with exponential backoff

4. COMMUNICATE
   □ Update #data-incidents with root cause and ETA for resolution
   □ Send summary email to stakeholders after resolution
   □ Document in post-mortem if breach > 2x SLO duration

5. PREVENT
   □ Add test case to cover failure mode
   □ Update runbook if new failure mode discovered
   □ Review if SLO needs adjustment based on actual stakeholder impact
```

---

## Key Takeaways from Production Experience

1. **Freshness breaches are silent by default** — systems don't fail; they just serve stale data. You must proactively monitor.
2. **Completeness and freshness must both be checked** — a table can appear fresh while missing entire regions or partitions.
3. **Downstream consumers should assert input freshness** — don't assume upstream SLOs are met; validate before consuming.
4. **dbt source freshness** is the simplest way to add freshness checks to an existing dbt project.
5. **Incident response speed matters** — a 60-minute SLO breach that's detected and fixed in 20 minutes has far less business impact than one detected 4 hours later.
