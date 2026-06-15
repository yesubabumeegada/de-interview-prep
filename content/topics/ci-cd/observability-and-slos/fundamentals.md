---
title: "Observability and SLOs for Data Pipelines - Fundamentals"
topic: ci-cd
subtopic: observability-and-slos
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, observability, slo, sla, sli, data-quality, monitoring, metrics, logs]
---

# Observability and SLOs for Data Pipelines — Fundamentals

## The Three Pillars of Observability

Observability is the ability to understand the internal state of a system by examining its external outputs. Originally coined for distributed software systems, it applies directly to data pipelines. The three pillars are:

1. **Metrics**: Numerical measurements collected over time (e.g., pipeline duration, row counts, failure rate)
2. **Logs**: Timestamped records of events (e.g., task started, record processed, error message)
3. **Traces**: End-to-end records of a request or job moving through multiple systems (e.g., tracking a single order record from Kafka ingestion through transformation to final warehouse load)

For data systems, each pillar answers different questions:

- **Metrics**: "Is my pipeline slow today? Are row counts normal? How many failures this week?"
- **Logs**: "What error occurred in the 3am run? Which records failed validation? What was the execution plan?"
- **Traces**: "Why did this order take 4 hours to appear in the dashboard? Where was the bottleneck?"

## SLI, SLO, and SLA: Definitions You Must Know

These three acronyms are frequently confused in interviews. Know the precise distinction:

### Service Level Indicator (SLI)
An SLI is a **measurable metric** that indicates service quality. It's a quantitative measurement of behavior. Examples for data pipelines:

- Percentage of daily runs that complete successfully
- Data freshness: age of the most recent data in a table
- Pipeline latency: time from source event to data availability in the warehouse
- Data completeness: percentage of expected records that arrived

### Service Level Objective (SLO)
An SLO is a **target** for an SLI, expressed as a percentage over a time window. It's an internal commitment:

```
SLO: "The orders table will have data less than 4 hours old for 99.5% of 5-minute measurement windows over any rolling 30-day period."

SLI: age of the most recent record in analytics.fct_orders
Target: < 4 hours
Compliance window: 99.5% of measurements
Period: rolling 30 days
```

### Service Level Agreement (SLA)
An SLA is a **contractual commitment** to an external party (business, customer, legal agreement). If you violate an SLA, there are consequences (refunds, penalties, damaged relationships). SLAs are typically less strict than SLOs to give a buffer:

```
SLA (external, to business stakeholders):
"The daily revenue dashboard will reflect data from the previous day by 8am, 
99% of business days."

SLO (internal, engineering commitment):
"The revenue pipeline will complete by 7am, 99.5% of business days."

The 1-hour buffer (7am vs 8am) and stricter internal target (99.5% vs 99%) 
give the team room to absorb incidents before breaching the SLA.
```

## The Error Budget Concept

The error budget is the amount of downtime or SLO violations you're allowed before breaching your SLO. It makes reliability a shared resource to be managed, not an infinite goal:

```
SLO: 99.5% data freshness compliance over 30 days

Total minutes in 30 days: 43,200
Allowed violations (0.5%): 43,200 × 0.005 = 216 minutes (~3.6 hours)

Error budget: 216 minutes per rolling 30-day window
```

If you've burned 200 of your 216 error budget minutes on Day 20, you are nearly out of budget. This should trigger:
- A freeze on risky deployments until the budget resets
- A review of what caused the violations
- Prioritization of reliability improvements over new features

If you always have budget to spare (never burn more than 50%), you might be over-investing in reliability and could safely deploy faster.

## Data Freshness Monitoring

Data freshness is the most commonly tracked SLO for data pipelines. "How stale is my data?" is the first question stakeholders ask when a dashboard looks wrong.

### dbt Source Freshness

dbt has built-in source freshness checking. Configure freshness thresholds in your sources YAML:

```yaml
# models/staging/schema.yml
sources:
  - name: raw
    database: RAW
    schema: EVENTS
    tables:
      - name: orders
        loaded_at_field: _ingested_at  # Column that indicates when the record arrived
        freshness:
          warn_after: {count: 6, period: hour}   # Warning if data is 6+ hours old
          error_after: {count: 24, period: hour}  # Error if data is 24+ hours old

      - name: customers
        loaded_at_field: _ingested_at
        freshness:
          warn_after: {count: 25, period: hour}
          error_after: {count: 49, period: hour}
```

```bash
# Run freshness check (typically as a step before dbt run)
dbt source freshness

# Output:
# Found 1 source, 2 tables.
# 09:15:01 | Freshness of raw.events.orders ........ [warn in 0.48s]
#   Last record: 7 hours ago (threshold: warn 6h, error 24h)
# 09:15:02 | Freshness of raw.events.customers ...... [pass in 0.51s]
#   Last record: 12 hours ago (threshold: warn 25h, error 49h)
```

### Custom Freshness Checks in Airflow

```python
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import snowflake.connector
from datetime import datetime, timedelta

def check_orders_freshness(**context):
    """Fail the DAG if orders data is more than 4 hours stale."""
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        database='ANALYTICS',
        schema='MARTS',
    )

    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            MAX(order_created_at) as latest_order,
            DATEDIFF('minute', MAX(order_created_at), CURRENT_TIMESTAMP()) as minutes_stale
        FROM fct_orders
    """)

    row = cursor.fetchone()
    latest_order, minutes_stale = row

    freshness_threshold_minutes = 4 * 60  # 4 hours

    if minutes_stale > freshness_threshold_minutes:
        raise ValueError(
            f"Data freshness SLO violated! "
            f"Latest order: {latest_order} ({minutes_stale} minutes ago). "
            f"Threshold: {freshness_threshold_minutes} minutes."
        )

    print(f"Freshness OK: latest order {minutes_stale} minutes ago")

freshness_check = PythonOperator(
    task_id='check_orders_freshness',
    python_callable=check_orders_freshness,
    dag=dag,
)
```

## Basic Metrics Every Data Engineer Should Track

For any production data pipeline, these are the minimum metrics to collect:

| Metric | Description | Why It Matters |
|--------|-------------|----------------|
| Pipeline duration | Time from start to completion | Detects slow pipelines before they miss SLOs |
| Row count | Number of records processed | Anomalies often appear as volume drops or spikes |
| Success rate | % of runs that complete successfully | The core reliability metric |
| Data freshness | Age of most recent data | Directly tied to the most common SLO |
| Error rate | % of records that fail validation | Data quality signal |

## Key Interview Takeaways

- **SLI** = the measurement (data is 6 hours old), **SLO** = the target (data must be < 4 hours old, 99.5% of the time), **SLA** = the contract (data available by 8am per business agreement)
- The **error budget** quantifies how much unreliability you're allowed — it makes reliability a manageable resource
- **dbt source freshness** is the simplest way to add data freshness monitoring to an existing dbt project
- Data pipeline observability pillars: **metrics** (how often/how slow), **logs** (what happened), **traces** (where was the bottleneck)
- Freshness, volume, and success rate are the three most universally important data pipeline metrics
