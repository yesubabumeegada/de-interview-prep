---
title: "Data Quality Fundamentals — Real World"
topic: data-quality
subtopic: data-quality-fundamentals
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-quality, production, case-study, pipeline, remediation]
---

# Data Quality Fundamentals — Real World Patterns

## Pattern 1: DQ Gate in an Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd

def ingest_orders(**context):
    df = pd.read_parquet("s3://raw/orders/{{ ds }}.parquet")
    context["ti"].xcom_push(key="row_count", value=len(df))

def run_dq_checks(**context):
    df = pd.read_parquet("s3://raw/orders/{{ ds }}.parquet")
    
    failures = []
    if df["order_id"].isna().any():
        failures.append("NULL order_id")
    if (df["amount"] <= 0).any():
        failures.append("Non-positive amount")
    if df["order_id"].duplicated().any():
        failures.append("Duplicate order_id")
    
    context["ti"].xcom_push(key="dq_failures", value=failures)
    return "dq_failed" if failures else "dq_passed"

def handle_dq_failure(**context):
    failures = context["ti"].xcom_pull(key="dq_failures")
    # Send alert
    send_slack_alert(f"DQ Failed for {context['ds']}: {failures}")
    # Write to quarantine
    df = pd.read_parquet("s3://raw/orders/{{ ds }}.parquet")
    df["failures"] = str(failures)
    df.to_parquet(f"s3://quarantine/orders/{{ ds }}.parquet")

def transform_to_silver(**context):
    df = pd.read_parquet("s3://raw/orders/{{ ds }}.parquet")
    # transformation logic...
    df.to_parquet("s3://silver/orders/{{ ds }}.parquet")

with DAG("orders_pipeline", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=ingest_orders)
    
    dq_check = BranchPythonOperator(task_id="dq_check", python_callable=run_dq_checks)
    
    dq_passed = EmptyOperator(task_id="dq_passed")
    dq_failed = PythonOperator(task_id="dq_failed", python_callable=handle_dq_failure)
    
    transform = PythonOperator(task_id="transform", python_callable=transform_to_silver)
    
    ingest >> dq_check >> [dq_passed, dq_failed]
    dq_passed >> transform
```

---

## Pattern 2: Row-Level Tagging (Soft Fail)

Instead of dropping bad rows, tag them and let consumers decide:

```sql
-- Silver layer: tag each row with DQ status
CREATE TABLE silver.orders AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    -- DQ flags
    CASE WHEN order_id IS NULL THEN TRUE ELSE FALSE END AS dq_null_pk,
    CASE WHEN amount <= 0 THEN TRUE ELSE FALSE END AS dq_invalid_amount,
    CASE WHEN order_date > CURRENT_DATE THEN TRUE ELSE FALSE END AS dq_future_date,
    -- Overall DQ pass
    CASE 
        WHEN order_id IS NULL OR amount <= 0 THEN 'FAILED'
        WHEN order_date > CURRENT_DATE THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    CURRENT_TIMESTAMP AS dq_evaluated_at
FROM bronze.orders;

-- Gold layer: always filter to clean data
CREATE VIEW gold.orders AS
SELECT * EXCEPT (dq_null_pk, dq_invalid_amount, dq_future_date, dq_status, dq_evaluated_at)
FROM silver.orders
WHERE dq_status = 'PASSED';
```

---

## Pattern 3: DQ Metrics Dashboard Query

```sql
-- Daily DQ health report
WITH daily_metrics AS (
    SELECT
        table_name,
        DATE(evaluated_at) AS metric_date,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
        SUM(failing_count) AS total_failing_records,
        SUM(total_count) AS total_records,
        AVG(pass_rate) AS avg_pass_rate
    FROM dq_metrics_store
    WHERE DATE(evaluated_at) >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2
),
with_trend AS (
    SELECT *,
        LAG(avg_pass_rate) OVER (PARTITION BY table_name ORDER BY metric_date) AS prev_day_rate,
        avg_pass_rate - LAG(avg_pass_rate) OVER (
            PARTITION BY table_name ORDER BY metric_date
        ) AS day_over_day_delta
    FROM daily_metrics
)
SELECT
    table_name,
    metric_date,
    ROUND(avg_pass_rate * 100, 2) AS dq_score_pct,
    day_over_day_delta,
    CASE
        WHEN avg_pass_rate >= 0.99 THEN 'GREEN'
        WHEN avg_pass_rate >= 0.95 THEN 'YELLOW'
        ELSE 'RED'
    END AS health_status
FROM with_trend
ORDER BY metric_date DESC, avg_pass_rate ASC;
```

---

## Common Production Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Validate only in dev | DQ issues only caught in prod | Run same checks in all environments |
| Silent failures | Bad data flows downstream undetected | Always log violations, even if non-blocking |
| No rule versioning | Can't tell if new rules caused score drop | Version rules, track when each was introduced |
| Checking aggregate only | Individual bad rows missed | Always check row-level AND aggregate |
| Hard-coding thresholds | Seasonal data causes false alarms | Use rolling baselines, not fixed thresholds |

---

## Interview Story: Fixing a Silent DQ Bug

> **Situation:** A revenue dashboard showed 15% less revenue than expected one Monday morning.
>
> **Investigation:** Traced through lineage — Gold → Silver → Bronze. Found that a CDC pipeline from the orders DB had been silently deduplicating on `(order_id, updated_at)` but the source started emitting microsecond-precision timestamps instead of milliseconds. This meant cancellations weren't deduplicating correctly — they were being treated as new rows and summing canceled amounts into revenue.
>
> **Fix:** Added a DQ check that validated `SUM(net_amount) BETWEEN expected_min AND expected_max` based on a rolling 7-day average. This would have caught the 15% drop on day 1. Also fixed the dedup key to use only `order_id` for the final state.
>
> **Lesson:** Business-logic-level checks (revenue in expected range) catch failures that row-level checks miss.
