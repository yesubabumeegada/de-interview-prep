---
title: "Data Quality - Intermediate"
topic: etl-concepts
subtopic: data-quality
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, data-quality, great-expectations, data-contracts, anomaly-detection, dbt]
---

# Data Quality — Intermediate

## Data Contracts

A **data contract** is a formal, versioned agreement between data producers and consumers defining the schema, semantics, and quality guarantees of a dataset.

```yaml
# data_contract_orders_v2.yaml
id: urn:datacontract:orders:v2
name: Orders Data Contract
version: 2.0.0
owner: data-platform-team
status: active

models:
  orders:
    description: Confirmed customer orders
    fields:
      order_id:
        type: string
        required: true
        unique: true
        pattern: "^ORD-[0-9]{10}$"
      customer_id:
        type: string
        required: true
      total_usd:
        type: number
        required: true
        minimum: 0
        maximum: 100000
      status:
        type: string
        enum: [pending, processing, shipped, delivered, cancelled, refunded]
      created_at:
        type: timestamp
        required: true

quality:
  row_count:
    min: 1000
    warning_threshold: 500
  freshness:
    max_age_hours: 2
  completeness:
    customer_id: 100%
    total_usd: 100%
    status: 100%

sla:
  availability: 99.9%
  freshness_hours: 2
  support_channel: "#data-platform-oncall"
```

### Validating a Data Contract

```python
import yaml
import pandas as pd
import re
from dataclasses import dataclass

@dataclass
class ContractViolation:
    field: str
    rule: str
    message: str
    severity: str

def validate_against_contract(df: pd.DataFrame, contract_path: str) -> list[ContractViolation]:
    """Validate a DataFrame against a data contract YAML."""
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    violations = []
    model = list(contract["models"].values())[0]

    for field, rules in model["fields"].items():
        if field not in df.columns:
            if rules.get("required"):
                violations.append(ContractViolation(
                    field=field, rule="required_column",
                    message=f"Required column '{field}' is missing",
                    severity="error"
                ))
            continue

        series = df[field]

        # Required / not null
        if rules.get("required") and series.isna().any():
            null_count = series.isna().sum()
            violations.append(ContractViolation(
                field=field, rule="not_null",
                message=f"{null_count} nulls in required field '{field}'",
                severity="error"
            ))

        # Pattern validation
        if "pattern" in rules:
            non_null = series.dropna()
            pattern  = re.compile(rules["pattern"])
            invalid  = non_null[~non_null.astype(str).str.match(pattern)]
            if not invalid.empty:
                violations.append(ContractViolation(
                    field=field, rule="pattern",
                    message=f"{len(invalid)} values don't match pattern {rules['pattern']}",
                    severity="error"
                ))

        # Enum validation
        if "enum" in rules:
            invalid = series.dropna()[~series.dropna().isin(rules["enum"])]
            if not invalid.empty:
                violations.append(ContractViolation(
                    field=field, rule="enum",
                    message=f"Invalid values: {invalid.unique().tolist()}",
                    severity="error"
                ))

        # Range validation
        if "minimum" in rules:
            below_min = series.dropna()[series.dropna() < rules["minimum"]]
            if not below_min.empty:
                violations.append(ContractViolation(
                    field=field, rule="minimum",
                    message=f"{len(below_min)} values below minimum {rules['minimum']}",
                    severity="error"
                ))

    return violations
```

---

## Anomaly Detection in Data Pipelines

Statistical anomaly detection catches data quality issues that rule-based checks miss.

### Z-Score Based Detection

```python
import numpy as np
from scipy import stats

def detect_metric_anomalies(
    metric_history: list[float],
    new_value: float,
    z_threshold: float = 3.0
) -> dict:
    """
    Detect if a new metric value is anomalous compared to historical baseline.
    Uses Z-score: how many standard deviations from the mean.
    """
    mean   = np.mean(metric_history)
    std    = np.std(metric_history)
    z_score = (new_value - mean) / std if std > 0 else 0

    return {
        "value":     new_value,
        "mean":      mean,
        "std":       std,
        "z_score":   z_score,
        "is_anomaly": abs(z_score) > z_threshold,
        "direction": "high" if z_score > 0 else "low",
    }

# Example: Detect anomalous row counts
daily_row_counts = [95_000, 98_000, 102_000, 97_500, 99_200, 101_000]
today_count      = 45_000  # Suspicious drop

result = detect_metric_anomalies(daily_row_counts, today_count)
print(f"Z-score: {result['z_score']:.2f}, Anomaly: {result['is_anomaly']}")
# Z-score: -5.23, Anomaly: True
```

### Time-Series Anomaly Detection (Prophet)

```python
from prophet import Prophet
import pandas as pd

def detect_row_count_anomaly_prophet(
    history_df: pd.DataFrame,  # columns: ds (date), y (row_count)
    new_date: str,
    new_count: int
) -> dict:
    """
    Use Facebook Prophet to detect anomalous row counts.
    Accounts for weekly seasonality and trend.
    """
    model = Prophet(
        interval_width=0.95,  # 95% confidence interval
        weekly_seasonality=True,
        yearly_seasonality=True,
    )
    model.fit(history_df)

    future    = model.make_future_dataframe(periods=1)
    forecast  = model.predict(future)

    today_forecast = forecast[forecast["ds"] == new_date].iloc[0]
    yhat      = today_forecast["yhat"]
    yhat_lower = today_forecast["yhat_lower"]
    yhat_upper = today_forecast["yhat_upper"]

    is_anomaly = not (yhat_lower <= new_count <= yhat_upper)

    return {
        "date":         new_date,
        "actual":       new_count,
        "expected":     yhat,
        "lower_bound":  yhat_lower,
        "upper_bound":  yhat_upper,
        "is_anomaly":   is_anomaly,
        "pct_deviation": (new_count - yhat) / yhat * 100,
    }
```

---

## Great Expectations in Production

### Checkpoint-Based Validation

```python
import great_expectations as gx

def run_gx_checkpoint(
    data_source_name: str,
    expectation_suite_name: str,
    batch_request: dict,
    context_root: str = "/opt/gx"
) -> bool:
    """
    Run a GX checkpoint and return pass/fail.
    Results are stored to Data Docs automatically.
    """
    context = gx.get_context(context_root_dir=context_root)

    checkpoint_config = {
        "name": f"{data_source_name}_checkpoint",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [{
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
            {
                "name": "send_slack_notification_on_failure",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": "${SLACK_WEBHOOK_URL}",
                    "notify_on": "failure",
                },
            },
        ],
    }

    checkpoint = context.add_or_update_checkpoint(**checkpoint_config)
    result = checkpoint.run()
    return result.success
```

### GX with Airflow Integration

```python
from airflow.decorators import task

@task
def validate_orders_quality(**context):
    """Airflow task that runs GX validation."""
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    gx_context = gx.get_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="snowflake_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="orders",
        runtime_parameters={
            "query": f"""
                SELECT * FROM orders
                WHERE order_date = '{context['ds']}'
            """
        },
        batch_identifiers={"run_id": context["run_id"]},
    )

    results = gx_context.run_checkpoint(
        checkpoint_name="orders_daily_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": "orders.critical",
        }]
    )

    if not results.success:
        raise ValueError(f"Data quality validation failed for {context['ds']}")

    return {"quality_passed": True, "run_date": context["ds"]}
```

---

## dbt Test Severity and Custom Tests

```yaml
# models/schema.yml — advanced test configuration
models:
  - name: orders
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: created_at
          interval: 6
          config:
            severity: error    # Fail build if data older than 6 hours

    columns:
      - name: total_usd
        tests:
          - not_null:
              config:
                severity: warn   # Warn but don't fail
                warn_if: ">= 10"   # Only warn if >= 10 nulls
                error_if: ">= 100" # Error if >= 100 nulls
```

### Custom Macro-Based dbt Test

```sql
-- macros/test_column_sum_equals.sql
{% macro test_column_sum_equals(model, column_name, expected_sum, tolerance=0.01) %}

SELECT COUNT(*) AS violations
FROM (
    SELECT SUM({{ column_name }}) AS actual_sum
    FROM {{ model }}
) AS computed
WHERE ABS(actual_sum - {{ expected_sum }}) / {{ expected_sum }} > {{ tolerance }}

{% endmacro %}
```

---

## Quality Metrics Tracking Over Time

```python
import sqlalchemy as sa
from datetime import datetime

def record_quality_metrics(
    engine,
    pipeline_name: str,
    run_date: str,
    metrics: dict
):
    """
    Persist quality metrics for trending and alerting.
    Used to detect gradual degradation (not just point-in-time failures).
    """
    sql = """
        INSERT INTO data_quality_metrics (
            pipeline_name, run_date, metric_name, metric_value, recorded_at
        )
        VALUES (:pipeline, :run_date, :metric, :value, NOW())
    """
    with engine.begin() as conn:
        for metric_name, metric_value in metrics.items():
            conn.execute(sa.text(sql), {
                "pipeline": pipeline_name,
                "run_date": run_date,
                "metric":   metric_name,
                "value":    float(metric_value),
            })

# Query trending metrics
def get_metric_trend(engine, pipeline: str, metric: str, days: int = 30) -> pd.DataFrame:
    sql = """
        SELECT run_date, metric_value
        FROM data_quality_metrics
        WHERE pipeline_name = :pipeline
          AND metric_name    = :metric
          AND run_date >= CURRENT_DATE - :days
        ORDER BY run_date
    """
    return pd.read_sql(sa.text(sql), engine,
                       params={"pipeline": pipeline, "metric": metric, "days": days})
```

---

## Interview Tips

> **Tip 1:** Data contracts are a senior-level topic that signals organizational maturity. Mention that contracts create accountability between producers and consumers, and reduce "who broke what" debates when quality fails.

> **Tip 2:** Anomaly detection (Prophet, Z-score) catches issues that static thresholds miss — like a row count that drops 30% due to a slow upstream system. Static checks only catch absolute violations.

> **Tip 3:** GX checkpoints are the production-grade way to run validations. They integrate with alerting, store results historically, and generate Data Docs for stakeholder visibility.

> **Tip 4:** dbt test severities (`warn_if`, `error_if` thresholds) allow gradual quality gates. A first occurrence of 5 nulls in a low-priority column shouldn't block a deploy; 1,000 nulls should.

> **Tip 5:** Always track quality metrics over time, not just for current runs. A column that was 99.9% populated last month and is now 95% is a trend that needs investigation even if 95% passes your point-in-time threshold.
