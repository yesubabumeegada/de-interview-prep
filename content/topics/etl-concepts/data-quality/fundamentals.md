---
title: "Data Quality - Fundamentals"
topic: etl-concepts
subtopic: data-quality
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [etl, data-quality, great-expectations, dbt-tests, completeness, accuracy]
---

# Data Quality — Fundamentals

## The Four Dimensions of Data Quality

Data quality is measured across four primary dimensions:

| Dimension | Definition | Example Failure |
|---|---|---|
| **Completeness** | All expected data is present | 5% of order rows have NULL customer_id |
| **Accuracy** | Data correctly represents reality | Order total is $0 for non-zero orders |
| **Consistency** | Data agrees across systems | Order count in OLTP ≠ warehouse |
| **Timeliness** | Data is available when needed | Yesterday's data not loaded by 6 AM SLA |

---

## Why Data Quality Checks Matter

Without data quality checks in pipelines, bad data silently flows downstream and corrupts:
- Business KPIs (revenue figures off by millions)
- ML model training data (garbage in, garbage out)
- Financial reports (regulatory risk)
- Customer-facing products (wrong recommendations, billing errors)

A failed data quality check that stops a pipeline is **better** than silent corruption.

---

## dbt Tests — The First Line of Defense

dbt provides built-in and custom tests that run against models after each transformation.

### Built-in dbt Tests

```yaml
# models/schema.yml
version: 2

models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id

      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']

      - name: total_usd
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

### Custom dbt Test (SQL)

```sql
-- tests/assert_order_total_matches_line_items.sql
-- Fails if any order's total_usd differs from the sum of its line items
SELECT
    o.order_id,
    o.total_usd              AS order_total,
    SUM(li.price_usd * li.quantity) AS computed_total,
    ABS(o.total_usd - SUM(li.price_usd * li.quantity)) AS discrepancy
FROM {{ ref('orders') }} o
JOIN {{ ref('line_items') }} li USING (order_id)
GROUP BY o.order_id, o.total_usd
HAVING ABS(o.total_usd - SUM(li.price_usd * li.quantity)) > 0.01
```

Run tests: `dbt test --select orders`

---

## Great Expectations

Great Expectations (GX) is a Python library for defining, running, and documenting data expectations.

### Core Concepts

- **Expectation**: A declarative assertion about data (e.g., "column X is never null")
- **Expectation Suite**: A collection of expectations for a dataset
- **Checkpoint**: Runs an expectation suite against a data batch and reports results
- **Data Docs**: Auto-generated HTML documentation of expectations and results

### Basic Usage

```python
import great_expectations as gx
import pandas as pd

# Load data
df = pd.read_csv("orders.csv")

# Create a GX context
context = gx.get_context()

# Create a batch of data
datasource = context.sources.add_pandas("my_pandas_source")
asset      = datasource.add_dataframe_asset("orders_asset")
batch_req  = asset.build_batch_request(dataframe=df)
batch      = context.get_batch_list_from_batch_request(batch_req)[0]

# Run expectations
validator = context.get_validator(batch_request=batch_req)

validator.expect_column_to_exist("order_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("total_usd", min_value=0, max_value=100_000)
validator.expect_column_values_to_be_in_set(
    "status",
    value_set=["pending", "processing", "shipped", "delivered", "cancelled"]
)
validator.expect_table_row_count_to_be_between(min_value=1000, max_value=10_000_000)

# Save and run
validator.save_expectation_suite("orders_suite")
results = validator.validate()

print(f"Success: {results.success}")
print(f"Failed: {results.statistics['unsuccessful_expectations']}")
```

---

## SQL-Based Quality Checks

Many teams use pure SQL checks in pipelines without a dedicated framework.

```sql
-- Completeness check: NULL rate on critical columns
SELECT
    COUNT(*)                                          AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_pct
FROM orders
WHERE order_date = CURRENT_DATE - 1;

-- Accuracy check: negative totals
SELECT COUNT(*) AS negative_total_count
FROM orders
WHERE total_usd < 0;

-- Referential integrity check
SELECT COUNT(*) AS orphaned_orders
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Timeliness check: is today's data loaded?
SELECT MAX(order_date) AS latest_date,
       CURRENT_DATE - MAX(order_date) AS days_behind
FROM orders;
```

---

## Quality Gates in Pipelines

A **quality gate** stops the pipeline if checks fail, preventing bad data from propagating.

```python
from dataclasses import dataclass
from typing import Callable
import pandas as pd

@dataclass
class QualityCheck:
    name: str
    check_fn: Callable[[pd.DataFrame], bool]
    severity: str  # "error" = stop pipeline, "warn" = log and continue

def run_quality_gate(df: pd.DataFrame, checks: list[QualityCheck]) -> bool:
    """
    Run all quality checks. Stops pipeline on any 'error' severity failure.
    Returns True if pipeline should proceed.
    """
    all_pass = True
    for check in checks:
        passed = check.check_fn(df)
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {check.name}")

        if not passed and check.severity == "error":
            all_pass = False

    return all_pass

# Define checks
checks = [
    QualityCheck(
        name="order_id_not_null",
        check_fn=lambda df: df["order_id"].notna().all(),
        severity="error"
    ),
    QualityCheck(
        name="row_count_minimum",
        check_fn=lambda df: len(df) >= 100,
        severity="error"
    ),
    QualityCheck(
        name="total_usd_non_negative",
        check_fn=lambda df: (df["total_usd"] >= 0).all(),
        severity="warn"
    ),
]

df = extract_orders()
if not run_quality_gate(df, checks):
    raise RuntimeError("Quality gate failed. Pipeline halted.")
```

---

## Row Count Reconciliation

The simplest and most powerful data quality check: compare row counts between source and target.

```python
def reconcile_row_counts(
    source_engine,
    target_engine,
    table: str,
    date_col: str,
    check_date: str,
    tolerance_pct: float = 0.01  # 1% tolerance
) -> dict:
    """
    Compare row counts between source and target for a given date.
    Returns reconciliation result.
    """
    src_count = pd.read_sql(
        sa.text(f"SELECT COUNT(*) FROM {table} WHERE DATE({date_col}) = :d"),
        source_engine, params={"d": check_date}
    ).iloc[0, 0]

    tgt_count = pd.read_sql(
        sa.text(f"SELECT COUNT(*) FROM {table} WHERE DATE({date_col}) = :d"),
        target_engine, params={"d": check_date}
    ).iloc[0, 0]

    diff     = abs(src_count - tgt_count)
    diff_pct = diff / max(src_count, 1)

    result = {
        "date":        check_date,
        "source_count": src_count,
        "target_count": tgt_count,
        "diff":         diff,
        "diff_pct":     diff_pct,
        "passed":       diff_pct <= tolerance_pct,
    }
    return result
```

---

## Interview Tips

> **Tip 1:** Frame data quality in terms of business impact. "Our quality checks prevented $2M in incorrect revenue recognition" is more compelling than "we check for nulls."

> **Tip 2:** Know the four dimensions (completeness, accuracy, consistency, timeliness) and give examples from each. Interviewers often ask you to design a quality framework from scratch.

> **Tip 3:** Explain the difference between a **warning** (log and continue) and an **error** (stop the pipeline). Not all quality failures warrant halting — failing to load a small optional enrichment column shouldn't block the entire pipeline.

> **Tip 4:** dbt tests run as part of the transformation workflow, not as a separate system. GX is used when you need pre-load validation (before data enters the warehouse).

> **Tip 5:** Row count reconciliation is the first check any interviewer expects you to mention. It's simple, cheap, and catches most data pipeline failures instantly.
