---
title: "Data Quality - Real World"
topic: etl-concepts
subtopic: data-quality
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, data-quality, production, case-study, great-expectations, dbt]
---

# Data Quality — Real World

## Case Study 1: Revenue Reporting Corruption

### Problem

A SaaS company discovered that their monthly revenue reports had been overstating revenue by ~$1.2M for 3 months. The root cause was duplicate order records caused by a retry bug in their order service — the same order was inserted twice with different `order_id` values but identical business keys.

### Detection (After the Fact)

```sql
-- Query that would have caught duplicates during pipeline
SELECT
    user_id,
    product_id,
    purchase_timestamp,
    amount_usd,
    COUNT(*) AS duplicate_count
FROM orders
WHERE purchase_timestamp >= CURRENT_DATE - 90
GROUP BY user_id, product_id, purchase_timestamp, amount_usd
HAVING COUNT(*) > 1
ORDER BY amount_usd DESC;
```

### Prevention: Business-Key Dedup Check in Pipeline

```python
def check_business_key_uniqueness(df: pd.DataFrame) -> dict:
    """
    Check for duplicates on business keys (not surrogate keys).
    Returns check result with sample duplicates for investigation.
    """
    biz_keys = ["user_id", "product_id", "purchase_timestamp", "amount_usd"]

    # Only check columns that exist
    available_keys = [k for k in biz_keys if k in df.columns]

    dupes = df[df.duplicated(subset=available_keys, keep=False)]

    result = {
        "total_rows":       len(df),
        "duplicate_rows":   len(dupes),
        "duplicate_pct":    len(dupes) / len(df) * 100 if len(df) > 0 else 0,
        "passed":           len(dupes) == 0,
        "sample_duplicates": dupes.head(5).to_dict("records") if not dupes.empty else [],
    }
    return result

# Add to dbt schema.yml
dbt_test = """
- name: orders
  tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns:
          - user_id
          - product_id
          - purchase_timestamp
          - amount_usd
"""
```

### Revenue Impact Calculation

```sql
-- Identify the overstated revenue after discovering duplicates
WITH deduped AS (
    SELECT
        user_id, product_id, purchase_timestamp, amount_usd,
        MIN(order_id) AS canonical_order_id   -- Keep first (oldest) order_id
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY user_id, product_id, purchase_timestamp, amount_usd
),
reported AS (
    SELECT SUM(amount_usd) AS reported_revenue FROM orders WHERE order_date >= '2024-01-01'
),
actual AS (
    SELECT SUM(amount_usd) AS actual_revenue FROM deduped
)
SELECT
    reported_revenue,
    actual_revenue,
    reported_revenue - actual_revenue AS overstatement
FROM reported, actual;
```

---

## Case Study 2: dbt Quality Framework at Scale

### Problem

A data platform team managed 200+ dbt models across 15 business domains. Quality failures in one domain would silently cascade to others. There was no visibility into quality trends — only "it's broken today."

### Solution: Centralized Quality Metrics + Alerting

```python
# scripts/collect_dbt_test_results.py
import json
import subprocess
from datetime import datetime
import sqlalchemy as sa

def collect_dbt_test_results(project_dir: str, target: str = "prod") -> list[dict]:
    """
    Run dbt test and collect structured results for trend tracking.
    """
    result = subprocess.run(
        ["dbt", "test", "--target", target, "--output", "json"],
        cwd=project_dir,
        capture_output=True,
        text=True
    )

    # Parse dbt JSON output
    results = []
    for line in result.stdout.splitlines():
        try:
            r = json.loads(line)
            if r.get("type") == "test_result":
                results.append({
                    "test_name":   r["data"]["node"]["name"],
                    "model":       r["data"]["node"]["depends_on"]["nodes"][0].split(".")[-1],
                    "status":      r["data"]["status"],
                    "failures":    r["data"].get("failures", 0),
                    "run_at":      datetime.utcnow().isoformat(),
                })
        except (json.JSONDecodeError, KeyError):
            pass

    return results

def save_test_results(results: list[dict], engine):
    """Persist test results for trending."""
    df = pd.DataFrame(results)
    df.to_sql("dbt_test_results", engine, if_exists="append", index=False)
```

### Quality Dashboard SQL

```sql
-- Weekly quality trend by domain
SELECT
    DATE_TRUNC('week', run_at::date) AS week,
    SPLIT_PART(model, '_', 1)        AS domain,
    COUNT(*)                          AS total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pass_rate_pct
FROM dbt_test_results
WHERE run_at >= CURRENT_DATE - 90
GROUP BY 1, 2
ORDER BY 1 DESC, 6 ASC;
```

---

## Case Study 3: Great Expectations in a Financial Pipeline

### Setup

A fintech company processes wire transfers. Any data quality issue in the pipeline could result in incorrect transfers — a regulatory and financial risk.

```python
# great_expectations/checkpoints/wire_transfers_checkpoint.yml
# (auto-generated by GX, shown here for reference)

name: wire_transfers_daily
config_version: 1.0
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: snowflake_raw
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: wire_transfers
      data_connector_query:
        index: -1   # Latest batch
    expectation_suite_name: wire_transfers.critical
  
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
  - name: pagerduty_on_failure
    action:
      class_name: PagerdutyAlertAction
      api_key: ${PAGERDUTY_API_KEY}
      routing_key: ${PAGERDUTY_ROUTING_KEY}
      notify_on: failure
```

```python
# Define the expectation suite programmatically
def build_wire_transfer_expectations(validator):
    """Critical expectations for wire transfer data quality."""

    # Completeness
    validator.expect_column_values_to_not_be_null("transfer_id")
    validator.expect_column_values_to_not_be_null("sender_account_id")
    validator.expect_column_values_to_not_be_null("receiver_account_id")
    validator.expect_column_values_to_not_be_null("amount_usd")
    validator.expect_column_values_to_not_be_null("transfer_timestamp")

    # Uniqueness
    validator.expect_column_values_to_be_unique("transfer_id")

    # Accuracy
    validator.expect_column_values_to_be_between("amount_usd", min_value=0.01, max_value=10_000_000)

    # Format validation
    validator.expect_column_values_to_match_regex(
        "transfer_id",
        r"^TRF-[0-9]{16}$"
    )

    # Business rules
    validator.expect_column_pair_values_to_not_be_equal(
        "sender_account_id",
        "receiver_account_id"  # Can't transfer to yourself
    )

    # Row count (should match upstream count from payment processor API)
    validator.expect_table_row_count_to_be_between(
        min_value=int(get_expected_count() * 0.95),
        max_value=int(get_expected_count() * 1.05),
    )

    validator.save_expectation_suite("wire_transfers.critical")
```

---

## Incident Response Playbook

### When a Quality Check Fails in Production

```python
class QualityIncidentHandler:
    def __init__(self, slack_client, pagerduty_client, lineage_graph):
        self.slack   = slack_client
        self.pager   = pagerduty_client
        self.lineage = lineage_graph

    def handle_failure(self, check_result: dict):
        table    = check_result["table"]
        check    = check_result["check_name"]
        severity = check_result["severity"]

        # 1. Determine downstream impact
        impact = self.lineage.impact_analysis(table)

        # 2. Route alert based on severity and impact
        message = self._format_alert(check_result, impact)

        if severity == "critical" or impact["critical_affected"]:
            self.pager.create_incident(
                title=f"Critical data quality failure: {table}.{check}",
                body=message,
                severity="critical"
            )
        else:
            self.slack.send_message(channel="#data-quality-alerts", text=message)

        # 3. Halt downstream pipelines to prevent corruption
        if severity == "error":
            self._halt_downstream(impact["downstream_tables"])

    def _format_alert(self, result: dict, impact: dict) -> str:
        return f"""
*Data Quality Alert*
Table: `{result['table']}`
Check: `{result['check_name']}`
Severity: {result['severity']}
Details: {result.get('message', 'No details')}

*Impact Analysis*
Downstream tables affected: {impact['affected_tables']}
Critical affected: {', '.join(impact['critical_affected']) or 'None'}

*Next Steps*
1. Check pipeline logs: /airflow/dag/{result['table']}_pipeline
2. Review data: /data-docs/{result['table']}
3. Runbook: /wiki/quality-runbook#{result['check_name']}
        """.strip()

    def _halt_downstream(self, tables: list[str]):
        """Pause Airflow DAGs for affected tables."""
        for table in tables:
            dag_id = f"{table}_pipeline"
            # airflow_client.pause_dag(dag_id)
            print(f"Paused DAG: {dag_id}")
```

---

## Interview Tips

> **Tip 1:** Frame quality incidents in terms of business impact. "A duplicate detection check would have caught the $1.2M overstatement in the first weekly run" is a compelling argument for investing in quality frameworks.

> **Tip 2:** The combination of dbt tests (transformation quality) + GX checkpoints (ingestion quality) + observability monitoring (continuous trend analysis) is the full production quality stack. Know all three layers.

> **Tip 3:** Always link quality failures to lineage. "This check failed in raw.orders, which affects 8 downstream models including the executive dashboard" demonstrates operational maturity.

> **Tip 4:** PagerDuty integration for critical quality failures shows you treat data quality as an operational SLA, not just a nice-to-have. Interviewers from mature data organizations appreciate this.

> **Tip 5:** Business-key uniqueness (not just surrogate key uniqueness) is the most commonly missed quality check. Always ask: "what makes two rows the same business entity?" and check for duplicates on those columns.
