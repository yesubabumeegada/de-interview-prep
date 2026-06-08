---
title: "Catalog & Governance — Real World"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [governance, catalog, production, pii, compliance]
---

# Catalog & Governance — Real World

## Pattern 1: Implementing Column Masking for PII

```python
# Unity Catalog column masking (Databricks SQL)

-- Step 1: Define masking function based on role
CREATE OR REPLACE FUNCTION prod_lakehouse.governance.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_member('pii_full_access') THEN email
    WHEN is_member('pii_masked_access') THEN REGEXP_REPLACE(email, '(.).*@', '$1***@')
    ELSE '***REDACTED***'
  END;

-- Step 2: Apply mask to column
ALTER TABLE prod_lakehouse.silver.customers
ALTER COLUMN email SET MASK prod_lakehouse.governance.mask_email;

-- Step 3: Test (run as a non-PII user)
SELECT customer_id, email FROM silver.customers LIMIT 5;
-- PII user:    john.doe@example.com
-- Masked user: j***@example.com
-- No access:   ***REDACTED***

-- For Snowflake equivalent (Dynamic Data Masking)
CREATE OR REPLACE MASKING POLICY email_mask AS (email STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII_ADMIN', 'ETL_SERVICE') THEN email
    WHEN CURRENT_ROLE() = 'ANALYST' THEN REGEXP_REPLACE(email, '(.).*@', '$1***@')
    ELSE '***'
  END;

ALTER TABLE silver.customers MODIFY COLUMN email 
  SET MASKING POLICY email_mask;
```

---

## Pattern 2: Automated Lineage with OpenLineage + DataHub

```python
# Airflow DAG with OpenLineage integration
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval="@daily", start_date=datetime(2024,1,1))
def orders_pipeline():
    
    @task(outlets=[Dataset("s3://bucket/bronze/orders")])
    def ingest_orders():
        # OpenLineage emits: job=ingest_orders, output=bronze.orders
        spark.sql("""
            INSERT INTO bronze.orders
            SELECT * FROM fivetran_raw.orders WHERE date = '{{ ds }}'
        """)
    
    @task(
        inlets=[Dataset("s3://bucket/bronze/orders")],
        outlets=[Dataset("s3://bucket/silver/orders")]
    )
    def transform_silver():
        # OpenLineage emits: job=transform_silver,
        #   input=bronze.orders, output=silver.orders
        # Column lineage: silver.amount ← bronze.order_total
        spark.sql("""
            MERGE INTO silver.orders USING bronze.orders AS src
            ON silver.orders.order_id = src.order_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    @task(
        inlets=[Dataset("s3://bucket/silver/orders")],
        outlets=[Dataset("s3://bucket/gold/daily_revenue")]
    )
    def aggregate_gold():
        spark.sql("""
            INSERT OVERWRITE gold.daily_revenue
            SELECT order_date, SUM(amount) AS revenue
            FROM silver.orders
            GROUP BY order_date
        """)
    
    ingest_orders() >> transform_silver() >> aggregate_gold()

# DataHub receives lineage events automatically
# Lineage graph in DataHub:
#   fivetran.orders → bronze.orders → silver.orders → gold.daily_revenue
# Column lineage:
#   fivetran.order_total → bronze.order_total → silver.amount → gold.revenue
```

---

## Pattern 3: Data Quality Gate in the Governance Framework

```python
# Great Expectations as governance gate
# Runs before data is promoted from Bronze → Silver
from great_expectations.data_context import DataContext

def validate_bronze_quality(table_path: str) -> bool:
    """Return True if data passes quality checks, False if blocked."""
    context = DataContext()
    batch_request = {
        "datasource_name": "spark_datasource",
        "data_connector_name": "delta_data_connector",
        "data_asset_name": table_path,
    }
    
    checkpoint_result = context.run_checkpoint(
        checkpoint_name="bronze_orders_checkpoint",
        batch_request=batch_request
    )
    
    if not checkpoint_result.success:
        # Log failure details to governance audit table
        failed_expectations = [
            r for r in checkpoint_result.run_results.values()
            if not r["validation_result"]["success"]
        ]
        
        spark.createDataFrame([{
            "table": table_path,
            "run_time": datetime.now().isoformat(),
            "checks_failed": len(failed_expectations),
            "blocked": True,
            "details": str(failed_expectations[:5]),  # top 5 failures
        }]).write.format("delta").mode("append").save("s3://bucket/governance/quality_gate_log")
        
        raise ValueError(f"Quality gate FAILED for {table_path}: {len(failed_expectations)} checks failed")
    
    return True

# Airflow task: validate BEFORE Silver promotion
validate_task = PythonOperator(
    task_id="validate_bronze_quality",
    python_callable=validate_bronze_quality,
    op_args=["s3://bucket/bronze/orders"],
)

# Pipeline: ingest → [quality gate] → transform_silver
ingest_task >> validate_task >> transform_silver_task
```

---

## Interview Tips

> **Tip 1:** "What's the first governance project you'd tackle for a 5-person data team?" — Start with PII classification (highest regulatory risk, smallest team can handle). Scan all tables with an automated PII detector (regex patterns on sample data). Tag PII columns in Glue/catalog. Add column masking for the 3-5 most sensitive columns (email, SSN, credit card). This can be done in 1-2 weeks and directly addresses GDPR/CCPA exposure. Don't start with a full catalog implementation — the ROI is lower urgency and it takes months.

> **Tip 2:** "How do you handle schema changes in production that would break downstream consumers?" — Data contracts + versioning. Before merging a breaking schema change: check the lineage graph for all downstream consumers. Notify owners (Slack + Jira). Give a 14-day deprecation window. Add the new column alongside the old one (additive first). After all consumers migrated, drop the old column. Gate breaking changes behind a PR review that requires downstream team approvals. Automated: dbt schema tests catch breaking changes in CI before they hit production.

> **Tip 3:** "How do you audit who accessed sensitive data?" — Audit logging at multiple layers: (1) Unity Catalog system tables: `SELECT * FROM system.access.audit WHERE action_name='dataRead' AND resource LIKE '%customers%'` — shows every query on sensitive tables; (2) Databricks cluster logs or AWS CloudTrail for Athena: every query is logged; (3) S3 access logs: every S3 GET on raw PII files. For compliance: retain audit logs for 7 years (S3 Glacier with Object Lock), export to SIEM (Splunk/Datadog) for anomaly detection.
