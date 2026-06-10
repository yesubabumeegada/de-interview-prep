---
title: "Great Expectations — Senior Deep Dive"
topic: data-quality
subtopic: great-expectations
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [great-expectations, gx-cloud, integration, scale, architecture]
---

# Great Expectations — Senior Deep Dive

## GX at Scale: Architecture Patterns

### Pattern 1: Centralized GX Context with S3 Backend

```python
# gx/great_expectations.yml — store metadata in S3
data_context_config = DataContextConfig(
    store_backend_defaults=S3StoreBackendDefaults(
        default_bucket_name="my-gx-store",
        default_prefix="great_expectations/",
    ),
    data_docs_sites={
        "s3_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "my-gx-docs",
                "prefix": "data_docs/",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    },
)

context = BaseDataContext(project_config=data_context_config)
```

### Pattern 2: Multi-Suite Per Table

```python
# Different suites for different environments / use cases
suites = {
    "ingestion":    "orders_ingestion_suite",    # raw schema checks
    "silver":       "orders_silver_suite",        # business rules
    "gold":         "orders_gold_suite",          # aggregate accuracy
    "regression":   "orders_regression_suite",    # model training data
}

def run_suite_for_layer(layer: str, df):
    context = gx.get_context()
    # ... lookup suite and run checkpoint
    result = context.checkpoints.get(f"orders_{layer}_checkpoint").run(
        batch_parameters={"dataframe": df}
    )
    return result
```

---

## GX + dbt Integration

dbt generates metadata that GX can consume to auto-create expectations:

```python
import json
import great_expectations as gx

def generate_suite_from_dbt_manifest(manifest_path: str, model_name: str):
    """Read dbt manifest and create GX expectations from column tests."""
    
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    context = gx.get_context()
    suite = context.suites.add(gx.ExpectationSuite(name=f"{model_name}_dbt_suite"))
    
    # Find the model node
    node_key = f"model.my_project.{model_name}"
    node = manifest["nodes"].get(node_key, {})
    
    # Generate expectations from column tests
    for col_name, col_meta in node.get("columns", {}).items():
        for test in col_meta.get("data_tests", []):
            if test == "not_null":
                suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=col_name
                ))
            elif test == "unique":
                suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
                    column=col_name
                ))
            elif isinstance(test, dict) and "accepted_values" in test:
                suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                    column=col_name,
                    value_set=test["accepted_values"]["values"]
                ))
    
    context.suites.update(suite)
    return suite
```

---

## Expectation Parameterization — Dynamic Thresholds

Avoid hardcoding thresholds by computing them from historical data:

```python
import pandas as pd
import great_expectations as gx

def build_dynamic_suite(
    historical_df: pd.DataFrame,
    current_df: pd.DataFrame,
    suite_name: str,
    z_threshold: float = 3.0,
) -> gx.ExpectationSuite:
    """
    Build a suite where row count and metric thresholds are derived
    from historical baseline (mean ± z_threshold * std).
    """
    import numpy as np
    
    context = gx.get_context()
    suite = gx.ExpectationSuite(name=suite_name)
    
    # Row count baseline
    hist_counts = historical_df.groupby("batch_date").size()
    mean_count = hist_counts.mean()
    std_count = hist_counts.std()
    
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=max(0, int(mean_count - z_threshold * std_count)),
        max_value=int(mean_count + z_threshold * std_count),
    ))
    
    # Revenue baseline
    hist_revenue = historical_df.groupby("batch_date")["amount"].sum()
    mean_rev = hist_revenue.mean()
    std_rev = hist_revenue.std()
    
    suite.add_expectation(gx.expectations.ExpectColumnSumToBeBetween(
        column="amount",
        min_value=max(0, mean_rev - z_threshold * std_rev),
        max_value=mean_rev + z_threshold * std_rev,
    ))
    
    return context.suites.add(suite)
```

---

## GX Cloud — Enterprise Deployment

GX Cloud is the managed SaaS version with:
- Centralized expectation management (no YAML files)
- Web UI for business users to review DQ
- Built-in scheduling and alerting
- Role-based access (data stewards vs engineers)

```python
import great_expectations as gx

# Connect to GX Cloud
context = gx.get_context(
    mode="cloud",
    cloud_base_url="https://api.greatexpectations.io",
    cloud_organization_id="your-org-id",
    cloud_access_token="your-token",
)

# Everything else is the same API
datasource = context.sources.add_pandas("prod_source")
# ...
```

---

## Performance at Scale

| Challenge | Solution |
|---|---|
| Validating 1B-row Spark tables | Use Spark backend, push-down expectations to Spark SQL |
| 500 tables to validate daily | Parallelize with Airflow dynamic task mapping |
| Suite management at scale | Generate suites programmatically from schema registry |
| Slow Data Docs build | Build docs asynchronously after validation, not blocking |
| Test flakiness from `mostly` | Set environment-specific `mostly` thresholds (dev: 0.8, prod: 0.99) |

---

## Interview Tips

> **Tip 1:** "How do you prevent GX from becoming a maintenance burden?" — Auto-generate suites from schema registry / dbt manifest. Use profiling for initial suite generation. Keep suites in Git with PR reviews. Don't hand-write 1000 JSON expectations.

> **Tip 2:** "What are the limitations of GX?" — (1) Performance on very large datasets without Spark backend. (2) Custom expectations require significant boilerplate. (3) Data Docs can be slow to build for many suites. (4) GX Cloud adds cost. For simple checks, dbt tests + custom SQL may be lighter.

> **Tip 3:** "When would you NOT use GX?" — For pure SQL/dbt pipelines, dbt's built-in tests (not_null, unique, accepted_values, relationships) + dbt-expectations package cover 90% of use cases without adding a Python dependency. Use GX when you need Python-based validation, cross-source checks, or rich HTML reporting.

## ⚡ Cheat Sheet

**Great Expectations core objects**
```python
import great_expectations as gx
context = gx.get_context()

# Expectation suite
suite = context.add_expectation_suite("orders_suite")
validator = context.get_validator(batch_request=batch_req, expectation_suite_name="orders_suite")

# Common expectations
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_between("amount", 0, 100000)
validator.expect_column_pair_values_a_to_be_greater_than_b("ship_date", "order_date")
validator.expect_column_values_to_match_regex("email", r"^[\w._%+-]+@[\w.-]+\.[a-z]{2,}$")

# Run checkpoint
result = context.run_checkpoint("orders_checkpoint")
assert result["success"], f"DQ failure: {result}"
```

**Anomaly detection patterns**
```python
# Z-score for numeric columns
def zscore_anomaly(series, threshold=3.0):
    z = (series - series.mean()) / series.std()
    return z.abs() > threshold

# Rolling mean comparison (for time series)
df["rolling_avg"] = df["revenue"].rolling(7).mean()
df["anomaly"] = abs(df["revenue"] - df["rolling_avg"]) > 2 * df["revenue"].rolling(7).std()
```

**Data contract (dbt schema.yml)**
```yaml
models:
  - name: orders
    description: "Gold orders table — SLA: updated within 1 hour of source"
    config: {contract: {enforced: true}}
    columns:
      - name: order_id
        data_type: bigint
        constraints: [{type: not_null}, {type: unique}]
      - name: amount
        data_type: double
        constraints: [{type: not_null}]
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: updated_at
          interval: 2
```

**SLA monitoring**
```sql
-- Alert if table hasn't been updated within SLA window
SELECT table_name,
       MAX(updated_at) AS last_updated,
       DATEDIFF('hour', MAX(updated_at), NOW()) AS hours_since_update,
       CASE WHEN DATEDIFF('hour', MAX(updated_at), NOW()) > sla_hours THEN 'BREACHED' ELSE 'OK' END AS status
FROM table_sla_registry
JOIN gold_tables USING (table_name)
GROUP BY table_name, sla_hours;
```

**DQ dimensions**
```
Completeness:  % non-null values
Accuracy:      matches source of truth
Consistency:   same value across systems
Timeliness:    data arrives within SLA
Uniqueness:    no duplicates on PK
Validity:      conforms to expected format/range
```

**Incident response flow**
```
1. Alert fires (DQ check fails, SLA breached)
2. Triage: severity — who's impacted? (BI dashboard, ML model, external SLA?)
3. Notify: page on-call DE + inform data consumers
4. Contain: quarantine bad data (move to _quarantine schema; don't serve bad data)
5. Fix: patch pipeline or source data
6. Backfill: reprocess affected time range
7. Post-mortem: root cause + prevention (add check that would have caught this earlier)
```
