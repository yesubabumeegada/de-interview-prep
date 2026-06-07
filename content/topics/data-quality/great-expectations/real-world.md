---
title: "Great Expectations — Real World"
topic: data-quality
subtopic: great-expectations
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [great-expectations, airflow, production, pipeline, integration]
---

# Great Expectations — Real World Patterns

## Pattern: GX Validation in an Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import great_expectations as gx
import pandas as pd

def validate_orders_with_gx(**context):
    ds = context["ds"]
    
    # Load data
    df = pd.read_parquet(f"s3://data-lake/raw/orders/dt={ds}/data.parquet")
    
    # Get GX context (S3-backed)
    gx_context = gx.get_context()
    
    # Get batch
    datasource = gx_context.sources.get("orders_source")
    asset = datasource.get_asset("orders")
    batch_def = asset.get_batch_definition("full_load")
    
    # Run checkpoint
    checkpoint = gx_context.checkpoints.get("orders_daily_checkpoint")
    result = checkpoint.run(batch_parameters={"dataframe": df})
    
    if not result.success:
        failed = [
            er.expectation_config.expectation_type
            for er in result.run_results.values()
            for er in er["validation_result"].results
            if not er.success
        ]
        raise ValueError(f"GX validation failed for {ds}: {failed}")
    
    print(f"GX validation passed for {ds}: {result.statistics}")


with DAG("orders_with_gx", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    validate = PythonOperator(
        task_id="gx_validate_orders",
        python_callable=validate_orders_with_gx,
    )
```

---

## Pattern: Incremental Validation with Partitioned Data

```python
import great_expectations as gx
from datetime import date, timedelta
import pandas as pd

def validate_partition(run_date: date):
    """Validate a single day's partition."""
    
    context = gx.get_context()
    df = pd.read_parquet(
        f"s3://bucket/orders/dt={run_date.isoformat()}/",
        storage_options={"anon": False},
    )
    
    # Partition-specific expectations
    asset = context.sources.get("orders_s3").get_asset("daily_orders")
    batch_def = asset.get_batch_definition("partition")
    
    # Additional runtime expectations based on day-of-week
    # Monday should have more orders than Sunday
    if run_date.weekday() == 0:  # Monday
        expected_min = 50_000
    else:
        expected_min = 20_000
    
    suite = context.suites.get("orders_daily_suite")
    # Temporarily add runtime expectation
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=expected_min, max_value=500_000
        )
    )
    
    result = context.checkpoints.get("daily_checkpoint").run(
        batch_parameters={"dataframe": df}
    )
    return result.success


# Backfill validation for last 7 days
for i in range(7):
    run_date = date.today() - timedelta(days=i)
    success = validate_partition(run_date)
    print(f"{run_date}: {'PASS' if success else 'FAIL'}")
```

---

## Pattern: Validation Results Stored to Delta Lake

```python
import great_expectations as gx
import pandas as pd
from datetime import datetime
from delta import DeltaTable

def run_and_store_gx_results(df: pd.DataFrame, table_name: str, run_id: str):
    context = gx.get_context()
    checkpoint = context.checkpoints.get(f"{table_name}_checkpoint")
    result = checkpoint.run(batch_parameters={"dataframe": df})
    
    # Extract results into a flat DataFrame
    records = []
    for vr in result.run_results.values():
        for er in vr["validation_result"].results:
            records.append({
                "run_id": run_id,
                "table_name": table_name,
                "evaluated_at": datetime.utcnow().isoformat(),
                "expectation_type": er.expectation_config.expectation_type,
                "column": er.expectation_config.kwargs.get("column", "_table_"),
                "success": er.success,
                "result_json": str(er.result),
            })
    
    results_df = pd.DataFrame(records)
    
    # Append to DQ metrics Delta table
    spark_df = spark.createDataFrame(results_df)
    spark_df.write.format("delta").mode("append").save("s3://bucket/dq_metrics/gx_results/")
    
    return result.success
```

---

## Common GX Gotchas

| Gotcha | Fix |
|---|---|
| `mostly=1.0` is default — any failure = suite fail | Use `mostly=0.99` for non-critical columns |
| Expectation suite JSON conflicts in multi-dev environments | Store suites in S3 or GX Cloud, not local files |
| Data Docs build is slow with 1000+ expectations | Build async, or use GX Cloud which handles this |
| Spark backend requires gx[spark] extra | `pip install great-expectations[spark]` |
| Schema drift breaks `match_ordered_list` | Use `match_set_of_columns` for more flexibility |
| Large DataFrames loaded fully in Pandas | Use Spark or sample for validation when >10M rows |

---

## Production Checklist

```
☐ Suites stored in version control (S3/Git)
☐ Checkpoints configured with Slack/PagerDuty actions
☐ Data Docs published to S3 static site
☐ GX results written to metrics table for trending
☐ `mostly` thresholds set per environment (lower in dev)
☐ Custom expectations unit tested
☐ Validation runs on every pipeline execution, not just manually
☐ Profiling used to bootstrap new suites, then manually reviewed
```
