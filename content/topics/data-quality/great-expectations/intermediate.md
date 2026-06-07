---
title: "Great Expectations — Intermediate"
topic: data-quality
subtopic: great-expectations
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [great-expectations, checkpoint, spark, profiling, custom-expectations]
---

# Great Expectations — Intermediate

## Profiling — Auto-Generate Expectations

GX can profile your data and suggest expectations automatically:

```python
import great_expectations as gx
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
import pandas as pd

df = pd.read_parquet("orders.parquet")

context = gx.get_context()
datasource = context.sources.add_pandas("orders_source")
asset = datasource.add_dataframe_asset("orders")
batch_def = asset.add_batch_definition_whole_dataframe("full")

suite, validation_result = context.assistants.onboarding.run(
    batch_request=batch_def.build_batch_request({"dataframe": df}),
    expectation_suite_name="orders_auto_suite",
)

# Review and prune auto-generated expectations
print(f"Generated {len(suite.expectations)} expectations")
context.build_data_docs()
```

---

## Custom Expectations

When built-in expectations aren't enough, write your own:

```python
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics import column_aggregate_metric_provider
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from typing import Optional, Dict

class ExpectColumnValuesToBeValidEmail(ColumnExpectation):
    """Custom expectation: column must contain valid email addresses."""
    
    examples = [
        {
            "data": {"email": ["user@example.com", "bad-email", None]},
            "tests": [
                {
                    "title": "mostly valid emails",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "email", "mostly": 0.5},
                    "out": {"success": True},
                }
            ],
        }
    ]
    
    metric_dependencies = ("column_values.not_null",)
    success_keys = ("mostly",)
    
    EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine=None,
    ):
        import re
        import pandas as pd
        
        column = configuration.kwargs["column"]
        mostly = configuration.kwargs.get("mostly", 1.0)
        
        series = metrics.get("table.head")[column]
        valid = series.dropna().apply(lambda x: bool(re.match(self.EMAIL_PATTERN, str(x))))
        pass_rate = valid.mean() if len(valid) > 0 else 1.0
        
        return {
            "success": pass_rate >= mostly,
            "result": {
                "pass_rate": pass_rate,
                "failing_rows": int((~valid).sum()),
            }
        }
```

---

## Connecting GX to SQL Databases

```python
import great_expectations as gx

context = gx.get_context()

# Add PostgreSQL datasource
datasource = context.sources.add_postgres(
    name="prod_postgres",
    connection_string="postgresql+psycopg2://user:pass@host:5432/dbname",
)

# Validate a full table
asset = datasource.add_table_asset("orders", table_name="orders", schema_name="public")
batch_def = asset.add_batch_definition_whole_table("full_table")
batch = batch_def.get_batch()

suite = context.suites.add(gx.ExpectationSuite(name="orders_sql_suite"))
validator = context.get_validator(batch=batch, expectation_suite_name="orders_sql_suite")

validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_between("amount", min_value=0)
validator.expect_table_row_count_to_be_between(min_value=10000)
validator.save_expectation_suite()

result = validator.validate()
```

---

## GX with Apache Spark

```python
import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GX_Spark").getOrCreate()
df = spark.read.parquet("s3://bucket/orders/")

context = gx.get_context()

datasource = context.sources.add_spark("spark_source")
asset = datasource.add_dataframe_asset("orders_spark")
batch_def = asset.add_batch_definition_whole_dataframe("full")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.add(gx.ExpectationSuite(name="orders_spark_suite"))
validator = context.get_validator(batch=batch, expectation_suite_name="orders_spark_suite")

validator.expect_column_values_to_not_be_null("order_id")
validator.expect_table_row_count_to_be_between(min_value=1_000_000)
validator.save_expectation_suite()

result = validator.validate()
print(f"Spark DQ passed: {result.success}")
```

---

## Checkpoint Actions — Alerting on Failure

```python
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

context = gx.get_context()

# Checkpoint with multiple actions
checkpoint = context.checkpoints.add(
    Checkpoint(
        name="orders_checkpoint_with_alerts",
        validations=[
            {
                "batch_definition": batch_def,
                "expectation_suite_name": "orders_suite",
            }
        ],
        action_list=[
            # Update Data Docs on every run
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
            # Slack notification on failure
            {
                "name": "send_slack_notification",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": "https://hooks.slack.com/services/xxx/yyy/zzz",
                    "notify_on": "failure",
                    "renderer": {"class_name": "SlackRenderer"},
                },
            },
        ],
    )
)

result = checkpoint.run(batch_parameters={"dataframe": df})
```

---

## Expectation Suites as Code — Version Control

Store suites as JSON in Git:

```json
// gx/expectations/orders_suite.json
{
  "expectation_suite_name": "orders_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "order_id" },
      "meta": { "owner": "data-engineering", "added": "2024-01-15" }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": { "column": "amount", "min_value": 0.01, "max_value": 100000 },
      "meta": { "owner": "finance", "ticket": "DE-1234" }
    }
  ],
  "meta": {
    "great_expectations_version": "0.18.0"
  }
}
```

---

## Interview Tips

> **Tip 1:** "How do you use GX with Airflow?" — Create a GX checkpoint and call it from a PythonOperator. On failure, raise an `AirflowException` to fail the task and trigger retry/alert logic.

> **Tip 2:** "How do you handle schema drift?" — Use `expect_table_columns_to_match_set` (order-insensitive) rather than `match_ordered_list`. Set it to warning severity so new columns don't fail the pipeline but do get flagged.

> **Tip 3:** "What's the difference between a Suite and a Checkpoint?" — A Suite is the definition of expectations. A Checkpoint is the execution configuration: it connects a data source, a suite, and a set of post-validation actions. Think of Suite as the test cases, Checkpoint as the test runner.
