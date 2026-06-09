---
title: "Great Expectations — Scenarios"
topic: data-quality
subtopic: great-expectations
content_type: scenario_question
tags: [great-expectations, interview, scenarios]
---

# Great Expectations — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: First GX Suite

**Scenario:** A new `customers` table is being ingested daily. Set up basic GX validation.

<details>
<summary>💡 Hint</summary>

import great_expectations as gx

</details>

<details>
<summary>✅ Solution</summary>

```python
import great_expectations as gx
import pandas as pd

context = gx.get_context()
df = pd.read_parquet("customers.parquet")

datasource = context.sources.add_pandas("customers_source")
asset = datasource.add_dataframe_asset("customers")
batch_def = asset.add_batch_definition_whole_dataframe("full")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.add(gx.ExpectationSuite(name="customers_suite"))
validator = context.get_validator(batch=batch, expectation_suite_name="customers_suite")

# Must-have: PK not null, unique
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_unique("customer_id")

# Business rules
validator.expect_column_values_to_not_be_null("email", mostly=0.95)
validator.expect_column_values_to_match_regex(
    "email", r"^[^@]+@[^@]+\.[^@]+$", mostly=0.95
)
validator.expect_column_values_to_be_in_set("status", ["active", "inactive", "pending"])
validator.expect_column_values_to_be_between("age", min_value=0, max_value=120, mostly=0.99)
validator.expect_table_row_count_to_be_between(min_value=1000)

validator.save_expectation_suite()
result = validator.validate()
print(f"Passed: {result.success}")
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: GX Failing on New Data

**Scenario:** Your GX checkpoint passes in dev but fails in production with "column `region_code` was not found." How do you debug?

<details>
<summary>💡 Hint</summary>

**Root cause possibilities:** 1. A new column was added to the suite that doesn't exist in prod source 2. The prod source has different schema (table was modified) 3. Suite was updated and deployed before the schema migration ran

</details>

<details>
<summary>✅ Solution</summary>

**Root cause possibilities:**
1. A new column was added to the suite that doesn't exist in prod source
2. The prod source has different schema (table was modified)
3. Suite was updated and deployed before the schema migration ran

**Debugging steps:**
```python
# 1. Inspect what columns the suite expects
context = gx.get_context()
suite = context.suites.get("orders_suite")
for exp in suite.expectations:
    print(exp.expectation_type, exp.kwargs.get("column"))

# 2. Inspect what columns the data actually has
import pandas as pd
prod_df = pd.read_parquet("s3://prod/orders/latest.parquet")
print(prod_df.columns.tolist())

# 3. Find the diff
suite_columns = {e.kwargs.get("column") for e in suite.expectations if "column" in e.kwargs}
data_columns = set(prod_df.columns)
missing_in_data = suite_columns - data_columns
extra_in_data = data_columns - suite_columns
print("Expected but missing:", missing_in_data)
print("In data but not expected:", extra_in_data)
```

**Fix:**
- If schema changed legitimately → update suite to match new schema
- If column was dropped accidentally → fix upstream schema migration
- For resilience → add `expect_column_to_exist` with severity=warning so missing columns are flagged but don't fail the pipeline

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Scaling GX to 200 Tables

**Scenario:** Your team needs to add GX validation to 200 tables. How do you scale this without writing 200 suites manually?

<details>
<summary>💡 Hint</summary>

**Auto-generation pipeline:**

</details>

<details>
<summary>✅ Solution</summary>

**Auto-generation pipeline:**
```python
import pandas as pd
import great_expectations as gx
from typing import List

def auto_generate_suite(table_name: str, df: pd.DataFrame) -> gx.ExpectationSuite:
    """Auto-generate a baseline suite from data profiling."""
    context = gx.get_context()
    
    expectations = []
    
    # Table-level
    row_count = len(df)
    expectations.append(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=max(1, int(row_count * 0.5)),
        max_value=int(row_count * 2.0),
    ))
    
    for col in df.columns:
        null_rate = df[col].isna().mean()
        
        # Not-null for columns that are always populated
        if null_rate == 0:
            expectations.append(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
        elif null_rate < 0.05:
            expectations.append(gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col, mostly=0.95
            ))
        
        # Uniqueness for likely PKs
        if col.endswith("_id") and df[col].nunique() == len(df):
            expectations.append(gx.expectations.ExpectColumnValuesToBeUnique(column=col))
        
        # Accepted values for low-cardinality columns
        unique_vals = df[col].dropna().unique()
        if df[col].dtype == "object" and len(unique_vals) <= 20:
            expectations.append(gx.expectations.ExpectColumnValuesToBeInSet(
                column=col, value_set=set(unique_vals)
            ))
        
        # Range for numerics
        if pd.api.types.is_numeric_dtype(df[col]):
            q1 = df[col].quantile(0.01)
            q99 = df[col].quantile(0.99)
            expectations.append(gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=float(q1), max_value=float(q99), mostly=0.98
            ))
    
    suite = gx.ExpectationSuite(name=f"{table_name}_auto_suite", expectations=expectations)
    return context.suites.add(suite)


# Run for all tables
tables_to_onboard: List[str] = [...]  # from catalog
for table_name in tables_to_onboard:
    df = spark.table(table_name).limit(100_000).toPandas()
    suite = auto_generate_suite(table_name, df)
    print(f"Generated {len(suite.expectations)} expectations for {table_name}")
```

**Key design decisions to mention in interview:**
1. Auto-generate from profiling, then have data stewards review before going live
2. Store suites in S3, not local files — shared across all engineers
3. Use GX Cloud for web UI so non-engineers can manage expectations
4. Tag each expectation with `owner` metadata for accountability
5. Start with severity=warning for all auto-generated rules, promote to critical after 30 days of stability

</details>

</article>