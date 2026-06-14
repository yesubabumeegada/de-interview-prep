---
title: "Snowflake Snowpark & UDFs - Intermediate"
topic: snowflake
subtopic: snowpark-udfs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, snowpark, udf, udtf, ml, feature-engineering, pandas]
---

# Snowflake Snowpark & UDFs — Intermediate Concepts

## Snowpark DataFrames: Advanced Transformations

```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, when, regexp_extract, to_date, datediff,
    lag, lead, rank, row_number, window_spec,
    array_agg, object_construct
)
from snowflake.snowpark.window import Window

session = Session.builder.configs(connection_params).create()

orders = session.table("fact_orders")
customers = session.table("dim_customers")

# Join
enriched = orders.join(customers, on="customer_id", how="left")

# Window functions
window = Window.partition_by("customer_id").order_by(col("order_date").asc())

with_lag = enriched.with_column(
    "days_since_last_order",
    datediff("day", lag("order_date").over(window), col("order_date"))
)

# Conditional columns
classified = with_lag.with_column(
    "customer_tier",
    when(col("ltv") > 10000, lit("GOLD"))
    .when(col("ltv") > 1000, lit("SILVER"))
    .otherwise(lit("BRONZE"))
)

# Write back
classified.write.save_as_table("analytics.curated.enriched_orders", mode="overwrite")
```

---

## User-Defined Table Functions (UDTFs)

UDTFs return multiple rows per input row — useful for exploding nested data:

```sql
-- UDTF: parse a comma-separated string into rows
CREATE OR REPLACE FUNCTION split_tags(tag_string VARCHAR)
RETURNS TABLE (tag VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'TagSplitter'
AS $$
class TagSplitter:
    def process(self, tag_string: str):
        if not tag_string:
            return
        for tag in tag_string.split(','):
            yield (tag.strip(),)
$$;

-- Call with TABLE() in SQL
SELECT article_id, t.tag
FROM articles,
TABLE(split_tags(tags_column)) t;
-- One row per tag, one article may generate 5+ rows
```

---

## ML Model Inference Inside Snowflake

Deploy a trained model as a UDF — inference runs inside Snowflake:

```python
# Step 1: Train model locally
from sklearn.ensemble import RandomForestClassifier
import joblib

model = RandomForestClassifier(n_estimators=100)
model.fit(X_train, y_train)

# Step 2: Save model to a Snowflake stage
import os
joblib.dump(model, "/tmp/churn_model.pkl")
session.file.put("/tmp/churn_model.pkl", "@models_stage/", auto_compress=False, overwrite=True)

# Step 3: Register as a UDF that loads the model from stage
session.udf.register_from_file(
    file_path="@models_stage/churn_model.pkl",
    func_name="predict_churn",
    input_types=[FloatType(), FloatType(), FloatType()],  # ltv, days_inactive, order_count
    return_type=FloatType(),
    packages=["scikit-learn", "joblib"],
    is_permanent=True,
    stage_location="@models_stage"
)

# Or define inline:
@udf(name="predict_churn_inline", is_permanent=True, stage_location="@models_stage",
     packages=["scikit-learn", "joblib"], session=session)
def predict_churn(ltv: float, days_inactive: float, orders: float) -> float:
    import joblib
    import sys
    model = joblib.load(sys.modules["_snowflake"].importlib.resources.path("churn_model.pkl"))
    return float(model.predict_proba([[ltv, days_inactive, orders]])[0][1])
```

```sql
-- Step 4: Score all customers with SQL
SELECT customer_id, predict_churn(ltv, days_since_last_order, order_count) AS churn_prob
FROM dim_customers
ORDER BY churn_prob DESC;
```

---

## Snowpark for Feature Engineering

Common pattern: build a feature store table using Snowpark Python:

```python
from snowflake.snowpark.functions import col, datediff, lit, count, sum as sum_, avg

def build_customer_features(session: Session) -> None:
    orders = session.table("fact_orders")
    today = session.sql("SELECT CURRENT_DATE()").collect()[0][0]

    # Recency, Frequency, Monetary (RFM) features
    rfm = (
        orders
        .filter(col("order_date") >= "2023-01-01")
        .group_by("customer_id")
        .agg(
            datediff("day", col("order_date").max(), lit(today)).alias("recency_days"),
            count("order_id").alias("frequency"),
            sum_("amount").alias("monetary"),
            avg("amount").alias("avg_order_value")
        )
    )

    # Normalize using SQL functions
    from snowflake.snowpark.functions import (col as c, min as min_, max as max_)
    stats = rfm.select(
        min_("monetary").alias("min_m"),
        max_("monetary").alias("max_m")
    ).collect()[0]

    rfm_normalized = rfm.with_column(
        "monetary_norm",
        (col("monetary") - stats["MIN_M"]) / (stats["MAX_M"] - stats["MIN_M"])
    )

    rfm_normalized.write.save_as_table(
        "analytics.features.customer_rfm",
        mode="overwrite"
    )

# Run it
build_customer_features(session)
```

---

## Snowpark Pandas (Modin on Snowflake)

Use Pandas API, execution happens in Snowflake (no data export):

```python
import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # activate Snowpark backend

# Looks exactly like Pandas — runs on Snowflake
df = pd.read_snowflake("analytics.curated.fact_orders")

# Standard pandas operations
monthly = df.groupby(df["order_date"].dt.to_period("M"))["amount"].sum()
df["is_large_order"] = df["amount"] > df["amount"].quantile(0.9)

# Write back
df.to_snowflake("analytics.curated.fact_orders_enriched", if_exists="replace")
```

---

## Packaging and Deploying Snowpark Code

```python
# Local development → deploy to Snowflake

# Option 1: Register function from local file
session.add_packages("pandas", "scikit-learn")
session.add_import("/local/path/my_module.py")

@udf(session=session, is_permanent=True, stage_location="@code_stage")
def my_function(x: float) -> float:
    from my_module import process
    return process(x)

# Option 2: Package as a zip and upload
import zipfile
with zipfile.ZipFile("/tmp/my_pkg.zip", "w") as z:
    z.write("/local/path/my_module.py", "my_module.py")

session.add_import("/tmp/my_pkg.zip")
```

---

## Error Handling in Snowpark

```python
from snowflake.snowpark.exceptions import SnowparkSQLException

try:
    result = session.table("nonexistent_table").collect()
except SnowparkSQLException as e:
    print(f"Snowflake error: {e.message}")
    print(f"SQL state: {e.sql_error_code}")

# Safe collect with limit (avoid OOM on large tables)
def safe_collect(df, limit=10000):
    count = df.count()
    if count > limit:
        raise ValueError(f"DataFrame has {count} rows — use show() or write to table instead")
    return df.collect()
```

---

## Interview Tips

> **Tip 1:** "How is Snowpark DataFrame evaluation different from Pandas?" — "Snowpark DataFrames are lazy — operations build a query plan but nothing executes until you call `.show()`, `.collect()`, or `.write`. This means you can chain many transforms and Snowflake optimizes the whole plan into one SQL query. Pandas is eager — every operation executes immediately."

> **Tip 2:** "How do you deploy an ML model for scoring in Snowflake?" — "Train locally or in a notebook, serialize with joblib/pickle to a Snowflake stage, register as a Python UDF that loads the model from stage on first call (cached per worker). Then call it in SQL. This keeps scoring inside Snowflake with no data movement — works for sklearn, XGBoost, LightGBM."

> **Tip 3:** "What's a UDTF and when do you use it?" — "A User-Defined Table Function returns multiple rows per input row. Use it when you need to explode a delimited string into rows, parse a JSON array into individual records, or generate a date series from a start/end date pair. Yield-based Python handler makes it clean to implement."
