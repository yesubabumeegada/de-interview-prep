---
title: "Snowflake Snowpark & UDFs - Senior Deep Dive"
topic: snowflake
subtopic: snowpark-udfs
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [snowflake, snowpark, containers, optimization, ml-pipeline, production]
---

# Snowflake Snowpark & UDFs — Senior Deep Dive

## Snowpark Container Services (SPCS)

Run any Docker container inside Snowflake's network — full access to Snowflake data without egress:

```yaml
# spec.yaml — container spec
spec:
  containers:
    - name: mlflow-tracker
      image: /mydb/myschema/myrepo/mlflow:latest
      env:
        SNOWFLAKE_WAREHOUSE: ML_WH
      volumeMounts:
        - name: model-storage
          mountPath: /models
  volumes:
    - name: model-storage
      source: "@model_stage"
  endpoints:
    - name: mlflow-ui
      port: 5000
      public: true
```

```sql
-- Create and start service
CREATE SERVICE mlflow_service
    IN COMPUTE POOL gpu_pool
    FROM SPECIFICATION $$
    spec:
      containers:
      - name: training
        image: /repo/training:v2
        resources:
          requests:
            nvidia.com/gpu: 1
    $$;

-- Call container endpoint from SQL
SELECT SYSTEM$GET_SERVICE_STATUS('mlflow_service');

-- Stop when done (billing stops)
ALTER SERVICE mlflow_service SUSPEND;
```

**Use cases:**
- GPU-accelerated model training (PyTorch, TensorFlow) with Snowflake data
- LLM fine-tuning on private data
- Custom inference servers (faster than UDFs for high-throughput)
- Running arbitrary tools (dbt, Airbyte) inside Snowflake's network

---

## UDF Performance Optimization

### Handler Caching

Python UDFs spin up a Python interpreter per batch. Use class-level caching for expensive initialization:

```python
# BAD: loads model on every call (extremely slow)
def predict(features: list) -> float:
    import joblib
    model = joblib.load("/tmp/model.pkl")  # loads every row!
    return model.predict([features])[0]

# GOOD: load once per worker process using class-level state
class ChurnPredictor:
    def __init__(self):
        import joblib
        import sys
        # Model loaded once per worker — cached across all rows in the batch
        self._model = joblib.load(
            sys._xoptions.get("snowflake_import_directory") + "churn_model.pkl"
        )

    def process(self, ltv: float, recency: float, frequency: float) -> float:
        return float(self._model.predict_proba([[ltv, recency, frequency]])[0][1])
```

### Vectorized UDFs vs Row UDFs

| Approach | Throughput | Memory | When to Use |
|----------|-----------|--------|------------|
| Row-level UDF | Low | Low | Simple transformations, infrequent calls |
| Vectorized (Pandas Series) | 10-100x faster | Medium | Bulk scoring, numpy operations |
| UDTF with batching | High | Configurable | Multi-output, complex generation |

```python
# Vectorized — receives full batch as Pandas Series
from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import PandasSeries, FloatType
import pandas as pd
import numpy as np

@pandas_udf(is_permanent=True, stage_location="@models_stage", session=session,
            return_type=FloatType())
def score_bulk(ltv: PandasSeries[float], recency: PandasSeries[float]) -> PandasSeries[float]:
    # numpy operations on entire batch — no Python loop
    score = np.log1p(ltv) / (1 + recency / 30)
    return pd.Series(score.clip(0, 100))
```

---

## Building an ML Pipeline in Snowpark

End-to-end pipeline: feature engineering → training → scoring:

```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, datediff, lit, count, sum as sum_, avg
from snowflake.ml.modeling.ensemble import RandomForestClassifier
from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.pipeline import Pipeline

session = Session.builder.configs(conn).create()

# Feature engineering (Snowpark DataFrame)
features = (
    session.table("fact_orders")
    .group_by("customer_id")
    .agg(
        count("order_id").alias("order_count"),
        sum_("amount").alias("total_ltv"),
        datediff("day", col("order_date").max(), lit("2024-03-01")).alias("recency_days")
    )
    .join(session.table("dim_customers").select("customer_id", "churn_label"), "customer_id")
)

# Train/test split
train_df, test_df = features.random_split([0.8, 0.2], seed=42)

# Build pipeline (runs entirely in Snowflake)
pipeline = Pipeline(steps=[
    ("scaler", StandardScaler(input_cols=["order_count","total_ltv","recency_days"],
                              output_cols=["order_count_s","ltv_s","recency_s"])),
    ("model", RandomForestClassifier(
        input_cols=["order_count_s","ltv_s","recency_s"],
        label_cols=["churn_label"],
        output_cols=["churn_pred"]
    ))
])

pipeline.fit(train_df)

# Evaluate on test set
predictions = pipeline.predict(test_df)
predictions.select("churn_label", "churn_pred").show()

# Save model to registry
from snowflake.ml.registry import Registry
reg = Registry(session=session, database_name="analytics", schema_name="ml_models")
reg.log_model(pipeline, model_name="churn_model", version_name="v1")
```

---

## Snowpark Stored Procedures: Control Flow Patterns

```sql
-- Stored procedure with retry logic and logging
CREATE OR REPLACE PROCEDURE run_pipeline_with_retry(
    target_table VARCHAR,
    max_retries INT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import time

def run(session: Session, target_table: str, max_retries: int) -> str:
    for attempt in range(max_retries):
        try:
            session.sql(f"TRUNCATE TABLE {target_table}").collect()
            session.sql(f"""
                INSERT INTO {target_table}
                SELECT * FROM staging.{target_table}_raw
                WHERE loaded_at > (SELECT MAX(loaded_at) FROM {target_table})
            """).collect()

            count = session.table(target_table).count()
            session.sql(f"""
                INSERT INTO pipeline_log VALUES ('{target_table}', 'SUCCESS', {count}, CURRENT_TIMESTAMP())
            """).collect()
            return f"SUCCESS: {count} rows"

        except SnowparkSQLException as e:
            if attempt == max_retries - 1:
                session.sql(f"""
                    INSERT INTO pipeline_log VALUES ('{target_table}', 'FAILED', 0, CURRENT_TIMESTAMP())
                """).collect()
                raise
            time.sleep(2 ** attempt)  # exponential backoff

    return "EXHAUSTED_RETRIES"
$$;

CALL run_pipeline_with_retry('fact_orders', 3);
```

---

## Security: UDF Execution Context

```sql
-- CALLER's RIGHTS: UDF runs with the privilege of whoever calls it
CREATE FUNCTION safe_lookup(key VARCHAR) RETURNS VARCHAR
LANGUAGE PYTHON
EXECUTE AS CALLER  -- default
...

-- OWNER's RIGHTS: UDF runs with the privilege of the UDF owner (allows privilege escalation control)
CREATE FUNCTION admin_lookup(key VARCHAR) RETURNS VARCHAR
LANGUAGE PYTHON
EXECUTE AS OWNER   -- runs as owner, caller's role doesn't matter
...
```

**Security pattern:** Use `EXECUTE AS OWNER` for UDFs that need access to privileged tables — callers can use the UDF without being granted direct table access. The UDF acts as a controlled gateway.

---

## Interview Tips

> **Tip 1:** "How do you handle Python dependency management in Snowpark UDFs?" — "Snowflake maintains an Anaconda channel with 300+ pre-approved packages. Specify with `PACKAGES = ('pandas', 'scikit-learn')`. For custom packages not in the channel, upload a zip to a stage and reference with `IMPORTS`. Enterprise accounts can use Private Connectivity to pull from private PyPI."

> **Tip 2:** "What are Snowpark Container Services and when would you use them over UDFs?" — "SPCS runs full Docker containers in Snowflake's network — for GPU training, custom runtimes (Node.js, Go), or tools that need persistent state or external ports (MLflow, Jupyter). UDFs are better for per-row or per-batch functions that need to be called from SQL. SPCS is an always-on service; UDFs are ephemeral."

> **Tip 3:** "How do you optimize a Python UDF that's running slowly?" — "Four levers: (1) Switch to vectorized/Pandas UDF — eliminates Python row-by-row overhead. (2) Cache expensive initialization (model loading) in class `__init__`. (3) Reduce the dataset before calling the UDF (filter in SQL first). (4) Consider if the logic can be rewritten in SQL — Snowflake's SQL engine is heavily optimized; Python adds overhead per batch."
