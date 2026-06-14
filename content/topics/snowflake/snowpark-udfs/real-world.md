---
title: "Snowflake Snowpark & UDFs - Real-World Examples"
topic: snowflake
subtopic: snowpark-udfs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, snowpark, production, ml, feature-store, inference]
---

# Snowflake Snowpark & UDFs — Real-World Production Examples

## Production Pattern: Real-Time Fraud Scoring with Snowpark UDF

A payments company scores every transaction for fraud at query time using a UDF:

```python
# 1. Train XGBoost model locally on Snowflake data (Snowpark reads it)
from snowflake.snowpark import Session
import xgboost as xgb
import joblib

session = Session.builder.configs(conn).create()

# Pull features (Snowpark reads from Snowflake, no pandas needed for large data)
features_df = session.table("ml_features.transaction_features").to_pandas()
X = features_df[["amount", "merchant_category", "hour_of_day", "velocity_24h"]]
y = features_df["is_fraud"]

model = xgb.XGBClassifier(n_estimators=500, max_depth=6)
model.fit(X, y)

# 2. Upload model to stage
joblib.dump(model, "/tmp/fraud_model.pkl")
session.file.put("/tmp/fraud_model.pkl", "@ml_models/", overwrite=True)

# 3. Register as vectorized UDF for batch scoring
from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import PandasSeries, FloatType
import pandas as pd
import sys

@pandas_udf(
    is_permanent=True,
    name="fraud_score",
    replace=True,
    stage_location="@ml_models",
    packages=["xgboost", "joblib", "pandas"],
    session=session
)
def fraud_score(
    amount: PandasSeries[float],
    merchant_cat: PandasSeries[str],
    hour: PandasSeries[int],
    velocity: PandasSeries[float]
) -> PandasSeries[float]:
    import joblib, sys
    model_path = sys._xoptions["snowflake_import_directory"] + "fraud_model.pkl"
    model = getattr(fraud_score, "_model", None) or joblib.load(model_path)
    fraud_score._model = model  # cache in function attribute
    X = pd.DataFrame({"amount": amount, "merchant_category": merchant_cat,
                       "hour_of_day": hour, "velocity_24h": velocity})
    return pd.Series(model.predict_proba(X)[:, 1])
```

```sql
-- 4. Score all today's transactions in SQL
SELECT
    transaction_id,
    amount,
    merchant_name,
    fraud_score(amount, merchant_category, hour_of_day, velocity_24h) AS fraud_probability,
    CASE WHEN fraud_score(amount, merchant_category, hour_of_day, velocity_24h) > 0.85
         THEN 'BLOCK' ELSE 'PASS' END AS decision
FROM transactions
WHERE transaction_date = CURRENT_DATE()
ORDER BY fraud_probability DESC;
```

**Results:** Scoring 2M daily transactions takes ~45 seconds on a Medium warehouse. Zero data movement out of Snowflake. Model is retrained weekly via a scheduled Snowpark stored procedure.

---

## Production Pattern: ETL Replacement with Snowpark

A complex Python ETL pipeline (pandas on EC2) was migrated to Snowpark to eliminate infrastructure:

**Before:** 6 EC2 instances, pandas, 4 hour runtime, $1,200/month infra
**After:** Snowpark stored procedures on Medium warehouse, 45 min runtime, ~$80/month

```python
# Before: pandas on EC2 (loading 500M rows into memory = 128 GB RAM required)
import pandas as pd
df = pd.read_csv("s3://bucket/large_file.csv")  # 45 GB CSV
df["revenue_usd"] = df["revenue_local"] / df["fx_rate"]
df.groupby("region")["revenue_usd"].sum().to_csv("output.csv")

# After: Snowpark (data never leaves Snowflake, Medium warehouse = 32 GB RAM per node)
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_

session = Session.builder.configs(conn).create()

result = (
    session.table("fact_revenue")
    .with_column("revenue_usd", col("revenue_local") / col("fx_rate"))
    .group_by("region")
    .agg(sum_("revenue_usd").alias("total_usd"))
)

result.write.save_as_table("analytics.curated.revenue_by_region", mode="overwrite")
```

---

## Production Pattern: Dynamic SQL Generation in Stored Procedures

Generating and executing DDL dynamically — common in data platform engineering:

```sql
CREATE OR REPLACE PROCEDURE create_partition_tables(
    base_table_name VARCHAR,
    start_year INT,
    end_year INT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_partitions'
AS $$
from snowflake.snowpark import Session

def create_partitions(session: Session, base_table: str, start_year: int, end_year: int) -> str:
    created = []
    for year in range(start_year, end_year + 1):
        table_name = f"{base_table}_{year}"
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            AS SELECT * FROM {base_table}
            WHERE YEAR(order_date) = {year}
        """
        session.sql(ddl).collect()
        count = session.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        created.append(f"{table_name}: {count} rows")

    return "\n".join(created)
$$;

CALL create_partition_tables('fact_orders', 2020, 2024);
```

---

## Production: UDF for JSON Schema Normalization

A UDF that standardizes inconsistent JSON payloads from multiple sources:

```sql
CREATE OR REPLACE FUNCTION normalize_event(raw_json VARIANT, source_system VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'normalize'
AS $$
import json

FIELD_MAPS = {
    "system_a": {"userId": "user_id", "ts": "event_timestamp", "evt": "event_type"},
    "system_b": {"uid": "user_id", "timestamp": "event_timestamp", "eventName": "event_type"},
    "system_c": {"user": "user_id", "time": "event_timestamp", "action": "event_type"},
}

def normalize(raw_json: dict, source_system: str) -> dict:
    if not raw_json or source_system not in FIELD_MAPS:
        return raw_json

    mapping = FIELD_MAPS[source_system]
    return {mapping.get(k, k): v for k, v in raw_json.items()}
$$;

-- Normalize events from three source systems into consistent schema
SELECT
    normalize_event(raw_payload, source_system):user_id::STRING AS user_id,
    normalize_event(raw_payload, source_system):event_timestamp::TIMESTAMP AS event_ts,
    normalize_event(raw_payload, source_system):event_type::STRING AS event_type
FROM raw_events_union;
```

---

## Monitoring Snowpark Jobs in Production

```sql
-- Find slow stored procedure calls
SELECT
    query_text,
    user_name,
    TOTAL_ELAPSED_TIME / 1000 AS duration_secs,
    EXECUTION_STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_type = 'CALL'
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;

-- Find expensive UDF calls (high bytes scanned + long runtime)
SELECT
    query_text,
    BYTES_SCANNED / 1e9 AS gb_scanned,
    TOTAL_ELAPSED_TIME / 1000 AS secs,
    ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%fraud_score%'  -- your UDF name
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY secs DESC;
```
