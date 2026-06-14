---
title: "Snowflake Snowpark & UDFs - Scenario Questions"
topic: snowflake
subtopic: snowpark-udfs
content_type: scenario_question
tags: [snowflake, snowpark, udf, scenarios, interview]
---

# Scenario Questions — Snowflake Snowpark & UDFs

<article data-difficulty="junior">

## 🟢 Junior: Create a Python UDF for Custom String Masking

**Scenario:** Your team needs a reusable SQL function that masks a credit card number — showing only the last 4 digits (e.g., `4532-1234-5678-9012` → `****-****-****-9012`). Create it as a Python UDF that can be called in any SQL query.

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE FUNCTION mask_credit_card(card_number VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'mask'
AS $$
import re

def mask(card_number: str) -> str:
    if not card_number:
        return None

    # Remove spaces and dashes for processing
    digits_only = re.sub(r'[\s\-]', '', card_number)

    if len(digits_only) < 4:
        return '****'

    last_four = digits_only[-4:]

    # Rebuild with original format if it had dashes
    if '-' in card_number:
        # Standard 4-group format
        return f'****-****-****-{last_four}'
    else:
        return f'{"*" * (len(digits_only) - 4)}{last_four}'
$$;

-- Test it
SELECT
    '4532-1234-5678-9012' AS original,
    mask_credit_card('4532-1234-5678-9012') AS masked;
-- Result: ****-****-****-9012

SELECT mask_credit_card(card_number) AS safe_card FROM payments LIMIT 10;
```

**Key considerations:**
- `HANDLER = 'mask'` matches the Python function name exactly
- Returns `NULL` for NULL input (defensive coding)
- The UDF is scalar — called once per row

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Replace a Pandas ETL with Snowpark

**Scenario:** Your team has a Python script that:
1. Pulls 50M rows from Snowflake into Pandas
2. Calculates 30-day rolling revenue per customer
3. Writes results back to Snowflake

It crashes with OOM on the production server (32 GB RAM). Rewrite using Snowpark so computation stays in Snowflake.

<details>
<summary>✅ Solution</summary>

```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, to_date, dateadd, lit
from snowflake.snowpark.window import Window
import datetime

session = Session.builder.configs(connection_params).create()

orders = session.table("fact_orders")

# Define 30-day rolling window (Snowflake handles this in SQL — no RAM issue)
rolling_window = (
    Window
    .partition_by("customer_id")
    .order_by(col("order_date").cast("timestamp").cast("long"))
    .range_between(
        -30 * 24 * 60 * 60,  # 30 days in seconds (range is in seconds for TIMESTAMP)
        0
    )
)

result = (
    orders
    .select(
        col("customer_id"),
        col("order_date"),
        col("amount"),
        sum_("amount").over(rolling_window).alias("rolling_30d_revenue")
    )
)

# Write back — no pandas, no OOM
result.write.save_as_table(
    "analytics.curated.customer_rolling_revenue",
    mode="overwrite"
)

print(f"Wrote {result.count()} rows")
```

**Why this works:**
- Snowpark translates `.over(rolling_window)` into SQL window functions
- SQL runs entirely on Snowflake compute — no data pulled to the application server
- 50M rows processed on a Medium warehouse in ~5 minutes vs OOM crash on pandas

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design an ML Inference Pipeline with Model Versioning

**Scenario:** Your ML team trains a new churn prediction model weekly. The pipeline must: (1) deploy new models without downtime, (2) support A/B testing between two model versions, (3) log predictions for drift monitoring. Design the full Snowflake-native architecture.

<details>
<summary>✅ Solution</summary>

**Architecture overview:**

```
Weekly training job (Snowpark)
    → Register model in Snowflake Model Registry (versioned)
    → Deploy as versioned UDF
    → A/B routing UDF (50/50 split based on customer_id hash)
    → Prediction log table (for drift monitoring)
```

```python
from snowflake.ml.registry import Registry

session = Session.builder.configs(conn).create()
reg = Registry(session=session, database_name="analytics", schema_name="ml_models")

# Deploy model with version tag — old version still accessible
reg.log_model(
    new_model,
    model_name="churn_predictor",
    version_name="v2024_03_15",
    tags={"stage": "production", "accuracy": "0.87"}
)

# Get specific version as UDF
model_v1 = reg.get_model("churn_predictor").version("v2024_03_08")
model_v2 = reg.get_model("churn_predictor").version("v2024_03_15")
```

```sql
-- A/B routing: route customer to model version by ID hash
CREATE OR REPLACE FUNCTION score_churn_ab(
    customer_id VARCHAR,
    ltv FLOAT,
    recency FLOAT,
    frequency FLOAT
)
RETURNS OBJECT  -- returns {score, model_version}
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'route_and_score'
AS $$
import hashlib
import json

def route_and_score(customer_id: str, ltv: float, recency: float, frequency: float) -> dict:
    # Deterministic routing (same customer always hits same model)
    hash_val = int(hashlib.md5(customer_id.encode()).hexdigest(), 16) % 100
    version = "v2024_03_15" if hash_val < 50 else "v2024_03_08"

    # Load appropriate model (simplified — in practice use registry API)
    import sys, joblib
    model = joblib.load(f"{sys._xoptions['snowflake_import_directory']}{version}_model.pkl")
    score = float(model.predict_proba([[ltv, recency, frequency]])[0][1])

    return {"score": score, "model_version": version}
$$;

-- Score and log predictions
INSERT INTO ml_prediction_log (customer_id, score, model_version, predicted_at)
SELECT
    customer_id,
    score_churn_ab(customer_id, ltv, recency_days, order_count):score::FLOAT,
    score_churn_ab(customer_id, ltv, recency_days, order_count):model_version::STRING,
    CURRENT_TIMESTAMP()
FROM dim_customers;

-- Monitor drift: compare score distributions between versions
SELECT
    model_version,
    COUNT(*) AS predictions,
    AVG(score) AS avg_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median_score,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY score) AS p90_score
FROM ml_prediction_log
WHERE predicted_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY model_version;
```

**Key design decisions:**
- Deterministic routing (hash of customer_id) ensures the same customer is always scored by the same model — prevents leakage in A/B results
- Prediction log enables retrospective drift analysis and business outcome attribution
- Old model version stays accessible in registry until explicitly deprecated

</details>
</article>
