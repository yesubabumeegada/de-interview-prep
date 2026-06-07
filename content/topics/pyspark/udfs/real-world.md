---
title: "PySpark UDFs - Real World Patterns"
topic: pyspark
subtopic: udfs
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, udf, business-logic, ml-inference, api-lookup, benchmarks, production]
---

# PySpark UDFs — Real-World Patterns

## Pattern 1: Complex Business Logic UDF

**Problem:** An insurance company needs to calculate risk scores based on 20+ rules involving customer demographics, claim history, and external factors. The rules are too complex for nested `when/otherwise`.

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, ArrayType

# Output schema for the risk assessment
risk_schema = StructType([
    StructField("risk_score", DoubleType()),
    StructField("risk_tier", StringType()),
    StructField("risk_factors", ArrayType(StringType())),
])

@F.pandas_udf(risk_schema)
def calculate_risk_score(
    age: pd.Series,
    claim_count: pd.Series,
    claim_total: pd.Series,
    years_as_customer: pd.Series,
    credit_score: pd.Series,
    vehicle_age: pd.Series,
    zip_risk_zone: pd.Series,
) -> pd.DataFrame:
    """Vectorized risk scoring with complex business rules."""
    
    n = len(age)
    scores = np.zeros(n)
    factors = [[] for _ in range(n)]
    
    # Rule 1: Age factor (vectorized)
    age_factor = np.where(age < 25, 1.5,
                 np.where(age > 70, 1.3,
                 np.where(age.between(25, 35), 0.9, 1.0)))
    scores += age_factor * 20
    
    # Rule 2: Claim history
    claim_factor = np.minimum(claim_count * 0.3, 2.0)
    scores += claim_factor * 25
    high_claims = claim_count > 3
    for i in np.where(high_claims)[0]:
        factors[i].append("high_claim_frequency")
    
    # Rule 3: Loyalty discount
    loyalty_discount = np.minimum(years_as_customer * 0.02, 0.15)
    scores *= (1 - loyalty_discount)
    
    # Rule 4: Credit factor
    credit_factor = np.where(credit_score > 750, 0.8,
                   np.where(credit_score < 600, 1.4, 1.0))
    scores *= credit_factor
    low_credit = credit_score < 600
    for i in np.where(low_credit)[0]:
        factors[i].append("low_credit_score")
    
    # Rule 5: Vehicle age
    scores += np.where(vehicle_age > 10, 5, 0)
    
    # Rule 6: Geographic risk zone
    zone_multiplier = zip_risk_zone.map({"high": 1.3, "medium": 1.0, "low": 0.8}).fillna(1.0)
    scores *= zone_multiplier
    
    # Determine tier
    tiers = np.where(scores > 80, "high_risk",
            np.where(scores > 50, "medium_risk", "low_risk"))
    
    return pd.DataFrame({
        "risk_score": scores.round(2),
        "risk_tier": tiers,
        "risk_factors": factors,
    })

# Apply
result = df.withColumn("risk",
    calculate_risk_score(
        "age", "claim_count", "claim_total",
        "years_as_customer", "credit_score",
        "vehicle_age", "zip_risk_zone"
    )
).select("*", "risk.*").drop("risk")
```

---

## Pattern 2: ML Model Inference UDF

**Problem:** Apply a trained scikit-learn model to score 100 million records. The model is 500MB and can't be broadcast efficiently.

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from typing import Iterator

# Strategy: Iterator UDF — load model once per executor partition
@F.pandas_udf("double")
def predict_churn(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    """
    Load model once, predict across all batches in the partition.
    Iterator pattern avoids re-loading the model per batch.
    """
    import joblib
    import os
    
    # Load model once (will be reused across all batches in this partition)
    model_path = "/tmp/churn_model.pkl"
    
    # Download model from distributed storage if not cached
    if not os.path.exists(model_path):
        import boto3
        s3 = boto3.client("s3")
        s3.download_file("ml-models", "churn/v2/model.pkl", model_path)
    
    model = joblib.load(model_path)
    
    for batch_df in batch_iter:
        features = batch_df[["recency", "frequency", "monetary", "tenure"]].values
        predictions = model.predict_proba(features)[:, 1]  # Probability of churn
        yield pd.Series(predictions)

# Prepare features and predict
features_df = (customer_df
    .select("customer_id", "recency", "frequency", "monetary", "tenure")
)

scored = features_df.withColumn(
    "churn_probability",
    predict_churn(F.struct("recency", "frequency", "monetary", "tenure"))
)

# Optimization: repartition to match executor count for model loading efficiency
scored = (features_df
    .repartition(100)  # 100 partitions = model loaded 100 times max
    .withColumn("churn_probability",
        predict_churn(F.struct("recency", "frequency", "monetary", "tenure")))
)
```

### Model Distribution Strategies

| Strategy | Model Size | Latency | Memory |
|----------|-----------|---------|--------|
| Broadcast variable | < 100MB | Low (pre-distributed) | High (all executors) |
| S3/HDFS download per partition | Any size | First-batch delay | On-demand |
| Mounted filesystem | Any size | Low (local read) | Minimal |
| Model server (API call) | Any size | Network latency | Minimal |

---

## Pattern 3: External API Lookup UDF

**Problem:** Enrich customer addresses with geocoding from an external API. Rate limit to 100 requests/second, handle failures gracefully.

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from typing import Iterator

geocode_schema = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("geocode_status", StringType()),
])

@F.pandas_udf(geocode_schema)
def geocode_addresses(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    """
    Geocode addresses using external API with rate limiting and caching.
    Uses Iterator pattern to maintain connection and cache across batches.
    """
    import requests
    import time
    from functools import lru_cache
    
    session = requests.Session()
    session.headers.update({"Authorization": "Bearer API_KEY"})
    
    # LRU cache to avoid redundant API calls
    @lru_cache(maxsize=10000)
    def geocode_single(address):
        try:
            resp = session.get(
                "https://geocode.api.com/v1/search",
                params={"q": address},
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("results"):
                    r = data["results"][0]
                    return (r["lat"], r["lng"], "success")
            elif resp.status_code == 429:
                time.sleep(1)  # Rate limited — back off
                return geocode_single(address)  # Retry
            return (None, None, f"error_{resp.status_code}")
        except Exception as e:
            return (None, None, f"exception_{type(e).__name__}")
    
    requests_this_second = 0
    last_second = time.time()
    
    for batch in batch_iter:
        results = {"latitude": [], "longitude": [], "geocode_status": []}
        
        for address in batch:
            # Rate limiting: max 100 requests per second
            current_second = time.time()
            if current_second - last_second < 1:
                requests_this_second += 1
                if requests_this_second >= 100:
                    time.sleep(1 - (current_second - last_second))
                    last_second = time.time()
                    requests_this_second = 0
            else:
                last_second = current_second
                requests_this_second = 0
            
            lat, lng, status = geocode_single(address if address else "")
            results["latitude"].append(lat)
            results["longitude"].append(lng)
            results["geocode_status"].append(status)
        
        yield pd.DataFrame(results)

# Apply with controlled parallelism
geocoded = (addresses_df
    .repartition(10)  # Limit to 10 parallel API callers
    .withColumn("geo", geocode_addresses(F.col("full_address")))
    .select("*", "geo.*")
    .drop("geo")
)
```

---

## Performance Benchmarks — Complete Comparison

```python
# Benchmark setup: 50M rows, text processing task
# Task: Extract email domain + validate format + categorize

# Approach 1: Python UDF (row-at-a-time)
@F.udf(StringType())
def python_process(email):
    if not email or "@" not in email:
        return "invalid"
    domain = email.split("@")[1]
    if domain.endswith(".com"):
        return "commercial"
    elif domain.endswith(".edu"):
        return "education"
    return "other"
# Time: 180 seconds

# Approach 2: Pandas UDF (vectorized)
@F.pandas_udf(StringType())
def pandas_process(emails: pd.Series) -> pd.Series:
    domains = emails.str.split("@").str[1]
    result = pd.Series("other", index=emails.index)
    result[emails.isna() | ~emails.str.contains("@", na=False)] = "invalid"
    result[domains.str.endswith(".com", na=False)] = "commercial"
    result[domains.str.endswith(".edu", na=False)] = "education"
    return result
# Time: 35 seconds

# Approach 3: Native Spark functions
native_result = (df.withColumn("category",
    F.when(F.col("email").isNull() | ~F.col("email").contains("@"), "invalid")
     .when(F.col("email").endswith(".com"), "commercial")
     .when(F.col("email").endswith(".edu"), "education")
     .otherwise("other")))
# Time: 8 seconds

# Approach 4: Spark SQL expression
sql_result = df.selectExpr("""
    CASE
        WHEN email IS NULL OR email NOT LIKE '%@%' THEN 'invalid'
        WHEN email LIKE '%.com' THEN 'commercial'
        WHEN email LIKE '%.edu' THEN 'education'
        ELSE 'other'
    END AS category
""")
# Time: 8 seconds
```

### Summary Table

| Approach | Time (50M rows) | Relative | When to Use |
|----------|-----------------|----------|-------------|
| Native Spark | 8s | 1x | Always preferred |
| Pandas UDF | 35s | 4.4x | When native can't express logic |
| Python UDF | 180s | 22.5x | Last resort only |
| Scala UDF | 10s | 1.25x | JVM-native complex logic |

---

## Interview Tips

> **Tip 1:** "How would you deploy an ML model for batch scoring in Spark?" — "Use the Iterator Pandas UDF pattern. Load the model once per partition using the Iterator type hint — the model persists across all batches in that partition. Distribute the model via S3/HDFS download cached locally, or broadcast for small models. Control parallelism with repartition to balance between scoring throughput and model loading overhead. For 100M rows with a 500MB model, I'd use 100-200 partitions."

> **Tip 2:** "How do you handle external API calls in Spark UDFs?" — "Three key concerns: rate limiting (track requests per second, sleep when approaching limits), failure handling (try/except with status-based retry logic), and efficiency (LRU cache to avoid redundant calls, connection pooling with requests.Session). Control parallelism with repartition — fewer partitions means fewer concurrent callers. Always use the Iterator pattern to maintain the HTTP session across batches."

> **Tip 3:** "Walk me through your decision process for implementing a complex transformation." — "First, try native functions — even complex logic can often be expressed with nested when/otherwise, regexp_extract, and array higher-order functions. Second, if the logic is inherently procedural but vectorizable, use a Pandas UDF with numpy/pandas operations. Third, if it needs external resources (models, APIs), use Iterator Pandas UDF for resource reuse. Python row-at-a-time UDF is the last resort — I've eliminated them from every production pipeline I've optimized."
