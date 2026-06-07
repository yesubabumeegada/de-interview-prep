---
title: "PySpark UDFs - Intermediate"
topic: pyspark
subtopic: udfs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, udf, pandas-udf, vectorized, grouped-map, type-hints, performance]
---

# PySpark UDFs — Intermediate

## Pandas UDFs (Vectorized UDFs)

Pandas UDFs process data in batches using Apache Arrow for efficient transfer. They're 3-100x faster than row-at-a-time Python UDFs.

```python
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# Series to Series — most common pattern
@F.pandas_udf(DoubleType())
def normalize_amount(amounts: pd.Series) -> pd.Series:
    """Normalize values to 0-1 range within the batch."""
    min_val = amounts.min()
    max_val = amounts.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(amounts))
    return (amounts - min_val) / (max_val - min_val)

df = df.withColumn("normalized", normalize_amount(F.col("amount")))

# Series to Series — string operations
@F.pandas_udf(StringType())
def clean_text(texts: pd.Series) -> pd.Series:
    """Vectorized text cleaning with pandas string methods."""
    return (texts
        .str.lower()
        .str.strip()
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.replace(r'\s+', ' ', regex=True))

df = df.withColumn("clean_name", clean_text(F.col("name")))
```

---

## Pandas UDF Types

### Series to Series (Scalar)

```python
# Input: pd.Series → Output: pd.Series (same length)
@F.pandas_udf(DoubleType())
def haversine_distance(
    lat1: pd.Series, lon1: pd.Series,
    lat2: pd.Series, lon2: pd.Series
) -> pd.Series:
    """Calculate distance between coordinates — vectorized."""
    import numpy as np
    R = 6371  # Earth radius in km
    
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat/2)**2 + 
         np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * 
         np.sin(dlon/2)**2)
    return 2 * R * np.arcsin(np.sqrt(a))

# Multiple column inputs
df = df.withColumn("distance_km",
    haversine_distance("lat1", "lon1", "lat2", "lon2"))
```

### Series to Scalar (Aggregate)

```python
# Input: pd.Series → Output: scalar (used in groupBy.agg)
@F.pandas_udf(DoubleType())
def weighted_avg(values: pd.Series, weights: pd.Series) -> float:
    """Custom weighted average aggregation."""
    return (values * weights).sum() / weights.sum()

# Use as aggregate function
result = df.groupBy("category").agg(
    weighted_avg(F.col("price"), F.col("quantity")).alias("weighted_price")
)
```

### Grouped Map (applyInPandas)

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Process each group as a pandas DataFrame
output_schema = StructType([
    StructField("user_id", StringType()),
    StructField("metric", DoubleType()),
    StructField("z_score", DoubleType()),
    StructField("is_outlier", IntegerType()),
])

def detect_outliers(pdf: pd.DataFrame) -> pd.DataFrame:
    """Z-score outlier detection per user group."""
    mean = pdf["metric"].mean()
    std = pdf["metric"].std()
    
    if std == 0:
        pdf["z_score"] = 0.0
        pdf["is_outlier"] = 0
    else:
        pdf["z_score"] = (pdf["metric"] - mean) / std
        pdf["is_outlier"] = (pdf["z_score"].abs() > 3).astype(int)
    
    return pdf[["user_id", "metric", "z_score", "is_outlier"]]

# Apply per group
result = df.groupBy("user_id").applyInPandas(detect_outliers, schema=output_schema)
```

---

## UDF Performance Comparison

```python
import time
import numpy as np

# Test data: 10 million rows
test_df = (spark.range(10_000_000)
    .withColumn("value", (F.rand() * 100).cast("double"))
    .withColumn("text", F.concat(F.lit("item_"), F.col("id").cast("string")))
)
test_df.cache()
test_df.count()

# Benchmark 1: Row-at-a-time Python UDF
@F.udf(DoubleType())
def python_udf(value):
    import math
    return math.sqrt(value) * 2.5 if value and value > 0 else 0.0

# Benchmark 2: Pandas UDF (vectorized)
@F.pandas_udf(DoubleType())
def pandas_udf(values: pd.Series) -> pd.Series:
    return np.sqrt(values) * 2.5

# Benchmark 3: Native Spark function
native_expr = F.sqrt(F.col("value")) * 2.5

# Run benchmarks
def benchmark(df_expr, name):
    start = time.time()
    test_df.withColumn("result", df_expr).write.mode("overwrite").parquet(f"/tmp/{name}")
    duration = time.time() - start
    return duration

results = {
    "Python UDF": benchmark(python_udf(F.col("value")), "python"),
    "Pandas UDF": benchmark(pandas_udf(F.col("value")), "pandas"),
    "Native": benchmark(native_expr, "native"),
}

for name, duration in results.items():
    print(f"{name}: {duration:.2f}s")
```

### Typical Results (10M rows)

| Approach | Duration | Relative Speed |
|----------|----------|---------------|
| Native Spark | 3s | 1x (baseline) |
| Pandas UDF | 8s | 2.7x slower |
| Python UDF | 45s | 15x slower |

---

## Type Hints (Spark 3.0+)

Type hints provide cleaner syntax and better IDE support:

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# With type hints — return type inferred from annotation
@pandas_udf("double")
def compute_score(
    revenue: pd.Series,
    frequency: pd.Series,
    recency: pd.Series
) -> pd.Series:
    """RFM scoring with vectorized operations."""
    # Normalize each component
    r_score = 1 - (recency / recency.max())
    f_score = frequency / frequency.max()
    m_score = revenue / revenue.max()
    return r_score * 0.3 + f_score * 0.3 + m_score * 0.4

# Iterator of Series — for initialization-heavy UDFs
from typing import Iterator

@pandas_udf("double")
def predict_with_model(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Load model once, predict across all batches."""
    import pickle
    # Model loaded once per executor partition
    model = pickle.load(open("/tmp/model.pkl", "rb"))
    
    for batch in batch_iter:
        yield pd.Series(model.predict(batch.values.reshape(-1, 1)))

# Iterator of multiple Series
from typing import Iterator, Tuple

@pandas_udf("string")
def classify_text(
    batch_iter: Iterator[Tuple[pd.Series, pd.Series]]
) -> Iterator[pd.Series]:
    """Classify text using title and body columns."""
    import transformers
    classifier = transformers.pipeline("text-classification")
    
    for titles, bodies in batch_iter:
        combined = titles + " " + bodies
        predictions = classifier(combined.tolist())
        yield pd.Series([p["label"] for p in predictions])
```

---

## Grouped Map with Pandas — Advanced Patterns

```python
# Time-series forecasting per group
from pyspark.sql.types import *

forecast_schema = StructType([
    StructField("store_id", StringType()),
    StructField("date", StringType()),
    StructField("actual", DoubleType()),
    StructField("forecast", DoubleType()),
])

def forecast_store_sales(pdf: pd.DataFrame) -> pd.DataFrame:
    """Train a model per store and generate forecast."""
    from sklearn.linear_model import LinearRegression
    
    pdf = pdf.sort_values("date")
    pdf["day_num"] = range(len(pdf))
    
    # Simple linear regression
    X = pdf[["day_num"]].values
    y = pdf["actual"].values
    
    model = LinearRegression().fit(X, y)
    pdf["forecast"] = model.predict(X)
    
    return pdf[["store_id", "date", "actual", "forecast"]]

# Apply model per store
forecasts = (sales_df
    .groupBy("store_id")
    .applyInPandas(forecast_store_sales, schema=forecast_schema))
```

---

## Best Practices

```python
# DO: Use pandas string operations instead of Python loops
@F.pandas_udf(StringType())
def good_parse(texts: pd.Series) -> pd.Series:
    return texts.str.extract(r'(\d{3}-\d{4})')[0]

# DON'T: Loop over elements in a Pandas UDF
@F.pandas_udf(StringType())
def bad_parse(texts: pd.Series) -> pd.Series:
    results = []
    for text in texts:  # NEVER loop in a Pandas UDF!
        import re
        match = re.search(r'(\d{3}-\d{4})', text or '')
        results.append(match.group(1) if match else None)
    return pd.Series(results)

# DO: Handle nulls gracefully
@F.pandas_udf(DoubleType())
def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, float('nan'))

# DO: Use numpy for numerical operations
@F.pandas_udf("array<double>")
def rolling_stats(values: pd.Series) -> pd.Series:
    return values.rolling(window=7, min_periods=1).mean()
```

---

## Interview Tips

> **Tip 1:** "What's the difference between a regular UDF and a Pandas UDF?" — "A regular UDF processes one row at a time with full Python-JVM serialization per row. A Pandas UDF processes data in batches using Apache Arrow for efficient columnar transfer. The data arrives as a pandas Series or DataFrame, allowing vectorized operations with numpy and pandas. Pandas UDFs are typically 3-100x faster, with the biggest gains on numerical operations."

> **Tip 2:** "When would you use a grouped map UDF (applyInPandas)?" — "When I need to apply different logic per group and the output schema differs from the input. Classic examples: per-group normalization, training a model per customer segment, time-series forecasting per store, or outlier detection per device. The key requirement is that the operation needs the full group's data at once — not just a single aggregation."

> **Tip 3:** "How do you choose between Python UDF types?" — "Start with native functions — always fastest. If not possible, Pandas UDF Series-to-Series for element-wise transforms. Pandas UDF Series-to-Scalar for custom aggregations in groupBy.agg(). applyInPandas for per-group processing that needs the full group. Row-at-a-time Python UDFs only when you can't vectorize (rare cases with complex stateful logic or external API calls per row)."
