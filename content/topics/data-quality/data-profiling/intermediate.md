---
title: "Data Profiling — Intermediate"
topic: data-quality
subtopic: data-profiling
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-profiling, spark, incremental, correlation, warehouse]
---

# Data Profiling — Intermediate

## Profiling at Scale — PySpark

For datasets that don't fit in memory, profile with Spark:

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType
from typing import List

def spark_profile(df: DataFrame, sample_frac: float = 1.0) -> dict:
    """Profile a large Spark DataFrame."""
    
    if sample_frac < 1.0:
        df = df.sample(sample_frac)
    
    total_rows = df.count()
    
    # Aggregate all columns in a single pass
    agg_exprs = []
    for field in df.schema.fields:
        col = field.name
        agg_exprs.extend([
            F.count(F.when(F.col(col).isNull(), 1)).alias(f"{col}__null_count"),
            F.approx_count_distinct(col).alias(f"{col}__approx_distinct"),
        ])
        
        if isinstance(field.dataType, NumericType):
            agg_exprs.extend([
                F.min(col).alias(f"{col}__min"),
                F.max(col).alias(f"{col}__max"),
                F.avg(col).alias(f"{col}__mean"),
                F.stddev(col).alias(f"{col}__std"),
                F.percentile_approx(col, [0.25, 0.5, 0.75, 0.99]).alias(f"{col}__percentiles"),
            ])
    
    stats_row = df.agg(*agg_exprs).collect()[0]
    
    profile = {"total_rows": total_rows, "columns": {}}
    
    for field in df.schema.fields:
        col = field.name
        null_count = stats_row[f"{col}__null_count"]
        distinct = stats_row[f"{col}__approx_distinct"]
        
        col_profile = {
            "dtype": str(field.dataType),
            "null_count": null_count,
            "null_pct": round(null_count / total_rows * 100, 2) if total_rows > 0 else 0,
            "approx_distinct": distinct,
            "cardinality_pct": round(distinct / total_rows * 100, 2) if total_rows > 0 else 0,
        }
        
        if isinstance(field.dataType, NumericType):
            percs = stats_row[f"{col}__percentiles"]
            col_profile.update({
                "min": stats_row[f"{col}__min"],
                "max": stats_row[f"{col}__max"],
                "mean": round(stats_row[f"{col}__mean"] or 0, 4),
                "std": round(stats_row[f"{col}__std"] or 0, 4),
                "p25": percs[0] if percs else None,
                "p50": percs[1] if percs else None,
                "p75": percs[2] if percs else None,
                "p99": percs[3] if percs else None,
            })
        
        profile["columns"][col] = col_profile
    
    return profile


spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3://bucket/orders/")
profile = spark_profile(df, sample_frac=0.1)  # 10% sample for speed
```

---

## Incremental Profiling — Track Changes Over Time

Profile each daily batch and store results to detect drift:

```python
import pandas as pd
from datetime import date
import json

def profile_and_store(df: pd.DataFrame, table_name: str, run_date: date):
    """Profile today's batch and store metrics for trending."""
    
    metrics = []
    total_rows = len(df)
    
    for col in df.columns:
        series = df[col]
        row = {
            "table_name": table_name,
            "column_name": col,
            "run_date": run_date.isoformat(),
            "row_count": total_rows,
            "null_count": int(series.isna().sum()),
            "null_pct": round(series.isna().mean() * 100, 4),
            "unique_count": int(series.nunique()),
        }
        
        if pd.api.types.is_numeric_dtype(series):
            row.update({
                "min_val": float(series.min()) if not series.isna().all() else None,
                "max_val": float(series.max()) if not series.isna().all() else None,
                "mean_val": float(series.mean()) if not series.isna().all() else None,
                "std_val": float(series.std()) if not series.isna().all() else None,
                "p50": float(series.quantile(0.5)) if not series.isna().all() else None,
                "p99": float(series.quantile(0.99)) if not series.isna().all() else None,
            })
        
        metrics.append(row)
    
    # Append to profiling history table
    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_parquet(
        f"s3://dq-store/profiles/{table_name}/dt={run_date}/",
        index=False
    )
    
    return metrics_df

# Detect drift: compare today's profile to 30-day baseline
def detect_profile_drift(table_name: str, column: str, metric: str = "null_pct"):
    history = pd.read_parquet(f"s3://dq-store/profiles/{table_name}/")
    
    recent = history[history["column_name"] == column].sort_values("run_date")
    baseline = recent.iloc[:-1][metric].values  # All except today
    today = recent.iloc[-1][metric]
    
    if len(baseline) < 7:
        return {"drift_detected": False, "reason": "insufficient history"}
    
    import numpy as np
    mean, std = np.mean(baseline), np.std(baseline)
    z = abs(today - mean) / max(std, 0.0001)
    
    return {
        "column": column,
        "metric": metric,
        "today": round(today, 4),
        "baseline_mean": round(mean, 4),
        "z_score": round(z, 2),
        "drift_detected": z > 3.0,
    }
```

---

## Correlation Analysis

Profiling relationships between columns:

```python
import pandas as pd
import numpy as np

def profile_correlations(df: pd.DataFrame, threshold: float = 0.8) -> list[dict]:
    """Find highly correlated column pairs."""
    
    numeric_df = df.select_dtypes(include=np.number)
    corr_matrix = numeric_df.corr()
    
    high_correlations = []
    cols = corr_matrix.columns
    
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            corr = corr_matrix.iloc[i, j]
            if abs(corr) >= threshold:
                high_correlations.append({
                    "col1": cols[i],
                    "col2": cols[j],
                    "correlation": round(corr, 4),
                    "type": "positive" if corr > 0 else "negative",
                    "strength": "perfect" if abs(corr) > 0.99 else "strong",
                })
    
    return sorted(high_correlations, key=lambda x: abs(x["correlation"]), reverse=True)

correlations = profile_correlations(orders_df, threshold=0.7)
for c in correlations:
    print(f"{c['col1']} ↔ {c['col2']}: {c['correlation']}")
```

---

## SQL-Based Profiling (Warehouse-Native)

```sql
-- Profile all columns in one query (works in Snowflake, BigQuery, Redshift)
SELECT
    'orders' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    AVG(amount) AS mean_amount,
    STDDEV(amount) AS std_amount,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) AS median_amount,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) AS p99_amount,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS non_positive_amounts,
    
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order,
    COUNT(DISTINCT DATE_TRUNC('day', order_date)) AS distinct_days,
    
    SUM(CASE WHEN status NOT IN ('pending','shipped','delivered','cancelled') THEN 1 ELSE 0 END) AS invalid_status
FROM orders;
```

---

## Interview Tips

> **Tip 1:** "How do you profile a 10TB dataset?" — Sample first (10-20% for profiling is usually sufficient), use Spark for distributed computation, and aggregate all column stats in a single pass. Never collect the full DataFrame to the driver.

> **Tip 2:** "What do you look for first in a profiling report?" — Null rates (completeness), then cardinality (is the PK actually unique?), then ranges (are there negative prices?), then top values (are there unexpected categories?). These four cover 90% of DQ issues.

> **Tip 3:** "How does incremental profiling differ from one-time profiling?" — One-time profiling gives a snapshot. Incremental profiling runs on each batch and stores results, enabling trend detection (null rate increasing over time) and anomaly detection without hardcoded thresholds.
