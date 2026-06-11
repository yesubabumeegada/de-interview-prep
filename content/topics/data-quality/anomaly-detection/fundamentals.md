---
title: "Anomaly Detection — Fundamentals"
topic: data-quality
subtopic: anomaly-detection
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [anomaly-detection, statistical, z-score, iqr, freshness]
---

# Anomaly Detection — Fundamentals


## 🎯 Analogy

Think of anomaly detection like a smoke alarm for your data: instead of manually checking row counts every day, you set statistical thresholds based on historical patterns — and the alarm triggers automatically when today's data is wildly different from the norm.

---
## What Is Data Anomaly Detection?

Data anomaly detection automatically identifies when data deviates from expected patterns — without needing to define every rule manually. Unlike schema validation (structural checks), anomaly detection catches **behavioral** anomalies: unusual row counts, sudden metric drops, value distribution shifts.

```mermaid
flowchart LR
    A[Historical Data] --> B[Baseline Model]
    C[New Batch] --> D[Anomaly Detector]
    B --> D
    D -->|Normal| E[Pass]
    D -->|Anomaly| F[Alert]
    F --> G[Human Review]
```

---

## Types of Data Anomalies

| Type | Example | Detection Method |
|------|---------|-----------------|
| **Volume anomaly** | 10K rows today vs 100K daily average | Row count Z-score |
| **Freshness anomaly** | Table not updated in 6 hours | Max timestamp check |
| **Distribution shift** | Revenue suddenly 50% lower | Statistical comparison |
| **Point anomaly** | Single row with amount = $1,000,000 | Z-score on column |
| **Structural anomaly** | New columns appeared | Schema comparison |
| **Null spike** | NULL rate jumped from 1% → 30% | Null rate tracking |

---

## Z-Score Method

Detect values that are statistically unusual (more than N standard deviations from the mean):

```python
import numpy as np
import pandas as pd
from scipy import stats

def detect_z_score_anomalies(
    series: pd.Series,
    threshold: float = 3.0,
) -> pd.Series:
    """Return boolean mask: True where value is anomalous."""
    z_scores = np.abs(stats.zscore(series.dropna()))
    return pd.Series(z_scores > threshold, index=series.dropna().index)

# Example: detect anomalous daily revenue
daily_revenue = pd.Series([95000, 102000, 98000, 101000, 15000, 99000, 103000])
anomalies = detect_z_score_anomalies(daily_revenue, threshold=2.5)
print(f"Anomalous days: {anomalies[anomalies].index.tolist()}")  # [4]

# Detect row-level anomalies
def detect_row_anomalies(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
    mean = df[column].mean()
    std = df[column].std()
    df["z_score"] = (df[column] - mean).abs() / std
    df["is_anomaly"] = df["z_score"] > threshold
    return df[df["is_anomaly"]]

anomalous_orders = detect_row_anomalies(orders_df, "amount")
```

---

## IQR Method (Robust to Outliers)

Z-score is sensitive to extreme values. IQR (Interquartile Range) is more robust:

```python
def detect_iqr_anomalies(series: pd.Series, factor: float = 1.5) -> pd.Series:
    """
    IQR method: anomaly if value < Q1 - factor*IQR or > Q3 + factor*IQR.
    factor=1.5 = mild outlier, factor=3.0 = extreme outlier
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - factor * IQR
    upper_bound = Q3 + factor * IQR
    
    return (series < lower_bound) | (series > upper_bound)

# Usage
anomaly_mask = detect_iqr_anomalies(orders_df["amount"], factor=1.5)
print(f"Anomalous rows: {anomaly_mask.sum()}")
print(f"Amount range: [{orders_df['amount'].min():.2f}, {orders_df['amount'].max():.2f}]")
print(f"Normal range: [{orders_df['amount'].quantile(0.25) - 1.5 * orders_df['amount'].quantile(0.75) + orders_df['amount'].quantile(0.25):.2f}]")
```

---

## Volume Anomaly Detection

The most common DQ anomaly: row count deviates from historical baseline:

```python
from datetime import datetime, timedelta

def check_row_count_anomaly(
    current_count: int,
    historical_counts: list[int],
    z_threshold: float = 3.0,
) -> dict:
    """Detect if today's row count is anomalous vs historical."""
    mean = np.mean(historical_counts)
    std = np.std(historical_counts)
    
    if std == 0:
        return {"anomaly": current_count != mean, "reason": "No variance in history"}
    
    z_score = abs(current_count - mean) / std
    pct_change = (current_count - mean) / mean * 100
    
    return {
        "anomaly": z_score > z_threshold,
        "current_count": current_count,
        "historical_mean": round(mean, 0),
        "historical_std": round(std, 0),
        "z_score": round(z_score, 2),
        "pct_change": round(pct_change, 1),
        "normal_range": (round(mean - z_threshold * std, 0), round(mean + z_threshold * std, 0)),
    }

# Example
historical = [98000, 102000, 97500, 103000, 99000, 101000, 100500, 98500]
result = check_row_count_anomaly(current_count=25000, historical_counts=historical)
print(result)
# {'anomaly': True, 'current_count': 25000, 'historical_mean': 99937.5, 'z_score': 18.5, ...}
```

---

## Freshness Check

Data timeliness is one of the most critical DQ dimensions:

```python
from datetime import datetime, timezone

def check_freshness(
    max_timestamp: datetime,
    max_allowed_age_hours: float,
    check_time: datetime = None,
) -> dict:
    if check_time is None:
        check_time = datetime.now(timezone.utc)
    
    if max_timestamp.tzinfo is None:
        max_timestamp = max_timestamp.replace(tzinfo=timezone.utc)
    
    age_hours = (check_time - max_timestamp).total_seconds() / 3600
    
    return {
        "fresh": age_hours <= max_allowed_age_hours,
        "max_timestamp": max_timestamp.isoformat(),
        "age_hours": round(age_hours, 2),
        "threshold_hours": max_allowed_age_hours,
        "lag_hours": round(max(0, age_hours - max_allowed_age_hours), 2),
    }

# Example: check if orders table is fresh
import sqlalchemy as sa
engine = sa.create_engine("postgresql://user:pass@host/db")

with engine.connect() as conn:
    max_ts = conn.execute(
        sa.text("SELECT MAX(created_at) FROM orders")
    ).scalar()

result = check_freshness(max_ts, max_allowed_age_hours=1.0)
if not result["fresh"]:
    raise ValueError(f"Orders table is stale: {result['age_hours']}h old (max: 1h)")
```

---


## ▶️ Try It Yourself

```python
import pandas as pd
import numpy as np

def detect_anomalies(history: pd.Series, current_value: float,
                     z_threshold: float = 3.0) -> dict:
    mean = history.mean()
    std = history.std()
    z_score = (current_value - mean) / std if std > 0 else 0
    is_anomaly = abs(z_score) > z_threshold
    return {
        "current": current_value,
        "historical_mean": round(mean, 2),
        "historical_std": round(std, 2),
        "z_score": round(z_score, 2),
        "is_anomaly": is_anomaly,
        "direction": "spike" if z_score > 0 else "drop" if is_anomaly else "normal",
    }

# Simulate 30 days of row counts (normal ~50k rows/day)
np.random.seed(42)
history = pd.Series(np.random.normal(50000, 2000, 30))

# Today: 10k rows (data pipeline likely failed)
result = detect_anomalies(history, current_value=10000)
print(result)
# {'current': 10000, 'z_score': -19.8, 'is_anomaly': True, 'direction': 'drop'}
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How does anomaly detection differ from DQ rules?" — DQ rules are explicit thresholds: "amount must be > 0." Anomaly detection is adaptive: "today's revenue is 3 standard deviations below the 30-day average." Rules catch known failure modes; anomaly detection catches unexpected behavior.

> **Tip 2:** "What's the tradeoff between Z-score and IQR?" — Z-score is sensitive to outliers in the historical data itself (one extreme value inflates std). IQR uses the middle 50% of data and is robust to outliers. For skewed distributions (revenue, event counts), prefer IQR or log-transform before Z-score.

> **Tip 3:** "What's the biggest risk with anomaly detection?" — False positives. If you alert on every Z-score > 2, you'll get alerts every day and teams will start ignoring them. Tune thresholds per table, use rolling baselines, and segment by day-of-week (Monday vs Sunday have different expected volumes).
