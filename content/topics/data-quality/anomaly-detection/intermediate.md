---
title: "Anomaly Detection — Intermediate"
topic: data-quality
subtopic: anomaly-detection
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [anomaly-detection, isolation-forest, prophet, monte-carlo, elementary]
---

# Anomaly Detection — Intermediate

## Time-Series Anomaly Detection with Rolling Statistics

For daily metrics, a rolling window baseline is more accurate than an all-time average:

```python
import pandas as pd
import numpy as np

def rolling_anomaly_detector(
    df: pd.DataFrame,
    metric_col: str,
    date_col: str,
    window_days: int = 28,
    z_threshold: float = 3.0,
    seasonality: bool = True,
) -> pd.DataFrame:
    """
    Detect anomalies in a time-series metric using rolling statistics.
    If seasonality=True, compares to same day-of-week in rolling window.
    """
    df = df.sort_values(date_col).copy()
    df["dow"] = pd.to_datetime(df[date_col]).dt.dayofweek
    
    if seasonality:
        # Compare to same day-of-week
        df["rolling_mean"] = df.groupby("dow")[metric_col].transform(
            lambda x: x.rolling(window=window_days // 7, min_periods=3).mean().shift(1)
        )
        df["rolling_std"] = df.groupby("dow")[metric_col].transform(
            lambda x: x.rolling(window=window_days // 7, min_periods=3).std().shift(1)
        )
    else:
        df["rolling_mean"] = df[metric_col].rolling(window=window_days, min_periods=7).mean().shift(1)
        df["rolling_std"] = df[metric_col].rolling(window=window_days, min_periods=7).std().shift(1)
    
    df["z_score"] = (df[metric_col] - df["rolling_mean"]) / df["rolling_std"].replace(0, np.nan)
    df["is_anomaly"] = df["z_score"].abs() > z_threshold
    df["pct_change_vs_baseline"] = (df[metric_col] - df["rolling_mean"]) / df["rolling_mean"] * 100
    
    return df

# Usage
daily_orders = pd.DataFrame({
    "date": pd.date_range("2024-01-01", periods=90),
    "order_count": np.random.normal(10000, 500, 90).astype(int)
})
# Inject anomaly on day 60
daily_orders.loc[60, "order_count"] = 2000

result = rolling_anomaly_detector(daily_orders, "order_count", "date", seasonality=True)
anomalies = result[result["is_anomaly"]]
print(anomalies[["date", "order_count", "rolling_mean", "z_score"]])
```

---

## Isolation Forest — ML-Based Anomaly Detection

For multi-dimensional anomaly detection (e.g., multiple metrics at once):

```python
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np

def train_isolation_forest(
    historical_df: pd.DataFrame,
    feature_cols: list[str],
    contamination: float = 0.01,  # Expected 1% anomaly rate
) -> tuple:
    scaler = StandardScaler()
    X = scaler.fit_transform(historical_df[feature_cols].dropna())
    
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42,
    )
    model.fit(X)
    
    return model, scaler

def detect_anomalies(
    df: pd.DataFrame,
    feature_cols: list[str],
    model: IsolationForest,
    scaler: StandardScaler,
) -> pd.DataFrame:
    X = scaler.transform(df[feature_cols].fillna(0))
    df = df.copy()
    df["anomaly_score"] = model.decision_function(X)  # Negative = more anomalous
    df["is_anomaly"] = model.predict(X) == -1          # -1 = anomaly
    return df

# Usage: detect anomalous batches using multiple metrics
historical_metrics = pd.DataFrame({
    "row_count": np.random.normal(100000, 5000, 365),
    "null_pct": np.random.uniform(0, 0.02, 365),
    "mean_amount": np.random.normal(50, 5, 365),
    "p99_amount": np.random.normal(500, 50, 365),
})

model, scaler = train_isolation_forest(
    historical_metrics,
    feature_cols=["row_count", "null_pct", "mean_amount", "p99_amount"],
)

# Check today's batch
today_metrics = pd.DataFrame({
    "row_count": [20000],    # Suspicious drop
    "null_pct": [0.01],
    "mean_amount": [51],
    "p99_amount": [510],
})

result = detect_anomalies(today_metrics, ["row_count", "null_pct", "mean_amount", "p99_amount"], model, scaler)
print(f"Anomaly detected: {result['is_anomaly'].iloc[0]}")
print(f"Anomaly score: {result['anomaly_score'].iloc[0]:.3f}")
```

---

## Elementary — dbt-Native Anomaly Detection

Elementary is a dbt package that adds anomaly detection directly in your dbt models:

```yaml
# models/schema.yml
models:
  - name: orders
    tests:
      - elementary.volume_anomalies:
          timestamp_column: order_date
          time_bucket:
            period: day
          anomaly_sensitivity: 3    # Z-score threshold
          days_back: 14             # Training window
          
      - elementary.freshness_anomalies:
          timestamp_column: order_date
          max_loaded_at_delay_allowed_in_hours: 2
    
    columns:
      - name: amount
        tests:
          - elementary.column_anomalies:
              column_anomalies:
                - null_count
                - null_percent
                - average
                - min
                - max
              timestamp_column: order_date
              time_bucket:
                period: day
              anomaly_sensitivity: 3
              days_back: 14
```

```bash
# Run elementary
dbt test --select elementary
# View results
edr monitor  # Opens web UI with anomaly dashboard
```

---

## Null Rate Spike Detection

A sudden increase in NULL rates is one of the most common DQ issues:

```python
class NullRateMonitor:
    """Track null rates over time and detect spikes."""
    
    def __init__(self, baseline_df: pd.DataFrame):
        self.baselines = {}
        for col in baseline_df.columns:
            values = baseline_df[col].values
            self.baselines[col] = {
                "mean": float(np.isnan(values.astype(float)).mean() if baseline_df[col].dtype == float else baseline_df[col].isna().mean()),
                "std": float(baseline_df.groupby(pd.Grouper(freq="D"))[col].apply(lambda x: x.isna().mean()).std()),
            }
    
    def check(self, new_df: pd.DataFrame, z_threshold: float = 3.0) -> list[dict]:
        alerts = []
        for col in new_df.columns:
            if col not in self.baselines:
                continue
            
            current_null_rate = new_df[col].isna().mean()
            baseline = self.baselines[col]
            
            if baseline["std"] == 0:
                if current_null_rate != baseline["mean"]:
                    alerts.append({"column": col, "current": current_null_rate, "baseline": baseline["mean"]})
                continue
            
            z = abs(current_null_rate - baseline["mean"]) / baseline["std"]
            if z > z_threshold:
                alerts.append({
                    "column": col,
                    "current_null_pct": round(current_null_rate * 100, 2),
                    "baseline_null_pct": round(baseline["mean"] * 100, 2),
                    "z_score": round(z, 2),
                })
        
        return alerts
```

---

## Interview Tips

> **Tip 1:** "How do you handle seasonality in anomaly detection?" — Segment by day-of-week (Mondays have different volumes than Sundays). Use same-day-of-week rolling averages. For hourly data, segment by hour. Some tools (Prophet, Greykite) model seasonality explicitly.

> **Tip 2:** "What's contamination in Isolation Forest?" — The expected fraction of anomalies in training data. If you set `contamination=0.01`, the model assumes 1% of historical data were anomalies. Setting it too high increases false positives; too low misses real anomalies.

> **Tip 3:** "What's the difference between Elementary and Monte Carlo?" — Elementary is open-source, dbt-native, runs in your warehouse. Monte Carlo is a commercial data observability platform with automated anomaly detection across your whole stack (no config needed). Elementary requires you to define what to monitor; Monte Carlo learns from your data automatically.
