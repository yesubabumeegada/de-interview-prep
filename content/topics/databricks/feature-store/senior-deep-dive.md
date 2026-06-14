---
title: "Databricks Feature Store - Senior Deep Dive"
topic: databricks
subtopic: feature-store
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, feature-store, architecture, feature-engineering, ml-platform]
---

# Databricks Feature Store — Senior Deep Dive

## Feature Store Architecture at Scale

```
┌─────────────────────────────────────────────────────────────┐
│                  Databricks Lakehouse                        │
│                                                              │
│  Raw Data (Bronze)                                           │
│       ↓                                                      │
│  Feature Engineering Jobs (Spark/dbt)                        │
│       ↓                                                      │
│  ┌─────────────────┐    ┌──────────────────────┐            │
│  │  Offline Store  │    │   Online Store        │            │
│  │  (Delta Tables) │───→│  (DynamoDB/CosmosDB) │            │
│  │  - Batch train  │    │  - Real-time serving  │            │
│  │  - Batch score  │    │  - <10ms lookup       │            │
│  └─────────────────┘    └──────────────────────┘            │
│       ↑                          ↑                           │
│  MLflow Training Set      Model Serving Endpoint             │
│  (PIT joins)              (key-value feature fetch)          │
└─────────────────────────────────────────────────────────────┘
```

---

## Feature Governance: Ownership and Discoverability

At scale, feature tables become shared infrastructure — governance is critical:

```python
# Tag features with business metadata
fs.update_feature_table(
    name="ml.user_features",
    description="User purchase behavior features. Owner: risk-team. SLA: updated daily by 6am UTC.",
)

# Use Unity Catalog tags for governance
spark.sql("""
    ALTER TABLE ml.user_features
    SET TAGS ('owner' = 'risk-team',
              'update_sla' = 'daily_6am_utc',
              'sensitivity' = 'PII',
              'approved_uses' = 'fraud_detection,churn_prediction')
""")

# Search for relevant features (Unity Catalog)
spark.sql("""
    SELECT table_name, column_name, comment
    FROM system.information_schema.columns
    WHERE table_schema = 'ml'
      AND lower(comment) LIKE '%purchase%'
""")
```

---

## Feature Pipeline SLAs and Monitoring

```python
# Feature freshness monitoring — critical for model accuracy
def check_feature_freshness(feature_table: str, max_age_hours: int) -> bool:
    from datetime import datetime, timedelta

    latest = spark.sql(f"""
        SELECT MAX(feature_timestamp) AS latest_ts
        FROM {feature_table}
    """).collect()[0]["latest_ts"]

    age_hours = (datetime.now() - latest).total_seconds() / 3600

    if age_hours > max_age_hours:
        # Alert via PagerDuty / Slack
        send_alert(f"STALE: {feature_table} last updated {age_hours:.1f}h ago (SLA: {max_age_hours}h)")
        return False
    return True

# Run in Databricks Workflow as pre-check before model serving
assert check_feature_freshness("ml.user_features", max_age_hours=25), "Feature freshness SLA violated"
```

---

## Cross-Team Feature Sharing

Problem: Team A computed "user lifetime value" features. Team B wants to use them for a different model. Without a feature store, they'd re-implement the same logic.

```python
# Team A publishes features
fs.create_table(
    name="prod.shared_features.user_ltv",
    primary_keys=["user_id"],
    df=ltv_features_df,
    description="User LTV features. Contact: revenue-team@company.com",
    partition_columns=["ltv_date"]
)

# Team B consumes without re-implementing
# Just add a FeatureLookup to their training set
feature_lookups = [
    FeatureLookup(
        table_name="prod.shared_features.user_ltv",
        feature_names=["predicted_ltv_90d", "ltv_segment"],
        lookup_key="user_id"
    )
]
```

**Unity Catalog permissions for cross-team sharing:**
```sql
-- Grant Team B read access to Team A's feature table
GRANT SELECT ON TABLE prod.shared_features.user_ltv TO `team-b-service-principal`;
```

---

## Handling Feature Drift

When feature distributions shift, model predictions degrade even if code is unchanged:

```python
# Profile feature distributions at training time — store as artifact
import mlflow
from ydata_profiling import ProfileReport

with mlflow.start_run():
    training_df = training_set.load_df().toPandas()

    profile = ProfileReport(training_df, title="Training Feature Profile", minimal=True)
    profile.to_file("/tmp/feature_profile.html")
    mlflow.log_artifact("/tmp/feature_profile.html", "feature_profiles")

    # Log distribution statistics for future drift detection
    for col in feature_cols:
        mlflow.log_metric(f"train_mean_{col}", training_df[col].mean())
        mlflow.log_metric(f"train_std_{col}", training_df[col].std())
        mlflow.log_metric(f"train_null_pct_{col}", training_df[col].isnull().mean())

# At serving time: compare inference distribution to training stats
def detect_feature_drift(inference_df, model_run_id: str, threshold: float = 0.3):
    client = MlflowClient()
    run = client.get_run(model_run_id)
    alerts = []

    for col in feature_cols:
        train_mean = run.data.metrics.get(f"train_mean_{col}", 0)
        serve_mean = inference_df[col].mean()

        if train_mean != 0:
            pct_change = abs(serve_mean - train_mean) / abs(train_mean)
            if pct_change > threshold:
                alerts.append(f"{col}: serving mean={serve_mean:.2f} vs training={train_mean:.2f} ({pct_change:.0%} drift)")

    return alerts
```

---

## Design Decision: When NOT to Use the Feature Store

Feature stores add overhead — not every use case warrants them:

| Scenario | Feature Store? | Reason |
|----------|---------------|---------|
| One-off analysis / exploration | ❌ No | Too much setup for throwaway work |
| Shared, reused features across teams | ✅ Yes | Eliminates duplication |
| Real-time serving with <50ms latency requirement | ✅ Yes | Online store lookup |
| Batch-only, one model, single team | ❌ Maybe | Delta join may be simpler |
| Heavy PIT requirements (financial risk models) | ✅ Yes | PIT correctness is critical |
| Simple feature: date parts, basic aggregations | ❌ Maybe | Compute on-the-fly in SQL |

---

## Interview Tips

> **Tip 1:** "How do you handle feature drift in production?" — "Log feature distribution statistics (mean, std, null %) as MLflow metrics at training time. At serving, compute the same stats on the inference batch and compare. If any feature drifts more than X% from training baseline, alert the team. Also configure Lakehouse Monitoring on the inference table — it auto-computes PSI and Jensen-Shannon divergence per feature column."

> **Tip 2:** "How would you architect features for a real-time fraud detection model requiring <50ms latency?" — "Offline Delta tables can't serve in 50ms. Publish features to an online store (DynamoDB/CosmosDB) using `fs.publish_table`. The feature engineering pipeline writes to Delta (offline), which syncs to the online store via the feature store. At serving, the model endpoint fetches from DynamoDB with single-digit ms latency using the entity key."

> **Tip 3:** "What happens if a feature pipeline fails and features are stale?" — "Model accuracy degrades silently — it receives stale values but doesn't know they're old. Defend with: (1) Freshness monitoring that checks MAX(feature_timestamp) before each serving window. (2) Circuit breaker that falls back to default values or blocks predictions if freshness SLA is violated. (3) Feature table SLA in the description so consumers know expected freshness."
