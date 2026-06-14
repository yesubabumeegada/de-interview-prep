---
title: "Databricks Feature Store - Real-World Examples"
topic: databricks
subtopic: feature-store
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, feature-store, production, real-time, recommendation, fraud]
---

# Databricks Feature Store — Real-World Production Examples

## Production Pattern: Real-Time Recommendation Features

An e-commerce company serves 50K product recommendation requests/second with sub-20ms feature latency:

```python
# Offline pipeline: compute user and product features (runs every 15 min)
def compute_user_engagement_features(events_df):
    return events_df \
        .filter(F.col("event_time") >= F.date_sub(F.current_date(), 30)) \
        .groupBy("user_id") \
        .agg(
            F.count(F.when(F.col("event_type") == "view", 1)).alias("views_30d"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases_30d"),
            F.sum("session_duration_s").alias("total_session_time_30d"),
            F.countDistinct("category").alias("distinct_categories_browsed"),
            F.datediff(F.current_date(), F.max("event_time")).alias("days_since_active")
        ) \
        .withColumn("feature_timestamp", F.current_timestamp())

user_features_df = compute_user_engagement_features(events)
fs.write_table(name="prod.features.user_engagement", df=user_features_df, mode="merge")

# Publish to DynamoDB for real-time serving (runs after each write)
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec
fs.publish_table(
    name="prod.features.user_engagement",
    online_store=AmazonDynamoDBSpec(region="us-east-1", table_name="user-engagement-features"),
    mode="merge"
)
```

**Serving path (model endpoint):**
```python
# At inference time: just pass user_id → feature store fetches from DynamoDB
predictions = fs.score_batch(
    model_uri="models:/product-recommender/Production",
    df=request_df[["user_id", "session_context"]]
)
# Latency: ~8ms (DynamoDB lookup) + ~4ms (model inference) = 12ms total
```

---

## Production Pattern: Fraud Detection with PIT Features

A payments company builds a fraud model requiring point-in-time correct features:

**The challenge:** User's transaction count at fraud time was 3. If we join current count (250 after 18 months), the model sees a very different user profile.

```python
# Feature table with timestamp column for PIT lookups
fraud_labels = spark.sql("""
    SELECT
        transaction_id,
        user_id,
        transaction_timestamp,  -- this is the "as of" timestamp
        is_fraud
    FROM prod.fraud.labeled_transactions
    WHERE transaction_timestamp >= '2023-01-01'
""")

# PIT lookup: get feature values as of transaction_timestamp
feature_lookups = [
    FeatureLookup(
        table_name="prod.features.user_transaction_history",
        feature_names=["txn_count_7d", "txn_count_30d", "avg_txn_amount_30d",
                       "distinct_merchants_7d", "international_txn_pct_30d"],
        lookup_key="user_id",
        timestamp_lookup_key="transaction_timestamp"  # ← PIT correctness
    ),
    FeatureLookup(
        table_name="prod.features.device_risk_signals",
        feature_names=["device_age_days", "ip_country_mismatch", "new_device_flag"],
        lookup_key="device_fingerprint",
        timestamp_lookup_key="transaction_timestamp"
    )
]

training_set = fs.create_training_set(
    df=fraud_labels,
    feature_lookups=feature_lookups,
    label="is_fraud"
)

# Result: 23% improvement in model AUC vs non-PIT training
# No data leakage from future transaction history
```

---

## Production Pattern: Feature Sharing Across 5 Teams

A company has 5 ML teams, each building separate models. Without a feature store, "user segment" was computed 5 different ways — inconsistently.

**Before feature store:**
- Team A: `CASE WHEN orders > 10 THEN 'high_value' ...` 
- Team B: `CASE WHEN lifetime_spend > 1000 THEN 'high_value' ...`
- Team C: bought a third-party segmentation — different results again

**After feature store:**
```python
# Data platform team owns and maintains canonical feature tables
fs.create_table(
    name="prod.shared.user_segments",
    primary_keys=["user_id"],
    df=canonical_segments_df,
    description="""
    Canonical user segmentation. Updated daily at 3am UTC.
    Owner: data-platform@company.com
    Segments: high_value (LTV > $500 OR orders > 20), active, at_risk, churned.
    Do NOT reimplement — use this table.
    """
)

# All 5 teams consume the same table
feature_lookups = [
    FeatureLookup(
        table_name="prod.shared.user_segments",
        feature_names=["user_segment", "ltv_predicted", "churn_risk_score"],
        lookup_key="user_id"
    )
]
```

**Outcome:** "High value" means the same thing in all 5 models. Segment changes propagate automatically to all models on next retraining.

---

## Production Incident: Training-Serving Skew

A recommendation model's click-through rate dropped 18% post-deployment with no code changes.

**Root cause:** The feature engineering notebook computed features on Pacific Time. The serving pipeline computed the same feature on UTC. A "days_since_last_order" feature computed at 11pm Pacific = 1 day, at 11pm UTC = 2 days — systematic bias.

**Resolution:**
```python
# Fix: always use UTC timestamps explicitly in feature computation
def compute_recency_features(orders_df):
    return orders_df.withColumn(
        "days_since_last_order",
        F.datediff(
            F.to_utc_timestamp(F.current_timestamp(), "UTC"),  # explicit UTC
            F.to_utc_timestamp(F.col("order_timestamp"), "UTC")
        )
    )

# Better fix: use the feature store — same code runs for training and serving
# Training reads features as computed; serving reads the same row
# Timezone inconsistency can only exist in the feature computation job — isolated to one place
```

**Lesson:** Feature stores don't eliminate bugs in feature computation, but they isolate the bug to one place instead of two code paths (training vs serving). Fix it once; both paths are correct.

---

## Monitoring Feature Pipeline Health

```python
# Databricks Workflow with feature health checks
checks = {
    "prod.features.user_engagement": {"max_age_hours": 1, "min_rows": 1_000_000},
    "prod.features.user_transaction_history": {"max_age_hours": 24, "min_rows": 500_000},
    "prod.shared.user_segments": {"max_age_hours": 25, "min_rows": 800_000},
}

for table, sla in checks.items():
    result = spark.sql(f"""
        SELECT
            MAX(feature_timestamp) AS latest,
            COUNT(*) AS row_count
        FROM {table}
    """).collect()[0]

    age_hours = (datetime.now() - result["latest"]).total_seconds() / 3600
    assert age_hours <= sla["max_age_hours"], f"STALE: {table} is {age_hours:.1f}h old"
    assert result["row_count"] >= sla["min_rows"], f"LOW ROW COUNT: {table} has {result['row_count']} rows"

print("All feature tables healthy")
```
