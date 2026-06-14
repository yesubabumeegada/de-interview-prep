---
title: "Databricks Feature Store - Scenario Questions"
topic: databricks
subtopic: feature-store
content_type: scenario_question
tags: [databricks, feature-store, scenarios, interview, point-in-time]
---

# Scenario Questions — Databricks Feature Store

<article data-difficulty="junior">

## 🟢 Junior: Build User Features for a Churn Model

**Scenario:** You're building a customer churn prediction model. You have a `purchases` table with `user_id`, `purchase_date`, `amount`. Create a feature table with three features: total purchase count, total spend, and days since last purchase — then use it to train a model.

<details>
<summary>✅ Solution</summary>

```python
from databricks.feature_store import FeatureStoreClient, FeatureLookup
import pyspark.sql.functions as F
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

fs = FeatureStoreClient()

# Step 1: Compute features
purchases_df = spark.table("prod.sales.purchases")

user_features = purchases_df.groupBy("user_id").agg(
    F.count("*").alias("total_purchases"),
    F.sum("amount").alias("total_spend"),
    F.datediff(F.current_date(), F.max("purchase_date")).alias("days_since_last_purchase")
)

# Step 2: Create feature table
fs.create_table(
    name="ml.churn.user_purchase_features",
    primary_keys=["user_id"],
    df=user_features,
    description="User purchase behavior features for churn prediction"
)

# Step 3: Write features
fs.write_table(name="ml.churn.user_purchase_features", df=user_features, mode="overwrite")

# Step 4: Create training set
labels_df = spark.table("ml.churn.labels")  # user_id, churned (0/1)

training_set = fs.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name="ml.churn.user_purchase_features",
            feature_names=["total_purchases", "total_spend", "days_since_last_purchase"],
            lookup_key="user_id"
        )
    ],
    label="churned"
)

training_df = training_set.load_df().toPandas()
X = training_df.drop(columns=["user_id", "churned"])
y = training_df["churned"]

# Step 5: Train and log via feature store
with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X, y)

    fs.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name="churn-classifier"
    )
```

**Why `fs.log_model` and not `mlflow.log_model`:** Feature store records the feature lineage so that `fs.score_batch` knows to look up the features from the feature table at serving time — you only need to pass `user_id`.

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Fix Training-Serving Skew

**Scenario:** Your team's click-through rate model was deployed last month. CTR dropped 15% post-deployment vs. offline evaluation metrics. You suspect training-serving skew. The model uses a feature `avg_session_duration_7d`. Diagnose the skew and explain how the feature store prevents it going forward.

<details>
<summary>✅ Solution</summary>

**Diagnosing the skew:**

```python
# Step 1: Compare training feature distributions vs serving distributions

# Training distribution (stored as MLflow metric artifacts)
client = MlflowClient()
run = client.get_run("TRAINING_RUN_ID")
train_mean = run.data.metrics.get("train_mean_avg_session_duration_7d")
train_std = run.data.metrics.get("train_std_avg_session_duration_7d")

# Serving distribution (sample from inference logs)
serving_stats = spark.sql("""
    SELECT
        AVG(avg_session_duration_7d) AS serve_mean,
        STDDEV(avg_session_duration_7d) AS serve_std,
        COUNT(*) AS sample_count
    FROM prod.ml.ctr_inference_logs
    WHERE inference_timestamp >= DATEADD(day, -7, current_timestamp())
""").collect()[0]

print(f"Training: mean={train_mean:.2f}, std={train_std:.2f}")
print(f"Serving:  mean={serving_stats['serve_mean']:.2f}, std={serving_stats['serve_std']:.2f}")
# Output: Training: mean=245.3, std=89.1
#         Serving:  mean=312.8, std=102.4  ← 27% higher mean
```

**Finding the root cause:**
```python
# Check how the feature is computed in training vs serving
# Training notebook:
# avg_session_duration_7d = sessions.filter(days=7).groupby(user_id).agg(mean(duration_seconds))

# Serving pipeline (found the bug):
# avg_session_duration_7d = sessions.filter(days=7).groupby(user_id).agg(mean(duration_minutes))
# ← different unit! seconds vs minutes
```

**Fixing with Feature Store (compute once, use everywhere):**

```python
# Single feature computation — used for both training and serving
def compute_session_features(sessions_df):
    return sessions_df \
        .filter(F.col("session_date") >= F.date_sub(F.current_date(), 7)) \
        .groupBy("user_id") \
        .agg(
            # Explicit unit, documented in column name
            F.avg(F.col("duration_seconds")).alias("avg_session_duration_7d_seconds")
        ) \
        .withColumn("feature_timestamp", F.current_timestamp())

# Write to feature store once
fs.write_table(name="ml.ctr.session_features",
               df=compute_session_features(sessions), mode="merge")

# Training reads from feature store
# Serving reads from the same feature store via fs.score_batch
# Same code path → no skew possible
```

**Key insight:** The feature store doesn't prevent bugs in the feature computation itself, but it ensures the same computation is used for both training and serving — reducing skew surface from two code paths to one.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design Features for a Real-Time Fraud Detection System

**Scenario:** You're designing the feature engineering layer for a real-time fraud detection system. Requirements: (1) predict fraud within 200ms of transaction, (2) features must be accurate as of the transaction time (no future leakage), (3) features include user 24h transaction velocity, device history, and merchant risk score. Design the feature store architecture.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
Transaction event (Kafka)
         │
         ├── Streaming pipeline → Feature Store (offline Delta)
         │   (compute 24h velocity, device signals)          │
         │                                                   │
         │                                              Publish job
         │                                                   │
         │                                    Feature Store (online: DynamoDB)
         │                                                   │
         └── Model Serving Endpoint ←── Feature lookup (DynamoDB, ~5ms)
             (predict in <200ms total)
```

**1. Offline feature tables (Delta):**

```python
# User transaction velocity — streaming computation
velocity_features = (
    kafka_stream
    .withWatermark("transaction_time", "5 minutes")
    .groupBy(F.window("transaction_time", "24 hours", "5 minutes"), "user_id")
    .agg(
        F.count("*").alias("txn_count_24h"),
        F.sum("amount").alias("txn_volume_24h"),
        F.countDistinct("merchant_id").alias("distinct_merchants_24h"),
        F.max("amount").alias("max_single_txn_24h")
    )
    .select("user_id", "txn_count_24h", "txn_volume_24h",
            "distinct_merchants_24h", "max_single_txn_24h",
            F.col("window.end").alias("feature_timestamp"))
)

fs.write_table("prod.fraud.user_velocity", velocity_features, mode="merge", streaming=True)

# Merchant risk scores — batch, updated daily
merchant_risk = compute_merchant_risk(historical_fraud_df)
fs.write_table("prod.fraud.merchant_risk", merchant_risk, mode="merge")
```

**2. Online store publication (for sub-200ms serving):**

```python
# Publish velocity features to DynamoDB every 5 minutes
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec

for table in ["prod.fraud.user_velocity", "prod.fraud.merchant_risk", "prod.fraud.device_history"]:
    fs.publish_table(
        name=table,
        online_store=AmazonDynamoDBSpec(region="us-east-1",
                                        table_name=table.replace(".", "-")),
        mode="merge"
    )
```

**3. Point-in-time training (no leakage):**

```python
# Historical fraud labels with exact transaction timestamp
fraud_labels = spark.table("prod.fraud.labeled_transactions")

training_set = fs.create_training_set(
    df=fraud_labels,
    feature_lookups=[
        FeatureLookup(
            table_name="prod.fraud.user_velocity",
            feature_names=["txn_count_24h", "txn_volume_24h", "max_single_txn_24h"],
            lookup_key="user_id",
            timestamp_lookup_key="transaction_timestamp"  # PIT correctness
        ),
        FeatureLookup(
            table_name="prod.fraud.merchant_risk",
            feature_names=["merchant_fraud_rate_90d", "merchant_risk_tier"],
            lookup_key="merchant_id",
            timestamp_lookup_key="transaction_timestamp"
        )
    ],
    label="is_fraud"
)
```

**4. Serving latency budget:**
```
Transaction received:     0ms
Feature lookup (DynamoDB): 5-8ms  (3 tables × ~2ms each)
Model inference:           10-15ms (XGBoost, tabular)
Response returned:         ~25ms total
Budget remaining:          175ms buffer for network/overhead
```

**Trade-offs:**
- Streaming features (5-min refresh) vs real-time computation: real-time is more accurate but much harder. 5-min lag is acceptable for fraud — velocity in the last 24h doesn't change meaningfully in 5 minutes.
- DynamoDB cost: ~$50-100/month for fraud-scale traffic. Justified by avoiding fraud losses.
- PIT correctness is non-negotiable for fraud: future leakage gives artificially high AUC in training but fails in production.

</details>
</article>
