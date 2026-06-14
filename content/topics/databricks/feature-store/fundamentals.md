---
title: "Databricks Feature Store - Fundamentals"
topic: databricks
subtopic: feature-store
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, feature-store, ml, feature-engineering, point-in-time]
---

# Databricks Feature Store — Fundamentals

## 🎯 Analogy

A feature store is like a shared pantry for ML teams. Instead of each data scientist re-cooking the same features (user purchase history, avg session length) from scratch, they all pull from a central, versioned, well-tested pantry. The feature store ensures everyone uses the same recipe, and serving uses the same features that were used in training.

---

## What Is a Feature Store?

A feature store is a centralized repository for ML features — pre-computed, versioned, and shared across teams. It solves two critical problems:

**Training-serving skew:** The feature computation logic in training differs from production serving → model gets different inputs → bad predictions.

**Feature duplication:** 5 teams each write their own version of "user 30-day purchase count" → inconsistency, wasted effort.

```
Without feature store:
  Training pipeline → compute features → train model
  Serving pipeline  → recompute features → predict
  (different code paths → skew)

With feature store:
  Feature engineering job → write to feature store
  Training → read from feature store
  Serving  → read from feature store  ← same features, no skew
```

---

## Creating Feature Tables

```python
from databricks.feature_store import FeatureStoreClient, feature_table
import pyspark.sql.functions as F

fs = FeatureStoreClient()

# Compute features
def compute_user_features(orders_df):
    return orders_df.groupBy("user_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_amount").alias("total_spend"),
        F.avg("order_amount").alias("avg_order_value"),
        F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order")
    )

user_features_df = compute_user_features(orders_df)

# Create or replace feature table
fs.create_table(
    name="ml.user_features",                # {catalog}.{schema}.{table}
    primary_keys=["user_id"],               # unique identifier
    df=user_features_df,
    description="User-level purchase behavior features, updated daily"
)
```

---

## Writing and Reading Features

```python
# Write (append or merge new feature values)
fs.write_table(
    name="ml.user_features",
    df=user_features_df,
    mode="merge"    # merge on primary key; use "overwrite" for full refresh
)

# Read features for training
feature_df = fs.read_table("ml.user_features")

# Join with labels to create training set
training_data = labels_df.join(feature_df, on="user_id", how="left")
```

---

## Training with the Feature Store

The key: log which features were used so serving can fetch them automatically:

```python
from databricks.feature_store import FeatureLookup
import mlflow

# Define which features to pull and from where
feature_lookups = [
    FeatureLookup(
        table_name="ml.user_features",
        feature_names=["total_orders", "total_spend", "avg_order_value", "days_since_last_order"],
        lookup_key="user_id"
    ),
    FeatureLookup(
        table_name="ml.product_features",
        feature_names=["category_popularity", "avg_price"],
        lookup_key="product_id"
    )
]

# Create training set — feature store joins features automatically
training_set = fs.create_training_set(
    df=labels_df,          # DataFrame with label + lookup keys
    feature_lookups=feature_lookups,
    label="purchased",     # target column
    exclude_columns=["event_timestamp"]
)

training_df = training_set.load_df()

# Train model and log — must use fs.log_model, not mlflow.log_model
with mlflow.start_run():
    model = RandomForestClassifier()
    model.fit(training_df.drop("purchased"), training_df["purchased"])

    fs.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,   # records feature lineage
        registered_model_name="purchase-propensity"
    )
```

---

## Serving with the Feature Store

At inference time, only pass the primary keys — the feature store fetches everything else:

```python
# In production: pass only user_id and product_id
# Feature store fetches the feature values automatically
predictions = fs.score_batch(
    model_uri="models:/purchase-propensity/Production",
    df=spark.createDataFrame([
        {"user_id": "U001", "product_id": "P123"},
        {"user_id": "U002", "product_id": "P456"}
    ])
    # No feature columns needed — feature store looks them up
)
```

---

## Key Concepts

| Concept | Meaning |
|---------|---------|
| **Feature table** | Delta table with primary key + feature columns |
| **Primary key** | Unique row identifier (user_id, product_id, etc.) |
| **FeatureLookup** | Spec for joining features at training/serving time |
| **Training set** | Labels + features joined via FeatureLookups |
| **Point-in-time lookup** | Join features using the value at the event time, not current value |

---

## Interview Tips

> **Tip 1:** "What is training-serving skew and how does a feature store solve it?" — "Skew is when training uses one version of a feature computation and serving uses a different version — the model sees different inputs than it was trained on. A feature store uses the same table for both training reads and serving lookups, so the computation logic is written once and shared."

> **Tip 2:** "What's the primary key in a feature table?" — "It's the join key — usually an entity ID like user_id or product_id. When you do a feature lookup, the feature store matches on primary key to fetch the right feature row for each entity in your inference batch."

> **Tip 3:** "Why use `fs.log_model` instead of `mlflow.log_model`?" — "fs.log_model records which feature tables and columns the model was trained on. This lineage is used during serving — `fs.score_batch` knows to look up those exact features from the feature store automatically. Without it, serving has no idea where to fetch features."
