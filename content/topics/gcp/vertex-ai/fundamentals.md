---
title: "Vertex AI — Fundamentals"
topic: gcp
subtopic: vertex-ai
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [gcp, vertex-ai, mlops, feature-store, bigquery-ml, interview]
---

# Vertex AI — Fundamentals

Vertex AI is Google Cloud's unified machine learning platform. For data engineers — not ML scientists — Vertex AI is primarily about **data infrastructure for ML**: building and serving features, orchestrating pipelines, running batch predictions at scale, and connecting the data warehouse to model training. You don't need to understand backpropagation to ace Vertex AI interview questions; you need to understand where your data goes, how features are served, and how to make models consume BigQuery data efficiently.

---

## The Data Engineer's View of Vertex AI

Think of Vertex AI as the bridge between your data platform (BigQuery, Cloud Storage, Pub/Sub) and ML models. The data engineer's responsibilities in an ML platform typically include:

- Building and maintaining the **Feature Store** (turning raw data into model-ready features).
- Orchestrating ML pipelines via **Vertex AI Pipelines** (the same way you orchestrate data pipelines with Airflow/Composer).
- Running **Batch Prediction** jobs that read from BigQuery and write predictions back.
- Exporting datasets from BigQuery into **Vertex AI Datasets** for training jobs.
- Monitoring deployed models for **data drift** using Vertex AI Model Monitoring.

---

## Vertex AI Feature Store

The Feature Store is Vertex AI's solution to a classic ML data problem: the same features (e.g., "user's 30-day purchase count") are computed redundantly across dozens of models, causing inconsistency, duplicated compute, and train-serve skew.

### Core Concepts

**Feature Store (Bigtable-backed)** — the original Feature Store architecture uses Bigtable for online serving (low-latency, ~millisecond lookup) and a BigQuery backend for offline serving (batch training data export).

**Feature Store 2.0 (BigQuery-native)** — the newer architecture uses BigQuery as both the online and offline store, with a feature registry to catalog feature definitions.

### Key Concepts for Interviews

**Point-in-time correctness**: when generating training data, you must join feature values as they existed at the time of the label event — not today's feature values. This prevents **data leakage** (using future information to predict the past).

```
Event: user_123 churned on 2024-03-15
Training row should use: feature values as of 2024-03-14
NOT: today's feature values (which include post-churn behavior)
```

The Feature Store's batch serving method handles this automatically via `read_feature_values` with a timestamp parameter.

### Feature Store (Bigtable-based) — Data Model

```
Feature Store
└─ Entity Type: "user" (keyed by user_id)
    ├─ Feature: "purchase_count_30d"
    ├─ Feature: "avg_basket_size_90d"
    └─ Feature: "days_since_last_login"
└─ Entity Type: "product" (keyed by product_id)
    ├─ Feature: "view_count_7d"
    └─ Feature: "conversion_rate_30d"
```

```python
from google.cloud import aiplatform

aiplatform.init(project="my-project", location="us-central1")

# Create a feature store
featurestore = aiplatform.Featurestore.create(
    featurestore_id="user_features",
    online_store_fixed_node_count=3  # Bigtable nodes for online serving
)

# Create entity type
user_entity = featurestore.create_entity_type(
    entity_type_id="user",
    description="User-level features"
)

# Create features
user_entity.batch_create_features(
    feature_configs={
        "purchase_count_30d": {"value_type": "INT64"},
        "avg_basket_size_90d": {"value_type": "DOUBLE"},
    }
)
```

### Ingesting Features from BigQuery

```python
# Import features from BigQuery
user_entity.ingest_from_bq(
    feature_ids=["purchase_count_30d", "avg_basket_size_90d"],
    feature_time="feature_timestamp",  # column with feature computation time
    bq_source_uri="bq://my-project.features.user_features_daily",
    entity_id_field="user_id"
)
```

---

## BigQuery ML (BQML): ML Inside BigQuery

BigQuery ML lets you train and serve ML models using SQL — without extracting data from BigQuery. For data engineers, BQML is the fastest path from a BigQuery table to a production prediction.

```sql
-- Train a logistic regression model on BigQuery data
CREATE OR REPLACE MODEL `project.ml_models.churn_model`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['churned'],
  data_split_method = 'AUTO_SPLIT'
) AS
SELECT
  purchase_count_30d,
  avg_basket_size_90d,
  days_since_last_login,
  customer_segment,
  churned  -- label column: 0 or 1
FROM `project.features.user_training_data`
WHERE training_date BETWEEN '2023-01-01' AND '2024-01-01';

-- Evaluate the model
SELECT *
FROM ML.EVALUATE(MODEL `project.ml_models.churn_model`,
  (SELECT * FROM `project.features.user_holdout_data`));

-- Generate predictions (batch)
SELECT
  user_id,
  predicted_churned,
  predicted_churned_probs[OFFSET(1)].prob AS churn_probability
FROM ML.PREDICT(MODEL `project.ml_models.churn_model`,
  (SELECT * FROM `project.features.current_user_features`))
ORDER BY churn_probability DESC;
```

**BQML-supported model types**: linear regression, logistic regression, k-means clustering, matrix factorization, boosted trees (XGBoost), deep neural networks, time series (ARIMA+), and imported TensorFlow/ONNX models.

**Why data engineers care**: BQML models appear in Vertex AI Model Registry — you can deploy a BQML model to Vertex AI endpoints for online serving, or run batch predictions via Vertex AI Batch Prediction against the model.

---

## Vertex AI Workbench

Vertex AI Workbench provides managed Jupyter notebooks running on GCP. For data engineers, this is where you:

- Explore BigQuery data using the BigQuery integration in Jupyter.
- Develop feature engineering code that will later be productionized in pipelines.
- Prototype BQML or Vertex AI Pipeline components.

**Managed Notebooks** auto-upgrade the runtime, handle IAM, and integrate with Cloud Storage for persistence. **User-managed notebooks** give you more control but require more maintenance.

```python
# In a Workbench notebook: query BigQuery and use pandas-gbq
import pandas_gbq

df = pandas_gbq.read_gbq(
    """
    SELECT user_id, purchase_count_30d, churned
    FROM `project.features.user_features`
    WHERE feature_date = '2024-01-01'
    LIMIT 10000
    """,
    project_id="my-project"
)

print(df.describe())
```

---

## Vertex AI Model Registry

The Model Registry catalogs trained models with versioning, metadata, and deployment tracking. From a data engineering perspective, it's important because:

1. **BQML models** can be registered here after training.
2. **Batch prediction jobs** reference a model version from the registry.
3. **Model monitoring** is configured per model/endpoint in the registry.

---

## Key Concepts Summary

| Concept | Data Engineer Relevance |
|---|---|
| Feature Store | Centralized feature computation; prevents train-serve skew |
| Point-in-time correctness | Critical for generating unbiased training datasets |
| BQML `CREATE MODEL` | Train ML models entirely in SQL on BigQuery data |
| Vertex AI Workbench | Managed notebooks for feature exploration and prototyping |
| Model Registry | Versioned model catalog; batch prediction references it |
| Batch Prediction | Run inference on a BigQuery table at scale |
| Model Monitoring | Detect data drift in production inputs vs. training data |

---

## Common Misconceptions

1. **"Vertex AI is for ML engineers only"** — False. Feature Stores, pipelines, and batch predictions are squarely in the data engineer's scope.
2. **"BQML is a toy"** — False. BQML supports XGBoost, DNNs, and ARIMA+, and integrates with Vertex AI for deployment. Many production churn and recommendation models run on BQML.
3. **"Feature Store replaces BigQuery"** — False. Feature Store stores pre-computed features for online/offline serving; BigQuery is where features are computed via SQL pipelines that feed the Feature Store.
