---
title: "AI - Machine Learning Pipelines"
topic: ai
subtopic: machine-learning-pipelines
content_type: scenario_question
difficulty_level: junior
layer: scenarios
tags: [ai, machine-learning, pipelines, scenarios, debugging, scaling]
---

# Machine Learning Pipelines — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: The Leaky Pipeline

You join a team that has a churn prediction model with a suspiciously high AUC of 0.97. When you look at the training code, you see:

```python
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier

# Load data
X, y = load_dataset("churn_data.parquet")

# Scale ALL data first
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Then split
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2)

# Train and evaluate
model = GradientBoostingClassifier()
model.fit(X_train, y_train)
print(f"Test AUC: {model.score(X_test, y_test):.4f}")  # 0.97
```

When you deploy the model, real-world performance is much lower (~0.72 AUC). What's wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about what information the scaler has access to when `fit_transform` is called on `X_scaled`. Specifically, consider what the StandardScaler learns (mean and standard deviation) and whether it should know statistics about the test set.

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause: Data Leakage via Early Scaling

The scaler is fitted on **all data** (train + test) before the split. This means:
1. The scaler learns the mean and std of the **entire dataset**, including test samples
2. When the model is evaluated on `X_test`, those samples have already influenced the preprocessing
3. This constitutes data leakage — the test set is no longer truly "unseen"

The gap between 0.97 (training eval) and 0.72 (production) is the leakage penalty being exposed in the real world.

### The Fix: Use sklearn Pipeline

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.ensemble import GradientBoostingClassifier

# Load data
X, y = load_dataset("churn_data.parquet")

# Split FIRST — test set is never touched until final evaluation
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Pipeline: scaler is fit ONLY on X_train
pipeline = Pipeline([
    ("scaler", StandardScaler()),  # fit on X_train only
    ("model", GradientBoostingClassifier(n_estimators=200, random_state=42)),
])

# Cross-validate on training data
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(pipeline, X_train, y_train, cv=cv, scoring="roc_auc")
print(f"CV AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

# Final evaluation — only done once!
pipeline.fit(X_train, y_train)
test_auc = roc_auc_score(y_test, pipeline.predict_proba(X_test)[:, 1])
print(f"Test AUC: {test_auc:.4f}")  # Now matches production more closely
```

### How to Detect Leakage

Look for these red flags:
- Test AUC is suspiciously close to train AUC (especially above 0.95 for typical business problems)
- Large gap between offline eval and online performance
- `fit_transform` called on full dataset before `train_test_split`
- Features derived from the target variable included in training (e.g., "churn_probability_last_month")

### Prevention Checklist

- [ ] Always use `sklearn.pipeline.Pipeline` or equivalent
- [ ] `train_test_split` before any preprocessing
- [ ] Beware of temporal leakage: future data predicting past labels
- [ ] Beware of target encoding leakage: encoding uses label information
- [ ] Use `cross_val_score` (not manual loops) to ensure correct split

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Pipeline Debugging at Scale

Your team runs a daily ML training pipeline on Kubeflow. The pipeline has been stable for 6 months but today's run failed at the feature engineering step after running for 3 hours. The error log shows:

```
[ERROR] OOMKilled: container exceeded memory limit of 16Gi
[ERROR] Feature engineering step failed on partition user_segment=enterprise
Processed 45/48 partitions successfully
```

The team wants to simply increase memory to 64Gi and rerun. You suggest a more systematic approach. What is your debugging plan and long-term fix?

<details>
<summary>💡 Hint</summary>

Think about: (1) why only the "enterprise" partition fails — what's special about it? (2) how to fix the immediate problem without wasting 3 hours of recompute, (3) what architectural changes would prevent this class of failure in the future.

</details>

<details>
<summary>✅ Solution</summary>

### Immediate Debugging: Understand the Root Cause

```python
# Investigate why enterprise partition is different
import pandas as pd

# Compare partition sizes
partition_sizes = {
    "consumer": 2_500_000,
    "smb": 800_000,
    "enterprise": 145_000,  # Fewer rows but...
}

# Check enterprise user feature complexity
enterprise_df = spark.table("users").filter("segment = 'enterprise'")
enterprise_df.describe().show()

# Enterprise users likely have:
# - Many more historical records per user (high tenure)
# - Complex nested features (org hierarchy, contract history)
# - Cross-joins with product catalog (enterprise uses more products)

# Estimate memory per partition
avg_row_size_bytes = enterprise_df.rdd.map(lambda r: len(str(r))).mean()
total_size_gb = (enterprise_df.count() * avg_row_size_bytes) / (1024**3)
print(f"Enterprise partition estimated size: {total_size_gb:.1f} GB")
```

### Fix 1: Resume Without Restarting from Scratch

```python
# Use checkpoint to resume from last successful partition
def feature_engineering_with_checkpoints(spark, partitions, checkpoint_dir):
    completed = set(get_completed_partitions(checkpoint_dir))
    
    for partition in partitions:
        if partition in completed:
            print(f"Skipping {partition} — already computed")
            continue
        
        try:
            compute_features_for_partition(spark, partition)
            mark_partition_complete(checkpoint_dir, partition)
            print(f"Completed {partition}")
        except MemoryError as e:
            print(f"OOM on {partition} — switching to chunked processing")
            compute_features_chunked(spark, partition, chunk_size=10_000)
            mark_partition_complete(checkpoint_dir, partition)
```

### Fix 2: Partition-Aware Resource Allocation (KFP)

```python
from kfp import dsl

@dsl.pipeline(name="adaptive-feature-pipeline")
def adaptive_feature_pipeline():
    for segment, config in SEGMENT_CONFIGS.items():
        feature_op = compute_features(segment=segment)
        
        # Set resources based on known partition characteristics
        feature_op.set_memory_request(config["memory_gb"])
        feature_op.set_cpu_request(config["cpu_cores"])
    
# Config driven by profiling data
SEGMENT_CONFIGS = {
    "consumer":   {"memory_gb": "16Gi", "cpu_cores": "4"},
    "smb":        {"memory_gb": "32Gi", "cpu_cores": "8"},
    "enterprise": {"memory_gb": "64Gi", "cpu_cores": "16"},
}
```

### Fix 3: Architectural — Skew-Resistant Feature Engineering

```python
from pyspark.sql import functions as F

def compute_features_skew_safe(spark, df):
    """
    Use salting to distribute skewed partitions.
    Enterprise users have high cardinality join keys.
    """
    
    SALT_FACTOR = 10  # Split enterprise data into 10 sub-partitions
    
    # Salt the join key for skewed partitions
    df_salted = df.withColumn(
        "salted_key",
        F.when(
            F.col("segment") == "enterprise",
            F.concat(F.col("user_id"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
        ).otherwise(F.col("user_id"))
    )
    
    # Repartition to distribute load evenly
    df_repartitioned = df_salted.repartition(200, "salted_key")
    
    # Compute features
    features = df_repartitioned.groupBy("user_id", "segment").agg(
        F.count("*").alias("event_count"),
        F.avg("value").alias("avg_value"),
        F.max("timestamp").alias("last_event_ts"),
    )
    
    return features
```

### Long-Term Prevention

1. **Profile partition sizes in CI**: Fail fast if any partition exceeds a threshold before the 3-hour compute
2. **Adaptive resource requests**: Query partition metadata at pipeline start to set appropriate memory per step
3. **Memory-bounded aggregations**: Use approximate algorithms (HLL for cardinality, T-Digest for quantiles) instead of exact ones for large partitions
4. **Monitoring**: Alert on partition size growth trends — if enterprise partition grows 10% week over week, you'll hit OOM before it happens

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Scaling a Pipeline to 10x Data Volume

Your fraud detection ML pipeline currently handles 10M transactions/day and runs in 4 hours. The business is growing 10x to 100M transactions/day within 6 months. The current architecture:

- Single Spark job reads raw data from S3
- Pandas-based feature engineering (single node, 64GB RAM)
- XGBoost training on a single 128GB instance
- MLflow tracking to a local PostgreSQL database
- Batch scoring: daily batch, 4-hour SLA

The fraud team now requires: sub-1-hour training pipeline, real-time scoring at 50K TPS, and full data lineage for compliance. Design the target architecture.

<details>
<summary>💡 Hint</summary>

Think through each bottleneck: (1) Pandas on single node won't scale to 100M rows — what distributed framework handles feature engineering? (2) XGBoost on a single instance — can it be distributed? (3) Batch scoring vs real-time — fundamentally different architectures. (4) MLflow on local Postgres — what breaks at scale?

</details>

<details>
<summary>✅ Solution</summary>

### Architecture Diagram

```
Raw Events (Kafka) ─────────────────────────────────────────────►
                                                                  │
                    ┌─────────────────────────────────────────────▼─────────────────┐
                    │          Feature Platform (Flink + Feature Store)              │
                    │   Real-time features ──► Online Store (Redis Cluster)          │
                    │   Batch features ──────► Offline Store (Delta Lake / S3)       │
                    └─────────────────────────────────────────────┬─────────────────┘
                                                                  │
                    ┌─────────────────────────────────────────────▼─────────────────┐
                    │              Training Pipeline (Kubeflow on EKS)               │
                    │   Spark distributed features ──► XGBoost Distributed Training  │
                    │   MLflow on RDS ──────────────► Model Registry (S3 + MLflow)  │
                    └─────────────────────────────────────────────┬─────────────────┘
                                                                  │
                    ┌─────────────────────────────────────────────▼─────────────────┐
                    │              Serving Layer (50K TPS)                            │
                    │   API Gateway ──► Load Balancer ──► Model Servers (10 pods)    │
                    │   Feature fetch from Redis ──► XGBoost inference ──► Response  │
                    └────────────────────────────────────────────────────────────────┘
```

### Part 1: Distributed Feature Engineering

```python
# Replace Pandas with PySpark
from pyspark.sql import SparkSession, functions as F
from delta import DeltaTable

def build_fraud_features_distributed(spark: SparkSession, date: str):
    """
    Distributed feature computation at 100M transactions/day.
    Target: < 20 minutes runtime.
    """
    
    # Read from Delta Lake (ACID, time-travel, schema evolution)
    transactions = spark.read.format("delta").load(
        f"s3://fraud-data/transactions/"
    ).filter(F.col("date") == date)
    
    print(f"Processing {transactions.count():,} transactions")
    
    # User-level velocity features (last 1h, 6h, 24h, 7d)
    for window_hours in [1, 6, 24, 168]:
        window_spec = Window.partitionBy("user_id").orderBy("transaction_ts").rangeBetween(
            -window_hours * 3600, 0
        )
        transactions = transactions.withColumn(
            f"tx_count_{window_hours}h",
            F.count("*").over(window_spec)
        ).withColumn(
            f"tx_amount_sum_{window_hours}h",
            F.sum("amount_usd").over(window_spec)
        )
    
    # Merchant features
    merchant_stats = transactions.groupBy("merchant_id").agg(
        F.avg("amount_usd").alias("merchant_avg_tx_amount"),
        F.stddev("amount_usd").alias("merchant_tx_amount_std"),
        F.countDistinct("user_id").alias("merchant_unique_users_24h"),
    )
    
    features = transactions.join(merchant_stats, "merchant_id", "left")
    
    # Write to offline feature store (Delta Lake)
    features.write.format("delta").mode("append").partitionBy("date").save(
        "s3://fraud-features/user_transaction_features/"
    )
    
    # Materialize hot features to Redis for online serving
    hot_features = features.select(
        "user_id", "tx_count_1h", "tx_amount_sum_1h", "merchant_unique_users_24h"
    )
    hot_features.foreachPartition(lambda rows: materialize_to_redis(rows))
    
    print("Feature computation complete")
```

### Part 2: Distributed XGBoost Training

```python
# Use XGBoost distributed training with Dask or Spark
import xgboost as xgb
from xgboost.spark import SparkXGBClassifier

def train_fraud_model_distributed(spark, features_path: str):
    """XGBoost distributed training — target: < 30 min for 80M rows."""
    
    features_df = spark.read.format("delta").load(features_path)
    
    # Spark XGBoost Classifier — runs XGBoost workers across Spark executors
    xgb_classifier = SparkXGBClassifier(
        num_workers=16,           # 16 Spark workers
        label_col="is_fraud",
        max_depth=6,
        n_estimators=500,
        learning_rate=0.05,
        scale_pos_weight=100,     # Handle class imbalance (1% fraud rate)
        tree_method="hist",       # Histogram method — much faster
        device="cuda",            # GPU per worker
        enable_sparse_data_optim=True,
    )
    
    model = xgb_classifier.fit(features_df)
    
    # Evaluate
    predictions = model.transform(features_df)
    
    return model
```

### Part 3: Real-Time Serving at 50K TPS

```python
# FastAPI model server with connection pooling
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
import xgboost as xgb
import numpy as np
from prometheus_client import Histogram, Counter

# Metrics
PREDICT_LATENCY = Histogram("fraud_predict_latency_ms", "Prediction latency", buckets=[1, 2, 5, 10, 25, 50, 100])
PREDICT_COUNT = Counter("fraud_predictions_total", "Total predictions", ["label"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: load model + connect to Redis
    app.state.model = xgb.Booster()
    app.state.model.load_model("/models/fraud_model.json")
    app.state.redis = await aioredis.from_url(
        "redis://fraud-redis-cluster:6379",
        max_connections=100,
    )
    yield
    await app.state.redis.close()

app = FastAPI(lifespan=lifespan)

@app.post("/predict")
async def predict(request: dict):
    import time
    start = time.monotonic()
    
    user_id = request["user_id"]
    
    # Fetch real-time features from Redis (~0.5ms)
    redis_features = await app.state.redis.hgetall(f"user_features:{user_id}")
    
    # Combine with request-time features
    feature_vector = build_feature_vector(request, redis_features)
    
    # XGBoost inference (~1ms)
    dmatrix = xgb.DMatrix(np.array([feature_vector]))
    fraud_score = float(app.state.model.predict(dmatrix)[0])
    
    # Record metrics
    latency_ms = (time.monotonic() - start) * 1000
    PREDICT_LATENCY.observe(latency_ms)
    
    label = "fraud" if fraud_score > 0.5 else "legitimate"
    PREDICT_COUNT.labels(label=label).inc()
    
    return {"fraud_score": fraud_score, "latency_ms": latency_ms}
```

### Part 4: MLflow at Scale

```python
# Replace local PostgreSQL with managed MLflow
# - Backend store: Aurora PostgreSQL (Multi-AZ)
# - Artifact store: S3
# - Tracking server: ECS/EKS deployment

MLFLOW_TRACKING_URI = "https://mlflow.internal.company.com"
MLFLOW_S3_ARTIFACT_ROOT = "s3://fraud-mlflow-artifacts/"

# Example: log model with full lineage
with mlflow.start_run(run_name="fraud-distributed-v8"):
    mlflow.set_tags({
        "pipeline_version": "v8.2.0",
        "data_version": date,
        "data_row_count": str(transaction_count),
        "feature_store_version": "feast-1.4.2",
        "git_sha": git_sha,
        "spark_cluster_id": cluster_id,
    })
    mlflow.log_params({"n_estimators": 500, "max_depth": 6, "scale_pos_weight": 100})
    mlflow.log_metrics({"test_auc": auc, "test_precision": precision, "test_recall": recall})
    mlflow.xgboost.log_model(model, "model", registered_model_name="fraud-detection-v8")
```

### Migration Plan (6-Month Timeline)

| Month | Milestone |
|-------|-----------|
| 1 | Migrate feature engineering to Spark; benchmark on 50M rows |
| 2 | Migrate MLflow to RDS + S3 backend; add lineage tags |
| 3 | Deploy distributed XGBoost training; validate metrics parity |
| 4 | Build real-time feature pipeline (Flink → Redis) |
| 5 | Deploy FastAPI serving cluster; shadow test against batch scoring |
| 6 | Cut over to real-time; decommission batch scoring |

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What are the key stages of a production ML pipeline?**
A: Data ingestion and validation → feature engineering → model training → model evaluation → model registration → deployment → monitoring. Each stage should be independently testable, versioned, and automated to enable reliable retraining on schedule or on trigger.

**Q: What is the difference between a batch training pipeline and an online learning pipeline?**
A: Batch training periodically retrains on accumulated historical data — simpler, more stable, but slower to adapt. Online learning updates model parameters incrementally with new data — faster to adapt to distribution shifts, but harder to debug, test, and ensure stability.

**Q: How do you prevent training-serving skew in an ML pipeline?**
A: Share preprocessing logic via a single library or feature store used by both training and serving paths. Avoid feature transformations that rely on training-time state (e.g., fitted scalers) that are inconsistently serialized. Integration tests that run the same inputs through both paths and compare outputs help catch divergence.

**Q: What is a pipeline trigger strategy and what are common options?**
A: Triggers determine when retraining runs: time-based (scheduled cron), event-based (new data arrival, data drift alert, model performance degradation), or manual. Most production systems combine time-based schedules with drift-triggered emergency retraining.

**Q: How do you implement data validation in an ML pipeline?**
A: Use schema validation (Great Expectations, TFX Data Validation) to check column types, null rates, and value ranges. Add statistical validation to detect distribution drift between the current batch and a reference dataset. Fail the pipeline on severe violations to prevent training on corrupted data.

**Q: What is a shadow deployment and when would you use it in an ML pipeline?**
A: Shadow deployment runs a new model in parallel with the production model, serving real traffic but not acting on its predictions. Use it to compare new model behavior against the champion model on live data before making it production — reduces risk of bad deployments.

**Q: How do you handle pipeline failures during a retraining run without corrupting the production model?**
A: Use atomic model registration: only promote the new model to production after it passes all evaluation gates. Keep the previous model version available for instant rollback. Design pipelines with idempotent stages so failed runs can be safely retried without side effects.

**Q: What metrics would you use to automatically decide whether to promote a newly trained model?**
A: Absolute performance thresholds (e.g., AUC > 0.85) combined with relative improvement over the current production model (e.g., ≥ 1% AUC lift). Also check business metrics (precision/recall tradeoffs for the specific use case), data coverage, and fairness metrics across demographic groups.

---

## 💼 Interview Tips

- Always describe ML pipelines as having discrete stages with clear interfaces — this signals you understand how to test, debug, and maintain them independently.
- Senior interviewers will probe pipeline failure modes: what happens if data is late? If the model degrades? Walk through your rollback and circuit-breaker strategies.
- Distinguish between ML pipeline orchestrators (Airflow, Kubeflow Pipelines, Prefect, Metaflow) and their tradeoffs — using the right tool for the scale and team matters.
- Bring up the concept of pipeline drift: not just model drift, but upstream data pipeline changes that silently break ML pipeline assumptions — this shows systems-level thinking.
- Mention CI/CD for ML (CI/CD/CT — Continuous Training) as an evolution beyond standard software CI/CD. Interviewers at senior levels expect familiarity with automated retraining loops.
- Avoid describing ML pipelines as just "train a model and deploy it" — demonstrate awareness of the evaluation, monitoring, and feedback loop components that make pipelines production-grade.
