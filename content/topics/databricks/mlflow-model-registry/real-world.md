---
title: "MLflow & Model Registry - Real-World Examples"
topic: databricks
subtopic: mlflow-model-registry
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, mlflow, production, mlops, model-governance, retraining]
---

# MLflow & Model Registry — Real-World Production Examples

## Production Pattern: Automated Retraining Pipeline

A fintech company retrains their fraud detection model weekly using a Databricks Workflow:

```python
# Task 1: Prepare training data
def prepare_features(spark, feature_date: str):
    df = spark.table("prod.features.transaction_features") \
        .filter(f"feature_date >= '{feature_date}'") \
        .filter("label IS NOT NULL")  # only labeled records

    mlflow.log_metric("training_rows", df.count())
    mlflow.log_param("feature_date_from", feature_date)
    return df

# Task 2: Train and log model
def train_model(X_train, y_train, X_val, y_val):
    with mlflow.start_run(run_name=f"weekly-retrain-{date.today()}") as run:
        mlflow.autolog()

        model = XGBClassifier(
            n_estimators=500,
            max_depth=6,
            learning_rate=0.05,
            use_label_encoder=False,
            eval_metric="logloss"
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], early_stopping_rounds=20)

        # Business metric: expected revenue impact
        precision = precision_score(y_val, model.predict(X_val))
        recall = recall_score(y_val, model.predict(X_val))
        avg_fraud_amount = 347.0  # dollars
        prevented = recall * y_val.sum() * avg_fraud_amount
        mlflow.log_metric("estimated_weekly_savings_usd", prevented)

        return run.info.run_id

# Task 3: Gate and promote
def gate_and_promote(run_id: str, model_name: str):
    client = MlflowClient()
    run = client.get_run(run_id)

    f1 = run.data.metrics["f1_score"]
    recall = run.data.metrics["recall"]

    # Hard gates: both must pass
    if recall < 0.85:
        raise Exception(f"FAIL: recall={recall:.3f} < 0.85 (fraud recall gate)")
    if f1 < 0.88:
        raise Exception(f"FAIL: f1={f1:.3f} < 0.88 (f1 gate)")

    # Soft gate: compare to current production
    prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    if prod_versions:
        prod_run = client.get_run(prod_versions[0].run_id)
        prod_f1 = prod_run.data.metrics.get("f1_score", 0)
        if f1 < prod_f1 - 0.005:  # allow up to 0.5% regression
            raise Exception(f"FAIL: new f1={f1:.3f} worse than prod f1={prod_f1:.3f}")

    # Register and promote
    mv = mlflow.register_model(f"runs:/{run_id}/model", model_name)
    client.transition_model_version_stage(model_name, mv.version, "Production",
                                          archive_existing_versions=True)
    print(f"PROMOTED: {model_name} v{mv.version} (f1={f1:.3f}, recall={recall:.3f})")
```

**Result:** Zero manual promotion steps. Model updates weekly without engineer intervention. Failed gates trigger PagerDuty alerts.

---

## Production Pattern: Multi-Model Experiment Comparison

A recommendation team runs experiments across 3 model families before quarterly retraining:

```python
# Standardized experiment runner
MODELS = {
    "lightgbm": lambda: LGBMClassifier(n_estimators=500, num_leaves=63),
    "xgboost": lambda: XGBClassifier(n_estimators=500, max_depth=6),
    "catboost": lambda: CatBoostClassifier(iterations=500, depth=6, verbose=0)
}

mlflow.set_experiment("/quarterly-retrain/Q2-2024")

for model_name, model_factory in MODELS.items():
    with mlflow.start_run(run_name=model_name):
        mlflow.log_param("model_family", model_name)
        model = model_factory()
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        probas = model.predict_proba(X_test)[:, 1]

        mlflow.log_metrics({
            "accuracy": accuracy_score(y_test, preds),
            "f1": f1_score(y_test, preds),
            "roc_auc": roc_auc_score(y_test, probas),
            "avg_precision": average_precision_score(y_test, probas),
            "latency_ms_p95": measure_inference_latency(model, X_test)
        })

        mlflow.log_model(model, "model",
            signature=infer_signature(X_train, preds),
            registered_model_name=None  # don't register yet
        )

# Data scientist reviews UI, picks winner, registers manually
# Or: CI picks the best by roc_auc if above threshold
```

---

## Production Pattern: Incident — Model Silent Failure

A retailer's demand forecasting model started making bad predictions after a schema change — without errors:

**Root cause:** A new feature column was added to the inference pipeline but not to the training data. The model silently received a zero-filled column, degrading accuracy by 23%.

**How they caught it:** MLflow model signature validation was disabled. Re-enabling it surfaced the mismatch immediately.

**Resolution:**
```python
# Always log with signatures to catch schema drift
signature = infer_signature(X_train, model.predict(X_train))
mlflow.sklearn.log_model(model, "model", signature=signature)

# In inference pipeline — validate before calling predict
loaded_model = mlflow.pyfunc.load_model("models:/demand-forecast/Production")

# This raises if column set/types don't match training signature
predictions = loaded_model.predict(inference_df)  # catches mismatch at prediction time

# Add proactive schema check
expected_cols = set(signature.inputs.input_names())
actual_cols = set(inference_df.columns)
missing = expected_cols - actual_cols
if missing:
    raise ValueError(f"Missing features: {missing}")
```

**Lesson:** Model signatures aren't optional. Enable them on every logged model. Add column validation in the inference pipeline before calling predict.

---

## Production Pattern: Experiment Governance at Scale

A 30-person data science team with 1,000+ experiments/month:

```python
# Naming convention enforced via wrapper
def start_managed_run(team: str, project: str, model_type: str, **tags):
    """Enforce naming convention and required tags for all runs."""
    required_tags = ["team", "project", "owner_email", "jira_ticket"]
    for tag in required_tags:
        if tag not in tags:
            raise ValueError(f"Missing required tag: {tag}")

    experiment_path = f"/{team}/{project}"
    mlflow.set_experiment(experiment_path)

    run = mlflow.start_run(
        run_name=f"{model_type}-{datetime.now().strftime('%Y%m%d-%H%M')}",
        tags=tags
    )
    return run

# Usage
with start_managed_run(
    team="recommendations",
    project="cold-start-v2",
    model_type="two-tower",
    owner_email="alice@company.com",
    jira_ticket="REC-442",
    environment="staging"
):
    mlflow.autolog()
    model.fit(X_train, y_train)
```

**Result:** Every run is attributable to a team/project/ticket. Compliance and cost attribution are automated from experiment metadata.

---

## Monitoring Checklist

```python
# Weekly MLflow hygiene query (runs in Databricks notebook)
client = MlflowClient()

# 1. Find experiments with no runs in 90 days (stale)
all_experiments = client.search_experiments()
for exp in all_experiments:
    recent = client.search_runs(
        [exp.experiment_id],
        filter_string=f"attributes.start_time > {(datetime.now() - timedelta(days=90)).timestamp() * 1000}"
    )
    if not recent:
        print(f"STALE: {exp.name} — no runs in 90 days")

# 2. Models with no Production version (registered but abandoned)
all_models = client.search_registered_models()
for model in all_models:
    prod = client.get_latest_versions(model.name, stages=["Production"])
    if not prod:
        print(f"ORPHAN: {model.name} — no Production version")

# 3. Production models with no recent run (stale model)
for model in all_models:
    prod_versions = client.get_latest_versions(model.name, stages=["Production"])
    for v in prod_versions:
        run = client.get_run(v.run_id)
        run_age_days = (time.time() - run.info.end_time / 1000) / 86400
        if run_age_days > 180:
            print(f"AGING: {model.name} v{v.version} trained {run_age_days:.0f} days ago")
```
