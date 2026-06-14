---
title: "MLflow & Model Registry - Intermediate"
topic: databricks
subtopic: mlflow-model-registry
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, mlflow, experiment-tracking, model-versioning, hyperparameter-tuning]
---

# MLflow & Model Registry — Intermediate Concepts

## Comparing Runs Programmatically

```python
from mlflow.tracking import MlflowClient
import pandas as pd

client = MlflowClient()

# Search runs in an experiment, filter by metric threshold
runs = client.search_runs(
    experiment_ids=["1"],
    filter_string="metrics.f1_score > 0.90 AND params.n_estimators >= 100",
    order_by=["metrics.f1_score DESC"],
    max_results=10
)

# Convert to DataFrame for analysis
df = pd.DataFrame([{
    "run_id": r.info.run_id,
    "f1_score": r.data.metrics.get("f1_score"),
    "accuracy": r.data.metrics.get("accuracy"),
    "n_estimators": r.data.params.get("n_estimators"),
    "status": r.info.status
} for r in runs])

print(df)
best_run = df.loc[df["f1_score"].idxmax()]
```

---

## Hyperparameter Tuning with MLflow

```python
import mlflow
from itertools import product

mlflow.set_experiment("/experiments/xgb-tuning")

param_grid = {
    "max_depth": [3, 5, 7],
    "learning_rate": [0.01, 0.1, 0.3],
    "n_estimators": [100, 300]
}

best_f1 = 0
best_run_id = None

for depth, lr, n_est in product(
    param_grid["max_depth"],
    param_grid["learning_rate"],
    param_grid["n_estimators"]
):
    with mlflow.start_run(run_name=f"xgb-d{depth}-lr{lr}-n{n_est}"):
        mlflow.log_params({"max_depth": depth, "learning_rate": lr, "n_estimators": n_est})

        model = XGBClassifier(max_depth=depth, learning_rate=lr, n_estimators=n_est)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], early_stopping_rounds=10)

        f1 = f1_score(y_val, model.predict(X_val))
        mlflow.log_metric("f1_score", f1)
        mlflow.xgboost.log_model(model, "model")

        if f1 > best_f1:
            best_f1, best_run_id = f1, mlflow.active_run().info.run_id

# Register the best model
mlflow.register_model(f"runs:/{best_run_id}/model", "xgb-churn-predictor")
```

---

## Custom Model Flavors (pyfunc)

When your model isn't a standard sklearn/XGBoost class, wrap it in a `pyfunc`:

```python
import mlflow.pyfunc
import pandas as pd

class EnsembleModel(mlflow.pyfunc.PythonModel):
    """Two-model ensemble: XGBoost + LightGBM averaged."""

    def load_context(self, context):
        import joblib
        self.xgb = joblib.load(context.artifacts["xgb_model"])
        self.lgbm = joblib.load(context.artifacts["lgbm_model"])

    def predict(self, context, model_input: pd.DataFrame) -> pd.Series:
        xgb_pred = self.xgb.predict_proba(model_input)[:, 1]
        lgbm_pred = self.lgbm.predict_proba(model_input)[:, 1]
        return pd.Series((xgb_pred + lgbm_pred) / 2)

# Save artifacts
import joblib
joblib.dump(xgb_model, "/tmp/xgb.pkl")
joblib.dump(lgbm_model, "/tmp/lgbm.pkl")

with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="ensemble",
        python_model=EnsembleModel(),
        artifacts={
            "xgb_model": "/tmp/xgb.pkl",
            "lgbm_model": "/tmp/lgbm.pkl"
        },
        pip_requirements=["xgboost==1.7.0", "lightgbm==3.3.5"]
    )
```

---

## Model Signatures and Input Examples

Signatures enforce input/output schema — catch mismatches before production:

```python
from mlflow.models.signature import infer_signature

# Infer from training data
signature = infer_signature(X_train, model.predict(X_train))

# Log with signature
mlflow.sklearn.log_model(
    model,
    "model",
    signature=signature,
    input_example=X_train.head(3)   # shows expected input format
)

# Signature is validated at load time
loaded = mlflow.pyfunc.load_model("models:/my-model/Production")
loaded.predict(bad_input)  # raises MlflowException if schema mismatch
```

---

## Registry Webhooks and Automation

Trigger CI/CD on model stage transitions:

```python
from mlflow.tracking import MlflowClient
import json

client = MlflowClient()

# Create webhook: notify Slack when model moves to Production
client.create_registry_webhook(
    events=["MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"],
    http_url_spec={
        "url": "https://hooks.slack.com/services/XXX/YYY/ZZZ",
        "secret": "your-webhook-secret",
        "enable_ssl_verification": True
    },
    model_name="fraud-detection-classifier",
    description="Notify #mlops-alerts on production promotion"
)

# List webhooks
hooks = client.list_registry_webhooks(model_name="fraud-detection-classifier")
```

**Common automation pattern:** On promotion to Staging → run integration test job. On promotion to Production → deploy to model serving endpoint automatically.

---

## Tags and Descriptions

```python
client = MlflowClient()

# Tag a model version with business metadata
client.set_model_version_tag(
    name="fraud-detection-classifier",
    version="3",
    key="validated_by",
    value="mlops-team"
)
client.set_model_version_tag(
    name="fraud-detection-classifier",
    version="3",
    key="training_dataset_version",
    value="2024-Q1"
)

# Add a description (shown in UI)
client.update_model_version(
    name="fraud-detection-classifier",
    version="3",
    description="XGBoost retrained on 2024-Q1 data. F1=0.934. Approved for production 2024-04-15."
)
```

---

## Interview Tips

> **Tip 1:** "How do you find the best run across hundreds of experiments?" — "Use `client.search_runs()` with a filter string like `metrics.f1_score > 0.90 AND params.model_type = 'xgboost'` and `order_by=['metrics.f1_score DESC']`. This returns a ranked list — take the top run's run_id and register it."

> **Tip 2:** "What is a pyfunc model and when do you use it?" — "A pyfunc is a generic MLflow model wrapper that lets you define custom `predict()` logic — useful for ensembles, preprocessing + model pipelines, or any model that doesn't fit a standard framework flavor. It's serialized as a Python class with a `load_context` method that loads artifacts."

> **Tip 3:** "What's a model signature and why does it matter?" — "A signature defines the expected input columns/types and output type. MLflow validates inputs against the signature when you call `predict()` — catches column mismatches, wrong dtypes, and missing features before they cause silent prediction errors in production."
