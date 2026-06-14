---
title: "MLflow & Model Registry - Fundamentals"
topic: databricks
subtopic: mlflow-model-registry
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, mlflow, model-registry, experiment-tracking, ml-ops]
---

# MLflow & Model Registry — Fundamentals

## 🎯 Analogy

MLflow is like GitHub for ML experiments. Each experiment run is a commit — you track what code you ran, what parameters you used, and what results you got. The Model Registry is like npm or PyPI — a versioned store where you promote models from dev → staging → production with approvals.

---

## What Is MLflow?

MLflow is an open-source MLOps platform (created by Databricks) for managing the full ML lifecycle:

```
Data Prep → Training → Tracking → Registry → Deployment → Monitoring
               ↑            ↑           ↑
           MLflow        MLflow      MLflow
           Autolog      Tracking    Registry
```

**Four core components:**

| Component | Purpose |
|-----------|---------|
| **Tracking** | Log parameters, metrics, artifacts per run |
| **Projects** | Package ML code for reproducibility |
| **Models** | Standard format for saving/loading models |
| **Registry** | Versioned model lifecycle management |

---

## Tracking Experiments

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

mlflow.set_experiment("/Users/you@company.com/fraud-detection")

with mlflow.start_run(run_name="rf-v1-baseline"):
    # Log hyperparameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("min_samples_split", 10)

    # Train model
    model = RandomForestClassifier(n_estimators=100, max_depth=5)
    model.fit(X_train, y_train)

    # Log metrics
    preds = model.predict(X_test)
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
    mlflow.log_metric("f1_score", f1_score(y_test, preds))

    # Log the model artifact
    mlflow.sklearn.log_model(model, "model")

    # Log any additional artifacts
    mlflow.log_artifact("feature_importance.png")
    mlflow.log_artifact("confusion_matrix.png")
```

---

## Autologging

Most frameworks support automatic logging — no manual log_param/log_metric calls needed:

```python
import mlflow

mlflow.autolog()  # enable for all supported frameworks

# Now just train normally — MLflow captures everything
from sklearn.ensemble import GradientBoostingClassifier
model = GradientBoostingClassifier(n_estimators=200, learning_rate=0.1)
model.fit(X_train, y_train)
# MLflow auto-logged: params, metrics, model artifact, feature importances
```

**Supported frameworks:** scikit-learn, XGBoost, LightGBM, PyTorch, TensorFlow/Keras, Spark MLlib, Hugging Face Transformers.

---

## Model Registry

After training, register the best model for controlled promotion:

```python
# Register a model from a run
model_uri = f"runs:/{run_id}/model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name="fraud-detection-classifier"
)
# Creates version 1 of "fraud-detection-classifier"

# Transition through stages
from mlflow.tracking import MlflowClient
client = MlflowClient()

client.transition_model_version_stage(
    name="fraud-detection-classifier",
    version=1,
    stage="Staging"      # None → Staging → Production → Archived
)
```

**Model stages:**

```
None (dev) → Staging (testing) → Production (live) → Archived (retired)
```

---

## Loading Models from Registry

```python
# Load the current Production model
import mlflow.pyfunc

model = mlflow.pyfunc.load_model(
    model_uri="models:/fraud-detection-classifier/Production"
)

# Or load a specific version
model_v3 = mlflow.pyfunc.load_model(
    model_uri="models:/fraud-detection-classifier/3"
)

predictions = model.predict(new_data)
```

---

## Key Concepts

| Concept | Meaning |
|---------|---------|
| **Experiment** | Container for related runs (e.g., "fraud-model-v2") |
| **Run** | One training execution — has params, metrics, artifacts |
| **Artifact** | Files saved alongside a run (model pickle, plots, etc.) |
| **Model version** | A specific registered model snapshot |
| **Stage** | Lifecycle state: None, Staging, Production, Archived |

---

## Interview Tips

> **Tip 1:** "What is MLflow and why use it?" — "MLflow tracks ML experiments (parameters, metrics, artifacts) so you can reproduce any past result, compare runs, and avoid re-inventing the wheel. The Model Registry adds version control and promotion workflows — critical for teams deploying multiple model versions."

> **Tip 2:** "What does `mlflow.autolog()` do?" — "It automatically captures training parameters, evaluation metrics, and the trained model artifact for supported frameworks. Saves boilerplate, ensures nothing is forgotten. You just call it before training and MLflow handles the rest."

> **Tip 3:** "What's the difference between an experiment, a run, and a registered model?" — "Experiment = project folder (all runs for fraud detection). Run = one training attempt (trained with n_estimators=100, got 94% accuracy). Registered model = a named model in the registry — may have multiple versions, each pointing to a run's artifact."
