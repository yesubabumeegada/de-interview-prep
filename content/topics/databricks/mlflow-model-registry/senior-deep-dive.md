---
title: "MLflow & Model Registry - Senior Deep Dive"
topic: databricks
subtopic: mlflow-model-registry
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, mlflow, mlops, model-governance, ci-cd, ab-testing, model-monitoring]
---

# MLflow & Model Registry — Senior Deep Dive

## MLOps Architecture with MLflow

```
┌─────────────────────────────────────────────────────────┐
│                   Databricks Platform                    │
│                                                          │
│  Feature Store ──→ Training Job ──→ MLflow Tracking     │
│                         │                    │           │
│                    Experiment            Run artifacts   │
│                    comparison                │           │
│                         │           Model Registry      │
│                         └──────→  (versioned + staged)  │
│                                        │                 │
│                              Model Serving Endpoint      │
│                              (real-time inference)       │
│                                        │                 │
│                              Lakehouse Monitoring        │
│                              (drift detection)           │
└─────────────────────────────────────────────────────────┘
```

---

## Production Model Governance

At scale, model promotion must be gated by automated checks:

```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

def promote_to_production(model_name: str, version: int, threshold: float = 0.92) -> bool:
    """Gate production promotion on metric threshold + shadow traffic test."""

    mv = client.get_model_version(model_name, str(version))

    # 1. Check metric gate
    run = client.get_run(mv.run_id)
    f1 = run.data.metrics.get("f1_score", 0)
    if f1 < threshold:
        print(f"BLOCKED: f1={f1:.3f} < threshold={threshold}")
        client.set_model_version_tag(model_name, str(version), "promotion_blocked_reason", f"f1={f1:.3f}")
        return False

    # 2. Check data validation tag (set by upstream validation job)
    if mv.tags.get("data_validation_passed") != "true":
        print("BLOCKED: data validation not passed")
        return False

    # 3. Archive current production model
    current_prod = client.get_latest_versions(model_name, stages=["Production"])
    for old_model in current_prod:
        client.transition_model_version_stage(
            model_name, old_model.version, "Archived",
            archive_existing_versions=False
        )

    # 4. Promote new version
    client.transition_model_version_stage(model_name, str(version), "Production")
    client.set_model_version_tag(model_name, str(version), "promoted_at", str(datetime.now()))
    print(f"PROMOTED: {model_name} v{version} to Production (f1={f1:.3f})")
    return True
```

---

## Champion/Challenger A/B Testing

Run two model versions simultaneously and route traffic by percentage:

```python
import mlflow.pyfunc
import random

class ChampionChallengerModel(mlflow.pyfunc.PythonModel):
    """Route 90% traffic to champion, 10% to challenger."""

    def load_context(self, context):
        self.champion = mlflow.pyfunc.load_model("models:/fraud-model/Production")
        self.challenger = mlflow.pyfunc.load_model("models:/fraud-model/Staging")
        self.challenger_pct = 0.10

    def predict(self, context, model_input):
        import uuid
        use_challenger = random.random() < self.challenger_pct

        if use_challenger:
            pred = self.challenger.predict(model_input)
            model_input["_model_version"] = "challenger"
        else:
            pred = self.champion.predict(model_input)
            model_input["_model_version"] = "champion"

        return pred

# Register the router as its own model
with mlflow.start_run():
    mlflow.pyfunc.log_model("router", python_model=ChampionChallengerModel())
```

---

## Model Drift Monitoring with Lakehouse Monitoring

Databricks Lakehouse Monitoring integrates with MLflow endpoints to detect drift:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Create a monitor on the inference table
w.quality_monitors.create(
    table_name="prod.ml.fraud_model_predictions",
    assets_dir="/Shared/monitors/fraud_model",
    output_schema_name="prod.ml_monitoring",
    inference_log={
        "timestamp_col": "prediction_timestamp",
        "granularities": ["1 day", "1 week"],
        "model_id_col": "model_version",
        "prediction_col": "prediction_score",
        "label_col": "actual_fraud",  # when labels become available
        "problem_type": "PROBLEM_TYPE_CLASSIFICATION"
    }
)
```

**Metrics tracked automatically:**
- Feature drift (population stability index per feature)
- Prediction drift (score distribution shift)
- Model accuracy (precision/recall/F1 when labels arrive)
- Data quality (null rates, out-of-range values)

---

## Multi-Environment Registry Promotion

Production-grade teams use separate MLflow registries per environment:

```python
# dev registry → staging registry → prod registry
# Promoted via CI/CD, not by data scientists directly

import mlflow
from mlflow.tracking import MlflowClient

def promote_across_registries(
    source_registry_uri: str,
    dest_registry_uri: str,
    model_name: str,
    version: str
):
    """Copy a model version from staging to prod registry."""
    src_client = MlflowClient(tracking_uri=source_registry_uri)
    dst_client = MlflowClient(tracking_uri=dest_registry_uri)

    # Download model from source
    model_uri = f"models:/{model_name}/{version}"
    local_path = mlflow.artifacts.download_artifacts(model_uri, dst_path="/tmp/model_transfer")

    # Log to destination registry
    with mlflow.start_run(tracking_uri=dest_registry_uri):
        mlflow.log_artifacts(local_path, artifact_path="model")
        run_id = mlflow.active_run().info.run_id

    dst_client.register_model(
        name=model_name,
        source=f"runs:/{run_id}/model",
        tags={"promoted_from": source_registry_uri, "source_version": version}
    )
```

---

## Model Lineage and Reproducibility

Capture full lineage — data version, code version, environment:

```python
with mlflow.start_run():
    # Log data lineage
    mlflow.log_param("training_data_path", "dbfs:/data/features/2024-Q1/v3")
    mlflow.log_param("training_data_row_count", len(X_train))
    mlflow.log_param("training_data_hash", hashlib.md5(X_train.to_csv().encode()).hexdigest())

    # Log code version
    import subprocess
    git_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    mlflow.log_param("git_commit", git_hash)
    mlflow.log_param("notebook_path", dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

    # Log environment
    mlflow.log_param("spark_version", spark.version)
    mlflow.log_param("python_version", platform.python_version())

    # Standard training
    model.fit(X_train, y_train)
    mlflow.sklearn.log_model(model, "model")
```

---

## Interview Tips

> **Tip 1:** "How do you ensure model reproducibility in a team environment?" — "Three axes: (1) Data lineage — log the exact dataset path, row count, and hash used for training. (2) Code version — log the git commit hash in every run. (3) Environment — log the exact dependency versions via conda_env or pip_requirements. With those three, any team member can recreate any run exactly."

> **Tip 2:** "How would you implement champion/challenger testing safely?" — "Register both models in the MLflow Registry — champion in Production stage, challenger in Staging. Build a routing layer (pyfunc wrapper or model serving traffic split) that sends 5-10% of live traffic to the challenger, logs both predictions with a version tag to the inference table, then run statistical significance tests on the live metrics before promoting the challenger."

> **Tip 3:** "What's the risk of promoting models manually?" — "Humans skip steps under pressure — especially validation checks and archiving the previous production version. Automate promotion through a gating function that enforces metric thresholds, checks data quality tags set by upstream validation jobs, and archives the old model atomically. Webhook-driven CI/CD ensures no model reaches production without the full gate passing."
