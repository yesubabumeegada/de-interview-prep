---
title: "MLflow & Model Registry - Scenario Questions"
topic: databricks
subtopic: mlflow-model-registry
content_type: scenario_question
tags: [databricks, mlflow, model-registry, scenarios, interview]
---

# Scenario Questions — MLflow & Model Registry

<article data-difficulty="junior">

## 🟢 Junior: Track a Model Training Experiment

**Scenario:** You're training a RandomForest classifier to predict customer churn. Your manager wants to compare 3 different `max_depth` settings (3, 5, 10) and keep a record of which parameters produced the best model. Set up MLflow tracking so each training run is logged and you can find the best one.

<details>
<summary>✅ Solution</summary>

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment("/churn-prediction/baseline-comparison")

best_f1 = 0
best_run_id = None

for depth in [3, 5, 10]:
    with mlflow.start_run(run_name=f"rf-max_depth-{depth}"):
        # Log parameters
        mlflow.log_param("model_type", "RandomForestClassifier")
        mlflow.log_param("max_depth", depth)
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("random_state", 42)

        # Train
        model = RandomForestClassifier(max_depth=depth, n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        probas = model.predict_proba(X_test)[:, 1]

        # Log metrics
        f1 = f1_score(y_test, preds)
        mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("roc_auc", roc_auc_score(y_test, probas))

        # Log model
        mlflow.sklearn.log_model(model, "model")

        print(f"max_depth={depth}: f1={f1:.3f}")

        if f1 > best_f1:
            best_f1 = f1
            best_run_id = mlflow.active_run().info.run_id

print(f"\nBest run: {best_run_id} (f1={best_f1:.3f})")

# Register the best model
mlflow.register_model(f"runs:/{best_run_id}/model", "churn-classifier")
```

**Key points:**
- One `mlflow.start_run()` context per training run — never nest or share runs across loops
- Log everything before calling `register_model` — the run must be finished first
- Use `mlflow.set_experiment()` at the top so all runs land in the same experiment
- `register_model` creates version 1 in the registry — still in "None" stage, not production yet

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Safe Model Promotion Pipeline

**Scenario:** Your team has multiple data scientists committing models to MLflow. The current process is manual — anyone can promote a model to Production in the UI. Last month, a poorly tested model caused a 4-hour incident. Design a controlled promotion process that enforces quality gates automatically.

<details>
<summary>✅ Solution</summary>

```python
from mlflow.tracking import MlflowClient
from datetime import datetime
import mlflow

client = MlflowClient()

def validate_and_promote(model_name: str, version: str,
                          f1_threshold: float = 0.88,
                          recall_threshold: float = 0.82) -> str:
    """
    Automated promotion gate.
    Returns: 'promoted', 'blocked', or raises on error.
    """
    mv = client.get_model_version(model_name, version)
    run = client.get_run(mv.run_id)
    metrics = run.data.metrics
    params = run.data.params

    failures = []

    # Gate 1: metric thresholds
    f1 = metrics.get("f1_score", 0)
    recall = metrics.get("recall", 0)
    if f1 < f1_threshold:
        failures.append(f"f1={f1:.3f} < {f1_threshold}")
    if recall < recall_threshold:
        failures.append(f"recall={recall:.3f} < {recall_threshold}")

    # Gate 2: must have been tested in Staging first
    if mv.current_stage not in ("Staging",):
        failures.append(f"Must be in Staging before Production (current: {mv.current_stage})")

    # Gate 3: required tags must be set
    required_tags = ["validated_by", "test_dataset_version"]
    for tag in required_tags:
        if tag not in mv.tags:
            failures.append(f"Missing required tag: {tag}")

    # Gate 4: compare to current production (no regression > 1%)
    prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    if prod_versions:
        prod_run = client.get_run(prod_versions[0].run_id)
        prod_f1 = prod_run.data.metrics.get("f1_score", 0)
        if f1 < prod_f1 - 0.01:
            failures.append(f"Regression: new f1={f1:.3f} vs prod f1={prod_f1:.3f}")

    if failures:
        reason = "; ".join(failures)
        client.set_model_version_tag(model_name, version, "promotion_blocked_reason", reason)
        client.set_model_version_tag(model_name, version, "promotion_blocked_at", str(datetime.now()))
        print(f"BLOCKED: {model_name} v{version}\n  Reason: {reason}")
        return "blocked"

    # All gates passed — promote
    client.transition_model_version_stage(
        model_name, version, "Production",
        archive_existing_versions=True   # auto-archive old production
    )
    client.set_model_version_tag(model_name, version, "promoted_at", str(datetime.now()))
    print(f"PROMOTED: {model_name} v{version} to Production")
    return "promoted"

# Usage
result = validate_and_promote("churn-classifier", "5")
```

**What this enforces:**
- No direct None → Production jumps (must pass through Staging)
- Metric floors prevent bad models from sneaking through
- Required tags ensure human sign-off (`validated_by`) and traceability (`test_dataset_version`)
- Auto-archives the previous production model — no orphaned production versions

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design MLOps Governance for a 20-Person DS Team

**Scenario:** A 20-person data science team is growing fast. Models are being promoted inconsistently — some with great test coverage, some as quick experiments. You need to design an MLOps governance framework using MLflow that supports: (1) reproducibility, (2) controlled promotion, (3) model lineage, (4) drift monitoring. What's your architecture?

<details>
<summary>✅ Solution</summary>

**Architecture overview:**

```
Workspace structure:
  /teams/{team}/{project}/experiments  ← isolated by team
  prod.ml_registry                     ← single shared registry
  prod.ml.inference_logs               ← centralized prediction logging
  prod.ml_monitoring.*                 ← drift detection outputs

Promotion workflow:
  Data scientist trains → logs to team experiment
       ↓
  Registers to shared registry (stage: None)
       ↓
  Automated CI job validates (metric gates + shadow traffic)
       ↓
  Moves to Staging → 48h shadow traffic evaluation
       ↓
  Automated gate compares shadow vs champion metrics
       ↓
  Promotion to Production (atomic: archive old → promote new)
       ↓
  Lakehouse Monitor tracks drift on inference table
```

**1. Reproducibility — enforced via wrapper:**

```python
def managed_run(team, project, model_type, owner_email, jira_ticket):
    """Force required fields — runs without them are rejected."""
    mlflow.set_experiment(f"/{team}/{project}")
    return mlflow.start_run(
        run_name=f"{model_type}-{datetime.now():%Y%m%d-%H%M}",
        tags={
            "team": team,
            "owner_email": owner_email,
            "jira_ticket": jira_ticket,
            "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip(),
            "training_data_hash": compute_data_hash(X_train),
        }
    )
```

**2. Lineage — log data + code + environment:**

```python
with managed_run("risk", "fraud-v3", "xgboost", "alice@co.com", "RISK-441"):
    mlflow.log_param("training_table", "prod.features.txn_features_v3")
    mlflow.log_param("training_row_count", len(X_train))
    mlflow.log_param("feature_version", "v3")
    mlflow.autolog()
    model.fit(X_train, y_train)
```

**3. Drift monitoring — automated weekly:**

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

w.quality_monitors.create(
    table_name="prod.ml.fraud_predictions",
    output_schema_name="prod.ml_monitoring",
    inference_log={
        "timestamp_col": "ts",
        "model_id_col": "model_version",
        "prediction_col": "score",
        "label_col": "actual_label",
        "problem_type": "PROBLEM_TYPE_CLASSIFICATION",
        "granularities": ["1 day", "1 week"]
    }
)
# Databricks auto-generates drift dashboard and alerts
```

**4. Promotion gate — runs in CI/CD (GitHub Actions → Databricks job):**

```python
# In CI pipeline triggered on registry webhook (model → Staging)
import subprocess, sys

result = validate_and_promote(model_name, version,
    f1_threshold=MODEL_GATES[model_name]["f1"],
    recall_threshold=MODEL_GATES[model_name]["recall"]
)

if result == "blocked":
    sys.exit(1)  # Fail CI — Slack alert sent via webhook
```

**Trade-offs of this design:**
- Centralized registry = single source of truth, but a bottleneck if gating is too slow → use async shadow evaluation for non-critical models
- Strict tags required → friction for exploratory work → use a separate "sandbox" experiment path with no governance
- Lakehouse Monitoring = additional cost (serverless compute for drift jobs) → justified only for high-value production models

</details>
</article>
