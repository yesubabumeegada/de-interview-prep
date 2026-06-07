---
title: "AI - Experiment Tracking"
topic: ai
subtopic: experiment-tracking
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ai, experiment-tracking, mlflow-registry, wandb, custom-metrics, artifact-logging]
---

# Experiment Tracking — Intermediate

## MLflow Model Registry

The Model Registry promotes models through a lifecycle: None → Staging → Production → Archived.

```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# ── Register a model from a run ───────────────────────────────────────
run_id = "abc123def456"
model_version = mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name="churn-classifier",
    tags={
        "test_auc": "0.891",
        "data_version": "2024-01-15",
        "git_sha": "a1b2c3",
    },
)
print(f"Registered version: {model_version.version}")

# ── Transition stages ─────────────────────────────────────────────────
client.transition_model_version_stage(
    name="churn-classifier",
    version=model_version.version,
    stage="Staging",
    archive_existing_versions=False,
)

# Add description and test notes
client.update_model_version(
    name="churn-classifier",
    version=model_version.version,
    description="GBM v3.2 with Yeo-Johnson transform and 30d rolling features. "
                "Test AUC=0.891 vs baseline 0.872. Shadow test: 98.3% agreement.",
)

# Promote to Production (archives previous production version)
client.transition_model_version_stage(
    name="churn-classifier",
    version=model_version.version,
    stage="Production",
    archive_existing_versions=True,
)

# ── Query the registry ───────────────────────────────────────────────
# Get current production model
prod_versions = client.get_latest_versions("churn-classifier", stages=["Production"])
prod_version = prod_versions[0]
print(f"Production: v{prod_version.version}, AUC: {prod_version.tags.get('test_auc')}")

# Load production model anywhere
prod_model = mlflow.sklearn.load_model("models:/churn-classifier/Production")
```

### Aliases (MLflow 2.x+)

Aliases are named pointers — more flexible than stages.

```python
# Create named aliases
client.set_registered_model_alias("churn-classifier", "champion", "5")
client.set_registered_model_alias("churn-classifier", "challenger", "6")
client.set_registered_model_alias("churn-classifier", "rollback", "4")

# Load by alias
champion_model = mlflow.sklearn.load_model("models:/churn-classifier@champion")
challenger_model = mlflow.sklearn.load_model("models:/churn-classifier@challenger")

# Promote: move champion alias to new version
client.set_registered_model_alias("churn-classifier", "champion", "6")
client.delete_registered_model_alias("churn-classifier", "challenger")
```

---

## Comparing Runs Programmatically

```python
import mlflow
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_experiment_results(experiment_name: str) -> pd.DataFrame:
    """Load all runs into a DataFrame for analysis."""
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        filter_string="status = 'FINISHED'",
        order_by=["metrics.test_auc DESC"],
    )
    
    # Rename columns for readability
    runs.columns = [c.replace("params.", "").replace("metrics.", "") for c in runs.columns]
    
    return runs

runs_df = load_experiment_results("churn-hyperparameter-search")

# Top 5 runs
print(runs_df[["run_name", "test_auc", "train_auc", "n_estimators", "learning_rate", "max_depth"]].head())

# Parameter correlation with AUC
numeric_params = ["n_estimators", "learning_rate", "max_depth", "subsample"]
correlations = runs_df[numeric_params + ["test_auc"]].corr()["test_auc"].drop("test_auc")
print("\nCorrelation with test_auc:")
print(correlations.sort_values(ascending=False))

# Overfitting analysis
runs_df["overfit_gap"] = runs_df["train_auc"] - runs_df["test_auc"]
print(f"\nMean overfit gap: {runs_df['overfit_gap'].mean():.4f}")

# Scatter plot: learning_rate vs test_auc
fig, ax = plt.subplots(figsize=(8, 5))
scatter = ax.scatter(
    runs_df["learning_rate"],
    runs_df["test_auc"],
    c=runs_df["max_depth"],
    cmap="viridis",
    alpha=0.7,
)
plt.colorbar(scatter, label="max_depth")
ax.set_xlabel("Learning Rate")
ax.set_ylabel("Test AUC")
ax.set_title("Hyperparameter Exploration")
plt.tight_layout()
```

---

## Custom Metrics

```python
import numpy as np
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

def log_calibration_metrics(y_test, y_prob, n_bins=10):
    """Log calibration quality metrics — important for probability outputs."""
    
    # Brier Score: lower is better calibrated
    brier = brier_score_loss(y_test, y_prob)
    mlflow.log_metric("brier_score", brier)
    
    # Expected Calibration Error (ECE)
    fraction_of_positives, mean_predicted_value = calibration_curve(
        y_test, y_prob, n_bins=n_bins
    )
    ece = np.mean(np.abs(fraction_of_positives - mean_predicted_value))
    mlflow.log_metric("expected_calibration_error", ece)
    
    # Reliability diagram
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(mean_predicted_value, fraction_of_positives, marker="s", label="Model")
    ax.plot([0, 1], [0, 1], "--", label="Perfect calibration")
    ax.set_xlabel("Mean Predicted Probability")
    ax.set_ylabel("Fraction of Positives")
    ax.set_title(f"Reliability Diagram (ECE={ece:.4f})")
    ax.legend()
    mlflow.log_figure(fig, "calibration_plot.png")
    plt.close()


def log_business_metrics(y_test, y_prob, revenue_per_retained=100, cost_per_outreach=5):
    """Log business-relevant metrics for stakeholder communication."""
    
    thresholds = np.arange(0.1, 0.9, 0.05)
    best_roi = -np.inf
    best_threshold = 0.5
    
    for threshold in thresholds:
        y_pred = (y_prob >= threshold).astype(int)
        
        # True positives = churners we correctly identified and retained
        tp = ((y_pred == 1) & (y_test == 1)).sum()
        # False positives = non-churners we incorrectly contacted (waste)
        fp = ((y_pred == 1) & (y_test == 0)).sum()
        
        revenue = tp * revenue_per_retained
        cost = (tp + fp) * cost_per_outreach
        roi = revenue - cost
        
        if roi > best_roi:
            best_roi = roi
            best_threshold = threshold
    
    mlflow.log_metric("best_business_roi", best_roi)
    mlflow.log_metric("optimal_threshold_for_roi", best_threshold)
    
    print(f"Best ROI: ${best_roi:,.0f} at threshold {best_threshold:.2f}")


# Usage in training run
with mlflow.start_run(run_name="churn-with-business-metrics"):
    # ... train model ...
    
    log_calibration_metrics(y_test, y_prob)
    log_business_metrics(y_test, y_prob)
```

---

## Weights & Biases (W&B)

W&B is popular for deep learning experiments with rich visualization.

```python
import wandb
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

# Initialize run
run = wandb.init(
    project="fraud-detection",
    name="transformer-v2",
    config={
        "learning_rate": 1e-4,
        "batch_size": 256,
        "n_epochs": 50,
        "hidden_dim": 512,
        "n_heads": 8,
        "dropout": 0.1,
    },
    tags=["transformer", "v2", "production-candidate"],
)

# Access config (allows W&B sweep to override)
config = wandb.config

# Training loop with W&B logging
optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)
criterion = nn.BCELoss()

for epoch in range(config.n_epochs):
    model.train()
    train_loss = 0
    
    for batch_features, batch_labels in train_loader:
        optimizer.zero_grad()
        outputs = model(batch_features)
        loss = criterion(outputs.squeeze(), batch_labels.float())
        loss.backward()
        optimizer.step()
        train_loss += loss.item()
    
    # Validate
    model.eval()
    val_loss = 0
    with torch.no_grad():
        for batch_features, batch_labels in val_loader:
            outputs = model(batch_features)
            val_loss += criterion(outputs.squeeze(), batch_labels.float()).item()
    
    # Log metrics (W&B auto-creates charts)
    wandb.log({
        "epoch": epoch,
        "train_loss": train_loss / len(train_loader),
        "val_loss": val_loss / len(val_loader),
        "learning_rate": optimizer.param_groups[0]["lr"],
    })

# Log final model and artifacts
wandb.save("model.pt")

# Log a confusion matrix
wandb.log({
    "conf_mat": wandb.plot.confusion_matrix(
        probs=None,
        y_true=y_test.tolist(),
        preds=y_pred.tolist(),
        class_names=["legitimate", "fraud"],
    )
})

wandb.finish()
```

### W&B Hyperparameter Sweeps

```yaml
# sweep_config.yaml
program: train.py
method: bayes  # Bayesian optimization (smarter than random/grid)

metric:
  name: val_auc
  goal: maximize

parameters:
  learning_rate:
    distribution: log_uniform_values
    min: 0.00001
    max: 0.01
  batch_size:
    values: [64, 128, 256, 512]
  hidden_dim:
    values: [128, 256, 512, 1024]
  dropout:
    distribution: uniform
    min: 0.0
    max: 0.5
  n_layers:
    values: [2, 3, 4]

early_terminate:
  type: hyperband
  min_iter: 5
  eta: 3
```

```bash
# Create and run sweep
wandb sweep sweep_config.yaml
wandb agent SWEEP_ID  # Run on available hardware
# Run multiple agents in parallel
wandb agent SWEEP_ID --count 20  # Run 20 trials
```

---

## Interview Tips

> **Tip 1:** "How does MLflow Model Registry differ from W&B Registry?" — "MLflow Registry is tightly coupled with the MLflow ecosystem — each version points to a run in MLflow tracking. It excels at lifecycle management (stage transitions, approvals) and is more commonly used in enterprise MLOps. W&B Registry is better integrated with W&B's visualization and sweep tools and is popular in research and deep learning. Both solve the same problem: a governed catalog of production models."

> **Tip 2:** "When would you use W&B over MLflow?" — "W&B for deep learning: better real-time loss curves, gradient histograms, media logging (images, audio), and sweep optimization are excellent. MLflow for classical ML and production: better model registry governance, better integration with deployment tools (BentoML, SageMaker), and self-hosted option with no data leaving your network."

> **Tip 3:** "What's the Brier Score and when should you log it?" — "Brier Score measures the mean squared error of probability predictions: sum((p_pred - p_actual)^2) / n. It's crucial when you need well-calibrated probabilities — like fraud scoring where the threshold matters, or insurance where predicted probabilities drive pricing. AUC tells you ranking quality; Brier Score tells you calibration quality. Log both."

> **Tip 4:** "How do you find the best model across 500 hyperparameter search runs efficiently?" — "Use mlflow.search_runs() with filter_string and order_by — never load all runs manually. For hyperparameter search, log a composite metric (e.g., weighted combination of AUC and calibration error) and sort by it. Use parallel coordinates plots in the MLflow UI or W&B to visually identify which parameter ranges are promising."
