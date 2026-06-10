---
title: "AI - Experiment Tracking"
topic: ai
subtopic: experiment-tracking
content_type: scenario_question
difficulty_level: junior
layer: scenarios
tags: [ai, experiment-tracking, scenarios, reproducibility, hyperparameter-search, governance]
---

# Experiment Tracking — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Reproducing a Past Result

Your team deployed a fraud detection model 3 months ago that achieved 0.93 AUC. The model was retrained last week but only achieved 0.88 AUC. You need to reproduce the 0.93 AUC result to understand what changed. Your colleague says: "I think I trained it in a notebook and didn't track anything."

What's your investigation approach and what would you implement to prevent this in the future?

<details>
<summary>💡 Hint</summary>

Even without formal tracking, there may be artifacts: the deployed model file, Git history, Docker images, S3 bucket versions. Think about what forensic evidence exists and how to use it to reconstruct the experiment.

</details>

<details>
<summary>✅ Solution</summary>

### Forensic Investigation

```python
# Step 1: Examine the deployed model artifact for clues
import mlflow
import joblib
import json

# Try to find the old model in MLflow (even if imperfectly tracked)
client = mlflow.tracking.MlflowClient()
runs = client.search_runs(
    experiment_ids=["1"],
    filter_string="metrics.test_auc > 0.90",
    order_by=["start_time DESC"],
)
print(f"Found {len(runs)} runs with AUC > 0.90")
for run in runs:
    print(f"  {run.info.start_time}: AUC={run.data.metrics.get('test_auc')}")

# Step 2: Check Git history
import subprocess
# Search git log for model-related changes around deployment time
log = subprocess.check_output([
    "git", "log", "--since=3 months ago", "--until=2 months ago",
    "--grep=model", "--oneline"
]).decode()
print("Git commits:", log)

# Step 3: Check S3 for versioned artifacts
import boto3
s3 = boto3.client("s3")

# List object versions (if S3 versioning is enabled)
response = s3.list_object_versions(
    Bucket="ml-models-bucket",
    Prefix="fraud_detector/",
)
for version in response.get("Versions", []):
    print(f"{version['LastModified']}: {version['Key']} (version: {version['VersionId'][:8]})")
```

```python
# Step 4: Inspect the deployed model file for embedded metadata
model = joblib.load("fraud_detector_prod.pkl")

# Some sklearn models store creation info
print(type(model))
print(dir(model))

# If it's an MLflow model, check the MLmodel file
with open("fraud_detector_prod/MLmodel") as f:
    print(f.read())
# This may contain: run_id, model version, signature

# Step 5: Compare training data
# Check S3 for data files from ~3 months ago
response = s3.list_objects_v2(
    Bucket="training-data",
    Prefix="fraud/",
)
for obj in response["Contents"]:
    print(f"{obj['LastModified']}: {obj['Key']}")
```

### Conclusion and Prevention Plan

```python
# Minimum viable experiment tracking — add this to every training script
import mlflow
import subprocess
import hashlib
import os

def mandatory_run_context(model_name: str, run_name: str = None):
    """
    Context manager that ensures basic tracking even for quick experiments.
    Use as: with mandatory_run_context("fraud_detector") as run:
    """
    git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()[:8]
    
    mlflow.set_experiment(model_name)
    
    run = mlflow.start_run(run_name=run_name or f"run-{git_sha}")
    
    # Auto-log essentials
    mlflow.set_tags({
        "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip(),
        "git_branch": subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode().strip(),
        "hostname": os.uname().nodename,
        "python_version": subprocess.check_output(["python", "--version"]).decode().strip(),
        "author": subprocess.check_output(["git", "config", "user.email"]).decode().strip(),
    })
    
    return run

# POLICY: Add a pre-commit hook that blocks commits with untracked training notebooks
```

```bash
# .githooks/pre-commit
#!/bin/bash
# Warn if Jupyter notebooks with model training code are being committed without MLflow tracking
notebooks_with_model_fit=$(git diff --cached --name-only | grep ".ipynb" | xargs grep -l "\.fit(" 2>/dev/null)

if [ -n "$notebooks_with_model_fit" ]; then
    echo "WARNING: Committing notebooks with model training. Did you add MLflow tracking?"
    echo "Files: $notebooks_with_model_fit"
    echo "Add 'with mlflow.start_run():' to ensure reproducibility."
    echo "To skip this warning: git commit --no-verify"
    # Don't block, just warn — blocking would frustrate researchers
fi
```

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Hyperparameter Search Gone Wrong

You ran a 500-trial Optuna hyperparameter search for your recommendation model over 3 days. The best trial achieved 0.92 NDCG@10. When you retrain the best configuration on your full dataset (not just the CV training fold), you get 0.83 NDCG@10. Your manager is frustrated — you spent 3 days and $2K in compute for no improvement over the baseline (0.84 NDCG@10). What went wrong and how do you design a better search?

<details>
<summary>💡 Hint</summary>

Think about what Optuna optimizes: the cross-validation metric. Is this the same as what you care about in production? Consider: selection bias in hyperparameter search — if you evaluate 500 configurations and pick the best, some of that "best" is noise. Also: was the search space well-defined?

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause: Selection Bias (Overfitting to Validation Set)

When you evaluate 500 hyperparameter configurations on the same validation set, the "best" configuration has likely overfit to that specific validation set. The true improvement might only be 0.02-0.03 NDCG, but noise in the validation estimate made one config look like 0.92.

```python
# The problem: using the same validation set for 500 trials
# The best result includes positive noise from THIS particular split

# Evidence of selection bias:
import numpy as np

# Simulate: if true performance is 0.85 ± 0.03 for all configs
# And we evaluate 500 configs on ONE held-out set
true_performances = np.random.normal(0.85, 0.02, 500)  # All configs similar
# CV noise: each evaluation has variance from the split
observed_performances = true_performances + np.random.normal(0, 0.03, 500)

best_observed = observed_performances.max()
best_true = true_performances[observed_performances.argmax()]

print(f"Best observed: {best_observed:.4f}")
print(f"True performance: {best_true:.4f}")
print(f"Selection bias: +{best_observed - best_true:.4f}")
# Output: Selection bias is typically +0.03 to +0.06 for 500 trials
```

### Fix 1: Nested Cross-Validation

```python
from sklearn.model_selection import cross_val_score, KFold, StratifiedKFold
from sklearn.base import clone
import optuna
import numpy as np

def nested_cv_score(model_factory, X, y, inner_cv=5, outer_cv=5):
    """
    Nested CV: inner loop for hyperparameter selection, outer loop for evaluation.
    Gives unbiased estimate of selected model's true performance.
    """
    outer = StratifiedKFold(n_splits=outer_cv, shuffle=True, random_state=42)
    outer_scores = []
    
    for fold, (train_idx, test_idx) in enumerate(outer.split(X, y)):
        X_train_outer, X_test_outer = X.iloc[train_idx], X.iloc[test_idx]
        y_train_outer, y_test_outer = y.iloc[train_idx], y.iloc[test_idx]
        
        # Inner loop: hyperparameter search
        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 500),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            }
            model = model_factory(**params)
            inner = StratifiedKFold(n_splits=inner_cv, shuffle=True, random_state=42)
            scores = cross_val_score(model, X_train_outer, y_train_outer, cv=inner, scoring="roc_auc")
            return scores.mean()
        
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=50, show_progress_bar=False)
        
        # Evaluate best config on held-out outer fold
        best_params = study.best_params
        best_model = model_factory(**best_params)
        best_model.fit(X_train_outer, y_train_outer)
        
        fold_score = roc_auc_score(y_test_outer, best_model.predict_proba(X_test_outer)[:, 1])
        outer_scores.append(fold_score)
        print(f"Fold {fold+1}: AUC={fold_score:.4f}, best_params={best_params}")
    
    return np.array(outer_scores)

scores = nested_cv_score(GradientBoostingClassifier, X, y)
print(f"Unbiased AUC: {scores.mean():.4f} ± {scores.std():.4f}")
```

### Fix 2: Better Search Strategy

```python
import optuna

# Problem: 500 random trials with wide search space
# Fix: staged search — narrow first, then refine

def staged_hyperparameter_search(X_train, y_train):
    """Two-stage search: broad exploration, then refinement."""
    
    # Stage 1: Wide search, 100 trials, identify promising regions
    def coarse_objective(trial):
        params = {
            "n_estimators": trial.suggest_categorical("n_estimators", [100, 200, 500, 1000]),
            "learning_rate": trial.suggest_categorical("learning_rate", [0.01, 0.05, 0.1, 0.2]),
            "max_depth": trial.suggest_int("max_depth", 2, 8),
        }
        scores = cross_val_score(
            GradientBoostingClassifier(**params), X_train, y_train,
            cv=3, scoring="roc_auc"
        )
        return scores.mean()
    
    coarse_study = optuna.create_study(direction="maximize")
    coarse_study.optimize(coarse_objective, n_trials=50)
    
    best_n_est = coarse_study.best_params["n_estimators"]
    best_lr = coarse_study.best_params["learning_rate"]
    
    print(f"Coarse search: best n_estimators={best_n_est}, lr={best_lr}")
    
    # Stage 2: Refine around best area, more CV folds for accuracy
    def fine_objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", best_n_est//2, best_n_est*2),
            "learning_rate": trial.suggest_float("learning_rate", best_lr*0.5, best_lr*2, log=True),
            "max_depth": coarse_study.best_params["max_depth"],
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
        }
        # More CV folds for more accurate estimate
        scores = cross_val_score(
            GradientBoostingClassifier(**params), X_train, y_train,
            cv=5, scoring="roc_auc"
        )
        return scores.mean()
    
    fine_study = optuna.create_study(direction="maximize")
    fine_study.optimize(fine_objective, n_trials=50)
    
    return fine_study.best_params, fine_study.best_value

best_params, cv_score = staged_hyperparameter_search(X_train, y_train)

# CRITICAL: Validate on held-out test set (never used in search)
final_model = GradientBoostingClassifier(**best_params)
final_model.fit(X_train, y_train)
test_auc = roc_auc_score(y_test, final_model.predict_proba(X_test)[:, 1])

print(f"CV AUC: {cv_score:.4f}")
print(f"True Test AUC: {test_auc:.4f}")
print(f"Optimism bias: +{cv_score - test_auc:.4f}")
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Model Registry Governance

Your company has 50+ models in production. A compliance officer discovers that 8 models have been deployed to production without any formal approval — engineers can merge to main and the CI/CD automatically promotes models. Additionally, there's no record of WHY any model was promoted, making regulatory audits impossible. Design a governance framework.

<details>
<summary>💡 Hint</summary>

Think about: (1) who should approve what (not just "someone" but specific roles), (2) what evidence is required before approval, (3) how to make approval trackable and immutable, and (4) how to enforce this without blocking ML team velocity.

</details>

<details>
<summary>✅ Solution</summary>

### Governance Framework Design

```python
from enum import Enum
from typing import List, Optional, Dict
from datetime import datetime

class ModelRiskTier(str, Enum):
    """Risk tiers determine approval requirements."""
    TIER_1 = "tier_1"  # High impact (credit, fraud, hiring) — strictest
    TIER_2 = "tier_2"  # Medium impact (pricing, recommendations)
    TIER_3 = "tier_3"  # Low impact (internal tools, analytics)

APPROVAL_REQUIREMENTS = {
    ModelRiskTier.TIER_1: {
        "technical_approvers": 2,         # 2 senior engineers
        "business_approvers": 1,           # Product manager
        "compliance_approvers": 1,         # Legal/compliance sign-off
        "fairness_analysis_required": True,
        "model_card_required": True,
        "shadow_test_days": 7,
        "max_auto_promote": False,         # NEVER auto-promote
    },
    ModelRiskTier.TIER_2: {
        "technical_approvers": 1,
        "business_approvers": 1,
        "compliance_approvers": 0,
        "fairness_analysis_required": True,
        "model_card_required": True,
        "shadow_test_days": 3,
        "max_auto_promote": False,
    },
    ModelRiskTier.TIER_3: {
        "technical_approvers": 1,
        "business_approvers": 0,
        "compliance_approvers": 0,
        "fairness_analysis_required": False,
        "model_card_required": False,
        "shadow_test_days": 1,
        "max_auto_promote": True,          # Can auto-promote after 1 approver
    },
}


class RegistryGovernanceEnforcer:
    """
    Enforces approval workflow before model promotion.
    Blocks promotion without required approvals.
    """
    
    def __init__(self, mlflow_client, audit_logger, notification_client):
        self.client = mlflow_client
        self.audit = audit_logger
        self.notifier = notification_client
    
    def initiate_promotion_request(
        self,
        model_name: str,
        version: str,
        target_stage: str,
        requester: str,
        justification: str,
        evidence: dict,
    ) -> str:
        """Create a promotion request — does NOT promote yet."""
        
        # Look up model risk tier from registry tags
        model = self.client.get_registered_model(model_name)
        risk_tier = ModelRiskTier(model.tags.get("risk_tier", "tier_3"))
        requirements = APPROVAL_REQUIREMENTS[risk_tier]
        
        request_id = f"{model_name}-v{version}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        # Store promotion request as model version tags
        self.client.set_model_version_tag(model_name, version, "promotion_request_id", request_id)
        self.client.set_model_version_tag(model_name, version, "promotion_status", "pending_approval")
        self.client.set_model_version_tag(model_name, version, "promotion_requester", requester)
        self.client.set_model_version_tag(model_name, version, "promotion_target_stage", target_stage)
        self.client.set_model_version_tag(model_name, version, "promotion_justification", justification[:500])
        self.client.set_model_version_tag(model_name, version, "required_technical_approvals", str(requirements["technical_approvers"]))
        
        # Log to immutable audit trail
        self.audit.log({
            "event": "promotion_requested",
            "request_id": request_id,
            "model_name": model_name,
            "version": version,
            "target_stage": target_stage,
            "requester": requester,
            "risk_tier": risk_tier.value,
            "required_approvals": requirements,
            "timestamp": datetime.utcnow().isoformat(),
        })
        
        # Notify required approvers
        self.notifier.send(
            channel=f"#ml-approvals-{risk_tier.value}",
            message=f"Promotion request for {model_name} v{version} → {target_stage}\n"
                   f"Requester: {requester}\n"
                   f"Justification: {justification}\n"
                   f"Required: {requirements['technical_approvers']} technical approvals",
        )
        
        return request_id
    
    def record_approval(
        self,
        request_id: str,
        model_name: str,
        version: str,
        approver: str,
        approver_role: str,  # "technical", "business", "compliance"
        approved: bool,
        comments: str = "",
    ):
        """Record an approval decision."""
        
        timestamp = datetime.utcnow().isoformat()
        key = f"approval_{approver_role}_{approver}_{timestamp[:10]}"
        value = f"{'APPROVED' if approved else 'REJECTED'}|{comments[:200]}"
        
        self.client.set_model_version_tag(model_name, version, key, value)
        
        self.audit.log({
            "event": "approval_recorded",
            "request_id": request_id,
            "approver": approver,
            "approver_role": approver_role,
            "decision": "approved" if approved else "rejected",
            "comments": comments,
            "timestamp": timestamp,
        })
        
        # Check if all required approvals are in
        if approved and self._all_approvals_received(model_name, version):
            self._execute_promotion(model_name, version)
        elif not approved:
            self.client.set_model_version_tag(model_name, version, "promotion_status", "rejected")
    
    def _all_approvals_received(self, model_name: str, version: str) -> bool:
        """Check if all required approvals are present."""
        mv = self.client.get_model_version(model_name, version)
        model = self.client.get_registered_model(model_name)
        risk_tier = ModelRiskTier(model.tags.get("risk_tier", "tier_3"))
        requirements = APPROVAL_REQUIREMENTS[risk_tier]
        
        approvals = {
            "technical": sum(1 for k, v in mv.tags.items() 
                            if k.startswith("approval_technical_") and v.startswith("APPROVED")),
            "business": sum(1 for k, v in mv.tags.items()
                           if k.startswith("approval_business_") and v.startswith("APPROVED")),
            "compliance": sum(1 for k, v in mv.tags.items()
                             if k.startswith("approval_compliance_") and v.startswith("APPROVED")),
        }
        
        return (
            approvals["technical"] >= requirements["technical_approvers"]
            and approvals["business"] >= requirements["business_approvers"]
            and approvals["compliance"] >= requirements["compliance_approvers"]
        )
    
    def _execute_promotion(self, model_name: str, version: str):
        """Execute the promotion after all approvals received."""
        mv = self.client.get_model_version(model_name, version)
        target_stage = mv.tags.get("promotion_target_stage")
        
        self.client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=target_stage,
            archive_existing_versions=(target_stage == "Production"),
        )
        
        self.client.set_model_version_tag(model_name, version, "promotion_status", "promoted")
        self.client.set_model_version_tag(model_name, version, "promoted_at", datetime.utcnow().isoformat())
        
        print(f"Promotion executed: {model_name} v{version} → {target_stage}")
```

### CI/CD Enforcement

```yaml
# .github/workflows/block_unauthorized_promotion.yml
name: Block Unauthorized Model Promotion

on:
  push:
    branches: [main]
    paths:
      - "configs/model_registry*.yaml"

jobs:
  check-promotion-approval:
    runs-on: ubuntu-latest
    steps:
      - name: Verify promotion approval
        run: |
          python scripts/verify_promotion_approval.py \
            --model-name ${{ github.event.inputs.model_name }} \
            --version ${{ github.event.inputs.version }} \
            --required-approvals 2
        env:
          MLFLOW_TRACKING_URI: ${{ secrets.MLFLOW_TRACKING_URI }}
```

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is experiment tracking and what core artifacts should every experiment capture?**
A: Experiment tracking records all inputs, outputs, and metadata for ML runs to enable comparison and reproducibility. Every experiment should capture: hyperparameters, dataset version/hash, code version (git commit), environment (dependency versions), metrics over time, and the final saved model artifact.

**Q: How does MLflow differ from Weights & Biases for experiment tracking?**
A: MLflow is open-source and self-hostable, integrating tightly with the broader MLflow ecosystem (model registry, serving). W&B is a cloud-first SaaS offering richer collaboration, real-time visualization, and sweeps for hyperparameter search — better for teams that prioritize UX over self-hosting.

**Q: What is a run vs. an experiment in MLflow?**
A: An experiment is a named collection of runs grouped by project or objective. A run is a single execution with its own set of logged parameters, metrics, tags, and artifacts. Organizing runs within experiments enables clean comparison across iterations.

**Q: How do you track hyperparameter search at scale?**
A: Use sweep/tuning integrations (Optuna, Ray Tune, W&B Sweeps, SageMaker Hyperparameter Tuning). Each trial is logged as a separate run under the same experiment, with the search strategy, search space bounds, and best run identified programmatically after the sweep completes.

**Q: How do you ensure experiment reproducibility when dependencies change over time?**
A: Log the full environment: requirements.txt or conda env YAML, Docker image digest, and git commit SHA. Use a model registry to link each registered model to the exact run and artifact that produced it. DVC or dataset versioning pins the data side.

**Q: What is the difference between logging metrics per step vs. per epoch, and why does it matter?**
A: Per-step logging gives fine-grained visibility into training dynamics (loss spikes, gradient instability). Per-epoch logging reduces storage overhead and is sufficient for most comparisons. For debugging, per-step is critical; for long-running production jobs, epoch-level with periodic checkpoints is more practical.

**Q: How would you handle experiment tracking in a distributed training job?**
A: Designate rank-0 (the primary worker) as the sole logger to avoid duplicate metric writes. Use framework integrations (PyTorch Lightning, Hugging Face Trainer) that handle this automatically. Aggregate metrics from all workers before logging, or use distributed-aware tracking backends.

**Q: What is a model registry and how does it relate to experiment tracking?**
A: A model registry is a centralized store for versioned, production-ready models with lifecycle stages (Staging, Production, Archived). Experiment tracking feeds it — a promising run's artifact is promoted to the registry with metadata linking it to its originating run, enabling governance and deployment workflows.

---

## 💼 Interview Tips

- Frame experiment tracking as a prerequisite for ML governance, not just a developer convenience — this resonates with senior interviewers focused on production reliability.
- Show you understand the full lifecycle: tracking → model registry → deployment → monitoring. Candidates who treat experiment tracking in isolation miss the bigger picture.
- Mention concrete tooling tradeoffs: MLflow for self-hosted/open-source needs, W&B for team collaboration, SageMaker Experiments for AWS-native workflows — choose based on organizational constraints.
- A common mistake is tracking only final metrics. Emphasize logging intermediate metrics, system metrics (GPU utilization, throughput), and data statistics to enable richer post-hoc debugging.
- Senior interviewers often ask about team adoption — be ready to discuss how you'd standardize experiment naming conventions, tagging schemas, and artifact storage policies across a team.
- Avoid conflating experiment tracking with model monitoring — tracking is about the development phase; monitoring is about production behavior. Distinguish these clearly.
