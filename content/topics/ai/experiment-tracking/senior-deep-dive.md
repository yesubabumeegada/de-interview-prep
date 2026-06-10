---
title: "AI - Experiment Tracking"
topic: ai
subtopic: experiment-tracking
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ai, experiment-tracking, distributed-training, optuna, ray-tune, reproducibility]
---

# Experiment Tracking — Senior Deep Dive

## Distributed Training Tracking

Distributed training (multiple GPUs, multiple nodes) requires coordinated tracking so only one process writes to MLflow.

```python
import torch
import torch.distributed as dist
import mlflow
import os
from typing import Optional

def setup_distributed():
    """Initialize distributed training."""
    dist.init_process_group(backend="nccl")
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)
    return local_rank

def is_main_process() -> bool:
    """Only rank 0 logs to MLflow."""
    if not dist.is_initialized():
        return True
    return dist.get_rank() == 0

class DistributedExperimentTracker:
    """
    Wraps MLflow tracking for distributed training.
    Only the main process (rank 0) writes to the tracking server.
    All other processes call no-ops.
    """
    
    def __init__(self, experiment_name: str, run_name: Optional[str] = None):
        self._active = is_main_process()
        
        if self._active:
            mlflow.set_experiment(experiment_name)
            self._run = mlflow.start_run(run_name=run_name)
            self.run_id = self._run.info.run_id
        else:
            self.run_id = None
    
    def log_param(self, key: str, value):
        if self._active:
            mlflow.log_param(key, value)
    
    def log_params(self, params: dict):
        if self._active:
            mlflow.log_params(params)
    
    def log_metric(self, key: str, value: float, step: Optional[int] = None):
        if self._active:
            mlflow.log_metric(key, value, step=step)
    
    def log_metrics(self, metrics: dict, step: Optional[int] = None):
        if self._active:
            mlflow.log_metrics(metrics, step=step)
    
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None):
        if self._active:
            mlflow.log_artifact(local_path, artifact_path)
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        if self._active:
            mlflow.end_run()


# Training loop with distributed tracking
def train_distributed(config: dict):
    local_rank = setup_distributed()
    device = torch.device(f"cuda:{local_rank}")
    
    model = FraudDetectionModel(**config["model_params"]).to(device)
    model = torch.nn.parallel.DistributedDataParallel(model, device_ids=[local_rank])
    
    with DistributedExperimentTracker("fraud-distributed", run_name=config["run_name"]) as tracker:
        tracker.log_params({
            **config["model_params"],
            "n_gpus": dist.get_world_size() if dist.is_initialized() else 1,
            "distributed_backend": "nccl",
        })
        
        optimizer = torch.optim.AdamW(model.parameters(), lr=config["learning_rate"])
        
        for epoch in range(config["n_epochs"]):
            train_loss = train_epoch(model, train_loader, optimizer, device)
            val_metrics = evaluate(model, val_loader, device)
            
            # Aggregate metrics across all GPUs
            avg_val_loss = aggregate_metric(val_metrics["loss"])
            avg_val_auc = aggregate_metric(val_metrics["auc"])
            
            tracker.log_metrics({
                "train_loss": train_loss,
                "val_loss": avg_val_loss,
                "val_auc": avg_val_auc,
            }, step=epoch)
        
        if is_main_process():
            torch.save(model.module.state_dict(), "model.pt")
            tracker.log_artifact("model.pt")


def aggregate_metric(local_value: float) -> float:
    """Average a metric across all distributed processes."""
    if not dist.is_initialized():
        return local_value
    
    tensor = torch.tensor(local_value).cuda()
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    return float(tensor) / dist.get_world_size()
```

---

## Hyperparameter Optimization with Optuna

Optuna uses Bayesian optimization (Tree-structured Parzen Estimator) to find optimal hyperparameters more efficiently than grid or random search.

```python
import optuna
import mlflow
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score, StratifiedKFold

# Set up MLflow experiment
mlflow.set_experiment("churn-optuna-search")

def objective(trial: optuna.Trial) -> float:
    """Objective function: returns metric to maximize."""
    
    # Define search space
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000, step=50),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "max_depth": trial.suggest_int("max_depth", 2, 8),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 20),
        "max_features": trial.suggest_categorical("max_features", ["sqrt", "log2", 0.5, 0.8]),
    }
    
    with mlflow.start_run(nested=True):  # Nested run under parent experiment
        mlflow.log_params(params)
        mlflow.set_tag("optuna_trial", str(trial.number))
        
        model = GradientBoostingClassifier(**params, random_state=42)
        
        # 5-fold CV score (more reliable than single validation)
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc", n_jobs=-1)
        
        auc = cv_scores.mean()
        mlflow.log_metric("cv_auc_mean", auc)
        mlflow.log_metric("cv_auc_std", cv_scores.std())
        
        return auc


# Create study with pruning (stops bad trials early)
sampler = optuna.samplers.TPESampler(seed=42)  # Bayesian optimization
pruner = optuna.pruners.HyperbandPruner(min_resource=50, max_resource=1000, reduction_factor=3)

study = optuna.create_study(
    direction="maximize",
    sampler=sampler,
    pruner=pruner,
    study_name="churn-hyperparameter-search",
    storage="sqlite:///optuna.db",  # Persist across sessions
    load_if_exists=True,
)

# Run optimization
with mlflow.start_run(run_name="optuna-search-parent"):
    mlflow.set_tag("search_type", "TPE")
    mlflow.log_param("n_trials", 100)
    
    study.optimize(objective, n_trials=100, timeout=3600, n_jobs=4)  # 4 parallel trials
    
    # Log best results to parent run
    mlflow.log_metric("best_cv_auc", study.best_value)
    mlflow.log_params({f"best_{k}": v for k, v in study.best_params.items()})

print(f"Best trial: {study.best_trial.number}")
print(f"Best AUC: {study.best_value:.4f}")
print(f"Best params: {study.best_params}")

# Visualize (in notebook)
optuna.visualization.plot_optimization_history(study)
optuna.visualization.plot_param_importances(study)
optuna.visualization.plot_parallel_coordinate(study)
```

### Ray Tune for Large-Scale HPO

```python
import ray
from ray import tune
from ray.tune.schedulers import ASHAScheduler
from ray.tune.search.optuna import OptunaSearch
import mlflow

def train_with_ray(config: dict, checkpoint_dir=None):
    """Ray Tune training function."""
    model = GradientBoostingClassifier(
        n_estimators=config["n_estimators"],
        learning_rate=config["learning_rate"],
        max_depth=config["max_depth"],
        subsample=config["subsample"],
        random_state=42,
    )
    
    # Cross-validation
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring="roc_auc")
    
    # Report to Ray Tune
    tune.report(mean_auc=cv_scores.mean(), std_auc=cv_scores.std())


# Configure search
search_space = {
    "n_estimators": tune.randint(100, 1000),
    "learning_rate": tune.loguniform(0.01, 0.3),
    "max_depth": tune.randint(2, 8),
    "subsample": tune.uniform(0.5, 1.0),
}

# ASHA: Asynchronous Successive Halving — aggressive pruning of bad trials
scheduler = ASHAScheduler(
    metric="mean_auc",
    mode="max",
    max_t=1000,       # Max n_estimators
    grace_period=100, # Minimum n_estimators before pruning
    reduction_factor=3,
)

optuna_search = OptunaSearch(metric="mean_auc", mode="max")

ray.init(num_cpus=16, num_gpus=2)

result = tune.run(
    train_with_ray,
    config=search_space,
    num_samples=200,       # Total trials
    scheduler=scheduler,
    search_alg=optuna_search,
    resources_per_trial={"cpu": 4},
    local_dir="ray_results/",
    verbose=1,
)

best_config = result.get_best_config(metric="mean_auc", mode="max")
print(f"Best config: {best_config}")
```

---

## Experiment Reproducibility

Reproducibility means: given the same code and data, get the same model.

```python
import random
import numpy as np
import torch
import os
import subprocess
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class ReproducibilityConfig:
    """All information needed to reproduce a training run."""
    
    # Code version
    git_sha: str
    git_branch: str
    requirements_hash: str  # Hash of requirements.txt
    
    # Data version
    data_s3_path: str
    data_sha256: str        # Hash of training data
    data_dvc_sha: Optional[str] = None
    
    # Random seeds
    python_seed: int = 42
    numpy_seed: int = 42
    torch_seed: int = 42
    sklearn_seed: int = 42
    
    # Environment
    python_version: str = ""
    cuda_version: Optional[str] = None
    
    # Hardware (affects numerical precision)
    hardware_type: str = "cpu"  # "cpu", "v100", "a100"


def setup_reproducibility(config: ReproducibilityConfig):
    """Set all random seeds for reproducibility."""
    random.seed(config.python_seed)
    np.random.seed(config.numpy_seed)
    torch.manual_seed(config.torch_seed)
    
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(config.torch_seed)
        # Deterministic cuDNN (slower but reproducible)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False
    
    os.environ["PYTHONHASHSEED"] = str(config.python_seed)


def capture_environment() -> ReproducibilityConfig:
    """Capture current environment for logging."""
    import hashlib, sys, importlib
    
    git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    git_branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode().strip()
    
    with open("requirements.txt", "rb") as f:
        req_hash = hashlib.md5(f.read()).hexdigest()
    
    return ReproducibilityConfig(
        git_sha=git_sha,
        git_branch=git_branch,
        requirements_hash=req_hash,
        data_s3_path="",  # Fill in during training
        data_sha256="",
        python_version=sys.version,
        cuda_version=torch.version.cuda if torch.cuda.is_available() else None,
        hardware_type="cuda" if torch.cuda.is_available() else "cpu",
    )


def log_reproducibility_config(config: ReproducibilityConfig):
    """Log all reproducibility info to MLflow."""
    mlflow.log_params({
        f"repro_{k}": str(v) for k, v in asdict(config).items()
    })


# Usage in training
repro_config = capture_environment()
setup_reproducibility(repro_config)

with mlflow.start_run(run_name="reproducible-training"):
    log_reproducibility_config(repro_config)
    # ... training code ...
```

---

## Interview Tips

> **Tip 1:** "How do you track experiments in distributed training without duplicate writes?" — "Designate rank 0 as the logging process. All other ranks call no-ops. Use a wrapper class that checks is_main_process() before every MLflow call. For metrics that need aggregation across GPUs (e.g., average loss), use dist.all_reduce() before logging. Never log from multiple ranks simultaneously — you'll get duplicate entries."

> **Tip 2:** "Optuna vs Ray Tune vs Ax — when do you choose each?" — "Optuna: excellent for mid-scale searches (< 1000 trials), easy to set up, great visualizations, integrates with MLflow natively. Ray Tune: when you need to scale to hundreds of concurrent trials across many machines, or need ASHA early stopping for deep learning. Ax (Facebook): when you need multi-objective optimization or have strict compute budgets. Most teams start with Optuna."

> **Tip 3:** "How do you make an ML experiment perfectly reproducible?" — "Five layers: (1) code — git commit hash, (2) environment — pinned requirements.txt, Docker image hash, (3) data — DVC SHA or S3 version ID, (4) seeds — set all random seeds (Python, NumPy, PyTorch, CUDA), (5) hardware — note GPU model because floating point operations are hardware-specific. Even with all this, GPU training has non-deterministic operations by default — set cudnn.deterministic=True."

> **Tip 4:** "What's the difference between Optuna's TPE and random search?" — "Random search samples hyperparameter combinations uniformly at random. TPE (Tree-structured Parzen Estimator) builds a probabilistic model of which hyperparameter regions produced good results, and samples more from those regions in subsequent trials — it's Bayesian optimization. With 50+ trials, TPE finds better configurations than random search with the same budget. For fewer than 20 trials, the overhead of TPE's modeling isn't worth it."

## ⚡ Cheat Sheet

**Distributed Training — Key Rule**
- Only **rank 0** logs to MLflow; all others call no-ops
- Aggregate metrics across GPUs with `dist.all_reduce(tensor, op=dist.ReduceOp.SUM)` before logging
- Save model only on `is_main_process()` — `model.module.state_dict()` (unwrap DDP)
- Backend: `nccl` for GPU-GPU; `gloo` for CPU or mixed

**HPO Tool Decision Matrix**
| Tool | Best For | Key Feature |
|---|---|---|
| Optuna (TPE) | Mid-scale (< 1000 trials), easy setup | Bayesian + pruning, MLflow native |
| Ray Tune + ASHA | Large-scale, many concurrent trials | Distributed, early stopping |
| Ax (Facebook) | Multi-objective, strict budget | Bayesian multi-objective |
| Grid search | ≤ 3 hyperparameters, small search space | Exhaustive |

**TPE vs Random Search**
- Random: uniform sampling regardless of history
- TPE (Tree-structured Parzen Estimator): builds probabilistic model of good regions, samples more there
- Break-even: ~50 trials — below that, random is competitive; above, TPE wins

**Reproducing an ML Experiment — 5 Layers**
1. Code: git commit SHA
2. Environment: pinned `requirements.txt` + Docker image hash
3. Data: DVC SHA or S3 version ID / Delta version
4. Seeds: Python, NumPy, PyTorch, CUDA (`cudnn.deterministic=True`)
5. Hardware: GPU model (floating point ops are hardware-specific)

**Key Optuna Patterns**
```python
study = optuna.create_study(
    direction="maximize",
    sampler=optuna.samplers.TPESampler(seed=42),
    pruner=optuna.pruners.HyperbandPruner(min_resource=50, max_resource=1000),
    storage="sqlite:///optuna.db",  # persists across sessions
    load_if_exists=True,
)
study.optimize(objective, n_trials=100, n_jobs=4)  # parallel
```

**MLflow Nested Runs Pattern**
- Parent run: search metadata (n_trials, search_type)
- Child runs: `mlflow.start_run(nested=True)` — one per trial
- Best result logged to parent after study completes
