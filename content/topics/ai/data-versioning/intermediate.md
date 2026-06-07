---
title: "AI - Data Versioning"
topic: ai
subtopic: data-versioning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ai, data-versioning, dvc-pipelines, delta-lake, data-registry, diffs]
---

# Data Versioning — Intermediate

## DVC Pipelines

DVC pipelines define ML workflows as stages with explicit dependencies and outputs, enabling reproducible, cached execution.

```yaml
# dvc.yaml
stages:
  download_data:
    cmd: python src/download_data.py --date ${item.date}
    params:
      - params.yaml:
          - data.source_bucket
          - data.date
    outs:
      - data/raw/${item.date}/transactions.parquet

  preprocess:
    cmd: python src/preprocess.py
    deps:
      - src/preprocess.py
      - data/raw/${item.date}/transactions.parquet
    params:
      - params.yaml:
          - preprocess.test_size
          - preprocess.min_transactions
    outs:
      - data/processed/train.parquet
      - data/processed/test.parquet
    metrics:
      - metrics/data_stats.json:
          cache: false

  train:
    cmd: python src/train.py
    deps:
      - src/train.py
      - data/processed/train.parquet
    params:
      - params.yaml:
          - model.n_estimators
          - model.learning_rate
          - model.max_depth
    outs:
      - models/churn_model.pkl
    metrics:
      - metrics/train_scores.json:
          cache: false

  evaluate:
    cmd: python src/evaluate.py
    deps:
      - src/evaluate.py
      - models/churn_model.pkl
      - data/processed/test.parquet
    metrics:
      - metrics/test_scores.json:
          cache: false
    plots:
      - metrics/roc_curve.csv:
          x: fpr
          y: tpr
          title: ROC Curve
      - metrics/pr_curve.csv:
          x: recall
          y: precision
```

```yaml
# params.yaml
data:
  source_bucket: "s3://raw-data/transactions"
  date: "2024-01-15"

preprocess:
  test_size: 0.2
  min_transactions: 5  # Minimum transactions per user

model:
  n_estimators: 200
  learning_rate: 0.05
  max_depth: 4
```

```bash
# Run pipeline — only changed stages execute
dvc repro

# Visualize pipeline DAG
dvc dag

# Run a specific stage and its dependencies
dvc repro train

# Force rerun all stages
dvc repro -f

# Dry run — show what would execute
dvc repro --dry
```

---

## Remote Storage Management

```bash
# ── S3 Remote ─────────────────────────────────────────────────────────
dvc remote add prod_remote s3://ml-data/dvc-store
dvc remote modify prod_remote region us-east-1
dvc remote modify prod_remote profile ml-prod

# ── GCS Remote ────────────────────────────────────────────────────────
dvc remote add gcs_remote gs://ml-data-bucket/dvc
dvc remote modify gcs_remote credentialpath /path/to/service-account.json

# ── Multi-remote setup ─────────────────────────────────────────────────
# Use 'prod_remote' as default, 'local_cache' for fast iteration
dvc remote add local_cache /data/dvc-cache
dvc remote default prod_remote  # Default for push/pull

# Push to specific remote
dvc push -r local_cache

# Cache configuration: shared cache across DVC repos
dvc config cache.dir /shared/dvc-cache  # NFS mount
dvc config cache.type symlink           # Symlinks save disk space
```

---

## Data Diffs and Comparisons

```python
# Compare two dataset versions
import pandas as pd
import numpy as np
from scipy.stats import ks_2samp

def compare_dataset_versions(v1_path: str, v2_path: str) -> dict:
    """
    Statistical comparison of two versions of the same dataset.
    Use after DVC checkout to compare versions.
    """
    df_v1 = pd.read_parquet(v1_path)
    df_v2 = pd.read_parquet(v2_path)
    
    report = {
        "v1_rows": len(df_v1),
        "v2_rows": len(df_v2),
        "row_delta": len(df_v2) - len(df_v1),
        "v1_cols": list(df_v1.columns),
        "v2_cols": list(df_v2.columns),
        "new_cols": list(set(df_v2.columns) - set(df_v1.columns)),
        "dropped_cols": list(set(df_v1.columns) - set(df_v2.columns)),
        "column_stats": {},
    }
    
    # Compare distributions for common numeric columns
    common_cols = list(set(df_v1.select_dtypes("number").columns) & 
                       set(df_v2.select_dtypes("number").columns))
    
    for col in common_cols:
        v1_col = df_v1[col].dropna()
        v2_col = df_v2[col].dropna()
        
        ks_stat, ks_pval = ks_2samp(v1_col, v2_col)
        
        report["column_stats"][col] = {
            "v1_mean": round(float(v1_col.mean()), 4),
            "v2_mean": round(float(v2_col.mean()), 4),
            "mean_pct_change": round((v2_col.mean() - v1_col.mean()) / (abs(v1_col.mean()) + 1e-10) * 100, 2),
            "v1_null_rate": round(float(df_v1[col].isna().mean()), 4),
            "v2_null_rate": round(float(df_v2[col].isna().mean()), 4),
            "distribution_shifted": ks_pval < 0.01,
            "ks_pvalue": round(ks_pval, 4),
        }
    
    # Significant shifts
    report["significant_shifts"] = [
        col for col, stats in report["column_stats"].items()
        if stats["distribution_shifted"]
    ]
    
    return report


# Usage
import subprocess

# Checkout v1
subprocess.run(["git", "checkout", "data-v1.0"])
subprocess.run(["dvc", "checkout"])

# Checkout v2 for comparison
subprocess.run(["git", "checkout", "data-v2.0"])
subprocess.run(["dvc", "checkout"])

# But compare both (use paths in cache or pull both versions)
diff = compare_dataset_versions(
    v1_path="data_v1/train.parquet",
    v2_path="data/processed/train.parquet"
)

print(f"Row count: {diff['v1_rows']:,} → {diff['v2_rows']:,} ({diff['row_delta']:+,})")
print(f"New columns: {diff['new_cols']}")
print(f"Significant distribution shifts: {diff['significant_shifts']}")
```

### DVC Metrics Comparison

```bash
# Compare metrics across git tags/branches
dvc metrics show                    # Current metrics
dvc metrics diff HEAD~1             # vs last commit
dvc metrics diff data-v1.0 data-v2.0  # vs specific versions

# Output:
# Path                       Metric      HEAD     data-v1.0   Change
# metrics/test_scores.json   test_auc    0.891    0.872       0.019
```

---

## DVCLive for Experiment Tracking

DVCLive integrates DVC with live experiment tracking.

```python
from dvclive import Live
from sklearn.metrics import roc_auc_score
import numpy as np

with Live(dir="dvclive") as live:
    # Log parameters
    live.log_param("n_estimators", 200)
    live.log_param("learning_rate", 0.05)
    
    for epoch in range(100):
        # ... train one epoch ...
        
        # Log metrics per step
        live.log_metric("train_loss", train_loss)
        live.log_metric("val_auc", val_auc)
        live.next_step()  # Increment step counter
    
    # Final metrics
    live.log_metric("test_auc", test_auc, timestamp=True)
    
    # Log model as artifact
    live.log_artifact("models/churn_model.pkl", type="model", name="churn-v3")

# Results available in dvclive/ directory:
# dvclive/metric_history/train_loss.tsv
# dvclive/metric_history/val_auc.tsv
# dvclive/metrics.json
# dvclive/report.html
```

---

## Interview Tips

> **Tip 1:** "How does DVC pipeline caching work?" — "DVC hashes the content of all stage dependencies (input files + code file + params). If the hash matches a previous run, DVC skips re-execution and restores outputs from the cache. This is content-addressed: the same inputs always map to the same cache key. Change one line of code or one byte of input data, and the cache misses."

> **Tip 2:** "What happens when two engineers simultaneously update the same DVC-tracked dataset?" — "DVC uses git for coordination. If both push new .dvc files for the same path, git detects a conflict in the pointer files and requires manual merge — just like any git conflict. The remote data isn't affected (it's content-addressed, both versions coexist). Resolve by choosing which .dvc pointer wins and push the resolution."

> **Tip 3:** "How do you use DVC to debug a model performance drop between two training runs?" — "Use dvc metrics diff to compare metrics, git log to find when they diverged, and the compare_dataset_versions function to identify which features shifted. The pattern: checkout the last good version (git + dvc checkout), run evaluate.py, then checkout the bad version and run again. DVC caching means only changed stages re-execute."

> **Tip 4:** "What's a DVC data registry and how does it reduce duplication?" — "A data registry is a dedicated git repo that contains only .dvc pointer files for datasets, organized by name and version. Other projects import from the registry using dvc import, which creates a tracked dependency. When the registry dataset updates, dependents can run dvc update to pull the latest version. This replaces ad-hoc S3 paths shared via Slack."
