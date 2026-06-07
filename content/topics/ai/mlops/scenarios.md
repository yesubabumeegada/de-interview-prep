---
title: "AI - MLOps"
topic: ai
subtopic: mlops
content_type: scenario_question
difficulty_level: beginner
layer: scenarios
tags: [ai, mlops, scenarios, stale-models, pipeline-failures, governance]
---

# MLOps — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Stale Model in Production

You join a team and discover their customer churn model hasn't been retrained in 8 months. The business changed significantly: a new pricing tier was introduced 6 months ago and 30% of customers are on it. The old model has no knowledge of this tier. Customer success reports churn predictions are "useless." How do you assess and fix the situation?

<details>
<summary>💡 Hint</summary>

Think about: (1) how to measure how stale the model actually is (not just calendar time), (2) what the quickest path to improvement is vs the proper long-term fix, and (3) what processes should have caught this earlier.

</details>

<details>
<summary>✅ Solution</summary>

### Step 1: Diagnose the Actual Degradation

```python
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
import joblib

# Load current production model
model = joblib.load("models/churn_model_v1.joblib")

# Load recent production data with known outcomes
recent_data = pd.read_parquet("s3://data/churn_outcomes_last_30_days.parquet")

# Segment by new tier
results = {}
for tier in ["basic", "premium", "enterprise_new"]:  # enterprise_new is the new tier
    subset = recent_data[recent_data["plan_type"] == tier]
    if len(subset) > 100:
        try:
            auc = roc_auc_score(
                subset["actually_churned"],
                model.predict_proba(subset.drop(["actually_churned", "user_id"], axis=1))[:, 1]
            )
        except Exception as e:
            auc = None
            print(f"Prediction error for tier {tier}: {e}")
        
        results[tier] = {
            "n": len(subset),
            "auc": auc,
            "churn_rate": subset["actually_churned"].mean(),
        }

print(pd.DataFrame(results).T)
# Expected output:
# tier              n      auc    churn_rate
# basic         45000    0.81      0.08
# premium       20000    0.77      0.06
# enterprise_new 18000   None      0.14   <- model fails on new tier (unseen category)
```

### Step 2: Quick Fix — Handle Unknown Categories

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

# The likely immediate issue: OHE was fit without handle_unknown="ignore"
# New tier causes ValueError in production

# Fix: retrain with handle_unknown="ignore"
enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)

# This allows the model to continue working even with unseen categories
# Prediction for new tier will use zeroed-out category columns
# Better than crashing, not as good as actually training on the tier
```

### Step 3: Proper Retraining with New Data

```python
import mlflow
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

# Load data from AFTER new tier launch to include real behavior
# Use data from past 6 months (covers new tier)
data = pd.read_parquet("s3://data/churn_training_6months.parquet")

X = data.drop("churned", axis=1)
y = data["churned"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Build new pipeline — handle_unknown is now correct
cat_cols = X.select_dtypes("object").columns.tolist()
num_cols = X.select_dtypes("number").columns.tolist()

preprocessor = ColumnTransformer([
    ("num", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("scl", StandardScaler()),
    ]), num_cols),
    ("cat", Pipeline([
        ("imp", SimpleImputer(strategy="constant", fill_value="missing")),
        ("enc", OneHotEncoder(handle_unknown="ignore")),  # KEY FIX
    ]), cat_cols),
])

pipeline = Pipeline([
    ("prep", preprocessor),
    ("model", GradientBoostingClassifier(n_estimators=300, random_state=42)),
])

with mlflow.start_run(run_name="churn-retrained-with-new-tier"):
    mlflow.set_tags({"reason": "new_pricing_tier", "data_range": "6m"})
    pipeline.fit(X_train, y_train)
    
    auc = roc_auc_score(y_test, pipeline.predict_proba(X_test)[:, 1])
    mlflow.log_metric("test_auc", auc)
    
    # Per-tier metrics
    for tier in X["plan_type"].unique():
        mask = X_test["plan_type"] == tier
        if mask.sum() > 50:
            tier_auc = roc_auc_score(
                y_test[mask],
                pipeline.predict_proba(X_test[mask])[:, 1]
            )
            mlflow.log_metric(f"auc_{tier}", tier_auc)
    
    mlflow.sklearn.log_model(pipeline, "model", registered_model_name="churn-classifier")
```

### Step 4: Prevention — Automated Monitoring

```python
# What should have caught this 6 months ago:
# 1. New category alert
# 2. Performance monitoring by segment

def setup_monitoring_alerts():
    """Configure alerts that would have caught this issue."""
    
    MONITORING_CONFIG = {
        # Alert on new unseen categories
        "new_category_alert": {
            "feature": "plan_type",
            "baseline_categories": ["basic", "premium"],
            "alert_threshold_pct": 5,  # Alert if >5% of traffic is new category
            "action": "trigger_retraining",
        },
        
        # Alert on AUC degradation
        "performance_degradation": {
            "metric": "auc_on_labeled_holdout",
            "baseline": 0.85,
            "alert_threshold": 0.80,  # Alert if drops below 0.80
            "window_days": 7,
            "action": "page_ml_team",
        },
        
        # Mandatory retraining schedule
        "scheduled_retraining": {
            "max_days_without_retraining": 60,  # Retrain at least every 2 months
            "action": "trigger_retraining",
        },
    }
    
    return MONITORING_CONFIG
```

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Data Pipeline Failure Breaking Model Retraining

Your automated daily retraining pipeline fails every morning with:
```
ERROR: Training data has 0 rows after filtering
ERROR: Feature engineering step produced NaN in 45% of rows
ERROR: Model training skipped — data quality gate failed
```

You're the on-call ML engineer. The production model is now 5 days stale because the pipeline has been failing silently. What's your investigation process and how do you prevent this in the future?

<details>
<summary>💡 Hint</summary>

Think about: (1) tracing the 0-row bug — what filter could cause this? (2) the 45% NaN issue — which upstream table is broken? (3) why was it "silent" for 5 days — what alerting was missing? (4) how to make the system fail loud, fast, and informatively.

</details>

<details>
<summary>✅ Solution</summary>

### Immediate Investigation

```python
# Run pipeline steps manually to find root cause

# Step 1: Check data availability
import boto3
from datetime import datetime, timedelta

s3 = boto3.client("s3")

for days_back in range(7):
    date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    response = s3.list_objects_v2(
        Bucket="data-bucket",
        Prefix=f"transactions/date={date}/"
    )
    object_count = response.get("KeyCount", 0)
    print(f"{date}: {object_count} objects")

# Output:
# 2024-01-20: 0 objects  <- BUG: missing data!
# 2024-01-19: 0 objects
# 2024-01-18: 0 objects
# 2024-01-17: 0 objects
# 2024-01-16: 0 objects  <- Last good date
# 2024-01-15: 847 objects
# 2024-01-14: 834 objects
```

```python
# Step 2: Trace the 0-row filter
# Training script filters on date range
def load_training_data(start_date: str, end_date: str):
    df = spark.read.parquet("s3://data-bucket/transactions/")
    
    # BUG: filter uses "date" column but source now uses "event_date"
    filtered = df.filter(
        (df["date"] >= start_date) &  # Column "date" doesn't exist!
        (df["date"] <= end_date)
    )
    # Silently returns 0 rows because the column filter drops everything
    return filtered

# Fix: Validate column names before filtering
def load_training_data_safe(start_date: str, end_date: str, spark):
    df = spark.read.parquet("s3://data-bucket/transactions/")
    
    # Assert expected columns exist
    expected_cols = {"event_date", "user_id", "amount", "merchant_id"}
    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols
    assert not missing, f"Missing columns: {missing}. Available: {actual_cols}"
    
    filtered = df.filter(
        (df["event_date"] >= start_date) &
        (df["event_date"] <= end_date)
    )
    
    row_count = filtered.count()
    assert row_count > 10_000, f"Too few rows: {row_count}. Expected >10K"
    
    return filtered
```

```python
# Step 3: Investigate 45% NaN issue
# Likely a join failure producing NaN for unmatched rows

def diagnose_nans(df):
    """Find which columns have high NaN rates and why."""
    nan_report = {}
    for col in df.columns:
        nan_rate = df[col].isna().mean()
        if nan_rate > 0.1:  # More than 10% NaN
            nan_report[col] = {
                "nan_rate": nan_rate,
                "dtype": str(df[col].dtype),
            }
    
    return nan_report

# Output will reveal: columns added via JOIN to merchant_features table have 45% NaN
# Root cause: merchant_features table has data for only 55% of merchants
# (New merchants added in last 5 days have no feature records yet)

# Fix: Left join with NULL handling
from pyspark.sql import functions as F

def join_with_merchant_features(transactions_df, merchant_features_df):
    result = transactions_df.join(merchant_features_df, "merchant_id", "left")
    
    # Fill NaN with defaults for new merchants
    result = result.withColumn(
        "merchant_avg_fraud_rate",
        F.coalesce(F.col("merchant_avg_fraud_rate"), F.lit(0.02))  # Global average
    ).withColumn(
        "merchant_age_days",
        F.coalesce(F.col("merchant_age_days"), F.lit(0))
    )
    
    return result
```

### Why Was It Silent for 5 Days?

```python
# The pipeline was alerting on SUCCESS, not FAILURE
# The data quality gate "failed" silently — it raised an exception that
# was caught by the pipeline runner and logged as "skipped training"
# with no notification

# Fix: Fail loud and notify

from airflow.operators.python import PythonOperator

def validate_training_data(**context):
    """
    Data validation that FAILS THE DAG on bad data.
    Never silently skip training.
    """
    df = load_training_data(
        start_date=context["ds"],
        end_date=context["ds"],
        spark=get_spark(),
    )
    
    row_count = df.count()
    nan_rate = df.select([
        F.mean(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    
    violations = []
    
    if row_count < 50_000:
        violations.append(f"Row count too low: {row_count} < 50,000")
    
    for col, rate in nan_rate.items():
        if rate > 0.20:  # >20% NaN for any column
            violations.append(f"Column {col} has {rate:.0%} NaN")
    
    if violations:
        # This raises an exception → Airflow marks task as FAILED → PagerDuty alert
        raise ValueError(
            f"Data quality validation FAILED on {context['ds']}:\n" +
            "\n".join(f"  - {v}" for v in violations)
        )
    
    print(f"Data validation passed: {row_count:,} rows, all columns within NaN threshold")
```

### Prevention: Data Contract Testing

```python
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite

def create_training_data_expectations():
    """Define expectations for training data schema and quality."""
    
    suite = ExpectationSuite("churn_training_data")
    
    # Schema expectations
    suite.add_expectation({
        "expectation_type": "expect_column_to_exist",
        "kwargs": {"column": "event_date"}
    })
    suite.add_expectation({
        "expectation_type": "expect_column_to_exist",
        "kwargs": {"column": "user_id"}
    })
    
    # Volume expectations
    suite.add_expectation({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 50_000, "max_value": 5_000_000}
    })
    
    # Completeness expectations
    suite.add_expectation({
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "user_id", "mostly": 1.0}
    })
    suite.add_expectation({
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "event_date", "mostly": 1.0}
    })
    suite.add_expectation({
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "amount", "mostly": 0.95}  # 95% non-null
    })
    
    # Value range expectations
    suite.add_expectation({
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "amount", "min_value": 0, "max_value": 100_000}
    })
    
    return suite
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Regulatory Audit of ML Models

Your company is a financial services firm. A regulator requests an audit of your credit scoring model. They want to know: (1) exactly what data was used to train the model, (2) what the model's performance is across demographic groups, (3) what would happen if a customer changes one input (counterfactual explanation), and (4) how you ensure the model is retrained appropriately. You have 2 weeks to prepare the audit response.

<details>
<summary>💡 Hint</summary>

Think about: (1) data lineage — can you trace training data back to source systems with timestamps? (2) fairness analysis — do you have disaggregated metrics? (3) explainability — SHAP, LIME, or counterfactual methods? (4) governance documentation — model cards, approval records, retraining logs.

</details>

<details>
<summary>✅ Solution</summary>

### Audit Response Framework

```python
from dataclasses import dataclass, field
from typing import List, Dict
from datetime import datetime

@dataclass
class AuditPackage:
    """Complete regulatory audit package for a production ML model."""
    
    model_name: str
    model_version: str
    audit_date: str
    
    # 1. Data lineage
    training_data_sources: List[dict]
    training_data_date_range: str
    data_preprocessing_steps: List[str]
    
    # 2. Performance by demographic group
    overall_metrics: Dict[str, float]
    metrics_by_group: Dict[str, Dict[str, float]]  # {"age_group": {"18-25": 0.84}}
    adverse_impact_analysis: dict
    
    # 3. Explainability artifacts
    feature_importance: Dict[str, float]
    shap_summary_plot_path: str
    counterfactual_examples: List[dict]
    
    # 4. Governance documentation
    model_card_path: str
    approval_history: List[dict]
    retraining_history: List[dict]
    monitoring_sla: dict


def generate_audit_package(model_name: str, model_version: str) -> AuditPackage:
    """Generate complete audit package from MLflow registry."""
    import mlflow
    
    client = mlflow.tracking.MlflowClient()
    
    # Get model version metadata
    mv = client.get_model_version(model_name, model_version)
    run = client.get_run(mv.run_id)
    
    return AuditPackage(
        model_name=model_name,
        model_version=model_version,
        audit_date=datetime.utcnow().strftime("%Y-%m-%d"),
        
        training_data_sources=[
            {
                "table": run.data.tags.get("source_table_1", "credit_applications"),
                "s3_path": run.data.tags.get("data_s3_path"),
                "row_count": int(run.data.params.get("n_train", 0)),
                "date_range": run.data.tags.get("data_date_range"),
            }
        ],
        training_data_date_range=run.data.tags.get("training_data_range", ""),
        data_preprocessing_steps=[
            "1. Remove applicants under 18",
            "2. Impute missing income with median by zip code",
            "3. StandardScaler for numeric features",
            "4. OneHotEncoder for categorical features",
        ],
        overall_metrics={k: v for k, v in run.data.metrics.items()},
        metrics_by_group={},  # Populated below
        adverse_impact_analysis={},
        feature_importance={},
        shap_summary_plot_path="",
        counterfactual_examples=[],
        model_card_path=run.data.tags.get("model_card_url", ""),
        approval_history=[],
        retraining_history=[],
        monitoring_sla={},
    )
```

### Question 1: Data Lineage

```python
class DataLineageTracer:
    """Trace training data back to source systems."""
    
    def get_lineage_report(self, model_name: str, model_version: str) -> dict:
        import mlflow
        
        client = mlflow.tracking.MlflowClient()
        mv = client.get_model_version(model_name, model_version)
        run = client.get_run(mv.run_id)
        tags = run.data.tags
        
        return {
            "model": f"{model_name} v{model_version}",
            "trained_on": run.info.start_time,
            "by_team": tags.get("team"),
            "git_sha": tags.get("git_sha"),
            "source_tables": [
                {
                    "name": "credit_applications",
                    "database": "dwh",
                    "s3_snapshot": tags.get("train_data_s3"),
                    "row_count": run.data.params.get("n_train"),
                    "date_range": tags.get("data_date_range"),
                    "schema_version": tags.get("schema_version"),
                }
            ],
            "feature_computation": tags.get("feature_pipeline_version"),
            "dvc_pipeline_sha": tags.get("dvc_pipeline_sha"),
        }
```

### Question 2: Fairness Analysis

```python
from sklearn.metrics import roc_auc_score, selection_rate
import pandas as pd
import numpy as np

def compute_fairness_metrics(model, test_data: pd.DataFrame, label_col: str) -> dict:
    """
    Compute fairness metrics across demographic groups.
    Required for regulatory compliance.
    """
    
    X = test_data.drop([label_col, "protected_attrs"], axis=1, errors="ignore")
    y = test_data[label_col]
    
    scores = model.predict_proba(X)[:, 1]
    predictions = (scores >= 0.5).astype(int)
    
    fairness_report = {}
    
    for protected_attr in ["age_group", "gender", "race_ethnicity"]:
        if protected_attr not in test_data.columns:
            continue
        
        group_metrics = {}
        groups = test_data[protected_attr].unique()
        
        approval_rates = {}
        
        for group in groups:
            mask = test_data[protected_attr] == group
            group_data = test_data[mask]
            
            group_scores = scores[mask]
            group_preds = predictions[mask]
            group_labels = y[mask]
            
            auc = roc_auc_score(group_labels, group_scores) if group_labels.nunique() > 1 else None
            approval_rate = (group_preds == 0).mean()  # 0 = no default = approved
            
            group_metrics[group] = {
                "n": int(mask.sum()),
                "approval_rate": round(float(approval_rate), 4),
                "auc": round(float(auc), 4) if auc else None,
                "base_rate": round(float(group_labels.mean()), 4),
            }
            approval_rates[group] = approval_rate
        
        # Adverse Impact Ratio (80% rule: minority rate / majority rate >= 0.8)
        max_rate = max(approval_rates.values())
        air_violations = {
            g: round(r / max_rate, 4)
            for g, r in approval_rates.items()
            if r / max_rate < 0.8
        }
        
        fairness_report[protected_attr] = {
            "groups": group_metrics,
            "adverse_impact_violations": air_violations,
            "meets_80pct_rule": len(air_violations) == 0,
        }
    
    return fairness_report
```

### Question 3: Counterfactual Explanations

```python
def generate_counterfactual(model, instance: dict, desired_outcome: int = 0) -> dict:
    """
    Generate counterfactual: 'What would need to change for this applicant to be approved?'
    
    Uses DiCE (Diverse Counterfactual Explanations) approach.
    """
    import dice_ml
    import pandas as pd
    
    # DiCE requires knowing which features are mutable
    mutable_features = ["income", "employment_years", "debt_amount"]
    immutable_features = ["age", "gender", "race_ethnicity"]  # Should NOT be mutable
    
    d = dice_ml.Data(
        dataframe=pd.DataFrame([instance]),
        continuous_features=["income", "employment_years", "debt_amount"],
        outcome_name="approved"
    )
    
    m = dice_ml.Model(model=model, backend="sklearn")
    exp = dice_ml.Dice(d, m)
    
    counterfactuals = exp.generate_counterfactuals(
        query_instances=pd.DataFrame([instance]),
        total_CFs=3,
        desired_class=desired_outcome,
        features_to_vary=mutable_features,
    )
    
    return {
        "original": instance,
        "current_decision": "denied" if model.predict([list(instance.values())])[0] == 1 else "approved",
        "counterfactuals": counterfactuals.cf_examples_list[0].final_cfs_df.to_dict("records"),
        "explanation": "Showing 3 minimal changes that would result in approval",
    }

# Example output:
# {
#   "original": {"income": 45000, "employment_years": 1, "debt_amount": 25000},
#   "current_decision": "denied",
#   "counterfactuals": [
#     {"income": 62000, "employment_years": 1, "debt_amount": 25000},   # Increase income
#     {"income": 45000, "employment_years": 3, "debt_amount": 25000},   # More tenure
#     {"income": 45000, "employment_years": 1, "debt_amount": 15000},   # Reduce debt
#   ]
# }
```

### Question 4: Governance Documentation

```python
def generate_retraining_history(model_name: str) -> list:
    """Pull complete retraining history from MLflow."""
    import mlflow
    
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    
    history = []
    for v in sorted(versions, key=lambda x: x.version):
        run = client.get_run(v.run_id)
        
        history.append({
            "version": v.version,
            "created": datetime.fromtimestamp(v.creation_timestamp/1000).strftime("%Y-%m-%d %H:%M"),
            "stage": v.current_stage,
            "auc": run.data.metrics.get("test_auc"),
            "trigger": run.data.tags.get("trigger", "unknown"),
            "approved_by": v.tags.get("approved_by"),
            "data_date_range": run.data.tags.get("data_date_range"),
            "git_sha": run.data.tags.get("git_sha"),
        })
    
    return history
```

</details>
</article>
