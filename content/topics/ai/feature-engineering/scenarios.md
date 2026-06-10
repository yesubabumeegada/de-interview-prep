---
title: "AI - Feature Engineering"
topic: ai
subtopic: feature-engineering
content_type: scenario_question
difficulty_level: junior
layer: scenarios
tags: [ai, feature-engineering, scenarios, leakage, skew, feature-store]
---

# Feature Engineering — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Feature Leakage Investigation

You're training a credit default model and achieve 0.96 AUC. Your manager is skeptical — typical models in this domain score 0.72-0.78 AUC. You look at the top features:

```python
import shap

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)
shap.summary_plot(shap_values, X_test, max_display=10)
```

The top 3 features by SHAP importance are:
1. `collection_agency_assigned` (importance: 0.34)
2. `days_past_due_final` (importance: 0.28)
3. `account_closed_reason` (importance: 0.19)

Your target variable is `defaulted_within_12_months`. What's wrong, and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about the timing of when each feature is recorded vs when the label is assigned. Collection agencies, days past due, and account closed reason — when are these values populated in the database? Could they only exist AFTER a default has already occurred?

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause: Target Leakage (Label Leakage)

All three top features are consequences of default, not predictors of default:
- `collection_agency_assigned`: only populated AFTER a default is sent to collections
- `days_past_due_final`: records the FINAL delinquency status — only known after the default period ends
- `account_closed_reason = "default"`: explicitly encodes the label

The model has essentially learned to predict default from post-default data. In production, these features don't exist at prediction time (when the loan is still active).

### How to Detect Leakage

```python
import pandas as pd
import numpy as np

def check_feature_leakage(df: pd.DataFrame, target: str, features: list) -> pd.DataFrame:
    """
    Heuristics to detect potential target leakage.
    """
    results = []
    
    for feat in features:
        # 1. Correlation with target (suspiciously high = leakage risk)
        corr = df[feat].corr(df[target])
        
        # 2. Missingness pattern: high null rate for non-events
        null_rate_by_target = df.groupby(target)[feat].apply(lambda x: x.isna().mean())
        
        # 3. Near-perfect conditional distribution
        if df[feat].dtype == object:
            # Check if any category value perfectly predicts target
            conditional = df.groupby(feat)[target].mean()
            max_conditional = conditional.max()
        else:
            max_conditional = 0.0
        
        results.append({
            "feature": feat,
            "correlation_with_target": round(abs(corr), 4),
            "null_rate_target_0": round(null_rate_by_target.get(0, 0), 4),
            "null_rate_target_1": round(null_rate_by_target.get(1, 0), 4),
            "max_conditional_target_rate": round(max_conditional, 4),
            "leakage_risk": "HIGH" if abs(corr) > 0.5 or max_conditional > 0.8 else "LOW",
        })
    
    return pd.DataFrame(results).sort_values("correlation_with_target", ascending=False)

leakage_report = check_feature_leakage(df, "defaulted_within_12_months", feature_cols)
print(leakage_report[leakage_report["leakage_risk"] == "HIGH"])
```

### The Fix: Temporal Feature Isolation

```python
def build_leakage_safe_features(df: pd.DataFrame, prediction_date: str) -> pd.DataFrame:
    """
    Only include features available BEFORE the prediction date.
    """
    prediction_ts = pd.to_datetime(prediction_date)
    
    # Only use loan application data + pre-prediction behavioral data
    safe_features = [
        # Application-time features (always available)
        "loan_amount", "annual_income", "debt_to_income",
        "credit_score_at_origination", "employment_years",
        
        # Pre-prediction behavioral features (with timestamp check)
        "payment_history_12m",          # computed from payments before prediction_date
        "average_utilization_6m",       # computed from pre-prediction balances
        "inquiry_count_6m",             # hard inquiries before prediction_date
        "delinquency_count_24m",        # past delinquencies, not current
        
        # DO NOT include
        # "collection_agency_assigned",  <- post-default
        # "days_past_due_final",         <- post-default
        # "account_closed_reason",       <- post-default
    ]
    
    return df[safe_features + ["defaulted_within_12_months"]]
```

### Prevention: Feature Timestamp Registry

```python
# Document when each feature is "valid from" in a feature registry
FEATURE_REGISTRY = {
    "loan_amount": {"available_at": "application_time", "risk": "none"},
    "credit_score_at_origination": {"available_at": "origination_time", "risk": "none"},
    "days_past_due_final": {"available_at": "account_close_time", "risk": "HIGH_LEAKAGE"},
    "collection_agency_assigned": {"available_at": "post_default", "risk": "HIGH_LEAKAGE"},
    "payment_history_12m": {"available_at": "monthly_aggregation", "risk": "low"},
}

# Auto-filter high-risk features in pipeline
safe_features = [
    feat for feat, meta in FEATURE_REGISTRY.items()
    if meta["risk"] not in ("HIGH_LEAKAGE",)
]
```

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Skewed Feature Distribution Killing Model Performance

You're building an income prediction model. The model achieves poor performance and you investigate the income feature distribution:

```python
import matplotlib.pyplot as plt
import numpy as np

income = df["annual_income"]
print(income.describe())
# count    50000
# mean     85000
# std      210000
# min      15000
# 25%      35000
# 50%      55000
# 75%      90000
# max      8500000   <- billionaire outlier

# Skewness: 12.3 (heavily right-skewed)
print(f"Skewness: {income.skew():.1f}")
```

The model is using `StandardScaler` and linear regression. Performance is poor and income has almost no impact in the model despite being the most predictive variable. What's happening and what's your systematic approach?

<details>
<summary>💡 Hint</summary>

Think about what StandardScaler does with an extreme outlier that has 100x the mean. What does the resulting feature look like for 99% of the data? Consider log transformation and robust scaling alternatives.

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause: Outlier Compression

With `income.max() = $8.5M` and `income.mean() = $85K`:

```python
# StandardScaler: (x - mean) / std
mean, std = 85_000, 210_000

# $8.5M billionaire
billionaire_scaled = (8_500_000 - mean) / std  # = 40.07

# $55K median earner  
median_scaled = (55_000 - mean) / std  # = -0.14

# $90K 75th percentile
p75_scaled = (90_000 - mean) / std  # = 0.024
```

After scaling, 99% of incomes are compressed into the range [-0.4, 0.5], while the billionaire sits at 40. The linear model weights this feature to be small because one outlier dominates — any coefficient that fits the billionaire would catastrophically misfit everyone else.

### Solution 1: Log Transform

```python
import numpy as np
import pandas as pd
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.pipeline import Pipeline

# Log1p handles zero values (log(1+x))
log_transformer = FunctionTransformer(
    func=np.log1p,
    inverse_func=np.expm1,
    validate=True,
)

# After log transform, skewness drops dramatically
income_log = np.log1p(df["annual_income"])
print(f"Log-transformed skewness: {income_log.skew():.2f}")  # ~0.3

# Build into pipeline
income_pipeline = Pipeline([
    ("log", FunctionTransformer(np.log1p, np.expm1)),
    ("scale", StandardScaler()),
])
```

### Solution 2: RobustScaler (Preferred for Outlier-Heavy Data)

```python
from sklearn.preprocessing import RobustScaler

# Uses median and IQR — immune to extreme outliers
# (x - median) / IQR
scaler = RobustScaler(quantile_range=(10.0, 90.0))  # Use 10th-90th percentile IQR

income_scaled = scaler.fit_transform(df[["annual_income"]])
print(f"After RobustScaler:")
print(f"  Median person scaled: {income_scaled[df.annual_income == 55000][0]:.2f}")  # ~0
print(f"  Billionaire scaled:   {income_scaled[df.annual_income == 8_500_000][0]:.2f}")  # ~20 (not 40)
```

### Solution 3: Yeo-Johnson Power Transform

```python
from sklearn.preprocessing import PowerTransformer

# Auto-selects optimal power to make data more Gaussian
pt = PowerTransformer(method="yeo-johnson", standardize=True)
income_transformed = pt.fit_transform(df[["annual_income"]])

print(f"Yeo-Johnson transformed skewness: {pd.Series(income_transformed.flatten()).skew():.2f}")
```

### Systematic Feature Distribution Analysis

```python
from scipy.stats import normaltest, skew, kurtosis
import pandas as pd
import numpy as np

def analyze_feature_distributions(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    """Flag features that need transformation before modeling."""
    results = []
    
    for col in numeric_cols:
        series = df[col].dropna()
        
        stat, pval = normaltest(series)
        sk = skew(series)
        kurt = kurtosis(series)
        
        # Outlier count (beyond 3 std from mean)
        z_scores = np.abs((series - series.mean()) / series.std())
        n_outliers = (z_scores > 3).sum()
        
        recommendation = "none"
        if abs(sk) > 2:
            recommendation = "log_transform" if sk > 0 else "sqrt_transform"
        if n_outliers > len(series) * 0.01:  # More than 1% outliers
            recommendation = "robust_scaler"
        
        results.append({
            "feature": col,
            "skewness": round(sk, 3),
            "kurtosis": round(kurt, 3),
            "n_outliers_3std": n_outliers,
            "normality_pvalue": round(pval, 4),
            "is_normal": pval > 0.05,
            "recommendation": recommendation,
        })
    
    return pd.DataFrame(results).sort_values("skewness", key=abs, ascending=False)

report = analyze_feature_distributions(df, numeric_cols)
print(report[report["recommendation"] != "none"])
```

### Complete Fixed Pipeline

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import PowerTransformer, RobustScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge

# Categorize features by transformation needed
log_transform_cols = ["annual_income", "loan_amount", "total_debt"]
robust_scale_cols = ["years_employed", "num_accounts"]
standard_cols = ["credit_score", "payment_history_12m"]
cat_cols = ["employment_type", "loan_purpose"]

preprocessor = ColumnTransformer([
    ("log_cols", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("log", FunctionTransformer(np.log1p, np.expm1)),
        ("scl", StandardScaler()),
    ]), log_transform_cols),
    ("robust_cols", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("scl", RobustScaler()),
    ]), robust_scale_cols),
    ("std_cols", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("scl", StandardScaler()),
    ]), standard_cols),
    ("cat", Pipeline([
        ("imp", SimpleImputer(strategy="most_frequent")),
        ("enc", OneHotEncoder(handle_unknown="ignore")),
    ]), cat_cols),
])

pipeline = Pipeline([
    ("prep", preprocessor),
    ("model", Ridge(alpha=1.0)),
])

pipeline.fit(X_train, y_train)
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Feature Store Design for a Multi-Team Organization

Your company has 8 ML teams, each independently computing similar features from the same data warehouse. Teams report these problems:
- Churn model and recommendation model compute "user activity score" with different formulas
- Every team runs expensive Spark jobs independently — $200K/month in duplicate compute
- When a new analyst joins, onboarding to feature engineering takes 3 weeks
- An audit found that 3 different models use "30-day revenue" features computed differently, causing inconsistent decisions across products

Design a centralized feature store architecture for this organization.

<details>
<summary>💡 Hint</summary>

Think about: (1) governance and ownership — who "owns" a feature? (2) compute efficiency — batch once, serve many times. (3) discovery — how do teams find existing features instead of creating duplicates? (4) consistency guarantees — how do you ensure the same feature returns the same value in training and serving?

</details>

<details>
<summary>✅ Solution</summary>

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Feature Platform                                  │
│                                                                       │
│  Feature Registry (Metadata)                                          │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ Feature: user_activity_score_30d                               │  │
│  │ Owner: growth-team                                             │  │
│  │ Formula: git://feature-repo/user_stats.py:activity_score_30d  │  │
│  │ Tags: user, activity, approved                                 │  │
│  │ SLA: 4 hours freshness                                         │  │
│  │ Consumers: churn-v3, reco-v12, upsell-v2                       │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Computation Layer                    Storage Layer                   │
│  ┌─────────────────────────┐         ┌──────────────────────────┐   │
│  │ Batch (Spark/dbt)       │────────►│ Offline Store            │   │
│  │ - Runs once per feature │         │ Delta Lake on S3          │   │
│  │ - Shared across teams   │         │ Partitioned by date+entity│   │
│  │                         │         └──────────────────────────┘   │
│  │ Stream (Flink)          │─────┐   ┌──────────────────────────┐   │
│  │ - Real-time features    │     └──►│ Online Store             │   │
│  │                         │         │ Redis Cluster            │   │
│  └─────────────────────────┘         │ DynamoDB (large scale)   │   │
│                                      └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Feature Registry Design

```python
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum

class FeatureStatus(str, Enum):
    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    DEPRECATED = "deprecated"

class DataType(str, Enum):
    FLOAT32 = "float32"
    INT64 = "int64"
    STRING = "string"
    BOOL = "bool"

@dataclass
class FeatureDefinition:
    name: str
    entity: str                           # "user_id", "product_id"
    data_type: DataType
    description: str
    formula: str                          # Git path to computation code
    owner_team: str
    status: FeatureStatus
    
    # Computation metadata
    freshness_sla_hours: float            # Max acceptable staleness
    computation_type: str                 # "batch" | "streaming" | "on_demand"
    
    # Lineage
    source_tables: List[str]              # Upstream data sources
    upstream_features: List[str] = field(default_factory=list)  # Feature-to-feature deps
    
    # Consumers
    consuming_models: List[str] = field(default_factory=list)
    consuming_teams: List[str] = field(default_factory=list)
    
    # Audit
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_updated_at: datetime = field(default_factory=datetime.utcnow)
    approved_by: Optional[str] = None
    
    # Cost tracking
    compute_cost_per_run_usd: Optional[float] = None
    
    tags: List[str] = field(default_factory=list)

# Example feature definitions
FEATURES = {
    "user_activity_score_30d": FeatureDefinition(
        name="user_activity_score_30d",
        entity="user_id",
        data_type=DataType.FLOAT32,
        description="Composite activity score based on logins, purchases, and support contacts over 30 days. "
                    "Higher = more active. Range: 0-1.",
        formula="git://feature-repo/user_features/activity.py:compute_activity_score_30d",
        owner_team="growth-platform",
        status=FeatureStatus.APPROVED,
        freshness_sla_hours=4.0,
        computation_type="batch",
        source_tables=["dwh.user_events", "dwh.transactions", "dwh.support_tickets"],
        consuming_models=["churn-v3", "recommendation-v12", "upsell-v2"],
        consuming_teams=["churn-team", "reco-team", "growth-team"],
        tags=["user", "activity", "core", "pii-free"],
    ),
}
```

### Feature Discovery Interface

```python
class FeatureDiscovery:
    """
    Searchable catalog for ML teams to find existing features.
    Goal: teams find and reuse before creating new features.
    """
    
    def search(
        self,
        query: str = "",
        entity: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owner_team: Optional[str] = None,
        status: FeatureStatus = FeatureStatus.APPROVED,
    ) -> List[FeatureDefinition]:
        """Search the feature registry."""
        results = list(FEATURES.values())
        
        if query:
            results = [f for f in results if query.lower() in f.name.lower() 
                      or query.lower() in f.description.lower()]
        
        if entity:
            results = [f for f in results if f.entity == entity]
        
        if tags:
            results = [f for f in results if any(t in f.tags for t in tags)]
        
        if status:
            results = [f for f in results if f.status == status]
        
        return results
    
    def get_feature_lineage(self, feature_name: str) -> dict:
        """Return full lineage graph for a feature."""
        feature = FEATURES[feature_name]
        return {
            "feature": feature_name,
            "source_tables": feature.source_tables,
            "upstream_features": feature.upstream_features,
            "consuming_models": feature.consuming_models,
            "owner": feature.owner_team,
            "formula": feature.formula,
        }

# Usage: new team member searches before building
discovery = FeatureDiscovery()
activity_features = discovery.search(query="activity", entity="user_id")
for f in activity_features:
    print(f"{f.name} (owner: {f.owner_team}): {f.description[:80]}...")
```

### Governance: Feature Pull Request Workflow

```yaml
# .github/workflows/feature_review.yml
name: Feature Definition Review

on:
  pull_request:
    paths:
      - "feature_repo/**"

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Check feature definition completeness
        run: python scripts/validate_feature_definition.py

      - name: Check for duplicate features
        run: python scripts/check_duplicate_features.py

      - name: Run feature unit tests
        run: pytest feature_repo/tests/

      - name: Estimate compute cost
        run: python scripts/estimate_feature_cost.py

      - name: Require data team approval
        uses: actions/github-script@v6
        with:
          script: |
            github.rest.pulls.requestReviewers({
              owner: context.repo.owner,
              repo: context.repo.repo,
              pull_number: context.issue.number,
              team_reviewers: ['data-platform-team']
            })
```

### Compute Efficiency: Shared Job Scheduling

```python
class SharedFeatureScheduler:
    """
    Schedule feature computation jobs once per feature, shared across all teams.
    Eliminates duplicate compute.
    """
    
    def get_daily_schedule(self) -> List[dict]:
        """Generate optimized daily computation schedule."""
        jobs = []
        
        # Group features by entity and source tables to maximize batching
        from itertools import groupby
        
        approved_features = [f for f in FEATURES.values() 
                            if f.status == FeatureStatus.APPROVED
                            and f.computation_type == "batch"]
        
        # Sort by entity, then group
        by_entity = sorted(approved_features, key=lambda f: (f.entity, str(f.source_tables)))
        
        for (entity, sources), features_iter in groupby(by_entity, key=lambda f: (f.entity, str(f.source_tables))):
            features = list(features_iter)
            
            # Compute all features for this entity in one Spark job
            jobs.append({
                "job_name": f"compute_features_{entity}_{hash(sources)}",
                "entity": entity,
                "source_tables": features[0].source_tables,
                "features_to_compute": [f.name for f in features],
                "estimated_cost_usd": sum(f.compute_cost_per_run_usd or 10 for f in features) * 0.3,  # 70% savings
            })
        
        return jobs

# Projected savings
scheduler = SharedFeatureScheduler()
optimized_jobs = scheduler.get_daily_schedule()
print(f"Optimized to {len(optimized_jobs)} shared jobs")
print(f"Estimated monthly savings: $140K (70% reduction from $200K)")
```

### Consistency Testing

```python
def run_consistency_check(feature_name: str, entity_ids: List[int], date: str) -> dict:
    """
    Verify that offline and online feature values match.
    Run nightly as part of feature platform monitoring.
    """
    store = FeatureStore(repo_path="feature_repo/")
    redis = redis.Redis.from_url(os.getenv("REDIS_URL"))
    
    discrepancies = []
    
    for entity_id in entity_ids:
        # Get offline value
        entity_df = pd.DataFrame({
            "user_id": [entity_id],
            "event_timestamp": [pd.Timestamp(date)],
        })
        offline_val = store.get_historical_features(
            entity_df=entity_df,
            features=[f"user_features:{feature_name}"],
        ).to_df()[feature_name].iloc[0]
        
        # Get online value
        online_val = float(redis.hget(f"user:{entity_id}:features", feature_name) or 0)
        
        # Allow 0.1% tolerance for floating point
        if abs(offline_val - online_val) / (abs(offline_val) + 1e-10) > 0.001:
            discrepancies.append({
                "entity_id": entity_id,
                "offline": offline_val,
                "online": online_val,
                "abs_diff": abs(offline_val - online_val),
                "rel_diff": abs(offline_val - online_val) / (abs(offline_val) + 1e-10),
            })
    
    return {
        "feature": feature_name,
        "checked": len(entity_ids),
        "consistent": len(entity_ids) - len(discrepancies),
        "discrepant": len(discrepancies),
        "consistency_rate": 1 - len(discrepancies) / len(entity_ids),
        "discrepancies": discrepancies[:10],  # First 10 examples
    }
```

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is feature leakage and how do you detect it?**
A: Feature leakage occurs when information from the future or from the target variable inadvertently enters the training features, causing inflated evaluation metrics. Detect it by checking feature creation timestamps relative to the label timestamp, validating that features are available at prediction time, and looking for suspiciously high feature-target correlations.

**Q: What is the difference between a feature store's online store and offline store?**
A: The offline store (e.g., S3 + Parquet, Delta Lake) stores historical feature values at scale for training dataset generation. The online store (e.g., Redis, DynamoDB) serves low-latency point lookups of the latest feature values for real-time model inference.

**Q: How do you handle high-cardinality categorical features?**
A: Options include target encoding (replace category with mean target), frequency encoding, entity embeddings (learned in a neural network), or hashing (feature hashing trick). The right choice depends on cardinality magnitude, downstream model type, and leakage risk with target encoding.

**Q: What is point-in-time correct feature retrieval and why is it essential?**
A: Point-in-time correct retrieval ensures that when constructing a training row for a given event timestamp, only feature values that were known *before* that timestamp are used. Without it, future information leaks into training data, making offline metrics optimistic and production performance disappointing.

**Q: What are the trade-offs between computing features at training time vs. pre-computing and storing them?**
A: Computing at training time avoids storage costs and is always fresh, but is slow and may be inconsistent with serving logic. Pre-computing ensures training-serving consistency and fast retrieval, but adds infrastructure complexity and storage overhead.

**Q: How would you handle missing values in a feature used by a production model?**
A: Impute with a statistically sound strategy (median, mode, or model-based), but always log missingness as a separate binary feature to preserve its signal. In production, add monitoring for missing rate changes — a spike often signals upstream data issues.

**Q: What is feature interaction and when should you engineer it explicitly?**
A: Feature interactions capture combined effects of two or more features that aren't captured individually (e.g., age × income). Explicit engineering is most valuable for linear models and tree models with depth limits. Deep learning models can learn interactions automatically, so explicit engineering matters less there.

**Q: How do you validate that a new feature actually improves model quality?**
A: Run an ablation study — compare model performance with and without the feature, using the same train/validation/test split and holdout. Also measure: feature importance scores, permutation importance, and the feature's contribution to prediction confidence on a held-out set. Confirm improvements hold on production data distribution.

---

## 💼 Interview Tips

- Lead with feature leakage when discussing feature engineering — it's the most common source of "great offline metrics, terrible production performance" and signals production experience to interviewers.
- When discussing feature stores, frame your answer around the offline/online separation and training-serving consistency — these are the core architectural decisions, not just tooling choices.
- Senior interviewers want to hear about feature governance: versioning, documentation, reuse across teams, and deprecation policies. Feature engineering at scale is as much an organizational problem as a technical one.
- Avoid presenting one-size-fits-all approaches — always mention that the right encoding or imputation strategy depends on the model type, data distribution, and business context.
- Bring up monitoring: a feature that works today may drift tomorrow. Mentioning feature distribution monitoring (mean, variance, null rate) alongside model monitoring shows end-to-end thinking.
- When asked about embedding features, mention that they create training-serving coupling — the embedding model version at training time must match the version at serving time.
