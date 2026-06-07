---
title: "AI - Bias and Fairness"
topic: ai
subtopic: bias-and-fairness
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ai, bias, fairness, fairlearn, aif360, disparate-impact, debiasing]
---

# Bias and Fairness — Intermediate

## Fairlearn

Microsoft's Fairlearn provides tools to assess and mitigate fairness issues in ML models.

```python
from fairlearn.metrics import (
    MetricFrame,
    demographic_parity_difference,
    equalized_odds_difference,
    selection_rate,
    false_positive_rate,
    false_negative_rate,
)
from fairlearn.reductions import ExponentiatedGradient, DemographicParity, EqualizedOdds
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np

# Load test data
X_test = pd.read_parquet("data/test_features.parquet")
y_test = pd.read_parquet("data/test_labels.parquet")["label"]
protected = X_test["gender"]  # Sensitive feature

y_pred = model.predict(X_test.drop("gender", axis=1))

# Comprehensive fairness assessment
metrics = {
    "accuracy": accuracy_score,
    "selection_rate": selection_rate,
    "false_positive_rate": false_positive_rate,
    "false_negative_rate": false_negative_rate,
}

metric_frame = MetricFrame(
    metrics=metrics,
    y_true=y_test,
    y_pred=y_pred,
    sensitive_features=protected,
)

print("Overall metrics:")
print(metric_frame.overall)
print("\nMetrics by gender:")
print(metric_frame.by_group)

print(f"\nDemographic Parity Difference: {demographic_parity_difference(y_test, y_pred, sensitive_features=protected):.4f}")
print(f"Equalized Odds Difference: {equalized_odds_difference(y_test, y_pred, sensitive_features=protected):.4f}")
```

### In-Processing: Fairness Constraints During Training

```python
from fairlearn.reductions import ExponentiatedGradient, DemographicParity, EqualizedOdds

base_model = GradientBoostingClassifier(n_estimators=200, random_state=42)

# Demographic Parity constraint
constraint = DemographicParity()
mitigator = ExponentiatedGradient(
    estimator=base_model,
    constraints=constraint,
    max_iter=50,
)

# Note: sensitive_features passed during fit, NOT in X
mitigator.fit(
    X_train.drop("gender", axis=1),
    y_train,
    sensitive_features=X_train["gender"],
)

y_pred_fair = mitigator.predict(X_test.drop("gender", axis=1))

# Compare original vs fair model
print("Original model:")
print(f"  Accuracy: {accuracy_score(y_test, y_pred):.4f}")
print(f"  DP Diff: {demographic_parity_difference(y_test, y_pred, sensitive_features=protected):.4f}")

print("\nFair model (with DP constraint):")
print(f"  Accuracy: {accuracy_score(y_test, y_pred_fair):.4f}")
print(f"  DP Diff: {demographic_parity_difference(y_test, y_pred_fair, sensitive_features=protected):.4f}")
```

---

## IBM AIF360

AIF360 provides a comprehensive suite of bias detection and mitigation algorithms.

```python
from aif360.datasets import BinaryLabelDataset
from aif360.metrics import BinaryLabelDatasetMetric, ClassificationMetric
from aif360.algorithms.preprocessing import Reweighing
from aif360.algorithms.inprocessing import PrejudiceRemover
from aif360.algorithms.postprocessing import EqOddsPostprocessing, CalibratedEqOddsPostprocessing
import pandas as pd
import numpy as np

# Convert pandas DataFrame to AIF360 format
def to_aif360_dataset(df, label_col, protected_col, privileged_group_value):
    return BinaryLabelDataset(
        df=df,
        label_names=[label_col],
        protected_attribute_names=[protected_col],
        privileged_protected_attributes=[[privileged_group_value]],
    )

# Privileged group = those who historically received better treatment
privileged = [{"gender": "male"}]
unprivileged = [{"gender": "female"}]

train_dataset = to_aif360_dataset(train_df, "approved", "gender", "male")
test_dataset = to_aif360_dataset(test_df, "approved", "gender", "male")

# Measure bias in original dataset
metric = BinaryLabelDatasetMetric(
    train_dataset,
    unprivileged_groups=unprivileged,
    privileged_groups=privileged,
)

print(f"Disparate Impact (original): {metric.disparate_impact():.4f}")
print(f"Statistical Parity Difference: {metric.statistical_parity_difference():.4f}")
```

---

## Pre-Processing: Reweighing

Reweighing assigns higher weights to under-represented (privileged) combinations during training.

```python
from aif360.algorithms.preprocessing import Reweighing

reweigher = Reweighing(
    unprivileged_groups=unprivileged,
    privileged_groups=privileged,
)
reweigher.fit(train_dataset)
train_dataset_reweighed = reweigher.transform(train_dataset)

# Train model with reweighing
from sklearn.linear_model import LogisticRegression

X_rw = train_dataset_reweighed.features
y_rw = train_dataset_reweighed.labels.ravel()
weights_rw = train_dataset_reweighed.instance_weights  # KEY: use these weights!

model_rw = LogisticRegression()
model_rw.fit(X_rw, y_rw, sample_weight=weights_rw)

# Evaluate
y_pred_rw = model_rw.predict(test_dataset.features)

# Create AIF360 prediction dataset for metrics
test_pred_rw = test_dataset.copy()
test_pred_rw.labels = y_pred_rw.reshape(-1, 1)

cm_rw = ClassificationMetric(
    test_dataset,
    test_pred_rw,
    unprivileged_groups=unprivileged,
    privileged_groups=privileged,
)

print(f"Equal Opportunity Difference: {cm_rw.equal_opportunity_difference():.4f}")
print(f"Average Odds Difference: {cm_rw.average_odds_difference():.4f}")
```

---

## Post-Processing: Equalized Odds

Adjust decision thresholds post-training to equalize error rates across groups.

```python
from aif360.algorithms.postprocessing import EqOddsPostprocessing, CalibratedEqOddsPostprocessing

# Get predicted probabilities
y_prob = model.predict_proba(X_val.drop("gender", axis=1))[:, 1]

# Create AIF360 dataset with scores
val_dataset_with_scores = val_dataset.copy()
val_dataset_with_scores.scores = y_prob.reshape(-1, 1)

# Apply Equalized Odds post-processing
eq_odds = EqOddsPostprocessing(
    unprivileged_groups=unprivileged,
    privileged_groups=privileged,
    seed=42,
)

# Fit on validation set (finds optimal thresholds per group)
eq_odds.fit(val_dataset, val_dataset_with_scores)

# Transform test set predictions
test_pred_eq = eq_odds.predict(test_dataset_with_scores)

# The model now uses DIFFERENT thresholds per gender group
print("Threshold for male:", eq_odds.threshold[0])    # e.g., 0.45
print("Threshold for female:", eq_odds.threshold[1])  # e.g., 0.38 (lower → more approvals)
```

---

## Disparate Impact Analysis

```python
import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency

def full_disparate_impact_analysis(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: np.ndarray,
    sensitive_feature: np.ndarray,
    feature_name: str = "group",
) -> pd.DataFrame:
    """
    Comprehensive disparate impact analysis across all groups.
    """
    results = []
    groups = np.unique(sensitive_feature)
    
    # Overall stats
    overall_positive_rate = y_pred.mean()
    overall_tpr = y_pred[y_true == 1].mean()
    overall_fpr = y_pred[y_true == 0].mean()
    
    for group in groups:
        mask = sensitive_feature == group
        y_t = y_true[mask]
        y_p = y_pred[mask]
        y_pr = y_prob[mask]
        
        n = mask.sum()
        positive_rate = y_p.mean()
        
        # TPR and FPR
        tpr = y_p[y_t == 1].mean() if (y_t == 1).sum() > 0 else np.nan
        fpr = y_p[y_t == 0].mean() if (y_t == 0).sum() > 0 else np.nan
        
        # Adverse Impact Ratio
        air = positive_rate / overall_positive_rate if overall_positive_rate > 0 else np.nan
        
        # Statistical significance of difference
        contingency = np.array([
            [y_p.sum(), (1-y_p).sum()],
            [y_pred[~mask].sum(), (1-y_pred[~mask]).sum()]
        ])
        chi2, pval, _, _ = chi2_contingency(contingency)
        
        results.append({
            "group": group,
            "n": n,
            "n_pct": n / len(y_true),
            "positive_rate": positive_rate,
            "adverse_impact_ratio": air,
            "meets_80pct_rule": air >= 0.8,
            "tpr": tpr,
            "fpr": fpr,
            "tpr_diff_from_overall": tpr - overall_tpr,
            "fpr_diff_from_overall": fpr - overall_fpr,
            "chi2_pvalue": pval,
            "statistically_significant": pval < 0.05,
            "avg_score": y_pr.mean(),
        })
    
    return pd.DataFrame(results).sort_values("adverse_impact_ratio")

# Usage
analysis = full_disparate_impact_analysis(
    y_true=y_test.values,
    y_pred=y_pred,
    y_prob=y_prob,
    sensitive_feature=X_test["race_ethnicity"].values,
    feature_name="race_ethnicity",
)
print(analysis.to_string(index=False))
```

---

## Interview Tips

> **Tip 1:** "What's the difference between pre-, in-, and post-processing debiasing?" — "Pre-processing modifies the training data before training (reweighing, resampling, relabeling). In-processing adds fairness constraints to the optimization objective during training (ExponentiatedGradient, PrejudiceRemover). Post-processing modifies predictions after the model is trained (different thresholds per group, output transformation). Post-processing is least intrusive but less powerful; in-processing is most principled but requires model changes."

> **Tip 2:** "How does reweighing work?" — "Reweighing gives higher sample weights to training examples from underrepresented combinations of (protected attribute, label). If male-approved examples are overrepresented (historically privileged), they get lower weights. If female-approved examples are underrepresented, they get higher weights. The model then minimizes a weighted loss function, effectively equalizing the influence of each group on training."

> **Tip 3:** "Why does equalized odds post-processing use different thresholds per group?" — "If Group A has a higher base rate of positive outcomes, the model's scores reflect that — Group A naturally scores higher. Using the same threshold for both groups produces more false negatives for Group B (they're scored lower but would benefit from the positive outcome). Different thresholds calibrate the decision point so both groups have equal TPR and FPR."

> **Tip 4:** "What's the difference between fairness through unawareness (FTU) and fairness through awareness?" — "FTU: remove protected attributes from the model — 'if we don't use race, the model can't be biased by race.' This fails because correlated proxies (zip code, name, language) encode the protected attribute. Fairness through awareness: explicitly model the protected attribute and enforce that predictions are independent of it conditional on merit. FTA is more robust but legally complex — directly using protected attributes in modeling is itself regulated."
