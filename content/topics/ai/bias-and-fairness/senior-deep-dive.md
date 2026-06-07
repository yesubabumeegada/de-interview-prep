---
title: "AI - Bias and Fairness"
topic: ai
subtopic: bias-and-fairness
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ai, bias, fairness, intersectionality, causal-fairness, gdpr, eu-ai-act]
---

# Bias and Fairness — Senior Deep Dive

## Fairness-Accuracy Tradeoff

There is a fundamental mathematical tension between accuracy and fairness criteria. Understanding this tension is essential for senior ML engineers.

### The Impossibility Result

Chouldechova (2017) proved that when base rates differ across groups, you cannot simultaneously satisfy:
- Calibration (predictive parity)
- Equalized odds (equal TPR and FPR)

```python
import numpy as np

def demonstrate_impossibility(base_rate_A: float, base_rate_B: float, n: int = 10000):
    """
    Show that calibration and equalized odds conflict when base rates differ.
    """
    rng = np.random.default_rng(42)
    
    # Group A: higher base rate
    y_A = rng.binomial(1, base_rate_A, n // 2)
    # Group B: lower base rate
    y_B = rng.binomial(1, base_rate_B, n // 2)
    
    # Perfect calibration: predict true probability
    scores_A = rng.beta(2, 1, n // 2) * base_rate_A + rng.beta(1, 2, n // 2) * (1 - base_rate_A)
    scores_B = rng.beta(2, 1, n // 2) * base_rate_B + rng.beta(1, 2, n // 2) * (1 - base_rate_B)
    
    # Same threshold for both groups
    threshold = 0.5
    pred_A = (scores_A >= threshold).astype(int)
    pred_B = (scores_B >= threshold).astype(int)
    
    tpr_A = pred_A[y_A == 1].mean()
    fpr_A = pred_A[y_A == 0].mean()
    tpr_B = pred_B[y_B == 1].mean()
    fpr_B = pred_B[y_B == 0].mean()
    
    print(f"Base rates: Group A={base_rate_A:.0%}, Group B={base_rate_B:.0%}")
    print(f"TPR: Group A={tpr_A:.3f}, Group B={tpr_B:.3f} (diff={abs(tpr_A-tpr_B):.3f})")
    print(f"FPR: Group A={fpr_A:.3f}, Group B={fpr_B:.3f} (diff={abs(fpr_A-fpr_B):.3f})")
    print(f"Cannot equalize both TPR and FPR when base rates differ!")

demonstrate_impossibility(0.3, 0.1)
```

### Choosing the Right Fairness Metric for the Use Case

```python
FAIRNESS_METRIC_GUIDE = {
    "loan_approval": {
        "recommended": "Equalized Odds",
        "rationale": "Equal TPR ensures creditworthy applicants in all groups are equally likely to be approved. Equal FPR prevents systematically higher false approval of any group.",
        "legal_basis": "Equal Credit Opportunity Act",
    },
    "criminal_recidivism": {
        "recommended": "Predictive Parity (Calibration)",
        "rationale": "COMPAS case: Northpointe argued scores should be equally calibrated. A score of 7/10 should mean 70% recidivism risk for all races.",
        "controversy": "Equalized odds requires different false positive rates — which is fairness?",
    },
    "job_screening": {
        "recommended": "Demographic Parity (with merit adjustment)",
        "rationale": "Selection rates should reflect qualified applicant pool composition.",
        "legal_basis": "EEOC 80% rule",
    },
    "medical_diagnosis": {
        "recommended": "Equal Opportunity",
        "rationale": "Equal TPR ensures all patients with the condition are equally likely to be diagnosed. False negatives (missed diagnoses) are the highest harm.",
    },
}
```

---

## Intersectionality

Fairness analysis on single attributes can miss discrimination at intersections.

```python
import pandas as pd
import numpy as np
from itertools import combinations

def intersectional_fairness_analysis(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    sensitive_df: pd.DataFrame,  # Multiple sensitive attributes
    min_group_size: int = 50,
) -> pd.DataFrame:
    """
    Analyze fairness across all intersections of protected attributes.
    Example: not just 'gender' and 'race' separately, but 'Black women' as a distinct group.
    """
    results = []
    
    # Single attributes
    for col in sensitive_df.columns:
        for value in sensitive_df[col].unique():
            mask = sensitive_df[col] == value
            if mask.sum() < min_group_size:
                continue
            
            y_t = y_true[mask]
            y_p = y_pred[mask]
            
            tpr = y_p[y_t == 1].mean() if (y_t == 1).sum() > 0 else np.nan
            
            results.append({
                "intersection": f"{col}={value}",
                "n": mask.sum(),
                "positive_rate": y_p.mean(),
                "tpr": tpr,
                "type": "single_attribute",
            })
    
    # Pairwise intersections
    for col1, col2 in combinations(sensitive_df.columns, 2):
        for v1 in sensitive_df[col1].unique():
            for v2 in sensitive_df[col2].unique():
                mask = (sensitive_df[col1] == v1) & (sensitive_df[col2] == v2)
                if mask.sum() < min_group_size:
                    continue
                
                y_t = y_true[mask]
                y_p = y_pred[mask]
                
                tpr = y_p[y_t == 1].mean() if (y_t == 1).sum() > 0 else np.nan
                
                results.append({
                    "intersection": f"{col1}={v1}, {col2}={v2}",
                    "n": mask.sum(),
                    "positive_rate": y_p.mean(),
                    "tpr": tpr,
                    "type": "pairwise_intersection",
                })
    
    result_df = pd.DataFrame(results)
    
    # Compute adverse impact vs overall
    overall_positive_rate = y_pred.mean()
    result_df["adverse_impact_ratio"] = result_df["positive_rate"] / overall_positive_rate
    result_df["flagged"] = result_df["adverse_impact_ratio"] < 0.8
    
    return result_df.sort_values("adverse_impact_ratio")


# Usage
sensitive_data = pd.DataFrame({
    "gender": X_test["gender"],
    "race_ethnicity": X_test["race_ethnicity"],
    "age_group": pd.cut(X_test["age"], bins=[0, 30, 45, 60, 100], labels=["18-30", "30-45", "45-60", "60+"]),
})

analysis = intersectional_fairness_analysis(y_test.values, y_pred, sensitive_data)
print("Groups with adverse impact (AIR < 0.8):")
print(analysis[analysis["flagged"]][["intersection", "n", "positive_rate", "adverse_impact_ratio"]])
```

---

## Causal Fairness

Causal fairness goes beyond correlation — it asks whether the protected attribute CAUSES the outcome, or whether there's a legitimate non-discriminatory pathway.

```python
# Causal fairness concepts using causal graphs
# This requires domain knowledge about causal relationships

# Example: Hiring model
# Causal graph:
# Gender → College Major → Job Performance (legitimate path)
# Gender → Hiring Decision (direct discrimination — should be blocked)

# Counterfactual fairness: would the decision change if the person had a different protected attribute?

def counterfactual_fairness_check(
    model,
    X: pd.DataFrame,
    protected_col: str,
    protected_value_1,
    protected_value_2,
    mediator_cols: list = None,  # Legitimate mediating variables
) -> pd.DataFrame:
    """
    Check counterfactual fairness: 
    Does changing protected attribute (while keeping mediators fixed) change the prediction?
    
    If yes → direct discrimination.
    If no → fair (or discrimination only through legitimate mediators).
    """
    
    # Original predictions
    X_original = X.copy()
    pred_original = model.predict_proba(X_original.drop(protected_col, axis=1))[:, 1]
    
    # Counterfactual: flip protected attribute, keep everything else
    X_counterfactual = X.copy()
    
    mask_1 = X_counterfactual[protected_col] == protected_value_1
    mask_2 = X_counterfactual[protected_col] == protected_value_2
    
    X_counterfactual.loc[mask_1, protected_col] = protected_value_2
    X_counterfactual.loc[mask_2, protected_col] = protected_value_1
    
    pred_counterfactual = model.predict_proba(
        X_counterfactual.drop(protected_col, axis=1)
    )[:, 1]
    
    # Score change from flipping attribute
    score_change = pred_counterfactual - pred_original
    
    return pd.DataFrame({
        "original_group": X[protected_col],
        "original_score": pred_original,
        "counterfactual_score": pred_counterfactual,
        "score_change": score_change,
        "abs_change": abs(score_change),
    })

result = counterfactual_fairness_check(
    model=pipeline,
    X=X_test,
    protected_col="gender",
    protected_value_1="male",
    protected_value_2="female",
)

print(f"Mean score change from gender flip: {result['score_change'].mean():.4f}")
print(f"% with significant score change (>0.05): {(result['abs_change'] > 0.05).mean():.1%}")
```

---

## Regulatory Landscape

### GDPR (EU)

```python
GDPR_FAIRNESS_REQUIREMENTS = {
    "Article_22": {
        "title": "Automated Individual Decision-Making",
        "requirement": "Data subjects have the right not to be subject to solely automated decisions "
                      "that significantly affect them, without human involvement.",
        "implementation": [
            "Provide human review process for automated denials",
            "Allow individuals to contest automated decisions",
            "Provide meaningful explanation (not black-box)",
        ],
    },
    "Article_13-14": {
        "title": "Transparency",
        "requirement": "Controllers must inform subjects about automated decision-making logic.",
        "implementation": [
            "Document model logic in plain language",
            "Provide feature importance or SHAP values on request",
            "Publish model cards for high-risk models",
        ],
    },
}
```

### EU AI Act

```python
EU_AI_ACT_RISK_TIERS = {
    "unacceptable_risk": {
        "examples": [
            "Social scoring by governments",
            "Real-time biometric identification in public spaces",
            "Subliminal manipulation",
        ],
        "status": "Prohibited",
    },
    "high_risk": {
        "examples": [
            "AI in hiring and employment",
            "Credit scoring",
            "Educational assessment",
            "Law enforcement",
            "Medical devices",
            "Critical infrastructure",
        ],
        "requirements": [
            "Risk management system",
            "Data governance (training data quality)",
            "Technical documentation",
            "Transparency and user information",
            "Human oversight",
            "Accuracy, robustness, cybersecurity",
            "Conformity assessment before market",
        ],
        "penalties": "Up to 30M EUR or 6% global annual turnover",
    },
    "limited_risk": {
        "examples": ["Chatbots", "Deep fakes"],
        "requirements": ["Transparency obligation (disclose AI use)"],
    },
    "minimal_risk": {
        "examples": ["AI-enabled video games", "Spam filters"],
        "requirements": ["Voluntary code of conduct"],
    },
}

def assess_eu_ai_act_compliance(model_use_case: str) -> dict:
    """Determine EU AI Act requirements for a model use case."""
    
    HIGH_RISK_KEYWORDS = [
        "hiring", "employment", "recruitment", "credit", "loan",
        "education", "assessment", "grading", "policing", "law enforcement",
        "biometric", "border control", "medical", "safety",
    ]
    
    use_case_lower = model_use_case.lower()
    
    is_high_risk = any(kw in use_case_lower for kw in HIGH_RISK_KEYWORDS)
    
    if is_high_risk:
        requirements = EU_AI_ACT_RISK_TIERS["high_risk"]["requirements"]
        return {
            "risk_tier": "high_risk",
            "requirements": requirements,
            "action_required": "Conformity assessment required before deployment",
            "penalties": EU_AI_ACT_RISK_TIERS["high_risk"]["penalties"],
        }
    
    return {"risk_tier": "minimal_risk", "requirements": [], "action_required": "None"}
```

---

## Interview Tips

> **Tip 1:** "Can you explain the COMPAS recidivism case and what it taught us about fairness?" — "COMPAS risk scores were used in US courts for bail and sentencing decisions. ProPublica showed Black defendants were nearly twice as likely to be falsely flagged as high-risk (false positive rate). Northpointe argued the scores were calibrated — a score of 7 means 70% recidivism for both races. Both claims are technically correct but reflect different fairness criteria. The impossibility theorem shows these can't both hold when base rates differ. The lesson: fairness is multidimensional, and the 'right' criterion is a societal choice, not a mathematical one."

> **Tip 2:** "What is intersectional bias and why is it missed by standard analysis?" — "Standard analysis checks gender bias and race bias separately. But 'Black women' is a distinct subgroup — they may face bias that neither Black men nor white women face. Analyzing protected attributes independently misses this. Intersectional analysis checks all combinations: gender x race x age x disability status. The challenge is sample size — intersectional groups can be too small for statistical significance."

> **Tip 3:** "How would you implement GDPR Article 22 compliance for an automated credit decision system?" — "Three components: (1) Human review path — any automated denial must be flaggable for human review within 30 days; (2) Meaningful explanation — use SHAP to identify the top 3-5 features driving the decision, translate to plain language ('Your debt-to-income ratio was 45%, above our threshold of 35%'); (3) Contest mechanism — allow users to provide additional information and trigger re-evaluation. Document all of this in the model card."

> **Tip 4:** "What's the difference between fairness through unawareness and counterfactual fairness?" — "FTU removes protected attributes from input: 'race isn't in my model.' Counterfactual fairness asks: if this individual had been of a different race (ceteris paribus), would the decision change? A model can be unfair by counterfactual definition even without using race, if correlated proxies carry the same information. Counterfactual fairness is stronger but requires a causal model of how attributes relate to outcomes."
