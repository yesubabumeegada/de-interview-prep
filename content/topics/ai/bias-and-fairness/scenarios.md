---
title: "AI - Bias and Fairness"
topic: ai
subtopic: bias-and-fairness
content_type: scenario_question
difficulty_level: junior
tags: [ai, bias, fairness, scenarios, model-bias, regulatory-audit, recommendations]
---

# Bias and Fairness — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Discovering Model Bias

You're analyzing a loan approval model's performance metrics and notice something alarming:

```
Overall approval rate: 62%
By demographic group:
  White applicants:    71%
  Asian applicants:    68%
  Hispanic applicants: 53%
  Black applicants:    44%

Model AUC by group:
  White: 0.89
  Asian: 0.87
  Hispanic: 0.84
  Black: 0.79   <- significantly lower!
```

Your manager says: "The model doesn't use race, so it can't be biased." How do you respond and what actions do you take?

<details>
<summary>💡 Hint</summary>

Think about: (1) why a model without race can still be biased (proxy variables), (2) what the metrics actually tell you, and (3) the specific numbers — what does the 80% rule say about these approval rates?

</details>

<details>
<summary>✅ Solution</summary>

### Response to Manager

"The model doesn't use race as a direct input, but it uses correlated proxies. Zip code correlates with race due to residential segregation patterns. Credit history correlates with race due to historical discrimination in lending. When you train on these features, the model implicitly learns race correlations. The law doesn't just prohibit direct use of protected attributes — disparate impact (unequal outcomes) is also illegal under ECOA, even if unintentional."

### Quantifying the Problem

```python
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

# Adverse Impact Ratio analysis
approval_rates = {
    "white": 0.71,
    "asian": 0.68,
    "hispanic": 0.53,
    "black": 0.44,
}

# Reference group = highest approval rate
max_rate = max(approval_rates.values())  # 0.71

print("Adverse Impact Ratios (vs white applicants):")
for group, rate in approval_rates.items():
    air = rate / max_rate
    status = "PASSES" if air >= 0.8 else "FAILS 80% RULE"
    print(f"  {group}: {rate:.0%} approval → AIR = {air:.2f} [{status}]")

# Output:
# white:    100% of reference rate [PASSES]
# asian:     96% of reference rate [PASSES]
# hispanic:  75% of reference rate [FAILS 80% RULE]  <- illegal!
# black:     62% of reference rate [FAILS 80% RULE]  <- illegal!
```

### Root Cause Investigation

```python
# Find which features are proxy variables for race
def find_proxy_variables(df, race_col="race_ethnicity", feature_cols=None):
    """Identify features that correlate with race/ethnicity."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import LabelEncoder
    from sklearn.metrics import roc_auc_score
    
    le = LabelEncoder()
    race_encoded = le.fit_transform(df[race_col])
    
    proxy_scores = {}
    for col in feature_cols or df.select_dtypes("number").columns:
        if col == race_col:
            continue
        try:
            lr = LogisticRegression(max_iter=100)
            lr.fit(df[[col]].fillna(df[col].median()), race_encoded)
            auc = roc_auc_score(race_encoded, lr.predict_proba(df[[col]].fillna(df[col].median())), multi_class="ovr")
            proxy_scores[col] = round(auc, 4)
        except:
            pass
    
    return pd.Series(proxy_scores).sort_values(ascending=False)

proxy_analysis = find_proxy_variables(train_df, race_col="race_ethnicity")
print("Proxy variable risk (AUC predicting race):")
print(proxy_analysis.head(10))
# zip_code:           0.84  <- STRONG PROXY
# years_at_address:   0.73  <- MODERATE PROXY
# bank_branch_id:     0.71  <- MODERATE PROXY
```

### Immediate Actions

```python
# Action Plan
REMEDIATION_PLAN = """
Immediate (Week 1):
1. Halt automated approvals for Hispanic and Black applicants 
   — route to human review until bias is addressed
   (Legal team must advise on exact approach)

2. Investigate top proxy variables:
   - Remove or transform zip_code (use region instead)
   - Remove bank_branch_id if it encodes neighborhood demographics

3. Notify compliance and legal team

Short-term (Month 1):
4. Apply fairness constraints (ExponentiatedGradient with DemographicParity)
5. Re-evaluate model with fairness metrics as primary gate
6. Conduct external bias audit

Ongoing:
7. Add adverse impact analysis to monthly model monitoring
8. Require fairness sign-off before any model update
"""

# Implement fairness-constrained model
from fairlearn.reductions import ExponentiatedGradient, DemographicParity
from sklearn.ensemble import GradientBoostingClassifier

base_model = GradientBoostingClassifier(n_estimators=200)
constraint = DemographicParity(difference_bound=0.05)  # Max 5% approval rate difference

fair_model = ExponentiatedGradient(
    estimator=base_model,
    constraints=constraint,
)

# Train without race but enforce demographic parity using race for constraint
fair_model.fit(
    X_train.drop(["race_ethnicity", "zip_code", "bank_branch_id"], axis=1),
    y_train,
    sensitive_features=X_train["race_ethnicity"],
)
```

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Regulatory Audit of Credit Model

A regulator contacts your company requesting a complete fairness audit of your auto loan pricing model. The model sets interest rates (not just approvals). The regulator wants: disparate impact analysis, the top features driving rate differences, and your remediation plan. You have 2 weeks.

<details>
<summary>💡 Hint</summary>

Pricing fairness is different from approval fairness — it's not binary, it's continuous. Think about how to measure disparate impact on interest rates. Also consider: even if base rates explain some of the rate difference, the question is whether the rate difference is fully explained by legitimate credit factors.

</details>

<details>
<summary>✅ Solution</summary>

### Pricing Fairness Analysis

```python
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression

class AutoLoanPricingAudit:
    """
    Regulatory audit for auto loan interest rate pricing model.
    Measures: raw rate disparities + unexplained disparities (after controlling for credit factors).
    """
    
    def __init__(self, df: pd.DataFrame, rate_col: str, protected_col: str, credit_factor_cols: list):
        self.df = df
        self.rate_col = rate_col
        self.protected_col = protected_col
        self.credit_factor_cols = credit_factor_cols
    
    def compute_raw_rate_disparity(self) -> pd.DataFrame:
        """Step 1: Simple average rate by demographic group."""
        group_stats = self.df.groupby(self.protected_col)[self.rate_col].agg(["mean", "std", "count"])
        group_stats.columns = ["avg_rate", "rate_std", "n"]
        group_stats["avg_rate_pct"] = group_stats["avg_rate"]
        
        min_rate = group_stats["avg_rate"].min()
        group_stats["rate_premium_vs_lowest"] = group_stats["avg_rate"] - min_rate
        
        return group_stats
    
    def compute_unexplained_disparity(self) -> dict:
        """
        Step 2: Rate disparity AFTER controlling for legitimate credit factors.
        Uses Oaxaca-Blinder decomposition approach.
        
        Unexplained gap = rate difference not explained by credit risk.
        This is the measure of discrimination.
        """
        
        # Encode protected attribute
        df = pd.get_dummies(self.df, columns=[self.protected_col])
        protected_dummies = [c for c in df.columns if c.startswith(self.protected_col)]
        
        # Model 1: Without protected attribute (legitimate pricing)
        X_credit = df[self.credit_factor_cols]
        y = df[self.rate_col]
        
        model_credit = LinearRegression()
        model_credit.fit(X_credit, y)
        
        predicted_fair = model_credit.predict(X_credit)
        
        # Unexplained residual by group
        df["fair_predicted_rate"] = predicted_fair
        df["unexplained_premium"] = df[self.rate_col] - df["fair_predicted_rate"]
        
        # Average unexplained premium per group
        unexplained_by_group = df.groupby(self.protected_col)["unexplained_premium"].agg(["mean", "std"])
        
        # Statistical test: is the unexplained premium significantly different?
        groups = df[self.protected_col].unique()
        
        ttest_results = {}
        reference = df[df[self.protected_col] == groups[0]]["unexplained_premium"]
        
        for group in groups[1:]:
            comparison = df[df[self.protected_col] == group]["unexplained_premium"]
            t_stat, pval = stats.ttest_ind(reference, comparison)
            ttest_results[f"{groups[0]}_vs_{group}"] = {
                "unexplained_gap_bps": (comparison.mean() - reference.mean()) * 100,  # in basis points
                "t_statistic": t_stat,
                "pvalue": pval,
                "statistically_significant": pval < 0.05,
            }
        
        return {
            "unexplained_by_group": unexplained_by_group.to_dict(),
            "ttest_results": ttest_results,
        }
    
    def generate_audit_report(self) -> dict:
        raw = self.compute_raw_rate_disparity()
        unexplained = self.compute_unexplained_disparity()
        
        return {
            "audit_date": pd.Timestamp.utcnow().strftime("%Y-%m-%d"),
            "model_audited": "auto_loan_pricing_v4",
            "total_loans_analyzed": len(self.df),
            "raw_rate_disparity": raw.to_dict(),
            "unexplained_rate_disparity": unexplained,
            "remediation_required": any(
                abs(r["unexplained_gap_bps"]) > 25 and r["statistically_significant"]
                for r in unexplained["ttest_results"].values()
            ),
            "recommended_actions": self._get_recommendations(raw, unexplained),
        }
    
    def _get_recommendations(self, raw, unexplained) -> list:
        recommendations = []
        
        for comparison, result in unexplained["ttest_results"].items():
            if abs(result["unexplained_gap_bps"]) > 25 and result["statistically_significant"]:
                groups = comparison.split("_vs_")
                recommendations.append(
                    f"URGENT: {groups[1]} applicants pay {abs(result['unexplained_gap_bps']):.0f} bps "
                    f"more than {groups[0]} after controlling for credit risk. "
                    f"This is statistically significant (p={result['pvalue']:.4f}). "
                    f"Investigate proxy variables and retrain with fairness constraints."
                )
        
        if not recommendations:
            recommendations.append("Rate disparities are explained by legitimate credit factors. No immediate action required.")
        
        return recommendations
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Fairness in Recommendation Systems

Your news recommendation system is accused of creating filter bubbles and disproportionately amplifying certain political viewpoints. An NGO publishes a report showing that users in rural areas see 3x more conservative content than urban users, and Black users see more crime-related news than similarly interested white users. Design a fairness audit and mitigation strategy.

<details>
<summary>💡 Hint</summary>

Recommendation fairness is different from classification fairness — it's about exposure of content to users AND visibility of content providers. Think about: (1) provider fairness (equal exposure opportunity for content creators), (2) user fairness (no systematically biased content delivery), (3) the tension between personalization and fairness.

</details>

<details>
<summary>✅ Solution</summary>

### Recommendation Fairness Framework

```python
import pandas as pd
import numpy as np
from typing import Dict, List

class RecommendationFairnessAuditor:
    """
    Audit recommendation system for:
    1. User-side fairness: similar users get similar quality recommendations
    2. Provider-side fairness: content creators get equal exposure opportunity
    3. Content bias: systematic topic skews by user demographics
    """
    
    def __init__(self, recommendation_logs: pd.DataFrame):
        """
        recommendation_logs: DataFrame with columns:
        - user_id, user_race, user_location_type (urban/rural)
        - item_id, item_topic, item_political_lean
        - timestamp, position, clicked
        """
        self.logs = recommendation_logs
    
    def audit_content_bias_by_user_group(self) -> dict:
        """
        Measure: do users in different demographic groups see systematically 
        different content topics?
        """
        results = {}
        
        for user_attr in ["user_race", "user_location_type"]:
            for topic_attr in ["item_topic", "item_political_lean"]:
                
                # Topic distribution per user group
                topic_dist = (
                    self.logs.groupby([user_attr, topic_attr])
                    .size()
                    .unstack(fill_value=0)
                    .apply(lambda x: x / x.sum(), axis=1)
                )
                
                # Measure divergence between groups using KL divergence
                from scipy.special import rel_entr
                
                groups = topic_dist.index.tolist()
                divergences = {}
                
                if len(groups) >= 2:
                    p = topic_dist.loc[groups[0]].values + 1e-10
                    q = topic_dist.loc[groups[1]].values + 1e-10
                    p /= p.sum()
                    q /= q.sum()
                    kl = float(np.sum(rel_entr(p, q)))
                    divergences[f"{groups[0]}_vs_{groups[1]}"] = kl
                
                results[f"{user_attr}_x_{topic_attr}"] = {
                    "distribution": topic_dist.to_dict(),
                    "kl_divergence": divergences,
                    "significant_bias": any(kl > 0.1 for kl in divergences.values()),
                }
        
        return results
    
    def audit_provider_fairness(self) -> dict:
        """
        Measure: do content creators from different backgrounds get equal exposure?
        Provider fairness: equal visibility regardless of creator demographics.
        """
        
        # This requires content creator metadata
        # For news: political affiliation, outlet type, geographic focus
        
        provider_exposure = (
            self.logs.groupby(["item_political_lean"])
            .agg(
                total_impressions=("item_id", "count"),
                total_clicks=("clicked", "sum"),
                avg_position=("position", "mean"),
            )
        )
        
        provider_exposure["ctr"] = provider_exposure["total_clicks"] / provider_exposure["total_impressions"]
        provider_exposure["impression_share"] = (
            provider_exposure["total_impressions"] / provider_exposure["total_impressions"].sum()
        )
        
        return provider_exposure.to_dict()
    
    def compute_calibration_by_user_group(self, user_interests: pd.DataFrame) -> dict:
        """
        Measure: are recommendation relevance/engagement rates equal across user groups?
        User fairness: all users should get equally relevant recommendations.
        """
        
        # Merge user demographics and interests with logs
        logs_with_interests = self.logs.merge(user_interests, on="user_id")
        
        ctr_by_group = logs_with_interests.groupby("user_race")["clicked"].agg(["mean", "std", "count"])
        ctr_by_group.columns = ["ctr", "ctr_std", "n"]
        
        # Check if lower CTR is due to less relevant recommendations or different behavior
        return ctr_by_group.to_dict()
    
    def recommend_mitigations(self, audit_results: dict) -> List[str]:
        """Generate mitigation recommendations based on audit findings."""
        
        mitigations = []
        
        # If content bias detected
        if any(r.get("significant_bias") for r in audit_results.get("content_bias", {}).values()):
            mitigations.extend([
                "Add topic diversity constraint to ranking: limit any single topic to max 30% of recommendations",
                "Implement political lean balancing: for political content, maintain a roughly balanced distribution",
                "Add location de-biasing: don't use location as a feature for political content ranking",
            ])
        
        # If provider bias detected
        mitigations.extend([
            "Implement calibration-aware ranking: ensure content exposure proportional to content quality, not just engagement signal",
            "Add exploration bonus for diverse content sources",
            "Audit recommendation model features for proxy variables encoding content creator demographics",
        ])
        
        return mitigations


# Algorithmic diversity constraint for ranking
def diversity_constrained_rerank(
    scores: np.ndarray,
    item_metadata: pd.DataFrame,
    topic_col: str,
    max_topic_concentration: float = 0.30,
    n_recommendations: int = 20,
) -> List[int]:
    """
    Rerank recommendations to enforce topic diversity.
    No single topic should exceed max_topic_concentration of recommendations.
    """
    
    sorted_indices = np.argsort(scores)[::-1]
    
    selected = []
    topic_counts = {}
    max_per_topic = int(n_recommendations * max_topic_concentration)
    
    for idx in sorted_indices:
        if len(selected) >= n_recommendations:
            break
        
        topic = item_metadata.iloc[idx][topic_col]
        current_count = topic_counts.get(topic, 0)
        
        if current_count < max_per_topic:
            selected.append(idx)
            topic_counts[topic] = current_count + 1
    
    # If we don't have enough after strict constraint, relax it
    if len(selected) < n_recommendations:
        remaining = [i for i in sorted_indices if i not in selected]
        selected.extend(remaining[:n_recommendations - len(selected)])
    
    return selected
```

### Ongoing Monitoring

```python
RECOMMENDATION_FAIRNESS_METRICS = {
    "content_kl_divergence_by_race": {
        "threshold": 0.10,
        "description": "KL divergence of topic distribution between racial groups",
        "alert_if_above": True,
    },
    "political_lean_gini_coefficient": {
        "threshold": 0.20,
        "description": "Inequality in political content exposure",
        "alert_if_above": True,
    },
    "ctr_gap_by_race": {
        "threshold": 0.05,
        "description": "Difference in CTR between racial groups",
        "alert_if_above": True,
    },
}
```

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between disparate impact and disparate treatment in ML models?**
A: Disparate treatment is intentional discrimination using protected attributes directly in the model. Disparate impact is unintentional — the model produces discriminatory outcomes even without using protected features, often through correlated proxy variables.

**Q: What is demographic parity and when is it insufficient?**
A: Demographic parity requires equal positive prediction rates across groups. It's insufficient when base rates differ between groups — forcing equal predictions can actually harm the minority group by ignoring legitimate statistical differences.

**Q: How do you detect bias in a model's training data?**
A: Compute group-stratified statistics (label distributions, feature distributions, sample counts) across protected attributes. Use tools like Facets or What-If Tool to visualize disparities, and audit for underrepresentation or historical labeling bias.

**Q: What is equalized odds and how does it differ from equal opportunity?**
A: Equalized odds requires equal true positive rates AND equal false positive rates across groups. Equal opportunity only requires equal true positive rates — it's a relaxation that focuses on not disadvantaging qualified individuals from any group.

**Q: What is fairness through unawareness and why does it often fail?**
A: It removes protected attributes from training data. It fails because proxy variables (zip code, name, purchase history) can reconstruct protected attributes with high accuracy, allowing bias to persist indirectly.

**Q: How would you monitor a deployed model for emerging bias over time?**
A: Track fairness metrics (TPR, FPR, precision) per demographic group in production dashboards. Set alerting thresholds on metric divergence, and periodically retrain or audit whenever data distributions shift or new demographic patterns emerge.

**Q: What is calibration bias and why does it matter?**
A: Calibration bias occurs when a model's predicted probabilities don't reflect actual outcomes equally across groups — e.g., a 70% confidence score means different things for different demographics. It matters because downstream decisions rely on well-calibrated probabilities.

**Q: What techniques can reduce bias without sacrificing model performance?**
A: Pre-processing (reweighting, resampling), in-processing (adversarial debiasing, fairness constraints in the loss function), and post-processing (threshold adjustment per group). Combining approaches typically yields better fairness-accuracy tradeoffs than any single technique.

---

## 💼 Interview Tips

- When discussing bias, always distinguish between the type of bias (data bias vs. model bias vs. evaluation bias) and tie your answer to concrete metrics rather than abstract concepts.
- Avoid framing fairness as simply "remove the protected attribute" — interviewers at senior levels expect you to explain proxy variables and why unawareness fails.
- Senior interviewers want to hear about the tradeoffs between fairness definitions (you can't satisfy demographic parity and equalized odds simultaneously in most real-world settings — cite this).
- Show familiarity with tooling: Fairlearn, AI Fairness 360, Google's What-If Tool — mentioning these signals you've worked on real bias audits.
- Always connect bias mitigation to business and regulatory risk (ECOA, GDPR, EU AI Act) — this demonstrates senior-level thinking about why bias matters beyond accuracy metrics.
- When asked "how would you handle bias discovered post-deployment," walk through a structured response: measure impact, communicate to stakeholders, decide on rollback vs. patch, retrain, and add monitoring — interviewers value process over just technical fixes.
