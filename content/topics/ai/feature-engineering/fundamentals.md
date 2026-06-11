---
title: "AI - Feature Engineering"
topic: ai
subtopic: feature-engineering
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ai, feature-engineering, encoding, scaling, imputation, feature-selection]
---

# Feature Engineering — Fundamentals


## 🎯 Analogy

Think of feature engineering like translating raw sensor readings into useful gauges: raw timestamps become 'day of week' and 'hour of day', raw transaction counts become 'purchases in last 30 days' — transforming data into signals the model can actually learn from.

---
## What Is Feature Engineering?

Feature engineering is the process of transforming raw data into representations that ML models can learn from effectively. It is often described as "the art of ML" — the same algorithm can perform vastly differently depending on how features are constructed.

```mermaid
flowchart LR
    A[Raw Data] --> B[Encode Categoricals]
    B --> C[Scale Numerics]
    C --> D[Impute Missing Values]
    D --> E[Select Best Features]
    E --> F[Model-Ready Feature Matrix]
```

---

## Encoding Categorical Variables

### One-Hot Encoding (OHE)

Best for **nominal** categories (no natural order) with low cardinality (<50 unique values).

```python
import pandas as pd
from sklearn.preprocessing import OneHotEncoder

df = pd.DataFrame({"color": ["red", "blue", "green", "red", "blue"]})

# sklearn OHE
enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
encoded = enc.fit_transform(df[["color"]])
print(enc.get_feature_names_out())
# ['color_blue' 'color_green' 'color_red']
print(encoded)
# [[0. 0. 1.]   <- red
#  [1. 0. 0.]   <- blue
#  [0. 1. 0.]   <- green
#  [0. 0. 1.]   <- red
#  [1. 0. 0.]]  <- blue

# Pandas get_dummies (quick alternative)
pd.get_dummies(df["color"], prefix="color", drop_first=True)
```

**When NOT to use OHE:**
- High cardinality (city names, product IDs) → use target encoding or embeddings
- Ordinal categories (low/medium/high) → use ordinal encoding

### Label Encoding

Converts each category to an integer. Only use for **ordinal** categories or tree-based models.

```python
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder

# LabelEncoder — single column
le = LabelEncoder()
df["education_encoded"] = le.fit_transform(df["education"])
# high_school=0, college=1, graduate=2  (alphabetical — may not reflect order!)

# OrdinalEncoder — respects ordering
enc = OrdinalEncoder(categories=[["low", "medium", "high"]])
df["risk_encoded"] = enc.fit_transform(df[["risk_level"]])
# low=0, medium=1, high=2
```

### Target Encoding (Mean Encoding)

Replaces each category with the mean of the target variable for that category. Powerful for high-cardinality features.

```python
import pandas as pd
import numpy as np
from sklearn.model_selection import KFold

def target_encode(df, col, target, n_splits=5, smoothing=1.0):
    """
    K-fold target encoding to prevent leakage.
    Smoothing regularizes rare categories toward the global mean.
    """
    global_mean = df[target].mean()
    encoded = pd.Series(index=df.index, dtype=float)
    
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    
    for train_idx, val_idx in kf.split(df):
        train_fold = df.iloc[train_idx]
        
        # Compute mean per category in training fold
        cat_means = train_fold.groupby(col)[target].agg(["mean", "count"])
        
        # Smoothed mean: weight toward global mean for rare categories
        cat_means["smoothed"] = (
            cat_means["mean"] * cat_means["count"] + global_mean * smoothing
        ) / (cat_means["count"] + smoothing)
        
        # Apply to validation fold
        encoded.iloc[val_idx] = df.iloc[val_idx][col].map(
            cat_means["smoothed"]
        ).fillna(global_mean)
    
    return encoded

# Usage
df["plan_type_encoded"] = target_encode(df, "plan_type", "churned")
```

---

## Scaling Numeric Features

### Why Scaling Matters

| Algorithm | Needs Scaling? |
|-----------|----------------|
| Linear/Logistic Regression | Yes — gradient descent is scale-sensitive |
| SVM | Yes — kernel distances are scale-sensitive |
| KNN | Yes — distance-based |
| Random Forest / XGBoost | No — tree splits are scale-invariant |
| Neural Networks | Yes — weight initialization assumptions |
| K-Means | Yes — Euclidean distance |

### StandardScaler (Z-Score)

Centers data to mean=0, std=1. Assumes roughly Gaussian distribution.

```python
from sklearn.preprocessing import StandardScaler
import numpy as np

data = np.array([[10, 1000], [20, 2000], [30, 3000]])

scaler = StandardScaler()
scaled = scaler.fit_transform(data)

print(f"Original mean: {data.mean(axis=0)}")   # [20. 2000.]
print(f"Scaled mean: {scaled.mean(axis=0)}")   # [0. 0.]
print(f"Scaled std:  {scaled.std(axis=0)}")    # [1. 1.]

# Inverse transform
original = scaler.inverse_transform(scaled)
```

### MinMaxScaler

Scales to [0, 1] range. Sensitive to outliers — one extreme value compresses all others.

```python
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler(feature_range=(0, 1))
scaled = scaler.fit_transform(data)
# Formula: (x - min) / (max - min)
```

### RobustScaler

Uses median and IQR — robust to outliers. Best for skewed distributions.

```python
from sklearn.preprocessing import RobustScaler

scaler = RobustScaler(quantile_range=(25.0, 75.0))
scaled = scaler.fit_transform(data)
# Formula: (x - median) / IQR
```

### PowerTransformer (Yeo-Johnson)

Transforms data to be more Gaussian. Works for positive and negative values.

```python
from sklearn.preprocessing import PowerTransformer
import matplotlib.pyplot as plt

# Skewed feature
income = np.random.lognormal(mean=10, sigma=1, size=1000)

pt = PowerTransformer(method="yeo-johnson")
income_transformed = pt.fit_transform(income.reshape(-1, 1))

# Before and after
fig, axes = plt.subplots(1, 2, figsize=(10, 4))
axes[0].hist(income, bins=50); axes[0].set_title("Original (skewed)")
axes[1].hist(income_transformed, bins=50); axes[1].set_title("After Yeo-Johnson")
```

---

## Handling Missing Values (Imputation)

### Types of Missingness

| Type | Description | Example | Approach |
|------|-------------|---------|----------|
| MCAR | Missing Completely At Random | Server error dropped rows | Any imputation is unbiased |
| MAR | Missing At Random (conditional) | Income not reported for high earners | Model-based imputation |
| MNAR | Missing Not At Random | Sensor fails at extreme temperatures | Needs domain knowledge |

### Simple Imputation

```python
from sklearn.impute import SimpleImputer
import numpy as np

X = np.array([[1, np.nan, 3], [4, 5, np.nan], [np.nan, 8, 9]])

# Numeric: median (robust to outliers)
imputer = SimpleImputer(strategy="median")
X_imputed = imputer.fit_transform(X)

# Categorical: most frequent
from sklearn.impute import SimpleImputer
cat_imputer = SimpleImputer(strategy="most_frequent")

# Constant fill
const_imputer = SimpleImputer(strategy="constant", fill_value=-1)
```

### KNN Imputation

Uses K nearest neighbors to impute missing values — captures feature correlations.

```python
from sklearn.impute import KNNImputer

# Impute using 5 nearest neighbors
knn_imputer = KNNImputer(n_neighbors=5, weights="distance")
X_imputed = knn_imputer.fit_transform(X)
```

### Iterative Imputation (MICE)

Models each feature as a function of others, imputing iteratively.

```python
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.ensemble import ExtraTreesRegressor

mice_imputer = IterativeImputer(
    estimator=ExtraTreesRegressor(n_estimators=10, random_state=42),
    max_iter=10,
    random_state=42,
)
X_imputed = mice_imputer.fit_transform(X)
```

### Adding Missingness Indicator

Often, missingness itself is informative — create a binary indicator.

```python
import pandas as pd

def add_missing_indicators(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if df[col].isna().any():
            df[f"{col}_was_missing"] = df[col].isna().astype(int)
    return df

df = add_missing_indicators(df, ["income", "credit_score", "employment_years"])
```

---

## Feature Selection

### Filter Methods (Model-Free)

```python
from sklearn.feature_selection import (
    SelectKBest, f_classif, mutual_info_classif,
    VarianceThreshold
)

# Remove near-zero variance features (useless for learning)
var_filter = VarianceThreshold(threshold=0.01)
X_filtered = var_filter.fit_transform(X_train)

# ANOVA F-test (linear relationship with target)
selector = SelectKBest(f_classif, k=20)
X_selected = selector.fit_transform(X_train, y_train)
selected_features = X_train.columns[selector.get_support()].tolist()

# Mutual Information (captures non-linear relationships)
mi_selector = SelectKBest(mutual_info_classif, k=20)
X_mi_selected = mi_selector.fit_transform(X_train, y_train)
```

### Wrapper Methods (Model-Based)

```python
from sklearn.feature_selection import RFE, RFECV
from sklearn.ensemble import RandomForestClassifier

# Recursive Feature Elimination
rf = RandomForestClassifier(n_estimators=100, random_state=42)
rfe = RFE(estimator=rf, n_features_to_select=15, step=1)
rfe.fit(X_train, y_train)

selected = X_train.columns[rfe.support_].tolist()
print(f"Selected features: {selected}")
print(f"Feature rankings: {rfe.ranking_}")

# RFECV: automatically find optimal number of features
rfecv = RFECV(estimator=rf, cv=5, scoring="roc_auc", min_features_to_select=5)
rfecv.fit(X_train, y_train)
print(f"Optimal features: {rfecv.n_features_}")
```

### Embedded Methods (Feature Importance)

```python
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import GradientBoostingClassifier

model = GradientBoostingClassifier(n_estimators=200, random_state=42)
model.fit(X_train, y_train)

# Feature importance
importance_df = pd.DataFrame({
    "feature": X_train.columns,
    "importance": model.feature_importances_,
}).sort_values("importance", ascending=False)

# Plot top 20
importance_df.head(20).plot(
    kind="barh", x="feature", y="importance",
    figsize=(10, 8), title="Feature Importance (GBM)"
)
plt.tight_layout()
plt.show()

# SHAP values (better than built-in importance)
import shap
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)
shap.summary_plot(shap_values, X_test, plot_type="bar")
```

---

## Common Pitfalls

| Pitfall | Problem | Fix |
|---------|---------|-----|
| Scaling before splitting | Data leakage | Use Pipeline |
| OHE on high cardinality | Memory explosion | Use target encoding or hashing |
| Imputing with global stats before splitting | Leakage | Fit imputer on train only |
| Dropping all missing rows | Loses information | Impute + add missingness indicator |
| Selecting features before splitting | Leakage | Feature selection inside cross-validation |
| Label encoding ordinals alphabetically | Wrong ordering | Use OrdinalEncoder with explicit order |

---


## ▶️ Try It Yourself

```python
import pandas as pd
import numpy as np
from datetime import datetime

# Raw data
orders = pd.DataFrame({
    "customer_id": [1, 1, 1, 2, 2],
    "amount": [100, 200, 50, 300, 150],
    "order_date": pd.to_datetime(["2024-01-01","2024-01-10","2024-01-20","2024-01-05","2024-01-15"]),
})

# Feature engineering: aggregate to customer level
snapshot_date = pd.Timestamp("2024-01-25")

features = orders.groupby("customer_id").agg(
    order_count=("amount", "count"),
    total_spend=("amount", "sum"),
    avg_order=("amount", "mean"),
    max_order=("amount", "max"),
    days_since_last=("order_date", lambda x: (snapshot_date - x.max()).days),
    days_since_first=("order_date", lambda x: (snapshot_date - x.min()).days),
).reset_index()

# Derived features
features["spend_per_day"] = features["total_spend"] / features["days_since_first"].clip(1)
features["order_frequency"] = features["order_count"] / features["days_since_first"].clip(1) * 30

print(features.T)
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When would you choose target encoding over one-hot encoding?" — "Target encoding is preferable for high-cardinality categorical features (>50 unique values) where OHE would create an impractical number of sparse columns. Examples: zip code, product ID, user agent. The tradeoff is target encoding can cause leakage if not done with k-fold cross-fitting — you must never use the target of the row being encoded."

> **Tip 2:** "What's the difference between StandardScaler and RobustScaler?" — "StandardScaler uses mean and std, so a single outlier can distort the entire scaling — an extreme income of $10M will compress all middle-income values into a tiny range. RobustScaler uses the median and IQR, so outliers have no influence on the scale. Use RobustScaler when you have significant outliers or skewed distributions."

> **Tip 3:** "How do you decide which imputation strategy to use?" — "First, understand the missingness mechanism. If data is MCAR, simple median imputation usually works. If missingness correlates with other features (MAR), KNN or MICE imputation captures those relationships better. Always add a binary 'was_missing' indicator regardless — missingness itself is often predictive of the target."

> **Tip 4:** "Why do tree-based models not need feature scaling?" — "Trees make splits based on feature thresholds: 'is age > 30?' The answer is the same whether age is measured in years or months — the split point shifts but the information content is identical. Scaling changes the absolute values but not the rank ordering, and trees only care about ranks. Linear models and distance-based algorithms depend on absolute values, so scaling matters."
