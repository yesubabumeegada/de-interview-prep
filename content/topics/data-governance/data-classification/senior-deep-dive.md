---
title: "Data Classification — Senior Deep Dive"
topic: data-governance
subtopic: data-classification
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-classification, ml-classification, information-theory, enterprise-taxonomy]
---

# Data Classification — Senior Deep Dive

## ML-Based Classification at Enterprise Scale

For large organizations with hundreds of thousands of columns, rule-based classification hits its limits. ML classifiers trained on labeled data can achieve higher coverage:

```python
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
import pandas as pd

class MLColumnClassifier:
    """
    Train an ML model to classify column sensitivity based on:
    - Column name
    - Data type
    - Sample values (string representation)
    - Statistical features (null rate, cardinality ratio, etc.)
    """
    
    def __init__(self):
        self.pipeline = None
        self.label_encoder = None
    
    def _extract_features(self, samples: list[dict]) -> pd.DataFrame:
        """
        Extract features from column metadata for ML classification.
        Each sample: {name, dtype, sample_values, null_rate, cardinality}
        """
        rows = []
        for s in samples:
            sample_str = " ".join(str(v) for v in s.get("sample_values", []) if v is not None)
            rows.append({
                "text": f"{s['name']} {s['dtype']} {sample_str}",  # Combined text feature
                "null_rate": s.get("null_rate", 0),
                "cardinality_ratio": s.get("cardinality_ratio", 1),
                "max_length": max((len(str(v)) for v in s.get("sample_values", []) if v), default=0),
                "avg_length": np.mean([len(str(v)) for v in s.get("sample_values", []) if v]) if s.get("sample_values") else 0,
            })
        return pd.DataFrame(rows)
    
    def train(self, labeled_columns: list[dict]):
        """
        Train on labeled column data.
        labeled_columns: [{name, dtype, sample_values, null_rate, cardinality_ratio, label}]
        """
        from sklearn.preprocessing import LabelEncoder
        from scipy.sparse import hstack
        import scipy
        
        df = self._extract_features(labeled_columns)
        labels = [s["label"] for s in labeled_columns]
        
        self.label_encoder = LabelEncoder()
        y = self.label_encoder.fit_transform(labels)
        
        X_text = TfidfVectorizer(ngram_range=(1, 2), max_features=5000).fit_transform(df["text"])
        X_numeric = scipy.sparse.csr_matrix(df[["null_rate", "cardinality_ratio", "max_length", "avg_length"]].values)
        X = hstack([X_text, X_numeric])
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        clf = RandomForestClassifier(n_estimators=200, random_state=42)
        clf.fit(X_train, y_train)
        
        y_pred = clf.predict(X_test)
        print(classification_report(y_test, y_pred, target_names=self.label_encoder.classes_))
        
        self.pipeline = clf
        self.vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=5000)
        return clf
    
    def predict(self, column_metadata: dict) -> tuple[str, float]:
        """Predict sensitivity label + confidence for a column."""
        features = self._extract_features([column_metadata])
        # In production: use saved vectorizer + clf
        prediction = self.pipeline.predict(features)[0]
        probabilities = self.pipeline.predict_proba(features)[0]
        confidence = probabilities.max()
        label = self.label_encoder.inverse_transform([prediction])[0]
        return label, confidence
```

---

## Enterprise Classification Taxonomy Design

Design a classification taxonomy that scales across business units:

```python
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ClassificationNode:
    """
    Hierarchical taxonomy node.
    Root → Domain → Subdomain → Leaf tag
    """
    id: str
    name: str
    description: str
    parent_id: Optional[str] = None
    regulatory_scope: List[str] = field(default_factory=list)
    required_controls: List[str] = field(default_factory=list)
    children: List["ClassificationNode"] = field(default_factory=list)

# Example enterprise taxonomy
TAXONOMY = ClassificationNode(
    id="root",
    name="Data Sensitivity Taxonomy",
    description="Enterprise-wide classification hierarchy",
    children=[
        ClassificationNode(
            id="pii",
            name="Personal Information",
            description="Data that can identify an individual",
            regulatory_scope=["gdpr", "ccpa"],
            required_controls=["masking", "rbac", "audit_log", "encryption"],
            children=[
                ClassificationNode(id="pii.direct", name="Direct Identifiers",
                                   description="Directly identifies: name, email, SSN"),
                ClassificationNode(id="pii.quasi", name="Quasi-Identifiers",
                                   description="Linkable when combined: ZIP+DOB+gender"),
                ClassificationNode(id="pii.sensitive", name="Sensitive PII",
                                   description="Special categories under GDPR Art 9: health, religion, sexuality"),
            ]
        ),
        ClassificationNode(
            id="financial",
            name="Financial Data",
            description="Revenue, payroll, forecasts",
            regulatory_scope=["sox", "pci-dss"],
            required_controls=["rbac", "encryption", "audit_log"],
        ),
        ClassificationNode(
            id="health",
            name="Protected Health Information",
            description="HIPAA-regulated medical data",
            regulatory_scope=["hipaa"],
            required_controls=["masking", "rbac", "audit_log", "encryption", "breach_notification"],
        ),
        ClassificationNode(
            id="proprietary",
            name="Proprietary / Trade Secrets",
            description="IP, source code, unpublished research",
            required_controls=["rbac", "encryption", "dlp"],
        ),
    ]
)

def get_controls_for_tag(taxonomy: ClassificationNode, tag_id: str) -> List[str]:
    """Walk the taxonomy tree to find required controls for a tag."""
    if taxonomy.id == tag_id:
        return taxonomy.required_controls
    for child in taxonomy.children:
        result = get_controls_for_tag(child, tag_id)
        if result is not None:
            # Merge parent + child controls
            return list(set(taxonomy.required_controls + result))
    return None
```

---

## Classification as a Policy Gate

Block pipelines that don't meet classification requirements:

```python
def validate_pipeline_classification(
    input_tables: list[str],
    output_tables: list[str],
    catalog_client,
) -> dict:
    """
    Validate that a pipeline:
    1. Doesn't downgrade classification (restricted → internal)
    2. Applies masking before writing PII to lower-sensitivity tables
    3. Has PII handling documented
    """
    violations = []
    
    # Get input sensitivity levels
    input_sensitivity = {t: catalog_client.get_sensitivity(t) for t in input_tables}
    
    for output_table in output_tables:
        output_sensitivity = catalog_client.get_sensitivity(output_table)
        
        for input_table, input_level in input_sensitivity.items():
            # Check: input is more sensitive than output → potential data leakage
            SENSITIVITY_ORDER = ["public", "internal", "confidential", "restricted"]
            
            input_rank = SENSITIVITY_ORDER.index(input_level) if input_level in SENSITIVITY_ORDER else 0
            output_rank = SENSITIVITY_ORDER.index(output_sensitivity) if output_sensitivity in SENSITIVITY_ORDER else 0
            
            if input_rank > output_rank:
                # Input is more sensitive than output — check if masking is applied
                pii_cols_from_input = catalog_client.get_pii_columns(input_table)
                output_cols = catalog_client.get_columns(output_table)
                
                # Are any PII columns flowing to the lower-sensitivity table?
                pii_in_output = [c for c in pii_cols_from_input if c in output_cols]
                
                if pii_in_output:
                    violations.append({
                        "type": "sensitivity_downgrade",
                        "input": f"{input_table} ({input_level})",
                        "output": f"{output_table} ({output_sensitivity})",
                        "pii_columns_at_risk": pii_in_output,
                        "message": f"PII columns {pii_in_output} flowing to lower-sensitivity table without masking",
                    })
    
    return {
        "valid": len(violations) == 0,
        "violations": violations,
    }
```

---

## Interview Tips

> **Tip 1:** "When would you use ML-based vs. rule-based classification?" — Rule-based for well-known PII patterns (email, phone, SSN): fast, interpretable, no training data needed. ML-based for domain-specific classification (e.g., HR-specific financial terms, proprietary data types) where column names are inconsistent or creative. In practice: rules first, ML for the long tail.

> **Tip 2:** "How do you handle classification inheritance?" — When silver.orders (internal) feeds gold.orders and includes a new column from a restricted source, the output classification should inherit the highest sensitivity level of any input. Implement sensitivity propagation in lineage-aware classification: walk upstream lineage, take the max sensitivity level.

> **Tip 3:** "What is a data classification policy gate?" — A CI/CD check that prevents deploying a pipeline if classification requirements aren't met. Examples: blocked merge if new columns aren't classified; blocked deployment if a restricted input flows to a public output without masking; required DPO sign-off for any pipeline that processes GDPR-sensitive data. Classification as code — policies enforced at deploy time, not discovered in audit.

## ⚡ Cheat Sheet

**Sensitivity taxonomy**: Public → Internal → Confidential → Restricted

**PII categories**
- Direct: full name, email, phone, SSN, passport
- Quasi: user_id + IP + location (combinable to identify)
- Special (GDPR Art 9): health, biometrics, sexual orientation
- Financial: credit card, salary, bank account

**Auto-classification**
```python
import re
PII_PATTERNS = {
    "email": r"^[\w._%+-]+@[\w.-]+\.[a-z]{2,}$",
    "phone": r"^\+?[1-9]\d{7,14}$",
    "ssn":   r"^\d{3}-\d{2}-\d{4}$",
}
# Combine: col name match + pattern on sample values → confidence score
# >0.85: auto-tag; <0.85: human review queue
```

**Snowflake tags**
```sql
ALTER TABLE gold.customers MODIFY COLUMN email SET TAG sensitivity = 'restricted';
ALTER TABLE gold.customers MODIFY COLUMN email SET TAG pii_type = 'email';
```

**dbt schema.yml**
```yaml
columns:
  - name: email
    meta: {sensitivity: restricted, pii_type: email}
    tags: [pii, gdpr]
```

**Enforcement checklist**
- Masking policy on all restricted columns
- No PII in dev/test — use synthetic data
- CI gate: block new PII columns without tag
- Quarterly re-scan for drift
