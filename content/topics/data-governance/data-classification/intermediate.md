---
title: "Data Classification — Intermediate"
topic: data-governance
subtopic: data-classification
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-classification, automated-classification, tagging, sensitivity, ml-classification]
---

# Data Classification — Intermediate

## Automated Classification Pipeline

Run classification on all tables automatically and flag unclassified assets:

```python
from dataclasses import dataclass
from typing import List, Dict, Optional
import sqlalchemy as sa

@dataclass
class ClassificationResult:
    table_name: str
    column_name: str
    inferred_sensitivity: str
    inferred_tags: List[str]
    confidence: float
    method: str  # "name_pattern" | "value_pattern" | "catalog_existing"

class AutoClassifier:
    """
    Classify data assets automatically by combining:
    1. Column name heuristics
    2. Value sampling
    3. Existing catalog tags
    """
    
    SENSITIVITY_RULES = [
        # (tag, sensitivity_level, column_name_keywords)
        ("pii:email",     "restricted",     ["email", "e_mail"]),
        ("pii:phone",     "restricted",     ["phone", "mobile", "cell"]),
        ("pii:ssn",       "restricted",     ["ssn", "social_security", "tin"]),
        ("pii:name",      "restricted",     ["first_name", "last_name", "full_name"]),
        ("pii:address",   "restricted",     ["address", "street", "city"]),
        ("financial",     "confidential",   ["salary", "revenue", "profit", "margin", "payroll"]),
        ("strategic",     "confidential",   ["forecast", "budget", "headcount_plan"]),
        ("health",        "restricted",     ["diagnosis", "condition", "medication", "icd_"]),
    ]
    
    def classify_table(self, engine, schema: str, table: str) -> List[ClassificationResult]:
        """Classify all columns in a table."""
        results = []
        
        with engine.connect() as conn:
            # Get column names
            cols = conn.execute(sa.text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """), {"schema": schema, "table": table}).fetchall()
        
        for col_row in cols:
            col_name = col_row.column_name.lower()
            
            for tag, sensitivity, keywords in self.SENSITIVITY_RULES:
                if any(kw in col_name for kw in keywords):
                    results.append(ClassificationResult(
                        table_name=f"{schema}.{table}",
                        column_name=col_row.column_name,
                        inferred_sensitivity=sensitivity,
                        inferred_tags=[tag, sensitivity],
                        confidence=0.75,
                        method="name_pattern",
                    ))
                    break
        
        return results
    
    def classify_all(self, engine, target_schema: str) -> Dict[str, List[ClassificationResult]]:
        """Classify all tables in a schema."""
        with engine.connect() as conn:
            tables = conn.execute(sa.text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = :schema AND table_type = 'BASE TABLE'
            """), {"schema": target_schema}).scalars().all()
        
        return {t: self.classify_table(engine, target_schema, t) for t in tables}
    
    def generate_tagging_sql(self, results: List[ClassificationResult]) -> str:
        """Generate SQL to apply tags in Snowflake."""
        sqls = []
        for r in results:
            if r.confidence >= 0.7:
                sqls.append(
                    f"ALTER TABLE {r.table_name} MODIFY COLUMN {r.column_name} "
                    f"SET TAG pii_type = '{r.inferred_tags[0]}';"
                )
        return "\n".join(sqls)
```

---

## Classification-Driven Controls

Apply the right controls automatically based on classification:

```python
from enum import Enum

class SensitivityLevel(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class ClassificationControlEnforcer:
    """
    Given a classification level, apply the appropriate technical controls.
    """
    
    def __init__(self, snowflake_client, s3_client, catalog_client):
        self.sf = snowflake_client
        self.s3 = s3_client
        self.catalog = catalog_client
    
    def enforce(self, table_name: str, sensitivity: SensitivityLevel):
        """Apply controls appropriate for the classification level."""
        
        if sensitivity == SensitivityLevel.RESTRICTED:
            self._apply_restricted_controls(table_name)
        elif sensitivity == SensitivityLevel.CONFIDENTIAL:
            self._apply_confidential_controls(table_name)
        elif sensitivity == SensitivityLevel.INTERNAL:
            self._apply_internal_controls(table_name)
        # PUBLIC: no additional controls needed
    
    def _apply_restricted_controls(self, table_name: str):
        """Restricted: masking + strict RBAC + audit logging."""
        # 1. Apply masking policy to all PII columns
        pii_cols = self.catalog.get_pii_columns(table_name)
        for col in pii_cols:
            self.sf.execute(f"""
                ALTER TABLE {table_name} MODIFY COLUMN {col}
                SET MASKING POLICY DEFAULT_PII_MASK
            """)
        
        # 2. Revoke SELECT from PUBLIC role
        self.sf.execute(f"REVOKE SELECT ON TABLE {table_name} FROM ROLE PUBLIC")
        
        # 3. Grant only to restricted role
        self.sf.execute(f"GRANT SELECT ON TABLE {table_name} TO ROLE PII_APPROVED_READER")
        
        # 4. Enable audit logging
        self.sf.execute(f"""
            ALTER TABLE {table_name} SET DATA_RETENTION_TIME_IN_DAYS = 90
        """)
        
        print(f"Applied RESTRICTED controls to {table_name}")
    
    def _apply_confidential_controls(self, table_name: str):
        """Confidential: role-based access + encryption."""
        self.sf.execute(f"REVOKE SELECT ON TABLE {table_name} FROM ROLE ANALYST_READ")
        self.sf.execute(f"GRANT SELECT ON TABLE {table_name} TO ROLE ANALYST_CONFIDENTIAL")
        print(f"Applied CONFIDENTIAL controls to {table_name}")
    
    def _apply_internal_controls(self, table_name: str):
        """Internal: require authentication, restrict public."""
        # Already covered by SSO + Snowflake auth
        print(f"Applied INTERNAL controls to {table_name} (SSO enforcement)")
```

---

## Classification Coverage Reporting

```sql
-- Classification coverage dashboard
SELECT
    table_schema AS schema_name,
    COUNT(DISTINCT table_name) AS total_tables,
    COUNT(DISTINCT CASE WHEN sensitivity_tag IS NOT NULL THEN table_name END) AS classified_tables,
    ROUND(
        COUNT(DISTINCT CASE WHEN sensitivity_tag IS NOT NULL THEN table_name END) * 100.0 
        / NULLIF(COUNT(DISTINCT table_name), 0), 1
    ) AS classification_coverage_pct,
    COUNT(CASE WHEN sensitivity_tag = 'restricted' THEN 1 END) AS restricted_columns,
    COUNT(CASE WHEN sensitivity_tag = 'confidential' THEN 1 END) AS confidential_columns,
    COUNT(CASE WHEN sensitivity_tag IS NULL THEN 1 END) AS unclassified_columns
FROM (
    SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        t.tag_value AS sensitivity_tag
    FROM information_schema.columns c
    LEFT JOIN TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
        'sensitivity', 'COLUMN'
    )) t ON t.object_name = c.column_name
    WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
) subq
GROUP BY table_schema
ORDER BY classification_coverage_pct ASC;  -- Show worst first
```

---

## Interview Tips

> **Tip 1:** "How do you automate data classification?" — Two-phase approach: (1) Static analysis: scan column names against a PII keyword dictionary (high precision, fast). (2) Dynamic analysis: sample column values against regex patterns for emails, SSNs, phone numbers (catches misnamed columns). Combine signals with confidence scoring. Flag high-confidence findings for auto-tagging, low-confidence for human review.

> **Tip 2:** "What's the difference between a tag and a classification?" — A tag is a label applied to an asset (e.g., `pii:email`). A classification is a broader category (e.g., `restricted`). Tags are specific and multiple can apply. Classification is the overall sensitivity tier that drives controls. One classification level (restricted) can encompass multiple tags (pii:email, pii:name, phi).

> **Tip 3:** "How do you keep classification current as data evolves?" — Run classification scanner on every new table (CI check), every schema change (catalog trigger), and weekly full scans. Alert when unclassified columns are found. Integrate with schema registry: new fields need classification before deployment. Otherwise classification drift leads to unprotected data.
