---
title: "Data Classification — Scenarios"
topic: data-governance
subtopic: data-classification
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [data-classification, interview, scenarios, sensitivity, governance]
---

# Data Classification — Interview Scenarios

## Scenario 1 (Junior): Classify a New Table

**Question:** A new table `gold.employee_records` is being created with columns: `employee_id`, `full_name`, `department`, `salary`, `manager_email`, `performance_rating`. How would you classify this table and its columns?

**Answer:**

```yaml
table: gold.employee_records
sensitivity: restricted  # Contains PII + financial data

columns:
  employee_id:
    sensitivity: internal
    tags: []
    reason: "Surrogate key — not PII without joining to name"
  
  full_name:
    sensitivity: restricted
    tags: [pii, pii:name]
    regulatory: [gdpr, ccpa]
    masking: hash_or_pseudonymize
  
  department:
    sensitivity: internal
    tags: []
    reason: "Not PII — aggregate-level info"
  
  salary:
    sensitivity: restricted
    tags: [financial, pii]
    regulatory: [gdpr, sox]
    reason: "Salary is personal financial data under GDPR"
    masking: null_in_non_prod
  
  manager_email:
    sensitivity: restricted
    tags: [pii, pii:email]
    regulatory: [gdpr, ccpa]
    masking: hash
  
  performance_rating:
    sensitivity: confidential
    tags: [hr-sensitive]
    regulatory: [gdpr]
    reason: "Personal data but not directly identifying — GDPR Art 5"
```

**Controls to apply:**
```
- Access: only HR team + direct manager (row-level security)
- Masking: full_name, manager_email hashed for all non-HR roles
- salary: NULL in non-prod environments
- Retention: 7 years after employment end (legal requirement)
- Regulatory: GDPR lawful basis = employee contract
```

---

## Scenario 2 (Mid-level): Classification Inconsistency Discovered

**Question:** During an audit, you discover that `gold.customers` is classified as 'internal' but contains a `customer_email` column that has no PII tag. How did this happen and how do you fix it?

**Answer:**

**Root cause analysis:**
```
Most likely causes:
1. Table was classified before the email column was added (schema evolution)
2. Manual classification missed the column
3. Automated scanner wasn't run after the column was added
4. email column was named customer_contact_info (obfuscated name → scanner missed it)
```

**Immediate fix:**
```sql
-- Update table classification
ALTER TABLE gold.customers SET TAG sensitivity = 'restricted';

-- Tag the column
ALTER TABLE gold.customers MODIFY COLUMN customer_email SET TAG pii_type = 'email';
ALTER TABLE gold.customers MODIFY COLUMN customer_email SET TAG sensitivity = 'restricted';

-- Apply masking policy to column
ALTER TABLE gold.customers MODIFY COLUMN customer_email 
  SET MASKING POLICY email_mask;

-- Revoke inappropriate access
REVOKE SELECT ON TABLE gold.customers FROM ROLE ANALYST_INTERNAL;
GRANT SELECT ON TABLE gold.customers TO ROLE PII_APPROVED_READER;
```

**Prevention:**
```python
# CI check: detect sensitivity mismatches between table and column tags
def check_classification_consistency(engine) -> list[str]:
    violations = []
    tables_with_pii_cols = get_tables_with_pii_columns(engine)
    
    for table in tables_with_pii_cols:
        table_sensitivity = get_table_sensitivity(engine, table)
        if table_sensitivity in ("public", "internal"):
            violations.append(
                f"{table}: contains PII columns but table is classified as '{table_sensitivity}' — "
                "must be 'restricted'"
            )
    return violations
```

---

## Scenario 3 (Senior): Building Classification for 50,000 Columns

**Question:** Your company has 500 tables with an average of 100 columns each — 50,000 columns total. You need to classify all of them. How do you approach this at scale?

**Answer:**

**Phase 1: Automated first pass (Week 1)**
```python
# Run distributed classifier across all Spark-accessible tables
from classification import DistributedPIIClassifier

classifier = DistributedPIIClassifier()
results = {}

for table in catalog.list_all_tables():
    df = spark.read.table(table)
    findings = classifier.classify_dataframe(df, sample_fraction=0.001)
    results[table] = findings

# Auto-tag high-confidence findings (confidence >= 0.85)
# Queue medium-confidence findings for human review
# Default-classify unmatched columns as 'internal'
```

**Phase 2: Prioritized human review (Week 2-3)**
```
Priority 1: High-traffic tables (top 50 by query count) — review all findings
Priority 2: Medium-confidence ML findings — 500 columns flagged for review  
Priority 3: Tables in finance/HR/medical domains — manual review regardless
Priority 4: Remaining — accept auto-classification, quarterly re-scan
```

**Phase 3: Governance enforcement (Week 4)**
```
- CI check: new columns must be classified before merge
- Weekly scan: alert on new unclassified columns
- Quarterly re-classification: re-scan with updated classifier
```

**Metrics to track:**
```sql
SELECT
    COUNT(*) AS total_columns,
    COUNT(CASE WHEN classification IS NOT NULL THEN 1 END) AS classified,
    COUNT(CASE WHEN classification IS NULL THEN 1 END) AS unclassified,
    ROUND(COUNT(CASE WHEN classification IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) AS coverage_pct,
    COUNT(CASE WHEN classification = 'restricted' THEN 1 END) AS restricted_count
FROM column_classifications;
```

**Key insight:** Don't aim for 100% on day 1. Start with coverage of the highest-risk and highest-traffic tables. Use automation to handle scale, humans for edge cases and appeals. Classification is an ongoing program, not a one-time project.
