---
title: "Data Classification — Scenarios"
topic: data-governance
subtopic: data-classification
content_type: scenario_question
tags: [data-classification, interview, scenarios, sensitivity, governance]
---

# Data Classification — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Classify a New Table

**Scenario:** A new table `gold.employee_records` is being created with columns: `employee_id`, `full_name`, `department`, `salary`, `manager_email`, `performance_rating`. How would you classify this table and its columns?

<details>
<summary>💡 Hint</summary>

Classify at two levels: the table (by its most sensitive column) and each column individually. Use a standard sensitivity taxonomy (public / internal / confidential / restricted). For each column, ask: is this a direct identifier (name, email = PII → restricted), a quasi-identifier that enables re-identification (employee_id + role = confidential), or a general business attribute (department = internal)? Salary and performance ratings are confidential. The table inherits the highest column classification — so `employee_records` is **restricted**.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Classification Inconsistency Discovered

**Scenario:** During an audit, you discover that `gold.customers` is classified as 'internal' but contains a `customer_email` column that has no PII tag. How did this happen and how do you fix it?

<details>
<summary>💡 Hint</summary>

Classification drift is almost always a schema evolution problem: the table was classified at creation, then a new column was added later without triggering re-classification. Work through the root causes in order: (1) was the column there at classification time, (2) was the column obfuscated (`contact_info` instead of `email`), or (3) was the auto-scanner not rerun after the schema change? The fix is both immediate (reclassify and apply masking now) and systemic (run the PII scanner on every schema change event, not just on table creation).

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Building Classification for 50,000 Columns

**Scenario:** Your company has 500 tables with an average of 100 columns each — 50,000 columns total. You need to classify all of them. How do you approach this at scale?

<details>
<summary>💡 Hint</summary>

50,000 columns can't be manually reviewed — automate the first 80%. Use a classifier that combines column name pattern matching (regex for `email`, `ssn`, `phone`), data-type heuristics, and sample value inspection (does this VARCHAR look like an email address?). Auto-classify with confidence scores: high-confidence results go straight to the catalog, low-confidence go to a human review queue. Then enforce forward by triggering the classifier on every schema change event — so new columns are classified automatically within hours, not discovered in the next annual audit.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>