---
title: "Data Profiling — Scenarios"
topic: data-quality
subtopic: data-profiling
content_type: scenario_question
tags: [data-profiling, interview, scenarios]
---

# Data Profiling — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Profiling a New Dataset

**Scenario:** You receive a CSV file from an external vendor for the first time. What do you do before building a pipeline?

<details>
<summary>💡 Hint</summary>

**Before building the pipeline, document:** 1. Assumed primary key and uniqueness check 2. Required vs optional columns 3. Expected value ranges for numeric columns 4. Accepted values for categorical columns 5. Expected row count range per delivery 6. Freshness expectation (how old can this data...

</details>

<details>
<summary>✅ Solution</summary>

```python
import pandas as pd
from ydata_profiling import ProfileReport

# Step 1: Quick look
df = pd.read_csv("vendor_data.csv")
print(df.shape)
print(df.dtypes)
print(df.head())
print(df.describe())
print(df.isnull().sum())

# Step 2: Full profile
profile = ProfileReport(df, title="Vendor Data Initial Profile")
profile.to_file("vendor_profile.html")

# Step 3: Questions to answer from the profile:
# - Are there any columns with >50% nulls? (low quality)
# - Is there a clear primary key? (uniqueness check)
# - Are date fields parsed correctly?
# - Are there suspicious values (negative prices, future dates)?
# - What are the cardinalities? (helps design accepted_values rules)
# - Are there duplicates?
```

**Before building the pipeline, document:**
1. Assumed primary key and uniqueness check
2. Required vs optional columns
3. Expected value ranges for numeric columns
4. Accepted values for categorical columns
5. Expected row count range per delivery
6. Freshness expectation (how old can this data be?)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Profile Comparison After Migration

**Scenario:** You migrated 500M rows from Oracle to Snowflake. How do you validate the migration was complete and accurate?

<details>
<summary>💡 Hint</summary>

**Checks beyond aggregate stats:** 1. Sample 1000 random rows from both, join on PK, compare field-by-field 2. Check all date ranges match (no partitions missing) 3. Verify encoding for special characters (Oracle vs UTF-8) 4. Check for trailing spaces in string columns (common Oracle issue)

</details>

<details>
<summary>✅ Solution</summary>

```python
import pandas as pd
import sqlalchemy as sa

# Profile source (Oracle)
oracle_engine = sa.create_engine("oracle+cx_oracle://user:pass@oracle:1521/db")
oracle_stats = pd.read_sql("""
    SELECT
        COUNT(*) as row_count,
        COUNT(DISTINCT order_id) as distinct_orders,
        SUM(amount) as total_amount,
        AVG(amount) as mean_amount,
        MIN(order_date) as min_date,
        MAX(order_date) as max_date,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers
    FROM orders
""", oracle_engine)

# Profile target (Snowflake)
sf_engine = sa.create_engine("snowflake://user:pass@account/db/schema")
sf_stats = pd.read_sql("""
    SELECT
        COUNT(*) as row_count,
        COUNT(DISTINCT order_id) as distinct_orders,
        SUM(amount) as total_amount,
        AVG(amount) as mean_amount,
        MIN(order_date) as min_date,
        MAX(order_date) as max_date,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers
    FROM orders
""", sf_engine)

# Compare
tolerance = 0.001  # 0.1% tolerance for floating point
for metric in ["row_count", "distinct_orders", "total_amount", "null_customers"]:
    src = oracle_stats[metric].iloc[0]
    tgt = sf_stats[metric].iloc[0]
    delta = abs(src - tgt) / max(abs(src), 1)
    status = "✓ PASS" if delta <= tolerance else "✗ FAIL"
    print(f"{status} {metric}: Oracle={src:,.2f}, Snowflake={tgt:,.2f}, delta={delta:.4%}")
```

**Checks beyond aggregate stats:**
1. Sample 1000 random rows from both, join on PK, compare field-by-field
2. Check all date ranges match (no partitions missing)
3. Verify encoding for special characters (Oracle vs UTF-8)
4. Check for trailing spaces in string columns (common Oracle issue)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Profiling-Driven DQ System

**Scenario:** Explain how you'd build a system where profiling automatically generates and maintains DQ rules as data evolves.

<details>
<summary>💡 Hint</summary>

The system needs two modes: *onboarding* (profile a new table, auto-generate rules, human review and commit to Git) and *continuous* (daily profile run, compare stats to baseline, flag drift, update rules when change is intentional). The key tension is automation vs false positives — think about when the system should auto-update rules vs alert for human review. Also consider which statistics to track: row count, null rate, cardinality, distribution percentiles, and referential integrity.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**
```
1. Initial Onboarding
   Profile → Auto-generate rules → Human review → Rules committed to Git

2. Continuous Drift Detection
   Daily profile → Compare to baseline → Flag significant changes →
   Update rules if change is intentional → Alert if unintentional drift

3. Rule Lifecycle
   rules/ are versioned in Git
   Rule "confidence" starts at "candidate" → "active" after 14 stable days
   Rules auto-deprecated if column removed from schema
```

**Drift response logic:**
```python
def handle_profile_drift(col: str, metric: str, old_val: float, new_val: float):
    delta_pct = abs(new_val - old_val) / max(abs(old_val), 0.001) * 100
    
    if delta_pct > 20:  # Significant change
        # Check if a schema/ETL change was deployed today
        if deployment_happened_today():
            # Intentional change → update baseline
            update_baseline(col, metric, new_val)
            log_rule_update(col, metric, reason="post-deployment update")
        else:
            # Unexpected change → alert
            alert_data_team(
                col=col,
                metric=metric,
                change=f"{old_val:.2f} → {new_val:.2f} ({delta_pct:.1f}% change)",
            )
```

**Key insight:** Profiling is not a one-time activity. It's a continuous feedback loop that keeps DQ rules aligned with the reality of your data.

</details>

</article>