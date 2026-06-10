---
title: "Data Profiling — Senior Deep Dive"
topic: data-quality
subtopic: data-profiling
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-profiling, automated-rules, catalog, governance, scale]
---

# Data Profiling — Senior Deep Dive

## Profiling-Driven Rule Generation

The senior engineer's goal: make DQ rule creation systematic, not manual.

```python
from dataclasses import dataclass
from typing import List, Optional, Any
import pandas as pd
import numpy as np

@dataclass
class GeneratedRule:
    rule_type: str
    column: str
    params: dict
    confidence: str  # "high", "medium", "low"
    rationale: str

class AutoRuleGenerator:
    """Generate DQ rules from profiling statistics."""
    
    def __init__(self, df: pd.DataFrame, profile: dict):
        self.df = df
        self.profile = profile
    
    def generate_all_rules(self) -> List[GeneratedRule]:
        rules = []
        
        for col, stats in self.profile["columns"].items():
            rules.extend(self._generate_col_rules(col, stats))
        
        return rules
    
    def _generate_col_rules(self, col: str, stats: dict) -> List[GeneratedRule]:
        rules = []
        
        # Not-null rule
        if stats["null_pct"] == 0:
            rules.append(GeneratedRule(
                rule_type="not_null",
                column=col,
                params={},
                confidence="high",
                rationale="Column has 0% nulls in observed data",
            ))
        elif stats["null_pct"] < 5:
            rules.append(GeneratedRule(
                rule_type="not_null_mostly",
                column=col,
                params={"threshold": round(1 - stats["null_pct"] / 100 - 0.02, 2)},
                confidence="medium",
                rationale=f"Column has {stats['null_pct']}% nulls — threshold with buffer",
            ))
        
        # Uniqueness rule — if cardinality matches row count
        if stats["cardinality_pct"] > 99 and stats["null_pct"] == 0:
            rules.append(GeneratedRule(
                rule_type="unique",
                column=col,
                params={},
                confidence="high",
                rationale="Near-100% cardinality suggests primary key",
            ))
        
        # Accepted values — for low-cardinality string columns
        if stats["dtype"] == "object" and stats.get("unique_count", 0) <= 15:
            accepted = self.df[col].dropna().unique().tolist()
            rules.append(GeneratedRule(
                rule_type="accepted_values",
                column=col,
                params={"values": accepted},
                confidence="high" if stats["unique_count"] <= 10 else "medium",
                rationale=f"Only {stats['unique_count']} distinct values observed",
            ))
        
        # Range rule for numerics
        if stats.get("min") is not None:
            # Use p1/p99 with 10% buffer, not absolute min/max
            series = self.df[col].dropna()
            p1 = float(series.quantile(0.01))
            p99 = float(series.quantile(0.99))
            buffer = (p99 - p1) * 0.1
            
            rules.append(GeneratedRule(
                rule_type="between",
                column=col,
                params={
                    "min_value": round(p1 - buffer, 4),
                    "max_value": round(p99 + buffer, 4),
                    "mostly": 0.99,
                },
                confidence="medium",
                rationale=f"Based on p1-p99 range with 10% buffer",
            ))
        
        return rules
    
    def to_gx_expectations(self, validator) -> None:
        """Apply generated rules as GX expectations."""
        for rule in self.generate_all_rules():
            if rule.rule_type == "not_null":
                validator.expect_column_values_to_not_be_null(rule.column)
            elif rule.rule_type == "unique":
                validator.expect_column_values_to_be_unique(rule.column)
            elif rule.rule_type == "accepted_values":
                validator.expect_column_values_to_be_in_set(rule.column, rule.params["values"])
            elif rule.rule_type == "between":
                validator.expect_column_values_to_be_between(
                    rule.column,
                    min_value=rule.params["min_value"],
                    max_value=rule.params["max_value"],
                    mostly=rule.params.get("mostly", 0.99),
                )
```

---

## Enterprise Profiling — Data Catalog Integration

```python
import boto3
from datetime import datetime

class CatalogProfiler:
    """Profile tables and publish stats to AWS Glue Data Catalog."""
    
    def __init__(self):
        self.glue = boto3.client("glue")
        self.spark = SparkSession.builder.getOrCreate()
    
    def profile_and_publish(self, database: str, table: str):
        # Read table
        df = self.spark.table(f"{database}.{table}")
        
        # Compute profile
        profile = spark_profile(df, sample_frac=0.1)
        
        # Publish as table parameters in Glue
        table_params = {
            "dq_profile_date": datetime.utcnow().isoformat(),
            "dq_row_count": str(profile["total_rows"]),
            "dq_column_count": str(len(profile["columns"])),
        }
        
        for col, stats in profile["columns"].items():
            table_params[f"dq_{col}_null_pct"] = str(stats["null_pct"])
            table_params[f"dq_{col}_cardinality"] = str(stats.get("approx_distinct", 0))
        
        self.glue.update_table(
            DatabaseName=database,
            TableInput={
                "Name": table,
                "Parameters": table_params,
            }
        )
        
        print(f"Profile published to Glue catalog for {database}.{table}")
        return profile
```

---

## Interview Tips

> **Tip 1:** "How do you use profiling in a data migration?" — Profile source before migration, profile target after migration, compare both profiles. Expect: same row counts (or documented delta), same null rates, same numeric distributions. Automated profile comparison catches data loss / transformation bugs.

> **Tip 2:** "What's the difference between profiling and monitoring?" — Profiling is exploratory (done when onboarding a dataset). Monitoring is ongoing (track metrics over time). Profiling informs monitoring setup; monitoring keeps profiling metrics current.

> **Tip 3:** "How do you handle PII in profiling?" — Never include actual values in profiling reports. Use: null counts, value counts (not the values), cardinality, length statistics. For top-values analysis, apply data masking or exclude PII columns from the report.

## ⚡ Cheat Sheet

**Great Expectations core objects**
```python
import great_expectations as gx
context = gx.get_context()

# Expectation suite
suite = context.add_expectation_suite("orders_suite")
validator = context.get_validator(batch_request=batch_req, expectation_suite_name="orders_suite")

# Common expectations
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_between("amount", 0, 100000)
validator.expect_column_pair_values_a_to_be_greater_than_b("ship_date", "order_date")
validator.expect_column_values_to_match_regex("email", r"^[\w._%+-]+@[\w.-]+\.[a-z]{2,}$")

# Run checkpoint
result = context.run_checkpoint("orders_checkpoint")
assert result["success"], f"DQ failure: {result}"
```

**Anomaly detection patterns**
```python
# Z-score for numeric columns
def zscore_anomaly(series, threshold=3.0):
    z = (series - series.mean()) / series.std()
    return z.abs() > threshold

# Rolling mean comparison (for time series)
df["rolling_avg"] = df["revenue"].rolling(7).mean()
df["anomaly"] = abs(df["revenue"] - df["rolling_avg"]) > 2 * df["revenue"].rolling(7).std()
```

**Data contract (dbt schema.yml)**
```yaml
models:
  - name: orders
    description: "Gold orders table — SLA: updated within 1 hour of source"
    config: {contract: {enforced: true}}
    columns:
      - name: order_id
        data_type: bigint
        constraints: [{type: not_null}, {type: unique}]
      - name: amount
        data_type: double
        constraints: [{type: not_null}]
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: updated_at
          interval: 2
```

**SLA monitoring**
```sql
-- Alert if table hasn't been updated within SLA window
SELECT table_name,
       MAX(updated_at) AS last_updated,
       DATEDIFF('hour', MAX(updated_at), NOW()) AS hours_since_update,
       CASE WHEN DATEDIFF('hour', MAX(updated_at), NOW()) > sla_hours THEN 'BREACHED' ELSE 'OK' END AS status
FROM table_sla_registry
JOIN gold_tables USING (table_name)
GROUP BY table_name, sla_hours;
```

**DQ dimensions**
```
Completeness:  % non-null values
Accuracy:      matches source of truth
Consistency:   same value across systems
Timeliness:    data arrives within SLA
Uniqueness:    no duplicates on PK
Validity:      conforms to expected format/range
```

**Incident response flow**
```
1. Alert fires (DQ check fails, SLA breached)
2. Triage: severity — who's impacted? (BI dashboard, ML model, external SLA?)
3. Notify: page on-call DE + inform data consumers
4. Contain: quarantine bad data (move to _quarantine schema; don't serve bad data)
5. Fix: patch pipeline or source data
6. Backfill: reprocess affected time range
7. Post-mortem: root cause + prevention (add check that would have caught this earlier)
```
