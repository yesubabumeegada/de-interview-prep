---
title: "Spark Internals — Real World"
topic: spark
subtopic: spark-internals
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, internals, debugging, custom-extension, rule-disable, production]
---

# Spark Internals — Real World

## War Story: Catalyst Rule Bug Causing Wrong Results

**Scenario:** A financial reconciliation job started producing incorrect totals after upgrading from Spark 2.4 to Spark 3.0. The same query, same data, different numbers.

**Investigation:**
```python
# Run with extended explain to see what optimizer is doing:
df.explain(mode="extended")

# Compare optimized logical plan on 2.4 vs 3.0
# Found in 3.0 optimized plan:
# A new rule "EliminateOuterJoin" was removing a left join
# The rule incorrectly determined the join result was all matched
# (bug: statistics were wrong; rule assumed no nulls on join key)
```

**Fix:**
```python
# Disable the buggy rule while vendor patch is in progress:
spark.conf.set("spark.sql.optimizer.excludedRules",
    "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin")

# Also: always test correctness (not just performance) after version upgrades:
# 1. Run old and new versions in parallel on sample data
# 2. Compare row counts and aggregate values
# 3. Check explain() diffs for plan changes
```

---

## War Story: Custom Extension for PII Column Masking

**Scenario:** Security audit required that PII columns (email, SSN, phone) be automatically masked in DataFrames returned to interactive users, without requiring every analyst to remember to apply masking.

**Implementation:**
```python
# Custom logical plan rule that automatically wraps PII columns in mask()
from pyspark.sql.catalyst.plans.logical import Project
from pyspark.sql.catalyst.expressions import Alias, AttributeReference

PII_COLUMNS = {"email", "ssn", "phone_number", "date_of_birth"}

def mask_pii_rule(plan):
    """Inject masking around any Project that selects PII columns."""
    def transform_project(p):
        if not isinstance(p, Project):
            return p
        new_exprs = []
        for expr in p.projectList():
            name = getattr(expr, "name", "")
            if name in PII_COLUMNS:
                # Replace with masked version: hash + first 2 chars visible
                masked = F.concat(F.substring(F.col(name), 1, 2), F.lit("***"))
                new_exprs.append(masked.alias(name)._jc)
            else:
                new_exprs.append(expr)
        return p.copy(p.child(), new_exprs)
    return plan.transformDown(transform_project)

# Register as a post-parsing rule (before optimization)
# This ensures masking is applied before any optimizer might push it away
```

---

## Debugging the Query Plan Programmatically

```python
# Access query execution details:
df = orders.groupBy("region").sum("amount")

qe = df.queryExecution
print(qe.analyzed)          # Analyzed logical plan
print(qe.optimizedPlan)     # After Catalyst optimization
print(qe.sparkPlan)         # Physical plan (before preparations)
print(qe.executedPlan)      # Final physical plan (with AQE if enabled)

# Count nodes of each type in the physical plan:
def count_nodes(plan):
    from pyspark.sql.execution import SparkPlan
    counts = {}
    def walk(p):
        name = type(p).__name__
        counts[name] = counts.get(name, 0) + 1
        for child in p.children():
            walk(child)
    walk(plan._jvm_plan)
    return counts

# Check for unexpected join types:
plan_str = str(df.queryExecution.executedPlan)
if "BroadcastNestedLoopJoin" in plan_str:
    print("WARNING: BroadcastNestedLoopJoin detected — likely a non-equi join!")
if "CartesianProduct" in plan_str:
    print("WARNING: Cartesian product detected — cross join without condition!")
```

---

## Interview Tips

> **Tip 1:** "How would you debug a query that gives wrong results after a Spark upgrade?" — Compare the optimized logical plans between versions using `df.explain(mode="extended")`. Look for new optimizer rules that change the plan structure — predicate elimination, join reordering, or subquery decorrelation changes. If you find a new rule causing the issue, disable it with `spark.sql.optimizer.excludedRules` while filing a bug or upgrading to a patched version. Always test correctness by comparing aggregate values and row counts between versions on representative data before upgrading production.

> **Tip 2:** "Have you ever built a custom Catalyst extension?" — Strong answers describe a real use case: automatic PII masking (security), auto-tagging columns with lineage metadata, custom predicate pushdown for a proprietary storage system, or custom cost model for a special hardware configuration. The key points to cover: which extension point was used (optimizer rule vs planner strategy vs data source), what tree transformation was applied, and how it was registered (withExtensions API).
