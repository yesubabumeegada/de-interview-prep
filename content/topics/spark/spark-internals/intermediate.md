---
title: "Spark Internals — Intermediate"
topic: spark
subtopic: spark-internals
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, catalyst-rules, rule-batches, strategy, planner, codegen-fallback, extension]
---

# Spark Internals — Intermediate

## Catalyst Rule Batches

Catalyst's optimizer applies rules in ordered batches. Each batch runs until no more rules fire (fixpoint) or for a fixed number of iterations:

```scala
// Simplified Catalyst structure:
object Optimizer extends RuleExecutor {
  val batches = Seq(
    Batch("Eliminate Distinct", FixedPoint(1),
      EliminateDistinct),
    Batch("Substitution", FixedPoint(100),
      CombineUnions,
      ReplaceDeduplicateWithAggregate),
    Batch("Aggregate", FixedPoint(100),
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions),
    Batch("Operator Optimization before Inferring Filters", FixedPoint(100),
      PushDownPredicates,
      PushPredicateThroughNonJoin,
      BooleanSimplification,
      ConstantFolding,
      EliminateSorts,
      ...),
    Batch("Join Reordering", FixedPoint(1),
      CostBasedJoinReorder),
    ...
  )
}
```

```python
# Inspect the full optimizer rule list (Scala/Python introspection):
spark._jvm.org.apache.spark.sql.catalyst.optimizer.Optimizer

# Disable a specific rule (debugging/workaround):
spark.conf.set("spark.sql.optimizer.excludedRules",
    "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
```

---

## Writing Custom Catalyst Rules

For advanced users: inject custom optimization rules into the Catalyst pipeline:

```python
from pyspark.sql.catalyst.plans.logical import LogicalPlan
from pyspark.sql.catalyst.rules import Rule

# Example: auto-tag PII columns with a column suffix
class TagPIIColumns(Rule):
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        # Walk the plan tree and modify as needed
        return plan.transformAllExpressions(self._tag_pii)

    def _tag_pii(self, expr):
        # Add _PII suffix to known sensitive column references
        pii_columns = {"email", "phone", "ssn", "dob"}
        if hasattr(expr, "name") and expr.name in pii_columns:
            return expr.alias(f"{expr.name}_PII")
        return expr

# Register the rule (Scala API via Py4J)
spark._jvm.org.apache.spark.sql.SparkSessionExtensions \
    .injectOptimizerRule(TagPIIColumns())
```

```python
# Spark Extensions API (spark.conf or programmatic):
def custom_extensions(extensions):
    extensions.injectOptimizerRule(lambda _: MyRule())
    extensions.injectPlannerStrategy(lambda _: MyStrategy())

spark = SparkSession.builder \
    .withExtensions(custom_extensions) \
    .getOrCreate()
```

---

## Physical Planning: SparkStrategy and SparkPlanner

After logical optimization, `SparkPlanner` applies `SparkStrategy` instances to convert the logical plan to physical operators:

```
SparkStrategies applied in order:
  1. FileSourceStrategy   — DataSource V1 reads
  2. DataSourceV2Strategy — DataSource V2 (Iceberg, Delta)
  3. SpecialLimits        — LIMIT operations
  4. Aggregation          — GROUP BY, agg functions
  5. JoinSelection        — choose BHJ vs SMJ vs BNLJ
  6. InMemoryScans        — cached DataFrames
  7. BasicOperators       — filter, project, union, sort
```

```python
# JoinSelection strategy decides join type:
# It checks (in order):
# 1. Can we broadcast either side? → BroadcastHashJoin
# 2. Are both sides already sorted on join key? → SortMergeJoin without sort
# 3. Can smaller side fit in memory as hash table? → ShuffleHashJoin
# 4. Default → SortMergeJoin

# You can see which strategy was chosen:
df.explain(mode="formatted")
# Look for: BroadcastHashJoin, SortMergeJoin, ShuffleHashJoin, or BroadcastNestedLoopJoin
```

---

## Code Generation Deep Dive

Whole-stage code generation fuses multiple operators into a single Java class:

```python
# Check if whole-stage codegen is active:
df.explain()
# *(N) prefix = inside whole-stage codegen for stage N

# Disable WSCG (for debugging):
spark.conf.set("spark.sql.codegen.wholeStage", "false")

# Generate and inspect the code:
df.queryExecution.debug.codegen()
# (Scala API; shows generated Java source)
```

**When does codegen fall back?**
```python
# Codegen falls back to interpreted mode for:
# 1. UDFs (Python UDFs are black boxes to JVM)
# 2. Very complex expressions (too many branches)
# 3. Operators not yet migrated to WSCG (rare in modern Spark)
# 4. Explicit disable

# Fall back indication in plan:
# No *(N) prefix = interpreted mode (slower)
```

---

## AQE Internals: Re-Optimization

AQE inserts `QueryStageExec` nodes at shuffle boundaries:

```
Physical Plan with AQE:
  HashAggregate (final)
  └── AdaptiveSparkPlan (isFinalPlan: false)
      └── ShuffleQueryStageExec
          └── Exchange (shuffle)
              └── HashAggregate (partial)
                  └── FileScan

Execution:
  1. Stage 0 executes (FileScan + partial HashAgg)
  2. ShuffleQueryStageExec completes, measures output sizes
  3. AQE re-plans:
     - Was 200 shuffle partitions → coalesce to 8 (actual data was small)
     - Was SortMergeJoin → switch to BroadcastHashJoin (smaller side was 5MB)
  4. Stage 1 executes with new optimized plan
```

```python
# See AQE in action — compare plans at different stages:
df.explain()  # initial plan (isFinalPlan=false for AQE nodes)

# After execution, view final AQE-adjusted plan:
df.queryExecution.executedPlan.toString()
# Look for: AdaptiveSparkPlan isFinalPlan=true
```

---

## Interview Tips

> **Tip 1:** "How does Catalyst differ from traditional database query optimizers?" — Traditional optimizers (like PostgreSQL's planner) are monolithic and hard to extend. Catalyst is built as a composable rule engine — optimization rules are ordinary Scala functions on AST nodes, applied in configurable batches. New rules can be added without modifying core code, and rules can be disabled for debugging. The tree transformation API (transform, transformDown, transformUp) makes it easy to write and test new rules independently.

> **Tip 2:** "How does AQE re-optimize a plan without re-executing stages?" — AQE wraps shuffle boundaries in `QueryStageExec` nodes. When a stage completes, Spark measures its actual output size (partition sizes, row counts). It then re-invokes the planner with real statistics instead of estimates. If the statistics justify different choices (smaller than threshold → broadcast; skewed partition → split), the downstream plan is rewritten before executing the next stage. Previously executed stages are not re-run.

> **Tip 3:** "What's the difference between the Analyzed and Optimized Logical Plan?" — Analyzed plan has resolved column references (column names → actual column objects with types), validated type compatibility, and checked that all referenced tables/functions exist. Optimized plan has run Catalyst's rule-based transformations: predicates pushed down, columns pruned, constants folded, subqueries decorrelated. The Analyzed plan is the "what you asked for"; the Optimized plan is the semantically equivalent but more efficient version.
