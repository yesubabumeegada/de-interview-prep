---
title: "Spark Core Concepts — Senior Deep Dive"
topic: spark
subtopic: core-concepts
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, rdd, dependency-types, stage-pipeline, whole-stage-codegen, tungsten, off-heap, dataset]
---

# Spark Core Concepts — Senior Deep Dive

## Dependency Types: Narrow vs. Wide Precisely

Spark tracks two types of dependencies between RDDs:

**NarrowDependency:** each parent partition is used by at most one child partition.

```
Parent:  [P0] [P1] [P2] [P3]
           ↓    ↓    ↓    ↓     (1-to-1 or N-to-1)
Child:   [C0] [C1] [C2] [C3]
```

Subtypes:
- `OneToOneDependency`: exact partition-to-partition mapping (`map`, `filter`)
- `RangeDependency`: used in `union` — each child partition maps to a range of parent partitions

**ShuffleDependency (Wide):** each child partition may depend on ALL parent partitions.

```
Parent:  [P0] [P1] [P2] [P3]
          ↘↘↗  ↘↗↗  ↗↘↗  ↗↗     (all-to-all)
Child:   [C0] [C1] [C2] [C3]
```

```python
# Inspect dependencies programmatically:
rdd1 = sc.parallelize(range(8), 4)
rdd2 = rdd1.map(lambda x: x * 2)        # NarrowDep: OneToOne
rdd3 = rdd1.union(rdd2)                  # NarrowDep: Range
rdd4 = rdd3.groupBy(lambda x: x % 3)    # ShuffleDep: Wide

for dep in rdd4.dependencies:
    print(type(dep).__name__)   # ShuffleDependency
```

---

## Whole-Stage Code Generation (WSCG)

Spark 2.0 introduced whole-stage code generation — instead of the Volcano iterator model (virtual function calls per row), Spark compiles a fused loop for an entire stage:

**Volcano model (old):**
```java
// Per row: call Filter.next() → calls Map.next() → calls Scan.next()
// ~5+ virtual method calls per row — cache-unfriendly, JIT-hostile
while (input.hasNext()) {
    Row row = input.next();           // virtual call
    if (filter.eval(row)) {           // virtual call
        output.emit(project(row));    // virtual call
    }
}
```

**Whole-stage codegen (modern Spark):**
```java
// Generated code — tight loop, no virtual calls, JIT-optimizable
for (int i = 0; i < batch.numRows; i++) {
    long val = batch.getLong(i, 0);    // direct field access
    if (val > 100) {                   // inlined predicate
        result.appendLong(val * 2);    // inlined projection
    }
}
```

```python
# See WSCG in action — *(N) prefix means whole-stage codegen stage N
df.filter("amount > 100").select("amount").explain()

# == Physical Plan ==
# *(1) Project [amount#0]           ← stage 1 fused
# +- *(1) Filter (amount#0 > 100)   ← same stage, same generated class
#    +- *(1) FileScan parquet
```

WSCG provides 2-10× speedup for CPU-bound operations.

---

## Tungsten Memory Management

Tungsten is Spark's execution engine, managing memory at the byte level — below the JVM GC:

```
JVM Heap (default):
  Objects have 16-byte headers, field padding, pointer chasing
  GC pauses scale with object count

Tungsten Off-Heap (UnsafeRow):
  Compact binary format — no object overhead
  Direct memory access via sun.misc.Unsafe
  GC-free: memory managed by Spark, not JVM
```

```python
# Enable off-heap memory (Spark 1.6+)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")  # per executor

# Tungsten's UnsafeRow format:
# For Row(1L, "hello", 3.14):
# [null bitmap 8B][fields 8B each][variable-length data at end]
# Total: 8 + 8 + 8 + 8 (string pointer+len) + 5 (string bytes) = 37 bytes
# vs JVM objects: ~100+ bytes with headers and padding
```

**Sort optimization:** Tungsten uses a radix sort on compact binary keys instead of Timsort on object comparisons — 3-4× faster for numeric sorts.

---

## Dataset API and Type Safety

Dataset (Spark 1.6+) combines DataFrame's Catalyst optimization with RDD's type safety:

```scala
// Scala: Dataset is fully typed
case class Order(orderId: String, amount: Double, status: String)
val ds: Dataset[Order] = spark.read.parquet("orders").as[Order]

// Compile-time type checking:
ds.filter(_.amount > 100)   // lambda is typed — IDE completion works
ds.map(o => o.copy(amount = o.amount * 1.1))   // type-safe transform
```

```python
# Python has no Dataset (no compile-time types)
# Python DataFrames are equivalent to Dataset[Row] (untyped)
# Use type hints + runtime schema validation instead:

from pyspark.sql.types import StructType, StructField, DoubleType, StringType
schema = StructType([
    StructField("order_id", StringType()),
    StructField("amount", DoubleType()),
])
df = spark.read.schema(schema).parquet("orders")  # schema enforced at read time
```

**When to use Dataset vs DataFrame in Scala/Java:**
- DataFrame: ETL pipelines — Catalyst optimizes all operations
- Dataset: typed transformations where compile-time safety outweighs runtime overhead
- Performance: Dataset[T] with typed lambdas bypasses Catalyst for those operations → slower than equivalent DataFrame

---

## RDD Partition Pruning and Predicate Pushdown

DataFrames push predicates to the data source; RDDs support manual partition skipping:

```python
# HadoopRDD: partition = HDFS block (pruning via input splits)
# Partitioned Parquet: Spark reads partition metadata before scanning

df = spark.read.parquet("s3://data/orders/year=2024/month=01/")
# Spark reads only year=2024/month=01 partitions

# Partition pruning in SQL/DataFrame:
df = spark.read.parquet("s3://data/orders/")
df.filter("year = 2024 AND month = 1")  # pushed to directory listing
# Spark skips all other year/month directories
```

```python
# Manual partition function for RDD:
class HashPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    def __call__(self, key):
        return hash(key) % self.num_partitions

# Pre-partition data by key to avoid shuffles in joins:
rdd1 = rdd.partitionBy(10, HashPartitioner(10)).cache()
rdd2 = other_rdd.partitionBy(10, HashPartitioner(10)).cache()
# Now join doesn't shuffle — both already co-partitioned:
joined = rdd1.join(rdd2)   # no shuffle!
```

---

## Memory Pressure and GC Tuning

```python
# Signs of GC pressure in Spark UI:
# - Executor GC time > 5% of task time
# - Task duration variance (some tasks 10× slower)
# - SparkOutOfMemoryError or executor OOM killed

# GC tuning options:
spark.conf.set("spark.executor.extraJavaOptions",
    " ".join([
        "-XX:+UseG1GC",
        "-XX:InitiatingHeapOccupancyPercent=35",  # start GC earlier, smaller pauses
        "-XX:G1HeapRegionSize=16m",               # tune for larger Spark objects
        "-XX:MaxGCPauseMillis=500",               # target GC pause ceiling
        "-verbose:gc",                            # log GC events
        "-XX:+PrintGCDetails",
    ]))

# Or use ZGC (Java 15+) for sub-millisecond pauses:
"-XX:+UseZGC"

# Monitor GC in Spark UI:
# Executors tab → "GC Time" column
```

---

## Interview Tips

> **Tip 1:** "How does whole-stage code generation improve Spark's performance?" — Traditional Volcano model uses virtual function calls between operators per row — each function call involves a CPU branch misprediction, cache miss, and JIT de-optimization. WSCG generates a single tight Java loop for an entire stage, eliminating virtual calls, enabling JIT to optimize across operators, and allowing SIMD vectorization. For CPU-bound pipelines (filters, projections, aggregations), this gives 2-10× speedup.

> **Tip 2:** "What is Tungsten and how does it relate to memory management?" — Tungsten is Spark's execution engine that manages memory at the byte level using UnsafeRow — a compact binary format that avoids JVM object overhead (no 16-byte headers, no pointer chasing). It can use off-heap memory (outside JVM GC control), eliminating GC pauses for large datasets. Tungsten also includes a cache-efficient sort (radix sort on binary keys) and a vectorized hash map for aggregations.

> **Tip 3:** "Why is Dataset[T] sometimes slower than DataFrame in Scala?" — When you use typed lambdas on a Dataset (`.filter(row => row.amount > 100)`), Spark must deserialize the binary UnsafeRow into a JVM object (`Order` case class) to call your lambda. This deserialization overhead can negate Catalyst's gains. DataFrames keep data in binary UnsafeRow throughout and Catalyst optimizes operations without deserialization. Use DataFrame/SQL for performance-critical paths; use Dataset where type safety is worth the cost.
