---
title: "Spark Internals — Senior Deep Dive"
topic: spark
subtopic: spark-internals
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, catalyst, encoder, expression-trees, unsafe-row, sort-based-shuffle, tungsten-sort]
---

# Spark Internals — Senior Deep Dive

## Expression Trees and Evaluation

Every operation in Spark is represented as an expression tree:

```python
# This expression:
F.col("amount") * 1.1 + F.col("tax")

# Is an expression tree:
#   Add
#   ├── Multiply
#   │   ├── AttributeReference("amount", DoubleType)
#   │   └── Literal(1.1, DoubleType)
#   └── AttributeReference("tax", DoubleType)

# Expression evaluation modes:
# 1. Interpreted: tree walked node by node (slow, safe)
# 2. Code-generated: Catalyst generates Java: (amount * 1.1) + tax

# Check if expression is code-generated:
# In the physical plan: if inside *(N), it's code-generated
# Otherwise: interpreted (look for no-* prefix operators)
```

---

## Encoders: The Bridge Between JVM Objects and UnsafeRow

Encoders define the serialization/deserialization between Spark's binary format (UnsafeRow) and JVM objects:

```python
# Row → UnsafeRow: when writing to shuffle or cache
# UnsafeRow → Row: when executing Python UDFs or .collect()

# Types of encoders:
# ExpressionEncoder[Product] — for case classes (Scala) or structured data
# RowEncoder — for untyped DataFrames
# PythonPickleEncoder — for Python objects (slow!)

# Python UDF performance issue:
@udf(DoubleType())
def my_udf(x):
    return x * 1.1
# Each row:
# 1. UnsafeRow → pickle bytes (Spark → Python)
# 2. Python deserializes
# 3. Python executes my_udf
# 4. Python pickles result
# 5. JVM deserializes → UnsafeRow

# This is why Python UDFs are slow: 2 pickle round-trips per row

# Pandas UDF avoids per-row overhead:
@pandas_udf(DoubleType())
def fast_udf(col: pd.Series) -> pd.Series:
    return col * 1.1
# Entire column batch transferred via Apache Arrow (zero-copy for numeric types)
```

---

## Sort-Based Shuffle: How Shuffle Files Are Written

Spark's default shuffle is sort-based (`SortShuffleManager`):

```
Map task (writer side):
  1. Process input partition
  2. For each output row: hash the partition key → target reducer ID
  3. Sort all output rows by (reducer_id, sort_key)
  4. Write one sorted file per map task (no per-reducer files!)
     File structure: [reducer0_data | reducer1_data | ...] with index file

Reduce task (reader side):
  1. Read the index file from each map task
  2. Fetch only the byte range for this reducer's data from each map file
  3. Merge sorted streams

Benefits:
  - Only 1 file per map task (not 1 per reducer) → less disk I/O
  - Sequential disk writes (sorted) → SSD/HDD efficient
  - Partial merge during write if `spark.shuffle.sort.bypassMergeThreshold` met
```

```python
# Tune shuffle:
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")  # skip sort if fewer partitions
spark.conf.set("spark.shuffle.file.buffer", "32k")  # write buffer per task
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")  # read buffer per reducer
spark.conf.set("spark.shuffle.io.preferDirectBufs", "true")  # off-heap IO buffers
```

---

## Unsafe Sort: Tungsten's Replacement for TimSort

Tungsten's sort operates on compact binary keys rather than JVM objects:

```
TimSort (old Java object sort):
  - Compare via compareTo(): virtual method call, deserialize objects
  - Move object references: pointer array
  - CPU cache unfriendly: pointer chasing to actual object data

UnsafeSorter (Tungsten):
  - Extract fixed-width sort key prefix into compact array
  - Sort the prefix array (no object access for most comparisons)
  - Only access actual record when prefix is tied
  - Cache-friendly: sort array is contiguous memory

Example: sorting (Long, String) by Long
  Prefix = Long value (8 bytes)
  99% of comparisons done on Long prefix alone
  String compared only on Long tie (rare)
```

```python
# Tungsten sort bypasses Java's heap allocator entirely:
# Uses a LongArray backed by off-heap or on-heap (but direct allocation)
# No GC for sort structures
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")  # sort structures go here
```

---

## Vectorized Columnar Processing

Spark 3.x processes data in columnar batches using ColumnarBatch and ColumnVector:

```python
# Columnar batch: a batch of N rows in column-major layout
# ColumnVector: typed array for each column (IntColumnVector, DoubleColumnVector, etc.)

# Benefits:
# 1. SIMD: process 4/8/16 values in one CPU instruction
# 2. Cache locality: scan one column sequentially (not row by row)
# 3. Null bitmaps: test N nulls in one bit operation

# In practice: Parquet vectorized reader → ColumnarBatch
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")

# Columnar operators (experimental in 3.x, production in 4.x):
spark.conf.set("spark.sql.columnVector.offheap.enabled", "true")
# Stores ColumnVectors off-heap → less GC for columnar scans
```

---

## Catalyst Extension Points Summary

```python
# 1. Custom function (SQL UDF — interpreted, not codegen-compiled)
spark.udf.register("double_amount", lambda x: x * 2, DoubleType())

# 2. Custom function (built-in expression — fully codegen-compiled)
# Requires extending Expression (Scala only)

# 3. Custom optimizer rule (logical plan transformation)
# inject via withExtensions at session creation

# 4. Custom planner strategy (logical → physical plan conversion)
# inject via withExtensions

# 5. Custom Data Source V2 (full connector: read + write + stats)
# Implement TableProvider, ScanBuilder, WriteBuilder interfaces

# 6. Custom metric (add custom SparkListener):
class MyListener(SparkListener):
    def onTaskEnd(self, task_end):
        duration = task_end.taskInfo().duration()
        custom_metrics.record(duration)

spark.sparkContext.addSparkListener(MyListener())
```

---

## Interview Tips

> **Tip 1:** "How does Tungsten's sort differ from Java's default sort?" — Java's TimSort compares JVM objects via virtual method calls and moves object references — each comparison dereferences a pointer to access the actual object (cache-unfriendly). Tungsten's UnsafeSorter extracts a fixed-width key prefix (e.g., the Long sort key) into a compact binary array and sorts that array directly — no object access for most comparisons. Only when prefixes are equal does it access the full record. The sort array is cache-contiguous, enabling CPU prefetcher efficiency and SIMD operations.

> **Tip 2:** "Why are Python UDFs slow at the JVM level?" — Python UDFs require two serialization round-trips per row: (1) Spark's UnsafeRow is serialized to pickle bytes and sent to a Python worker process via a Unix socket; (2) Python deserializes, executes the function, pickles the result; (3) Spark deserializes the result. Each row incurs socket I/O and two pickle calls. Pandas UDFs replace this with Apache Arrow's zero-copy IPC protocol for entire column batches, reducing overhead from O(rows) to O(batches).

> **Tip 3:** "How does sort-based shuffle improve on hash-based shuffle?" — Hash shuffle (Spark 1.x) created one output file per reducer per map task — with 1000 mappers × 200 reducers = 200,000 files, causing OS file descriptor exhaustion and massive metadata overhead. Sort shuffle writes one sorted file per map task, with an index file that records each reducer's byte range. Reducers do range reads from these indexed files — dramatically fewer files (1000 instead of 200,000), better disk sequentiality, and lower file descriptor pressure.
