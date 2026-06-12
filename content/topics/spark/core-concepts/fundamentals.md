---
title: "Spark Core Concepts — Fundamentals"
topic: spark
subtopic: core-concepts
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, rdd, transformations, actions, lazy-evaluation, lineage, partitions, dag]
---

# Spark Core Concepts — Fundamentals

## 🎯 Analogy

Think of an RDD like a recipe for making a dish. The recipe (lineage) is stored, but no cooking happens until you actually order the dish (call an action). You can chain dozens of recipe modifications (transformations), and none of them run until someone actually orders.

---

## RDD: The Foundation

An **RDD (Resilient Distributed Dataset)** is Spark's fundamental data abstraction. Three key properties:

- **Resilient:** if a partition is lost, Spark re-computes it using the lineage
- **Distributed:** partitions live on different executors across the cluster
- **Dataset:** a collection of records (any Python/Scala/Java object)

```python
from pyspark.sql import SparkSession
sc = SparkSession.builder.master("local[*]").appName("core").getOrCreate().sparkContext

# Create RDD from collection
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=3)  # 3 partitions

# Create from file
rdd = sc.textFile("hdfs:///data/logs.txt")  # one partition per HDFS block (128MB)

# RDD metadata
print(rdd.getNumPartitions())  # 3
print(rdd.id())                # RDD id in lineage graph
```

---

## Transformations vs. Actions

This is the most fundamental concept in Spark:

| | Transformations | Actions |
|--|-----------------|---------|
| **What they do** | Describe operations (build plan) | Trigger execution |
| **Return type** | RDD / DataFrame (lazy) | Result value or side effect |
| **When they run** | Never immediately | Immediately |
| **Examples** | `map`, `filter`, `groupBy`, `join` | `count`, `collect`, `show`, `write` |

```python
# These do NOTHING yet — just build a plan:
rdd2 = rdd.map(lambda x: x * 2)
rdd3 = rdd2.filter(lambda x: x > 4)

# This TRIGGERS execution — plan is submitted to cluster:
result = rdd3.collect()   # returns [6, 8, 10]
```

---

## Lazy Evaluation

Spark evaluates lazily — transformations accumulate into a logical plan, executed only on action:

```mermaid
graph LR
    A[parallelize] -->|map × 2| B[filter > 4] -->|collect| C[Result]
    style A fill:#4f46e5,color:#fff
    style B fill:#4f46e5,color:#fff
    style C fill:#22c55e,color:#fff
```

**Why lazy evaluation?**
1. **Optimization:** Spark sees the full plan before executing — can reorder, push down, combine steps
2. **Efficiency:** only computes what's needed; if you filter before writing, Spark never processes filtered-out rows
3. **Fault tolerance:** can replay from source if a partition is lost

```python
# Prove laziness: no computation happens here
big_rdd = sc.textFile("hdfs:///data/500gb_file.txt")   # 0ms
filtered = big_rdd.filter(lambda line: "ERROR" in line) # 0ms
mapped = filtered.map(lambda line: line.split("\t"))     # 0ms
# Only now does Spark touch the data:
errors = mapped.take(10)   # reads just enough to get 10 results
```

---

## Narrow vs. Wide Transformations

| Narrow (no shuffle) | Wide (shuffle required) |
|----|-----|
| Each output partition depends on one input partition | Each output partition depends on **multiple** input partitions |
| `map`, `filter`, `flatMap`, `mapPartitions` | `groupByKey`, `reduceByKey`, `join`, `sortBy`, `repartition` |
| Can be pipelined in one stage | Stage boundary — data written to disk, reshuffled |

```python
# Narrow — same stage, no shuffle:
rdd.map(lambda x: x + 1).filter(lambda x: x > 0)

# Wide — new stage created, shuffle happens:
rdd.groupBy(lambda x: x % 5)   # 5 groups → data moves across network
```

---

## Key Transformations Reference

```python
rdd = sc.parallelize(range(10))

# map: one input → one output
rdd.map(lambda x: x ** 2).collect()  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

# filter: keep elements matching predicate
rdd.filter(lambda x: x % 2 == 0).collect()  # [0, 2, 4, 6, 8]

# flatMap: one input → zero or more outputs
sc.parallelize(["hello world", "spark rocks"]) \
    .flatMap(lambda s: s.split(" ")) \
    .collect()  # ["hello", "world", "spark", "rocks"]

# reduceByKey: combine values for same key
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
pairs.reduceByKey(lambda x, y: x + y).collect()  # [("a", 4), ("b", 2)]

# groupByKey: group all values for same key (use reduceByKey when possible!)
pairs.groupByKey().mapValues(list).collect()  # [("a", [1, 3]), ("b", [2])]

# sortBy: sort across partitions
rdd.sortBy(lambda x: -x).take(3)  # [9, 8, 7]

# distinct: remove duplicates (shuffle!)
sc.parallelize([1, 2, 2, 3, 3]).distinct().collect()  # [1, 2, 3]
```

---

## Key Actions Reference

```python
rdd = sc.parallelize(range(100))

rdd.count()              # 100  — count of elements
rdd.first()              # 0    — first element
rdd.take(5)              # [0, 1, 2, 3, 4]
rdd.collect()            # [0, 1, ..., 99]  ← pulls ALL data to Driver!
rdd.top(3)               # [99, 98, 97]  — top N (sorted desc)
rdd.sum()                # 4950
rdd.min()                # 0
rdd.max()                # 99
rdd.mean()               # 49.5

# foreach: run a function on each element (side effects only, no return)
rdd.foreach(lambda x: print(x))  # prints on executors, not driver!

# saveAsTextFile: write to filesystem
rdd.saveAsTextFile("hdfs:///output/numbers/")
```

---

## Partitions and Parallelism

```python
# Check partition count
rdd.getNumPartitions()   # e.g. 8

# Repartition: increase or decrease (shuffle)
rdd.repartition(16)      # increase → more parallelism
rdd.repartition(4)       # decrease → fewer, larger partitions

# Coalesce: decrease ONLY, avoids full shuffle
rdd.coalesce(4)          # merges partitions locally — much faster than repartition(4)

# Rule of thumb: 2-4 partitions per executor core
# For 40 cores total → 80-160 partitions
```

---

## RDD Lineage (DAG)

```python
# View the lineage graph
rdd1 = sc.textFile("data.txt")
rdd2 = rdd1.map(str.lower)
rdd3 = rdd2.filter(lambda s: "error" in s)
rdd4 = rdd3.map(lambda s: (s.split()[0], 1))
rdd5 = rdd4.reduceByKey(lambda a, b: a + b)

# Print lineage
print(rdd5.toDebugString().decode())
```

Output:
```
(2) PythonRDD[5] at ...
 |  MapPartitionsRDD[4] ...
 |  ShuffledRDD[3] ...      ← shuffle boundary (stage break)
 |  MapPartitionsRDD[2] ...
 |  FilteredRDD[1] ...
 |  MappedRDD[0] ...
 |  data.txt HadoopRDD
```

---

## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
sc = SparkSession.builder.master("local[*]").appName("core").getOrCreate().sparkContext

words = sc.parallelize(
    ["spark is fast", "spark is easy", "spark is powerful", "python is easy"]
)

word_counts = (
    words
    .flatMap(lambda s: s.split())        # narrow
    .map(lambda w: (w, 1))               # narrow
    .reduceByKey(lambda a, b: a + b)     # wide (shuffle!)
    .sortBy(lambda kv: -kv[1])           # wide (shuffle!)
)

word_counts.take(5)
# [('spark', 3), ('is', 4), ('easy', 2), ('fast', 1), ('powerful', 1)]
```

> **Run it:** Works with `local[*]` — no cluster needed.

---

## Interview Tips

> **Tip 1:** "What is lazy evaluation in Spark and why does it matter?" — Transformations build a logical plan but don't execute. Only actions trigger execution. This lets Spark optimize the full plan (predicate pushdown, column pruning, stage fusion) before running anything. It also means a bug in a transformation only surfaces when you hit an action — a common gotcha for beginners.

> **Tip 2:** "What's the difference between narrow and wide transformations?" — Narrow: each output partition depends on one input partition — no data movement, can be pipelined in one stage (map, filter, flatMap). Wide: output partitions depend on multiple input partitions — requires a shuffle, creates stage boundaries (groupBy, join, sort). Shuffles write data to disk and transfer over the network — the dominant cost in most Spark jobs.

> **Tip 3:** "`groupByKey` vs `reduceByKey` — which should you prefer?" — Always `reduceByKey` when possible. `reduceByKey` pre-aggregates on the map side before shuffling — dramatically less data moved. `groupByKey` shuffles ALL values for each key to one executor, then reduces — can cause OOM for high-cardinality keys. The result is the same but `reduceByKey` can be 10-100× faster.
