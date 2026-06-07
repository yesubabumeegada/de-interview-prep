---
title: "PySpark RDD Operations - Fundamentals"
topic: pyspark
subtopic: rdd-operations
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, rdd, transformations, actions, map, filter, reduce, spark]
---

# PySpark RDD Operations — Fundamentals

## What Is an RDD?

**RDD** = **Resilient Distributed Dataset** — the foundational data abstraction in Apache Spark. An RDD is an immutable, partitioned collection of elements that can be operated on in parallel across a cluster.

> **Key Insight:** While DataFrames are the preferred API today, RDDs are still the backbone of Spark. Every DataFrame operation compiles down to RDD operations. Understanding RDDs helps you debug, optimize, and handle unstructured data that doesn't fit neatly into a schema.

---

## Core Properties of RDDs

| Property | Meaning |
|----------|---------|
| **Resilient** | Fault-tolerant via lineage — can recompute lost partitions |
| **Distributed** | Data is split across cluster nodes |
| **Immutable** | Once created, an RDD cannot be modified — only transformed into a new RDD |
| **Lazy** | Transformations are not executed until an action triggers computation |
| **Partitioned** | Data is divided into logical chunks for parallel processing |

---

## Creating RDDs

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "RDD Basics")

# From a Python collection (parallelize)
numbers_rdd = sc.parallelize([1, 2, 3, 4, 5], numPartitions=3)

# From a text file
logs_rdd = sc.textFile("hdfs:///data/server_logs/*.txt")

# From another RDD (via transformation)
even_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)
```

---

## Transformations vs Actions

This is the most critical distinction in Spark:

| Concept | What It Does | When It Runs | Returns |
|---------|-------------|-------------|---------|
| **Transformation** | Defines a new RDD from an existing one | Lazy — only when an action triggers | A new RDD |
| **Action** | Computes a result from an RDD | Immediately — triggers the DAG | A value to the driver or storage |

### Common Transformations (Lazy)

```python
# map — apply function to each element
squared = numbers_rdd.map(lambda x: x ** 2)
# [1, 4, 9, 16, 25] (not computed yet!)

# filter — keep elements matching a condition
evens = numbers_rdd.filter(lambda x: x % 2 == 0)
# [2, 4]

# flatMap — map + flatten (one-to-many)
words = sc.parallelize(["hello world", "foo bar"])
split_words = words.flatMap(lambda line: line.split(" "))
# ["hello", "world", "foo", "bar"]

# mapPartitions — apply function to each partition (more efficient)
def process_partition(iterator):
    # Open a DB connection once per partition, not per row
    conn = get_db_connection()
    for record in iterator:
        yield transform(record, conn)
    conn.close()

result = rdd.mapPartitions(process_partition)

# distinct — remove duplicates
unique = numbers_rdd.distinct()

# union — combine two RDDs
combined = rdd_a.union(rdd_b)
```

### Common Actions (Trigger Execution)

```python
# collect — bring all data to driver (DANGER: can OOM on large data)
all_data = squared.collect()  # [1, 4, 9, 16, 25]

# count — number of elements
total = numbers_rdd.count()  # 5

# take — first N elements (safe alternative to collect)
sample = numbers_rdd.take(3)  # [1, 2, 3]

# reduce — aggregate all elements
total_sum = numbers_rdd.reduce(lambda a, b: a + b)  # 15

# first — first element
first = numbers_rdd.first()  # 1

# saveAsTextFile — write to storage (triggers full computation)
squared.saveAsTextFile("hdfs:///output/squared")

# foreach — apply function for side effects (no return)
numbers_rdd.foreach(lambda x: print(x))
```

---

## Word Count — The Classic Example

```python
# Read a text file, count word occurrences
text_rdd = sc.textFile("hdfs:///data/books/*.txt")

word_counts = (
    text_rdd
    .flatMap(lambda line: line.lower().split())       # Split into words
    .map(lambda word: (word, 1))                      # Pair each word with count 1
    .reduceByKey(lambda a, b: a + b)                  # Sum counts per word
    .sortBy(lambda pair: pair[1], ascending=False)    # Sort by count descending
)

# Action — triggers the full pipeline
top_10 = word_counts.take(10)
```

**Execution flow:**
1. `textFile` → reads partitions lazily
2. `flatMap` → transformation (lazy)
3. `map` → transformation (lazy)
4. `reduceByKey` → transformation with shuffle (lazy)
5. `sortBy` → transformation with shuffle (lazy)
6. `take(10)` → **ACTION** — Spark builds the DAG and executes

---

## Key-Value RDD Operations (Pair RDDs)

When RDD elements are tuples of `(key, value)`, special operations become available:

```python
sales = sc.parallelize([
    ("electronics", 500),
    ("clothing", 200),
    ("electronics", 300),
    ("clothing", 150),
    ("electronics", 700),
])

# reduceByKey — aggregate by key (preferred over groupByKey)
totals = sales.reduceByKey(lambda a, b: a + b)
# [("electronics", 1500), ("clothing", 350)]

# groupByKey — group all values by key (shuffles ALL data — avoid when possible)
grouped = sales.groupByKey()  # [("electronics", [500, 300, 700]), ...]

# sortByKey
sorted_sales = sales.sortByKey()

# join — inner join on key
inventory = sc.parallelize([("electronics", 100), ("clothing", 50)])
joined = sales.join(inventory)
# [("electronics", (500, 100)), ("electronics", (300, 100)), ...]

# countByKey — count elements per key (action)
counts = sales.countByKey()  # {"electronics": 3, "clothing": 2}
```

> **Critical:** Always prefer `reduceByKey` over `groupByKey`. `reduceByKey` combines locally before shuffling (like a combiner in MapReduce), while `groupByKey` shuffles ALL values across the network first.

---

## Narrow vs Wide Transformations

| Type | Shuffle? | Examples | Performance |
|------|----------|----------|-------------|
| **Narrow** | No | `map`, `filter`, `flatMap`, `union` | Fast — no network I/O |
| **Wide** | Yes | `reduceByKey`, `groupByKey`, `join`, `distinct`, `repartition` | Expensive — data moves across nodes |

Wide transformations create **stage boundaries** in Spark's execution plan. Each shuffle writes intermediate data to disk and reads it back on other nodes.

---

## When to Use RDDs vs DataFrames

| Use RDDs When... | Use DataFrames When... |
|-----------------|----------------------|
| Working with unstructured data (text, binary) | Data has a schema (rows and columns) |
| You need low-level control over partitioning | SQL-like operations (filter, aggregate, join) |
| Custom partitioning logic is required | You want Catalyst optimizer benefits |
| Processing complex objects that don't fit in rows | Performance matters (Tungsten, whole-stage codegen) |
| Existing RDD-based library code | Most typical ETL/analytics workloads |

> **Rule of thumb:** Start with DataFrames. Drop to RDDs only when you need fine-grained control or unstructured data handling.

---

## Persistence and Caching

```python
from pyspark import StorageLevel

# Cache in memory (shortcut for persist(MEMORY_ONLY))
expensive_rdd = raw_rdd.map(heavy_transform).filter(quality_check)
expensive_rdd.cache()

# Persist with custom storage level
expensive_rdd.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
expensive_rdd.unpersist()
```

| Storage Level | Where | Use When |
|--------------|-------|----------|
| `MEMORY_ONLY` | RAM only (drops partitions if no room) | RDD fits in memory, used many times |
| `MEMORY_AND_DISK` | RAM first, spill to disk | RDD is too large for RAM alone |
| `DISK_ONLY` | Disk only | Very large RDD, recomputation is very expensive |
| `MEMORY_ONLY_SER` | RAM as serialized bytes | Reduce GC pressure at cost of CPU |

---

## Lineage and Fault Tolerance

RDDs track their **lineage** — the sequence of transformations from the original data source. If a partition is lost (node failure), Spark recomputes just that partition from the lineage.

```python
# View the lineage (DAG of transformations)
print(word_counts.toDebugString())
```

Output:
```
(2) ShuffledRDD[4] at reduceByKey
 +-(2) MapPartitionsRDD[3] at map
    |  MapPartitionsRDD[2] at flatMap
    |  hdfs:///data/books/*.txt MapPartitionsRDD[1] at textFile
```

---

## Interview Tips

> **Tip 1:** "Explain transformations vs actions." — "Transformations are lazy operations that define a new RDD without executing anything — like map, filter, and reduceByKey. Actions trigger actual computation and return results to the driver or write to storage — like collect, count, and saveAsTextFile. This lazy evaluation lets Spark optimize the entire pipeline before executing it."

> **Tip 2:** "Why is reduceByKey better than groupByKey?" — "reduceByKey applies the reduce function locally on each partition before shuffling, sending much less data across the network. groupByKey shuffles ALL values for each key to a single node first, which can cause OOM errors and massive network traffic. It's the same reason a combiner improves MapReduce performance."

> **Tip 3:** "When would you use an RDD instead of a DataFrame?" — "RDDs for unstructured data like raw text or binary files, when I need custom partitioning logic, or when working with complex Python objects that don't map to columnar format. But for 95% of structured data work, DataFrames are better because the Catalyst optimizer and Tungsten engine make them significantly faster than hand-written RDD code."
