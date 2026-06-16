---
title: "Spark Core Concepts Interview Scenarios"
description: "Scenarios covering RDDs, transformations, actions, lazy evaluation, and lineage"
content_type: scenario_question
topic: spark
subtopic: core-concepts
tags: [spark, rdd, dataframe, dataset, transformations, actions, lazy-evaluation]
---

<article data-difficulty="junior">

## Scenario: `map()` vs `flatMap()` — When to Use Each?

A recruiter asks you: "Can you explain the difference between `map()` and `flatMap()` in Spark? Give me a concrete example of when you'd use each."

<details>
<summary>✅ Solution</summary>

### The Core Difference

| | `map()` | `flatMap()` |
|---|---|---|
| Input → Output | 1 element → 1 element | 1 element → 0 or more elements |
| Result structure | Same number of rows | Can change row count |
| Return type | Any single value | Must return an iterable |

### `map()` — One-to-One Transformation

Use `map()` when each input row produces **exactly one** output row:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Example: uppercase every word
words = sc.parallelize(["hello", "world", "spark"])
upper = words.map(lambda w: w.upper())
print(upper.collect())
# ["HELLO", "WORLD", "SPARK"]  — still 3 elements

# Example: parse CSV rows into tuples
raw = sc.parallelize(["Alice,30", "Bob,25", "Carol,35"])
parsed = raw.map(lambda line: line.split(","))
print(parsed.collect())
# [["Alice", "30"], ["Bob", "25"], ["Carol", "35"]]  — still 3 elements
```

### `flatMap()` — One-to-Many Transformation

Use `flatMap()` when each input row can produce **zero, one, or many** output rows. The results are **flattened** into a single collection:

```python
# Classic word count — split each sentence into words
sentences = sc.parallelize([
    "hello world",
    "spark is great",
    "hello spark"
])

# map() would give you lists of words (nested structure)
nested = sentences.map(lambda s: s.split(" "))
print(nested.collect())
# [["hello", "world"], ["spark", "is", "great"], ["hello", "spark"]]
# 3 elements, each is a list

# flatMap() flattens the result into individual words
words = sentences.flatMap(lambda s: s.split(" "))
print(words.collect())
# ["hello", "world", "spark", "is", "great", "hello", "spark"]
# 7 elements — input row count changed!
```

### Real-World Use Cases

**Use `map()` for:**
```python
# Transforming column values
df_rdd.map(lambda row: (row.user_id, row.revenue * 1.1))

# Parsing structured data
logs.map(lambda line: parse_log_entry(line))

# Type conversions
numbers.map(lambda x: float(x))
```

**Use `flatMap()` for:**
```python
# Exploding arrays (equivalent to SQL LATERAL VIEW EXPLODE)
# Each user has a list of tags
users = sc.parallelize([
    ("alice", ["python", "spark", "sql"]),
    ("bob", ["java", "scala"]),
    ("carol", [])   # empty list → produces 0 rows for this user
])
user_tags = users.flatMap(lambda u: [(u[0], tag) for tag in u[1]])
print(user_tags.collect())
# [("alice", "python"), ("alice", "spark"), ("alice", "sql"),
#  ("bob", "java"), ("bob", "scala")]
# carol disappears — flatMap naturally filters empty iterables!

# Tokenizing text for NLP
docs.flatMap(lambda doc: doc.lower().split())

# Generating multiple records from one input
events.flatMap(lambda e: generate_audit_records(e))
```

### In DataFrame API

```python
from pyspark.sql.functions import explode, split, col

# flatMap equivalent in DataFrame API
df = spark.createDataFrame([
    ("alice", "python,spark,sql"),
    ("bob", "java,scala")
], ["name", "skills"])

df.withColumn("skill", explode(split(col("skills"), ","))) \
  .select("name", "skill") \
  .show()
# +-----+------+
# | name| skill|
# +-----+------+
# |alice|python|
# |alice| spark|
# |alice|   sql|
# |  bob|  java|
# |  bob| scala|
```

### Memory Tip

Think of the names:
- `map`: maps each element to exactly **one** result
- `flatMap`: maps then **flattens** the resulting iterables

</details>

</article>

<article data-difficulty="mid">

## Scenario: Draw the DAG for a CSV → Filter → GroupBy → Agg → Parquet Pipeline

You have this PySpark pipeline:

```python
df = spark.read.csv("s3://bucket/sales/", header=True, inferSchema=True)
result = (df
    .filter(df.amount > 100)
    .groupBy("region")
    .agg({"amount": "sum"})
)
result.write.parquet("s3://bucket/output/")
```

Draw the DAG (stages and shuffles). How many shuffles occur and why? What determines stage boundaries?

<details>
<summary>✅ Solution</summary>

### What Creates a Stage Boundary?

Spark splits the DAG into **stages** wherever a **shuffle** is required. A shuffle happens when data must be **redistributed across partitions** — i.e., at any **wide dependency**:

- `groupBy`, `orderBy`, `distinct`, `repartition`, `join` (most types)
- `reduceByKey`, `sortByKey`, `groupByKey` (RDD API)

**Narrow dependencies** (filter, map, select, withColumn) stay within the same stage — each output partition depends on exactly one input partition.

### The DAG for This Pipeline

```
┌─────────────────────────────────────────────────────┐
│  Stage 1 (Map Stage)                                │
│                                                     │
│  FileScan CSV (S3)                                  │
│      ↓  [narrow — read partitions in parallel]      │
│  InMemoryFileIndex / PartitionFilters               │
│      ↓  [narrow — no data movement]                 │
│  Filter (amount > 100)                              │
│      ↓  [narrow — each partition filtered locally]  │
│  HashAggregate (partial)  ← partial aggregation!    │
│                                                     │
└─────────────────────────┬───────────────────────────┘
                          │
                   *** SHUFFLE ***
                   (exchange/repartition by "region")
                          │
┌─────────────────────────▼───────────────────────────┐
│  Stage 2 (Reduce Stage)                             │
│                                                     │
│  HashAggregate (final merge)                        │
│      ↓  [narrow — merge partial sums per partition] │
│  Write Parquet (S3)                                 │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### How Many Shuffles? One.

There is **exactly one shuffle** in this pipeline — caused by the `groupBy("region")`.

The read, filter, and write are all narrow operations. Spark is smart enough to **push the filter down** as close to the source as possible and perform **partial aggregation** before the shuffle (this is the two-phase HashAggregate optimization).

### Two-Phase Aggregation Explained

Without optimization, you'd send ALL rows to reducers. With two-phase:

```
Phase 1 (pre-shuffle, in Stage 1):
  Partition 0: rows for region=East → local partial sum = 4500
  Partition 1: rows for region=East → local partial sum = 3200
  Partition 2: rows for region=West → local partial sum = 7100

  ↓ shuffle: all East rows go to reducer 0, all West to reducer 1

Phase 2 (post-shuffle, in Stage 2):
  Reducer 0: merge 4500 + 3200 = 7700 (final East sum)
  Reducer 1: 7100 (final West sum)
```

This reduces shuffle data volume dramatically.

### Visualizing with EXPLAIN

```python
result.explain(mode="extended")

# Or use the Spark UI DAG visualization:
# Jobs → click job → DAG Visualization
# Stages tab → see ExchangeCoordinator for shuffle boundaries
```

Sample output (simplified):
```
== Physical Plan ==
*(2) HashAggregate(keys=[region], functions=[sum(amount)])  ← Stage 2
+- Exchange hashpartitioning(region, 200)                   ← SHUFFLE
   +- *(1) HashAggregate(keys=[region], functions=[partial_sum(amount)])  ← Stage 1
      +- *(1) Filter (amount > 100)
         +- FileScan csv [region, amount] ...
```

### How Many Output Partitions After the Shuffle?

Controlled by `spark.sql.shuffle.partitions` (default: 200).

For small data or few distinct regions, 200 partitions is excessive:

```python
spark.conf.set("spark.sql.shuffle.partitions", "8")
# or with AQE:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# AQE will automatically coalesce small shuffle partitions
```

### What If You Added a Sort?

```python
result.orderBy("total_amount")  # adds ANOTHER shuffle
```

```
Stage 1: read → filter → partial agg
  ↓ shuffle 1 (groupBy)
Stage 2: final agg → sort range partition
  ↓ shuffle 2 (orderBy — range partition for global sort)
Stage 3: local sort within each partition → write
```

Now you have **2 shuffles** and **3 stages**.

### Rule of Thumb

Number of shuffles ≈ number of wide transformations in your logical plan. Each shuffle = one stage boundary = one new stage.

</details>

</article>

<article data-difficulty="senior">

## Scenario: To Cache or Not to Cache a 50GB DataFrame Used 3 Times

A colleague proposes this optimization: "We use this intermediate DataFrame three times in our pipeline. Let's cache it so we don't recompute it."

The DataFrame is **50GB** after transformation. The cluster has **200GB total executor memory**. The cluster also runs other concurrent Spark jobs.

Analyze the trade-offs and make a recommendation.

<details>
<summary>✅ Solution</summary>

### First: Understand What "50GB" and "200GB" Mean in Context

Raw executor JVM heap ≠ available storage memory. Spark divides executor memory into:

```
spark.executor.memory = M (e.g., 36GB)
  ├── Reserved (300MB): internal Spark metadata
  ├── spark.memory.fraction (default 0.6 × remaining):
  │     ├── Execution memory: shuffle buffers, sort, hash tables
  │     └── Storage memory: cached RDDs/DataFrames ← this is what cache uses
  └── User memory (0.4 × remaining): your UDFs, data structures
```

With 200GB total executor heap across the cluster:
- Reserved: ~1–2GB total
- `spark.memory.fraction = 0.6` → ~118GB for execution+storage
- `spark.memory.storageFraction = 0.5` → ~59GB guaranteed for storage
- But execution can evict storage cache under pressure!

So you have approximately **59GB of cache-safe storage** — barely enough for one 50GB DataFrame at `MEMORY_ONLY`.

### What Storage Levels Actually Mean

```python
from pyspark import StorageLevel

df.cache()                                # = MEMORY_AND_DISK (DataFrame default)
df.persist(StorageLevel.MEMORY_ONLY)      # evicted partitions are recomputed
df.persist(StorageLevel.MEMORY_AND_DISK)  # evicted partitions spill to disk
df.persist(StorageLevel.DISK_ONLY)        # always on disk — no memory pressure
df.persist(StorageLevel.MEMORY_ONLY_SER)  # serialize to bytes — ~2x smaller, CPU cost
```

For a **50GB DataFrame** with 59GB available:

| Storage Level | Risk | Notes |
|---|---|---|
| `MEMORY_ONLY` | HIGH | Any concurrent job can evict your cache; recomputation triggered |
| `MEMORY_AND_DISK` | MEDIUM | Graceful degradation — slower but no recomputation |
| `DISK_ONLY` | LOW | Always available; adds disk I/O on each access |
| `MEMORY_ONLY_SER` | MEDIUM-LOW | ~25–30GB serialized (Kryo); fits in memory more reliably |

### The Cost of Recomputation (Is Cache Worth It?)

Cache is only beneficial if **recomputation cost > cache overhead**. Evaluate:

```python
# What does the DataFrame's lineage look like?
df.explain()

# If the DataFrame is:
# - A simple filter + select on a fast S3 read → recomputation is cheap
# - A complex multi-join with multiple shuffles → recomputation is expensive
```

For a 50GB DataFrame that required 3 shuffle stages to produce:
- Recomputation = 3 shuffles × ~50GB data = significant I/O + CPU
- Cache overhead = 50GB storage + eviction risk
- **Cache is likely worth it** if the 3 uses happen in the same job

For a 50GB DataFrame from a simple read + filter:
- Recomputation = fast re-read from S3 (especially with columnar Parquet)
- Cache overhead = 50GB pressuring all other running jobs
- **Cache may not be worth it**

### How Many Times Is "Worth It"?

General rule: cache when the DataFrame is used **3+ times AND recomputation is expensive**.

But factor in concurrency:
```python
# If other jobs are running on the same cluster:
# - 200GB cluster memory is shared
# - Your 50GB cache can be evicted at any moment by other executors
# - Eviction triggers partial recomputation anyway
```

### Recommendation Framework

```python
# Step 1: Measure recomputation time
import time
start = time.time()
df.count()  # forces materialization
first_run = time.time() - start

# Step 2: Cache and measure access time
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # populate cache

start = time.time()
df.count()  # from cache
cached_run = time.time() - start

print(f"Recompute: {first_run:.1f}s, Cached: {cached_run:.1f}s")
# If cached is 5x faster and you use it 3 times → cache saves 2 × (first_run - cached_run)
```

### My Concrete Recommendation for This Scenario

Given 50GB DataFrame, 200GB cluster, concurrent jobs:

```python
# Use MEMORY_AND_DISK_SER — best balance
df_intermediate = (
    raw_df
    .join(ref_df, "key")
    .groupBy("category")
    .agg(...)
)

from pyspark import StorageLevel
df_intermediate.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Use it 3 times
result1 = df_intermediate.filter(...).write.parquet(...)
result2 = df_intermediate.groupBy(...).agg(...)
result3 = df_intermediate.join(other_df, ...)

# ALWAYS unpersist when done!
df_intermediate.unpersist()
```

Rationale:
- `SER` reduces 50GB to ~25GB in memory (fits safer within the 59GB budget)
- `_AND_DISK` ensures graceful degradation if evicted
- `unpersist()` returns memory to the pool for other jobs — critical in shared clusters

### Alternative: Checkpoint Instead of Cache

If the lineage is very long (100+ transformations), consider checkpointing:

```python
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")
df_intermediate.checkpoint()  # materializes to disk + truncates lineage
# No eviction risk — always on reliable storage
# But: must write to reliable storage (S3/HDFS), higher latency than cache
```

### Summary Decision Matrix

| Scenario | Recommendation |
|---|---|
| Used 2 times, fast recompute | Don't cache |
| Used 3+ times, expensive recompute, dedicated cluster | `MEMORY_AND_DISK` |
| Used 3+ times, expensive recompute, shared cluster | `MEMORY_AND_DISK_SER` |
| Very long lineage (100+ steps) | `checkpoint()` |
| DataFrame > 80% of available storage | `DISK_ONLY` or rewrite pipeline |

</details>

</article>
