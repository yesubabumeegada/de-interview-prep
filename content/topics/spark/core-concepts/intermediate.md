---
title: "Spark Core Concepts — Intermediate"
topic: spark
subtopic: core-concepts
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, rdd, persistence, broadcast, accumulators, shared-variables, checkpointing, dataframe]
---

# Spark Core Concepts — Intermediate

## Persistence and Caching

Recomputing an RDD from scratch on every action is expensive. Caching stores the result in executor memory (or disk) for reuse:

```python
from pyspark import StorageLevel

rdd = sc.textFile("hdfs:///data/large.log").map(parse_line).filter(is_valid)

# Default cache: MEMORY_ONLY — fastest, lost if evicted
rdd.cache()   # shorthand for persist(StorageLevel.MEMORY_ONLY)

# Explicit storage levels:
rdd.persist(StorageLevel.MEMORY_ONLY)           # fastest; recomputes if evicted
rdd.persist(StorageLevel.MEMORY_AND_DISK)       # spills to disk if evicted
rdd.persist(StorageLevel.MEMORY_ONLY_SER)       # serialized — less memory, slower access
rdd.persist(StorageLevel.DISK_ONLY)             # always disk — avoid if possible
rdd.persist(StorageLevel.MEMORY_AND_DISK_2)     # replicate to 2 nodes — fault tolerant

# Always unpersist when done!
rdd.unpersist()
```

**When to cache:**
```python
# Cache when the same RDD/DataFrame is used in multiple actions:
filtered_df = large_df.filter("status = 'active'").cache()

count = filtered_df.count()          # action 1 — fills cache
avg = filtered_df.agg({"revenue": "avg"}).collect()  # action 2 — hits cache
filtered_df.write.parquet("output/")  # action 3 — hits cache

filtered_df.unpersist()  # free memory

# Don't cache one-time use — cache adds overhead
```

---

## Broadcast Variables

Broadcast variables efficiently distribute read-only data to all executors (sent once, cached locally):

```python
from pyspark.sql import functions as F

# Without broadcast (problem): each task fetches from driver
mapping = {"US": "North America", "DE": "Europe", "JP": "Asia"}
# In a UDF, this dict is serialized into EVERY task!
rdd.map(lambda row: mapping[row.country])  # mapping sent to 1000s of tasks

# With broadcast: sent once per executor
broadcast_mapping = sc.broadcast(mapping)
def map_country(row):
    return broadcast_mapping.value[row.country]  # reads local copy
rdd.map(map_country)

# Cleanup when done
broadcast_mapping.unpersist()
broadcast_mapping.destroy()  # removes from both driver and executors
```

```python
# Broadcast joins (DataFrame API):
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "key")
# Forces broadcast even if small_df is above autoBroadcastJoinThreshold
```

---

## Accumulators

Accumulators are distributed counters — tasks can add to them, only the driver reads:

```python
# Built-in numeric accumulator
error_count = sc.accumulator(0)
skip_count = sc.accumulator(0)

def process(record):
    global error_count, skip_count
    try:
        return parse(record)
    except ValueError:
        error_count += 1
        return None
    except SkipRecord:
        skip_count += 1
        return None

results = rdd.map(process).filter(lambda x: x is not None)
results.count()   # trigger execution

print(f"Errors: {error_count.value}")   # read on driver only
print(f"Skipped: {skip_count.value}")
```

**Important caveats:**
```python
# 1. Accumulators are only guaranteed correct inside actions, not transformations
#    Transformations may be re-executed on retry — double-counting!
rdd.map(lambda x: counter.add(1) or x)  # BAD: may count twice on retry

# 2. Accumulators in failed tasks ARE NOT counted
# 3. Named accumulators appear in Spark UI:
named_acc = sc.accumulator(0, name="records_processed")
```

---

## mapPartitions vs. map

`mapPartitions` operates on an entire partition at once — critical for expensive per-partition setup:

```python
# map: setup cost per row (bad for DB connections, model loads)
def predict_row(row):
    model = load_model()   # expensive! called once per row
    return model.predict(row)
rdd.map(predict_row)   # N rows = N model loads

# mapPartitions: setup cost once per partition
def predict_partition(rows):
    model = load_model()   # called once per partition
    for row in rows:
        yield model.predict(row)
rdd.mapPartitions(predict_partition)   # P partitions = P model loads
```

```python
# mapPartitionsWithIndex: know which partition you're in
def with_partition_id(idx, rows):
    for row in rows:
        yield (idx, row)

rdd.mapPartitionsWithIndex(with_partition_id).take(5)
```

---

## DataFrame API vs. RDD API

DataFrames are the modern API (Spark 1.3+). They use Catalyst for optimization:

| | RDD API | DataFrame API |
|--|---------|--------------|
| **Language** | Python/Scala/Java objects | Schema + columnar |
| **Optimization** | None — runs as-is | Catalyst + Tungsten |
| **Type safety** | Yes (generic objects) | Schema-enforced |
| **Performance** | Slower (Python objects, no opt) | 5-10× faster |
| **When to use** | Unstructured data, complex logic | Structured data (99% of cases) |

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

# DataFrame is the right choice for structured data:
df = spark.read.parquet("orders.parquet")
result = (df
    .filter(F.col("amount") > 100)
    .groupBy("region")
    .agg(
        F.sum("amount").alias("total"),
        F.count("*").alias("orders"),
        F.avg("amount").alias("avg_order")
    )
    .orderBy(F.desc("total"))
)
result.show()
```

---

## Schema Operations

```python
from pyspark.sql.types import *

# Define schema explicitly (preferred for production)
schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_date", DateType(), nullable=True),
])

df = spark.read.schema(schema).json("orders.json")

# Inspect schema
df.printSchema()
df.dtypes  # list of (name, type) tuples

# Add/modify columns
df = df.withColumn("amount_usd", F.col("amount") * 1.1)
df = df.withColumn("year", F.year(F.col("order_date")))

# Cast types
df = df.withColumn("amount", F.col("amount").cast(DecimalType(15, 2)))

# Rename
df = df.withColumnRenamed("order_date", "created_at")

# Drop
df = df.drop("temp_col")
```

---

## Checkpointing

Checkpointing saves RDD/DataFrame to HDFS, cutting the lineage graph. Important for:
1. Very long lineage chains (iterative algorithms, streaming)
2. Recovering from expensive recomputations

```python
# Set checkpoint directory
sc.setCheckpointDir("hdfs:///checkpoints/app1/")

rdd = sc.parallelize(range(1000))
for i in range(100):  # iterative algorithm
    rdd = rdd.map(complex_transform)
    if i % 10 == 0:
        rdd.checkpoint()   # truncate lineage every 10 iterations
        rdd.count()        # materialize checkpoint (must call action first!)

# DataFrame checkpointing
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/")
df = df.checkpoint()   # eager=True by default
```

---

## Interview Tips

> **Tip 1:** "When should you cache a DataFrame?" — Cache when you call multiple actions on the same DataFrame and the computation is expensive to repeat. Rule: if recomputation cost > memory cost, cache it. Don't cache if: used only once, data is very large relative to executor memory, or it's a simple filter/select that reads from a fast source. Always unpersist when done to free executor memory.

> **Tip 2:** "What's the difference between broadcast variables and normal variables in closures?" — Normal closure variables are serialized into every task — if it's a 100 MB dictionary, 1000 tasks each serialize 100 MB = 100 GB of network traffic. Broadcast variables send the data once per executor (not per task), cache it locally, and tasks read from the local cache. For any large lookup table used in a UDF, broadcast is essential.

> **Tip 3:** "When would you use mapPartitions over map?" — When the operation has significant per-partition setup cost: loading an ML model, opening a database connection, reading a config file. `map` pays that cost once per row. `mapPartitions` pays it once per partition (typically 200-2000 rows), a 100-1000× reduction. The tradeoff: partition data stays in memory until the iterator is exhausted, so it uses more memory than row-at-a-time map.
