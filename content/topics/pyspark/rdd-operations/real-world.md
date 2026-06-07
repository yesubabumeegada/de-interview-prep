---
title: "PySpark RDD Operations - Real World Patterns"
topic: pyspark
subtopic: rdd-operations
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, rdd, custom-partitioner, accumulator, graph-algorithms, migration, production]
---

# PySpark RDD Operations — Real-World Patterns

## Pattern 1: Custom Partitioner for Skewed Data

**Problem:** A retail company has event data where 70% of traffic comes from 5 popular stores. Default hash partitioning creates hot partitions that cause task stragglers.

```python
from pyspark import SparkContext, Partitioner

sc = SparkContext("local[*]", "SkewedPartitioner")

class SkewAwarePartitioner(Partitioner):
    """Routes hot keys across multiple partitions."""
    
    def __init__(self, num_partitions, hot_keys):
        self._num_partitions = num_partitions
        self._hot_keys = set(hot_keys)
        # Reserve partitions for hot keys (spread them)
        self._hot_partition_count = min(num_partitions // 2, len(hot_keys) * 4)
    
    def numPartitions(self):
        return self._num_partitions
    
    def partitionFunc(self, key):
        store_id, event_id = key
        if store_id in self._hot_keys:
            # Spread hot keys across multiple partitions using event_id
            return hash(event_id) % self._hot_partition_count
        else:
            # Cold keys get hash-partitioned normally
            return (hash(store_id) % (self._num_partitions - self._hot_partition_count)
                    + self._hot_partition_count)

# Identify hot keys from sample
events_rdd = sc.textFile("hdfs:///data/store_events/")
parsed = events_rdd.map(parse_event)  # Returns ((store_id, event_id), event_data)

# Sample to find hot keys
hot_keys = (parsed
    .map(lambda x: (x[0][0], 1))
    .reduceByKey(lambda a, b: a + b)
    .top(5, key=lambda x: x[1]))
hot_key_set = [k for k, v in hot_keys]

# Apply custom partitioner
partitioner = SkewAwarePartitioner(200, hot_key_set)
partitioned = parsed.partitionBy(partitioner.numPartitions(), partitioner.partitionFunc)

# Verify partition balance
sizes = partitioned.glom().map(len).collect()
print(f"Max partition: {max(sizes)}, Min: {min(sizes)}, Ratio: {max(sizes)/max(min(sizes),1):.1f}x")
```

**Before/After Performance:**

| Metric | Default Hash | Skew-Aware Partitioner |
|--------|-------------|----------------------|
| Max task duration | 45 min | 8 min |
| Median task duration | 3 min | 6 min |
| Total job time | 48 min | 12 min |
| Shuffle spill | 120 GB | 15 GB |

---

## Pattern 2: Accumulator-Based Metrics Collection

**Problem:** During a complex ETL pipeline, you need to track data quality metrics (null counts, parse errors, record counts by type) without adding extra passes over the data.

```python
from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam

sc = SparkContext("local[*]", "AccumulatorMetrics")

# Custom accumulator for dictionaries
class DictAccumulatorParam(AccumulatorParam):
    def zero(self, initial_value):
        return {}
    
    def addInPlace(self, acc1, acc2):
        for key, value in acc2.items():
            acc1[key] = acc1.get(key, 0) + value
        return acc1

# Register accumulators
parse_errors = sc.accumulator(0)
null_counts = sc.accumulator({}, DictAccumulatorParam())
record_type_counts = sc.accumulator({}, DictAccumulatorParam())

def process_record(raw_line):
    """Parse and validate a record, updating accumulators for metrics."""
    try:
        record = json.loads(raw_line)
    except json.JSONDecodeError:
        parse_errors.add(1)
        return None
    
    # Track nulls per field
    nulls = {field: 1 for field, value in record.items() if value is None}
    if nulls:
        null_counts.add(nulls)
    
    # Track record types
    record_type_counts.add({record.get("type", "unknown"): 1})
    
    return record

# Process pipeline
raw_rdd = sc.textFile("hdfs:///data/events/2024-01-15/")
processed = raw_rdd.map(process_record).filter(lambda x: x is not None)
processed.cache()

# Trigger computation
total_records = processed.count()

# Access metrics (only on driver after action completes)
print(f"Total valid records: {total_records}")
print(f"Parse errors: {parse_errors.value}")
print(f"Null counts by field: {null_counts.value}")
print(f"Record type distribution: {record_type_counts.value}")

# Push metrics to monitoring system
publish_metrics({
    "total_records": total_records,
    "parse_error_rate": parse_errors.value / (total_records + parse_errors.value),
    "null_fields": null_counts.value,
})
```

> **Important Caveats:**
> - Accumulators are write-only on executors, read-only on driver
> - In case of task retries, accumulators may be updated more than once
> - Only use for metrics/monitoring, never for control flow logic

---

## Pattern 3: RDD for Graph Algorithms

**Problem:** Compute connected components in a social network graph. DataFrame API doesn't support iterative graph algorithms natively.

```python
def connected_components(edges_rdd, max_iterations=20):
    """
    Find connected components using label propagation on RDDs.
    Each node adopts the smallest label among its neighbors.
    """
    # Initialize: each node's component = its own ID
    nodes = edges_rdd.flatMap(lambda e: [e[0], e[1]]).distinct()
    components = nodes.map(lambda node: (node, node))  # (node_id, component_id)
    
    # Build adjacency list
    adjacency = (
        edges_rdd
        .flatMap(lambda e: [(e[0], e[1]), (e[1], e[0])])  # Undirected
        .groupByKey()
        .mapValues(list)
    )
    adjacency.cache()
    
    for iteration in range(max_iterations):
        # Propagate minimum component label to neighbors
        proposals = (
            adjacency.join(components)
            .flatMap(lambda x: [(neighbor, x[1][1]) for neighbor in x[1][0]])
        )
        
        # Each node takes the minimum label (its own or from neighbors)
        new_components = (
            components.union(proposals)
            .reduceByKey(min)
        )
        
        # Check convergence
        changed = (
            components.join(new_components)
            .filter(lambda x: x[1][0] != x[1][1])
            .count()
        )
        
        components = new_components
        components.cache()
        
        if iteration % 5 == 0:
            components.checkpoint()
            components.count()  # Materialize checkpoint
        
        print(f"Iteration {iteration}: {changed} nodes changed")
        if changed == 0:
            break
    
    return components

# Usage
edges = sc.parallelize([
    ("A", "B"), ("B", "C"), ("D", "E"), ("F", "G"), ("G", "D")
])

components = connected_components(edges)
# Result: A,B,C → component "A"; D,E,F,G → component "D"
```

---

## Pattern 4: Legacy RDD Code Migration to DataFrame

**Problem:** Migrate an existing RDD-based ETL pipeline to DataFrames for performance gains while maintaining correctness.

```python
# BEFORE: Legacy RDD pipeline (slow, no optimization)
def legacy_etl(sc):
    raw = sc.textFile("hdfs:///data/transactions/")
    
    parsed = raw.map(lambda line: line.split(",")).map(
        lambda fields: (fields[0], float(fields[1]), fields[2], fields[3])
    )
    
    filtered = parsed.filter(lambda x: x[1] > 100.0)
    
    keyed = filtered.map(lambda x: (x[2], (x[1], 1)))  # key by category
    
    aggregated = keyed.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    result = aggregated.mapValues(lambda x: x[0] / x[1])
    
    return result.collect()


# AFTER: DataFrame equivalent (10-50x faster with Catalyst + Tungsten)
def modern_etl(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
        StructField("date", StringType()),
    ])
    
    df = spark.read.csv("hdfs:///data/transactions/", schema=schema)
    
    result = (df
        .filter(F.col("amount") > 100.0)
        .groupBy("category")
        .agg(F.avg("amount").alias("avg_amount"))
    )
    
    return result.collect()


# Migration validation: compare results
def validate_migration(legacy_result, modern_result):
    """Ensure migration produces identical results."""
    legacy_dict = dict(legacy_result)
    modern_dict = {row.category: row.avg_amount for row in modern_result}
    
    for key in legacy_dict:
        assert abs(legacy_dict[key] - modern_dict[key]) < 0.001, \
            f"Mismatch for {key}: {legacy_dict[key]} vs {modern_dict[key]}"
    
    print("Migration validation passed!")
```

### Migration Strategy Checklist

| Step | Action | Risk |
|------|--------|------|
| 1 | Define schema for raw data | Type mismatches |
| 2 | Replace textFile + map with read.csv/json/parquet | Format assumptions |
| 3 | Replace filter lambdas with column expressions | Logic differences |
| 4 | Replace reduceByKey with groupBy + agg | Floating-point ordering |
| 5 | Replace custom partitioning with repartition/bucketBy | Distribution changes |
| 6 | Run both pipelines in parallel and compare | Regression detection |

---

## Interview Tips

> **Tip 1:** "How would you handle data skew in RDD operations?" — "First, identify the hot keys using sampling. Then choose a strategy: salt the keys by appending a random suffix to spread hot keys across partitions, use a custom partitioner that routes hot keys to dedicated partitions, or use a two-phase aggregation where you partially aggregate with salted keys first, then aggregate again with the original keys. Monitor with glom().map(len).collect() to verify even distribution."

> **Tip 2:** "Describe a real scenario where you'd use accumulators." — "Data quality monitoring during ETL. I register accumulators for parse errors, null counts, and schema violations. As the pipeline processes records, accumulators collect metrics without extra passes over the data. After the job completes, I push these metrics to a dashboard. The key caveat is that accumulators aren't idempotent with task retries, so they're for approximate monitoring, not exact counts."

> **Tip 3:** "How would you migrate an RDD pipeline to DataFrames?" — "Run both in parallel with validation. First, define explicit schemas. Then replace textFile+map with structured readers, lambdas with column expressions, and reduceByKey with groupBy+agg. The tricky parts are custom partitioning (use repartition or bucketBy) and mapPartitions with resource management (use foreachPartition or Pandas UDFs). Always validate by comparing outputs within a tolerance for floating-point."
