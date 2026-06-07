---
title: "Spark Connect - Intermediate"
topic: pyspark
subtopic: spark-connect
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, spark-connect, remote-session, databricks-connect, limitations]
---

# Spark Connect — Intermediate

## Remote SparkSession Configuration

```python
from pyspark.sql import SparkSession

# Basic remote connection
spark = SparkSession.builder.remote("sc://spark-server:15002").getOrCreate()

# With session-level configs and auth
spark = (
    SparkSession.builder
    .remote("sc://spark-server:15002;token=eyJ0eXAi...;user_id=alice")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
```

| Config Scope | Settable from Client? | Examples |
|-------------|----------------------|----------|
| SQL runtime | Yes | shuffle.partitions, adaptive.enabled |
| Session | Yes | spark.sql.warehouse.dir |
| Executor resources | No (server only) | executor.memory, executor.cores |

---

## Databricks Connect (Built on Spark Connect)

```python
# pip install databricks-connect==14.3.*
from databricks.connect import DatabricksSession

spark = (
    DatabricksSession.builder
    .host("https://my-workspace.cloud.databricks.com")
    .token("dapi1234567890abcdef")
    .clusterId("0123-456789-abcde12")
    .getOrCreate()
)

# Standard DataFrame API — execution on Databricks cluster
df = spark.read.table("catalog.schema.sales")
result = df.groupBy("region").agg({"revenue": "sum"})
result.show()
```

| Feature | Spark Connect (OSS) | Databricks Connect |
|---------|--------------------|--------------------|
| Server setup | Manual | Managed (any cluster) |
| Auth | Token via URL | Databricks PAT / OAuth |
| Catalog | Manual config | Unity Catalog built-in |
| Cluster auto-start | No | Yes (on first query) |

---

## Supported Operations

All standard DataFrame transformations work identically over Spark Connect:

```python
from pyspark.sql.functions import col, sum, avg, broadcast
from pyspark.sql.window import Window

# Reads, filters, aggregations, joins, window functions — all work
df = spark.read.format("delta").load("s3://lakehouse/events/")
agg = df.groupBy("user_id").agg(sum("amount").alias("total"))

w = Window.partitionBy("region").orderBy(col("amount").desc())
ranked = df.withColumn("rank", row_number().over(w))

# Broadcast join hint (replaces sc.broadcast)
result = large_df.join(broadcast(small_df), "key")
```

---

## Current Limitations (Spark 3.5)

```python
# NO RDD API
spark.sparkContext  # Error — not available

# NO SparkContext operations
sc.parallelize([1, 2, 3])      # Not supported
sc.broadcast(lookup_dict)       # Not supported
sc.accumulator(0)               # Not supported

# Workarounds:
# RDD → DataFrame
df = spark.createDataFrame(data, schema=["col1", "col2"])

# Broadcast → join hint
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Accumulator → aggregation
error_count = df.filter("is_error = true").count()
```

| Feature | Status (Spark 3.5) |
|---------|-------------------|
| DataFrame API | Fully supported |
| Spark SQL | Fully supported |
| Pandas UDFs (Arrow) | Supported |
| Scalar Python UDFs | Supported |
| Structured Streaming | Supported |
| RDD API | Not supported |
| SparkContext access | Not supported |
| Custom Accumulators | Not supported |

---

## Error Handling Over gRPC

```python
from pyspark.errors import PySparkException

def safe_query(spark, query: str):
    try:
        return spark.sql(query).collect()
    except PySparkException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"Table not found: {e}")
            return None
        raise
    except Exception as e:
        # gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED)
        print(f"Connection error: {type(e).__name__}: {e}")
        return None
```

| gRPC Status | Meaning | Action |
|-------------|---------|--------|
| `UNAVAILABLE` | Server down | Retry with backoff |
| `DEADLINE_EXCEEDED` | Query timeout | Optimize query |
| `UNAUTHENTICATED` | Invalid token | Refresh and retry |
| `RESOURCE_EXHAUSTED` | Server overloaded | Back off |

---

## Connection Management

```python
import time
from pyspark.sql import SparkSession

class SparkConnectManager:
    def __init__(self, url: str, max_retries: int = 3):
        self.url = url
        self.max_retries = max_retries
        self._spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self._connect()
        return self._spark

    def _connect(self) -> SparkSession:
        for attempt in range(self.max_retries):
            try:
                session = SparkSession.builder.remote(self.url).getOrCreate()
                session.sql("SELECT 1").collect()  # Validate
                return session
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise ConnectionError(f"Failed after {self.max_retries} attempts: {e}")

    def reconnect(self):
        self._spark = None
        return self.spark

manager = SparkConnectManager("sc://spark-server:15002;token=my-token")
df = manager.spark.read.parquet("s3://data/events/")
```

---

## Interview Tips

> **Tip 1:** "How does Databricks Connect work?" — "It's built on Spark Connect protocol. You install databricks-connect locally, configure workspace URL and cluster ID, and use the standard DataFrame API. Code runs locally but execution happens on the Databricks cluster via gRPC."

> **Tip 2:** "How do you handle connection failures?" — "Retry with exponential backoff for gRPC errors, enable reattachable execution for in-progress queries, validate connections with a heartbeat query, and maintain a reconnection wrapper."

> **Tip 3:** "What can't you do with Spark Connect?" — "No RDD API, no SparkContext access, and executor resources are server-level (not settable per session). Workarounds: use DataFrames instead of RDDs, use broadcast() join hint instead of sc.broadcast(), and use aggregations instead of accumulators."
