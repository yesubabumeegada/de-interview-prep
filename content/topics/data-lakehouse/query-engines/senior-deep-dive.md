---
title: "Query Engines (Trino, Spark, Flink) — Senior Deep Dive"
topic: data-lakehouse
subtopic: query-engines
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [trino, spark, flink, duckdb, velox, execution-engine]
---

# Query Engines — Senior Deep Dive

## Spark Catalyst Optimizer Internals

```
Spark SQL query lifecycle:
  SQL/DataFrame → Unresolved Logical Plan
                → Analyzed Logical Plan (resolve references, types)
                → Optimized Logical Plan (Catalyst rules applied)
                → Physical Plans (multiple candidates)
                → Selected Physical Plan (cost model chooses)
                → RDD/bytecode execution

Key Catalyst optimizations:
  1. Predicate pushdown: filter as early as possible in the plan
     BEFORE: Scan → Filter → Join
     AFTER:  Filter → Scan (read fewer rows from S3)
  
  2. Column pruning: read only required columns from Parquet
     SELECT order_id, amount → read only 2 columns from columnar file
  
  3. Constant folding: evaluate expressions at planning time
     WHERE amount > 1000 + 500 → WHERE amount > 1500 (computed once)
  
  4. Join reordering (CBO): use statistics to order JOINs (small × large)
     Requires: ANALYZE TABLE to collect stats
     spark.conf.set("spark.sql.cbo.enabled", "true")
  
  5. Whole-stage code generation: compile DAG stages to single Java method
     Eliminates per-row virtual function calls
     Can be disabled: spark.conf.set("spark.sql.codegen.wholeStage", "false")

Tungsten engine (physical execution):
  Binary in-memory format: off-heap memory, no GC pressure
  Cache-friendly data layouts: columnar processing within Spark executor
  SIMD instructions: vectorized expression evaluation
```

---

## Flink State Management at Scale

```python
# Flink's state backend determines performance at scale

# RocksDB state backend (for large state):
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import RocksDBStateBackend

env = StreamExecutionEnvironment.get_execution_environment()

# RocksDB: spills to disk, can handle state >> RAM
env.set_state_backend(RocksDBStateBackend(
    checkpoint_storage_path="s3://bucket/flink-checkpoints",
    incremental=True  # only checkpoint state changes (much faster)
))

# Heap state backend (for small state):
# All state in JVM heap (fast access, limited by memory)
# Use when: state per key < 1MB, total state < 20GB

# State expiry (critical for long-running jobs):
from pyflink.datastream.state import StateTtlConfig
from pyflink.common.time import Time

ttl_config = StateTtlConfig.new_builder(Time.days(7)) \
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
    .build()
# Without TTL: state grows indefinitely → OOM on long-running jobs

# Exactly-once stateful processing:
# Flink checkpoints capture: Kafka offsets + all operator state
# On failure: restart from checkpoint → replay Kafka from saved offset
# + RocksDB state → exact state as at checkpoint
# = Exactly-once output (with transactional sink like Delta/Iceberg)
```

---

## Trino vs Presto Lineage and Arrow Flight

```
Presto vs Trino:
  Facebook created Presto (2012)
  Community forked to Trino (2020) after Facebook changes
  Presto: still active (Meta + Presto Foundation)
  Trino: community-led, faster feature development
  
  Technical divergence:
    Trino: native Iceberg V2 support, better dynamic filtering
    Presto: more compatible with Facebook's internal ecosystem
    
  Choose Trino for new deployments (more active community)
  Presto if you have existing investments / Meta ecosystem

Arrow Flight SQL (emerging standard):
  Apache Arrow: columnar in-memory format (avoids serialization overhead)
  Arrow Flight SQL: RPC protocol for query services using Arrow
  
  Benefit: eliminates Parquet/JSON serialization between engine and client
    Traditional: Engine → serialize to JSON → network → client deserialize
    Arrow Flight: Engine → already columnar (Arrow) → network → client uses directly
    Speedup: 3-10× for column-heavy result sets
  
  Support:
    Trino: experimental Arrow Flight SQL endpoint
    Databricks: Arrow-based JDBC/ODBC drivers
    DuckDB: native Arrow in-memory
    
  Use case: BI tools (Tableau, Superset) that support Arrow Flight get faster results
```

---

## DuckDB in the Lakehouse

```python
# DuckDB: in-process OLAP database
# Runs inside Python process, no server needed
# Reads Parquet/Delta/Iceberg directly from S3
# Perfect for: local development, data exploration, small team analytics

import duckdb

conn = duckdb.connect()

# Read from S3 Parquet directly (no cluster needed)
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    SET s3_region='us-east-1';
    SET s3_access_key_id='...';
    SET s3_secret_access_key='...';
""")

result = conn.execute("""
    SELECT 
        date_trunc('month', order_date) AS month,
        SUM(amount) AS revenue
    FROM read_parquet('s3://bucket/silver/orders/**/*.parquet')
    WHERE order_date >= '2024-01-01'
    GROUP BY 1
    ORDER BY 1
""").fetchdf()

# Read Iceberg table via DuckDB (with icebergcat extension)
conn.execute("INSTALL iceberg; LOAD iceberg;")
df = conn.execute("""
    SELECT * FROM iceberg_scan('s3://bucket/iceberg/orders/metadata/v1.metadata.json')
    LIMIT 100
""").fetchdf()

# DuckDB performance:
# Single machine, but uses all CPU cores + vectorized execution
# 1GB dataset: DuckDB ~1 sec vs Spark ~30 sec (cluster overhead)
# 100GB dataset: DuckDB ~30 sec (RAM-limited) vs Spark ~60 sec (distributed)
# 1TB dataset: DuckDB fails (OOM), Spark wins

# Use DuckDB for:
# Local development (test queries before running on cluster)
# Small analytics jobs (< 50GB) where Spark overhead is wasteful
# MotherDuck: DuckDB-as-a-service (cloud-hosted)
```

---

## Velox: Next-Generation Execution Engine

```
Velox (Meta, open-sourced 2022):
  C++ vectorized execution engine designed to be embedded in query engines
  
  Problem it solves:
    Spark, Presto, Flink all have their own execution engines
    Each engine re-implements the same operations (hash join, aggregation)
    Different performance characteristics, different bugs, different optimizations
  
  Velox: shared C++ execution library
    Presto: replaced Java execution with Velox (PrestoDB Velox)
    Spark: Gluten project embeds Velox in Spark native execution
    Arrow: shares data format (Arrow columnar = Velox-compatible)
  
  Performance improvements:
    Presto + Velox: 2-5× faster on TPC-H benchmarks
    Spark + Velox (Gluten): 3-8× faster on some queries
  
  Databricks Photon:
    Similar concept: C++ vectorized engine embedded in Databricks Runtime
    Replaces Spark Java execution for SQL operations
    Photon-enabled: 2-4× faster queries, lower compute cost
    Available: Databricks Runtime 9.1+, requires Premium tier
  
  Relevance for DE engineers:
    Photon: choose Photon-enabled Databricks clusters for SQL-heavy workloads
    Gluten: experimental, use for JVM bottleneck scenarios
    Both: transparent (same Spark/SQL API, faster execution underneath)
```

---

## Interview Tips

> **Tip 1:** "Why does Spark sometimes produce incorrect results with groupBy?" — Potential cause: data skew. If one group key has 100M rows and others have 100K, the executor handling that key becomes a bottleneck — not incorrect results, but slow. More dangerous: if using `approx_count_distinct` (HyperLogLog approximation) — results are approximate, not exact. Also: `sortWithinPartitions` only sorts within each partition, not globally — use `orderBy` for global sort. If you suspect correctness issue: add `.repartition(col("key"))` before groupBy to ensure all rows for a key are on one partition.

> **Tip 2:** "How does Trino handle queries that don't fit in memory?" — Trino spills to disk via "spilling" for hash joins and aggregations when memory is exhausted. Configure: `spill-enabled=true`, `spill-path=/data/spill`. Without spilling: Trino throws `ExceededMemoryLimitException`. With spilling: query completes but with disk I/O overhead (slower). For very large datasets: use Spark (designed for disk spilling from the start) or pre-aggregate in Spark, then serve summary results through Trino.

> **Tip 3:** "When would you use Flink over Spark for batch processing?" — Rarely. Spark is the standard for batch. But: Flink's batch execution (DataSet API or Flink SQL in BATCH mode) is competitive for: jobs with large state that doesn't fit in Spark's executor memory (Flink's state backend scales to TB via RocksDB), or pipelines that need to switch seamlessly between streaming and batch (same Flink SQL logic). In practice: most teams use Spark for batch, Flink for streaming, and accept the operational cost of maintaining two runtimes.
