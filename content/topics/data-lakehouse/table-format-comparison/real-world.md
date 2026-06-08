---
title: "Table Format Comparison — Real World"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [delta-lake, iceberg, hudi, production, migration]
---

# Table Format Comparison — Real World

## Industry Usage Patterns

```
Companies and their table format choices:

Databricks / Databricks customers (many Fortune 500):
  Format: Delta Lake
  Rationale: native optimization, Unity Catalog, managed platform
  Notable: Delta Delta Lake is default for all Databricks clusters

Netflix:
  Format: Apache Iceberg (they created it)
  Scale: 10,000+ tables, exabyte-scale
  Engines: Spark (ETL), Trino (analytics), Flink (streaming)
  Rationale: multi-engine, open standard, hidden partitioning

Uber:
  Format: Apache Hudi (they created it)
  Scale: 100,000+ tables, petabyte-scale
  Rationale: high-frequency trip updates, incremental ingestion

Apple:
  Format: Apache Iceberg (2021 announcement)
  Scale: petabytes, thousands of engineers
  Rationale: multi-engine standardization across internal tools

LinkedIn:
  Format: Apache Iceberg + Hudi
  Iceberg: for Presto/Trino analytics workloads
  Hudi: for CDC ingestion pipelines

AWS recommended stack:
  Format: Apache Iceberg
  Catalog: AWS Glue
  Compute: EMR (Spark) + Athena (SQL)
  Rationale: Iceberg native in Athena, Glue catalog integration

Conclusion: No format "won" — all three are production-proven at scale.
Choose based on your stack, not on abstract feature comparisons.
```

---

## Pattern: Multi-Format Lakehouse with XTable

```python
# Apache XTable (OneTable): translate metadata between formats
# Use case: write primary format, expose others for interoperability

# Install: pip install apache-xtable

# Convert Delta → Iceberg metadata
from xtable.client import TableMetaClient

# Config: source table + target formats
config = {
    "sourceFormat": "DELTA",
    "sourcePath": "s3://bucket/delta/orders",
    "targetFormats": ["ICEBERG"],
    "iceberg": {
        "catalogImpl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "catalogProperties": {
            "warehouse": "s3://bucket/iceberg",
            "region": "us-east-1"
        }
    }
}

# Run sync (generates Iceberg metadata from Delta transaction log)
TableMetaClient.sync(config)
# Result: Iceberg metadata written to s3://bucket/iceberg/orders/metadata/
# Same Parquet data files — no data copy!

# Schedule sync job (run every 15 minutes for near-real-time)
# Airflow DAG trigger: after Delta write completes
# Or: Databricks Workflows: runs XTable sync after each batch

# Trino reads Iceberg (synced from Delta):
SELECT * FROM iceberg.db.orders WHERE order_date = '2024-01-15';
-- Reads the same Parquet files that Delta writes

# Delta primary writer continues unaware of Iceberg readers
df.write.format("delta").mode("append").save("s3://bucket/delta/orders")
# XTable sync runs → Iceberg metadata updated
# Trino queries see fresh data within sync interval
```

---

## Pattern: Side-by-Side Format Benchmark

```python
# Performance benchmark: Delta vs Iceberg for the same workload
# Run before committing to a format for a new project

def benchmark_format(spark, format_name: str, path: str, df, queries: list):
    import time
    results = {"format": format_name, "write_time": 0, "query_times": []}
    
    # Write benchmark
    start = time.time()
    df.write.format(format_name).mode("overwrite") \
        .partitionBy("order_date") \
        .save(path)
    results["write_time"] = time.time() - start
    
    # Query benchmark (run each 3 times, take median)
    for query in queries:
        times = []
        for _ in range(3):
            start = time.time()
            spark.sql(query.replace("{path}", path)).collect()
            times.append(time.time() - start)
        results["query_times"].append({
            "query": query[:60],
            "median_sec": sorted(times)[1]
        })
    
    return results

queries = [
    "SELECT COUNT(*) FROM {path} WHERE order_date='2024-01-15'",
    "SELECT region, SUM(amount) FROM {path} GROUP BY region",
    "SELECT * FROM {path} WHERE customer_id=12345",
    "SELECT * FROM {path} WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31' AND status='delivered'",
]

delta_results = benchmark_format(spark, "delta", "s3://bucket/benchmark/delta", df, queries)
iceberg_results = benchmark_format(spark, "iceberg", "s3://bucket/benchmark/iceberg", df, queries)

# Compare and document
for d, i in zip(delta_results["query_times"], iceberg_results["query_times"]):
    winner = "Delta" if d["median_sec"] < i["median_sec"] else "Iceberg"
    print(f"{d['query']}: Delta={d['median_sec']:.2f}s, Iceberg={i['median_sec']:.2f}s → {winner}")
```

---

## Interview Tips

> **Tip 1:** "Your current stack is EMR + Athena. Your new team member says 'let's use Delta because Databricks is popular.' How do you respond?" — Acknowledge Delta's strengths, then explain the fit issue: Delta on EMR requires additional JAR configuration and Athena reads Delta via manifest files (extra complexity and potential staleness). Iceberg is natively supported by both Athena and EMR with Glue catalog — zero extra setup, no manifest file management. For your stack, Iceberg is the better choice. If you later move to Databricks, Delta is easy to add or UniForm bridges the gap.

> **Tip 2:** "Has anyone successfully run all three formats in the same pipeline?" — Yes, and it's more common than you'd think. A CDC ingestion layer might use Hudi (MOR for fast upserts), a shared analytics layer might use Iceberg (Trino + Spark access), and ML feature tables might use Delta (Databricks MLflow integration). The key is clear ownership: each layer has one primary format, and you don't mix formats within the same logical table. XTable handles the metadata translation for cross-format reads.

> **Tip 3:** "What does 'open lakehouse' actually mean in practice?" — It means: data stored as open-format files (Parquet) on commodity cloud storage (S3/GCS/ADLS), with an open table format (Iceberg) and an open catalog (Apache Polaris/Hive Metastore), readable by any engine implementing the open spec. The practical benefit: you can switch compute vendors without migrating data. You can query the same table with Spark today and Trino tomorrow. Compare to a proprietary warehouse where your data is locked in a format only the vendor's engine understands.
