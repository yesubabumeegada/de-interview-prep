---
title: "Query Engines — Scenarios"
topic: data-lakehouse
subtopic: query-engines
content_type: scenario_question
tags: [trino, spark, presto, query-engines, scenarios]
---

# Query Engines — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: When to Use Trino vs Apache Spark

**Scenario:** Your analytics team has two tools available: Trino (PrestoSQL) and Apache Spark. An analyst asks which one to use for: (a) an ad-hoc query on 1TB of Parquet on S3, (b) a nightly ETL job that joins 50TB across 6 tables, (c) training an ML model on 500GB of data.

<details>
<summary>💡 Hint</summary>

Trino is optimized for interactive/ad-hoc SQL — low latency, no disk spill by default, pure SQL. Spark is optimized for large-scale batch processing with disk spill support, ML libraries, and complex transformation logic.

</details>

<details>
<summary>✅ Solution</summary>

**Tool Selection:**

**(a) Ad-hoc query on 1TB Parquet — Use Trino**
- Trino processes in-memory with vectorized execution
- Faster for interactive queries (seconds to minutes)
- No need to write code — pure SQL
- Automatically reads Parquet column statistics for pruning

```sql
-- Trino query with predicate pushdown
SELECT region, sum(revenue) as total_revenue
FROM iceberg.gold.sales
WHERE sale_date >= DATE '2024-01-01'
GROUP BY region
ORDER BY total_revenue DESC;
```

**(b) Nightly ETL joining 50TB across 6 tables — Use Spark**
- Spark supports disk spill for joins that exceed memory
- Better for complex transformations (Python UDFs, window functions at scale)
- Cost-effective: can use spot/preemptible instances, checkpoint on failure
- Supports incremental processing with Structured Streaming or micro-batch

```python
# Spark ETL with broadcast hint for small dimension tables
from pyspark.sql.functions import broadcast

fact = spark.table("prod.gold.fact_sales")      # 50TB
dim_product = spark.table("prod.gold.dim_product")  # 100MB

result = fact.join(broadcast(dim_product), "product_id")     .groupBy("category").sum("revenue")

result.write.format("iceberg").mode("overwrite").saveAsTable("prod.gold.agg_sales")
```

**(c) ML model training on 500GB — Use Spark**
- Spark MLlib for distributed ML
- Or use Spark to preprocess/featurize, then export to pandas/sklearn

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
rf = RandomForestClassifier(numTrees=100, labelCol="label")
```

**Summary:**

| Use Case | Preferred Engine | Reason |
|----------|-----------------|--------|
| Interactive SQL < 2TB | Trino | Low latency |
| ETL > 10TB | Spark | Disk spill, fault tolerance |
| ML/complex Python | Spark | PySpark, MLlib |
| Cross-source federation | Trino | Multi-connector SQL |

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Optimizing a Slow Trino Query

**Scenario:** An analyst reports a Trino query that used to run in 2 minutes but now takes 45 minutes. The query joins an Iceberg fact table (5TB, 50M files after a bad pipeline run) with a dimension table (10GB). Diagnose and fix the performance issue.

<details>
<summary>💡 Hint</summary>

50M files is the red flag — small file problem. Trino's planning phase iterates over Iceberg manifests; with 50M files, planning alone can take minutes. Fix: compact files in Iceberg, then look at join strategy (broadcast vs hash join), and cost-based optimizer stats.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis via Trino Query Plan:**

```sql
EXPLAIN ANALYZE
SELECT f.customer_id, d.segment, sum(f.revenue) as revenue
FROM iceberg.gold.fact_sales f
JOIN iceberg.gold.dim_customer d ON f.customer_id = d.customer_id
WHERE f.sale_date >= DATE '2024-01-01'
GROUP BY 1, 2;
```

Look for:
- Planning time > execution time → file count problem
- Hash Join instead of Broadcast Join for small dimension → stats issue
- Full scan on fact table → predicate pushdown not working

**Fix 1: Compact the Fact Table (Iceberg)**

```sql
-- Check file count
SELECT count(*) as file_count, 
       avg(file_size_in_bytes)/1e6 as avg_size_mb
FROM iceberg.gold."fact_sales$files";

-- Compact via Spark (Trino can't run CALL procedures)
-- Run in Spark:
CALL prod.system.rewrite_data_files(
  table => 'prod.gold.fact_sales',
  strategy => 'binpack',
  options => map('target-file-size-bytes', '536870912')  -- 512MB
);
```

**Fix 2: Force Broadcast Join for Small Dimension**

```sql
-- Trino: use broadcast join hint
SELECT /*+ BROADCAST(d) */ f.customer_id, d.segment, sum(f.revenue)
FROM iceberg.gold.fact_sales f
JOIN iceberg.gold.dim_customer d ON f.customer_id = d.customer_id
WHERE f.sale_date >= DATE '2024-01-01'
GROUP BY 1, 2;
```

**Fix 3: Update Table Statistics for CBO**

```sql
-- Trino: analyze table for cost-based optimizer
ANALYZE iceberg.gold.fact_sales
WITH (partitions = ARRAY[ARRAY['2024-01-01'], ARRAY['2024-01-02']]);

ANALYZE iceberg.gold.dim_customer;
```

**Fix 4: Verify Predicate Pushdown**

```sql
-- Check if partition pruning is working
SELECT "$path", "$file_size"
FROM iceberg.gold.fact_sales
WHERE sale_date = DATE '2024-01-01'
LIMIT 5;
-- Should return files only from the 2024-01-01 partition
```

**Root Cause Prevention:**
```python
# Add file count alert to pipeline
def check_file_health(table_path: str, max_files: int = 100_000):
    file_count = spark.sql(f"SELECT count(*) FROM {table_path}.files").collect()[0][0]
    if file_count > max_files:
        raise ValueError(f"Too many files: {file_count}. Run compaction.")
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Building a Multi-Engine Query Federation Layer

**Scenario:** Your organization has data in: S3 (Iceberg tables via Glue), PostgreSQL (transactional), Snowflake (finance team's warehouse), and Elasticsearch (log data). Business analysts want to run SQL joins across all these in a single query without ETL. Design a federated query architecture using Trino.

<details>
<summary>💡 Hint</summary>

Trino's connector architecture allows querying multiple sources via a single SQL interface. Key concerns: performance (push predicates down to each source), security (credential management per connector), and data volume (avoid pulling large datasets from Snowflake/ES into Trino memory).

</details>

<details>
<summary>✅ Solution</summary>

**Trino Federation Architecture:**

```
Analysts → Trino Coordinator
               │
    ┌──────────┼──────────────────┐
    │          │                  │
Iceberg    PostgreSQL      Snowflake     Elasticsearch
Connector  Connector       Connector     Connector
    │          │                  │          │
  S3/Glue   RDS Postgres   Snowflake     ES Cluster
```

**Connector Configurations:**

```properties
# /etc/trino/catalog/iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://glue-metastore:9083
iceberg.catalog.type=glue
hive.s3.aws-access-key=...
hive.s3.aws-secret-key=...

# /etc/trino/catalog/postgres.properties
connector.name=postgresql
connection-url=jdbc:postgresql://rds.internal:5432/transactional
connection-user=trino_reader
connection-password=...

# /etc/trino/catalog/snowflake.properties
connector.name=snowflake
connection-url=jdbc:snowflake://org.snowflakecomputing.com
connection-user=trino_svc
connection-password=...
snowflake.warehouse=ANALYTICS_WH

# /etc/trino/catalog/elasticsearch.properties
connector.name=elasticsearch
elasticsearch.host=es.internal
elasticsearch.port=9200
elasticsearch.default-schema-name=default
```

**Cross-Source Federation Query:**

```sql
-- Join Iceberg (S3), PostgreSQL, and Snowflake in one query
SELECT
    c.customer_id,
    c.name,                           -- from PostgreSQL (transactional)
    o.total_orders,                    -- from Iceberg (data lake)
    f.lifetime_value,                  -- from Snowflake (finance)
    l.last_error_count                 -- from Elasticsearch (logs)
FROM postgres.public.customers c
JOIN (
    SELECT customer_id, count(*) as total_orders
    FROM iceberg.gold.fact_orders
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id
JOIN snowflake.finance.customer_ltv f ON c.customer_id = f.customer_id
LEFT JOIN (
    SELECT customer_id, count(*) as last_error_count
    FROM elasticsearch.default.app_logs
    WHERE timestamp > NOW() - INTERVAL '7' DAY
      AND level = 'ERROR'
    GROUP BY customer_id
) l ON c.customer_id = l.customer_id
WHERE c.region = 'APAC';
```

**Performance Optimization — Predicate Pushdown:**

```sql
-- Trino pushes WHERE clauses to each connector
-- PostgreSQL: WHERE region = 'APAC' → pushed to RDS
-- Iceberg: WHERE order_date >= '2024-01-01' → partition pruning
-- ES: WHERE level = 'ERROR' → ES filter query

-- Verify pushdown with EXPLAIN
EXPLAIN
SELECT * FROM elasticsearch.default.app_logs
WHERE level = 'ERROR' AND timestamp > NOW() - INTERVAL '7' DAY;
-- Should show: ScanFilterProject[filterPredicate=...] with ES pushdown
```

**Security Architecture:**

```python
# Credential rotation via Vault
class TrinoCredentialManager:
    def rotate_connector_credentials(self):
        for connector in ['postgres', 'snowflake']:
            new_creds = vault_client.get_secret(f"trino/{connector}")
            self.update_catalog_config(connector, new_creds)
            # Trino hot-reloads catalog configs without restart
```

**Caching Layer for Frequently-Joined Small Tables:**

```sql
-- Cache small dimension tables in Trino's Raptor/Alluxio
SET SESSION iceberg.cache_enabled = true;
SET SESSION cache_ttl = '1h';

-- Or materialize hot cross-source joins to Iceberg
CREATE TABLE iceberg.gold.customer_360 AS
SELECT ... FROM postgres.public.customers
JOIN snowflake.finance.customer_ltv ...;
-- Refresh via scheduled Trino CTAS
```

**Monitoring:**

```python
# Trino query stats via REST API
import requests

def get_slow_queries(threshold_seconds=60):
    r = requests.get("http://trino:8080/v1/query",
                     auth=('admin', ''))
    queries = r.json()
    return [q for q in queries
            if q.get('elapsedTime', 0) > threshold_seconds * 1000]
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between Trino and Presto?" — Trino is the continuation of the original Facebook Presto project after the founders left to form Starburst. PrestoSQL was renamed Trino in 2021. Facebook maintains its own fork (Meta Presto). For most purposes, Trino is the community-maintained open-source project to use.
> **Tip 2:** "How does Trino handle large joins?" — Trino uses hash joins with an in-memory hash table. For joins exceeding memory, Trino can spill to disk (enabled via `spill_enabled=true`). For better performance, use broadcast joins for small tables and ensure statistics are up to date for the CBO.
> **Tip 3:** "What is predicate pushdown and why does it matter?" — Predicate pushdown moves filter conditions from Trino's engine into the source system (e.g., Iceberg partition pruning, PostgreSQL WHERE clause, ES filter query). This reduces the amount of data transferred to Trino, dramatically improving query performance.
