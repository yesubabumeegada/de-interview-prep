---
title: "Cost Optimization — Fundamentals"
topic: system-design
subtopic: cost-optimization
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, cost-optimization, cloud-costs, storage, compute]
---

# Cost Optimization — Fundamentals


## 🎯 Analogy

Think of data platform cost optimization like utility bills: compute is your electricity (use auto-suspend, right-size, spot instances), storage is your water (compress, tier cold data to cheap storage), and data transfer is your phone bill (minimize cross-region and egress).

---
## The Main Cloud Cost Drivers in DE

| Cost Category | What Drives It | Optimization Levers |
|---|---|---|
| **Storage** | Data volume × storage tier | Compression, tiering, retention policies |
| **Compute** | CPU hours × instance type | Right-sizing, spot instances, auto-scaling |
| **Data transfer** | Cross-region / egress traffic | Co-locate compute and storage |
| **DW queries** | Bytes scanned per query | Partitioning, clustering, caching |
| **Streaming** | Throughput × retention × replicas | Tune partitions, retention, RF |

---

## Storage Optimization

```python
# Compression: biggest quick win for storage costs

# Parquet compression options:
# snappy: fast read/write, moderate compression (~2x)
# gzip:   slower, better compression (~4x)
# zstd:   best of both (Spark 2.3+, ~3x with fast read speed)

# Write with zstd compression:
df.write.option("compression", "zstd").parquet("s3://bucket/data/")

# Delta Lake: compression is automatic (Parquet under the hood)
# Enable zstd explicitly:
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Storage savings example:
# 100GB raw CSV → 10GB Parquet (snappy) = 90% reduction
# 100GB raw CSV → 7GB Parquet (zstd) = 93% reduction
# At $0.023/GB/month: 100GB = $2.30/month vs 7GB = $0.16/month

# Tiered storage lifecycle (S3):
# Automatically move data to cheaper tiers as it ages
# Configure in AWS S3 → Bucket → Management → Lifecycle Rules:
# 0 days:   S3 Standard        ($0.023/GB)
# 30 days:  S3 Standard-IA     ($0.0125/GB)
# 90 days:  S3 Glacier Instant ($0.004/GB)
# 365 days: S3 Glacier Deep    ($0.00099/GB)
```

---

## Compute Optimization

```
Right-sizing: use the smallest instance that meets requirements
  Don't: always use the largest "to be safe"
  Do: profile CPU and memory utilization; resize to 70% target utilization

Auto-scaling: scale down during off-peak hours
  EMR/Dataproc: set min nodes=2, max nodes=20 (scale up for big jobs, down when idle)
  Snowflake warehouse: AUTO_SUSPEND = 60 seconds (shut down when idle)
  Databricks: cluster auto-termination after 30 minutes idle

Spot/Preemptible instances: 60-90% cheaper than on-demand
  Risk: instance can be reclaimed with 2-minute notice
  Safe for: Spark batch jobs (driver on on-demand, workers on spot)
           Kafka consumers (stateless, auto-restart on interruption)
  Not safe for: Kafka brokers (leader, single-point state)
               Database primaries

Reserved/committed use: 30-50% savings for predictable baseline load
  AWS Reserved Instances: 1-year or 3-year commitment
  Snowflake: pre-purchase credits at discount
  Strategy: reserve baseline, use on-demand for spikes
```

---

## Query Cost Optimization (Snowflake/BigQuery)

```sql
-- BigQuery: billed per bytes scanned — minimize bytes!

-- Bad: SELECT * scans ALL columns
SELECT * FROM orders WHERE order_date = '2024-01-15';
-- Scans: all columns × all rows for that date

-- Good: select only needed columns (columnar storage = column pruning)
SELECT order_id, customer_id, amount FROM orders WHERE order_date = '2024-01-15';
-- Scans: 3 columns only (much cheaper)

-- Partition filter: eliminates entire partitions
-- Without: scans full table = 1TB
SELECT COUNT(*) FROM orders;
-- With: scans only matching partition = 1GB
SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-15';

-- Snowflake: billed per compute time (virtual warehouse seconds)
-- Save cost: cluster properly, use auto-suspend, avoid full scans

-- Avoid SELECT * in production queries:
-- Explicitly name needed columns → reads fewer micro-partitions (Snowflake)
-- → reads fewer column chunks (BigQuery)
-- → reads fewer Parquet row groups (Delta/Iceberg)
```

---

## Data Retention Policies

```python
# Delete data you don't need — the cheapest data is data you don't store

# Delta Lake: VACUUM (remove old versions)
spark.sql("VACUUM delta.`s3://bucket/delta/orders` RETAIN 168 HOURS")
# 168 hours = 7 days of time-travel history
# Deletes files referenced only by commits older than 7 days

# Delta Lake: set table retention property
spark.sql("""
    ALTER TABLE orders SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 7 days',
        'delta.logRetentionDuration' = 'interval 30 days'
    )
""")

# S3: delete objects older than N days
# AWS S3 Lifecycle rule (JSON):
# {
#   "Rules": [{ "Status": "Enabled",
#     "Expiration": { "Days": 365 },
#     "Filter": { "Prefix": "raw/events/" }
#   }]
# }

# Snowflake: TIME_TRAVEL and FAIL_SAFE storage
# Default: 90 days FAIL_SAFE (can't turn off) + 90 days TIME_TRAVEL
# Reduce TIME_TRAVEL for non-critical tables:
ALTER TABLE staging_table SET DATA_RETENTION_TIME_IN_DAYS = 1;
# Reduces Snowflake storage cost significantly for large staging tables
```

---


## ▶️ Try It Yourself

```python
# Cost optimization checklist as code
def audit_costs(config: dict) -> list:
    recommendations = []

    # 1. Warehouse auto-suspend
    if config.get("warehouse_auto_suspend_seconds", 9999) > 300:
        recommendations.append("Set warehouse AUTO_SUSPEND <= 300s to avoid idle billing")

    # 2. Partition pruning enabled?
    if not config.get("partition_pruning", False):
        recommendations.append("Enable partition pruning — reading full tables wastes compute $")

    # 3. Data compression
    if config.get("storage_format") not in ("parquet", "orc", "delta"):
        recommendations.append("Use columnar format (Parquet/ORC) — 3-10x storage reduction")

    # 4. Cold data tiering
    if config.get("cold_data_storage_class") == "standard":
        recommendations.append("Move data >90 days to S3 Glacier or similar — 80% cheaper")

    # 5. Spot/preemptible instances for batch
    if not config.get("use_spot_instances", False):
        recommendations.append("Use Spot/Preemptible instances for batch ETL — 60-80% savings")

    return recommendations

config = {"warehouse_auto_suspend_seconds": 3600, "storage_format": "csv"}
for rec in audit_costs(config):
    print(f"⚠️  {rec}")
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How do you reduce Parquet file storage costs?" — Three levers: (1) Compression: switch from snappy to zstd — same read speed, 20-30% better compression ratio. (2) Column pruning: store only columns you actually use (remove raw source columns you don't need after transformation). (3) Lifecycle policies: move files older than 30 days to S3 Infrequent Access, older than 90 days to Glacier. For a 10TB data lake: these three together can reduce storage costs by 80%.

> **Tip 2:** "What is the #1 way to reduce Snowflake costs?" — Auto-suspend warehouses when idle. Default is 10 minutes; set to 60 seconds for dev/BI warehouses that have bursty usage. A warehouse running idle at LARGE size costs ~$16/hour. At 60-second auto-suspend: only pay when queries are actually running. Second biggest: right-size warehouses — an XL warehouse costs 4× a M warehouse; make sure the job actually needs the extra size.

> **Tip 3:** "How do you make BigQuery cost-efficient?" — Never use `SELECT *`. Always specify only needed columns (columnar: unused columns aren't scanned). Partition all large tables by date and always include a date filter in queries. Use clustering on the most filtered non-date columns. Enable partitioned table requirement (reject unpartitioned queries). Set query cost alerts. Use cached results (queries identical to the previous 24 hours are free).
