---
title: "Storage Optimization — Real World"
topic: data-lakehouse
subtopic: storage-optimization
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [storage, production, cost, compaction, optimization]
---

# Storage Optimization — Real World

## Pattern 1: Automated Storage Optimization Pipeline

```python
# Daily storage optimization job — runs at 3 AM

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import boto3

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

TABLES_CONFIG = [
    # (table_path, zone, optimize_frequency, zorder_cols, vacuum_hours)
    ("s3://bucket/bronze/orders",     "bronze", "weekly",  None,                    720),  # 30 days retention
    ("s3://bucket/silver/orders",     "silver", "daily",   ["customer_id"],         168),  # 7 days
    ("s3://bucket/silver/clickstream","silver", "daily",   ["user_id","event_date"],168),
    ("s3://bucket/gold/daily_revenue","gold",   "weekly",  ["region"],              168),
]

def run_storage_optimization(today: str):
    day_of_week = datetime.strptime(today, "%Y-%m-%d").weekday()
    is_weekly = (day_of_week == 0)  # Monday
    
    for table_path, zone, frequency, zorder_cols, vacuum_hours in TABLES_CONFIG:
        if frequency == "daily" or (frequency == "weekly" and is_weekly):
            print(f"\n=== Optimizing {table_path} ===")
            
            # Pre-optimize stats
            pre_details = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
            pre_files = pre_details["numFiles"]
            pre_size_gb = pre_details["sizeInBytes"] / (1024**3)
            avg_file_mb = (pre_details["sizeInBytes"] / max(pre_files, 1)) / (1024**2)
            
            print(f"  Before: {pre_files:,} files, {pre_size_gb:.1f}GB, avg {avg_file_mb:.1f}MB/file")
            
            # OPTIMIZE (with optional ZORDER)
            if zorder_cols:
                zorder_str = ", ".join(zorder_cols)
                spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_str})")
            else:
                spark.sql(f"OPTIMIZE delta.`{table_path}`")
            
            # VACUUM
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN {vacuum_hours} HOURS")
            
            # Post-optimize stats
            post_details = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
            post_files = post_details["numFiles"]
            print(f"  After:  {post_files:,} files ({pre_files - post_files:,} removed)")

run_storage_optimization(datetime.now().strftime("%Y-%m-%d"))
```

---

## Pattern 2: Partition Analysis and Rebalancing

```python
# Diagnose and fix partition skew

def analyze_partition_sizes(spark, table_path: str, partition_cols: list):
    """Show partition size distribution to identify skew."""
    df = spark.read.format("delta").load(table_path)
    
    # Size per partition
    partition_stats = df.groupBy(*partition_cols) \
        .agg(
            {"*": "count"},
        ).withColumnRenamed("count(1)", "row_count")
    
    partition_stats.orderBy("row_count", ascending=False).show(20)
    
    stats = partition_stats.agg(
        {"row_count": "min"},
        {"row_count": "max"},
        {"row_count": "avg"},
    ).collect()[0]
    
    print(f"\nRow count per partition:")
    print(f"  Min: {stats[0]:,}")
    print(f"  Max: {stats[1]:,}")
    print(f"  Avg: {stats[2]:,.0f}")
    print(f"  Skew ratio (max/avg): {stats[1]/stats[2]:.1f}x")
    
    if stats[1] / stats[2] > 10:
        print("⚠️  High skew detected (>10x) — consider re-partitioning or salting")

# Example: orders partitioned by (order_date, region) has one very large partition
analyze_partition_sizes(spark, "s3://bucket/silver/orders", ["order_date", "region"])
# Skew ratio 50x → US region has 50x more orders than other regions

# Fix: re-partition with more granular strategy
# Old: partition by (order_date, region) — US is 50GB, others are 1GB
# New: partition by (order_date) only → more even size, filter on region uses ZORDER

df = spark.read.format("delta").load("s3://bucket/silver/orders")
df.write.format("delta") \
    .partitionBy("order_date") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("s3://bucket/silver/orders_v2")

spark.sql("OPTIMIZE delta.`s3://bucket/silver/orders_v2` ZORDER BY (region, customer_id)")
```

---

## Pattern 3: Cost Attribution per Team

```python
# Track storage costs by team/domain for chargeback

def generate_storage_cost_report(bucket: str):
    """Generate monthly storage cost report per domain."""
    s3 = boto3.client("s3", region_name="us-east-1")
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    
    # Get bucket size by prefix from CloudWatch BucketSizeBytes metric
    domains = ["bronze/orders", "silver/orders", "gold/", "bronze/clickstream"]
    
    report = []
    for domain in domains:
        # Use S3 Storage Lens or CloudWatch for size metrics
        response = cw.get_metric_statistics(
            Namespace="AWS/S3",
            MetricName="BucketSizeBytes",
            Dimensions=[
                {"Name": "BucketName", "Value": bucket},
                {"Name": "StorageType", "Value": "StandardStorage"},
                {"Name": "FilterId", "Value": domain.replace("/", "_")},
            ],
            StartTime=datetime.now() - timedelta(days=7),
            EndTime=datetime.now(),
            Period=86400,
            Statistics=["Average"],
        )
        
        if response["Datapoints"]:
            avg_bytes = response["Datapoints"][-1]["Average"]
            size_gb = avg_bytes / (1024**3)
            monthly_cost = size_gb * 0.023
            
            report.append({
                "domain": domain,
                "size_gb": round(size_gb, 1),
                "monthly_cost_usd": round(monthly_cost, 2),
            })
    
    for row in sorted(report, key=lambda x: -x["monthly_cost_usd"]):
        print(f"{row['domain']}: {row['size_gb']}GB → ${row['monthly_cost_usd']}/month")
    
    total = sum(r["monthly_cost_usd"] for r in report)
    print(f"\nTotal: ${total:.2f}/month")
    return report
```

---

## Interview Tips

> **Tip 1:** "Your storage bill just went from $5K to $15K in one month. How do you investigate?" — Step 1: AWS Cost Explorer — identify which service and bucket drove the increase. Step 2: S3 Storage Lens — show size growth by prefix and storage class. Step 3: likely culprits: (a) a new streaming job creating millions of small files (check file count by prefix), (b) lifecycle policy removed (Standard instead of Glacier for old data), (c) a backfill job wrote 3 years of data in one month, (d) Delta log not being vacuumed (orphan files accumulating). Step 4: run OPTIMIZE + VACUUM on suspect tables, fix lifecycle if removed.

> **Tip 2:** "How do you decide the right partition granularity?" — Rule: each partition should be 100MB–10GB of data. Calculate: total table size ÷ expected partition size = number of partitions. For 1TB of daily data, partitioning by day gives 2.7GB/day × 365 = 1TB — fine. Adding hour gives 113MB/hour — borderline. Adding region (10 regions) + hour = 11MB/hour/region — too small, small files problem. The right partition strategy depends on total data volume. Re-evaluate when table grows 5-10×.

> **Tip 3:** "What's the cheapest way to store 10 years of historical Bronze data?" — S3 Glacier Deep Archive: $0.00099/GB/month = $990/year per PB. Enable: S3 lifecycle rule after 365 days. Trade-off: retrieval takes 12 hours and costs $0.02/GB. For data only accessed for audits (once a year), Glacier Deep Archive is 23× cheaper than Standard. If you need occasional access (monthly), use Glacier Instant Retrieval ($0.004/GB + $0.03/GB retrieval — millisecond access). Model your access frequency before choosing the tier.
