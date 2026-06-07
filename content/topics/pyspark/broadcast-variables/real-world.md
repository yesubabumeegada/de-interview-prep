---
title: "PySpark Broadcast Variables - Real World Patterns"
topic: pyspark
subtopic: broadcast-variables
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, broadcast, dimension-tables, config-mapping, filtering, performance-comparison, production]
---

# PySpark Broadcast Variables — Real-World Patterns

## Pattern 1: Dimension Table Broadcast for Fact Enrichment

**Problem:** A star schema data warehouse with a 2-billion-row fact table needs to be enriched with 6 dimension tables for downstream analytics. Without broadcast, each join shuffles the fact table.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("StarSchemaEnrich").getOrCreate()

# Fact table: 2 billion rows, 200GB
sales_fact = spark.read.parquet("s3://warehouse/facts/sales/")

# Dimension tables
dim_date = spark.read.parquet("s3://warehouse/dims/date/")        # 3,650 rows (10 years)
dim_product = spark.read.parquet("s3://warehouse/dims/product/")  # 100K rows, 15MB
dim_store = spark.read.parquet("s3://warehouse/dims/store/")      # 5K rows, 2MB
dim_customer = spark.read.parquet("s3://warehouse/dims/customer/")# 50M rows, 3GB
dim_promo = spark.read.parquet("s3://warehouse/dims/promotion/")  # 10K rows, 1MB
dim_channel = spark.read.parquet("s3://warehouse/dims/channel/")  # 20 rows, 1KB

# Strategy: Broadcast all small dims, shuffle-join large dim_customer
enriched = (sales_fact
    # Broadcast small dimensions (eliminates 5 shuffles of 200GB fact table!)
    .join(F.broadcast(dim_date), "date_key")
    .join(F.broadcast(dim_product), "product_key")
    .join(F.broadcast(dim_store), "store_key")
    .join(F.broadcast(dim_promo), "promo_key")
    .join(F.broadcast(dim_channel), "channel_key")
    # Shuffle join for large dimension (50M rows too big to broadcast)
    .join(dim_customer, "customer_key")
)

# Verify join strategies
enriched.explain(mode="formatted")
# Should show: 5 × BroadcastHashJoin + 1 × SortMergeJoin
```

### Performance Impact

| Approach | Shuffles | Data Moved | Duration |
|----------|----------|-----------|----------|
| All SortMergeJoin | 6 | 200GB × 6 = 1.2TB | 90 min |
| Broadcast small dims | 1 (customer only) | 200GB + 3GB = 203GB | 18 min |
| With bucketed customer | 0 (pre-partitioned) | ~0 (local join) | 8 min |

```python
# Further optimization: pre-bucket fact and customer for zero-shuffle join
sales_fact.write.bucketBy(256, "customer_key").sortBy("customer_key").saveAsTable("sales_bucketed")
dim_customer.write.bucketBy(256, "customer_key").sortBy("customer_key").saveAsTable("customer_bucketed")

# Bucketed join — no shuffle even for large dimension!
spark.conf.set("spark.sql.autoBucketedScan.enabled", "true")
sales_b = spark.table("sales_bucketed")
customer_b = spark.table("customer_bucketed")
result = sales_b.join(customer_b, "customer_key")  # No shuffle!
```

---

## Pattern 2: Config/Mapping Broadcast

**Problem:** An ETL pipeline processes 100 different data sources, each with its own field mapping, validation rules, and transformation config. Load configs once, broadcast to all tasks.

```python
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

# Load all source configurations
config_data = {
    "source_crm": {
        "field_mapping": {"cust_id": "customer_id", "fname": "first_name", "lname": "last_name"},
        "null_threshold": 0.05,  # Max 5% nulls per column
        "dedup_keys": ["customer_id", "email"],
        "transformations": {"email": "lower", "phone": "digits_only"},
    },
    "source_erp": {
        "field_mapping": {"order_num": "order_id", "amt": "amount", "dt": "order_date"},
        "null_threshold": 0.01,
        "dedup_keys": ["order_id"],
        "transformations": {"amount": "abs", "order_date": "parse_date"},
    },
    # ... 98 more sources
}

# Broadcast config to all executors
bc_config = spark.sparkContext.broadcast(config_data)

def process_source(source_name, raw_df):
    """Process a source using its broadcast config."""
    config = bc_config.value[source_name]
    
    # Apply field mapping
    mapped_df = raw_df
    for old_name, new_name in config["field_mapping"].items():
        if old_name in raw_df.columns:
            mapped_df = mapped_df.withColumnRenamed(old_name, new_name)
    
    # Apply transformations
    for field, transform_type in config["transformations"].items():
        if field in mapped_df.columns:
            if transform_type == "lower":
                mapped_df = mapped_df.withColumn(field, F.lower(F.col(field)))
            elif transform_type == "digits_only":
                mapped_df = mapped_df.withColumn(field, F.regexp_replace(F.col(field), r"[^\d]", ""))
            elif transform_type == "abs":
                mapped_df = mapped_df.withColumn(field, F.abs(F.col(field)))
            elif transform_type == "parse_date":
                mapped_df = mapped_df.withColumn(field, F.to_date(F.col(field)))
    
    # Deduplication
    mapped_df = mapped_df.dropDuplicates(config["dedup_keys"])
    
    return mapped_df

# Process each source
for source_name in config_data.keys():
    raw_df = spark.read.parquet(f"s3://raw-data/{source_name}/")
    processed = process_source(source_name, raw_df)
    processed.write.mode("overwrite").parquet(f"s3://curated/{source_name}/")
```

---

## Pattern 3: Blacklist/Whitelist Filtering

**Problem:** Filter 10 billion web requests against a 500K-entry IP blacklist and a 2M-entry URL whitelist. The lists update daily.

```python
from pyspark.sql import functions as F

# Load blacklist/whitelist
blocked_ips = set(
    spark.read.text("s3://security/blocked_ips.txt")
    .rdd.map(lambda r: r[0].strip()).collect()
)  # 500K IPs, ~10MB in memory

allowed_domains = set(
    spark.read.text("s3://security/allowed_domains.txt")
    .rdd.map(lambda r: r[0].strip()).collect()
)  # 2M domains, ~40MB in memory

# Broadcast both sets
bc_blocked_ips = spark.sparkContext.broadcast(blocked_ips)
bc_allowed_domains = spark.sparkContext.broadcast(allowed_domains)

# Method 1: UDF-based filtering (flexible but slower)
@F.udf("boolean")
def is_request_allowed(ip, url):
    if ip in bc_blocked_ips.value:
        return False
    # Extract domain from URL
    try:
        domain = url.split("/")[2] if url else ""
    except IndexError:
        domain = ""
    return domain in bc_allowed_domains.value

filtered = requests_df.filter(is_request_allowed(F.col("source_ip"), F.col("url")))

# Method 2: Broadcast join (faster — no UDF)
blocked_df = spark.createDataFrame([(ip,) for ip in blocked_ips], ["blocked_ip"])
allowed_df = spark.createDataFrame([(d,) for d in allowed_domains], ["allowed_domain"])

filtered = (requests_df
    # Remove blocked IPs (left_anti = keep rows that DON'T match)
    .join(F.broadcast(blocked_df), 
          requests_df.source_ip == blocked_df.blocked_ip, 
          "left_anti")
    # Keep only allowed domains
    .withColumn("domain", F.split(F.col("url"), "/")[2])
    .join(F.broadcast(allowed_df),
          F.col("domain") == allowed_df.allowed_domain,
          "inner")
    .drop("domain", "allowed_domain")
)
```

### Performance Comparison

```python
# Benchmark: 10 billion requests, 500K blacklist, 2M whitelist

# Approach 1: UDF-based (broadcast sets in UDF)
# Duration: 45 minutes
# Why slower: Python UDF serialization for each row

# Approach 2: Broadcast join (left_anti + inner)
# Duration: 12 minutes
# Why faster: JVM-native BroadcastHashJoin, no Python overhead

# Approach 3: Bloom filter (approximate but fastest)
from pyspark.sql.functions import xxhash64

# Pre-compute bloom filter on driver, broadcast bits
# Duration: 8 minutes (with ~0.1% false positive rate)
```

| Approach | Duration | Accuracy | Memory per Executor |
|----------|----------|----------|-------------------|
| UDF with broadcast set | 45 min | Exact | ~50MB |
| Broadcast join | 12 min | Exact | ~50MB |
| Bloom filter | 8 min | ~99.9% | ~5MB |

---

## Pattern 4: Production Monitoring and Best Practices

```python
# Monitor broadcast health in production
def check_broadcast_health(spark):
    """Monitor broadcast variables and warn about issues."""
    sc = spark.sparkContext
    
    # Check driver memory usage
    import psutil
    driver_memory = psutil.virtual_memory()
    if driver_memory.percent > 80:
        print(f"WARNING: Driver memory at {driver_memory.percent}% — broadcasts may fail")
    
    # Check number of active broadcasts
    # (Spark doesn't expose this directly, but we can track)
    return {
        "driver_memory_used_pct": driver_memory.percent,
        "driver_memory_available_mb": driver_memory.available / 1024 / 1024,
    }

# Best practices for broadcast management
class BroadcastManager:
    """Manage broadcast variables with lifecycle control."""
    
    def __init__(self, sc):
        self.sc = sc
        self._broadcasts = {}
    
    def get_or_create(self, name, data_func, max_age_seconds=3600):
        """Get existing broadcast or create new one."""
        import time
        
        if name in self._broadcasts:
            bc, created_at = self._broadcasts[name]
            if time.time() - created_at < max_age_seconds:
                return bc
            else:
                # Stale — refresh
                bc.unpersist(blocking=True)
        
        data = data_func()
        bc = self.sc.broadcast(data)
        self._broadcasts[name] = (bc, time.time())
        return bc
    
    def cleanup_all(self):
        """Destroy all managed broadcasts."""
        for name, (bc, _) in self._broadcasts.items():
            try:
                bc.destroy()
            except Exception:
                pass
        self._broadcasts.clear()

# Usage
manager = BroadcastManager(sc)
bc_products = manager.get_or_create("products", lambda: load_products())
bc_config = manager.get_or_create("config", lambda: load_config(), max_age_seconds=300)
```

---

## Interview Tips

> **Tip 1:** "Design a star schema join strategy." — "Broadcast all dimension tables that fit in memory (typically date, product, store, promotion — usually under 100MB each). For large dimensions like customer (50M+ rows), use SortMergeJoin or better yet, bucket both fact and dimension on the join key to eliminate the shuffle entirely. The goal: minimize the number of times the fact table is shuffled. Each broadcast join eliminates one full shuffle of the fact table."

> **Tip 2:** "How do you decide between broadcast join and shuffle join for a 500MB table?" — "Consider three factors: executor memory (500MB per executor is significant — with 50 executors that's 25GB total), driver memory (driver must collect the table first), and join frequency (if this join happens once vs repeatedly in the pipeline). For a one-time join with adequate memory, broadcast saves significant shuffle time. If memory is tight or the table keeps growing, invest in bucketing for zero-cost joins long-term."

> **Tip 3:** "How do you handle updating broadcast data that changes daily?" — "Broadcast variables are immutable within a Spark application. For daily updates: in batch jobs, simply restart with fresh data. For long-running streaming jobs, use a BroadcastManager that tracks creation time and refreshes stale broadcasts by unpersisting the old one and creating a new one. Alternatively, use foreachBatch to reload the dimension table from storage each micro-batch — it's slightly less efficient but always fresh."
