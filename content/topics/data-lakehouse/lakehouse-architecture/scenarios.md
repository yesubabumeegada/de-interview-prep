---
title: "Lakehouse Architecture — Scenarios"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: scenario_question
tags: [lakehouse, medallion, architecture, scenarios]
---

# Lakehouse Architecture — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Medallion Architecture Explained

**Scenario:** Your team is redesigning the data platform around a medallion architecture (Bronze/Silver/Gold). A new data engineer asks you to explain what each layer is, what transformations happen, and why this structure exists.

<details>
<summary>💡 Hint</summary>

Bronze = raw/immutable copy. Silver = cleansed, validated, conformed. Gold = business-ready aggregates and marts. The key is progressive data quality improvement and clear data lineage at each stage.

</details>

<details>
<summary>✅ Solution</summary>

**Medallion Architecture:**

```
Source Systems → [Bronze] → [Silver] → [Gold] → Consumers
                  Raw        Clean      Business
```

**Bronze Layer (Raw):**
- Exact copy of source data, never modified
- Append-only (new data never overwrites old)
- Retains all fields including bad data
- Format: Parquet or Delta with schema-on-read

```python
# Bronze: ingest raw, no transformation
raw_df = spark.read.json("s3://raw-data/orders/2024-01-15/")

raw_df     .withColumn("_ingested_at", current_timestamp())     .withColumn("_source_file", input_file_name())     .write.format("delta")     .partitionBy("_ingested_date")     .mode("append")     .save("s3://lakehouse/bronze/orders/")
```

**Silver Layer (Cleansed):**
- Data quality rules applied (nulls, deduplication, type casting)
- Schema enforcement (structured columns)
- Business key standardization
- One silver table per source entity (not aggregated)

```python
# Silver: clean and conform
bronze_df = spark.read.format("delta").load("s3://lakehouse/bronze/orders/")

silver_df = bronze_df     .dropDuplicates(["order_id"])     .filter(col("order_id").isNotNull())     .withColumn("amount", col("amount").cast("decimal(18,2)"))     .withColumn("order_date", to_date(col("order_timestamp")))     .select("order_id", "customer_id", "amount", "order_date", "status")

silver_df.write.format("delta")     .partitionBy("order_date")     .mode("overwrite")     .option("replaceWhere", "order_date = '2024-01-15'")     .save("s3://lakehouse/silver/orders/")
```

**Gold Layer (Business-Ready):**
- Aggregated, denormalized for specific use cases
- Named for business concepts (revenue, churn, KPIs)
- Optimized for BI tools and dashboards

```python
# Gold: business aggregate
silver_orders = spark.read.format("delta").load("s3://lakehouse/silver/orders/")
silver_customers = spark.read.format("delta").load("s3://lakehouse/silver/customers/")

gold_revenue = silver_orders     .join(silver_customers, "customer_id")     .groupBy("order_date", "region", "customer_segment")     .agg(
        sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers")
    )

gold_revenue.write.format("delta")     .mode("overwrite")     .saveAsTable("gold.daily_revenue_by_region")
```

**Why This Structure?**
1. **Auditability:** Bronze is the system of record — always traceable to source
2. **Reprocessability:** Silver and Gold can be recomputed from Bronze if logic changes
3. **Consumer isolation:** BI tools only access Gold; bad transformations don't corrupt raw data
4. **Quality gates:** Each layer has progressively higher quality guarantees

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Lakehouse vs Data Warehouse — When to Use Each

**Scenario:** Your company currently uses Snowflake as its primary data warehouse. The data science team wants to work with unstructured data (text, images) and run Python-based ML pipelines. Leadership is considering building a lakehouse. Should you replace Snowflake, augment it, or migrate entirely?

<details>
<summary>💡 Hint</summary>

This is not either/or. A common pattern is a hybrid architecture: lakehouse for raw/ML/unstructured data, data warehouse for governed, structured analytics. Consider where each excels and design the integration points.

</details>

<details>
<summary>✅ Solution</summary>

**Comparison:**

| Dimension | Data Warehouse (Snowflake) | Lakehouse (Iceberg on S3) |
|-----------|--------------------------|--------------------------|
| Data types | Structured SQL | Any (structured, semi, unstructured) |
| ML workloads | Limited (Snowpark) | Native (Spark, Python) |
| Cost model | Compute + storage (coupled) | Storage cheap, compute elastic |
| Query performance | Excellent (SQL) | Good (Trino/Spark SQL) |
| Data freshness | Minutes (ELT pipeline) | Seconds (streaming) |
| Governance | Strong | Requires additional tooling |
| Compliance | Built-in (SOC2, HIPAA) | Manual setup |

**Recommended: Hybrid Architecture**

```
Sources → [Lakehouse Bronze/Silver] → [Snowflake Gold/Marts]
               ↓                              ↓
         ML/DS Workloads              BI/Analytics/Dashboards
         (Spark, Python)              (Tableau, Looker)
```

**Integration Pattern — Iceberg → Snowflake:**

```sql
-- Snowflake can read Iceberg tables directly (Snowflake Iceberg Tables)
CREATE OR REPLACE ICEBERG TABLE snowflake_db.gold.orders
  CATALOG = 'glue_catalog'
  EXTERNAL_VOLUME = 's3_external_vol'
  CATALOG_TABLE_NAME = 'prod.silver.orders';

-- Or use Snowpipe for continuous ingestion from lakehouse
CREATE PIPE orders_pipe AS
COPY INTO snowflake_db.silver.orders
FROM @s3_stage/silver/orders/
FILE_FORMAT = (TYPE = 'PARQUET');
```

**ML Integration Pattern:**

```python
# Data scientists read from lakehouse, write features back
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read silver data from lakehouse
training_data = spark.table("prod.silver.customer_events")     .filter("event_date >= '2023-01-01'")     .toPandas()

# Train model
model = train_model(training_data)

# Write predictions back to lakehouse gold
predictions_df = spark.createDataFrame(predictions)
predictions_df.write.format("iceberg")     .mode("overwrite")     .saveAsTable("prod.gold.churn_predictions")

# Snowflake reads predictions for BI
# (via Iceberg catalog or Snowpipe)
```

**Decision Framework:**
- Keep Snowflake for: governed SQL analytics, BI tools, finance/compliance reporting
- Add lakehouse for: ML workloads, unstructured data, streaming ingestion, cost-sensitive storage
- Migrate to lakehouse-only if: need full Python flexibility, multi-cloud, or Snowflake costs are prohibitive

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Real-Time Lakehouse for a High-Frequency Trading Platform

**Scenario:** A fintech company processes 10M trade events per second across 50 markets. They need a lakehouse that supports: (1) real-time risk calculations within 100ms, (2) T+1 regulatory reporting with full audit trail, (3) historical backtesting over 10 years of tick data (500TB), and (4) ML feature store updated every 5 seconds. Design the complete architecture.

<details>
<summary>💡 Hint</summary>

These requirements span different latency tiers: ultra-low latency (100ms) needs a hot tier (Redis/in-memory), near-real-time (5s ML) needs streaming lakehouse (Flink + Iceberg), T+1 batch needs durability and compaction, and backtesting needs efficient range scans on 500TB.

</details>

<details>
<summary>✅ Solution</summary>

**Multi-Tier Lakehouse Architecture:**

```
Market Data Feeds (10M events/sec)
          │
    ┌─────▼──────┐
    │   Kafka    │ ← partitioned by market/symbol
    └─────┬──────┘
          │
   ┌──────┼──────────────────┐
   │      │                  │
   ▼      ▼                  ▼
Hot     Warm              Cold Tier
Tier    Tier
Redis   Flink→Iceberg    Spark batch
(100ms) (5s ML features)  (T+1 reports)
   │      │                  │
   │   Feature Store      Audit Trail
   │   (Iceberg MoR)      (Iceberg CoW)
   │                         │
   └──────────────────────────┘
                │
         Trino (Backtesting)
         over 500TB Iceberg
```

**Tier 1: Hot Path (100ms SLA)**

```python
# Redis Streams for ultra-low latency risk calculation
import redis

r = redis.Redis(host='redis-cluster', port=6379)

def process_trade_event(trade: dict):
    # Write to Redis Streams
    r.xadd('trades', {
        'symbol': trade['symbol'],
        'price': trade['price'],
        'quantity': trade['quantity'],
        'timestamp': trade['timestamp']
    })
    
    # Risk calculation reads last N events per symbol
    recent = r.xrevrange(f"trades:{trade['symbol']}", count=1000)
    risk_metric = calculate_var(recent)  # Value at Risk
    r.set(f"risk:{trade['symbol']}", risk_metric, ex=1)  # TTL 1s
```

**Tier 2: Warm Path — Flink → Iceberg (5s ML features)**

```java
// Flink job: compute ML features, write to Iceberg every 5s
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // 5s checkpoints

DataStream<TradeEvent> trades = env
    .addSource(new FlinkKafkaConsumer<>("trades", schema, kafkaProps));

// 5-second tumbling window for VWAP feature
DataStream<FeatureRow> features = trades
    .keyBy(TradeEvent::getSymbol)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .aggregate(new VWAPAggregator());

// Write to Iceberg MoR table (low write latency)
features.addSink(
    IcebergSink.forRow(rowType)
        .tableLoader(TableLoader.fromCatalog(catalogLoader, tableId))
        .writeParallelism(100)
        .build()
);
```

**Tier 3: Cold Path — T+1 Regulatory Reporting**

```python
# Spark batch job: nightly compaction + report generation
def generate_regulatory_report(trade_date: str):
    # Read from Iceberg (all events for the day)
    trades = spark.table("prod.bronze.market_trades")         .filter(f"trade_date = '{trade_date}'")
    
    # Validate completeness (regulatory requirement)
    expected_count = get_expected_trade_count(trade_date)
    actual_count = trades.count()
    assert actual_count >= expected_count * 0.999, "Missing trades!"
    
    # Generate MiFID II transaction report
    report = trades         .select("trade_id", "symbol", "price", "quantity",
                "trader_id", "counterparty", "venue")         .withColumn("report_timestamp", current_timestamp())
    
    # Write to immutable audit partition (never delete)
    report.write.format("iceberg")         .option("write.metadata.delete-after-commit.enabled", "false")         .partitionBy("trade_date")         .mode("overwrite")         .saveAsTable("prod.gold.regulatory_reports")
```

**Tier 4: Backtesting on 500TB**

```sql
-- Iceberg with Z-ordering on (symbol, timestamp) for efficient range scans
-- Query 10 years of data for a single symbol (milliseconds, not hours)

CALL prod.system.rewrite_data_files(
  table => 'prod.bronze.market_trades',
  strategy => 'sort',
  sort_order => 'zorder(symbol, trade_timestamp)'
);

-- Backtesting query: full history for one symbol
SELECT trade_timestamp, price, quantity
FROM prod.bronze.market_trades
WHERE symbol = 'AAPL'
  AND trade_timestamp BETWEEN '2014-01-01' AND '2024-01-01'
ORDER BY trade_timestamp;
-- With Z-order: scans ~0.5TB instead of 500TB
```

**Audit Trail Design (7-year retention, immutable):**

```python
audit_options = {
    # Never auto-expire snapshots
    'write.metadata.delete-after-commit.enabled': 'false',
    'history.expire.max-snapshot-age-ms': str(7 * 365 * 24 * 60 * 60 * 1000),
    # Write-once, read-many S3 compliance mode
    's3.object-lock': 'COMPLIANCE',
}
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a data lake and a lakehouse?" — A data lake is object storage with raw files and no ACID guarantees. A lakehouse adds a transactional metadata layer (Iceberg/Delta/Hudi) on top of the lake, providing SQL semantics, ACID transactions, schema enforcement, and time travel — combining the flexibility of a lake with the reliability of a warehouse.
> **Tip 2:** "How do you handle late-arriving data in a lakehouse?" — Use event-time partitioning and allow late writes to historical partitions (Iceberg/Delta support this). For Silver/Gold layers, use `MERGE INTO` to upsert late records. Alert on lateness using watermarks in streaming jobs.
> **Tip 3:** "What is the role of a feature store in a lakehouse?" — A feature store manages ML features: versioned, shared across models, with point-in-time correct lookups (no data leakage). In a lakehouse, features are computed by Flink/Spark, stored in Iceberg (low-latency serving via MoR), and joined to training datasets using time-travel snapshots.
