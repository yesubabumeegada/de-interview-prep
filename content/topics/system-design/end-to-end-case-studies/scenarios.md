---
title: "End-to-End Case Studies — Scenarios"
topic: system-design
subtopic: end-to-end-case-studies
content_type: scenario_question
tags: [system-design, case-study, end-to-end, scenarios]
---

# End-to-End Case Studies — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Design a Simple Sales Analytics Pipeline

**Scenario:** A mid-sized e-commerce company wants daily sales reporting: total revenue by product category, top 10 products by units sold, and daily active customers. Data lives in a PostgreSQL transactional database. Design a simple end-to-end pipeline.

<details>
<summary>💡 Hint</summary>

Think source → ingest → transform → serve. For daily reporting, a batch pipeline is appropriate. Use a tool like Airbyte or Fivetran for ingestion, dbt for transformation, and a BI tool for serving.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**
```
PostgreSQL → Airbyte → Snowflake (raw) → dbt → Snowflake (analytics) → Tableau
```

**Step 1: Ingestion with Airbyte**
- Configure Airbyte PostgreSQL source connector
- Destination: Snowflake raw schema
- Sync mode: Incremental (append new rows using `updated_at`)
- Schedule: Every hour

**Step 2: dbt Transformations**

```sql
-- models/silver/orders.sql
SELECT
    o.order_id,
    o.customer_id,
    o.created_at::DATE AS order_date,
    oi.product_id,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_revenue
FROM {{ source('raw', 'orders') }} o
JOIN {{ source('raw', 'order_items') }} oi ON o.order_id = oi.order_id
JOIN {{ source('raw', 'products') }} p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
```

```sql
-- models/gold/daily_sales_summary.sql
SELECT
    order_date,
    category,
    SUM(line_revenue) AS total_revenue,
    SUM(quantity) AS total_units,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM {{ ref('orders') }}
GROUP BY 1, 2
```

**Step 3: Orchestration with Airflow**

```python
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

with DAG('sales_analytics', schedule_interval='0 6 * * *') as dag:
    sync = AirbyteTriggerSyncOperator(
        task_id='sync_postgres_to_snowflake',
        airbyte_conn_id='airbyte',
        connection_id='postgres-snowflake-connection'
    )

    transform = DbtCloudRunJobOperator(
        task_id='run_dbt_transformations',
        dbt_cloud_conn_id='dbt_cloud',
        job_id=12345
    )

    sync >> transform
```

**Step 4: BI in Tableau**
- Connect Tableau to Snowflake analytics schema
- Dashboard: Revenue by category (bar chart), Top 10 products (table), DAU trend (line chart)
- Schedule extract refresh daily at 7am

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Real-Time Fraud Detection Pipeline

**Scenario:** A payments company processes 5,000 transactions per second. They need to flag potentially fraudulent transactions within 500ms of receipt. Design an end-to-end real-time pipeline from ingestion through scoring to action.

<details>
<summary>💡 Hint</summary>

500ms end-to-end means every component must be low-latency. Architecture: Kafka → Flink (feature computation) → Redis (feature store lookup) → ML model (online inference) → action (block/flag). Batch historical features pre-computed and stored in Redis.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**
```
Payment API → Kafka → Flink → Feature Store (Redis) → ML Inference → Decision
                                     ↑
                              Spark batch job
                           (historical features)
```

**Latency Budget:**
| Component | Budget |
|-----------|--------|
| Kafka publish | 5ms |
| Flink processing | 50ms |
| Redis lookup | 5ms |
| ML inference | 30ms |
| Decision + response | 10ms |
| **Total** | **100ms** (well under 500ms) |

**Flink Job:**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(100);
env.enableCheckpointing(10_000); // 10s checkpoints

DataStream<Transaction> transactions = env
    .addSource(new FlinkKafkaConsumer<>("payments", schema, props));

// Real-time features: velocity checks
DataStream<EnrichedTransaction> enriched = transactions
    .keyBy(Transaction::getCardId)
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
    .aggregate(new VelocityFeatureAggregator());  // count, sum in window

// Async Redis lookup for historical features
DataStream<ScoredTransaction> scored = AsyncDataStream.unorderedWait(
    enriched,
    new AsyncRedisFeatureLookup(redisPool),
    200, TimeUnit.MILLISECONDS  // timeout
);
```

**Redis Feature Store:**
```python
# Batch job: compute 30-day historical features, push to Redis
def update_historical_features(card_id: str):
    features = spark.sql(f"""
        SELECT
            count(*) as txn_count_30d,
            avg(amount) as avg_amount_30d,
            stddev(amount) as stddev_amount_30d,
            count(DISTINCT merchant_country) as countries_30d
        FROM transactions
        WHERE card_id = '{card_id}'
          AND txn_date >= current_date - 30
    """).collect()[0]

    r.hset(f"features:{card_id}", mapping={
        "txn_count_30d": features.txn_count_30d,
        "avg_amount_30d": features.avg_amount_30d,
        "stddev_amount_30d": features.stddev_amount_30d,
        "countries_30d": features.countries_30d
    })
    r.expire(f"features:{card_id}", 86400)  # 24h TTL
```

**ML Inference Service:**
```python
from fastapi import FastAPI
import mlflow.pyfunc

app = FastAPI()
model = mlflow.pyfunc.load_model("models:/fraud_detector/production")

@app.post("/score")
async def score_transaction(features: dict):
    score = model.predict([features])[0]
    decision = "BLOCK" if score > 0.85 else "FLAG" if score > 0.6 else "ALLOW"
    return {"score": float(score), "decision": decision}
```

**Decision Actions:**
```python
def handle_decision(transaction_id: str, decision: str):
    if decision == "BLOCK":
        # Synchronous: block the payment
        payment_service.decline(transaction_id)
        alert_cardholder(transaction_id)
    elif decision == "FLAG":
        # Async: allow but queue for review
        review_queue.publish(transaction_id)
    # ALLOW: no action needed
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Global Data Platform for a Multinational Retailer

**Scenario:** A retailer operates in 40 countries with $20B annual revenue. They need a unified data platform that: handles 50TB/day ingestion from 200 source systems, supports local data residency laws (EU GDPR, China PIPL), provides sub-second BI for regional managers, and enables global consolidated reporting for headquarters.

<details>
<summary>💡 Hint</summary>

Data residency prevents centralizing all data — you need a federated architecture with regional data planes. Global reporting needs aggregates, not raw personal data. Design: regional lakehouse per jurisdiction + global metadata plane + aggregation-only cross-region flows.

</details>

<details>
<summary>✅ Solution</summary>

**Federated Regional Architecture:**

```
HQ (US)          EU Region         APAC Region       China Region
────────         ─────────         ───────────       ────────────
Global           EU Data           APAC Data         China Data
Aggregate        Plane             Plane             Plane
Lakehouse        (Frankfurt)       (Singapore)       (Beijing)
    ↑                ↓                 ↓                 ↓
Global Rollup    EU Lakehouse     APAC Lakehouse    CN Lakehouse
    ←────────────────────────────────────────────────────────
                 Aggregates only (no PII crosses borders)
```

**Regional Data Plane (per jurisdiction):**

Each region runs identical infrastructure:
```python
# Terraform module — instantiated per region
module "regional_data_plane" {
  source = "./modules/data-plane"

  region       = "eu-west-1"  # or ap-southeast-1, cn-north-1
  jurisdiction = "EU"

  # Each region gets its own:
  # - S3/OSS/Azure ADLS bucket (data never leaves)
  # - Iceberg catalog (Glue/HMS)
  # - Spark cluster (EMR/Databricks)
  # - Trino cluster
  # - Unity Catalog workspace
}
```

**Data Residency Enforcement:**

```python
# Data classification at ingestion
RESIDENCY_RULES = {
    "EU": {
        "pii_fields": ["customer_name", "email", "address", "ip_address"],
        "must_stay_in": ["eu-west-1", "eu-central-1"],
        "law": "GDPR"
    },
    "CN": {
        "pii_fields": ["customer_name", "phone", "id_number"],
        "must_stay_in": ["cn-north-1", "cn-northwest-1"],
        "law": "PIPL"
    }
}

def ingest_with_residency(df, jurisdiction: str):
    rules = RESIDENCY_RULES[jurisdiction]

    # Write full data (including PII) stays in region
    df.write.format("iceberg")         .saveAsTable(f"{jurisdiction.lower()}_catalog.silver.customers")

    # Create anonymized aggregate for cross-border export
    aggregate = df         .drop(*rules["pii_fields"])         .groupBy("country", "product_category", "order_date")         .agg(sum("revenue").alias("revenue"), count("*").alias("order_count"))

    return aggregate  # Safe to send to HQ
```

**Global Reporting Layer:**

```python
# HQ: receive aggregates from all regions
def consolidate_global_revenue(report_date: str):
    regional_aggregates = []

    for region in ["EU", "APAC", "CN", "NA"]:
        # Each region sends pre-aggregated, anonymized data
        agg = spark.read.format("iceberg")             .load(f"s3://hq-global/regional-aggregates/{region}/{report_date}/")
        regional_aggregates.append(agg)

    global_summary = reduce(lambda a, b: a.union(b), regional_aggregates)         .groupBy("product_category", "order_date")         .agg(sum("revenue").alias("global_revenue"),
             sum("order_count").alias("global_orders"))

    global_summary.write.format("iceberg")         .saveAsTable("hq.gold.global_revenue_summary")
```

**Sub-Second BI for Regional Managers:**

```sql
-- Pre-aggregate key metrics into materialized views (Trino)
-- Refresh every 15 minutes

CREATE OR REPLACE VIEW eu_catalog.gold.regional_kpis AS
SELECT
    store_id,
    product_category,
    DATE_TRUNC('hour', sale_time) AS hour_bucket,
    SUM(revenue) AS hourly_revenue,
    COUNT(*) AS transaction_count
FROM eu_catalog.silver.pos_transactions
WHERE sale_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY 1, 2, 3;

-- BI tool queries materialized view (<100ms)
```

**Governance and Audit:**

```python
# Cross-region data transfer audit log
def log_cross_border_transfer(source_region, dest_region, table, row_count):
    audit_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "source_region": source_region,
        "destination_region": dest_region,
        "table": table,
        "row_count": row_count,
        "data_type": "aggregate_only",  # confirm no PII
        "legal_basis": "legitimate_interest_consolidated_reporting"
    }
    # Write to immutable audit trail (WORM S3)
    audit_bucket.put_object(
        Key=f"transfers/{datetime.utcnow().date()}/{uuid4()}.json",
        Body=json.dumps(audit_entry)
    )
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you start a system design interview for a data pipeline?" — Clarify requirements first: data volume, latency SLA (batch vs streaming), SLA for availability, who are the consumers, and what counts as success. Then sketch the high-level flow before diving into components.
> **Tip 2:** "How do you handle data residency in a global platform?" — Never send raw PII across jurisdictions. Compute aggregates within the jurisdiction and only export anonymized/aggregated data. Use separate storage accounts per jurisdiction and enforce via IAM, not application logic.
> **Tip 3:** "What is the most common scaling bottleneck in data pipelines?" — The shuffle in distributed joins. When joining large datasets, the shuffle redistributes all matching keys across the network. Mitigate with broadcast joins for small tables, partition-aware joins, and pre-bucketing/clustering on join keys.
