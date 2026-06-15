---
title: "Interview Practice Problems — Scenario Questions"
topic: system-design
subtopic: interview-practice-problems
content_type: scenario_question
tags: [system-design, interview, scenarios, practice]
---

# Interview Practice Problems — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Design a Simple Analytics Pipeline

**Scenario:** Your startup has a PostgreSQL database with an `orders` table (100K rows/day). Your CEO wants a dashboard showing daily revenue by product category, updated every morning. Design the pipeline.

<details>
<summary>✅ Solution</summary>

**Step 1: Clarify requirements**
- Freshness: daily by 7 AM is acceptable (batch)
- Volume: 100K rows/day × ~500 bytes = ~50 MB/day (tiny)
- Analysts: 3 people, using Tableau
- Budget: startup, keep it cheap

**Step 2: Simple batch architecture**

```
PostgreSQL → nightly Airflow extract → S3 → dbt → Redshift Serverless → Tableau
```

**Step 3: Implementation**

```python
# Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd, boto3
from datetime import datetime, timedelta

def extract_orders(**context):
    pg = PostgresHook("postgres_prod")
    yesterday = context["ds"]  # YYYY-MM-DD
    df = pg.get_pandas_df(f"""
        SELECT order_id, category, amount, created_at
        FROM orders
        WHERE DATE(created_at) = '{yesterday}'
    """)
    s3 = boto3.client("s3")
    df.to_parquet(f"/tmp/orders_{yesterday}.parquet")
    s3.upload_file(f"/tmp/orders_{yesterday}.parquet",
                   "analytics-bucket", f"raw/orders/dt={yesterday}/data.parquet")

with DAG("daily_orders", schedule_interval="0 2 * * *",  # 2 AM daily
         start_date=datetime(2024,1,1), catchup=False) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_orders)
```

```sql
-- dbt model: models/revenue_by_category.sql
SELECT
    DATE(created_at) AS sale_date,
    category,
    SUM(amount) AS total_revenue,
    COUNT(order_id) AS order_count
FROM {{ source('raw', 'orders') }}
WHERE DATE(created_at) = '{{ var("run_date") }}'
GROUP BY 1, 2
```

**Step 4: SLA monitoring**

Add an Airflow `SLA` on the dbt task: `sla=timedelta(hours=5)` — alerts if not done by 7 AM.

**Cost:** ~$50/month (Redshift Serverless at startup scale).

**Key lesson:** Don't over-engineer. A daily Airflow DAG + dbt + Redshift Serverless is the right answer for this scale. Adding Kafka or Spark would be wasteful.

</details>

</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Design a CDC Pipeline from MySQL to a Lakehouse

**Scenario:** Your company has 30 MySQL tables (orders, customers, products, inventory) in a production database. A data science team needs near-real-time access (< 10 minutes lag) to these tables in a lakehouse for ML feature engineering. The MySQL server is already at 80% CPU. How do you design this?

<details>
<summary>✅ Solution</summary>

**Step 1: Requirements**
- Latency: < 10 minutes (near-real-time, not seconds)
- Source impact: zero additional load on MySQL primary (it's at 80% CPU)
- Correctness: must handle updates and deletes, not just inserts
- Schema changes: column additions should not break the pipeline

**Step 2: Architecture**

```
MySQL Primary (prod) → MySQL Replica (read replica)
                              │
                              ▼
                    [Debezium Connector]
                    (reads binary log from replica)
                              │
                              ▼
                    [Kafka] — 30 topics, one per table
                    mysql.ecommerce.orders
                    mysql.ecommerce.customers
                              │
                              ▼
                    [Spark Structured Streaming]
                    (reads Kafka, applies MERGE to Delta Lake)
                              │
                              ▼
                    [Delta Lake on S3]
                    s3://lake/curated/orders/
                    s3://lake/curated/customers/
```

**Why replica, not primary?** Debezium reads the MySQL binary log. Even read-only binary log reading adds I/O pressure. With primary at 80% CPU, we MUST use a replica.

**Step 3: Debezium Configuration**

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql-replica.internal",
  "database.port": "3306",
  "database.user": "debezium_ro",
  "database.include.list": "ecommerce",
  "snapshot.mode": "initial",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,ts_ms,before"
}
```

**Step 4: Delta Lake Upsert**

```python
from delta.tables import DeltaTable

def upsert_to_delta(micro_batch_df, epoch_id, table_path, pk_col):
    # Filter out schema change events
    data_df = micro_batch_df.filter(micro_batch_df["__op"].isin(["c","u","d","r"]))
    
    if not DeltaTable.isDeltaTable(spark, table_path):
        # First run: create table
        data_df.filter(data_df["__op"] != "d") \
               .drop("__op", "__ts_ms") \
               .write.format("delta").save(table_path)
        return
    
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.alias("t").merge(
        data_df.alias("s"),
        f"t.{pk_col} = s.{pk_col}"
    ).whenMatchedDelete(
        condition="s.__op = 'd'"
    ).whenMatchedUpdateAll(
        condition="s.__op = 'u'"
    ).whenNotMatchedInsertAll(
        condition="s.__op != 'd'"
    ).execute()
```

**Step 5: Schema evolution**

```python
# Enable schema evolution in Delta writes
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# Debezium wraps schema in envelope — new columns appear automatically
# Delta MERGE + autoMerge adds the new column transparently
```

**Step 6: Monitoring**

- Alert if Kafka consumer lag > 50,000 messages (indicates streaming job fell behind)
- Alert if any Delta table's max(ts_ms) > 10 minutes old (freshness SLA breach)
- Track MERGE operation metrics: inserts/updates/deletes per micro-batch

**Trade-off discussion:**
- Debezium vs Airbyte CDC: Debezium is more configurable, Airbyte is easier to operate
- Delta MERGE vs overwrite: MERGE handles updates/deletes; overwrite would re-read entire MySQL table each run

</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Real-Time Fraud Detection System

**Scenario:** A payment processor (50,000 TPS peak) needs real-time fraud detection. Each transaction must receive a BLOCK/ALLOW decision in < 200ms. The fraud rate is 0.05% and false positives cost more (chargebacks + customer churn) than false negatives. The ML team retrains the model daily. Design the full system.

<details>
<summary>✅ Solution</summary>

**Step 1: Clarify requirements**
- Throughput: 50K TPS peak, 20K avg
- Decision latency: < 200ms P99 end-to-end
- False positive rate target: < 0.1% (FP hurts revenue)
- Recall target: > 80% (catch 80% of actual fraud)
- Explainability: required for regulatory compliance
- Model retraining: daily, deployed with < 5 min downtime

**Step 2: Capacity estimation**

```
Events/day: 50K/sec × 86,400 = 4.32 billion
Feature store (Redis): 50M active users × ~2 KB features = 100 GB (fits in Redis cluster)
Model replicas needed: 50K TPS, model inference = 2ms → 50K × 0.002s = 100 CPU-cores → 50 replicas (2 cores each)
Kafka throughput: 50K events × 1 KB = 50 MB/sec (easily handled by 6-partition topic)
```

**Step 3: Full architecture**

```
Transaction API
      │  (synchronous path, < 200ms budget)
      ▼
[Feature Fetcher Service]  — Go microservice, async Redis pipeline
  - User velocity (tx count last 1min/5min/1hr)
  - Merchant risk score (fraud rate last 30d)
  - Device fingerprint (seen before? registration date)
  - Geographic distance from last transaction
      │
      ▼
[Model Serving — Triton Inference Server]
  - XGBoost ensemble (3 models, averaged)
  - Inference: ~2ms per request
  - Horizontal scaling: 50 pods, HPA on queue depth
      │
      ▼
[Decision Engine]
  - Hard rules (always apply: sanction list, velocity hard caps)
  - Model score threshold: > 0.85 = BLOCK, 0.55–0.85 = REVIEW, < 0.55 = ALLOW
      │
      ├─── BLOCK → return 402 to payment API
      ├─── REVIEW → async: publish to Kafka review queue → analyst queue
      └─── ALLOW → return 200

Async path (Kafka):
  All transactions → Kafka fraud-events topic
      │
  [Flink Stream Processor]
      - Update Redis velocity counters
      - Publish labeled outcomes (confirmed fraud) to Delta Lake
      │
  [Delta Lake] → daily Spark training job → new XGBoost model
```

**Step 4: Latency budget breakdown**

```
Network (client → API):          10ms
Feature fetch (Redis pipeline):  10ms  [3 Redis lookups, pipelined = 1 RTT]
Model inference (Triton):         5ms
Decision + response:              5ms
Network overhead:                20ms
Total P50:                       ~50ms
Total P99:                      ~150ms  ← within 200ms SLA
```

**Step 5: Feature engineering (Flink velocity tracking)**

```java
// Sliding window velocity counter updated in Flink
// Writes atomically to Redis using MULTI/EXEC
public class VelocityUpdater extends KeyedProcessFunction<String, Transaction, Void> {
    // State: count per time bucket
    private MapState<Long, Integer> bucketCounts;
    
    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Void> out) {
        long bucket = tx.getTimestamp() / 60_000; // 1-minute buckets
        bucketCounts.put(bucket, bucketCounts.get(bucket) + 1);
        
        // Write aggregated velocities to Redis
        Map<String, String> features = new HashMap<>();
        features.put("count_1m", String.valueOf(sumBuckets(1)));
        features.put("count_5m", String.valueOf(sumBuckets(5)));
        features.put("count_60m", String.valueOf(sumBuckets(60)));
        
        redisClient.hset("user:" + tx.getUserId() + ":velocity", features);
        redisClient.expire("user:" + tx.getUserId() + ":velocity", 7200);
    }
}
```

**Step 6: Model deployment (zero-downtime)**

```yaml
# Kubernetes rolling deployment for Triton model server
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 25%          # Spin up new pods before killing old
    maxUnavailable: 0%     # Never reduce capacity during rollout

# Canary via Istio: 5% traffic to new model version
# Monitor: FP rate, recall, p99 latency for 30 minutes before full rollout
# Rollback trigger: FP rate > 0.15% OR recall < 75%
```

**Step 7: Explainability for compliance**

```python
import shap

# SHAP values generated during inference and stored per decision
explainer = shap.TreeExplainer(fraud_model)
shap_values = explainer.shap_values(feature_vector)

explanation = {
    "transaction_id": tx_id,
    "fraud_score": float(score),
    "decision": decision,
    "top_factors": sorted(
        zip(feature_names, shap_values[0]),
        key=lambda x: abs(x[1]), reverse=True
    )[:5]  # Top 5 contributing features
}
# Stored in audit table: investigators can see "why" for any blocked tx
```

**Step 8: Failure modes and mitigations**

| Failure | Impact | Mitigation |
|---|---|---|
| Redis cluster down | Can't fetch features | Fallback: rules-only mode (hard caps only), ALLOW with async review |
| Model serving pods crash | No inference | Circuit breaker: default ALLOW with high-risk flag, alert oncall |
| Kafka consumer lag | Velocity counters stale | Alert at lag > 10K, auto-scale Flink task managers |
| False positive spike | Block legitimate customers | Automated rollback if FP rate > threshold in 5-minute window |
| Training data drift | Model degrades silently | Daily PSI (Population Stability Index) check, alert if > 0.2 |

**Trade-offs discussion points:**

1. **XGBoost vs Neural Network:** XGBoost wins for this use case — 2ms inference vs 15–50ms for a neural net, plus SHAP explainability.

2. **Redis vs DynamoDB for features:** Redis is 10× faster (sub-ms) but more expensive and harder to operate at 100 GB scale. DynamoDB is managed but 5–10ms latency. For a 200ms budget with 10ms allocated to features, either works — choose based on ops maturity.

3. **Synchronous vs async decision:** Synchronous (in the payment API call) is required for real-time BLOCK decisions. Async Kafka path is only for analytics and model training — never for the blocking decision itself.

4. **At-least-once vs exactly-once:** The Kafka→Flink→Redis path is at-least-once (velocity counters may over-count by 1 on reprocessing). This is acceptable — a small over-count on velocity makes the system slightly more conservative (safer for fraud detection).

</details>

</article>
