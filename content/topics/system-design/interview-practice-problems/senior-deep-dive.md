---
title: "Interview Practice Problems — Senior Deep Dive"
topic: system-design
subtopic: interview-practice-problems
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, interview, practice, data-engineering, senior]
---

# Interview Practice Problems — Senior Deep Dive

## Problem 6: Design a Real-Time Fraud Detection System

**Prompt:** Design a real-time fraud detection system for a payment processor that handles 50,000 transactions/sec. Decisions must be made in < 200ms.

### Step 1: Requirements Clarification

- **Throughput:** 50K TPS peak
- **Decision latency:** < 200ms P99 end-to-end
- **False positive tolerance:** < 0.1% (blocking legitimate transactions is costly)
- **Model freshness:** retrain daily on last 30 days of labeled data
- **Explainability:** auditors need to understand why a transaction was flagged
- **Consistency:** eventual for analytics, strong for the block/allow decision

### Step 2: Capacity Estimation

```
Transactions/day: 50K/sec × 86,400 = 4.32B
Storage (1 KB/tx): 4.32 TB/day raw
Feature vector size: ~200 features × 4 bytes = ~800 bytes
Redis feature cache: 100M active users × 1 KB = 100 GB (fits in Redis cluster)
Model inference: 1ms per prediction (XGBoost/LightGBM), 200 replicas for 50K TPS
```

### Step 3: High-Level Design

```
                    ┌─────────────────────────────┐
Payment API ──────► │      Kafka (fraud-tx topic)  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   Flink Stream Processor     │
                    │  (feature engineering)       │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   Feature Enrichment Layer   │
                    │  Redis: user velocity         │
                    │  Redis: merchant risk score   │
                    │  Redis: device fingerprint    │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   Model Serving (Triton /    │
                    │   TorchServe / BentoML)      │
                    │   XGBoost ensemble model     │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   Decision Engine            │
                    │   (rules + model score)      │
                    └──────────────┬──────────────┘
                                   │
                         BLOCK / ALLOW / REVIEW
```

### Step 4: Detailed Design

#### Feature Engineering (Flink)

```java
// Velocity features: count of transactions in last 1min, 5min, 1hr per user
DataStream<Transaction> txStream = ...;

txStream
  .keyBy(Transaction::getUserId)
  .process(new VelocityFeatureFunction(
      Arrays.asList(
          Time.minutes(1),
          Time.minutes(5),
          Time.hours(1),
          Time.hours(24)
      )
  ))
  .addSink(redisSink); // Write velocity features to Redis
```

**Features computed:**
| Feature | Type | Window |
|---|---|---|
| tx_count_1m | velocity | 1 min per user |
| tx_amount_sum_1h | velocity | 1 hour per user |
| merchant_fraud_rate_30d | historical | 30 day rolling |
| device_seen_before | boolean | lifetime |
| geo_distance_from_last_tx | real-time | instant |
| hour_of_day | temporal | none |
| amount_deviation_from_avg | statistical | 90 day |

#### Redis Feature Store

```python
# Feature retrieval: must complete in < 10ms
import redis

r = redis.Redis(cluster=True, host="redis-cluster.internal")

def get_features(user_id: str, tx: dict) -> dict:
    pipe = r.pipeline()
    pipe.hgetall(f"user:{user_id}:velocity")
    pipe.hgetall(f"merchant:{tx['merchant_id']}:risk")
    pipe.get(f"device:{tx['device_id']}:seen")
    results = pipe.execute()  # Single round-trip via pipeline
    
    return {
        "tx_count_1m": int(results[0].get("count_1m", 0)),
        "merchant_fraud_rate": float(results[1].get("fraud_rate_30d", 0.0)),
        "device_known": results[2] is not None,
        **tx  # raw transaction fields
    }
```

#### Model Serving

```python
# Triton ensemble: feature preprocessing + XGBoost inference
# model_config.pbtxt
name: "fraud_ensemble"
platform: "ensemble"
ensemble_scheduling {
  step { model_name: "feature_transform" model_version: 1
         input_map { key: "raw" value: "raw" }
         output_map { key: "features" value: "features" } }
  step { model_name: "xgboost_fraud" model_version: 1
         input_map { key: "features" value: "features" }
         output_map { key: "fraud_score" value: "fraud_score" } }
}
```

**Model selection rationale:**
- **XGBoost/LightGBM**: 1–3ms inference, interpretable (SHAP values for explainability)
- **Not deep learning**: 10–50ms inference too slow for 200ms budget
- **Ensemble of 3 models**: reduces variance, one model can fail silently

#### Decision Engine

```python
def make_decision(fraud_score: float, tx: dict) -> str:
    # Hard rules (always apply, before model)
    if tx["amount"] > 50000 and tx["country"] != tx["user_home_country"]:
        return "BLOCK"  # Rule-based hard block
    
    # Model-based thresholds (tuned to 0.1% FP rate)
    if fraud_score >= 0.85:
        return "BLOCK"
    elif fraud_score >= 0.55:
        return "REVIEW"  # Manual review queue
    else:
        return "ALLOW"
```

#### Latency Budget

```
Kafka produce → consume:  5ms
Feature fetch (Redis):    10ms
Model inference:          5ms
Decision + response:      5ms
Network overhead:         20ms
Total:                   ~45ms P50, ~150ms P99
```

#### Feedback Loop (Model Retraining)

```
Labeled fraud data → Delta Lake (fraud_labels table)
Daily batch Spark job:
  - Join transactions with fraud labels (confirmed within 30 days)
  - Feature engineering (same logic as online, run on Spark)
  - Train XGBoost with class_weight to handle 0.1% fraud rate imbalance
  - A/B test new model on 5% traffic via feature flag
  - Promote if recall improves without FP rate degradation
```

### Step 5: Trade-offs and Failure Modes

| Concern | Design Decision | Reasoning |
|---|---|---|
| Model stale features | Redis TTL = 24h + background refresh | Prevents stale velocity counts |
| Redis failure | Fallback: rule-only mode (no model) | Better safe than no decisions |
| Kafka consumer lag | Alert at > 10K backlog, auto-scale consumers | Latency SLA breach |
| False negative fraud | Daily retraining + label feedback loop | Model adapts to new patterns |
| Explainability | SHAP values stored per decision | Audit requirement met |
| Model versioning | Canary deploy: 5% traffic, monitor metrics | Zero-downtime model updates |

---

## Problem 7: Design a Petabyte-Scale Data Warehouse

**Prompt:** Design a data warehouse for a company with 5 PB of data, 500 analysts, and 10,000 queries/day.

### Architecture

```
[Sources: S3, Kafka, DBs]
         │
         ▼
[Ingestion: Fivetran + custom Spark jobs]
         │
         ▼
[Raw Layer: S3 (Delta Lake)] ← append-only, schema-on-read
         │
         ▼
[Curated Layer: Redshift / BigQuery / Snowflake] ← columnar, clustered
         │
    ┌────┴──────┐
    ▼           ▼
[Gold Layer]  [Data Marts]  ← pre-aggregated for BI
    │
    ▼
[BI: Tableau, Looker, Superset]
```

### Partitioning and Clustering Strategy

```sql
-- Redshift: partition by date, sort key by user_id + event_type
CREATE TABLE events (
    event_id    BIGINT,
    user_id     BIGINT,
    event_type  VARCHAR(50),
    event_ts    TIMESTAMP,
    amount      DECIMAL(18,4)
)
DISTSTYLE KEY DISTKEY(user_id)  -- collocate joins on user_id
SORTKEY(event_ts, event_type);  -- most queries filter on date + type

-- Partition elimination at query time:
-- WHERE event_ts BETWEEN '2024-01-01' AND '2024-01-31'
-- → scans only January blocks (sorted by event_ts)
```

**BigQuery equivalent:**
```sql
CREATE TABLE events
PARTITION BY DATE(event_ts)
CLUSTER BY user_id, event_type
OPTIONS(require_partition_filter = true);  -- Prevent full-table scans
```

### Cost Control at 5 PB Scale

| Strategy | Mechanism | Savings |
|---|---|---|
| Partition pruning | Require date filter in queries | 80–95% less data scanned |
| Materialized views | Pre-aggregate daily rollups | 10–100× query speedup |
| Result caching | Snowflake / BigQuery cache identical queries | 0 cost for repeated queries |
| Storage tiering | Move data > 1 year to S3 + Athena | 10× storage cost reduction |
| Workload isolation | Separate compute clusters per team | Prevent runaway queries |

### Concurrency at 10,000 Queries/Day

```
10,000 queries/day = ~7 queries/min average
Peak: 50 concurrent queries (9 AM analyst rush)

Solutions:
- Snowflake: multi-cluster warehouses auto-scale from 1 → 10 clusters
- BigQuery: serverless, no concurrency limit
- Redshift: WLM (Workload Management) queues by user group
  - ETL queue: 5 slots, high memory
  - Analyst queue: 15 slots, balanced
  - BI queue: 10 slots, fast queries only (max 5 min timeout)
```

---

## Problem 8: Design a Feature Store for ML (Online + Offline)

**Prompt:** Design a feature store that serves features for both ML training (offline, batch) and real-time inference (online, < 10ms).

### Core Design Challenge

The dual-access problem: training needs historical point-in-time correct features; inference needs the latest features in milliseconds.

### Architecture

```
[Feature Pipelines (Spark/Flink)]
         │
    ┌────┴────────────────────┐
    ▼                        ▼
[Offline Store]         [Online Store]
(Delta Lake / S3)       (Redis / DynamoDB)
(PB scale, batch)       (GB scale, ms latency)
         │                        │
         ▼                        ▼
[Training jobs]          [Inference service]
(historical features)    (latest features)
```

### Point-in-Time Correctness (Critical for Offline)

```python
# Feature point-in-time join: get feature values AS OF the label timestamp
# Prevents feature leakage (using future data to predict the past)

def point_in_time_join(entity_df: DataFrame, feature_table: DataFrame) -> DataFrame:
    """
    entity_df: (user_id, label_timestamp, label)
    feature_table: (user_id, feature_timestamp, feature_value)
    Returns: entity_df joined with latest feature BEFORE label_timestamp
    """
    return entity_df.join(
        feature_table,
        on=[
            entity_df.user_id == feature_table.user_id,
            feature_table.feature_timestamp <= entity_df.label_timestamp
        ],
        how="left"
    ).select(
        entity_df["*"],
        # Rank: take the most recent feature value before the label
        F.last("feature_value").over(
            Window.partitionBy("user_id", "label_timestamp")
                  .orderBy("feature_timestamp")
        ).alias("feature_value")
    )
```

### Feature Registry (Catalog)

```yaml
# feature_definitions.yaml
features:
  - name: user_30d_spend
    entity: user_id
    source: transactions_table
    transformation: "SUM(amount) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 30*86400 PRECEDING AND CURRENT ROW)"
    online_ttl: 3600   # refresh every hour in Redis
    offline_window: "30d"
    owner: "ml-platform@company.com"

  - name: merchant_avg_order_value
    entity: merchant_id
    source: orders_table
    transformation: "AVG(amount) OVER (PARTITION BY merchant_id ORDER BY ts ROWS BETWEEN 7*86400 PRECEDING AND CURRENT ROW)"
    online_ttl: 86400  # refresh daily
    offline_window: "7d"
```

### Online Feature Serving

```python
# Redis schema: HSET user:{user_id} feature_name value
# Pipeline lookup for low latency

async def get_online_features(user_id: str, feature_names: list) -> dict:
    async with redis_pool.pipeline() as pipe:
        pipe.hmget(f"user:{user_id}", feature_names)
        results = await pipe.execute()
    return dict(zip(feature_names, results[0]))

# Writes: Flink job keeps Redis in sync from Kafka events
# TTL management: EXPIRE key {ttl_seconds} on every write
```

### Managed Alternatives

| Platform | Online Store | Offline Store | Notes |
|---|---|---|---|
| **Feast** | Redis | BigQuery / S3 | Open source, Kubernetes |
| **Tecton** | DynamoDB | S3 + Spark | Managed, expensive |
| **Vertex AI Feature Store** | Bigtable | BigQuery | GCP-native |
| **SageMaker Feature Store** | DynamoDB | S3 | AWS-native |

---

## Problem 9: Design a Data Mesh for 20 Domain Teams

### Core Principles Applied

```
Domains: Orders, Payments, Inventory, Marketing, Logistics, ...
Each domain owns its data products end-to-end.

Central Platform Team provides:
  - Compute (Databricks / Spark clusters)
  - Storage (S3 + Delta Lake)
  - Catalog (Unity Catalog / DataHub)
  - CI/CD templates (GitHub Actions + dbt project template)
  - PII masking (automatic column-level masking in Unity Catalog)
  - Data product SLA monitoring

Domain Teams own:
  - Their source data
  - Their data product schema and SLA
  - Their dbt models
  - Their data quality checks
```

### Data Product Interface

```yaml
# data_product.yaml — published by each domain team
data_product:
  name: "orders_daily_summary"
  owner: "orders-team@company.com"
  domain: "orders"
  version: "2.1.0"
  
  output_ports:
    - type: delta_table
      location: "s3://data-mesh/orders/orders_daily_summary/"
      schema_path: "schemas/orders_daily_summary.json"
      partitioned_by: ["date"]
  
  sla:
    freshness: "daily by 06:00 UTC"
    availability: "99.5%"
    row_count_min: 10000
  
  access_policy:
    classification: "internal"
    pii_columns: ["customer_email"]  # auto-masked by Unity Catalog
  
  dependencies:
    - "orders_team.raw_orders"
    - "payments_team.payment_events"
```

---

## Senior-Level Discussion Topics

**When asked "how would you improve this at scale?":**

1. **Exactly-once delivery**: Use Kafka transactions + idempotent Delta writes
2. **Multi-region**: Active-active with conflict resolution (last-writer-wins with vector clocks)
3. **Cost at scale**: Implement compute/storage separation (query decoupled from storage via Iceberg)
4. **Schema governance**: Confluent Schema Registry + compatibility checks in CI/CD
5. **Observability**: OpenTelemetry traces from ingestion through to query (end-to-end lineage)
