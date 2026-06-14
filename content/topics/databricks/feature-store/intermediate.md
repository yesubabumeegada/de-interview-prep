---
title: "Databricks Feature Store - Intermediate"
topic: databricks
subtopic: feature-store
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, feature-store, point-in-time, online-store, streaming-features]
---

# Databricks Feature Store — Intermediate Concepts

## Point-in-Time Feature Lookups

The most important feature store concept for ML correctness: join features at the historical event time, not at the current time. Prevents **data leakage**.

**The problem without PIT:**
```
Event: user clicked on product at 2024-01-15 14:30
Feature value now: user has 50 orders
Feature value at click time: user had 12 orders
→ Training on "current" features causes leakage
```

**PIT lookup:**

```python
from databricks.feature_store import FeatureLookup

# labels_df must have an event_timestamp column
labels_df = spark.createDataFrame([
    {"user_id": "U001", "event_timestamp": "2024-01-15 14:30:00", "clicked": 1},
    {"user_id": "U001", "event_timestamp": "2024-03-20 09:00:00", "clicked": 0},
])

feature_lookups = [
    FeatureLookup(
        table_name="ml.user_features",
        feature_names=["total_orders", "days_since_last_order"],
        lookup_key="user_id",
        timestamp_lookup_key="event_timestamp"  # ← enables point-in-time
    )
]

# Feature store joins features as of event_timestamp — not current values
training_set = fs.create_training_set(
    df=labels_df,
    feature_lookups=feature_lookups,
    label="clicked"
)
```

**Requirement:** The feature table must have a timestamp column (`feature_timestamp`) so the store knows which historical version to join.

---

## Online Store Integration

For real-time serving (sub-100ms latency), offline Delta tables are too slow. Use an online store:

```python
# Publish features to an online store (e.g., AWS DynamoDB, Azure CosmosDB)
# Databricks manages the sync from offline Delta → online store

from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec

online_store = AmazonDynamoDBSpec(
    region="us-east-1",
    table_name="user-features-online",
    read_secret_prefix="scope:dynamo-read",
    write_secret_prefix="scope:dynamo-write"
)

fs.publish_table(
    name="ml.user_features",
    online_store=online_store,
    mode="merge"
)
# Now feature values are in DynamoDB for ms-latency serving
```

**Offline vs Online store:**

| | Offline (Delta) | Online (DynamoDB/CosmosDB) |
|--|--|--|
| **Latency** | Seconds | <10ms |
| **Use case** | Batch training, batch scoring | Real-time serving |
| **Cost** | Low | Higher |
| **Updates** | Batch/streaming | Near-real-time sync |

---

## Streaming Feature Pipelines

Keep features fresh with continuous updates:

```python
from pyspark.sql import functions as F

# Read from Kafka
events = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "user-events")
    .load()
)

# Compute streaming features (5-min window)
streaming_features = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes"),
        "user_id"
    )
    .agg(
        F.count("*").alias("events_5min"),
        F.sum("amount").alias("spend_5min")
    )
    .select("user_id", "events_5min", "spend_5min",
            F.col("window.end").alias("feature_timestamp"))
)

# Write streaming features to feature store
fs.write_table(
    name="ml.user_realtime_features",
    df=streaming_features,
    mode="merge",
    streaming=True    # enable streaming write
)
```

---

## Feature Table Versioning and Lineage

```python
# View feature table metadata
table_meta = fs.get_table("ml.user_features")
print(table_meta.description)
print(table_meta.features)    # list of Feature objects with name, dtype, doc

# See which models used these features
models_using = fs.get_consumers("ml.user_features")
# Returns: list of registered models that were trained on this feature table

# See which feature tables a model depends on
model_features = fs.get_feature_table_references("models:/purchase-propensity/Production")
# Returns: {"ml.user_features": ["total_orders", "avg_order_value"], ...}
```

---

## Multiple Lookup Keys and Composite Keys

```python
# Feature table with composite primary key
fs.create_table(
    name="ml.user_product_interactions",
    primary_keys=["user_id", "product_id"],   # composite key
    df=interactions_df
)

# Lookup requires both keys
feature_lookups = [
    FeatureLookup(
        table_name="ml.user_product_interactions",
        feature_names=["interaction_count", "last_interaction_days"],
        lookup_key=["user_id", "product_id"]  # list for composite
    )
]
```

---

## Interview Tips

> **Tip 1:** "What is point-in-time correctness and why does it matter?" — "PIT correctness means joining feature values as they existed at the time of each training event — not at the current time. Without it, you leak future information into training (e.g., a user's order count after the event happened), making the model optimistic and causing production degradation."

> **Tip 2:** "When would you use an online store vs just reading from the Delta feature table?" — "For real-time serving where latency must be <50ms, Delta reads are too slow (seconds per query). Publish features to an online store like DynamoDB or CosmosDB for key-value lookups. Offline Delta is fine for batch scoring and training. The feature store manages the sync — you don't maintain two separate pipelines."

> **Tip 3:** "How do you keep online features fresh?" — "Two options: (1) Scheduled batch job that runs `fs.publish_table` on a cron (e.g., hourly) — simple but features are stale between runs. (2) Streaming pipeline that computes features from Kafka/Event Hubs and writes to the feature store continuously via `streaming=True` — near-real-time freshness. Choose based on how stale features affect prediction quality."
