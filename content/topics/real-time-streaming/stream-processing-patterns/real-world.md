---
title: "Stream Processing Patterns — Real World"
topic: real-time-streaming
subtopic: stream-processing-patterns
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [streaming, patterns, production, kafka, flink, delta-lake, real-time]
---

# Stream Processing Patterns — Real World

## Pattern 1: Medallion Streaming Architecture

```python
"""
Production medallion: Kafka → Bronze → Silver → Gold
All layers use streaming (trigger=availableNow for scheduled, or continuous)

Bronze: raw ingest (no transformation)
Silver: parsed, validated, deduplicated
Gold:   business aggregations (per-minute/per-hour windows)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ===== BRONZE =====
def run_bronze_streaming():
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "app-events") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "100000") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(
            col("topic"),
            col("partition").cast("int"),
            col("offset").cast("long"),
            col("timestamp").alias("kafka_ts"),
            col("key").cast("string").alias("event_key"),
            col("value").cast("string").alias("raw_json"),
            current_timestamp().alias("bronze_ts")
        ) \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3://bucket/ckpt/bronze/app-events/") \
        .trigger(processingTime="30 seconds") \
        .start("s3://bucket/delta/bronze/app-events/")

# ===== SILVER =====
event_schema = StructType([
    StructField("event_id",   StringType()),
    StructField("user_id",    StringType()),
    StructField("event_type", StringType()),
    StructField("page_url",   StringType()),
    StructField("amount",     DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("session_id", StringType())
])

def bronze_to_silver(batch_df, batch_id):
    # Parse JSON
    parsed = batch_df \
        .select(from_json("raw_json", event_schema).alias("e"), "bronze_ts") \
        .select("e.*", "bronze_ts") \
        .withColumn("silver_ts", current_timestamp())
    
    # Validate (route invalid to DLQ)
    valid = parsed.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isNotNull() &
        col("event_time").isNotNull()
    )
    invalid = parsed.subtract(valid)
    if not invalid.isEmpty():
        invalid.write.format("delta").mode("append") \
            .save("s3://bucket/delta/dlq/app-events/")
    
    # Deduplicate on event_id across history (merge = skip if exists)
    if DeltaTable.isDeltaTable(spark, "s3://bucket/delta/silver/app-events/"):
        DeltaTable.forPath(spark, "s3://bucket/delta/silver/app-events/") \
            .alias("existing") \
            .merge(valid.alias("new"), "existing.event_id = new.event_id") \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        valid.write.format("delta").mode("overwrite") \
            .partitionBy("date") \
            .save("s3://bucket/delta/silver/app-events/")

def run_silver_streaming():
    return spark.readStream \
        .format("delta") \
        .load("s3://bucket/delta/bronze/app-events/") \
        .writeStream \
        .foreachBatch(bronze_to_silver) \
        .option("checkpointLocation", "s3://bucket/ckpt/silver/app-events/") \
        .trigger(processingTime="1 minute") \
        .start()

# ===== GOLD =====
def run_gold_streaming():
    """Aggregate: purchases per user per 5-minute window."""
    return spark.readStream \
        .format("delta") \
        .load("s3://bucket/delta/silver/app-events/") \
        .filter(col("event_type") == "purchase") \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window("event_time", "5 minutes"),
            "user_id"
        ) \
        .agg(
            count("event_id").alias("purchase_count"),
            sum("amount").alias("total_revenue"),
            countDistinct("session_id").alias("sessions")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id", "purchase_count", "total_revenue", "sessions"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", "s3://bucket/ckpt/gold/purchase-agg/") \
        .trigger(processingTime="5 minutes") \
        .start("s3://bucket/delta/gold/purchase-agg/")

# Run all three layers
bronze = run_bronze_streaming()
silver = run_silver_streaming()
gold   = run_gold_streaming()

for q in [bronze, silver, gold]:
    q.awaitTermination()
```

---

## Pattern 2: Real-Time Notification Pipeline

```python
"""
Pattern: event stream → filter/score → enrich → route → notify
Use case: send targeted push notifications based on user behavior
"""

from confluent_kafka import Consumer, Producer
import requests

consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'notifier'})
producer = Producer({'bootstrap.servers': 'kafka:9092'})
consumer.subscribe(['user-events'])

# Scoring model (loaded from MLflow registry)
import mlflow.pyfunc
model = mlflow.pyfunc.load_model("models:/churn-predictor/Production")

def process_notification_pipeline():
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        
        event = json.loads(msg.value())
        user_id = event['user_id']
        
        # Step 1: Score for churn risk
        features = extract_features(event)
        churn_score = model.predict([features])[0]
        
        # Step 2: Route based on score
        if churn_score > 0.8:
            channel = 'push-notification'    # urgent
        elif churn_score > 0.5:
            channel = 'email'                 # standard
        else:
            consumer.commit()
            continue  # no notification needed
        
        # Step 3: Enrich with user preferences
        user_prefs = get_user_preferences(user_id)  # Redis cache
        
        # Step 4: Publish to notification channel topic
        notification = {
            'user_id': user_id,
            'channel': channel,
            'churn_score': churn_score,
            'template': 'win_back_offer',
            'personalization': user_prefs.get('preferred_categories', []),
            'created_at': datetime.utcnow().isoformat()
        }
        
        producer.produce(
            topic=f'notifications-{channel}',
            key=user_id.encode(),
            value=json.dumps(notification).encode()
        )
        
        consumer.commit()

def get_user_preferences(user_id: str) -> dict:
    """Redis-cached user preferences lookup."""
    cached = redis_client.get(f"prefs:{user_id}")
    if cached:
        return json.loads(cached)
    
    # Cache miss: load from DB
    prefs = db.query("SELECT * FROM user_preferences WHERE user_id = %s", user_id)
    redis_client.setex(f"prefs:{user_id}", 3600, json.dumps(prefs))  # 1h TTL
    return prefs
```

---

## Pattern 3: Cross-Stream Correlation

```python
"""
Pattern: correlate events across multiple streams to detect complex patterns
Use case: detect user journey (ad_click → signup → purchase within 7 days)
"""

from pyspark.sql.functions import *

# Three event streams with watermarks
ad_clicks = spark.readStream.format("kafka") \
    .option("subscribe", "ad-clicks").load() \
    .select(from_json(col("value").cast("string"), ad_click_schema).alias("d")).select("d.*") \
    .withWatermark("click_time", "7 days")  # long watermark for 7-day window

signups = spark.readStream.format("kafka") \
    .option("subscribe", "user-signups").load() \
    .select(from_json(col("value").cast("string"), signup_schema).alias("d")).select("d.*") \
    .withWatermark("signup_time", "7 days")

purchases = spark.readStream.format("kafka") \
    .option("subscribe", "purchases").load() \
    .select(from_json(col("value").cast("string"), purchase_schema).alias("d")).select("d.*") \
    .withWatermark("purchase_time", "7 days")

# Step 1: Join ad_clicks with signups (user clicked ad then signed up within 7 days)
click_to_signup = ad_clicks.join(
    signups,
    expr("""
        ad_clicks.anonymous_id = signups.anonymous_id AND
        signup_time >= click_time AND
        signup_time <= click_time + interval 7 days
    """),
    "inner"
)

# Step 2: Join with purchases (signed-up user made a purchase within 7 days of signup)
full_journey = click_to_signup.join(
    purchases,
    expr("""
        click_to_signup.user_id = purchases.user_id AND
        purchase_time >= signup_time AND
        purchase_time <= signup_time + interval 7 days
    """),
    "leftOuter"    # left outer: include signups even without purchase
) \
.select(
    "ad_click_id", "ad_campaign_id", "anonymous_id", "user_id",
    "click_time", "signup_time", "purchase_time",
    "purchase_amount",
    (col("purchase_amount").isNotNull()).alias("converted")
)

# Write: attribution results for ad campaign analysis
full_journey.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/ckpt/ad-attribution/") \
    .start("s3://bucket/delta/gold/ad-attribution/")
```

---

## Interview Tips

> **Tip 1:** "How do you handle late-arriving events in a production streaming pipeline?" — Late events arrive after the watermark has advanced past their time window. Strategies: (a) Watermark tolerance: set `withWatermark("event_time", "10 minutes")` — accept events up to 10 minutes late; (b) `allowedLateness` (Flink/Spark): keep window state open longer after watermark passes, re-fire window when late events arrive; (c) Side output / DLQ: route truly late events (past allowed lateness) to a side output topic, process in a separate batch job for accuracy; (d) Reprocessing: if late events are critical (billing), replay the event stream from object storage through a batch job. Monitor: track late event rate per window — if > 5% of events are late, increase watermark tolerance.

> **Tip 2:** "How do you monitor data freshness in a streaming pipeline?" — Data freshness = how stale is the data in the serving layer relative to source events. Metrics to track: (a) Consumer lag (Kafka): events in topic minus last committed offset = how far behind; (b) Watermark lag (Flink/Spark): system time minus current watermark = how late is processing; (c) End-to-end latency: embed event timestamp in payload, measure delta at sink: `sink_write_time - event_time`; (d) DLQ rate: increasing DLQ volume = growing backlog of unprocessed events. Alerting: page on-call if end-to-end latency > SLA (e.g., > 5 minutes for a 1-minute SLA). Use a lightweight "heartbeat event" (synthetic event every 30 seconds) — if heartbeat latency grows, pipeline is falling behind.

> **Tip 3:** "When would you use a pull-based consumer vs push-based consumer?" — Pull-based (Kafka/Kinesis polling): consumer controls rate, natural backpressure (if consumer is slow, it just polls less). Simple retry on failure (re-poll). Good for: high-throughput batch processing, consumers with variable processing speed. Push-based (Kinesis EFO SubscribeToShard, Kafka Streams push internally): records pushed as soon as available, lower latency (no poll interval), but consumer must handle any arrival rate. Good for: low-latency use cases where you want immediate record delivery. HTTP/2 streaming (EFO) vs polling: EFO delivers records in ~20ms vs polling every 200ms. In practice: Kafka ecosystem uses pull everywhere; EFO is the exception for Kinesis low-latency consumers.
