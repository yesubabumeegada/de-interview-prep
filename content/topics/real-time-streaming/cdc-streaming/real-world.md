---
title: "CDC Streaming (Debezium) — Real World"
topic: real-time-streaming
subtopic: cdc-streaming
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [cdc, debezium, production, kafka, flink, spark, data-sync, iceberg]
---

# CDC Streaming (Debezium) — Real World

## Pattern 1: MySQL to Data Lake Sync

```python
"""
Pattern: MySQL production database → Debezium → Kafka → Spark Streaming → Delta Lake
Goal: near-real-time replica of MySQL orders table in Delta Lake for analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Debezium Kafka message schema (unwrapped by ExtractNewRecordState SMT)
cdc_schema = StructType([
    StructField("order_id",      LongType()),
    StructField("user_id",       LongType()),
    StructField("status",        StringType()),
    StructField("amount",        DecimalType(10, 2)),
    StructField("created_at",    TimestampType()),
    StructField("updated_at",    TimestampType()),
    StructField("__op",          StringType()),   # c=insert, u=update, d=delete, r=snapshot
    StructField("__ts_ms",       LongType()),     # DB change timestamp (ms)
    StructField("__deleted",     BooleanType())   # True for deletes
])

# Read CDC stream from Kafka
cdc_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "myserver.ecommerce.orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), cdc_schema).alias("d")) \
    .select("d.*")

def apply_cdc_to_delta(batch_df, batch_id):
    """
    Apply CDC events to Delta Lake using merge (UPSERT + DELETE).
    Exactly-once: Delta merge is idempotent on order_id.
    """
    if batch_df.isEmpty():
        return
    
    target_path = "s3://bucket/delta/orders_replica/"
    
    # Separate deletes from upserts
    deletes = batch_df.filter(col("__deleted") == True).select("order_id")
    upserts = batch_df.filter(col("__deleted") == False) \
        .select("order_id", "user_id", "status", "amount", "created_at", "updated_at", "__ts_ms") \
        .dropDuplicates(["order_id"]) \  # keep latest per order_id within batch
        .orderBy("__ts_ms")              # process in order
    
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        
        # Handle deletes first
        if not deletes.isEmpty():
            target.delete(
                target.toDF().join(deletes, "order_id").isNotNull()
            )
        
        # Upsert: insert new + update changed rows
        if not upserts.isEmpty():
            target.alias("t").merge(
                upserts.alias("s"),
                "t.order_id = s.order_id"
            ) \
            .whenMatchedUpdate(
                condition="s.__ts_ms > t.__ts_ms",  # only update if incoming is newer
                set={
                    "status":     "s.status",
                    "amount":     "s.amount",
                    "updated_at": "s.updated_at"
                }
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        # First batch: create Delta table
        upserts.drop("__ts_ms").write.format("delta") \
            .mode("overwrite") \
            .save(target_path)
    
    print(f"Batch {batch_id}: {upserts.count()} upserts, {deletes.count()} deletes")

# Apply CDC events
query = cdc_stream.writeStream \
    .foreachBatch(apply_cdc_to_delta) \
    .option("checkpointLocation", "s3://bucket/ckpt/orders-cdc/") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

---

## Pattern 2: CDC for Search Index Sync

```python
"""
Pattern: keep Elasticsearch in sync with PostgreSQL via Debezium CDC
Goal: when a product is updated in PostgreSQL, update Elasticsearch within 1 second
"""

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, helpers
import json

es = Elasticsearch(['http://elasticsearch:9200'])
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'es-sync',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['pg.public.products'])  # PostgreSQL products table

def process_product_change(payload: dict) -> dict:
    """Convert Debezium payload to Elasticsearch action."""
    op    = payload['op']       # c/u/d/r
    after = payload.get('after')
    before= payload.get('before')
    
    if op in ('c', 'u', 'r'):  # insert or update
        product = after
        return {
            '_op_type': 'index',   # creates or replaces the document
            '_index': 'products',
            '_id': str(product['product_id']),
            '_source': {
                'product_id':   product['product_id'],
                'name':         product['name'],
                'description':  product['description'],
                'category':     product['category'],
                'price':        float(product['price']),
                'inventory':    product['inventory_count'],
                'is_available': product['inventory_count'] > 0,
                'tags':         json.loads(product.get('tags', '[]')),
                'updated_at':   payload['ts_ms']
            }
        }
    elif op == 'd':  # delete
        return {
            '_op_type': 'delete',
            '_index': 'products',
            '_id': str(before['product_id'])
        }

# Batch processing: collect 100 events and bulk index to ES
buffer = []
BATCH_SIZE = 100

while True:
    msg = consumer.poll(timeout=0.1)
    
    if msg and not msg.error():
        payload = json.loads(msg.value())
        action = process_product_change(payload['payload'])
        if action:
            buffer.append(action)
    
    # Flush when buffer full or timeout (process every 1 second)
    if len(buffer) >= BATCH_SIZE or (buffer and msg is None):
        try:
            # Bulk index to Elasticsearch
            success, errors = helpers.bulk(es, buffer, raise_on_error=False)
            if errors:
                print(f"ES bulk errors: {errors[:5]}")  # log first 5
            else:
                consumer.commit()  # commit only after successful ES write
            buffer = []
        except Exception as e:
            print(f"ES bulk failed: {e}")
            # Don't commit → retry on next poll
```

---

## Pattern 3: CDC for Microservice Data Sync

```python
"""
Pattern: order-service writes to MySQL → CDC → user-service maintains its own read model
Avoids: direct DB calls across microservices (avoids tight coupling)
Uses: event-driven architecture with CDC as the integration layer
"""

from confluent_kafka import Consumer, Producer
import json

# user-service consumes order events from CDC stream
# Maintains local cache of order totals per user (for recommendation engine)
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'user-service-order-cache',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['myserver.ecommerce.orders'])

import redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

def update_user_order_cache(payload: dict):
    """
    User-service maintains per-user order statistics cache.
    Updated via CDC events from order-service MySQL.
    """
    op    = payload['op']
    after = payload.get('after')
    before= payload.get('before')
    
    if op in ('c', 'r'):  # insert
        user_id  = after['user_id']
        order_id = after['order_id']
        amount   = float(after['amount'])
        
        # Increment user's order count and total spend
        pipe = redis_client.pipeline()
        pipe.hincrby(f"user:{user_id}:stats", "order_count", 1)
        pipe.hincrbyfloat(f"user:{user_id}:stats", "total_spent", amount)
        pipe.zadd(f"user:{user_id}:orders", {order_id: float(after['created_at'].timestamp())})
        pipe.execute()
    
    elif op == 'u':  # update
        user_id = after['user_id']
        
        # If status changed to 'refunded': subtract from total spend
        if before['status'] != 'refunded' and after['status'] == 'refunded':
            refund_amount = float(before['amount'])
            redis_client.hincrbyfloat(f"user:{user_id}:stats", "total_spent", -refund_amount)
        
        # Update order details in sorted set (for timeline queries)
        redis_client.zadd(f"user:{user_id}:orders", 
                         {after['order_id']: float(after['updated_at'].timestamp())})
    
    elif op == 'd':  # delete
        user_id  = before['user_id']
        order_id = before['order_id']
        
        # Remove order from user's timeline
        redis_client.zrem(f"user:{user_id}:orders", order_id)
        # Note: we don't subtract from total_spent (order placed = revenue recognized)

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    
    payload = json.loads(msg.value())['payload']
    update_user_order_cache(payload)
    consumer.commit()

# user-service API: get user order stats (no cross-service DB call)
# GET /users/{user_id}/stats → read from Redis (< 5ms)
```

---

## Interview Tips

> **Tip 1:** "How do you handle a CDC pipeline that falls behind by several hours?" — First: diagnose root cause. Check Kafka consumer lag (events in topic vs. consumer committed offset). Check if Debezium connector is running and publishing (connector status = RUNNING, source-record-write-rate > 0). If the consumer is the bottleneck: scale out consumers (increase consumer group size, ensure Kafka topic has enough partitions). If Debezium is the bottleneck: check DB network connectivity, replication lag. Recovery: let the consumer catch up naturally (it will process as fast as possible). Monitor: consumer lag decreasing → healthy catch-up. If lag is due to a slow downstream (e.g., Elasticsearch bulk indexing): increase batch size, add buffer between consumer and downstream.

> **Tip 2:** "What's the difference between using Debezium vs Flink CDC connector vs Spark's Kafka CDC integration?" — Debezium (Kafka Connect): runs as a separate service, publishes CDC events to Kafka, decoupled from consumers. Multiple consumers can read independently. Best for: production CDC infrastructure where multiple downstream systems need the same data. Flink CDC connector (`flink-connector-debezium`): embeds Debezium directly inside the Flink job (no separate Kafka). Simpler setup, but CDC events are only available to the one Flink job. Best for: single-consumer CDC with Flink processing. Spark + Kafka: Spark reads CDC events from Kafka (published by Debezium). Decoupled — multiple Spark jobs can read. Best for: batch-style streaming (trigger-based) or when the team prefers Spark over Flink.

> **Tip 3:** "How do you test a CDC pipeline in a development environment?" — Create a development MySQL/PostgreSQL instance with Debezium running in Docker Compose. Use a tool like `debezium-testing/debezium-testing` for end-to-end integration tests. Key test cases: (a) Initial snapshot: verify all existing rows appear as `op=r` events; (b) INSERT: insert a row in MySQL, verify `op=c` event in Kafka within 1 second; (c) UPDATE: update a row, verify `op=u` event with correct before/after; (d) DELETE: delete a row, verify `op=d` event with before values; (e) Schema change: ALTER TABLE ADD COLUMN, verify new events have the new field; (f) Connector restart: stop connector, make DB changes, restart connector, verify events replay correctly. Use Testcontainers for CI/CD pipeline integration.
