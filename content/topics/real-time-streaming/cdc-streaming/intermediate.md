---
title: "CDC Streaming (Debezium) — Intermediate"
topic: real-time-streaming
subtopic: cdc-streaming
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [cdc, debezium, kafka, flink, spark, postgres, mysql, exactly-once, schema-evolution]
---

# CDC Streaming (Debezium) — Intermediate

## PostgreSQL CDC Setup

```json
// PostgreSQL CDC requires logical replication (WAL level = logical)
// postgresql.conf:
//   wal_level = logical
//   max_replication_slots = 4      (one per Debezium connector)
//   max_wal_senders = 4

// Create replication user:
// CREATE USER debezium REPLICATION LOGIN PASSWORD 'password';
// GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
// GRANT USAGE ON SCHEMA public TO debezium;

// PostgreSQL connector configuration:
{
  "name": "postgres-ecommerce-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    
    "database.hostname": "postgres-host",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "password",
    "database.dbname": "ecommerce",
    "topic.prefix": "pg",
    
    // Logical decoding plugin (must match what's installed on PostgreSQL server)
    "plugin.name": "pgoutput",   // built-in PostgreSQL 10+
    // or "decoderbufs" (requires plugin installation)
    
    // Publication: PostgreSQL logical replication publication
    // Debezium creates this automatically: FOR TABLE public.orders, public.customers
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "all_tables",  // or: filtered, disabled
    
    "slot.name": "debezium_slot",  // replication slot (unique per connector)
    
    "table.include.list": "public.orders,public.customers",
    
    // Heartbeat: emit heartbeat events to keep replication slot active
    // Without this: inactive tables → replication slot holds WAL forever (disk bloat)
    "heartbeat.interval.ms": "10000",
    "heartbeat.action.query": "UPDATE debezium_heartbeat SET ts = NOW()",
    
    "snapshot.mode": "initial",
    
    // TOAST columns: large values stored outside main table
    // Debezium may not include unchanged TOAST values in before image
    "toasted.value.placeholder": "__debezium_unavailable_value"
  }
}

// Critical PostgreSQL gotcha: REPLICA IDENTITY
// Default: PostgreSQL only includes PK in DELETE events (no before image)
// Fix: ALTER TABLE orders REPLICA IDENTITY FULL;
// FULL: include all column values in before image for updates and deletes
// Required for full before/after in Debezium events
```

---

## CDC to Iceberg: Data Lake Sync

```python
"""
Pattern: Debezium CDC events → Kafka → Flink → Iceberg table (upsert)
Result: Iceberg table stays in sync with MySQL source table
Latency: < 1 second
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)
env.enable_checkpointing(60_000)

tenv = StreamTableEnvironment.create(env)

# Create Kafka source table (Debezium JSON format)
tenv.execute_sql("""
    CREATE TABLE orders_cdc (
        order_id    BIGINT,
        user_id     BIGINT,
        status      STRING,
        amount      DECIMAL(10, 2),
        created_at  TIMESTAMP(3),
        updated_at  TIMESTAMP(3),
        PRIMARY KEY (order_id) NOT ENFORCED
    ) WITH (
        'connector'             = 'kafka',
        'topic'                 = 'myserver.ecommerce.orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id'  = 'flink-cdc-iceberg',
        'format'                = 'debezium-json',
        'debezium-json.schema-include' = 'true',
        'scan.startup.mode'    = 'earliest-offset'
    )
""")

# Create Iceberg sink table
tenv.execute_sql("""
    CREATE TABLE orders_iceberg (
        order_id    BIGINT,
        user_id     BIGINT,
        status      STRING,
        amount      DECIMAL(10, 2),
        created_at  TIMESTAMP(3),
        updated_at  TIMESTAMP(3),
        PRIMARY KEY (order_id) NOT ENFORCED
    ) WITH (
        'connector'      = 'iceberg',
        'catalog-name'   = 'hive_catalog',
        'catalog-type'   = 'hive',
        'uri'            = 'thrift://metastore:9083',
        'warehouse'      = 's3://bucket/warehouse',
        'database-name'  = 'ecommerce_replica',
        'table-name'     = 'orders',
        'format-version' = '2'  -- Iceberg v2: supports row-level deletes (upsert)
    )
""")

# Sync: inserts and updates become UPSERT, deletes become DELETE in Iceberg
tenv.execute_sql("""
    INSERT INTO orders_iceberg
    SELECT order_id, user_id, status, amount, created_at, updated_at
    FROM orders_cdc
""")
# Flink automatically handles Debezium +/-I rows → Iceberg upsert/delete
```

---

## Schema Evolution with Debezium

```python
"""
Schema evolution challenge: source table adds/removes columns.
Debezium detects schema changes and updates the Kafka topic schema.

Scenario: DBA adds 'discount_pct DECIMAL(5,2)' column to orders table
"""

# Before schema change:
# Kafka message: {"order_id": 1, "status": "pending", "amount": 100.00}

# After schema change:
# Kafka message: {"order_id": 1, "status": "pending", "amount": 100.00, "discount_pct": 0.10}

# Debezium behavior:
# 1. ALTER TABLE event: Debezium detects DDL change from binlog
# 2. New schema registered in Schema Registry
# 3. All new messages use new schema
# 4. Old consumers still reading old schema:
#    - Avro with forward compatibility: old consumer ignores new field (safe)
#    - JSON without schema: field may cause KeyError if consumer expects exact schema

# Best practice: use Avro with Schema Registry + compatible schema evolution

# Avro schema evolution rules:
# BACKWARD compatible: consumers using new schema can read old messages
#   → Add fields with default values (safe)
#   → Remove fields with defaults (safe)
# FORWARD compatible: old consumers can read new messages
#   → New schema can only add fields (old consumers ignore unknown fields)
# FULL compatible: both backward and forward
#   → Only add/remove fields with defaults

# Consumer that handles schema evolution gracefully:
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

consumer = AvroConsumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'cdc-consumer',
    'schema.registry.url': 'http://schema-registry:8081',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['myserver.ecommerce.orders'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    
    payload = msg.value()  # AvroConsumer auto-deserializes using schema registry
    
    # Access fields defensively (handle both old and new schema)
    order_id     = payload.get('order_id')
    amount       = payload.get('amount')
    discount_pct = payload.get('discount_pct', 0.0)  # default=0.0 for old messages
    
    process_order(order_id, amount, discount_pct)
    consumer.commit()
```

---

## Debezium Outbox Pattern

```sql
-- Transactional outbox: reliably publish events FROM application via Debezium CDC

-- Create outbox table in MySQL:
CREATE TABLE outbox (
    id              UUID         NOT NULL PRIMARY KEY,
    aggregate_type  VARCHAR(255) NOT NULL,  -- 'Order', 'Customer', etc.
    aggregate_id    VARCHAR(255) NOT NULL,  -- entity ID
    event_type      VARCHAR(255) NOT NULL,  -- 'OrderPlaced', 'OrderShipped'
    payload         JSON         NOT NULL,  -- event data
    created_at      DATETIME     DEFAULT CURRENT_TIMESTAMP
);

-- Application code (Java/Python): same transaction = business logic + outbox insert
BEGIN TRANSACTION;
  -- 1. Business operation
  INSERT INTO orders (order_id, user_id, amount, status) VALUES (...);
  
  -- 2. Outbox event (atomic with the business operation)
  INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
  VALUES (
    UUID(),
    'Order',
    :order_id,
    'OrderPlaced',
    JSON_OBJECT(
      'order_id', :order_id,
      'user_id',  :user_id,
      'amount',   :amount,
      'timestamp', NOW()
    )
  );
COMMIT;

-- Debezium watches outbox table → publishes changes to Kafka
-- Debezium Outbox Event Router: transforms raw CDC event into clean domain event
-- Routes to topic based on aggregate_type: Orders.events, Customer.events, etc.
```

```json
// Debezium connector with Outbox Event Router transform:
{
  "name": "outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.user": "debezium",
    "database.password": "password",
    "topic.prefix": "internal",
    "table.include.list": "ecommerce.outbox",
    
    // Outbox Event Router transformation
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.fields.additional.placement": "aggregate_type:envelope:type",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.route.topic.replacement": "${routedByValue}.events",
    
    // Result: order change → published to "Order.events" Kafka topic
    //         customer change → published to "Customer.events" Kafka topic
    
    // Optionally: delete outbox rows after publishing (keep table small)
    // Application can delete rows after Debezium has processed them
    // (check offset: outbox.id > last_processed_id)
  }
}
```

---

## Interview Tips

> **Tip 1:** "What is a PostgreSQL replication slot and what happens if it's not consumed?" — A replication slot is a PostgreSQL mechanism that tracks which WAL position a Debezium connector has consumed. PostgreSQL ensures WAL files are retained until all active replication slots have consumed them. If the Debezium connector stops consuming (e.g., Kafka Connect worker down), PostgreSQL keeps accumulating WAL files on disk. If replication lag grows for hours or days: disk fills up → PostgreSQL panics and crashes. Prevention: (a) monitor replication slot lag (< 1 GB typically); (b) set `pg_replication_slots.active` monitoring; (c) if Debezium will be down for extended period, drop the replication slot and restart with `snapshot.mode=schema_only` when resuming; (d) use heartbeat queries to keep the slot active during quiet periods.

> **Tip 2:** "How does Debezium handle the initial snapshot without affecting production database performance?" — Debezium's initial snapshot uses a `SELECT * FROM table` with a consistent read (shared lock or transaction isolation). For large tables (hundreds of millions of rows), this can: (a) consume significant I/O reading the table; (b) hold a shared lock briefly (for MySQL); (c) increase database load for hours. Mitigations: (a) run snapshot during off-peak hours; (b) use `snapshot.select.statement.overrides` to add a custom SELECT with LIMIT or ORDER BY for chunked snapshot; (c) for MySQL: use `snapshot.mode=schema_only` and then manually load historical data to the target; (d) point Debezium at a read replica for the initial snapshot (then switch to primary for streaming).

> **Tip 3:** "What is the difference between Debezium's `op=u` and `op=r` events?" — `op=u` (UPDATE): a row was updated in the source database. Contains both `before` (old row values) and `after` (new row values). Used for tracking changes. `op=r` (READ): emitted during the initial snapshot phase. Debezium reads existing rows and emits them as READ events. Contains only `after` (no before — these are "initial reads", not changes). When building a CDC pipeline, treat `op=r` events the same as `op=c` (INSERT) — upsert the full row into the target. After the snapshot completes, only `op=c/u/d` events will arrive.
