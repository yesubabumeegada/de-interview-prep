---
title: "CDC Streaming (Debezium) — Fundamentals"
topic: real-time-streaming
subtopic: cdc-streaming
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [cdc, debezium, kafka, change-data-capture, mysql, postgres, binlog]
---

# CDC Streaming (Debezium) — Fundamentals


## 🎯 Analogy

Think of CDC streaming like a live audit log feed: instead of polling the database every hour, Debezium tails the transaction log (binlog/WAL) and publishes every INSERT, UPDATE, DELETE to Kafka as it happens — millisecond propagation.

---
## What Is CDC?

CDC (Change Data Capture) is a pattern for capturing every INSERT, UPDATE, and DELETE from a database and streaming them as events.

```
Traditional ETL (batch):                   CDC (streaming):
  Every hour: SELECT * FROM orders          Every change: capture immediately
  WHERE updated_at > last_run               INSERT → event published to Kafka
  → Pull changed rows periodically          UPDATE → event published to Kafka
  → High latency (up to 1 hour)             DELETE → event published to Kafka
  → Misses hard deletes                     → Low latency (< 1 second)
  → Heavy DB load (full table scan)         → Captures hard deletes
                                            → Minimal DB load (reads log, not tables)

How CDC works (log-based):
  Databases maintain a transaction log for recovery:
    MySQL:      binary log (binlog)
    PostgreSQL: write-ahead log (WAL)
    SQL Server: transaction log
    Oracle:     redo log
  
  CDC reads the transaction log (already written) → no DB load
  Captures: every row change with before/after values
  Ordered: changes are ordered by commit time (within each table)
```

---

## Debezium Architecture

```
Debezium: open-source CDC platform built on Kafka Connect

Architecture:

  Source DB (MySQL)
       │
       │ reads transaction log (binlog)
       ▼
  Debezium Connector   ← runs inside Kafka Connect worker
  (MySQL Connector)
       │
       │ publishes change events
       ▼
  Kafka Topic:  {server}.{database}.{table}
  Example:      myserver.ecommerce.orders
       │
       ├── Flink Consumer
       ├── Spark Streaming Consumer
       ├── Kafka Streams Application
       └── Elasticsearch Sink

Components:
  Kafka Connect:   platform for running connectors (producers/consumers)
  Debezium:        Kafka Connect source connector (reads DB logs)
  Kafka:           message broker (stores CDC events durably)
  Consumers:       downstream systems that read CDC events

Supported databases:
  MySQL, PostgreSQL, SQL Server, Oracle, MongoDB, Cassandra, IBM Db2, Vitess
```

---

## Debezium Event Format

```json
// Debezium change event (Kafka message value):
{
  "schema": { /* Avro schema — omitted for brevity */ },
  "payload": {
    "before": {
      "order_id": 12345,
      "status": "pending",
      "amount": 150.00,
      "updated_at": 1705331400000
    },
    "after": {
      "order_id": 12345,
      "status": "shipped",
      "amount": 150.00,
      "updated_at": 1705334800000
    },
    "source": {
      "version": "2.4.0.Final",
      "connector": "mysql",
      "name": "myserver",
      "ts_ms": 1705334800123,
      "db": "ecommerce",
      "table": "orders",
      "server_id": 1,
      "file": "mysql-bin.000003",
      "pos": 154,
      "row": 0
    },
    "op": "u",
    "ts_ms": 1705334800456,
    "transaction": null
  }
}

Operation codes:
  "c": CREATE (INSERT) — before=null, after=new row
  "u": UPDATE          — before=old row, after=new row
  "d": DELETE          — before=old row, after=null
  "r": READ (snapshot) — before=null, after=row (during initial snapshot)

Key takeaways:
  - before/after: full row contents (before+after update, before=null for inserts)
  - op:           operation type (c/u/d/r)
  - ts_ms:        timestamp the change was applied in DB
  - source.file + source.pos: binlog position (for recovery/deduplication)
```

---

## Debezium Setup: MySQL Connector

```json
// 1. Configure MySQL for CDC:
// my.cnf:
// server-id         = 1
// log_bin           = mysql-bin
// binlog_format     = ROW          (required for Debezium — row-level changes)
// binlog_row_image  = FULL         (include full row, not just changed columns)
// expire_logs_days  = 7            (retain 7 days of binlog)

// 2. Create Debezium MySQL user:
// CREATE USER 'debezium'@'%' IDENTIFIED BY 'password';
// GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';

// 3. Deploy Debezium connector configuration (POST to Kafka Connect REST API):
{
  "name": "mysql-ecommerce-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    
    "database.hostname": "mysql-host",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "password",
    "database.server.id": "184054",   // unique ID for this connector instance
    "topic.prefix": "myserver",       // prefix for Kafka topic names
    
    // Which tables to capture
    "database.include.list": "ecommerce",
    "table.include.list": "ecommerce.orders,ecommerce.customers,ecommerce.payments",
    
    // Snapshot mode: initial load before streaming
    "snapshot.mode": "initial",       // or: never, schema_only, when_needed
    // initial: full snapshot of existing data first, then stream changes
    // never:   skip snapshot, start from current binlog position (miss existing data)
    
    // Kafka message format
    "key.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    
    // Offset storage (tracks binlog position for recovery)
    "database.history.kafka.topic": "dbhistory.ecommerce",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    
    // Performance tuning
    "max.batch.size": "2048",
    "max.queue.size": "8192",
    "poll.interval.ms": "1000"
  }
}

// 4. Created Kafka topics (automatically by Debezium):
// myserver.ecommerce.orders     → all changes to orders table
// myserver.ecommerce.customers  → all changes to customers table
// myserver.ecommerce.payments   → all changes to payments table
```

---

## Consuming CDC Events

```python
from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'cdc-processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

consumer.subscribe(['myserver.ecommerce.orders'])

def process_order_change(event):
    """Process a Debezium change event for the orders table."""
    
    op      = event['op']       # 'c', 'u', 'd', 'r'
    before  = event.get('before')  # None for inserts
    after   = event.get('after')   # None for deletes
    ts_ms   = event['ts_ms']    # when change happened in DB
    
    if op == 'c':  # INSERT
        print(f"New order: {after['order_id']} - ${after['amount']}")
        upsert_to_analytical_store(after)
    
    elif op == 'u':  # UPDATE
        print(f"Order updated: {after['order_id']}")
        print(f"  Status: {before['status']} → {after['status']}")
        upsert_to_analytical_store(after)
    
    elif op == 'd':  # DELETE
        print(f"Order deleted: {before['order_id']}")
        # Hard delete captured! Something batch ETL can't do easily.
        mark_deleted_in_analytical_store(before['order_id'])
    
    elif op == 'r':  # READ (snapshot)
        print(f"Snapshot: {after['order_id']}")
        upsert_to_analytical_store(after)  # same as insert during initial load

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f"Error: {msg.error()}")
        continue
    
    # Debezium message value is the change event
    value = json.loads(msg.value().decode('utf-8'))
    payload = value.get('payload', value)  # handle different serialization formats
    
    process_order_change(payload)
    consumer.commit(msg)
```

---


## ▶️ Try It Yourself

```bash
# Deploy Debezium MySQL connector via Kafka Connect REST API
curl -X POST http://localhost:8083/connectors   -H "Content-Type: application/json"   -d '{
    "name": "mysql-orders-cdc",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql-host",
      "database.port": "3306",
      "database.user": "debezium",
      "database.password": "secret",
      "database.server.id": "1",
      "database.server.name": "mydb",
      "table.include.list": "orders_db.orders",
      "database.history.kafka.bootstrap.servers": "localhost:9092",
      "database.history.kafka.topic": "mydb.schema-changes"
    }
  }'

# CDC events arrive on topic: mydb.orders_db.orders
# Each event: {"op": "c"/"u"/"d", "before": {...}, "after": {...}}
kafka-console-consumer.sh --bootstrap-server localhost:9092   --topic mydb.orders_db.orders --from-beginning | head -5
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "Why is log-based CDC better than query-based CDC (polling `WHERE updated_at > ?`)?" — Query-based CDC has several limitations: (a) Hard deletes are invisible — a deleted row doesn't show up in `SELECT WHERE updated_at > ?`; (b) DB load — full table scans or index scans on large tables; (c) Missed updates — if a row is updated twice between polls, only the latest state is captured (intermediate states lost); (d) Requires an `updated_at` column in every table. Log-based CDC reads the transaction log (already written for DB recovery), capturing every change with before/after state, including hard deletes. Overhead on the DB: minimal (replication client reads the log sequentially).

> **Tip 2:** "What is Debezium's snapshot mode and when would you use `snapshot.mode=never`?" — On first startup, Debezium takes a snapshot of existing data (reads all rows from the tables to be tracked). This ensures downstream systems have the full history before streaming begins. `initial` (default): take full snapshot, then stream changes. `schema_only`: capture current schema only, no existing rows — stream only future changes (use when existing data is already in the target, or you don't need historical data). `never`: skip snapshot entirely, start from current binlog position — useful for tables that already exist in the target (e.g., you manually loaded history), or for testing (to avoid waiting for a long snapshot).

> **Tip 3:** "What information does the Debezium event `source.pos` field provide and why is it important?" — `source.pos` is the binlog position (byte offset in the binlog file) when this change was committed. Combined with `source.file` (binlog filename), it uniquely identifies where in the transaction log this change occurred. Debezium stores this as the "offset" (committed to Kafka Connect's offset storage after each batch). On restart: Debezium reads the last committed offset, resumes from that binlog position — replaying only uncommitted events. This enables at-least-once delivery: if Debezium crashes after publishing to Kafka but before committing the offset, it will re-publish those events on restart. Consumers must handle duplicates via idempotent writes.
