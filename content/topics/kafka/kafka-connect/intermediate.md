---
title: "Kafka Connect - Intermediate"
topic: kafka
subtopic: kafka-connect
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [kafka, kafka-connect, debezium, CDC, S3-sink, error-handling, exactly-once]
---

# Kafka Connect — Intermediate

## Debezium CDC (Change Data Capture)

Debezium is the most widely used source connector family. It reads database change logs rather than polling tables — giving you real-time, low-latency CDC.

### Debezium PostgreSQL Connector

```json
{
  "name": "postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-host",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "secret",
    "database.dbname": "orders_db",
    "database.server.name": "orders-pg",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_pub",
    "table.include.list": "public.orders,public.customers",
    "topic.prefix": "orders-pg",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "tombstone"
  }
}
```

### Debezium Event Format

Without the `ExtractNewRecordState` SMT, Debezium events include before/after state:

```json
{
  "before": {"order_id": "123", "status": "PENDING"},
  "after":  {"order_id": "123", "status": "SHIPPED"},
  "op": "u",
  "ts_ms": 1700000000000,
  "source": {
    "db": "orders_db",
    "table": "orders",
    "lsn": 12345
  }
}
```

`op` values: `c` (create), `u` (update), `d` (delete), `r` (read/snapshot)

With `ExtractNewRecordState`: flattens to just the `after` value, simplifying downstream processing.

### Replication Slot Management

```sql
-- Check replication slot status (PostgreSQL)
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';

-- WARNING: stale replication slots cause WAL accumulation → disk full!
-- Monitor: pg_replication_slots.restart_lsn should advance
```

**Critical**: if Debezium goes offline without dropping the replication slot, PostgreSQL retains WAL logs indefinitely. This can fill disk. Always monitor slot lag.

## S3 Sink Connector Deep Dive

The S3 Sink is one of the most common data lake integrations.

```json
{
  "name": "s3-sink-orders",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "8",
    "topics": "orders",
    "s3.region": "us-east-1",
    "s3.bucket.name": "data-lake-raw",
    "s3.part.size": "134217728",
    "topics.dir": "topics",
    "flush.size": "100000",
    "rotate.interval.ms": "3600000",
    "rotate.schedule.interval.ms": "3600000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",
    "locale": "en_US",
    "timezone": "UTC",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "created_at",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "partition.duration.ms": "3600000",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "schema.compatibility": "FULL"
  }
}
```

### S3 File Layout

With `TimeBasedPartitioner` and `path.format`, files are organized as:

```
s3://data-lake-raw/topics/orders/year=2024/month=01/day=15/hour=14/
  orders+0+000001000.parquet
  orders+1+000002500.parquet
  orders+2+000001800.parquet
```

Filename format: `{topic}+{partition}+{start_offset}.{extension}`

### Flush Size vs Rotate Interval

| Config | Triggers flush when... |
|--------|----------------------|
| `flush.size` | N records accumulated |
| `rotate.interval.ms` | Time elapsed since last flush |
| `rotate.schedule.interval.ms` | Calendar-aligned flush (e.g., every hour on the hour) |

For balanced file sizes in production: `flush.size=100000` + `rotate.interval.ms=3600000`. Files rotate when either condition is met first.

## Error Handling and Dead Letter Queues

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq-s3-sink",
  "errors.deadletterqueue.topic.replication.factor": "3",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.retry.delay.max.ms": "60000",
  "errors.retry.timeout": "300000"
}
```

| `errors.tolerance` | Behavior |
|-------------------|----------|
| `none` (default) | Stop on first error |
| `all` | Skip bad records, send to DLQ |

With `errors.deadletterqueue.context.headers.enable=true`, the DLQ message includes headers explaining the failure (exception class, message, stack trace).

## Connector Performance Tuning

### Source Connector Parallelism

```json
{
  "tasks.max": "8",
  "table.include.list": "table1,table2,table3,table4,table5,table6,table7,table8",
  "poll.interval.ms": "1000",
  "batch.max.rows": "10000"
}
```

Each task processes one or more tables. `tasks.max` is bounded by the number of tables (JDBC) or partitions (Debezium single task).

### Sink Connector Parallelism

```json
{
  "tasks.max": "16",
  "consumer.override.fetch.min.bytes": "65536",
  "consumer.override.fetch.max.wait.ms": "500"
}
```

Sink connector tasks consume Kafka partitions like regular consumers. `tasks.max` is bounded by partition count.

## Worker Configuration (Distributed Mode)

```properties
# connect-distributed.properties
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
group.id=connect-cluster

# Storage topics (must exist before starting)
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status

# Replication factors (3 for production)
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3

# Performance
offset.flush.interval.ms=10000
offset.flush.timeout.ms=5000

# Converters (worker-level defaults; overridden per connector)
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://schema-registry:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081

# Plugin path for custom connectors
plugin.path=/opt/kafka/plugins
```

## Connector Lifecycle Management

```bash
# Pause a connector (stops consuming/producing)
curl -X PUT http://connect:8083/connectors/s3-sink/pause

# Resume
curl -X PUT http://connect:8083/connectors/s3-sink/resume

# Restart a failed task
curl -X POST http://connect:8083/connectors/s3-sink/tasks/0/restart

# Update connector config
curl -X PUT http://connect:8083/connectors/s3-sink/config \
  -H 'Content-Type: application/json' \
  -d '{"connector.class": "...", "tasks.max": "8", ...}'

# Check specific task status
curl http://connect:8083/connectors/s3-sink/status | jq '.tasks'
```

## Interview Tips

> **Tip 1:** Debezium replication slot management is a critical production concern. A stale slot with an offline Debezium causes WAL accumulation and eventually disk-full on PostgreSQL. Always monitor `pg_replication_slots.restart_lsn` and alert if it's not advancing.

> **Tip 2:** The `ExtractNewRecordState` SMT is the most common Debezium SMT. Without it, consumers must handle the envelope format (before/after/op). With it, you get the current row state directly. Know both formats.

> **Tip 3:** For S3 sink, explain the file flush controls: `flush.size` (record count), `rotate.interval.ms` (time), and `rotate.schedule.interval.ms` (calendar-aligned). In practice, you set both so that even low-traffic topics flush periodically.

> **Tip 4:** `errors.tolerance=all` with a DLQ is the production default for sink connectors. `errors.tolerance=none` (default) stops the connector on any bad record, which is usually wrong for high-volume data pipelines.

> **Tip 5:** The three Connect storage topics (`connect-configs`, `connect-offsets`, `connect-status`) must have `replication.factor=3` and use compaction. They are critical to cluster state — loss of these topics means losing all connector configurations and offsets.
