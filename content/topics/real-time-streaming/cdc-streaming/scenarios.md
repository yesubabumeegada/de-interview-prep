---
title: "CDC Streaming (Debezium) — Scenarios"
topic: real-time-streaming
subtopic: cdc-streaming
content_type: scenario_question
tags: [cdc, debezium, kafka, interview, scenarios, data-sync, exactly-once, schema]
---

# CDC Streaming (Debezium) — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Real-Time Data Warehouse Population

**Scenario:** Your company has an e-commerce MySQL database (50 tables, 100 GB, 10,000 transactions/minute). The data team needs a near-real-time replica in Snowflake for analytics. Currently, a nightly ETL job runs at 2 AM. Business wants data available within 5 minutes. Design the solution.

<details>
<summary>💡 Hint</summary>
Design end-to-end: MySQL → Debezium → Kafka → Snowflake Kafka Connector → Snowflake Streams + Tasks. Think about snapshot mode, SMTs for PII masking, latency budget per component, and initial migration approach.
</details>

<details>
<summary>✅ Solution</summary>

```
Architecture:
  MySQL (production) → Debezium → Kafka → Snowflake Kafka Connector → Snowflake

Component design:

1. Debezium MySQL Connector:
   - Deploy on Kafka Connect cluster (3 workers for HA)
   - Tables: ecommerce.orders, ecommerce.customers, ecommerce.products, ... (50 tables)
   - topic.prefix: prod
   - Topics created: prod.ecommerce.orders, prod.ecommerce.customers, etc.
   - snapshot.mode: initial (full snapshot on first run, then stream changes)
   - SMT: ExtractNewRecordState (unwrap Debezium envelope → flat record)
   - SMT: MaskField (redact SSN, credit card numbers before Kafka)
   - Schema: Avro + Schema Registry (ensures schema evolution is handled)
   
   MySQL prerequisites:
   - log_bin = ROW (already enabled for replication)
   - binlog_row_image = FULL
   - expire_logs_days = 7 (retain 7 days for recovery)
   - Debezium user: REPLICATION SLAVE + SELECT grants

2. Kafka cluster:
   - 6 brokers, replication factor = 3
   - Topic partitions: 50 tables × 4 partitions each = 200 partitions
   - Retention: 7 days (allows reprocessing without re-snapshot)
   - Message format: Avro + Confluent Schema Registry

3. Snowflake Kafka Connector (Confluent):
   - Reads Kafka topics → loads into Snowflake staging tables
   - snowflake.ingestion.method: SNOWPIPE_STREAMING (lowest latency, < 1 min)
   - Snowflake table per topic (created automatically)
   - Destination: RAW.ECOMMERCE_CDC schema
   - Buffer: 1 minute (reduces Snowpipe API calls)

4. Transformation in Snowflake (Snowflake Tasks + Streams):
   -- Snowflake STREAM: tracks changes to raw CDC table
   CREATE STREAM raw_orders_stream ON TABLE RAW.ECOMMERCE_CDC.ORDERS;
   
   -- Task: merge raw CDC into cleaned analytics table every 5 minutes
   CREATE TASK merge_orders_task
     WAREHOUSE = analytics_wh
     SCHEDULE = '5 MINUTES'
   AS
   MERGE INTO ANALYTICS.ORDERS AS target
   USING (
     SELECT * FROM raw_orders_stream WHERE __op != 'd'
   ) AS source
   ON target.order_id = source.order_id
   WHEN MATCHED THEN UPDATE SET
     status = source.status,
     amount = source.amount,
     updated_at = source.updated_at
   WHEN NOT MATCHED THEN INSERT (order_id, user_id, status, amount, ...)
     VALUES (source.order_id, source.user_id, source.status, source.amount, ...);
   
   -- Handle deletes
   DELETE FROM ANALYTICS.ORDERS
   WHERE order_id IN (SELECT order_id FROM raw_orders_stream WHERE __op = 'd');

5. Latency breakdown:
   MySQL change → Debezium reads binlog: < 100ms
   Debezium → Kafka: < 100ms
   Kafka → Snowpipe Streaming: < 60 seconds (buffer)
   Snowflake STREAM + Task (every 5 min): < 5 minutes
   Total: < 5 minutes ✓ (meets business requirement)

6. Initial migration:
   Week 1: Deploy Debezium (initial snapshot of 50 tables)
   Snapshot duration: 100 GB / (10 MB/sec Kafka throughput) ≈ 3 hours
   During snapshot: production read replica used (no production impact)
   Week 2: Validate CDC data matches nightly ETL
   Week 3: Decommission nightly ETL job

Cost vs. nightly ETL:
  Kafka cluster: $2,000/month
  Debezium/Kafka Connect: $500/month (3 small VMs)
  Snowpipe Streaming: $0.06/credit (minimal)
  Vs. nightly ETL: $0 (internal Spark cluster)
  Net cost increase: ~$2,500/month
  Business value: 5-minute latency enables same-day analytics decisions
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Debezium Connector Lost Its Binlog Position

**Scenario:** After a 6-hour Kafka Connect cluster outage, your Debezium MySQL connector failed to restart. Error: "The connector is trying to read binlog starting at 'mysql-bin.000042:154', but this is no longer available on the server." MySQL binlog only retains 48 hours but the issue is the specific file was purged. How do you recover without data loss?

<details>
<summary>💡 Hint</summary>
Diagnose whether the specific binlog file was purged. Two recovery paths: Option A (re-snapshot with backfill for the gap — simpler but has a data gap), Option B (point-in-time with schema_only snapshot to current position — no gap but complex).
</details>

<details>
<summary>✅ Solution</summary>

```
Root cause:
  MySQL rotates binlog files every N MB or on restart
  expire_logs_days = 2 (48 hours) — but files can be purged earlier
  During 6-hour outage: MySQL did not purge binlog (only 6 hours old)
  
  Actual problem: MySQL's active binlog position was manually purged
  OR: the binary log file was rotated AND the old file was deleted
  OR: MySQL server was restarted with log_bin disabled, then re-enabled
  
  Diagnosis:
    SHOW BINARY LOGS;  -- list available binlog files
    → mysql-bin.000042 NOT in list → file purged
    Current position: mysql-bin.000050:4567

Recovery options:

Option A: Snapshot reset (simplest — some data gap)
  1. Delete Debezium connector offsets (stored in Kafka offset topic):
     kafka-consumer-groups.sh --bootstrap-server kafka:9092 
       --group connect-mysql-ecommerce-connector --reset-offsets --to-earliest --execute
  
  2. Delete the database history Kafka topic:
     kafka-topics.sh --delete --topic dbhistory.ecommerce --bootstrap-server kafka:9092
  
  3. Update connector config: snapshot.mode = initial
  
  4. Restart connector: takes full snapshot of current state → publishes as 'r' events
  
  5. Gap: changes during 6-hour outage + changes between outage end and snapshot completion
     are NOT in Kafka
     
  6. Reconcile gap: compare Kafka events with MySQL at current state
     Query: INSERT INTO ecommerce.orders_cdc_backfill
            SELECT * FROM orders WHERE updated_at BETWEEN :outage_start AND :snapshot_time
     Publish these rows manually to Kafka as synthetic 'u' events

Option B: Point-in-time recovery (no data gap, complex)
  1. Identify current binlog position: SHOW MASTER STATUS; → mysql-bin.000050:4567
  2. Start connector at current position (skip history):
     Delete database.history topic
     Configure: snapshot.mode = schema_only
     Set: database.include.list to current tables
     Set initial binlog position via Kafka offsets API to mysql-bin.000050:4567
  
  3. This starts streaming from now forward (no history)
  
  4. Backfill gap from MySQL using timestamp range:
     SELECT * FROM orders WHERE updated_at BETWEEN :last_connector_ts AND NOW()
     Publish to Kafka with synthetic CDC format

Best practice to prevent this:
  1. Set expire_logs_days = 7 (at least 2× max expected outage + snapshot time)
  2. Monitor: Debezium connector lag (alert if > 1 hour behind)
  3. Alert: connector status != RUNNING → immediate response (before binlog purge)
  4. PostgreSQL: replication slot prevents WAL purge (no equivalent for MySQL binlog)
     → Must monitor connector actively and respond to failures quickly

Implemented fix:
  Option A chosen (faster, data team accepts 6-hour gap, backfill query run separately)
  
  Prevention:
  - MySQL: expire_logs_days = 7
  - PagerDuty alert: connector_status != RUNNING → P1 alert → 15-minute response SLA
  - Health dashboard: binlog lag in hours (alert if > 4 hours = half the retention window)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: CDC for GDPR Right-to-Erasure Compliance

**Scenario:** You use CDC to replicate a MySQL customers table to Kafka → S3 (Parquet files, 90-day retention) → Snowflake → Elasticsearch. A GDPR erasure request comes in for customer_id=78421. How do you erase this customer's data across all systems?

<details>
<summary>💡 Hint</summary>
Map all systems that contain this customer's data (SQL, Kafka topic history, S3 Parquet files, Snowflake, Elasticsearch). For each, pick the right erasure method: DELETE for SQL, crypto shredding for S3 (delete KMS key), tombstone for Kafka.
</details>

<details>
<summary>✅ Solution</summary>

```
Challenge: CDC creates immutable copies of data across multiple systems.
           GDPR requires erasure from ALL copies.

Systems with customer data:
  1. MySQL (source of truth): DELETE FROM customers WHERE customer_id = 78421
  2. Kafka topic: historical CDC events contain customer data
  3. S3 Parquet files: 90 days of archived CDC events
  4. Snowflake: replicated via CDC
  5. Elasticsearch: product search index (unlikely to have PII, check schema)

Step 1: Source database (MySQL)
  DELETE FROM customers WHERE customer_id = 78421;
  
  Debezium captures the DELETE → publishes op=d event to Kafka:
  {"op": "d", "before": {"customer_id": 78421, "email": "...", ...}, "after": null}
  
  Downstream consumers see the delete and remove from their systems.

Step 2: Kafka topic (historical events with PII)
  Problem: Kafka topic contains historical INSERT/UPDATE events with customer data
  Standard Kafka retention: 7 days → events naturally expire
  
  For immediate erasure: Kafka supports "null tombstone" per key
    kafka-producer.sh --topic pg.public.customers --key customer_id=78421 --value null
    With log compaction enabled: null tombstone marks key for deletion on next compaction
    
  For non-compacted topics: wait for natural retention expiry (7 days)
  OR: rewrite Kafka topic (very complex, not recommended)
  
  Document: "Kafka CDC events for customer 78421 will expire by [date 7 days from now]"
  Legal: typically acceptable under GDPR "reasonable measures" + log retention exemption

Step 3: S3 Parquet files (90-day archive)
  This is the hardest part: immutable Parquet files on S3
  
  Option A: Encryption-based erasure ("crypto shredding"):
    When originally written, customer PII columns are encrypted with a per-customer key
    Key stored in AWS KMS
    Erasure = delete the encryption key from KMS
    Encrypted data in S3 becomes unreadable (effectively erased)
    Files don't need to be rewritten
    
    Implementation:
      Encrypt: email = AES-256(email, KMS.encrypt(customer_id))
      Erase: KMS.delete_key(customer_id)  # key deletion is immediate
      S3 files still exist but PII is cryptographically inaccessible
  
  Option B: File rewrite (if crypto shredding not implemented):
    Find all S3 files containing customer_id=78421:
      SELECT DISTINCT "$path" FROM s3_parquet_catalog
      WHERE customer_id = 78421
    For each file: read, filter out customer's rows, rewrite
    Replace original file with redacted version
    
    This is expensive for large datasets but provides true physical erasure.

Step 4: Snowflake
  DELETE FROM ANALYTICS.CUSTOMERS WHERE customer_id = 78421;
  
  Also: any tables derived from customers (orders joined with customer PII)
  Run Snowflake Data Sharing lineage to find all derived tables
  Execute DELETE/UPDATE to null PII columns in each

Step 5: Elasticsearch
  curl -X DELETE 'http://elasticsearch:9200/customers/_doc/78421'
  
  If no separate customer index (customers embedded in orders):
    curl -X POST 'http://elasticsearch:9200/orders/_update_by_query' \
      -d '{"query": {"term": {"customer_id": 78421}},
           "script": {"source": "ctx._source.customer_email=null; ctx._source.customer_name=null"}}'

Step 6: Audit trail
  Store erasure record in compliance database:
  {
    "request_id": "GDPR-2024-001",
    "customer_id": 78421,
    "requested_at": "2024-01-15T10:00:00Z",
    "completed_at": "2024-01-15T11:30:00Z",
    "systems_erased": ["MySQL", "Snowflake", "Elasticsearch"],
    "systems_pending_expiry": ["Kafka (expires 2024-01-22)", "S3 (crypto-shredded)"],
    "operator": "data_privacy_team"
  }
  
  GDPR deadline: 30 days from request
  Completed: 1.5 hours ✓
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "How does CDC handle tables without a primary key?" — Without a primary key, Debezium cannot uniquely identify a row in the change event. Behavior: for MySQL, Debezium uses the row's unique identifier from the binlog (a physical row ID, not a business key). For PostgreSQL: requires REPLICA IDENTITY FULL to include all column values. Without a PK, deduplication in downstream systems is harder (no stable key for UPSERT). Best practice: every table tracked by CDC should have a primary key. If tables lack PKs (legacy), add a synthetic UUID column or use a composite key. Alternatively: use the full row hash as an idempotency key in the consumer.

> **Tip 2:** "What is the overhead of Debezium on the source database?" — Debezium reads the transaction log (binlog/WAL), which is already written by the database for its own recovery purposes. The overhead on MySQL: Debezium registers as a replica (MySQL's replication client). MySQL sends binlog events over the network (same as any replica). CPU/IO on MySQL: minimal (log reading is sequential I/O, not table scans). The only concern: MySQL waits for all replicas to acknowledge before purging binlog files — with expire_logs_days set, this is not a problem. For PostgreSQL: replication slots hold WAL files until consumed → monitor slot lag to prevent disk fill. Overall: CDC is much less intrusive than polling-based CDC (no SELECT scans on production tables).

> **Tip 3:** "What happens during a Debezium snapshot and how long does it take?" — Initial snapshot: Debezium takes a global read lock (MySQL) or uses a transaction with REPEATABLE READ isolation (PostgreSQL) to ensure a consistent point-in-time view. Then reads all rows from tracked tables with `SELECT * FROM table ORDER BY primary_key`. For each row: publishes an `op=r` event to Kafka. Lock is released immediately after establishing the consistent read transaction. Snapshot duration: proportional to table size and network throughput. 100 GB of data at 50 MB/sec throughput ≈ 33 minutes. During snapshot: no new events are missed (Debezium notes the binlog position at snapshot start, then streams changes that happened during the snapshot after it completes). After snapshot: streaming begins from the noted binlog position.

