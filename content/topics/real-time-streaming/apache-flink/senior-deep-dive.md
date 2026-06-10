---
title: "Apache Flink — Senior Deep Dive"
topic: real-time-streaming
subtopic: apache-flink
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [flink, savepoints, rocksdb, backpressure, two-phase-commit, production, tuning]
---

# Apache Flink — Senior Deep Dive

## Savepoints vs Checkpoints

```
Checkpoints:                          Savepoints:
  Automatic, periodic                   Manual, explicit
  Lightweight (incremental)             Full state snapshot
  Deleted when superseded               Retained until manually deleted
  Failure recovery only                 Job upgrades, A/B tests, rollbacks
  Managed by Flink                      Managed by operator
  Format: binary (internal)            Format: portable (can change Flink version)

Savepoint use cases:
  1. Code upgrade:       stop job → savepoint → deploy new version → restore from savepoint
  2. Flink upgrade:      savepoint → upgrade Flink version → restore
  3. Rescaling:          savepoint → change parallelism → restore (state redistributed)
  4. A/B test:           savepoint → branch into two jobs → compare outputs
  5. Scheduled backup:   periodic savepoints to S3 for disaster recovery

# CLI commands:
flink savepoint <jobId> s3://bucket/savepoints/        # trigger savepoint
flink run -s s3://bucket/savepoints/<id> myJob.jar     # restore from savepoint

# For incremental checkpoints (RocksDB only):
env.enableCheckpointing(60_000);
RocksDBStateBackend backend = new RocksDBStateBackend("s3://bucket/checkpoints/", true);
// true = incremental; only changed SST files uploaded (much smaller than full snapshots)
env.setStateBackend(backend);

/*
 Incremental checkpoint mechanics (RocksDB):
   Full checkpoint: all RocksDB SST files uploaded
   Incremental: only new/changed SST files since last checkpoint
   State: list of all referenced SST files (some shared across checkpoints)
   Recovery: download only the SST files needed (may be spread across multiple checkpoints)
   
   Savings: 90%+ checkpoint size reduction for large state
   Trade-off: recovery takes longer (must fetch files from multiple checkpoints)
*/
```

---

## Two-Phase Commit and Exactly-Once E2E

```java
/*
 Exactly-once end-to-end (source + processing + sink):
 
 Step 1: Source offset committed only when checkpoint completes
         (Kafka consumer: offset saved in checkpoint, not auto-committed)
 
 Step 2: Processing: idempotent operators (no side effects)
 
 Step 3: Sink: participates in 2-phase commit
 
 2PC protocol for transactional sinks:
   Pre-commit (on checkpoint barrier):
     Sink opens Kafka transaction / Iceberg staging
     Writes records to transaction (not yet visible to consumers)
     Reports "pre-committed" to Flink
   
   Commit (when checkpoint completes):
     JobManager signals all operators: checkpoint done
     Sink commits the Kafka transaction / Iceberg commit
     Records become visible to consumers
   
   Abort (on checkpoint failure):
     JobManager signals abort
     Sink rolls back the open transaction
*/

// TwoPhaseCommitSinkFunction: extend this for custom exactly-once sinks
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

public class ExactlyOnceDatabaseSink 
        extends TwoPhaseCommitSinkFunction<Order, Connection, Void> {
    
    @Override
    protected Connection beginTransaction() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        conn.setAutoCommit(false);
        return conn;
    }
    
    @Override
    protected void invoke(Connection conn, Order order, Context ctx) throws Exception {
        // Write to DB within transaction (not visible yet)
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO orders_processed VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
        );
        ps.setString(1, order.getOrderId());
        ps.setString(2, order.getCategory());
        ps.setDouble(3, order.getAmount());
        ps.executeUpdate();
    }
    
    @Override
    protected void preCommit(Connection conn) throws Exception {
        // Flush but don't commit (called when checkpoint barrier passes sink)
    }
    
    @Override
    protected void commit(Connection conn) {
        // Called when checkpoint is fully complete — make data visible
        try { conn.commit(); conn.close(); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
    
    @Override
    protected void abort(Connection conn) {
        // Called on checkpoint failure — roll back
        try { conn.rollback(); conn.close(); }
        catch (Exception e) { /* log */ }
    }
}
```

---

## Backpressure Diagnosis and Tuning

```
Backpressure: upstream operator produces faster than downstream can consume
  → buffers fill up → upstream slows down → input queues grow → high latency

Flink Web UI backpressure indicators:
  OK:          OK (< 10% backpressured)
  LOW:         Low (10-50% backpressured)
  HIGH:        High (> 50% backpressured — investigate immediately)

How to diagnose:
  1. Open Flink Web UI → Job → click each operator
  2. Find the operator showing HIGH backpressure
  3. The bottleneck is that operator (or its downstream)
  4. Check: CPU usage, GC pause, I/O wait, checkpoint duration

Tuning strategies:
  1. Increase parallelism of bottleneck operator:
       .setParallelism(8)  // for this operator only
     
  2. State backend tuning (RocksDB):
       // Enable bloom filters for faster point lookups
       RocksDBOptionsFactory factory = new DefaultConfigurableOptionsFactory()
           .setBlockCacheSize("256mb")
           .setWriteBufferSize("64mb")
           .setMaxWriteBufferNumber(3);
       env.setStateBackend(new EmbeddedRocksDBStateBackend());
     
  3. Network buffer tuning:
       // Increase buffer timeout (trade latency for throughput)
       env.setBufferTimeout(100);  // ms; default=100, set 0 for lowest latency
       
       // Buffer pool size (increase for high-throughput)
       taskmanager.network.memory.fraction: 0.1  # of total memory
       taskmanager.network.memory.min: 64mb
       taskmanager.network.memory.max: 1gb
     
  4. Async I/O for external lookups (avoid blocking network calls):
       DataStream<EnrichedOrder> enriched = AsyncDataStream.unorderedWait(
           orders,
           new AsyncDatabaseLookup(),   // implements AsyncFunction
           timeout=5, TimeUnit.SECONDS,
           capacity=100                  // max concurrent requests
       );

  5. GC tuning:
       -Xmx8g -Xms8g                        # fixed heap (avoid GC from resizing)
       -XX:+UseG1GC                          # G1 for large heaps
       -XX:MaxGCPauseMillis=200             # target GC pause
       -XX:ParallelGCThreads=4
```

---

## Flink Production Architecture

```
Production Flink deployment on Kubernetes (Flink Operator):

apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: order-processing-job
spec:
  image: my-registry/flink-job:1.18.0
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.checkpoints.dir: "s3://bucket/checkpoints/"
    state.savepoints.dir: "s3://bucket/savepoints/"
    execution.checkpointing.interval: "60000"
    execution.checkpointing.mode: EXACTLY_ONCE
    state.backend: rocksdb
    state.backend.incremental: "true"
    metrics.reporters: "prometheus"
    metrics.reporter.prometheus.class: "org.apache.flink.metrics.prometheus.PrometheusReporter"
  serviceAccount: flink-service-account
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 4
  job:
    jarURI: s3://bucket/jars/order-processing-1.0.jar
    parallelism: 8
    upgradeMode: savepoint  # use savepoint when deploying new version

Monitoring:
  JVM metrics:     heap used, GC count/time
  Flink metrics:   numRecordsInPerSecond, numRecordsOutPerSecond, currentInputWatermark
  Checkpoint:      checkpointDuration, checkpointSize, numberOfFailedCheckpoints
  Backpressure:    outPoolUsage (> 0.8 = backpressured), inPoolUsage
  Lag:             currentInputWatermark vs system time = watermark lag (should be < allowed lateness)

Alerting thresholds:
  Checkpoint duration > 5 minutes → investigate
  numberOfFailedCheckpoints > 2 → page on-call
  backpressure = HIGH for > 2 minutes → investigate
  watermark lag > 2× allowed lateness → source or processing issue
```

---

## Interview Tips

> **Tip 1:** "How do you upgrade a Flink job without data loss?" — Stop the running job with a savepoint: `flink stop --savepointPath s3://bucket/savepoints/ <jobId>`. Deploy the new code. Start the new job restoring from the savepoint: `flink run -s s3://bucket/savepoints/<id> newJob.jar`. Key constraints: operator UIDs must be stable (add `uid("my-operator-id")` to each operator in code — Flink uses these to match state during restore). If the state schema changes, implement state schema evolution using `TypeSerializer` migration. If parallelism changes, state is automatically redistributed (for KeyedState: rehashed; for OperatorState: redistributed across new instances).

> **Tip 2:** "What is RocksDB in the context of Flink and when should you use it?" — RocksDB is an embedded key-value store (LSM-tree based) used as Flink's state backend when state is too large for JVM heap. Instead of keeping all state in memory, Flink stores state in RocksDB on the TaskManager's local disk, with active working sets in a block cache. Use RocksDB when: state per key exceeds 100MB total, you have millions of distinct keys, or you see OutOfMemoryErrors with the default HashMapStateBackend. Trade-offs: RocksDB state lookups are slightly slower than heap (disk I/O + deserialization), but enables state sizes of hundreds of GB per TaskManager.

> **Tip 3:** "How does Flink handle the exactly-once guarantee at the sink?" — Flink's exactly-once guarantee is end-to-end only when the sink participates in the 2-phase commit protocol. The sink's pre-commit writes data to a transaction/staging area. The commit happens only after the checkpoint is complete. If the job fails after pre-commit but before commit, the next checkpoint will not include those records (because Kafka offsets weren't advanced in the checkpoint), and the sink's open transaction is aborted. This means those records are reprocessed in the next checkpoint cycle, producing one final committed output. Supported exactly-once sinks: Kafka (via transactions), Iceberg (via `DataFileCommitter`), JDBC (via `TwoPhaseCommitSinkFunction`).

## ⚡ Cheat Sheet

**Streaming fundamentals**
```
Event time:    when the event actually occurred (on the device)
Processing time: when the system processes it (can be much later)
Ingestion time: when it arrives at the message broker
Watermark:     max expected event time lag — defines when a window closes
Late data:     events arriving after the watermark → handled by allowedLateness or drop
```

**Apache Flink key concepts**
```java
// Keyed stream + window + aggregate
stream.keyBy(event -> event.userId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(new RevenueAggregator());

// Watermark strategy
WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.eventTimeMs);
```

**Spark Structured Streaming**
```python
# Read from Kafka
stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "orders") \
    .load()

# Window aggregation
from pyspark.sql.functions import window, col
agg = stream \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "5 minutes"), "region") \
    .sum("amount")

# Write to Delta (trigger: every 1 min or micro-batch)
agg.writeStream.format("delta").trigger(processingTime="1 minute") \
    .outputMode("append").option("checkpointLocation", "/chk/orders").start()
```

**Window types**
| Window | Description | Use case |
|---|---|---|
| Tumbling | Fixed non-overlapping | Hourly totals |
| Sliding | Fixed size, moves by slide interval | 5-min avg, every 1 min |
| Session | Gap-based (closes after inactivity) | User sessions |
| Global | Accumulates all events | Running total |

**Exactly-once semantics**
```
Source: idempotent read (Kafka offset tracking)
Processing: checkpointing (Flink) or write-ahead log (Spark)
Sink: idempotent write (Delta MERGE, upsert) or transactional sink
Kafka → Flink/Spark → Delta = exactly-once end-to-end (with checkpointing)
```

**CDC streaming (Debezium → Kafka → Lakehouse)**
```
1. Debezium captures MySQL/Postgres binlog → Kafka topic (op: c/u/d/r)
2. Flink/Spark reads Kafka topic
3. MERGE INTO Delta/Iceberg table:
   INSERT on c, UPDATE on u, DELETE on d
4. Result: real-time replicated lakehouse table
```

**Kinesis key operations**
```python
import boto3
kinesis = boto3.client('kinesis', region_name='us-east-1')
# Put record
kinesis.put_record(StreamName='orders', Data=json.dumps(event).encode(), PartitionKey=order_id)
# Get shard iterator
it = kinesis.get_shard_iterator(StreamName='orders', ShardId='shardId-000000000000',
                                 ShardIteratorType='LATEST')['ShardIterator']
# Read records
records = kinesis.get_records(ShardIterator=it, Limit=100)['Records']
```

**Stateful processing patterns**
```
Running total:    keyed state (ValueState[Double])
Sessionization:   keyed + timer-based (clear state after N seconds inactivity)
Pattern detection: CEP (Flink Complex Event Processing) — detect A then B within 5 min
Deduplication:    keyed state stores seen event IDs (with TTL for cleanup)
```

**Key interview points**
- Checkpointing: Flink snapshots operator state to S3/HDFS for fault tolerance
- Backpressure: slow downstream = upstream stops reading Kafka = natural flow control
- Parallelism = Kafka partitions: each Flink/Spark task reads one partition
- Streaming vs micro-batch: Flink = true streaming (event-by-event); Spark = micro-batch (more latency, simpler)
