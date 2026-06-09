---
title: "Apache Flink — Fundamentals"
topic: real-time-streaming
subtopic: apache-flink
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [flink, streaming, real-time, datastream-api, event-time, watermarks]
---

# Apache Flink — Fundamentals

## What Is Apache Flink?

Apache Flink is a **stateful stream processing framework** designed for high-throughput, low-latency, exactly-once event processing at scale.

```
Flink positioning:

                    Latency     Throughput   State     Exactly-once
Apache Flink        ~10ms       Very high    Rich      ✓ (native)
Spark Streaming     ~100ms      High         Limited   ✓ (with sinks)
Kafka Streams       ~10ms       High         Local     ✓ (limited)
Storm               ~2ms        Medium       None      At-least-once
ASA (Azure)         ~100ms      High         Limited   At-least-once

Flink strengths:
  - True streaming (record-by-record), not micro-batching
  - Event-time processing with watermarks (handles late data correctly)
  - Rich stateful operations (ValueState, MapState, ListState)
  - Exactly-once guarantees end-to-end with supported sinks
  - Unified batch and streaming API (same code runs on bounded or unbounded data)
```

---

## Core Architecture

```
Flink cluster architecture:

  JobManager (master)
  ├── Dispatcher:          accepts job submissions
  ├── ResourceManager:     manages TaskManager slots
  └── JobMaster:           coordinates a specific job execution

  TaskManagers (workers)
  ├── Task Slots:          unit of parallelism (1 slot = 1 thread)
  └── Network Buffers:     inter-task data exchange

Execution:
  Job → JobGraph (logical DAG) → ExecutionGraph (physical, with parallelism)
  Task = operator running in a slot
  
  Operator chaining:
    Adjacent operators fused into one task (avoids network overhead)
    Chained when: same parallelism, same slot sharing group
    Forced chain break: operator.startNewChain() or disableChaining()

Data exchange:
  Forward:     upstream partition → same downstream partition (local, fast)
  Hash:        keyBy() → hash(key) % parallelism → specific partition
  Broadcast:   all upstream → all downstream partitions (state broadcast)
  Rebalance:   round-robin across downstream (load balancing)
```

---

## DataStream API Basics

```java
// Maven dependency:
// <dependency>
//   <groupId>org.apache.flink</groupId>
//   <artifactId>flink-streaming-java</artifactId>
//   <version>1.18.0</version>
// </dependency>

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;

public class FlinkBasics {
    public static void main(String[] args) throws Exception {
        
        // 1. Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);  // global parallelism
        
        // 2. Source (socket for testing)
        DataStream<String> raw = env.socketTextStream("localhost", 9999);
        
        // 3. Transformations
        DataStream<Order> orders = raw
            .filter(line -> !line.isEmpty())                    // FilterFunction
            .map(line -> Order.parse(line))                     // MapFunction: 1-to-1
            .filter(o -> o.getAmount() > 0);                    // filter invalid
        
        // 4. KeyBy + aggregation
        DataStream<SalesTotal> totals = orders
            .keyBy(Order::getCategory)                          // partition by key
            .sum("amount");                                     // built-in aggregation
        
        // 5. Sink
        totals.print();
        
        // 6. Execute (triggers execution)
        env.execute("Order Processing Job");
    }
}

// Order POJO
public class Order {
    public String orderId;
    public String category;
    public double amount;
    public long timestamp;
    
    public static Order parse(String line) {
        String[] parts = line.split(",");
        Order o = new Order();
        o.orderId   = parts[0];
        o.category  = parts[1];
        o.amount    = Double.parseDouble(parts[2]);
        o.timestamp = Long.parseLong(parts[3]);
        return o;
    }
}
```

---

## Event Time vs Processing Time

```
Three notions of time in Flink:

1. Event Time (recommended for analytics)
   - Time when event actually occurred (in the data)
   - Correct results even with late/out-of-order data
   - Requires watermarks to track progress

2. Processing Time (default)
   - Wall-clock time when Flink processes the event
   - Simple, no late data handling needed
   - Non-deterministic (results vary with load, restarts)

3. Ingestion Time
   - Time when event enters Flink source
   - Between event and processing time

When to use which:
  Event time:      financial trades, user clicks, IoT sensors (correctness matters)
  Processing time: monitoring dashboards (low latency matters, slight inaccuracy ok)
  Ingestion time:  Kafka-sourced data with known ingestion delay

// Set event time in Flink:
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  // Flink < 1.12
// In Flink 1.12+, event time is the default
```

---

## Kafka Source and Sink

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

// Kafka Source (Flink 1.15+ connector)
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka-broker:9092")
    .setTopics("orders-topic")
    .setGroupId("flink-order-processor")
    .setStartingOffsets(OffsetsInitializer.earliest())  // or latest(), committed()
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> stream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),  // add watermarks separately
    "Kafka Orders Source"
);

// Kafka Sink
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka-broker:9092")
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("processed-orders")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
    )
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)  // requires Kafka transactions
    .build();

stream.sinkTo(sink);
```

---

## Interview Tips

> **Tip 1:** "What's the difference between Flink and Spark Streaming?" — Flink is a true streaming engine: it processes one record at a time, has native event-time support, and achieves exactly-once with lightweight checkpointing. Spark Structured Streaming uses micro-batching (small batches every 100ms–seconds), which adds latency. For sub-second latency requirements or complex event-time windowing with late data, Flink is the better choice. For teams already on the Spark ecosystem, Spark Streaming offers integration with Delta Lake, MLlib, and the broader Spark ecosystem.

> **Tip 2:** "What is operator chaining in Flink and why does it matter?" — Operator chaining fuses consecutive operators (e.g., map → filter → flatMap) into a single task that runs in one thread, avoiding the overhead of network serialization between operators. Result: fewer task slots used, less memory for network buffers, and faster execution. Flink chains operators automatically when they have the same parallelism and are not separated by a keyBy (which requires a network shuffle). Chaining can be disabled globally (`env.disableOperatorChaining()`) or per-operator (`operator.disableChaining()`) when debugging.

> **Tip 3:** "What are task slots in Flink?" — A task slot is the basic unit of resource allocation in a TaskManager. Each slot runs one parallel task (one partition of an operator). If parallelism=4 and you have 2 TaskManagers with 2 slots each, each slot runs one parallel instance. Slots from different TaskManagers can be shared (Slot Sharing) — operators from the same job can share a slot, which allows a job with parallelism 4 to run on 4 slots total (not 4 × N for N operators). This reduces the minimum number of TaskManager slots needed.
