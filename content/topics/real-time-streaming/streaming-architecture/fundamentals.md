---
title: "Streaming Architecture Patterns — Fundamentals"
topic: real-time-streaming
subtopic: streaming-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, architecture, kafka, message-broker, event-driven, pub-sub, queue]
---

# Streaming Architecture Patterns — Fundamentals


## 🎯 Analogy

Think of streaming architecture like a river system: data flows continuously from sources (IoT sensors, clickstreams, transactions) through processing nodes (Flink, Spark Streaming), and into sinks (Delta Lake, dashboards, APIs) — the water never stops flowing.

---
## Messaging Systems Overview

```
Core messaging concepts:

Message Queue (point-to-point):
  Producer → Queue → Consumer
  One message consumed by ONE consumer
  Message deleted after consumption
  Use: task distribution, work queues (RabbitMQ, SQS, Azure Service Bus)

Pub/Sub (publish-subscribe):
  Publisher → Topic → [Subscriber A]
                   → [Subscriber B]
                   → [Subscriber C]
  One message delivered to MULTIPLE subscribers
  Message retained for specified time
  Use: event broadcasting, multiple downstream systems

Event Streaming Platform (Kafka/Kinesis):
  Producer → Topic (immutable log) → [Consumer Group A]
                                   → [Consumer Group B]
                                   → [Consumer Group C]
  Records retained for N days (configurable)
  Each consumer group tracks its own offset (independent progress)
  Can replay from any point in the retention window
  
  Key difference from pub/sub:
    MQ/PubSub: messages consumed and gone
    Event stream: messages retained, replayable, multiple consumers

Comparison:

                     RabbitMQ/SQS    Pub/Sub (SNS)   Kafka/Kinesis
  Retention          Until consumed  Until consumed   Days-to-years
  Ordering           Queue FIFO      No              Per-partition
  Multiple consumers No (MQ)        Yes              Yes (per group)
  Replay             No             No               Yes
  Throughput         Medium         High             Very high
  Use case           Task queues    Notifications    Streaming analytics
```

---

## Kafka Architecture for Streaming

```
Kafka cluster:
  Brokers:    3-9 nodes (store topic data, serve reads/writes)
  ZooKeeper/  coordination service (metadata, leader election)
  KRaft:      (Kafka 3.x replaces ZooKeeper with built-in KRaft)
  
  Topic:      logical category of events
    Partitions: physical units of parallelism (immutable ordered log)
    Replication: each partition replicated to N brokers (HA)
  
  Producer → any broker → leader partition → followers (replication) → consumer

Message flow:
  1. Producer: choose partition (round-robin or hash(key))
  2. Producer: append to leader partition's log
  3. Leader: replicate to follower brokers
  4. Consumer: read from partition at current offset
  5. Consumer: commit offset after processing

Kafka use cases in streaming architecture:
  Source:    IoT sensors, application events, DB CDC events, clickstream
  Buffer:    decouple fast producers from slow consumers (absorb bursts)
  Log:       durable event log (replay for new consumers, debugging)
  Integration hub: connect microservices without tight coupling
  
Consumer Group:
  Multiple consumers in same group → partition data split between them
  Consumer A: reads partitions 0, 1, 2
  Consumer B: reads partitions 3, 4, 5
  → Scale: add consumers up to partition count (more consumers = more parallel processing)
  
  Different consumer groups = independent processing
  Analytics group: computes metrics (reads all partitions)
  Fraud group:     detects fraud (reads all partitions independently)
  Both groups read the same events at their own pace
```

---

## Event-Driven Architecture

```
Event-Driven Architecture (EDA):
  Services communicate via events (not direct API calls)
  
  Traditional (request-response):
    Order Service → [HTTP call] → Inventory Service → [HTTP call] → Payment Service
    Problems: tight coupling, cascade failures, synchronous blocking
  
  Event-driven:
    Order Service → [ORDER_PLACED event → Kafka]
                                          ↓
                              Inventory Service (reads from Kafka)
                              Payment Service (reads from Kafka)
                              Notification Service (reads from Kafka)
    Benefits: loose coupling, services fail independently, easy to add new consumers
  
  Events vs Commands:
    Event:   "OrderPlaced" — something happened (past tense, factual)
    Command: "PlaceOrder"  — request to do something (future, may fail)
    
    In streaming: primarily events (CDC events, user events, sensor readings)
    Commands: used in CQRS / SAGA patterns

EDA patterns:
  1. Event Notification:
     Service publishes event for others to react to
     Example: UserRegistered → Email service sends welcome email
     
  2. Event-Carried State Transfer:
     Event contains full new state (not just "what changed")
     Example: OrderUpdated {order_id, status, amount, user_id, ...}
     Consumers don't need to call back to get full order (self-contained event)
     
  3. Event Sourcing:
     Store ALL events, derive current state by replaying
     Example: bank account = replay of all transactions
     
  4. CQRS (Command Query Responsibility Segregation):
     Write path: commands → event store
     Read path:  materialized views updated by events
     Example: write orders to MySQL, maintain read-optimized Redis cache via CDC events
```

---

## Stream Processing Topologies

```
Topology: the graph of source → processors → sinks

Simple linear topology:
  Kafka Source → Filter → Map → Kafka Sink
  
Fan-out topology:
  Kafka Source → [Flink job] → Kafka Sink A (alerts)
                             → Delta Lake (archive)
                             → Elasticsearch (search)

Fan-in topology:
  Kafka Source A (orders)  ─┐
  Kafka Source B (payments) ─┤→ Flink join → Kafka Sink (matched)
  
Enrichment topology:
  Kafka Source (events)
       │
       ├──→ [async lookup Redis] → enriched events → Kafka Sink
       
Pipeline topology (multi-stage):
  Kafka (raw) → Bronze (Spark) → Silver (Flink) → Gold (aggregations) → Serving

Topology design principles:
  1. Decouple: each stage writes to Kafka (enables independent scaling, replay)
  2. Idempotent sinks: any stage can be replayed without duplicate data
  3. One responsibility per stage: enrichment, validation, and aggregation in separate jobs
  4. Checkpoint between stages: if Silver fails, replay from Kafka (Bronze still intact)
```

---


## ▶️ Try It Yourself

```python
# Architecture sketch: Kafka → Flink/Spark → Delta/Sink
# This demo simulates the producer → consumer flow locally

import threading
import queue
import time
import json

# Simulate Kafka as a Python queue
kafka_topic = queue.Queue(maxsize=1000)

def producer():
    for i in range(10):
        event = {"id": i, "event": "page_view", "ts": time.time()}
        kafka_topic.put(json.dumps(event))
        time.sleep(0.1)

def consumer():
    buffer = []
    while True:
        try:
            msg = kafka_topic.get(timeout=1)
            buffer.append(json.loads(msg))
            if len(buffer) >= 3:  # Mini-batch of 3
                revenue = len(buffer)  # Simulate aggregation
                print(f"Micro-batch: {len(buffer)} events processed")
                buffer.clear()
        except queue.Empty:
            break

t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)
t1.start(); t2.start()
t1.join(); t2.join()
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between a message queue and an event streaming platform?" — Message queue (SQS, RabbitMQ): messages are consumed and deleted. One message, one consumer. No ordering guarantee across messages (except FIFO queues). No replay. Use for task distribution where each task needs exactly one worker. Event streaming (Kafka, Kinesis): events are retained in an immutable log. Multiple independent consumer groups each read the same events at their own pace. Replay is possible from any offset within the retention window. Use for: analytics (multiple downstream systems), audit logs, event sourcing, and any scenario requiring data replayability or multiple consumers.

> **Tip 2:** "What is the role of Kafka in a microservices architecture?" — Kafka serves as the integration backbone: services publish events to Kafka when their state changes; other services subscribe to relevant events and react asynchronously. This decouples services: Order Service doesn't call Inventory Service directly (which would create tight coupling and cascade failures). Instead, Order Service publishes an `ORDER_PLACED` event. Inventory Service, Payment Service, and Notification Service each independently subscribe to this event and process it in their own time. Benefits: services can be deployed independently, fail independently, and scale independently. New services can be added without modifying existing ones (just subscribe to existing events).

> **Tip 3:** "What are the trade-offs between event-driven and request-response architectures?" — Request-response (REST/gRPC): immediate response (synchronous), simple to reason about, easy error handling (HTTP status codes), strong consistency (caller knows if request succeeded). Trade-offs: tight coupling (caller must know callee's address), cascade failures (if one service is slow, all callers wait), harder to scale asynchronously. Event-driven: loose coupling, high throughput, resilient to individual service failures, naturally supports multiple consumers. Trade-offs: eventual consistency (caller doesn't know if event was processed), harder to debug (distributed trace across multiple services), more complex error handling (what if consumer fails to process event?). Modern architectures use both: synchronous for user-facing APIs (low latency, immediate feedback), event-driven for background processing and service integration.
