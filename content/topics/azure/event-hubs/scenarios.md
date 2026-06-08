---
title: "Event Hubs — Scenarios"
topic: azure
subtopic: event-hubs
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, event-hubs, scenarios, interview, design, kafka]
---

# Event Hubs — Interview Scenarios

## Scenario 1: Design Event Hubs for a Ride-Sharing Platform

**Question:** A ride-sharing company has 500K active drivers sending GPS+status updates every 5 seconds, and 1M riders generating ride requests and status events. Design the Event Hubs ingestion layer.

**Answer:**

```
Scale calculation:
  Drivers: 500K × 1 event/5sec = 100K events/sec × 200 bytes = 20 MB/sec ingress
  Riders:  1M × 0.1 events/sec (requests + status) = 100K events/sec × 300 bytes = 30 MB/sec ingress
  Total ingress: 50 MB/sec
  
  Consumers (3 consumer groups):
    Real-time dispatch (ASA): 50 MB/sec egress
    Analytics (Databricks):   50 MB/sec egress
    Archive (Capture → ADLS): 50 MB/sec egress
  Total egress: 150 MB/sec

Event Hubs sizing:
  Tier: Premium (50 MB/sec ingress at better cost vs Standard TUs)
  Processing Units: 50MB/sec ÷ 100 MB/sec per PU = 1 PU minimum → use 2 PUs (headroom)
  Cost: 2 PUs × $0.08/hr × 720 hr/month = ~$115/month (vs Standard: 50 TUs × $21.6/month = $1,080)
  → Premium saves ~$900/month at this scale

Event Hubs layout:
  Namespace: rideshare-prod (Premium, 2 PUs)
  
  Event Hub 1: driver-location (32 partitions)
    Partition key: driver_id
    Reason: all location updates for same driver → same partition → ordered
    Event: {"driver_id":"d001","lat":40.71,"lng":-74.00,"speed":35,"status":"on_trip","ts":"..."}
    Retention: 1 day (location history not needed long-term in EH)
    Capture: enabled → ADLS for historical GPS analysis
    
  Event Hub 2: ride-events (32 partitions)
    Partition key: ride_id
    Event: {"ride_id":"r999","event_type":"pickup","rider_id":"u123","driver_id":"d001","ts":"..."}
    Retention: 7 days
    Capture: enabled → ADLS Bronze

Consumer groups per Event Hub:
  driver-location:
    dispatch-consumer    → ASA (match driver to nearby rider request)
    analytics-consumer   → Databricks (ETA model, traffic analysis)
    [capture auto]       → ADLS
  
  ride-events:
    billing-consumer     → Azure Functions (trigger billing on ride_complete event)
    analytics-consumer   → Databricks (ride completion, revenue analytics)
    [capture auto]       → ADLS

Network:
  Private endpoint for namespace (drivers connect via mobile → APIM → Event Hubs via private IP)
  APIM validates driver auth token before proxying to Event Hubs
```

---

## Scenario 2: Event Hubs Consumer Suddenly Has 6-Hour Lag

**Question:** Your Databricks Structured Streaming job reads from Event Hubs orders topic. Operations alerts you that the consumer lag is 6 hours (3.6B events behind). The job is running. What happened and how do you fix it?

**Answer:**

```
Investigation:

Step 1: Confirm the job is actually running
  Databricks UI → Job Run → Status: Running or Failed?
  If FAILED: check error log → likely OOM or transient Azure error
  If RUNNING: consumer is reading but slower than production rate

Step 2: Check Databricks Spark UI → Streaming tab
  Input rate: events/sec being read from Event Hubs
  Processing rate: events/sec being processed
  If input rate < production rate: Databricks can't read fast enough
  If input rate ≈ production rate but lag growing: production spiked

Step 3: Check Event Hubs metrics
  IncomingMessages rate: did production spike today?
  (e.g., flash sale → 10× normal traffic for 2 hours → lag accumulated)
  ThrottledRequests: was Event Hubs throttling reads? (TU limit hit)

Step 4: Check cluster resources
  Spark UI → Executors → CPU% and GC time
  If CPU 100%: cluster is compute-bound → scale up
  If GC > 20% of time: memory pressure → scale up or increase executor memory

Scenarios and fixes:

A) Production traffic spike created lag:
   Job is healthy, just catching up
   Increase cluster scale temporarily: add 4 more workers for 2 hours
   Monitor: lag should decrease as catch-up occurs
   Check: total data in retention period (7 days) to ensure events not expired

B) Cluster shrunk (auto-scaling removed workers during low load):
   Check cluster scaling history in Databricks UI
   Current cluster may have scaled to 2 workers (low overnight traffic)
   But morning traffic surge requires 10 workers
   Fix: increase min_workers in auto-scale policy, or set fixed worker count

C) Slow downstream write (writing to slow sink):
   Streaming to overloaded Azure SQL DB → writes back up
   Check: Spark UI → SQL tab → "write" operations taking long
   Fix: switch to async write, increase SQL DTU, or batch writes

D) ThrottledRequests on Event Hubs:
   Reading consumer hitting TU egress limit
   Fix: increase TUs (if Standard) or PUs (if Premium) via portal
   Long-term: optimize reads (read larger batches)

Expected outcome after fix:
  After adding compute: lag decreases at rate = (current_processing_rate - production_rate)
  If processing_rate = 2× production_rate: 6h lag clears in 6 hours
  Communicate ETA to stakeholders: "pipeline will be current by 4 PM"
```

---

## Scenario 3: Migrate from RabbitMQ to Event Hubs

**Question:** Your company uses RabbitMQ for inter-service messaging. You're moving to Azure and need to migrate to a managed Azure service. Events are order status updates (order_id, status, timestamp, metadata). Volume: 50K events/min. Recommend and design the target.

**Answer:**

```
Analysis: RabbitMQ use case
  RabbitMQ role: message broker between Order Service → Fulfillment Service → Notification Service
  Pattern: pub/sub with multiple consumers
  Volume: 50K events/min = ~833 events/sec × 1KB avg = ~833 KB/sec (< 1 MB/sec)
  Retention need: 24 hours (retry window)
  Ordering: per-order important

Decision: Event Hubs vs Service Bus
  Event Hubs: better for high-throughput analytics streaming, consumer lag tracking
  Service Bus: better for transactional messaging, per-message ACK, dead-letter queue
  
  Volume (833 events/sec) is low → both work
  Pattern is pub/sub with processing semantics → Service Bus Topics is more natural
  However: if analytics consumers also need the events (future analytics pipeline) → Event Hubs wins
  
  Recommendation: Event Hubs (fan-out to analytics + Services, unified platform)
  
  If strict exactly-once and dead-letter required for some services:
    Use Event Hubs for analytics consumers
    Use Service Bus for transactional consumers (order processing, payments)
    Decouple: one Azure Function reads Event Hubs → writes to Service Bus (bridge)

Target architecture:

Order Service (Azure App Service) →
  Event Hub: order-status-events (4 partitions, partition_key=order_id)
  Retention: 1 day (Standard tier, 4 TUs)
  
Consumer groups:
  fulfillment-consumer  → Azure Function → Fulfillment Service
  notification-consumer → Azure Function → Notification Service (email/SMS)
  analytics-consumer    → Databricks → Bronze Delta orders_status table
  capture               → ADLS (raw archive)

Migration steps:
  1. Create Event Hubs namespace and Event Hub
  2. Add Event Hubs producer to Order Service (alongside existing RabbitMQ)
  3. Create Azure Function consumers (implement same business logic as RabbitMQ handlers)
  4. Shadow mode: Order Service publishes to both RabbitMQ + Event Hubs
     Consumers read from Event Hubs only (validate behavior)
  5. Validate: compare consumer outputs between RabbitMQ and Event Hubs for 1 week
  6. Cutover: stop RabbitMQ publishing in Order Service
  7. Decommission RabbitMQ after 2-week observation

Cost: Standard tier, 4 TUs = ~$300/month
  (vs RabbitMQ VM: 2× Standard_DS2_v2 = ~$280/month → similar cost, zero management)
```

---

## Interview Tips

> **Tip 1:** "How do you ensure no events are lost during an Event Hubs outage?" — Enable Geo-DR pairing for namespace metadata protection (ensures Event Hub entity config is preserved). For event data: Enable Event Hubs Capture to ADLS — captured files survive even if Event Hubs is temporarily unavailable (files already written to ADLS). For active-active with zero data loss: implement Event Hubs Federation (replicate to secondary namespace). During planned maintenance: Event Hubs is zone-redundant (Premium/Standard ZRS) — individual zone failures don't cause outage. For multi-region disaster: the secondary namespace receives events from the replication task within seconds.

> **Tip 2:** "What's the Event Hubs retention limit and what happens when retention expires?" — Standard: 1–7 days. Premium: up to 90 days. Dedicated: unlimited (backed by Blob Storage). When retention expires, old events are automatically deleted — consumers that have fallen too far behind will lose access to those events (offset pointing to deleted data → consumer must reset to earliest available). Monitor consumer lag carefully: if a consumer is down for more than the retention period, it will miss events permanently. For consumers that need long retention (audit, replay): use Event Hubs Capture (writes to ADLS with unlimited retention) independently of the Event Hubs retention window.

> **Tip 3:** "When would you choose Event Hubs over Azure Service Bus?" — Event Hubs: high-throughput streaming (millions of events/sec), multiple independent consumers (fan-out to ASA + Databricks + archive simultaneously), analytics workloads, append-only log semantics, consumer controls its own read position, ordered within a partition. Service Bus: transactional messaging (exactly-once with sessions), per-message settlement (ACK/NACK/Dead-letter), message scheduling (deliver at future time), duplicate detection built-in, complex routing (topics + subscriptions with filters). Rule: IoT, clickstream, telemetry → Event Hubs. Order processing, payment notifications, work queues → Service Bus.
