---
title: "Cosmos DB — Fundamentals"
topic: azure
subtopic: cosmos-db
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, cosmos-db, nosql, multi-model, global-distribution, ru]
---

# Cosmos DB — Fundamentals

## What Is Azure Cosmos DB?

Azure Cosmos DB is a **fully managed, globally distributed NoSQL database** built for low-latency, high-throughput applications. It provides single-digit millisecond reads and writes at any scale, in any Azure region.

```
Cosmos DB key properties:
  Multi-model:        JSON documents, key-value, wide-column, graph in one service
  Global distribution: replicate data to any Azure region with one click
  Multi-master:       write to any region (active-active, not active-passive)
  Guaranteed SLA:     <10ms reads at P99, <15ms writes at P99 (with correct setup)
  Elastic scale:      instant scale up/down (no maintenance windows)
  Multiple APIs:      NoSQL (native), MongoDB, Cassandra, Gremlin, Table

Compared to traditional databases:
  Azure SQL DB:    ACID, relational, structured, complex queries → Cosmos DB wins on scale + latency
  Azure Cosmos DB: NoSQL, flexible schema, global, high throughput → SQL wins on complex analytics
  
  Use Cosmos DB for:
  ✓ User profiles (high read, flexible schema)
  ✓ Product catalogs (JSON, global reads)
  ✓ Shopping carts (key-value, low latency)
  ✓ IoT telemetry (high write throughput)
  ✓ Gaming leaderboards (sorted sets, low latency)
  ✗ Complex joins across entities → use relational DB
  ✗ Heavy analytics / aggregations → use Synapse + Synapse Link
```

---

## Core Concepts

```
Account → Database → Container → Item

Account:
  Top-level resource (create in Azure portal)
  Choose: API type (NoSQL, MongoDB, etc.) at account creation — cannot change
  Choose: geo-redundancy (single region, multi-region, multi-master)

Database:
  Logical namespace for containers
  Shared throughput: provision RU/s at database level (shared across containers)
  Or: each container has dedicated throughput

Container:
  Primary scalability unit (like a table or collection)
  Has: partition key, throughput (RU/s), indexing policy, TTL settings

Item:
  Individual JSON document (up to 2 MB)
  Must have: id (string), partition key value
  All other fields: flexible (schema-less)

Request Unit (RU):
  Cosmos DB's currency for throughput
  1 RU = cost to read a 1KB item by its primary key
  Write: ~5 RUs for 1KB item
  Query: varies by complexity (simple filter = 2-5 RU, complex cross-partition = 100s of RU)
  
  Provision RU/s: guarantee N RUs per second across all operations
  Autoscale: max N RU/s, scales down to 10% when idle (min charge)
  Serverless: pay only for RUs actually consumed (no provisioned throughput)
```

---

## Partition Key Design (Critical!)

```
Partition key: the single most important design decision in Cosmos DB
Everything in a logical partition shares the same key value
All documents with the same partition key = same logical partition = same physical partition

Rules:
  High cardinality:  many distinct values (not status='active'/'inactive')
  Even distribution: no hot partitions (avoid customer_id if one customer has 90% of writes)
  Frequently used:   appears in most queries as equality filter (enables partition-level routing)
  Immutable:         cannot change partition key value without deleting + reinserting document

Good partition keys:
  user_id:       for user-centric data (even if millions of users)
  product_id:    for product catalogs (many products, even read distribution)
  device_id:     for IoT (many devices, even write distribution)
  sessionId:     for session store (unique per session, TTL handles cleanup)

Bad partition keys:
  created_date:  range queries only, most writes go to "today" partition (hot)
  status:        only 3-4 values → impossible to scale past 3-4 physical partitions
  country:       some countries have 100× more traffic (US vs Fiji)
  true/false:    2 values maximum → cannot scale

Synthetic partition key (when no natural key works):
  Combine: user_id + "_" + date_month
  Or: hash modulo N (10 buckets spread load)
  Example: partition_key = user_id + "_" + (random 0-9) → 10× more even distribution
```

---

## Consistency Levels

```
Cosmos DB: 5 consistency levels (tradeoff between consistency and performance/availability)

Strong:        Read always sees latest committed write (like SQL)
               Cost: highest latency (waits for global replication), lowest throughput
               Use: financial transactions, inventory (cannot be wrong)

Bounded Staleness: reads may lag writes by K versions or T time interval
               Cost: high latency for reads, high availability for writes  
               Use: leaderboards (can be 5 sec old), analytics

Session:       Within a session, reads reflect your own writes (default)
               Cost: low latency, good throughput
               Use: user profile reads after update, shopping cart, most web apps
               Note: most common choice for typical applications

Consistent Prefix: reads never see out-of-order writes (A, B, C — never B without A)
               Cost: lower latency than Bounded Staleness
               Use: social feeds (out-of-order is confusing, slight delay is OK)

Eventual:      No ordering guarantees, highest performance
               Cost: lowest latency and highest throughput, reads may be stale
               Use: like counts, view counters (exact number not critical)

Interview rule of thumb:
  Financial / inventory → Strong (accept higher cost)
  Most web apps → Session (default, reads your own writes)
  High throughput analytics reads → Eventual
```

---

## Interview Tips

> **Tip 1:** "What happens if you choose a bad partition key in Cosmos DB?" — You get hot partitions: one or a few logical partitions receive most of the traffic. Cosmos DB has a 10,000 RU/s limit per physical partition. If you partition by `country` and 80% of traffic is from the US, the US partition gets throttled (429 Too Many Requests) while other partitions sit idle. The provisioned RU/s cannot help — the bottleneck is per-partition. Fix: re-create the container with a better partition key (no in-place change is possible). This is why partition key design must be right from the start.

> **Tip 2:** "What is a Request Unit (RU) and how do you estimate cost?" — RU is Cosmos DB's normalized throughput unit. 1 RU = cost to read a 1KB document by its partition key (point read). Writes cost ~5 RU/KB. Complex queries can cost 100+ RU. Estimate: (reads/sec × avg_read_cost_RU) + (writes/sec × avg_write_cost_RU) = total RU/s needed. Provision at least 20% above estimate for bursting. Cost: $0.008/100 RU-hr (Standard provisioned). Autoscale: max_RU/hr = max_RU × $0.012/100 RU-hr. Monitor: Cosmos DB Metrics → Normalized RU Consumption — if > 100% = throttling.

> **Tip 3:** "When would you use Cosmos DB Serverless vs provisioned throughput?" — Serverless: pay per RU consumed, no idle cost, max 5,000 RU/s, single region only. Best for: dev/test, sporadic workloads, new apps with unknown traffic patterns. Provisioned (standard): pay per RU/s provisioned (whether used or not), scales to millions of RU/s, multi-region. Best for: production apps with predictable traffic. Autoscale (middle ground): pay for max RU/s you set, automatically scales down to 10% when idle. Best for: variable production traffic. Decision: serverless for dev + apps < 5K RU/s peak; autoscale for variable production; manual provisioned for predictable high-volume.
