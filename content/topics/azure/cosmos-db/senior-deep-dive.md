---
title: "Cosmos DB — Senior Deep Dive"
topic: azure
subtopic: cosmos-db
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, cosmos-db, global-distribution, multi-master, partitioning, htap, design-patterns]
---

# Cosmos DB — Senior Deep Dive

## Global Distribution and Multi-Master

```
Global distribution architecture:

Single-master (default):
  One write region, N read regions
  Write: application → primary region → async replication → secondary regions
  Read: application → nearest region (low latency reads globally)
  Failover: automatic (configurable priority: East US → West EU → South Asia)
  
Multi-master (multi-region writes):
  Write to ANY configured region simultaneously
  Each region accepts writes immediately (no coordination required)
  Conflict resolution: last-write-wins (LWW by _ts timestamp) or custom conflict procedure
  Latency: writes always local (< 10ms), no cross-region write coordination
  Use for: globally distributed apps where local write latency is critical
    Example: gaming (write score to nearest region), collaborative editing

Conflict resolution in multi-master:
  Last-Writer-Wins (LWW): 
    System property _ts (Unix timestamp): highest _ts wins
    Or: custom property (e.g., version_counter)
    Problem: concurrent writes in same second → deterministic loser
  
  Custom conflict procedure (merge procedure):
    JavaScript stored procedure that receives both conflicting items
    You define the merge logic: combine fields, pick winner, alert
    Example: inventory decrement (take min of both values to prevent oversell)
  
  Conflict feed:
    All conflicts logged to container's conflict feed
    Audit: read conflicts to understand what was automatically resolved
    Manual resolution: read conflict, decide winner, apply, delete from conflict feed

Session token propagation (multi-region reads your own writes):
  Problem: write to East US → read from West EU → session consistency = may not see write
  Solution: client captures session token from write response
    response.headers["x-ms-session-token"]
  Pass session token on subsequent reads:
    container.ReadItemAsync(id, pk, options: new RequestOptions { SessionToken = token })
  This pins reads to the session that includes the write (read your own writes across regions)
```

---

## Advanced Partitioning: Hierarchical Partition Keys

```python
# Hierarchical partition key (preview → GA): two or three levels of partition key
# Solves: hot partitions where one key has too many items (> 20GB limit per logical partition)

# Problem: partition_key = tenant_id → tenant "BigCorp" has 5M orders (50GB) → hot partition
# Solution: hierarchical key = (tenant_id, user_id) → partition splits by both keys

# Create container with hierarchical partition key:
from azure.cosmos import PartitionKey

# Two-level hierarchy:
container = db.create_container_if_not_exists(
    id="orders",
    partition_key=PartitionKey(path=["/tenantId", "/userId"])  # list = hierarchical
)

# Item structure:
item = {
    "id": "ord_001",
    "tenantId": "BigCorp",      # first level
    "userId": "user_456",       # second level
    "amount": 99.99
}

# Query options:
# 1. Exact partition (most efficient):
items = list(container.query_items(
    "SELECT * FROM c WHERE c.tenantId='BigCorp' AND c.userId='user_456'",
    partition_key=PartitionKey(path=["/tenantId", "/userId"]),
    partition_key_value=["BigCorp", "user_456"]
))

# 2. Partial partition key (prefix match):
# Query all orders for BigCorp (not all users):
items = list(container.query_items(
    "SELECT * FROM c WHERE c.tenantId='BigCorp'",
    partition_key=PartitionKey(path=["/tenantId", "/userId"]),
    partition_key_value=["BigCorp"]   # prefix match → fan-out within BigCorp only
))

# 3. Cross-partition (all tenants — expensive):
items = list(container.query_items(
    "SELECT * FROM c WHERE c.amount > 1000",
    enable_cross_partition_query=True
))

# Benefits:
#   BigCorp's data splits across many logical partitions (one per userId)
#   No more 20GB per-partition limit for large tenants
#   Prefix queries fan out only within BigCorp (not global cross-partition)
```

---

## Design Patterns for Common Use Cases

```python
# Pattern 1: Event Sourcing with Change Feed

# Orders container: stores current order state
# order-events container: stores immutable event log (append-only)
# Change feed: order updates → materialize derived projections

# Write side:
async def place_order(order_data: dict):
    # Append event (never update events)
    event = {
        "id": f"evt_{uuid4()}",
        "orderId": order_data["order_id"],
        "eventType": "OrderPlaced",
        "eventTime": datetime.utcnow().isoformat(),
        "payload": order_data,
        "version": 1
    }
    await events_container.create_item(event)
    
    # Update current state (for read queries)
    await orders_container.upsert_item({
        "id": order_data["order_id"],
        "customerId": order_data["customer_id"],   # partition key
        "status": "pending",
        "totalAmount": order_data["total"],
        "lastEventType": "OrderPlaced",
        "lastUpdated": datetime.utcnow().isoformat()
    })

# Change feed consumer: rebuild projections
async def on_order_event(docs):
    for event in docs:
        if event["eventType"] == "OrderCompleted":
            await update_analytics_projection(event)
            await trigger_fulfillment(event)
            await update_customer_lifetime_value(event)

# Pattern 2: TTL for Session Store
session_container = db.create_container_if_not_exists(
    id="sessions",
    partition_key=PartitionKey(path="/userId"),
    default_time_to_live=3600   # 1 hour TTL by default on all items
)

# Session item auto-expires after 1 hour:
session = {
    "id": "sess_abc123",
    "userId": "user_456",      # partition key
    "cartItems": [...],
    "lastActivity": "2024-01-15T14:30:00Z"
    # No explicit TTL field needed (uses container default)
}
# To extend: update lastActivity → Cosmos resets TTL from last write time? 
# Actually: TTL counts from last write time by default

# Explicit TTL override per item (extend session):
session["ttl"] = 7200  # override: this session lives 2 hours
```

---

## HTAP Pattern: Cosmos DB + Synapse Analytics

```
HTAP = Hybrid Transactional/Analytical Processing
Same data serves both OLTP reads/writes AND analytical queries

Cosmos DB HTAP architecture:
  Transactional store (row-oriented, BTree indexes, optimized for OLTP):
    ← Application writes/reads here (orders, user profiles, sessions)
    Write: 5-10ms latency, fully ACID per document
    
  Analytical store (column-oriented, auto-synced from transactional, optimized for analytics):
    ← Synapse Spark / Serverless SQL queries here
    Lag: ~2-5 minutes from transactional write
    No indexing needed (column store is self-analyzing)
    Zero impact on OLTP operations (separate physical store)

Cost model for analytical store:
  Storage: $0.02/GB/month (vs transactional: ~$0.25/GB)
  Operations: analytical read operations billed differently (Synapse bills for compute)
  Transactions: analytical store writes are free (no RU charge for replication)

When to use HTAP vs ETL:
  HTAP: need near-real-time analytics (< 5 min lag), avoid pipeline maintenance
  ETL:  need historical data > Cosmos DB retention, complex transformations, 
        data from multiple sources in one analytical view

Production HTAP query optimization:
  Use partition pruning in Synapse SQL:
    OPENROWSET() WITH (...) AS c WHERE c.customerId = 'BigCorp'
    → Synapse uses partition metadata to prune irrelevant data
  
  Use Spark DataFrame pushdown:
    df.filter(col("status") == "completed").select("customerId", "totalAmount")
    → Spark connector pushes filter to Cosmos analytical store (column pruning)
    → Only reads needed columns (columnar benefit)
```

---

## Interview Tips

> **Tip 1:** "How do you handle cross-partition queries efficiently in Cosmos DB?" — Cross-partition queries fan out to all physical partitions, which is expensive in both RU cost and latency. Minimize with: (a) always include the partition key in queries when known (routes to single partition), (b) use hierarchical partition keys to allow prefix-based partial fan-outs (BigCorp's data only, not all tenants), (c) materialize projections — maintain denormalized views in a separate container partitioned by the query's filter field, (d) use Synapse Link for analytical queries that genuinely need full table scans (no RU impact on transactional container). If a query pattern always requires full container scan → it's an analytics query, not OLTP.

> **Tip 2:** "How does Cosmos DB handle the CAP theorem?" — Cosmos DB chooses CP or AP depending on consistency level: Strong consistency → prioritizes consistency (C) over availability (A) during partitions. Eventual consistency → prioritizes availability (A) over consistency (C). Session, Bounded Staleness, Consistent Prefix → intermediate tradeoffs. Unique to Cosmos: it offers 5 granular choices on the consistency spectrum rather than binary C vs A. In practice: multi-master configurations lean toward A (any region can accept writes, conflicts resolved after); strong consistency with single master leans toward C (write must reach quorum).

> **Tip 3:** "What is the 20GB per logical partition limit and how do you work around it?" — Each logical partition (all documents with the same partition key value) is limited to 20GB. If a single partition key value (e.g., tenant_id = "BigCorp") accumulates >20GB of data, you'll get throttling/errors on that partition. Solutions: (a) More granular partition key: change `tenant_id` → `user_id` (spreads BigCorp's data across many partitions), (b) Hierarchical partition key: `(tenant_id, user_id)` allows prefix queries while splitting storage by user, (c) Synthetic partition key: append a bucket suffix (e.g., `BigCorp_0` to `BigCorp_9`) and fan out writes/reads across buckets, (d) Archive old data: move items older than N months to a separate container or ADLS.

## ⚡ Cheat Sheet

**API choice**
| API | Best for | Notes |
|---|---|---|
| NoSQL (Core) | Document, key-value | Native; best SLA; all features |
| MongoDB | MongoDB migration | Wire-compatible |
| Cassandra | Wide-column / IoT | CQL-compatible |
| Gremlin | Graph traversal | Tinkerpop-compatible |
| Table | Azure Table Storage migration | Key-value |

**Consistency levels (ordered strong → weak)**
1. `Strong` — linearizable reads; highest latency; single-region or write region only
2. `BoundedStaleness` — lag by K operations or T time; useful for global reads
3. `Session` (default) — consistent within session; best balance for most apps
4. `ConsistentPrefix` — no out-of-order reads; lighter than session
5. `Eventual` — lowest latency; no ordering guarantees

**Partition key selection rules**
- High cardinality: avoid hot partitions (max 20 GB + 10K RU/s per logical partition)
- Even distribution: uniform read/write across partitions
- Include in most queries: avoids cross-partition scatter
- Good: `userId`, `deviceId`, `/id` — Bad: `country`, `status`, `boolean`

**RU/s (Request Units)**
- 1 RU = cost to read 1 KB document
- Write = ~5× read cost; cross-partition query = N × point-read cost
- Autoscale: set max RU/s; auto-scales 10%–100% of max; charged at max reached
- Serverless: charged per actual RUs; good for <1K RU/s average

**Change feed**
```python
# Change feed = ordered log of all writes + deletes (if enabled)
# Use for: event sourcing, materialized views, ETL fan-out
container.query_items_change_feed(is_start_from_beginning=True)
# Azure Functions trigger: auto-checkpoint; scales with partition count
```

**Key Cosmos DB DE patterns**
- Analytical store (HTAP): auto-sync to columnar format; query with Synapse Serverless SQL — no RU charge for reads
- Multi-region writes: conflict resolution via LWW (last-write-wins) or custom stored procedure
- TTL: automatic document expiry; `"ttl": 3600` on document or container default
