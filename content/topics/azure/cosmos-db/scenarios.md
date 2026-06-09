---
title: "Cosmos DB — Scenarios"
topic: azure
subtopic: cosmos-db
content_type: scenario_question
tags: [azure, cosmos-db, scenarios, interview, design, performance]
---

# Cosmos DB — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Global User Profile Store

**Scenario:** Build a user profile store for a social media app with 500M users globally. Users are in NA, EU, APAC. Each user has: profile info, preferences, connection counts, recent activity (last 100 actions). Read latency requirement: < 10ms. Writes: 100K/sec globally.

<details>
<summary>💡 Hint</summary>
Think about container design: user_profiles partitioned by /userId for sub-10ms point reads. Consider global distribution model (multi-master vs single-master), consistency level trade-offs, and enabling Synapse Link for analytics without RU impact.
</details>

<details>
<summary>✅ Solution</summary>

```
Scale:
  500M users × avg 5KB profile = 2.5TB total data
  Writes: 100K/sec × 5KB = 500 MB/sec
  Reads: 1M/sec × 1KB point reads = 1 GB/sec

Container design:

Container 1: user_profiles (core profile data — frequently read)
  Partition key: /userId (string, UUID — high cardinality, even distribution)
  Item structure:
    {
      "id": "usr_abc123",
      "userId": "usr_abc123",        // partition key = id for direct point reads
      "displayName": "Jane Smith",
      "bio": "...",
      "region": "NA",
      "tier": "premium",
      "createdAt": "2021-03-15T...",
      "followerCount": 12500,
      "followingCount": 450,
      "preferences": { "theme": "dark", "language": "en", ... }
    }
  Consistency: Session (read your own writes after update)
  Throughput: autoscale, max 500,000 RU/s

Container 2: user_activity (recent 100 actions)
  Partition key: /userId
  TTL: 2,592,000 (30 days) per item
  Item: { "id": "act_{uuid}", "userId": "...", "actionType": "like", "targetId": "...", "ts": "..." }
  Keep only last 100 per user → background function trims on new write

Global distribution:
  Write region: East US (single master — simpler conflict resolution)
  Read regions: West Europe, Southeast Asia, East US
  Consistency: Session (global reads-your-own-writes with session token propagation)
  
  Or multi-master (active-active):
    All 3 regions accept writes (NA users write to East US, EU to West EU, APAC to SEA)
    Conflict resolution: LWW on _ts field (profile updates — last write wins is acceptable)
    Benefit: profile updates by EU user: <5ms (local write, no cross-Atlantic round-trip)

Reads:
  Client in Tokyo → reads from Southeast Asia replica → <5ms
  Point read: ReadItemAsync("usr_abc123", partitionKey="usr_abc123") → 1 RU
  500M users × 1RU × $0.008/100RU/hr → monitoring confirms cost

Indexing:
  Exclude: /preferences/* (complex nested object, never queried)
  Include: /region, /tier, /createdAt (for admin queries and analytics)
  Composite: (/region, /createdAt) for admin "new users by region" queries

Synapse Link for analytics:
  Enable analytical_storage_ttl=-1 on user_profiles
  Synapse: daily aggregation → monthly active users by region, tier conversion rates
  Zero RU impact on OLTP

Cost estimate:
  Data: 2.5TB × $0.25/GB = $625/month (transactional storage)
  Throughput: 500K RU/s autoscale ≈ $3,500/month (blended usage)
  Analytical storage: 2.5TB × $0.02/GB = $50/month
  Multi-region replication: 3 regions × storage = $1,875/month
  Total: ~$6,000/month for 500M user global profile store
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cosmos DB Throttling (429 Too Many Requests)

**Scenario:** Your Cosmos DB container starts returning 429 errors at 2 PM every weekday. The container has 10,000 RU/s provisioned. How do you diagnose and fix?

<details>
<summary>💡 Hint</summary>
Check Normalized RU Consumption % at 2 PM. Find the expensive operation (log x-ms-request-charge per request type). Cross-partition aggregation queries at peak time are a common culprit — the fix is moving analytics to Synapse Link.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Identify which requests are being throttled
  Azure Monitor → Cosmos DB → Requests by Status Code
  Filter: StatusCode=429, Time=2PM-3PM
  Check: which operation type (ReadItem, QueryItems, CreateItem, etc.)

Step 2: Check Normalized RU Consumption
  Metric: Normalized RU Consumption %
  If 100% at 2 PM: container is saturated (consuming all 10K RU/s)
  Expected: spiky 100% = bursting above provisioned → throttle

Step 3: Check actual RU usage per request
  In code: log x-ms-request-charge header per operation
  
  Sample findings:
    ReadItem:    1 RU (normal — point reads)
    QueryItems:  450 RU (HIGH — cross-partition query doing full scan)
    CreateItem:  8 RU (slightly high — check if item is > 2KB or indexing is inefficient)

  Root cause: a daily 2 PM report job runs a cross-partition aggregation query:
    SELECT c.region, COUNT(*) FROM c WHERE c.status='active' GROUP BY c.region
    Cost: 450 RU × 1,000 executions/min = 450,000 RU/min = 7,500 RU/sec
    This consumes 75% of provisioned 10K RU/s, leaving only 2,500 for OLTP

Fix options (in priority order):

Option A (immediate): increase provisioned RU/s or enable autoscale
  10,000 → 20,000 RU/s (double the throughput for 2PM spike)
  Or: enable autoscale (max 20,000 RU/s)
  Cost: autoscale max 20K ≈ $13/hr → +$6/hr = $18/day for 2 PM hour

Option B (better): fix the expensive query
  Move the cross-partition aggregation query to Synapse Link (Analytical Store)
  Query runs on Synapse Serverless SQL, NOT on Cosmos DB → 0 RU consumed
  Synapse SQL cost: $5/TB scanned (2.5 minutes of analytical query) = < $0.01

Option C (structural): create a pre-aggregated summary container
  Background function: update region_summary container every 5 minutes
  Report job reads from region_summary (point reads, 2 RU each → 10 total RU)
  Trades freshness (5-min lag) for huge cost reduction

Option D: set x-ms-documentdb-query-enablecrosspartition=false on report queries
  If report doesn't need cross-partition: add partition filter
  If cross-partition is unavoidable: use Synapse Link as in Option B

Recommended: Option B immediately, then Option C for permanent fix
Result: 2PM throttling eliminated, provisioned RU reduced by 50%
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Choose Between Cosmos DB and Azure SQL for a New Service

**Scenario:** You're building a product recommendation engine that needs to: store product data (structured: name, category, price, specs in JSON), store user interaction history (viewed, clicked, purchased — 500M records/month), serve real-time recommendations (< 50ms), and support analytics on interaction data (daily ML model training).

<details>
<summary>💡 Hint</summary>
Map each requirement to the right data store: product catalog (flexible JSON schema), interaction history (high write volume, TTL for old data), recommendations serving (point read < 50ms), ML training (Synapse Link for zero-RU analytics). Consider Cosmos DB for the first three, Synapse/Databricks for ML.
</details>

<details>
<summary>✅ Solution</summary>

```
Analyze each requirement:

1. Product data (structured + JSON specs):
   Azure SQL: strong for structured data, JSON support via JSON columns
   Cosmos DB: native JSON document, flexible schema (varies by product type)
   Winner: Cosmos DB (product specs vary by category — laptops vs chairs vs food)

2. User interaction history (500M records/month):
   Write rate: 500M / 30 / 24 / 3600 ≈ 193 writes/second
   Azure SQL: manageable throughput (200 writes/sec = ~5 DTU for simple inserts)
   Cosmos DB: 193 writes/sec = ~1,000 RU/sec → cheap on serverless or 2,000 RU provisioned
   Consideration: 500M × 12 months = 6B records → 6TB of interaction data
   Azure SQL: 6TB requires Hyperscale → $0.22/vCore/hr + storage
   Cosmos DB: 6TB × $0.25/GB = $1,500/month + TTL to auto-delete old events
   Winner: Cosmos DB (scale to 6B records with TTL for old events, cheaper)

3. Real-time recommendations (< 50ms):
   Redis Cache (Azure Cache for Redis):  < 1ms, ideal for pre-computed recommendations
   Cosmos DB point read:                  < 10ms
   Azure SQL read with index:             5-20ms
   
   Pattern: pre-compute recommendations daily (ML model runs on Databricks)
   Store top-10 recommendations per user in Cosmos DB container
   Serve: point read per user → < 10ms ✓
   Winner: Cosmos DB (point reads, global distribution for low latency globally)

4. Analytics for ML training:
   Neither Cosmos DB nor Azure SQL is optimized for analytical workloads
   Use: Synapse Link on Cosmos DB interaction history container
   Synapse Spark reads interaction data → train recommendation model in Databricks
   Winner: Synapse Link on Cosmos DB (zero ETL, near-real-time for daily model refresh)

Final architecture:

Services → Event Hubs → Azure Functions → Cosmos DB
           (interactions)   (write amplified to 2 containers)
           
Cosmos DB containers:
  products:        partition by /categoryId, 10K RU/s
  interactions:    partition by /userId, 50K RU/s autoscale, TTL=365 days
  recommendations: partition by /userId, 5K RU/s (pre-computed, updated daily)

Analytics path:
  Cosmos DB interactions (Synapse Link) → Databricks ML job → write recommendations back to Cosmos DB

Serving path:
  API → Cosmos DB ReadItem(recommendations, userId, userId) → < 5ms → return top-10

Azure SQL: NOT used (no relational schema needed, no complex joins required)

Cost: ~$3,500/month for Cosmos DB (3 containers, multi-region)
vs Azure SQL Hyperscale for 6TB: ~$3,000/month + higher query complexity
Decision: Cosmos DB wins on flexibility, latency, and manageable cost
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "When would you NOT use Cosmos DB?" — Avoid Cosmos DB when: (a) you need complex SQL joins across entities (data model would require multiple cross-partition queries that are expensive and complex), (b) strict ACID transactions across multiple containers (Cosmos DB supports transactions within a single partition only), (c) heavy analytics workloads (use Synapse or Databricks — even with Synapse Link, Cosmos DB is not an analytical database), (d) budget is tight and data access is predictable (Azure SQL with proper indexing is cheaper for structured, relational data), (e) team expertise is SQL-only (Cosmos DB requires NoSQL data modeling skills that differ significantly from relational modeling).

> **Tip 2:** "What's the maximum item size in Cosmos DB and how do you handle larger documents?" — Maximum item size is 2MB. For larger payloads: (a) Split the document into smaller related documents (e.g., user profile + user activity + user media as separate containers), (b) Store large binary data (images, PDFs) in ADLS Gen2 and store only the URL in Cosmos DB, (c) Compress large JSON strings before storing (gzip in application, store as Base64), (d) Chunking pattern: split large document into chunks (chunk_0, chunk_1...) and reassemble in application. In practice, 2MB is rarely hit for properly modeled NoSQL data — if you're approaching 2MB, reconsider the data model.

> **Tip 3:** "How do you implement pagination in Cosmos DB?" — Use the continuation token: Cosmos DB returns `x-ms-continuation` header when there are more results. Store the token, pass it in the next request's `RequestOptions`. In Python SDK: `query_items` returns a pageable iterator — use `.by_page()` to get pages with continuation tokens. For user-facing pagination (page 1, 2, 3): store the continuation token in the session/cache, associate it with page number. Note: Cosmos DB doesn't support `OFFSET/SKIP` in most APIs — continuation token is the correct pattern. For large result sets, consider materializing paginated views in a separate container.

