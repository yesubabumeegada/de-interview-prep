---
title: "Cosmos DB — Intermediate"
topic: azure
subtopic: cosmos-db
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, cosmos-db, sdk, change-feed, indexing, synapse-link, queries]
---

# Cosmos DB — Intermediate

## Python SDK: CRUD Operations

```python
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.identity import DefaultAzureCredential

# Connect with Managed Identity (preferred)
credential = DefaultAzureCredential()
client = CosmosClient(
    url="https://myaccount.documents.azure.com:443/",
    credential=credential
)

db = client.get_database_client("ecommerce")
container = db.get_container_client("orders")

# ── CREATE ─────────────────────────────────────────────────────────────────────
order = {
    "id": "ord_001",              # required string field
    "customerId": "cust_123",     # partition key
    "items": [
        {"productId": "prod_A", "quantity": 2, "price": 49.99},
        {"productId": "prod_B", "quantity": 1, "price": 199.00}
    ],
    "totalAmount": 298.98,
    "status": "pending",
    "createdAt": "2024-01-15T14:30:00Z",
    "ttl": 31536000               # optional: auto-delete after 1 year (seconds)
}

response = container.create_item(body=order)
print(f"Created: {response['id']}, RU charge: {container.client_connection.last_response_headers['x-ms-request-charge']}")

# ── READ (point read — cheapest: 1 RU) ─────────────────────────────────────────
item = container.read_item(item="ord_001", partition_key="cust_123")
print(f"Status: {item['status']}")

# ── UPSERT ─────────────────────────────────────────────────────────────────────
order["status"] = "confirmed"
container.upsert_item(body=order)  # insert if not exists, update if exists

# ── QUERY ─────────────────────────────────────────────────────────────────────
# Single-partition query (efficient: targets one partition)
items = list(container.query_items(
    query="SELECT * FROM c WHERE c.customerId = @cid AND c.status = 'confirmed'",
    parameters=[{"name": "@cid", "value": "cust_123"}],
    partition_key="cust_123"    # route to single partition → fast + cheap
))

# Cross-partition query (expensive: scans all partitions — avoid if possible)
all_pending = list(container.query_items(
    query="SELECT * FROM c WHERE c.status = 'pending'",
    enable_cross_partition_query=True   # explicit flag required
))

# ── DELETE ─────────────────────────────────────────────────────────────────────
container.delete_item(item="ord_001", partition_key="cust_123")

# ── BATCH (transactional — all in same partition) ──────────────────────────────
operations = [
    ("create", ({"id": "ord_002", "customerId": "cust_123", "amount": 50.00},), {}),
    ("replace", ("ord_001", {"id":"ord_001","customerId":"cust_123","status":"shipped"}), {}),
]
# Note: TransactionalBatch in Python SDK available from azure-cosmos 4.5+
```

---

## Change Feed: Event-Driven Integration

```python
# Change Feed: ordered stream of all inserts and updates (not deletes) per partition
# Use cases: real-time propagation, cache invalidation, event sourcing, analytics

# Method 1: Azure Functions trigger (easiest for event-driven pattern)
# Function automatically triggered on every change in the container

# function.json:
# {
#   "bindings": [{
#     "type": "cosmosDBTrigger",
#     "name": "documents",
#     "direction": "in",
#     "connectionStringSetting": "COSMOS_CONNECTION",
#     "databaseName": "ecommerce",
#     "containerName": "orders",
#     "leaseContainerName": "leases",  -- tracks change feed position per consumer
#     "createLeaseContainerIfNotExists": true,
#     "startFromBeginning": false
#   }]
# }

# Python Azure Function handler:
import azure.functions as func
import json, logging

def main(documents: func.DocumentList) -> None:
    for doc in documents:
        order = doc.data
        logging.info(f"Order changed: {order['id']} → status: {order.get('status')}")
        
        # Trigger downstream:
        if order.get('status') == 'completed':
            send_confirmation_email(order['customerId'])
            update_inventory(order['items'])

# Method 2: SDK-based change feed processor (for custom consumers)
from azure.cosmos.aio import CosmosClient

async def process_changes():
    async with CosmosClient(url, credential) as client:
        container = client.get_database_client("ecommerce").get_container_client("orders")
        lease_container = client.get_database_client("ecommerce").get_container_client("leases")
        
        processor = container.get_change_feed_processor(
            name="my-processor",
            handler=on_change,
            lease_container=lease_container,
            start_time=None   # None = from now, "Beginning" = from start of history
        )
        await processor.start()
        await asyncio.sleep(60)
        await processor.stop()

async def on_change(docs: list, context) -> None:
    for doc in docs:
        print(f"Changed: {doc['id']}")
```

---

## Indexing Policy Optimization

```json
// Default: all properties indexed (convenient but costs extra RUs on writes)
// Custom indexing policy: trade write cost for read performance

// Pattern A: Include only queried fields (reduce write RU cost)
{
  "indexingMode": "consistent",
  "includedPaths": [
    {"path": "/customerId/?"},      // equality queries on customerId
    {"path": "/status/?"},          // equality queries on status
    {"path": "/createdAt/?"},       // range queries on date
    {"path": "/totalAmount/?"}      // range + sort on amount
  ],
  "excludedPaths": [
    {"path": "/items/*"},           // large nested array, never queried directly
    {"path": "/_etag/?"},           // system field, auto-excluded best practice
    {"path": "/description/?"}     // free-text field, not queried by equality
  ]
}

// Pattern B: Composite index for ORDER BY / multi-field queries
// Without composite index: ORDER BY on two fields = 429 (query rejected)
{
  "indexingMode": "consistent",
  "includedPaths": [{"path": "/*"}],
  "compositeIndexes": [
    [
      {"path": "/customerId", "order": "ascending"},
      {"path": "/createdAt", "order": "descending"}
    ]
  ]
}

// Query that benefits from composite index:
// SELECT * FROM c WHERE c.customerId='cust_123' ORDER BY c.createdAt DESC
// Without composite index: error or full partition scan
// With composite index: direct index lookup + ordered result (no sort step)

// Pattern C: Spatial index (for geo-queries)
{
  "spatialIndexes": [
    {
      "path": "/location/?",
      "types": ["Point"]
    }
  ]
}
// Enables: ST_DISTANCE, ST_WITHIN, ST_INTERSECTS on geo-JSON coordinates
```

---

## Synapse Link for Analytics

```python
# Synapse Link: analytical replica of Cosmos DB for HTAP (zero ETL)
# Cosmos DB transactional → analytical store (column-oriented) → Synapse queries

# Enable analytical store on container (must be done at creation or via SDK):
container_properties = db.create_container_if_not_exists(
    id="orders",
    partition_key=PartitionKey(path="/customerId"),
    analytical_storage_ttl=-1   # -1 = keep forever in analytical store
)

# Query analytical store via Serverless SQL Pool in Synapse:
# (Note: runs on Synapse side, not Cosmos DB)
query = """
    SELECT
        c.status,
        COUNT(*) AS order_count,
        SUM(c.totalAmount) AS total_revenue
    FROM OPENROWSET(
        PROVIDER = 'CosmosDB',
        CONNECTION = 'Account=myaccount;Database=ecommerce',
        OBJECT = 'orders',
        SERVER_CREDENTIAL = 'cosmos_credential'
    ) WITH (
        customerId VARCHAR(100) '$.customerId',
        status      VARCHAR(50)  '$.status',
        totalAmount FLOAT        '$.totalAmount',
        createdAt   VARCHAR(30)  '$.createdAt'
    ) AS c
    WHERE c.createdAt >= '2024-01-01'
    GROUP BY c.status
"""

# Or use Databricks Spark connector:
df = spark.read \
    .format("cosmos.olap") \
    .option("spark.synapse.linkedService", "CosmosDbLinkedService") \
    .option("spark.cosmos.container", "orders") \
    .option("spark.cosmos.preferredRegions", "East US") \
    .load()

revenue_by_region = df.groupBy("status") \
    .agg(F.sum("totalAmount").alias("total_revenue")) \
    .show()

# Benefits:
#   Zero impact on OLTP (analytical queries run against separate analytical store)
#   No ETL pipeline maintenance
#   Sub-minute latency from Cosmos write to analytics visibility
#   Pay for analytical store separately: $0.02/GB/month
```

---

## Interview Tips

> **Tip 1:** "What's the difference between a point read and a query in Cosmos DB, and why does it matter for cost?" — A point read (`ReadItemAsync` with both `id` and `partition_key`) costs exactly 1 RU regardless of item size up to 1KB. It directly addresses the physical location by partition key hash + item ID. A query (`SELECT * FROM c WHERE c.id = 'x'`) — even if it returns one item — goes through the query engine, costs 2.5+ RUs, and may scan the partition. Design pattern: always use point reads for known `id` + partition key lookups. Queries are for when you don't know the exact `id` or need multiple items.

> **Tip 2:** "How does the Change Feed work and what are its limitations?" — Change Feed is an ordered log of all creates and updates per logical partition, exposed as a stream. Consumers track their position via a "lease" document (stored in a separate container). Multiple independent consumers can read the same change feed at different positions (fan-out). Limitations: (a) Deletes are NOT included (items disappear silently — use TTL + soft delete pattern: set `deleted: true`, then TTL removes it; change feed shows the soft delete), (b) Change order is per-partition only (no global ordering across partitions), (c) Changes may be delivered at-least-once (process idempotently).

> **Tip 3:** "When would you exclude a path from the indexing policy?" — Exclude when: (a) the property is large (long text descriptions, binary data) that's never used in WHERE/ORDER BY — indexing it wastes RUs on every write; (b) nested arrays you only access by index position (not in queries); (c) high-cardinality fields used only for point reads (already found by partition key + id, no index needed). Rule: start with default indexing (all included), identify write-heavy operations that are slow, analyze which excluded paths would help. Every excluded path reduces write RU cost for that property.
