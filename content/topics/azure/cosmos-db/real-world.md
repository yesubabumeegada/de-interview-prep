---
title: "Cosmos DB — Real World"
topic: azure
subtopic: cosmos-db
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, cosmos-db, production, change-feed, e-commerce, iot, patterns]
---

# Cosmos DB — Real World

## Pattern 1: E-Commerce Order System

```python
# Cosmos DB for e-commerce: orders, carts, product catalog
# Design: one container per entity type (separate throughput, separate partition keys)

from azure.cosmos import CosmosClient, PartitionKey
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import uuid, json

client = CosmosClient("https://ecommerce.documents.azure.com:443/", DefaultAzureCredential())
db = client.get_database_client("ecommerce")

# Container design:
# orders:   partition_key=/customerId  (reads by customer, pattern: 1 customer → many orders)
# carts:    partition_key=/sessionId   (temporary, TTL=3600sec)
# products: partition_key=/categoryId  (reads by category for browsing)
# sessions: partition_key=/userId      (TTL=7200sec, auth sessions)

orders = db.get_container_client("orders")

# Place order (point read + write in same partition)
def place_order(customer_id: str, cart_items: list) -> dict:
    total = sum(item["price"] * item["quantity"] for item in cart_items)
    
    order = {
        "id":          str(uuid.uuid4()),
        "customerId":  customer_id,          # partition key
        "items":       cart_items,
        "totalAmount": total,
        "status":      "pending",
        "statusHistory": [
            {"status": "pending", "timestamp": datetime.utcnow().isoformat()}
        ],
        "createdAt":   datetime.utcnow().isoformat(),
        "estimatedDelivery": (datetime.utcnow() + timedelta(days=3)).isoformat()
    }
    
    response = orders.create_item(body=order)
    # RU cost: ~10 RUs for 2KB item with full indexing
    print(f"Order created: {order['id']} | RU: {orders.client_connection.last_response_headers['x-ms-request-charge']}")
    return order

# Update order status (upsert — idempotent)
def update_order_status(order_id: str, customer_id: str, new_status: str):
    # Point read first (1 RU)
    order = orders.read_item(item=order_id, partition_key=customer_id)
    
    # Update
    order["status"] = new_status
    order["statusHistory"].append({
        "status": new_status,
        "timestamp": datetime.utcnow().isoformat()
    })
    order["updatedAt"] = datetime.utcnow().isoformat()
    
    # Replace (point write — ~5 RU)
    orders.replace_item(item=order_id, body=order)

# Get all orders for a customer (single-partition query)
def get_customer_orders(customer_id: str, status: str = None) -> list:
    if status:
        query = "SELECT * FROM c WHERE c.customerId=@cid AND c.status=@status ORDER BY c.createdAt DESC"
        params = [{"name": "@cid", "value": customer_id}, {"name": "@status", "value": status}]
    else:
        query = "SELECT * FROM c WHERE c.customerId=@cid ORDER BY c.createdAt DESC"
        params = [{"name": "@cid", "value": customer_id}]
    
    return list(orders.query_items(
        query=query,
        parameters=params,
        partition_key=customer_id  # single-partition → cheap
    ))
```

---

## Pattern 2: IoT Time Series with Change Feed

```python
# IoT: 50K devices sending telemetry every 30 sec
# Design: store latest reading per device + historical time series

# Container: device_telemetry (partition_key=/deviceId, TTL=86400 → 24h retention)
# Container: device_latest   (partition_key=/deviceId, upsert current state)
# Change Feed: trigger alerts on anomalies

telemetry = db.get_container_client("device_telemetry")
latest = db.get_container_client("device_latest")

# Write telemetry (append-only)
def write_telemetry(device_id: str, readings: dict):
    reading = {
        "id": f"{device_id}_{int(datetime.utcnow().timestamp())}",
        "deviceId": device_id,        # partition key
        "readings": readings,
        "timestamp": datetime.utcnow().isoformat(),
        "ttl": 86400                   # auto-delete after 24 hours
    }
    telemetry.create_item(reading)
    
    # Upsert latest state (always 1 doc per device — cheap reads)
    latest.upsert_item({
        "id": device_id,              # id = deviceId → point read by id
        "deviceId": device_id,
        **readings,
        "lastSeen": reading["timestamp"]
    })

# Change Feed processor: detect anomalies on every write
import azure.functions as func

def process_telemetry_change(documents: func.DocumentList):
    for doc in documents:
        device_id = doc["deviceId"]
        readings = doc.get("readings", {})
        
        # Anomaly checks
        temp = readings.get("temperature_c", 0)
        pressure = readings.get("pressure_bar", 0)
        
        if temp > 180:
            send_alert(device_id, f"HIGH TEMP: {temp}°C")
        if pressure > 25:
            send_alert(device_id, f"HIGH PRESSURE: {pressure} bar")
        
        # Update rolling 1-hour average (store in latest container)
        if readings.get("power_kw"):
            update_rolling_average(device_id, readings["power_kw"])

# Query: get last 100 readings for a device (single-partition, cheap)
def get_device_history(device_id: str, hours: int = 1) -> list:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    return list(telemetry.query_items(
        query="SELECT TOP 100 * FROM c WHERE c.deviceId=@did AND c.timestamp>=@since ORDER BY c.timestamp DESC",
        parameters=[
            {"name": "@did", "value": device_id},
            {"name": "@since", "value": since}
        ],
        partition_key=device_id
    ))
```

---

## Pattern 3: Global Distribution with Active Geo-Replication

```python
# Multi-region setup: East US (write), West EU + Southeast Asia (reads)
# Application reads from nearest region, writes route to East US (single master)

from azure.cosmos import CosmosClient
from azure.cosmos.diagnostics import CosmosDiagnostics

# Client with preferred regions (read routing)
client = CosmosClient(
    url="https://ecommerce.documents.azure.com:443/",
    credential=DefaultAzureCredential(),
    preferred_locations=["Southeast Asia", "West Europe", "East US"]
    # SDK routes reads to nearest available region
    # Writes always go to write endpoint (East US in single-master mode)
)

# Monitoring:
# Azure Portal → Cosmos DB → Metrics:
#   Total Requests by region: see read distribution across regions
#   Server-side latency P99: verify < 10ms per region
#   Replication Lag: East US → West EU lag (should be < 1 min for consistent prefix)
#   Normalized RU Consumption: if > 100% → throttling → provision more RU/s

# Connection policy for latency optimization:
from azure.cosmos import ConnectionMode

client = CosmosClient(
    url="https://ecommerce.documents.azure.com:443/",
    credential=DefaultAzureCredential(),
    connection_mode=ConnectionMode.Direct,  # Direct mode: bypass gateway, ~1ms lower latency
    # vs Gateway mode: all traffic through HTTPS gateway (simpler firewall rules, ~5ms higher latency)
)
# Direct mode: recommended for production (lower latency, higher throughput)
# Gateway mode: use for restricted network environments (only port 443 available)
```

---

## Interview Tips

> **Tip 1:** "How do you model one-to-many relationships in Cosmos DB?" — Two approaches: (a) Embed (denormalize): include child documents inside the parent document (e.g., order items embedded in the order document). Best when: children are always read with parent, children count is bounded (< 100 items), and children are never accessed independently. (b) Reference (separate container): store children separately with parent ID. Best when: children are large (> 2MB total), children are queried independently, or child count is unbounded. Rule: embed when you read the parent and children together; reference when they're accessed independently. Cosmos DB rewards denormalization — joins are expensive cross-container.

> **Tip 2:** "What is the difference between RU/s provisioned and RU/s autoscale?" — Provisioned (manual): you set exactly N RU/s. Cost: N/100 × $0.008/hr regardless of usage. Bursts above N: throttled (429). Use for: steady, predictable high-volume traffic. Autoscale: you set maximum N RU/s. System scales from 10% of max (0.1×N) to N automatically. Cost: billed on highest RU/s actually used in the hour × $0.012/100 RU-hr. No throttling up to max. Use for: variable traffic (daytime vs nighttime, weekday vs weekend). Rule: if traffic is consistently > 50% of provisioned: consider autoscale. If traffic is always high: manual provisioned is cheaper. If traffic is spiky: autoscale avoids throttling while saving cost during quiet periods.

> **Tip 3:** "How would you migrate a MongoDB Atlas cluster to Azure Cosmos DB for MongoDB API?" — The Cosmos DB MongoDB API is wire-compatible with MongoDB 4.0/5.0/6.0. Migration steps: (1) Run MongoDB compatibility checker tool against your application's query patterns — Cosmos DB doesn't support all MongoDB operators. (2) Export data from Atlas using `mongodump` or Atlas Data Export. (3) Import to Cosmos DB using `mongorestore` pointing to Cosmos DB endpoint. (4) Test application: run full regression suite against Cosmos DB endpoint. Common incompatibilities: `$lookup` (cross-collection join — expensive in Cosmos), some aggregation pipeline operators, change stream resume tokens (different format). For live migration: use Azure DMS MongoDB-to-Cosmos DB mode (online, with change stream sync for minimal downtime).
