---
title: "Event-Driven Architecture — Scenario Questions"
topic: system-design
subtopic: event-driven-architecture
content_type: scenario_question
tags: [system-design, event-driven, scenarios, kafka, streaming]
---

# Event-Driven Architecture — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Choose Between Batch and Event-Driven

**Scenario:** Your e-commerce company sends order confirmation emails to customers. Currently, a cron job runs every hour and queries the orders table for any new orders placed in the last hour, then sends emails. This means customers sometimes wait up to 60 minutes for their confirmation email. The marketing team wants emails sent within 2 minutes of order placement. How do you redesign this?

<details>
<summary>✅ Solution</summary>

**The problem:** Hourly batch polling is incompatible with a 2-minute SLA.

**Solution: Event-driven email triggering**

**Architecture:**

```
Order Service
    │ (when order is placed)
    ▼
Kafka topic: "order-events"
    │
    ▼
Email Consumer (new service)
    │ (immediately sends confirmation email)
    ▼
Email Provider (SendGrid / SES)
```

**Step 1: Produce order event (in Order Service)**

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def place_order(order_data: dict):
    # 1. Save to database
    order = db.orders.insert(order_data)
    
    # 2. Publish event to Kafka
    producer.send(
        topic="order-events",
        key=str(order["order_id"]).encode(),  # Partition by order_id
        value={
            "event_type": "OrderPlaced",
            "event_id": str(uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "order_id": order["order_id"],
                "customer_email": order_data["email"],
                "customer_name": order_data["name"],
                "total_amount": order_data["total"],
                "items": order_data["items"]
            }
        }
    )
    producer.flush()
```

**Step 2: Email Consumer**

```python
from kafka import KafkaConsumer
import sendgrid

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="kafka:9092",
    group_id="email-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

sg = sendgrid.SendGridAPIClient(api_key="SG_API_KEY")

for message in consumer:
    event = message.value
    
    if event["event_type"] != "OrderPlaced":
        continue  # Only handle order placements
    
    order = event["data"]
    
    # Send confirmation email (< 30 seconds after order placed)
    sg.send({
        "to": order["customer_email"],
        "from": "orders@company.com",
        "subject": f"Order Confirmed: #{order['order_id']}",
        "body": f"Hi {order['customer_name']}, your order of ${order['total_amount']} is confirmed!"
    })
    
    print(f"Email sent for order {order['order_id']}")
    # Kafka auto-commits offset after successful processing
```

**Result:** Email sent within 30 seconds of order placement (2-minute SLA easily met).

**Other benefits of this redesign:**
- The email service is decoupled — can be updated without touching the Order Service
- Multiple consumers can be added (SMS service, inventory service) without changing Order Service
- Kafka retains events for 7 days — if email service goes down, it replays on restart

**What about the batch job?**
- Keep it as a safety net: reconciliation job runs daily to find any orders missing confirmation emails
- Event-driven for freshness + batch reconciliation for correctness

</details>

</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Implement the Outbox Pattern

**Scenario:** You're building a microservices system where the Order Service must (1) save an order to PostgreSQL and (2) publish an "OrderPlaced" event to Kafka. Currently, you write to PostgreSQL first, then publish to Kafka. Your tech lead says this is a dual-write bug — explain the bug and implement the Outbox pattern to fix it.

<details>
<summary>✅ Solution</summary>

**Step 1: Explain the dual-write bug**

```
Current (broken) code:
def place_order(order):
    db.execute("INSERT INTO orders VALUES (...)")  # Step 1
    kafka.send("order-events", order_event)        # Step 2

Bug scenarios:
  A) Step 1 succeeds, app crashes before Step 2
     → Order in DB, no Kafka event → downstream never knows about the order

  B) Step 1 succeeds, Kafka is temporarily down at Step 2
     → Same problem: order exists, no event

  C) Step 2 succeeds, Step 1 had a DB timeout (DB eventually rolled back)
     → Kafka event fired for an order that doesn't exist in DB
```

**Step 2: Outbox pattern — database table as the bridge**

```sql
-- Create outbox table (in the same database as orders)
CREATE TABLE outbox_events (
    event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type  VARCHAR(100) NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    published   BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP
);
```

**Step 3: Atomic write to both tables**

```python
import psycopg2
import json
from uuid import uuid4
from datetime import datetime

def place_order(order_data: dict, db_conn):
    """Write order AND outbox event in a single atomic transaction."""
    
    with db_conn:  # Context manager: commits or rolls back
        cursor = db_conn.cursor()
        
        # 1. Insert order
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, amount, status, created_at)
            VALUES (%s, %s, %s, 'PLACED', NOW())
        """, [order_data["order_id"], order_data["customer_id"], order_data["amount"]])
        
        # 2. Insert outbox event — same transaction
        cursor.execute("""
            INSERT INTO outbox_events (event_type, payload)
            VALUES (%s, %s)
        """, [
            "OrderPlaced",
            json.dumps({
                "order_id": order_data["order_id"],
                "customer_id": order_data["customer_id"],
                "amount": order_data["amount"],
                "placed_at": datetime.utcnow().isoformat()
            })
        ])
    
    # If transaction commits: both rows exist
    # If transaction rolls back (any error): neither row exists
    # NEVER a state where one exists without the other
```

**Step 4: Outbox publisher (background process)**

```python
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    acks="all",  # Wait for all replicas to acknowledge
    enable_idempotence=True  # Prevent duplicate Kafka messages
)

def publish_outbox_events(db_conn):
    """Poll outbox table and publish unpublished events to Kafka."""
    
    while True:
        with db_conn:
            cursor = db_conn.cursor()
            
            # Lock rows to prevent duplicate publishing (multiple publisher instances)
            cursor.execute("""
                SELECT event_id, event_type, payload
                FROM outbox_events
                WHERE published = FALSE
                ORDER BY created_at
                LIMIT 100
                FOR UPDATE SKIP LOCKED
            """)
            events = cursor.fetchall()
            
            if not events:
                time.sleep(0.5)  # No events: wait before polling again
                continue
            
            for event_id, event_type, payload in events:
                # Publish to Kafka
                future = producer.send(
                    topic="order-events",
                    key=event_id.encode(),
                    value=json.dumps({
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "data": payload
                    }).encode()
                )
                future.get(timeout=10)  # Wait for ack
                
                # Mark as published
                cursor.execute("""
                    UPDATE outbox_events
                    SET published = TRUE, published_at = NOW()
                    WHERE event_id = %s
                """, [event_id])

# Run as a daemon thread or separate process
```

**Step 5: Use Debezium instead (production-grade alternative)**

```json
// Debezium Outbox Event Router — no polling process needed
// Debezium reads the outbox table via CDC and publishes to Kafka automatically
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres",
  "database.dbname": "orders_db",
  "table.include.list": "public.outbox_events",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.type": "event_type",
  "transforms.outbox.table.field.event.payload": "payload"
}
// Debezium publishes a Kafka message for each new outbox_events row
// No polling overhead, sub-second latency
```

**Trade-offs:**
- Simple polling publisher: easier to understand, works without Debezium
- Debezium outbox router: lower latency, no polling overhead, but adds operational complexity
- Both are correct implementations; choose based on team familiarity

</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design an Event Sourcing System for Financial Transactions

**Scenario:** A digital bank wants to redesign their account ledger using event sourcing. Requirements: (1) every balance change must be auditable with full history, (2) current balance must be queryable in < 10ms, (3) the system must handle 50,000 transactions/minute, (4) regulators need the ability to reconstruct any account's state at any point in time (time travel). Design the full system.

<details>
<summary>✅ Solution</summary>

**Step 1: Why event sourcing fits here**
- Financial regulations require full audit trail → event sourcing provides this natively
- Fraud investigation needs time travel → replay events to any point in time
- Complex balance calculations (pending, cleared, locked funds) → aggregate from events
- CQRS naturally handles: write model (event log) vs read model (current balance)

**Step 2: Architecture**

```
Banking App (iOS/Web)
      │
      ▼
[Command Handler] — validates command, applies business rules
      │ if valid:
      ▼
[Event Store (Kafka + PostgreSQL)]
  - Kafka: durable event stream, partitioned by account_id
  - PostgreSQL: event table for exact-position queries
  - S3: cold archive for events > 90 days
      │
      ├── [Current Balance Projector] → Redis (< 1ms reads)
      ├── [Transaction History Projector] → PostgreSQL (pagination queries)
      └── [Regulatory Report Projector] → BigQuery (compliance queries)
```

**Step 3: Event schema design**

```python
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
from datetime import datetime
from uuid import UUID

class EventType(str, Enum):
    ACCOUNT_OPENED = "AccountOpened"
    MONEY_DEPOSITED = "MoneyDeposited"
    MONEY_WITHDRAWN = "MoneyWithdrawn"
    TRANSFER_SENT = "TransferSent"
    TRANSFER_RECEIVED = "TransferReceived"
    ACCOUNT_FROZEN = "AccountFrozen"
    ACCOUNT_UNFROZEN = "AccountUnfrozen"

@dataclass
class AccountEvent:
    event_id: UUID       # Unique event ID
    event_type: EventType
    account_id: str      # Partition key for ordering
    version: int         # Monotonically increasing per account — for optimistic locking
    amount: Decimal      # Amount of change (None for non-monetary events)
    balance_after: Decimal  # Denormalized — helps with reconciliation
    timestamp: datetime
    correlation_id: UUID # Links related events (e.g., transfer sends + receives)
    metadata: dict       # initiated_by, channel, device_id, etc.
    
    def to_kafka_value(self) -> bytes:
        return json.dumps({
            "event_id": str(self.event_id),
            "event_type": self.event_type.value,
            "account_id": self.account_id,
            "version": self.version,
            "amount": str(self.amount),
            "balance_after": str(self.balance_after),
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": str(self.correlation_id),
            "metadata": self.metadata
        }).encode()
```

**Step 4: Command handler with optimistic locking**

```python
class AccountCommandHandler:
    def __init__(self, event_store, redis_cache):
        self.event_store = event_store
        self.cache = redis_cache
    
    def deposit(self, account_id: str, amount: Decimal, initiated_by: str) -> AccountEvent:
        """Process a deposit command."""
        
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        
        # Load current state (from snapshot + recent events)
        account = self.rehydrate_account(account_id)
        
        if account.is_frozen:
            raise AccountFrozenError(f"Account {account_id} is frozen")
        
        # Create the event
        event = AccountEvent(
            event_id=uuid4(),
            event_type=EventType.MONEY_DEPOSITED,
            account_id=account_id,
            version=account.version + 1,  # Optimistic lock: next version
            amount=amount,
            balance_after=account.balance + amount,
            timestamp=datetime.utcnow(),
            correlation_id=uuid4(),
            metadata={"initiated_by": initiated_by, "channel": "mobile"}
        )
        
        # Append to event store (with optimistic concurrency check)
        self.event_store.append(event, expected_version=account.version)
        # If another request appended first (version conflict) → raises ConcurrencyError
        # → retry with fresh account state
        
        # Update Redis cache (current balance)
        self.cache.hset(f"account:{account_id}",
                        "balance", str(event.balance_after),
                        "version", event.version)
        
        return event
```

**Step 5: Event store implementation**

```python
class PostgreSQLEventStore:
    """PostgreSQL as durable event store — Kafka for streaming to projectors."""
    
    def append(self, event: AccountEvent, expected_version: int):
        with self.db.transaction():
            # Optimistic locking: check version before insert
            current_version = self.db.fetchone("""
                SELECT MAX(version) FROM account_events WHERE account_id = %s
            """, [event.account_id])[0] or 0
            
            if current_version != expected_version:
                raise ConcurrencyError(
                    f"Version conflict: expected {expected_version}, got {current_version}"
                )
            
            # Insert event
            self.db.execute("""
                INSERT INTO account_events
                (event_id, event_type, account_id, version, amount, balance_after, timestamp, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [event.event_id, event.event_type.value, event.account_id,
                  event.version, event.amount, event.balance_after,
                  event.timestamp, json.dumps(event.metadata)])
        
        # Publish to Kafka (async, for projectors)
        self.kafka_producer.send(
            topic="account-events",
            key=event.account_id.encode(),
            value=event.to_kafka_value()
        )
    
    def get_events(self, account_id: str, from_version: int = 0) -> list:
        return self.db.fetchall("""
            SELECT * FROM account_events
            WHERE account_id = %s AND version > %s
            ORDER BY version ASC
        """, [account_id, from_version])
```

**Step 6: Current balance projector (Redis)**

```python
# Flink job: consumes account-events Kafka topic, maintains Redis balance
class BalanceProjector:
    def process(self, event: AccountEvent):
        # Write current balance to Redis (< 1ms reads)
        self.redis.hset(f"account:{event.account_id}", mapping={
            "balance": str(event.balance_after),
            "version": event.version,
            "last_updated": event.timestamp.isoformat()
        })

# API reads from Redis (< 1ms):
def get_balance(account_id: str) -> Decimal:
    data = redis.hgetall(f"account:{account_id}")
    return Decimal(data["balance"])
```

**Step 7: Time travel — reconstruct any past state**

```python
def reconstruct_balance_at(account_id: str, target_timestamp: datetime) -> Decimal:
    """Reconstruct exact account balance at any point in history."""
    
    # Get all events up to target_timestamp
    events = db.fetchall("""
        SELECT event_type, amount, balance_after
        FROM account_events
        WHERE account_id = %s AND timestamp <= %s
        ORDER BY version ASC
    """, [account_id, target_timestamp])
    
    if not events:
        raise AccountNotFoundAtTimestamp(account_id, target_timestamp)
    
    # Return balance_after of last event before target_timestamp
    # (denormalized balance_after makes this O(1) instead of O(n) replay)
    return Decimal(events[-1]["balance_after"])

# Regulatory query example:
# "What was account ACC-123's balance on Jan 15, 2023 at 11:59 PM?"
balance = reconstruct_balance_at("ACC-123", datetime(2023, 1, 15, 23, 59, 59))
```

**Step 8: Handling 50,000 transactions/minute**

```
50,000 tx/min = ~833 tx/sec
Kafka: 200 partitions, partitioned by account_id (833 tx/sec easily handled)
PostgreSQL: write-heavy, shard by account_id (account_id % 10 → 10 DB shards)
  Each shard handles ~83 tx/sec — well within PostgreSQL limits (1000+ tx/sec)
Redis: 833 reads + writes/sec (trivial for Redis cluster)
```

**Step 9: Failure handling**

| Scenario | Response |
|---|---|
| PostgreSQL write succeeds, Kafka publish fails | Background job reads PostgreSQL events → publishes to Kafka. Projectors are eventually consistent. |
| Concurrency conflict (version mismatch) | Retry with fresh state (max 3 retries, exponential backoff) |
| Redis cache miss | Read from PostgreSQL event store, replay to get current balance, repopulate cache |
| Event store corruption | Restore from S3 cold archive → replay all events → rebuild projections |

</details>

</article>
