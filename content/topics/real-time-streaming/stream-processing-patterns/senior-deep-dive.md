---
title: "Stream Processing Patterns — Senior Deep Dive"
topic: real-time-streaming
subtopic: stream-processing-patterns
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, patterns, event-sourcing, saga, cqrs, outbox, architecture, exactly-once]
---

# Stream Processing Patterns — Senior Deep Dive

## Event Sourcing Pattern

```python
"""
Event Sourcing: store every state change as an immutable event.
Current state = replay all events in order.

Benefits:
  - Complete audit trail (every change recorded)
  - Time travel (reconstruct state at any point)
  - Replay events through new logic (recompute derived data)
  - Natural integration with stream processing (events are the stream)

Challenges:
  - Event schema evolution (old events must still be readable)
  - Snapshot management (don't replay 10 years of events for every read)
  - Eventual consistency (current state derived from log, not stored directly)
"""

# Example: bank account using event sourcing
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
import uuid

@dataclass
class AccountEvent:
    event_id: str
    account_id: str
    event_type: str        # OPENED, DEPOSITED, WITHDRAWN, CLOSED
    amount: Optional[float]
    timestamp: datetime
    version: int           # sequential version per account

# Event store (Kafka or DynamoDB events table)
class EventStore:
    def __init__(self, kafka_producer, topic: str):
        self.producer = kafka_producer
        self.topic = topic
    
    def append(self, account_id: str, event_type: str, amount: float = None,
                expected_version: int = None) -> AccountEvent:
        """Append event with optimistic concurrency control."""
        
        # Optimistic locking: check current version matches expected
        current_version = self._get_current_version(account_id)
        if expected_version is not None and current_version != expected_version:
            raise ConcurrencyException(
                f"Expected version {expected_version}, got {current_version}")
        
        event = AccountEvent(
            event_id=str(uuid.uuid4()),
            account_id=account_id,
            event_type=event_type,
            amount=amount,
            timestamp=datetime.utcnow(),
            version=current_version + 1
        )
        
        # Publish to Kafka (partition by account_id → ordered per account)
        self.producer.produce(
            topic=self.topic,
            key=account_id.encode(),
            value=json.dumps(asdict(event)).encode()
        )
        return event
    
    def _get_current_version(self, account_id: str) -> int:
        """Get latest version from DynamoDB version table."""
        response = dynamodb.get_item(
            TableName='account-versions',
            Key={'account_id': {'S': account_id}}
        )
        item = response.get('Item')
        return int(item['version']['N']) if item else 0

# Account aggregate: reconstruct state from events
class Account:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.balance = 0.0
        self.status = None
        self.version = 0
    
    @classmethod
    def from_events(cls, events: List[AccountEvent]) -> 'Account':
        """Reconstruct account state by replaying events."""
        account = cls(events[0].account_id)
        for event in events:
            account.apply(event)
        return account
    
    def apply(self, event: AccountEvent):
        """Apply a single event to update state."""
        if event.event_type == 'OPENED':
            self.status = 'open'
            self.balance = event.amount or 0
        elif event.event_type == 'DEPOSITED':
            self.balance += event.amount
        elif event.event_type == 'WITHDRAWN':
            if event.amount > self.balance:
                raise ValueError("Insufficient funds")
            self.balance -= event.amount
        elif event.event_type == 'CLOSED':
            self.status = 'closed'
        self.version = event.version

# Snapshot pattern (avoid replaying all events on every read):
# Every N events (e.g., N=100): snapshot current state to DynamoDB
# On load: read latest snapshot + replay only events after snapshot version
```

---

## Transactional Outbox Pattern

```python
"""
Outbox Pattern: reliably publish events from a service that writes to a database.

Problem WITHOUT outbox:
  Step 1: Write to database (success)
  Step 2: Publish to Kafka (FAILS - network error)
  Result: DB updated, Kafka not notified → data inconsistency

Problem with naive two-phase approach:
  Step 1: Publish to Kafka (success)
  Step 2: Write to database (FAILS)
  Result: Kafka event published, DB not updated → phantom event

Outbox Pattern:
  Step 1: Write to database AND outbox table in ONE transaction
  Step 2: Separate "relay" process reads outbox → publishes to Kafka
  Result: database and Kafka are always in sync

Atomicity: steps 1 is a single DB transaction (can't partially fail)
           relay is idempotent (retrying publish is safe)
"""

import psycopg2
import json

def place_order_with_outbox(order: dict):
    """
    Place order and publish event using outbox pattern.
    Both DB write and outbox write are in one transaction.
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        # 1. Write order to orders table
        cursor.execute(
            "INSERT INTO orders (order_id, user_id, amount, status) VALUES (%s, %s, %s, %s)",
            (order['order_id'], order['user_id'], order['amount'], 'pending')
        )
        
        # 2. Write event to outbox table (same transaction)
        event_payload = {
            'order_id': order['order_id'],
            'user_id': order['user_id'],
            'amount': order['amount'],
            'event_type': 'ORDER_PLACED',
            'timestamp': datetime.utcnow().isoformat()
        }
        cursor.execute(
            """INSERT INTO outbox (id, topic, key, payload, created_at, published)
               VALUES (%s, %s, %s, %s, NOW(), false)""",
            (str(uuid.uuid4()), 'orders', order['order_id'], json.dumps(event_payload))
        )
        
        conn.commit()  # atomic: both rows committed or neither
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Outbox relay (runs continuously or scheduled):
def outbox_relay():
    """Read unpublished events from outbox and publish to Kafka."""
    conn = psycopg2.connect(DATABASE_URL)
    
    while True:
        cursor = conn.cursor()
        
        # Lock unpublished events (skip locked for concurrent relays)
        cursor.execute("""
            SELECT id, topic, key, payload
            FROM outbox
            WHERE published = false
            ORDER BY created_at
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        """)
        rows = cursor.fetchall()
        
        for row_id, topic, key, payload in rows:
            try:
                # Publish to Kafka
                kafka_producer.produce(topic=topic, key=key.encode(), 
                                       value=payload.encode())
                kafka_producer.flush()
                
                # Mark as published
                cursor.execute(
                    "UPDATE outbox SET published = true, published_at = NOW() WHERE id = %s",
                    (row_id,)
                )
            except Exception as e:
                logger.error(f"Failed to publish outbox event {row_id}: {e}")
        
        conn.commit()
        time.sleep(1)  # poll every second

# Alternative: use Debezium on the outbox table (CDC → Kafka automatically)
# Debezium watches outbox table changes → publishes to Kafka
# Eliminates the relay process entirely
```

---

## SAGA Pattern for Distributed Transactions

```python
"""
SAGA: manage distributed transactions across multiple services without 2PC.
Each step is a local transaction; on failure, compensating transactions undo completed steps.

Choreography SAGA (event-driven):
  Each service publishes events and reacts to events from other services
  No central coordinator
  
Orchestration SAGA (central coordinator):
  Central orchestrator sends commands to services, receives replies
  Easier to monitor and debug
"""

# Orchestration SAGA example: order fulfillment
# Steps: Reserve inventory → Charge payment → Ship order
# Compensation: Release inventory → Refund payment → Cancel shipment

class OrderSagaOrchestrator:
    """
    Orchestrates multi-step order fulfillment.
    On any step failure: execute compensating transactions.
    """
    
    def __init__(self, kafka_producer, saga_state_store):
        self.producer = kafka_producer
        self.state_store = saga_state_store
    
    def start_saga(self, order: dict) -> str:
        saga_id = str(uuid.uuid4())
        
        # Save initial state
        self.state_store.save(saga_id, {
            'order_id': order['order_id'],
            'status': 'STARTED',
            'steps_completed': [],
            'current_step': 'RESERVE_INVENTORY'
        })
        
        # Send command to inventory service
        self.producer.produce(
            topic='inventory-commands',
            key=saga_id.encode(),
            value=json.dumps({
                'saga_id': saga_id,
                'command': 'RESERVE',
                'order_id': order['order_id'],
                'items': order['items']
            }).encode()
        )
        return saga_id
    
    def handle_inventory_reserved(self, saga_id: str, event: dict):
        """Inventory successfully reserved → proceed to payment."""
        state = self.state_store.get(saga_id)
        state['steps_completed'].append('RESERVE_INVENTORY')
        state['current_step'] = 'CHARGE_PAYMENT'
        self.state_store.save(saga_id, state)
        
        # Send payment command
        self.producer.produce(
            topic='payment-commands',
            key=saga_id.encode(),
            value=json.dumps({
                'saga_id': saga_id,
                'command': 'CHARGE',
                'order_id': state['order_id'],
                'amount': event['total_amount']
            }).encode()
        )
    
    def handle_payment_failed(self, saga_id: str, event: dict):
        """Payment failed → compensate: release inventory."""
        state = self.state_store.get(saga_id)
        state['status'] = 'COMPENSATING'
        self.state_store.save(saga_id, state)
        
        # Send compensating command: release reservation
        self.producer.produce(
            topic='inventory-commands',
            key=saga_id.encode(),
            value=json.dumps({
                'saga_id': saga_id,
                'command': 'RELEASE',
                'order_id': state['order_id']
            }).encode()
        )
        
        # Notify order service: saga failed
        self.producer.produce(
            topic='order-events',
            key=saga_id.encode(),
            value=json.dumps({
                'saga_id': saga_id,
                'event': 'ORDER_FAILED',
                'reason': event.get('reason', 'Payment failed')
            }).encode()
        )
```

---

## Interview Tips

> **Tip 1:** "What is the outbox pattern and why is it preferred over dual writes?" — Dual write problem: write to DB then publish to Kafka. If Kafka publish fails after DB commit, the event is lost (inconsistency). The outbox pattern atomically writes the business data AND the event to the outbox table in one DB transaction. A separate relay process reads the outbox and publishes to Kafka. Since the relay can retry safely (Kafka production is idempotent with proper keys), and the DB transaction either commits both or neither, you get reliable event publishing. Debezium on the outbox table (CDC approach) eliminates even the relay process — Debezium watches the outbox table and publishes changes to Kafka automatically.

> **Tip 2:** "How do you handle schema evolution in event sourcing?" — Events are immutable — you can't modify past events. Strategies: (a) Upcasting: when reading old events, transform them to the new schema in code (version-aware deserializer). Example: event v1 has `name`, v2 splits into `first_name` + `last_name` → upcast adds a concatenated field for v1 events; (b) Append-only evolution: only add new fields, never remove (backward-compatible). Old consumers ignore unknown fields; (c) Event versioning: `ORDER_PLACED_V2` new event type; consumers handle both V1 and V2; (d) Snapshot migration: take snapshots using new schema, future replays start from snapshot (skip old-schema events). Avro/Protobuf with Schema Registry enforces compatibility rules.

> **Tip 3:** "What are the trade-offs of choreography vs orchestration sagas?" — Choreography: each service reacts to events from others (no central coordinator). Pros: loose coupling, simple individual services. Cons: hard to understand the overall flow (business logic scattered across services), difficult to monitor transaction status, prone to cyclic event dependencies. Orchestration: a central saga orchestrator sends commands and receives replies. Pros: business flow visible in one place (orchestrator code), easier to add steps, explicit compensation logic, observable (query orchestrator for saga status). Cons: orchestrator is a coupling point, potential single point of failure (mitigated with stateful orchestrators on Kafka). In practice: orchestration sagas are easier to maintain in large teams.
