---
title: "Data Contracts — Real World"
topic: data-quality
subtopic: data-contracts
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-contracts, production, kafka, schema-registry, migration]
---

# Data Contracts — Real World Patterns

## Pattern 1: Kafka Streaming with Schema Registry

```python
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json

# Schema registered in registry
PAYMENT_AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "Payment",
    "namespace": "com.company.payments",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "amount_usd", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
        {"name": "status", "type": {"type": "enum", "name": "PaymentStatus",
            "symbols": ["pending", "processing", "completed", "failed", "refunded"]}},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ]
})

def create_consumer_with_schema_validation():
    schema_registry_client = SchemaRegistryClient({"url": "https://schema-registry:8081"})
    
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        PAYMENT_AVRO_SCHEMA,
        lambda data, ctx: data,
    )
    
    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "payments-consumer",
        "auto.offset.reset": "earliest",
    })
    
    consumer.subscribe(["payments"])
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        try:
            # Schema validation happens here — throws if schema doesn't match
            payment = avro_deserializer(
                msg.value(),
                SerializationContext("payments", MessageField.VALUE)
            )
            process_payment(payment)
        except Exception as e:
            print(f"Schema validation failed: {e}")
            send_to_dlq(msg)  # Dead letter queue
```

---

## Pattern 2: Contract Migration — Parallel Run

When renaming a column (breaking change), run old and new schemas simultaneously:

```python
# Phase 1: Producer publishes BOTH old and new column names
def publish_payment_v2(payment: dict) -> dict:
    """During migration, emit both field names."""
    return {
        # v1 field (deprecated, will be removed in 90 days)
        "cust_id": payment["customer_id"],
        # v2 field (new canonical name)
        "customer_id": payment["customer_id"],
        # Migration metadata
        "_schema_version": "2.0",
        "_cust_id_deprecated": True,
        # ... other fields
    }

# Phase 2: Consumer reads new field, falls back to old
def read_customer_id(record: dict) -> str:
    """Consumer reads v2 field, falls back to v1 during migration."""
    return record.get("customer_id") or record.get("cust_id")

# Phase 3 (after 90 days): Remove cust_id from producer
# Update consumer to only read customer_id
```

---

## Pattern 3: Contract Registry REST API

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import yaml

app = FastAPI(title="Data Contract Registry")
_registry: Dict[str, Any] = {}  # In production: use a database

class Contract(BaseModel):
    id: str
    name: str
    version: str
    owner: str
    schema_definition: Dict[str, Any]
    quality: Dict[str, Any]

@app.post("/contracts/", status_code=201)
def register_contract(contract: Contract):
    if contract.id in _registry:
        raise HTTPException(400, f"Contract {contract.id} already exists")
    _registry[contract.id] = contract.dict()
    return {"message": f"Contract {contract.id} registered", "id": contract.id}

@app.get("/contracts/{contract_id}")
def get_contract(contract_id: str):
    if contract_id not in _registry:
        raise HTTPException(404, f"Contract {contract_id} not found")
    return _registry[contract_id]

@app.post("/contracts/{contract_id}/validate")
def validate_data(contract_id: str, sample_data: List[Dict]):
    """Validate a sample of data against the contract."""
    contract = _registry.get(contract_id)
    if not contract:
        raise HTTPException(404, "Contract not found")
    
    import pandas as pd
    df = pd.DataFrame(sample_data)
    violations = []
    
    for field in contract["schema_definition"]["fields"]:
        col = field["name"]
        if col not in df.columns and field.get("required", True):
            violations.append(f"Missing required column: {col}")
    
    return {
        "valid": len(violations) == 0,
        "violations": violations,
        "rows_checked": len(sample_data),
    }

@app.get("/contracts/{contract_id}/consumers")
def get_consumers(contract_id: str):
    contract = _registry.get(contract_id)
    if not contract:
        raise HTTPException(404, "Contract not found")
    return {"contract_id": contract_id, "consumers": contract.get("consumers", [])}
```

---

## Real-World Lessons Learned

| Lesson | Anti-Pattern | Best Practice |
|---|---|---|
| Start small | Contract every table at once | Start with top 10 most-broken pipelines |
| Automate compatibility | Manual schema reviews | CI/CD compatibility checks on every PR |
| Track violations | Ignore non-critical violations | Log all violations, trend over time |
| Consumer sign-off | Producer decides what's breaking | Consumers vote on breaking change PRs |
| Deprecation grace period | Immediate removal | 90-day deprecation notice + monitoring |
| Test with real data | Validate schema only | Validate schema AND quality (nulls, ranges) |
