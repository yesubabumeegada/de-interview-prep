---
title: "Schema Validation — Scenarios"
topic: data-quality
subtopic: schema-validation
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [schema-validation, interview, scenarios]
---

# Schema Validation — Interview Scenarios

## Scenario 1 (Junior): Type Mismatch Crash

**Question:** Your pipeline crashes because `amount` is a string in today's file but was always a float. How do you handle this?

**Answer:**
```python
import pandas as pd

def safe_read_with_type_coercion(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    
    # Try to coerce amount to float
    if "amount" in df.columns and df["amount"].dtype == "object":
        print(f"Warning: amount is {df['amount'].dtype}, coercing to float")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        
        null_after_coerce = df["amount"].isna().sum()
        if null_after_coerce > 0:
            print(f"Warning: {null_after_coerce} rows have unparseable amount values")
    
    # Validate after coercion
    if not pd.api.types.is_numeric_dtype(df["amount"]):
        raise TypeError("amount cannot be cast to numeric")
    
    return df
```

**Prevention:** Always specify schema at read time and register it in a schema registry. Any type change should trigger a compatibility alert.

---

## Scenario 2 (Mid-level): Adding a Required Column

**Question:** You need to add a `region_id` (required, not null) column to the `orders` table. How do you manage this safely?

**Answer:**

**Option 1: Add as optional first, make required later**
```python
# Phase 1: Add as optional (backward compatible)
# Producers start populating it, consumers can optionally use it
# Schema v2.1: region_id optional

# Phase 2 (after all producers populate it): Make required
# Schema v2.2: region_id required
# Validate that no NULLs exist before flipping nullability

# Validation before making required:
null_pct = df["region_id"].isna().mean()
if null_pct > 0:
    raise ValueError(f"Cannot make region_id required: {null_pct:.1%} are NULL")
```

**Option 2: Default value for backward compatibility**
```python
# Add with a default for existing records
df["region_id"] = df.get("region_id", pd.Series(["UNKNOWN"] * len(df)))
# Or derive from existing data:
df["region_id"] = df["country_code"].map(COUNTRY_TO_REGION)
```

**Key principle:** Never add a required column without a migration plan for existing data.

---

## Scenario 3 (Senior): Schema Registry Outage

**Question:** Your Kafka consumers use Schema Registry to deserialize Avro messages. The registry goes down. What happens and how do you design for resilience?

**Answer:**

**What happens without resilience:**
- Consumers fail to deserialize (can't fetch schema by ID)
- Consumer lag grows
- If no retry, messages pile up or are lost

**Resilient design:**
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from functools import lru_cache
import time

class ResilientSchemaRegistryClient:
    """Schema registry client with local cache for outage resilience."""
    
    def __init__(self, registry_url: str):
        self._client = SchemaRegistryClient({"url": registry_url})
        self._cache: dict = {}
        self._last_fetch: dict = {}
        self._cache_ttl = 3600  # Cache schemas for 1 hour
    
    def get_schema(self, schema_id: int) -> str:
        now = time.time()
        
        # Return from cache if fresh or if registry is down
        if schema_id in self._cache:
            age = now - self._last_fetch.get(schema_id, 0)
            if age < self._cache_ttl:
                return self._cache[schema_id]
        
        try:
            schema = self._client.get_schema(schema_id).schema_str
            self._cache[schema_id] = schema
            self._last_fetch[schema_id] = now
            return schema
        except Exception as e:
            # Registry down: return cached schema if available
            if schema_id in self._cache:
                print(f"Schema registry unavailable, using cached schema for ID {schema_id}")
                return self._cache[schema_id]
            raise  # Can't deserialize without schema


# Additional resilience: pre-warm cache at startup
def prewarm_schema_cache(client: ResilientSchemaRegistryClient, schema_ids: list):
    """Pre-fetch all known schema IDs at startup."""
    for schema_id in schema_ids:
        client.get_schema(schema_id)
    print(f"Pre-warmed cache with {len(schema_ids)} schemas")
```

**Additional design decisions:**
1. Consumer uses exponential backoff on registry errors
2. Messages are not committed until successfully deserialized
3. After N retries, route to dead letter queue
4. Schema registry is deployed with HA (multi-node) and separate from Kafka broker health
