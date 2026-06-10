---
title: "Schema Validation — Scenarios"
topic: data-quality
subtopic: schema-validation
content_type: scenario_question
tags: [schema-validation, interview, scenarios]
---

# Schema Validation — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Type Mismatch Crash

**Scenario:** Your pipeline crashes because `amount` is a string in today's file but was always a float. How do you handle this?

<details>
<summary>💡 Hint</summary>

**Prevention:** Always specify schema at read time and register it in a schema registry. Any type change should trigger a compatibility alert.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Adding a Required Column

**Scenario:** You need to add a `region_id` (required, not null) column to the `orders` table. How do you manage this safely?

<details>
<summary>💡 Hint</summary>

**Option 1: Add as optional first, make required later**

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Schema Registry Outage

**Scenario:** Your Kafka consumers use Schema Registry to deserialize Avro messages. The registry goes down. What happens and how do you design for resilience?

<details>
<summary>💡 Hint</summary>

**What happens without resilience:** - Consumers fail to deserialize (can't fetch schema by ID) - Consumer lag grows - If no retry, messages pile up or are lost

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is schema validation and why is it important in data pipelines?**
A: Schema validation verifies that incoming data conforms to an expected structure — correct field names, data types, nullability constraints, and required fields. It is critical because schema violations in upstream data silently corrupt downstream tables, models, and reports if not caught at ingestion.

**Q: What is the difference between strict and lenient schema validation?**
A: Strict validation rejects any record that doesn't exactly match the schema (unexpected fields, type mismatches). Lenient validation accepts data with unexpected additional fields (evolution-friendly) but still rejects type violations and missing required fields. Lenient is more common in evolving systems.

**Q: What is a schema registry and when would you use one?**
A: A schema registry (e.g., Confluent Schema Registry) is a centralized service that stores and versions serialization schemas (Avro, Protobuf, JSON Schema) for streaming data. It enforces schema compatibility rules at publish time, preventing incompatible schema changes from reaching consumers.

**Q: What are the three schema compatibility modes in Confluent Schema Registry?**
A: Backward compatibility (new schema can read old data — add optional fields, remove optional fields), forward compatibility (old schema can read new data — remove optional fields, add optional fields), and full compatibility (both backward and forward). Full compatibility is the most restrictive.

**Q: How do you validate schema in a dbt project?**
A: Use dbt's `schema.yml` files to define expected column names, data types, and tests (not_null, unique, accepted_values, relationships). dbt tests run these validations on transformed data, and `dbt compile` validates model SQL at build time.

**Q: What is JSON Schema and how is it used for data validation?**
A: JSON Schema is a vocabulary for annotating and validating JSON documents, defining required fields, types, formats, enum values, and nested structures. It is used to validate API payloads, event streams, and configuration files before they enter data pipelines.

**Q: How do you handle schema evolution in a production data lake?**
A: Use formats that support schema evolution (Delta Lake, Iceberg, Hudi) which track schema history and handle column additions, renames, and type widening safely. Define a compatibility policy (additive-only changes are non-breaking) and version schemas using semantic versioning.

**Q: What happens when a schema validation check fails in a streaming pipeline?**
A: Invalid records should be routed to a dead-letter queue or quarantine storage rather than dropped or blocking the pipeline. Alert the producing team with the specific violation, track violation rates as a metric, and implement a reprocessing mechanism once the schema issue is resolved.

---

## 💼 Interview Tips

- Know the difference between schema validation at ingest (preventing bad data from entering) and schema testing after transformation (verifying your pipeline's output).
- Show familiarity with Avro/Protobuf and schema registries for streaming — this is expected knowledge for senior DE roles working with Kafka.
- Be ready to discuss schema evolution strategies: interviewers want to know how you would add a required field to a schema without breaking existing producers.
- Mention Delta Lake or Iceberg schema evolution capabilities — these are the modern answer to schema management in data lakes.
- Common mistake: treating schema validation as a one-time setup activity rather than an ongoing monitoring concern as sources evolve.
- Senior interviewers appreciate discussion of the organizational challenge: producers often don't notify consumers of schema changes, making automated schema drift detection essential.
