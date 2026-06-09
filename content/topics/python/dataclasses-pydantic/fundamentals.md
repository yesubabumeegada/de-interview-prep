---
title: "Dataclasses & Pydantic — Fundamentals"
topic: python
subtopic: dataclasses-pydantic
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, dataclasses, pydantic, type-validation, pipeline-config, BaseModel]
---

# Dataclasses & Pydantic — Fundamentals

Data engineers deal with structured data constantly: pipeline configurations, API response schemas, database row models, event schemas. Representing this structure with plain dicts is convenient but fragile — a typo in a key name, a missing field, or a wrong type causes a runtime error hours into a pipeline run. Dataclasses and Pydantic provide structure, type safety, and validation at the point of data entry.

---

## Plain Dicts vs Named Tuples vs Dataclasses

### The dict Problem

```python
# A pipeline config as a plain dict — dangerous
config = {
    "sourc": "s3://bucket/path",   # typo! "sourc" instead of "source"
    "batch_size": "1000",          # string, not int — will fail when used
    "enable_retries": True,
}

# No error here — the typo is valid dict syntax
print(config["source"])  # KeyError at runtime, hours later
```

### Named Tuples — Better but Limited

```python
from collections import namedtuple

PipelineConfig = namedtuple("PipelineConfig", ["source", "batch_size", "enable_retries"])

config = PipelineConfig(source="s3://bucket/path", batch_size=1000, enable_retries=True)
config.source       # ✓ Attribute access
config.wrong_field  # AttributeError ← caught at access time, not at creation
# But: no type validation, no default values, immutable
```

### Dataclasses — The Standard Python Solution

```python
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineConfig:
    source: str
    batch_size: int
    enable_retries: bool = True
    max_retries: int = 3
    output_path: Optional[str] = None
    tags: list = field(default_factory=list)  # Mutable default — use field()!


# Usage
config = PipelineConfig(
    source="s3://my-bucket/events/",
    batch_size=1000,
)
print(config.source)       # "s3://my-bucket/events/"
print(config.batch_size)   # 1000
print(config.enable_retries)  # True (default)
print(config.tags)         # [] (new list per instance)

# CRITICAL: never use a mutable default directly
@dataclass
class BadConfig:
    tags: list = []   # TypeError at definition time!
    # All instances would SHARE the same list — use field(default_factory=list)
```

### Dataclass Features for DE

```python
from dataclasses import dataclass, field, asdict, astuple


@dataclass
class IngestionMetrics:
    source: str
    records_read: int = 0
    records_written: int = 0
    errors: int = 0
    duration_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.records_read == 0:
            return 0.0
        return (self.records_read - self.errors) / self.records_read

    def __post_init__(self):
        """Validate after initialization."""
        if self.records_read < 0:
            raise ValueError(f"records_read cannot be negative: {self.records_read}")


metrics = IngestionMetrics(source="salesforce", records_read=10000, errors=5)

# Convert to dict (useful for logging, JSON serialization)
print(asdict(metrics))
# {'source': 'salesforce', 'records_read': 10000, 'records_written': 0, 'errors': 5, ...}

print(metrics.success_rate)  # 0.9995
```

---

## Pydantic: Dataclasses with Runtime Validation

Pydantic provides runtime type validation — it actively checks that values match the declared types at creation time.

```python
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class EventSchema(BaseModel):
    event_id: str
    user_id: int
    event_type: str
    amount: float
    timestamp: datetime
    metadata: Optional[dict] = None


# Valid creation:
event = EventSchema(
    event_id="evt_001",
    user_id=12345,
    event_type="purchase",
    amount=99.99,
    timestamp="2024-01-15T10:30:00",  # String → auto-coerced to datetime
)
print(event.timestamp)  # datetime(2024, 1, 15, 10, 30, 0)
print(type(event.timestamp))  # <class 'datetime.datetime'>

# Invalid creation — Pydantic raises ValidationError immediately:
try:
    bad_event = EventSchema(
        event_id="evt_002",
        user_id="not-a-number",  # Should be int
        event_type="purchase",
        amount=99.99,
        timestamp="2024-01-15",
    )
except Exception as e:
    print(e)
    # 1 validation error for EventSchema
    # user_id
    #   Input should be a valid integer, unable to parse string as an integer [type=int_parsing, ...]
```

### Pydantic vs Python Dataclasses

| Feature | Python `@dataclass` | Pydantic `BaseModel` |
|---|---|---|
| Type annotations | Syntax/IDE hints only | Runtime validation |
| Default values | ✓ | ✓ |
| Validation on create | Only in `__post_init__` | Automatic |
| Type coercion | ❌ | ✓ (e.g., "123" → int) |
| JSON serialization | `asdict()` (no types) | `.model_dump()`, `.model_json()` |
| Nested models | Manual | Automatic recursive validation |
| Performance | Faster | Slightly slower (validation overhead) |
| DE use case | Internal data containers, metrics | API responses, external configs, schema enforcement |

---

## Pydantic for Pipeline Configuration

```python
from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional
from enum import Enum


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class S3Config(BaseModel):
    bucket: str
    prefix: str
    region: str = "us-east-1"

    @field_validator("bucket")
    @classmethod
    def bucket_must_not_have_s3_prefix(cls, v: str) -> str:
        if v.startswith("s3://"):
            raise ValueError("bucket should not include 's3://' prefix")
        return v.lower()


class PipelineConfig(BaseModel):
    name: str
    source: S3Config
    target: S3Config
    batch_size: int = Field(default=1000, gt=0, le=100_000)
    write_mode: WriteMode = WriteMode.APPEND
    enable_retries: bool = True
    max_retries: int = Field(default=3, ge=0, le=10)
    notify_on_failure: Optional[str] = None  # Email address


# Create from a dict (e.g., loaded from YAML config file)
config_dict = {
    "name": "daily_events_ingestion",
    "source": {"bucket": "raw-data-bucket", "prefix": "events/dt=2024-01-15/"},
    "target": {"bucket": "processed-bucket", "prefix": "events/cleaned/"},
    "batch_size": 5000,
    "write_mode": "append",
}

config = PipelineConfig(**config_dict)
print(config.source.bucket)   # "raw-data-bucket"
print(config.write_mode)      # WriteMode.APPEND
print(config.batch_size)      # 5000

# Pydantic validates batch_size > 0:
try:
    bad = PipelineConfig(
        name="bad",
        source={"bucket": "bucket", "prefix": "p/"},
        target={"bucket": "bucket2", "prefix": "p2/"},
        batch_size=-100,  # Violates gt=0
    )
except Exception as e:
    print(e)
    # batch_size: Input should be greater than 0
```

---

## Pydantic for API Response Schemas

The most common DE use case: validating that an external API returns the expected structure before ingesting it.

```python
from pydantic import BaseModel, field_validator
from typing import Optional, List
from datetime import datetime


class OrderItem(BaseModel):
    product_id: str
    quantity: int
    unit_price: float
    discount: float = 0.0

    @field_validator("discount")
    @classmethod
    def discount_must_be_valid(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError(f"discount must be between 0 and 1, got {v}")
        return v


class Order(BaseModel):
    order_id: str
    customer_id: int
    items: List[OrderItem]
    total: float
    created_at: datetime
    status: str


class OrdersApiResponse(BaseModel):
    orders: List[Order]
    page: int
    total_pages: int
    total_count: int


# Validate an API response:
import requests
import json


def fetch_and_validate_orders(api_url: str, date: str) -> List[Order]:
    """Fetch orders from API and validate the response schema."""
    response = requests.get(f"{api_url}/orders?date={date}")
    response.raise_for_status()

    try:
        validated = OrdersApiResponse.model_validate(response.json())
    except Exception as e:
        raise ValueError(
            f"API response validation failed for date {date}: {e}\n"
            f"Raw response: {response.text[:500]}"
        ) from e

    print(f"Validated {len(validated.orders)} orders from {validated.total_count} total")
    return validated.orders
```

---

## Serialization: model_dump() and model_json()

```python
from pydantic import BaseModel
from datetime import datetime


class Event(BaseModel):
    event_id: str
    user_id: int
    timestamp: datetime
    metadata: dict = {}


event = Event(
    event_id="evt_001",
    user_id=42,
    timestamp="2024-01-15T10:30:00",
)

# Convert to dict
event_dict = event.model_dump()
print(event_dict)
# {'event_id': 'evt_001', 'user_id': 42, 'timestamp': datetime(2024, 1, 15, 10, 30), 'metadata': {}}

# Convert to JSON string
event_json = event.model_dump_json()
print(event_json)
# '{"event_id":"evt_001","user_id":42,"timestamp":"2024-01-15T10:30:00","metadata":{}}'

# Exclude fields
event_dict_no_meta = event.model_dump(exclude={"metadata"})

# Include only specific fields
event_dict_minimal = event.model_dump(include={"event_id", "user_id"})
```

---

## Key Takeaways for Junior DEs

1. **Use `@dataclass`** for internal data containers where you control both creation and consumption — pipeline state, metrics, intermediate results.
2. **Use `Pydantic BaseModel`** for data that comes from external sources — API responses, config files, user input, database records — where validation is critical.
3. **`field(default_factory=list)`** — never use a mutable default (`[]`) in a dataclass directly; use `field(default_factory=list)`.
4. **Pydantic coerces types** — "123" is automatically converted to `int`, a date string to `datetime`. This is convenient but be aware of what's being coerced.
5. **Validation errors at creation time** catch data quality issues immediately, not hours later when a pipeline crashes at an unexpected point.
