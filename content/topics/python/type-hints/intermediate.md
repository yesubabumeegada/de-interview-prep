---
title: "Python Type Hints - Intermediate"
topic: python
subtopic: type-hints
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, type-hints, generics, protocol, typevar, pydantic]
---

# Python Type Hints — Intermediate

## Generic Types with TypeVar

TypeVar lets you write functions and classes that work with any type while preserving type relationships.

```python
from typing import TypeVar, Sequence

T = TypeVar('T')

def first_element(items: Sequence[T]) -> T:
    """Returns the first element, preserving its type."""
    return items[0]

# mypy knows these types:
name = first_element(["alice", "bob"])    # str
count = first_element([1, 2, 3])          # int
```

### Bounded TypeVar (Constrained Generics)

```python
from typing import TypeVar
from datetime import datetime

# T must be a subclass of (str | int | datetime)
Sortable = TypeVar('Sortable', str, int, datetime)

def sort_column(values: list[Sortable]) -> list[Sortable]:
    return sorted(values)

# TypeVar bound to a base class
from typing import TypeVar
import pandas as pd

DataFrameType = TypeVar('DataFrameType', bound=pd.DataFrame)

def validate_schema(df: DataFrameType) -> DataFrameType:
    """Returns the same DataFrame subtype it receives."""
    assert len(df.columns) > 0, "Empty DataFrame"
    return df
```

---

## Protocol — Structural Typing (Duck Typing with Safety)

Protocol defines an interface without inheritance — if an object has the right methods, it matches.

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class DataSource(Protocol):
    """Any object that has read_batch and close methods."""
    def read_batch(self, size: int) -> list[dict]: ...
    def close(self) -> None: ...

class S3Source:
    def read_batch(self, size: int) -> list[dict]:
        return [{"key": "value"}]  # read from S3
    def close(self) -> None:
        pass  # cleanup

class KafkaSource:
    def read_batch(self, size: int) -> list[dict]:
        return [{"event": "click"}]  # consume from Kafka
    def close(self) -> None:
        pass  # commit offsets

def ingest_data(source: DataSource, batch_size: int = 1000) -> int:
    """Works with ANY object that implements DataSource protocol."""
    total = 0
    while batch := source.read_batch(batch_size):
        total += len(batch)
    source.close()
    return total

# Both work — no inheritance needed
ingest_data(S3Source())
ingest_data(KafkaSource())
```

---

## TypedDict for JSON-Like Data

TypedDict gives structure to dictionaries — common for API responses and config files.

```python
from typing import TypedDict, NotRequired

class PipelineMetrics(TypedDict):
    rows_read: int
    rows_written: int
    duration_seconds: float
    errors: NotRequired[list[str]]  # optional key

class APIResponse(TypedDict):
    status: str
    data: list[dict]
    pagination: dict[str, int]

def fetch_records(endpoint: str) -> APIResponse:
    response = requests.get(endpoint)
    return response.json()  # mypy validates returned shape

# Access with full type safety
result = fetch_records("/api/users")
total = result["pagination"]["total"]  # mypy knows this is int
```

---

## Literal Types

Restrict a value to specific constants.

```python
from typing import Literal

FileFormat = Literal["parquet", "csv", "json", "avro"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

def write_output(
    data: list[dict],
    path: str,
    format: FileFormat,
    compression: Literal["snappy", "gzip", "none"] = "snappy"
) -> None:
    if format == "parquet":
        write_parquet(data, path, compression)
    elif format == "csv":
        write_csv(data, path)
    # mypy error if you check format == "xml" — impossible value

def set_log_level(level: LogLevel) -> None:
    ...

set_log_level("DEBUG")    # OK
set_log_level("TRACE")    # mypy error: not in Literal
```

---

## Overloaded Functions

Tell mypy that different input types produce different output types.

```python
from typing import overload

@overload
def parse_record(raw: str) -> dict: ...
@overload
def parse_record(raw: bytes) -> dict: ...
@overload
def parse_record(raw: list[str]) -> list[dict]: ...

def parse_record(raw: str | bytes | list[str]) -> dict | list[dict]:
    if isinstance(raw, list):
        return [json.loads(r) for r in raw]
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8')
    return json.loads(raw)

# mypy knows:
single = parse_record('{"a": 1}')       # dict
batch = parse_record(['{"a": 1}'])       # list[dict]
```

---

## Pydantic for Runtime Validation

Type hints only work at static analysis time. Pydantic enforces types at runtime.

```python
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class EventRecord(BaseModel):
    event_id: str
    user_id: int
    timestamp: datetime
    amount: float = Field(ge=0, description="Must be non-negative")
    source: Literal["web", "mobile", "api"]

    @field_validator('event_id')
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        if not v.startswith("evt_"):
            raise ValueError("event_id must start with 'evt_'")
        return v

# Runtime validation
record = EventRecord(
    event_id="evt_123",
    user_id=42,
    timestamp="2024-01-15T10:30:00",  # auto-parsed to datetime
    amount=99.50,
    source="web"
)

# Raises ValidationError with clear message
try:
    bad = EventRecord(event_id="bad", user_id="not_int", ...)
except ValidationError as e:
    print(e.json())
```

---

## Typing with Dataclasses

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class ETLJobConfig:
    job_name: str
    source_tables: list[str]
    target_schema: str
    batch_size: int = 10_000
    max_retries: int = 3
    notify_on_failure: list[str] = field(default_factory=list)
    description: Optional[str] = None

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")

config = ETLJobConfig(
    job_name="daily_user_sync",
    source_tables=["raw.users", "raw.profiles"],
    target_schema="analytics",
    notify_on_failure=["data-team@company.com"]
)
```

---

## ParamSpec for Decorator Typing

ParamSpec preserves the signature of decorated functions.

```python
from typing import ParamSpec, TypeVar, Callable
from functools import wraps
import time

P = ParamSpec('P')
R = TypeVar('R')

def retry(max_attempts: int = 3) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Typed decorator — preserves wrapped function's signature."""
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(2 ** attempt)
            raise RuntimeError("Unreachable")
        return wrapper
    return decorator

@retry(max_attempts=3)
def fetch_data(url: str, timeout: int = 30) -> dict:
    ...

# mypy knows fetch_data still takes (url: str, timeout: int) -> dict
```

---

## Comparison Table: When to Use What

| Tool | Purpose | Enforcement |
|------|---------|-------------|
| Basic type hints | Document interfaces | Static (mypy) |
| TypeVar | Generic functions/classes | Static |
| Protocol | Structural interfaces (duck typing) | Static |
| TypedDict | Typed dictionaries | Static |
| Literal | Restrict to constants | Static |
| Pydantic | Full runtime validation | Runtime |
| dataclass | Structured data containers | Static + `__post_init__` |
| @overload | Multiple return types | Static |

---

## Interview Tips

> **Tip 1:** "When would you use Protocol vs ABC?" — "Protocol is structural typing (duck typing with safety) — classes don't need to inherit anything. Use it when you want to accept any object with the right methods. ABC requires explicit inheritance. Protocol is preferred in modern Python for dependency injection and plugin architectures because it's less coupled."

> **Tip 2:** "How does Pydantic differ from dataclasses?" — "Dataclasses are data containers with static type checking only. Pydantic validates at runtime — it coerces types, enforces constraints, and raises clear errors. For pipeline configs and API payloads where bad data arrives at runtime, Pydantic catches issues that mypy can't. The tradeoff is Pydantic is slower due to validation overhead."

> **Tip 3:** "What is TypeVar used for?" — "TypeVar creates generic functions that preserve type relationships. If a function takes a list[T] and returns T, mypy knows that passing list[str] returns str. Without TypeVar, you'd lose type information and mypy would infer Any. It's essential for writing reusable, type-safe utilities."
