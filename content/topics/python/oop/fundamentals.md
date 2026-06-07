---
title: "Python OOP - Fundamentals"
topic: python
subtopic: oop
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, oop, classes, inheritance, polymorphism, encapsulation, dataclasses]
---

# Python OOP — Fundamentals

## Why This Matters for DE Interviews

Object-Oriented Programming in Python is how you build maintainable, testable data pipelines. Interviewers want to see that you can design **reusable components** — custom extractors, transformer classes, validation layers — not just write scripts.

---

## 1. Classes — The Blueprint

**What it is:** A class defines the structure and behavior of objects. It bundles data (attributes) and functions (methods) together.

```python
class DataExtractor:
    """Base class for all data extractors in the pipeline."""
    
    def __init__(self, source: str, batch_size: int = 1000):
        self.source = source
        self.batch_size = batch_size
        self._records_processed = 0  # "private" by convention
    
    def extract(self) -> list[dict]:
        """Override in subclasses."""
        raise NotImplementedError("Subclasses must implement extract()")
    
    @property
    def records_processed(self) -> int:
        """Read-only access to processed count."""
        return self._records_processed
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source='{self.source}')"

# Usage
extractor = DataExtractor("s3://bucket/raw/")
print(extractor)  # DataExtractor(source='s3://bucket/raw/')
```

### Instance vs Class vs Static Methods

```python
class PipelineMetrics:
    _instance_count = 0  # Class variable — shared across all instances
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name  # Instance variable
        self.metrics: dict[str, float] = {}
        PipelineMetrics._instance_count += 1
    
    def record(self, metric_name: str, value: float) -> None:
        """Instance method — operates on a specific instance."""
        self.metrics[metric_name] = value
    
    @classmethod
    def get_instance_count(cls) -> int:
        """Class method — operates on the class, not an instance."""
        return cls._instance_count
    
    @staticmethod
    def validate_metric_name(name: str) -> bool:
        """Static method — utility that doesn't need class or instance."""
        return bool(name) and name.replace("_", "").isalnum()
```

---

## 2. Inheritance — Reusing and Specializing

**What it is:** A child class inherits attributes and methods from a parent class, then adds or overrides behavior.

```python
class S3Extractor(DataExtractor):
    """Specialized extractor for S3 sources."""
    
    def __init__(self, bucket: str, prefix: str, **kwargs):
        super().__init__(source=f"s3://{bucket}/{prefix}", **kwargs)
        self.bucket = bucket
        self.prefix = prefix
    
    def extract(self) -> list[dict]:
        """Concrete implementation for S3."""
        # In real code: boto3 calls here
        print(f"Extracting from {self.source} in batches of {self.batch_size}")
        records = [{"id": i, "data": f"record_{i}"} for i in range(self.batch_size)]
        self._records_processed += len(records)
        return records


class PostgresExtractor(DataExtractor):
    """Specialized extractor for PostgreSQL."""
    
    def __init__(self, connection_string: str, query: str, **kwargs):
        super().__init__(source=connection_string, **kwargs)
        self.query = query
    
    def extract(self) -> list[dict]:
        """Concrete implementation for Postgres."""
        print(f"Running: {self.query[:50]}...")
        records = []  # Would execute query here
        self._records_processed += len(records)
        return records
```

### Method Resolution Order (MRO)

```python
class A:
    def greet(self): return "A"

class B(A):
    def greet(self): return "B"

class C(A):
    def greet(self): return "C"

class D(B, C):
    pass

# Python uses C3 linearization
print(D.__mro__)
# (<class 'D'>, <class 'B'>, <class 'C'>, <class 'A'>, <class 'object'>)
print(D().greet())  # "B" — follows MRO left to right
```

---

## 3. Polymorphism — Same Interface, Different Behavior

**What it is:** Different classes respond to the same method call in their own way. This enables writing generic code that works with any compatible object.

```python
def run_extraction(extractors: list[DataExtractor]) -> list[dict]:
    """Works with ANY extractor subclass — that's polymorphism."""
    all_records = []
    for extractor in extractors:
        records = extractor.extract()  # Each class has its own implementation
        all_records.extend(records)
    return all_records

# Usage — mixed types, same interface
extractors = [
    S3Extractor("my-bucket", "raw/events/", batch_size=5000),
    PostgresExtractor("postgresql://host/db", "SELECT * FROM users"),
]
results = run_extraction(extractors)
```

---

## 4. Encapsulation — Controlling Access

**What it is:** Hiding internal implementation details and exposing only what's needed. Python uses conventions (not strict enforcement).

```python
class DatabaseConnection:
    """Encapsulates connection management details."""
    
    def __init__(self, host: str, port: int, database: str):
        self._host = host          # "protected" — internal use
        self._port = port
        self.__password = None     # "private" — name-mangled
        self._connection = None
    
    @property
    def is_connected(self) -> bool:
        """Read-only property — external code can check, not modify."""
        return self._connection is not None
    
    def set_password(self, password: str) -> None:
        """Controlled write access with validation."""
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters")
        self.__password = password
    
    def connect(self) -> None:
        if not self.__password:
            raise RuntimeError("Password not set")
        # Internal connection logic hidden
        self._connection = f"connected to {self._host}:{self._port}/{self._connection}"
```

**Python naming conventions:**
- `_single_underscore` — "protected" by convention, don't access from outside
- `__double_underscore` — name-mangled to `_ClassName__attr`, harder to access accidentally
- No underscore — public, part of the API

---

## 5. Abstract Classes — Enforcing Contracts

**What it is:** Abstract Base Classes (ABCs) define interfaces that subclasses MUST implement. They can't be instantiated directly.

```python
from abc import ABC, abstractmethod
from typing import Any

class BaseTransformer(ABC):
    """All transformers must implement these methods."""
    
    @abstractmethod
    def transform(self, data: list[dict]) -> list[dict]:
        """Apply transformation to data."""
        pass
    
    @abstractmethod
    def validate(self, record: dict) -> bool:
        """Validate a single record."""
        pass
    
    def transform_batch(self, data: list[dict]) -> list[dict]:
        """Concrete method — shared logic across all transformers."""
        valid_records = [r for r in data if self.validate(r)]
        return self.transform(valid_records)


class CleansingTransformer(BaseTransformer):
    """Concrete implementation — must implement ALL abstract methods."""
    
    def __init__(self, required_fields: list[str]):
        self.required_fields = required_fields
    
    def transform(self, data: list[dict]) -> list[dict]:
        return [{k: v.strip() if isinstance(v, str) else v 
                 for k, v in record.items()} for record in data]
    
    def validate(self, record: dict) -> bool:
        return all(field in record for field in self.required_fields)

# This works:
transformer = CleansingTransformer(["id", "name"])

# This raises TypeError: Can't instantiate abstract class
# base = BaseTransformer()
```

---

## 6. Dunder (Magic) Methods

**What they are:** Special methods with double underscores that Python calls implicitly. They let your objects work with built-in operations.

```python
class DataBatch:
    """A batch of records that behaves like a Python collection."""
    
    def __init__(self, records: list[dict], batch_id: str):
        self.records = records
        self.batch_id = batch_id
    
    def __len__(self) -> int:
        """len(batch) works."""
        return len(self.records)
    
    def __getitem__(self, index) -> dict:
        """batch[0] and slicing work."""
        return self.records[index]
    
    def __iter__(self):
        """for record in batch: works."""
        return iter(self.records)
    
    def __contains__(self, item) -> bool:
        """'x in batch' works."""
        return item in self.records
    
    def __add__(self, other: "DataBatch") -> "DataBatch":
        """batch1 + batch2 merges records."""
        return DataBatch(
            self.records + other.records,
            batch_id=f"{self.batch_id}+{other.batch_id}"
        )
    
    def __repr__(self) -> str:
        return f"DataBatch(id='{self.batch_id}', size={len(self)})"
    
    def __eq__(self, other) -> bool:
        return self.records == other.records

# Usage — feels like a native Python object
batch = DataBatch([{"id": 1}, {"id": 2}, {"id": 3}], "batch_001")
print(len(batch))        # 3
print(batch[0])          # {"id": 1}
for record in batch:     # Iteration works
    print(record)
```

---

## 7. Dataclasses — Modern Python Data Containers

**What they are:** Python 3.7+ decorator that auto-generates `__init__`, `__repr__`, `__eq__`, and more. Perfect for data transfer objects.

```python
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

@dataclass
class PipelineConfig:
    """Configuration with auto-generated boilerplate."""
    name: str
    source_path: str
    destination_path: str
    batch_size: int = 10000
    retry_count: int = 3
    tags: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    description: Optional[str] = None

# Auto-generated __init__ — no boilerplate!
config = PipelineConfig(
    name="daily_ingest",
    source_path="s3://raw/events/",
    destination_path="snowflake://warehouse/events",
    tags=["production", "critical"],
)

# Auto-generated __repr__
print(config)
# PipelineConfig(name='daily_ingest', source_path=..., batch_size=10000, ...)

# Auto-generated __eq__ (compares all fields)
config2 = PipelineConfig("daily_ingest", "s3://raw/events/", "snowflake://warehouse/events")
print(config == config2)  # Compares field by field

# Convert to dict (useful for serialization)
config_dict = asdict(config)
```

### Frozen Dataclasses (Immutable)

```python
@dataclass(frozen=True)
class TableSchema:
    """Immutable schema definition — can be used as dict key."""
    database: str
    schema: str
    table: str
    
    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

schema = TableSchema("analytics", "public", "user_events")
# schema.database = "other"  # Raises FrozenInstanceError!

# Frozen dataclasses are hashable — can be dict keys or set members
schema_cache = {schema: ["col1", "col2", "col3"]}
```

---

## Putting It All Together — Pipeline Design

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class PipelineResult:
    records_processed: int = 0
    records_failed: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    errors: list[str] = field(default_factory=list)

class PipelineStep(ABC):
    @abstractmethod
    def execute(self, data: list[dict]) -> list[dict]:
        pass

class ValidateStep(PipelineStep):
    def __init__(self, required_fields: list[str]):
        self.required_fields = required_fields
    
    def execute(self, data: list[dict]) -> list[dict]:
        return [r for r in data if all(f in r for f in self.required_fields)]

class TransformStep(PipelineStep):
    def __init__(self, transformations: dict[str, callable]):
        self.transformations = transformations
    
    def execute(self, data: list[dict]) -> list[dict]:
        results = []
        for record in data:
            for field_name, func in self.transformations.items():
                if field_name in record:
                    record[field_name] = func(record[field_name])
            results.append(record)
        return results

class Pipeline:
    def __init__(self, steps: list[PipelineStep]):
        self.steps = steps
    
    def run(self, data: list[dict]) -> PipelineResult:
        result = PipelineResult()
        current_data = data
        for step in self.steps:
            current_data = step.execute(current_data)
        result.records_processed = len(current_data)
        result.records_failed = len(data) - len(current_data)
        return result

# Usage
pipeline = Pipeline([
    ValidateStep(["id", "name", "email"]),
    TransformStep({"email": str.lower, "name": str.strip}),
])
```

---

## Interview Tips

> **Tip 1:** When asked "How would you design a data pipeline in Python?", reach for ABCs + composition. Show an abstract `PipelineStep` class with concrete implementations. Interviewers love seeing the Strategy pattern applied to ETL.

> **Tip 2:** Know when to use `@dataclass` vs a regular class. Use dataclasses for DTOs (data transfer objects) like configs, records, and results. Use regular classes when you need complex behavior, custom `__init__` logic, or inheritance hierarchies.

> **Tip 3:** If asked about encapsulation in Python, explain that Python uses conventions (`_protected`, `__private`) rather than enforcement. Mention `@property` for computed attributes and controlled access — it shows you write production-quality code, not just scripts.
