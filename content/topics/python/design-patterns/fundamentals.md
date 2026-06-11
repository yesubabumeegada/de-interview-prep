---
title: "Python Design Patterns - Fundamentals"
topic: python
subtopic: design-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, design-patterns, factory, singleton, strategy, observer]
---

# Python Design Patterns — Fundamentals


## 🎯 Analogy

Think of design patterns like standard recipes in cooking: the Singleton is a shared pot everyone uses, the Factory is a meal-prep service that creates the right dish based on your order, and the Strategy is choosing cooking method (grill vs bake) at runtime.

---
## What Are Design Patterns?

Design patterns are **reusable solutions to common software design problems**. They aren't code you copy-paste — they're blueprints for structuring your classes and functions to solve recurring challenges.

> **Key Insight:** In Data Engineering, patterns help you build pipelines that are testable, extensible, and maintainable. A well-patterned ETL codebase lets you add new data sources without rewriting existing logic.

---

## Why Data Engineers Need Design Patterns

| Problem | Without Pattern | With Pattern |
|---------|----------------|--------------|
| Adding a new data source | Modify existing extract code | Add a new connector class (Factory) |
| Switching transform logic | if/else chains grow | Swap in a new strategy (Strategy) |
| Database connection reuse | Opening connections everywhere | Single shared pool (Singleton) |
| Pipeline configuration | Hardcoded params | Step-by-step builder (Builder) |

---

## Pattern 1: Factory — Create Data Source Connectors

The Factory pattern creates objects without exposing creation logic. You ask for a "postgres connector" and get one back without knowing construction details.

```python
from abc import ABC, abstractmethod

class DataSourceConnector(ABC):
    """Base class for all data source connectors."""
    
    @abstractmethod
    def connect(self) -> None:
        pass
    
    @abstractmethod
    def extract(self, query: str) -> list[dict]:
        pass

class PostgresConnector(DataSourceConnector):
    def __init__(self, host: str, port: int, database: str):
        self.host = host
        self.port = port
        self.database = database
    
    def connect(self) -> None:
        print(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database}")
    
    def extract(self, query: str) -> list[dict]:
        print(f"Executing: {query}")
        return [{"id": 1, "name": "sample"}]

class S3Connector(DataSourceConnector):
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix
    
    def connect(self) -> None:
        print(f"Connecting to S3 bucket: {self.bucket}")
    
    def extract(self, query: str) -> list[dict]:
        print(f"Reading from s3://{self.bucket}/{self.prefix}")
        return [{"file": "data.parquet", "rows": 1000}]

class ConnectorFactory:
    """Factory that creates the right connector based on source type."""
    
    _connectors = {
        "postgres": PostgresConnector,
        "s3": S3Connector,
    }
    
    @classmethod
    def create(cls, source_type: str, **kwargs) -> DataSourceConnector:
        connector_class = cls._connectors.get(source_type)
        if not connector_class:
            raise ValueError(f"Unknown source type: {source_type}")
        return connector_class(**kwargs)

# Usage — caller doesn't need to know which class to instantiate
connector = ConnectorFactory.create("postgres", host="db.prod", port=5432, database="analytics")
connector.connect()
data = connector.extract("SELECT * FROM events LIMIT 10")
```

**When to use:** You have multiple data sources with the same interface but different implementations.

---

## Pattern 2: Strategy — Swappable Transform Logic

The Strategy pattern lets you swap algorithms at runtime. In DE, this means different transformation strategies for different data formats or business rules.

```python
from abc import ABC, abstractmethod
import json
import csv
from io import StringIO

class TransformStrategy(ABC):
    """Interface for transformation strategies."""
    
    @abstractmethod
    def transform(self, raw_data: str) -> list[dict]:
        pass

class JsonTransform(TransformStrategy):
    def transform(self, raw_data: str) -> list[dict]:
        records = json.loads(raw_data)
        # Normalize nested JSON
        return [{"id": r["id"], "value": r.get("nested", {}).get("value")} for r in records]

class CsvTransform(TransformStrategy):
    def transform(self, raw_data: str) -> list[dict]:
        reader = csv.DictReader(StringIO(raw_data))
        return [dict(row) for row in reader]

class DataPipeline:
    """Pipeline that uses a strategy for transformation."""
    
    def __init__(self, strategy: TransformStrategy):
        self._strategy = strategy
    
    def set_strategy(self, strategy: TransformStrategy) -> None:
        self._strategy = strategy
    
    def run(self, raw_data: str) -> list[dict]:
        return self._strategy.transform(raw_data)

# Swap strategies based on input format
pipeline = DataPipeline(strategy=JsonTransform())
result = pipeline.run('[{"id": 1, "nested": {"value": "hello"}}]')

pipeline.set_strategy(CsvTransform())
result = pipeline.run("id,name\n1,Alice\n2,Bob")
```

---

## Pattern 3: Singleton — Database Connection Pool

Singleton ensures only one instance exists. Critical for expensive resources like database connection pools.

```python
class ConnectionPool:
    """Singleton connection pool — only one instance per process."""
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, max_connections: int = 10):
        if self._initialized:
            return
        self._pool: list = []
        self._max_connections = max_connections
        self._initialized = True
        print(f"Pool created with max {max_connections} connections")
    
    def get_connection(self):
        if self._pool:
            return self._pool.pop()
        return self._create_connection()
    
    def _create_connection(self):
        return {"connection": "active", "id": id(self)}

# Both variables point to the same instance
pool_a = ConnectionPool(max_connections=5)
pool_b = ConnectionPool(max_connections=20)  # max_connections ignored — already initialized
print(pool_a is pool_b)  # True
```

> **Caution:** Singletons can make testing harder. In modern Python, prefer module-level instances or dependency injection for new code.

---

## Pattern 4: Builder — Pipeline Configuration

Builder constructs complex objects step by step. Perfect for configuring pipelines with many optional parameters.

```python
class PipelineConfig:
    def __init__(self):
        self.source = None
        self.destination = None
        self.transforms = []
        self.batch_size = 1000
        self.retry_count = 3

class PipelineBuilder:
    def __init__(self):
        self._config = PipelineConfig()
    
    def from_source(self, source: str) -> "PipelineBuilder":
        self._config.source = source
        return self
    
    def to_destination(self, dest: str) -> "PipelineBuilder":
        self._config.destination = dest
        return self
    
    def with_transform(self, transform: str) -> "PipelineBuilder":
        self._config.transforms.append(transform)
        return self
    
    def batch_size(self, size: int) -> "PipelineBuilder":
        self._config.batch_size = size
        return self
    
    def retries(self, count: int) -> "PipelineBuilder":
        self._config.retry_count = count
        return self
    
    def build(self) -> PipelineConfig:
        if not self._config.source or not self._config.destination:
            raise ValueError("Source and destination are required")
        return self._config

# Fluent API for pipeline configuration
config = (
    PipelineBuilder()
    .from_source("s3://raw-data/events/")
    .to_destination("warehouse.analytics.events")
    .with_transform("deduplicate")
    .with_transform("validate_schema")
    .batch_size(5000)
    .retries(5)
    .build()
)
```

---

## Pattern Summary Table

| Pattern | DE Use Case | Key Benefit |
|---------|-------------|-------------|
| Factory | Data source connectors | Add sources without modifying code |
| Strategy | Transform/format logic | Swap algorithms at runtime |
| Singleton | Connection pools | Resource control |
| Builder | Pipeline configuration | Readable, validated construction |

---


## ▶️ Try It Yourself

```python
# Strategy pattern: swap algorithm at runtime
from abc import ABC, abstractmethod

class CompressionStrategy(ABC):
    @abstractmethod
    def compress(self, data: bytes) -> bytes: pass

class GzipStrategy(CompressionStrategy):
    def compress(self, data: bytes) -> bytes:
        import gzip
        return gzip.compress(data)

class NoopStrategy(CompressionStrategy):
    def compress(self, data: bytes) -> bytes:
        return data  # No compression (dev/test)

class FileWriter:
    def __init__(self, strategy: CompressionStrategy = None):
        self.strategy = strategy or NoopStrategy()

    def write(self, path: str, data: bytes):
        compressed = self.strategy.compress(data)
        print(f"Writing {len(data)}B -> {len(compressed)}B to {path}")

# Swap strategy without changing the writer
writer = FileWriter(GzipStrategy())
writer.write("/tmp/orders.gz", b"order_id,amount
1,100
2,200
" * 100)

writer.strategy = NoopStrategy()  # Switch at runtime
writer.write("/tmp/orders.csv", b"order_id,amount
1,100
")
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** When asked "which pattern would you use?", always start by identifying the problem: "I need to create different objects based on type → Factory" or "I need swappable algorithms → Strategy." Show your reasoning, not just the pattern name.

> **Tip 2:** Connect patterns to DE scenarios. Don't talk about web controllers — talk about data source connectors, transform pipelines, and connection pooling. Interviewers want to know you can apply patterns to real pipeline problems.

> **Tip 3:** Know the tradeoffs. Singleton makes testing harder. Factory adds indirection. Strategy adds classes. Be ready to say "I'd use a simple dict mapping for small cases, but the full pattern when the team needs extensibility."
