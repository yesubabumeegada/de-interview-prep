---
title: "Python OOP - Scenario Questions"
topic: python
subtopic: oop
content_type: scenario_question
tags: [python, oop, interview, scenarios, classes, inheritance]
---

# Scenario Questions — Python OOP

<article data-difficulty="junior">

## 🟢 Junior: Design a Pipeline Configuration Class

**Scenario:** Create a `PipelineConfig` class that stores source, destination, batch_size, and retries. It should validate that batch_size > 0 and retries >= 0. Include a method to generate a connection string.

<details>
<summary>✅ Solution</summary>

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PipelineConfig:
    source: str
    destination: str
    batch_size: int = 10000
    retries: int = 3
    timeout_seconds: int = 300
    
    def __post_init__(self):
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")
        if self.retries < 0:
            raise ValueError(f"retries must be non-negative, got {self.retries}")
    
    @property
    def connection_string(self) -> str:
        return f"{self.source} → {self.destination} (batch={self.batch_size})"
    
    def with_overrides(self, **kwargs) -> 'PipelineConfig':
        """Return a new config with specified fields overridden."""
        from dataclasses import asdict
        current = asdict(self)
        current.update(kwargs)
        return PipelineConfig(**current)

# Usage
config = PipelineConfig(source="s3://raw/orders", destination="snowflake://warehouse")
print(config.connection_string)  # "s3://raw/orders → snowflake://warehouse (batch=10000)"

# Override for testing
test_config = config.with_overrides(batch_size=100, retries=0)
```

**Why dataclass:** Less boilerplate than manual `__init__`, auto-generates `__repr__`, `__eq__`, and works with type hints. `__post_init__` provides validation.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Plugin System Using Abstract Classes

**Scenario:** Design a data source plugin system where different connectors (S3, JDBC, Kafka) implement a common interface. Include: `connect()`, `read(query)`, `close()`. Use abstract base classes.

<details>
<summary>✅ Solution</summary>

```python
from abc import ABC, abstractmethod
from typing import Iterator, Any
from contextlib import contextmanager

class DataSource(ABC):
    """Abstract base for all data source plugins."""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the source."""
        pass
    
    @abstractmethod
    def read(self, query: str) -> Iterator[dict]:
        """Read data as an iterator of records."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the connection and release resources."""
        pass
    
    @contextmanager
    def session(self):
        """Context manager for safe connection handling."""
        self.connect()
        try:
            yield self
        finally:
            self.close()


class S3Source(DataSource):
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix
        self._client = None
    
    def connect(self):
        import boto3
        self._client = boto3.client('s3')
    
    def read(self, query: str) -> Iterator[dict]:
        response = self._client.list_objects_v2(Bucket=self.bucket, Prefix=query)
        for obj in response.get('Contents', []):
            yield {'key': obj['Key'], 'size': obj['Size']}
    
    def close(self):
        self._client = None


class JDBCSource(DataSource):
    def __init__(self, connection_url: str, username: str, password: str):
        self.url = connection_url
        self.username = username
        self.password = password
        self._conn = None
    
    def connect(self):
        import psycopg2
        self._conn = psycopg2.connect(self.url, user=self.username, password=self.password)
    
    def read(self, query: str) -> Iterator[dict]:
        cursor = self._conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        for row in cursor:
            yield dict(zip(columns, row))
    
    def close(self):
        if self._conn:
            self._conn.close()


# Usage with the common interface
def extract_data(source: DataSource, query: str) -> list[dict]:
    """Works with ANY DataSource implementation."""
    with source.session():
        return list(source.read(query))

# S3
s3_data = extract_data(S3Source("my-bucket", "raw/"), "orders/2024-01-15/")

# JDBC
db_data = extract_data(
    JDBCSource("postgresql://host:5432/db", "user", "pass"),
    "SELECT * FROM orders WHERE date = '2024-01-15'"
)
```

**Key OOP principles demonstrated:**
- **Abstraction:** `DataSource` ABC defines the contract
- **Polymorphism:** `extract_data()` works with any implementation
- **Encapsulation:** Connection details hidden inside each class
- **Open/Closed:** Add new sources (KafkaSource) without modifying existing code

</details>

</article>
