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

---

## ⚡ Quick-fire Q&A

**Q: What are the four pillars of object-oriented programming and how do they apply in Python?**
A: Encapsulation (bundling data + methods, using `_` conventions for private members), Inheritance (subclassing to reuse/extend behavior), Polymorphism (different classes implementing the same interface—Python uses duck typing rather than strict interface enforcement), and Abstraction (hiding implementation details via abstract base classes with `abc.ABC`).

**Q: What is the difference between `__str__` and `__repr__`?**
A: `__repr__` should return an unambiguous string that could recreate the object (developer-facing). `__str__` should return a readable, human-friendly string (user-facing). `str(obj)` calls `__str__`; `repr(obj)` calls `__repr__`. If only `__repr__` is defined, `str()` falls back to it.

**Q: What is `@classmethod` vs. `@staticmethod`?**
A: A `@classmethod` receives the class (`cls`) as the first argument and can access/modify class state—commonly used as alternative constructors. A `@staticmethod` receives no implicit first argument and is essentially a regular function namespaced inside the class—use it when the function is logically related to the class but does not need `self` or `cls`.

**Q: What is a dataclass and when is it preferred over a regular class?**
A: `@dataclass` auto-generates `__init__`, `__repr__`, `__eq__`, and optionally `__hash__` and `__lt__` based on annotated class attributes. Use it for simple data-holding classes (records, configuration objects, pipeline metadata) to eliminate boilerplate. For complex initialization logic or methods, a regular class may be clearer.

**Q: What is the MRO (Method Resolution Order) and how does Python determine it?**
A: MRO defines the order in which Python searches classes for a method in multiple inheritance. Python uses the C3 linearization algorithm. Inspect it with `ClassName.__mro__` or `ClassName.mro()`. The MRO ensures consistent, predictable lookup order and prevents diamond inheritance ambiguity.

**Q: What is `super()` and why should you use it instead of calling the parent class directly?**
A: `super()` returns a proxy that delegates method calls to the next class in the MRO, rather than the immediate parent. This is essential in multiple inheritance: calling the parent directly (`ParentClass.method(self)`) skips the MRO and breaks cooperative multiple inheritance. Always use `super().__init__(...)` in `__init__`.

**Q: What is an abstract base class (ABC) and when is it useful in data engineering?**
A: An ABC (using `abc.ABC` and `@abstractmethod`) defines an interface contract—subclasses must implement all abstract methods or they cannot be instantiated. In DE, define an ABC for sources, sinks, or transformers: `class BaseExtractor(ABC): @abstractmethod def extract(self): ...`. This enforces a consistent interface across S3, database, and API extractors.

**Q: What is composition vs. inheritance and when do you prefer composition in Python?**
A: Inheritance models "is-a" relationships; composition models "has-a" relationships. Prefer composition when you want to combine behaviors from multiple independent sources without the tight coupling of inheritance. In Python, composition is often more flexible—a `Pipeline` class that holds a list of `Step` objects is easier to extend than a deep inheritance hierarchy.

---

## 💼 Interview Tips

- Abstract base classes for extractor/transformer/loader interfaces is a DE-specific OOP pattern. Show you design plugin architectures: define the ABC, let each source (S3, Kafka, Postgres) implement it, and write generic pipeline code against the interface.
- Dataclasses are the modern choice for pipeline configuration and metadata objects—demonstrate you know `@dataclass(frozen=True)` for immutable records and `field(default_factory=list)` for mutable defaults.
- Senior interviewers probe `super()` in multiple inheritance: walk through a diamond inheritance example and explain why cooperative `super()` is essential. Candidates who call `ParentClass.__init__(self)` directly reveal a gap.
- Composition over inheritance is a senior-level design principle. Describe a scenario where you refactored a deep class hierarchy into a composition-based pipeline that was easier to test and extend.
- Python's duck typing enables polymorphism without formal interfaces—but ABCs add contract enforcement at instantiation time. Know when enforced interfaces (ABCs) are worth the overhead vs. duck typing convention.
