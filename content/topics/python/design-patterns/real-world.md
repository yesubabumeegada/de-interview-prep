---
title: "Python Design Patterns - Real World"
topic: python
subtopic: design-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, design-patterns, production, etl-framework]
---

# Python Design Patterns — Real World Production Patterns

## Combining Patterns in Production ETL Systems

Real systems compose multiple patterns. These four examples show how.

---

## Pattern 1: Configurable ETL Framework (Factory + Strategy + Builder)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

class TransformStrategy(ABC):
    @abstractmethod
    def apply(self, records: list[dict]) -> list[dict]: pass

class DeduplicateStrategy(TransformStrategy):
    def __init__(self, key_fields: list[str]):
        self.key_fields = key_fields
    def apply(self, records: list[dict]) -> list[dict]:
        seen = set()
        return [r for r in records if (k := tuple(r.get(f) for f in self.key_fields)) not in seen and not seen.add(k)]

class SourceConnector(ABC):
    @abstractmethod
    def extract(self) -> list[dict]: pass

class SourceFactory:
    _registry: dict[str, type] = {}
    @classmethod
    def register(cls, name: str, klass: type): cls._registry[name] = klass
    @classmethod
    def create(cls, source_type: str, **kwargs) -> SourceConnector:
        return cls._registry[source_type](**kwargs)

@dataclass
class ETLPipeline:
    name: str
    source: SourceConnector
    transforms: list[TransformStrategy] = field(default_factory=list)

    def run(self) -> dict:
        data = self.source.extract()
        for t in self.transforms:
            data = t.apply(data)
        return {"pipeline": self.name, "output_rows": len(data)}

class PipelineBuilder:
    def __init__(self, name: str):
        self._name = name
        self._source = None
        self._transforms = []

    def from_source(self, source_type: str, **kwargs) -> "PipelineBuilder":
        self._source = SourceFactory.create(source_type, **kwargs)
        return self

    def add_dedup(self, keys: list[str]) -> "PipelineBuilder":
        self._transforms.append(DeduplicateStrategy(keys))
        return self

    def build(self) -> ETLPipeline:
        return ETLPipeline(self._name, self._source, self._transforms)
```

---

## Pattern 2: Plugin-Based Data Connector System

Teams add connectors without modifying core code. Uses Registry + Abstract Base.

```python
import importlib, pkgutil
from abc import ABC, abstractmethod

class ConnectorPlugin(ABC):
    @classmethod
    @abstractmethod
    def name(cls) -> str: pass
    @abstractmethod
    def read(self, **kwargs) -> list[dict]: pass
    @abstractmethod
    def write(self, data: list[dict], **kwargs) -> int: pass

class PluginRegistry:
    _plugins: dict[str, type[ConnectorPlugin]] = {}

    @classmethod
    def register(cls, plugin_class: type[ConnectorPlugin]) -> None:
        cls._plugins[plugin_class.name()] = plugin_class

    @classmethod
    def get(cls, name: str, **kwargs) -> ConnectorPlugin:
        if name not in cls._plugins:
            raise KeyError(f"Plugin '{name}' not found. Available: {list(cls._plugins.keys())}")
        return cls._plugins[name](**kwargs)

    @classmethod
    def discover(cls, package_path: str) -> None:
        """Auto-discover plugins in a package."""
        package = importlib.import_module(package_path)
        for _, mod_name, _ in pkgutil.iter_modules(package.__path__):
            module = importlib.import_module(f"{package_path}.{mod_name}")
            for attr in dir(module):
                obj = getattr(module, attr)
                if isinstance(obj, type) and issubclass(obj, ConnectorPlugin) and obj is not ConnectorPlugin:
                    cls.register(obj)

class BigQueryPlugin(ConnectorPlugin):
    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
    @classmethod
    def name(cls) -> str: return "bigquery"
    def read(self, **kwargs) -> list[dict]: return [{"source": "bq"}]
    def write(self, data: list[dict], **kwargs) -> int: return len(data)

PluginRegistry.register(BigQueryPlugin)
bq = PluginRegistry.get("bigquery", project="my-project", dataset="analytics")
```

---

## Pattern 3: Observable Pipeline with Metrics (Observer + Decorator)

```python
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable

@dataclass
class StageMetrics:
    stage_name: str
    input_count: int
    output_count: int
    duration_ms: float

class MetricsCollector:
    def __init__(self):
        self.metrics: list[StageMetrics] = []
    def record(self, m: StageMetrics): self.metrics.append(m)
    def summary(self) -> dict:
        return {"stages": len(self.metrics), "total_ms": sum(m.duration_ms for m in self.metrics)}

def observed_stage(collector: MetricsCollector):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(data: list[dict], *args, **kwargs) -> list[dict]:
            start = time.perf_counter()
            result = func(data, *args, **kwargs)
            ms = (time.perf_counter() - start) * 1000
            collector.record(StageMetrics(func.__name__, len(data), len(result), ms))
            return result
        return wrapper
    return decorator

metrics = MetricsCollector()

@observed_stage(metrics)
def extract(data: list[dict]) -> list[dict]:
    return [{"id": i, "val": i * 10} for i in range(1000)]

@observed_stage(metrics)
def transform(data: list[dict]) -> list[dict]:
    return [r for r in data if r["val"] > 500]

data = extract([])
data = transform(data)
print(metrics.summary())
```

---

## Pattern 4: Retry with Circuit Breaker for API Extraction

```python
import time, random
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"

@dataclass
class CircuitBreaker:
    threshold: int = 5
    timeout: float = 30.0
    _failures: int = field(default=0, init=False)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _last_fail: float = field(default=0.0, init=False)

    @property
    def is_open(self) -> bool:
        if self._state == CircuitState.OPEN and time.time() - self._last_fail > self.timeout:
            self._state = CircuitState.CLOSED
            self._failures = 0
        return self._state == CircuitState.OPEN

    def record_failure(self):
        self._failures += 1
        self._last_fail = time.time()
        if self._failures >= self.threshold:
            self._state = CircuitState.OPEN

    def record_success(self):
        self._failures = 0
        self._state = CircuitState.CLOSED

def retry_with_breaker(retries: int = 3, breaker: CircuitBreaker = None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if breaker and breaker.is_open:
                raise RuntimeError("Circuit breaker OPEN")
            for attempt in range(retries):
                try:
                    result = func(*args, **kwargs)
                    if breaker: breaker.record_success()
                    return result
                except Exception as e:
                    if breaker: breaker.record_failure()
                    if attempt == retries - 1: raise
                    time.sleep(2 ** attempt + random.uniform(0, 0.5))
        return wrapper
    return decorator

api_breaker = CircuitBreaker(threshold=3, timeout=60)

@retry_with_breaker(retries=3, breaker=api_breaker)
def fetch_api(endpoint: str) -> list[dict]:
    import requests
    return requests.get(endpoint, timeout=10).json()["results"]
```

---

## Interview Tips

> **Tip 1:** Show how patterns compose: "Factory creates connectors, Strategy handles transforms, Builder assembles the pipeline from config." This demonstrates architectural thinking beyond single-pattern knowledge.

> **Tip 2:** For circuit breaker, give real metrics: "Our API extractor hit rate limits at 50 req/s. The breaker prevents quota burn and gives the service recovery time." Concrete numbers show production experience.

> **Tip 3:** Plugin systems show scalability thinking. Explain how Registry lets other teams contribute connectors independently — they implement the interface and register. This mirrors how Airflow providers and dbt adapters work.
