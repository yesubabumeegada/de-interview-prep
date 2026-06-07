---
title: "Python Design Patterns - Scenario Questions"
topic: python
subtopic: design-patterns
content_type: scenario_question
tags: [python, design-patterns, interview, scenarios]
---

# Scenario Questions — Python Design Patterns

<article data-difficulty="junior">

## 🟢 Junior: Identify the Right Pattern

**Scenario:** Your pipeline extracts from 5 sources (PostgreSQL, MySQL, S3, API, CSV). The code has a giant `if/elif` block creating connections. A new developer asks: "We need Kafka. Where do I add it?"

Which pattern would you recommend to refactor this?

<details>
<summary>✅ Solution</summary>

**Answer: Factory Pattern**

```python
from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def connect(self) -> None: pass
    @abstractmethod
    def read(self) -> list[dict]: pass

class PostgresSource(DataSource):
    def __init__(self, host: str, port: int, db: str):
        self.host, self.port, self.db = host, port, db
    def connect(self): print(f"Connected to {self.host}")
    def read(self) -> list[dict]: return []

class KafkaSource(DataSource):
    def __init__(self, servers: str, topic: str):
        self.servers, self.topic = servers, topic
    def connect(self): print(f"Connected to Kafka: {self.servers}")
    def read(self) -> list[dict]: return []

class SourceFactory:
    _sources: dict[str, type[DataSource]] = {}

    @classmethod
    def register(cls, name: str, klass: type[DataSource]):
        cls._sources[name] = klass

    @classmethod
    def create(cls, name: str, **kwargs) -> DataSource:
        if name not in cls._sources:
            raise ValueError(f"Unknown: {name}")
        return cls._sources[name](**kwargs)

SourceFactory.register("postgres", PostgresSource)
SourceFactory.register("kafka", KafkaSource)

# Adding Kafka = create class + register. Zero changes to existing code.
source = SourceFactory.create("kafka", servers="localhost:9092", topic="events")
```

**Why Factory:** Open/Closed principle — add sources without modifying existing code.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement Strategy for File Format Readers

**Scenario:** Your pipeline receives CSV, JSON, and Parquet files. Implement a Strategy pattern where:
1. Each format has its own reader
2. Pipeline selects the right strategy based on file extension
3. Adding Avro requires zero changes to existing code

<details>
<summary>✅ Solution</summary>

```python
from abc import ABC, abstractmethod
from pathlib import Path
import json, csv

class FileReaderStrategy(ABC):
    @abstractmethod
    def read(self, path: Path) -> list[dict]: pass
    @abstractmethod
    def supports(self, ext: str) -> bool: pass

class CsvReader(FileReaderStrategy):
    def read(self, path: Path) -> list[dict]:
        with open(path) as f:
            return [dict(row) for row in csv.DictReader(f)]
    def supports(self, ext: str) -> bool:
        return ext in (".csv", ".tsv")

class JsonReader(FileReaderStrategy):
    def read(self, path: Path) -> list[dict]:
        with open(path) as f:
            return json.load(f)
    def supports(self, ext: str) -> bool:
        return ext in (".json", ".jsonl")

class ParquetReader(FileReaderStrategy):
    def read(self, path: Path) -> list[dict]:
        import pyarrow.parquet as pq
        return pq.read_table(path).to_pylist()
    def supports(self, ext: str) -> bool:
        return ext == ".parquet"

class FileIngestionPipeline:
    def __init__(self):
        self._strategies: list[FileReaderStrategy] = [CsvReader(), JsonReader(), ParquetReader()]

    def ingest(self, filepath: str) -> list[dict]:
        path = Path(filepath)
        for s in self._strategies:
            if s.supports(path.suffix):
                return s.read(path)
        raise ValueError(f"No reader for: {path.suffix}")

    def register(self, strategy: FileReaderStrategy) -> None:
        self._strategies.append(strategy)

# Add Avro — zero changes to existing code
class AvroReader(FileReaderStrategy):
    def read(self, path: Path) -> list[dict]:
        import fastavro
        with open(path, "rb") as f:
            return list(fastavro.reader(f))
    def supports(self, ext: str) -> bool:
        return ext == ".avro"

pipeline = FileIngestionPipeline()
pipeline.register(AvroReader())
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Full ETL Framework Using Multiple Patterns

**Scenario:** Design an ETL framework with:
1. Pluggable sources/sinks (Factory/Registry)
2. Configurable transforms (Strategy)
3. Monitoring without coupling (Observer)
4. Testable with no external deps (DI)
5. Config-file pipeline definition (Builder)

<details>
<summary>✅ Solution</summary>

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

# Core interfaces (DI-ready)
class Source(ABC):
    @abstractmethod
    def extract(self) -> list[dict]: pass

class Sink(ABC):
    @abstractmethod
    def load(self, records: list[dict]) -> int: pass

class Transform(ABC):
    @abstractmethod
    def apply(self, records: list[dict]) -> list[dict]: pass

class Monitor(ABC):
    @abstractmethod
    def on_stage(self, stage: str, in_count: int, out_count: int): pass

# Registry (Factory with decorators)
class Registry:
    _sources: dict[str, type] = {}
    _transforms: dict[str, type] = {}
    _sinks: dict[str, type] = {}

    @classmethod
    def source(cls, name):
        def dec(k): cls._sources[name] = k; return k
        return dec

    @classmethod
    def transform(cls, name):
        def dec(k): cls._transforms[name] = k; return k
        return dec

# Pipeline engine with Observer
@dataclass
class Pipeline:
    name: str
    source: Source
    transforms: list[Transform]
    sink: Sink
    monitors: list[Monitor] = field(default_factory=list)

    def run(self) -> dict:
        data = self.source.extract()
        self._notify("extract", 0, len(data))
        for t in self.transforms:
            n = len(data)
            data = t.apply(data)
            self._notify(type(t).__name__, n, len(data))
        loaded = self.sink.load(data)
        return {"pipeline": self.name, "loaded": loaded}

    def _notify(self, stage, in_c, out_c):
        for m in self.monitors:
            m.on_stage(stage, in_c, out_c)

# Builder from config dict
class PipelineBuilder:
    @staticmethod
    def from_config(cfg: dict) -> Pipeline:
        source = Registry._sources[cfg["source"]["type"]](**cfg["source"].get("params", {}))
        transforms = [Registry._transforms[t["type"]](**t.get("params", {})) for t in cfg.get("transforms", [])]
        sink = Registry._sinks[cfg["sink"]["type"]](**cfg["sink"].get("params", {}))
        return Pipeline(cfg["name"], source, transforms, sink)
```

**Architecture:** `Config → Builder → Registry lookup → Pipeline(Source, Transforms, Sink) → Observer notifications`

Testing: inject `InMemorySource` + `InMemorySink` — no DB needed.

</details>

</article>

---

## Interview Tips

> **Tip 1:** Talk through your reasoning aloud: "The constraint is extensibility — adding sources without modifying code. That's Factory. Swappable transforms? Strategy." Interviewers evaluate reasoning as much as answers.

> **Tip 2:** At senior level, show pattern composition. Sketch the flow: "Config → Builder → Registry → Pipeline → Observer." This demonstrates architectural vision beyond individual patterns.

> **Tip 3:** Always mention testability: "DI means I test the full pipeline with in-memory fakes in <1 second. Integration tests hit real services only in CI." This shows you value developer productivity.
