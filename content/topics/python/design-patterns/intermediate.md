---
title: "Python Design Patterns - Intermediate"
topic: python
subtopic: design-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, design-patterns, observer, decorator-pattern, template-method, pipeline]
---

# Python Design Patterns — Intermediate

## Beyond the Basics

At the mid-level, you combine patterns to solve coordination, extensibility, and composability problems in ETL systems.

---

## Pattern 1: Observer — Event-Driven Pipeline Notifications

Observer notifies multiple listeners when something happens — pipeline failures, SLA breaches, data quality alerts.

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

class EventType(Enum):
    PIPELINE_START = "pipeline_start"
    PIPELINE_COMPLETE = "pipeline_complete"
    PIPELINE_FAILURE = "pipeline_failure"

@dataclass
class PipelineEvent:
    event_type: EventType
    pipeline_name: str
    timestamp: datetime
    details: dict

class PipelineObserver(ABC):
    @abstractmethod
    def on_event(self, event: PipelineEvent) -> None:
        pass

class SlackNotifier(PipelineObserver):
    def on_event(self, event: PipelineEvent) -> None:
        if event.event_type == EventType.PIPELINE_FAILURE:
            print(f"🚨 Slack: '{event.pipeline_name}' failed: {event.details}")

class MetricsCollector(PipelineObserver):
    def on_event(self, event: PipelineEvent) -> None:
        print(f"📊 Metric: {event.event_type.value} for {event.pipeline_name}")

class PipelineOrchestrator:
    def __init__(self):
        self._observers: list[PipelineObserver] = []

    def subscribe(self, observer: PipelineObserver) -> None:
        self._observers.append(observer)

    def _notify(self, event: PipelineEvent) -> None:
        for observer in self._observers:
            observer.on_event(event)

    def run_pipeline(self, name: str, steps: list) -> None:
        self._notify(PipelineEvent(EventType.PIPELINE_START, name, datetime.now(), {}))
        try:
            for step in steps:
                step()
            self._notify(PipelineEvent(EventType.PIPELINE_COMPLETE, name, datetime.now(), {}))
        except Exception as e:
            self._notify(PipelineEvent(EventType.PIPELINE_FAILURE, name, datetime.now(), {"error": str(e)}))
```

---

## Pattern 2: Template Method — Base ETL with Overridable Steps

Defines the ETL skeleton in a base class; subclasses override specific steps.

```python
from abc import ABC, abstractmethod
import time

class BaseETLJob(ABC):
    def run(self) -> dict:
        """Fixed sequence, customizable steps."""
        start = time.time()
        raw_data = self.extract()
        transformed = self.transform(raw_data)
        row_count = self.load(transformed)
        return {"rows": row_count, "duration_sec": time.time() - start}

    @abstractmethod
    def extract(self) -> list[dict]: pass

    @abstractmethod
    def transform(self, data: list[dict]) -> list[dict]: pass

    @abstractmethod
    def load(self, data: list[dict]) -> int: pass

class UserEventsETL(BaseETLJob):
    def extract(self) -> list[dict]:
        return [{"user_id": 1, "event": "login"}, {"user_id": 2, "event": "purchase"}]

    def transform(self, data: list[dict]) -> list[dict]:
        return [{"user_id": r["user_id"], "event_type": r["event"].upper()} for r in data]

    def load(self, data: list[dict]) -> int:
        return len(data)  # Write to warehouse
```

---

## Pattern 3: Chain of Responsibility — Data Validation Pipeline

Each handler checks one rule and passes the record along if valid.

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

@dataclass
class ValidationResult:
    is_valid: bool = True
    errors: list[str] = field(default_factory=list)

class ValidationHandler(ABC):
    def __init__(self):
        self._next: "ValidationHandler | None" = None

    def set_next(self, handler: "ValidationHandler") -> "ValidationHandler":
        self._next = handler
        return handler

    def validate(self, record: dict, result: ValidationResult) -> ValidationResult:
        self._check(record, result)
        if self._next and result.is_valid:
            return self._next.validate(record, result)
        return result

    @abstractmethod
    def _check(self, record: dict, result: ValidationResult) -> None: pass

class NullCheckHandler(ValidationHandler):
    def __init__(self, required_fields: list[str]):
        super().__init__()
        self.required_fields = required_fields

    def _check(self, record: dict, result: ValidationResult) -> None:
        for f in self.required_fields:
            if record.get(f) is None:
                result.is_valid = False
                result.errors.append(f"Missing: {f}")

class RangeCheckHandler(ValidationHandler):
    def __init__(self, ranges: dict[str, tuple]):
        super().__init__()
        self.ranges = ranges

    def _check(self, record: dict, result: ValidationResult) -> None:
        for f, (lo, hi) in self.ranges.items():
            val = record.get(f)
            if val is not None and not (lo <= val <= hi):
                result.is_valid = False
                result.errors.append(f"{f}={val} not in [{lo},{hi}]")

# Build chain
null_check = NullCheckHandler(["user_id", "amount"])
range_check = RangeCheckHandler({"amount": (0, 100000)})
null_check.set_next(range_check)

result = null_check.validate({"user_id": 1, "amount": 55.99}, ValidationResult())
```

---

## Pattern 4: Registry — Plugin System for Data Sources

```python
from typing import Type

class SourceRegistry:
    _registry: dict[str, Type] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(source_class: Type) -> Type:
            cls._registry[name] = source_class
            return source_class
        return decorator

    @classmethod
    def get(cls, name: str, **kwargs):
        if name not in cls._registry:
            raise ValueError(f"Unknown: {name}. Available: {list(cls._registry.keys())}")
        return cls._registry[name](**kwargs)

@SourceRegistry.register("postgres")
class PostgresSource:
    def __init__(self, connection_string: str):
        self.conn_str = connection_string

@SourceRegistry.register("kafka")
class KafkaSource:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.servers = bootstrap_servers
        self.topic = topic

source = SourceRegistry.get("kafka", bootstrap_servers="localhost:9092", topic="events")
```

---

## Pattern 5: Pipeline Pattern — Composable Stages

```python
from typing import Callable

class TransformPipeline:
    def __init__(self):
        self._stages: list[tuple[str, Callable]] = []

    def add_stage(self, name: str, func: Callable) -> "TransformPipeline":
        self._stages.append((name, func))
        return self

    def run(self, data: list[dict]) -> list[dict]:
        for name, func in self._stages:
            data = func(data)
        return data

pipeline = (
    TransformPipeline()
    .add_stage("dedup", lambda recs: list({r["id"]: r for r in recs}.values()))
    .add_stage("filter", lambda recs: [r for r in recs if r.get("amount", 0) > 0])
)
results = pipeline.run([{"id": 1, "amount": 100}, {"id": 1, "amount": 100}, {"id": 2, "amount": -5}])
```

---

## Interview Tips

> **Tip 1:** For Observer, explain how it decouples monitoring from pipeline logic. You can add Slack, PagerDuty, or DataDog observers without changing orchestrator code. This is how production alerting works.

> **Tip 2:** Template Method is everywhere in DE frameworks. Airflow operators, Spark jobs, and dbt models all use it — the framework defines structure, you fill in specifics. Show you understand "inversion of control."

> **Tip 3:** Chain of Responsibility maps to data quality gates. Each validation is independent, testable, and reorderable. Compare it to "fail fast" — stop on critical errors, continue on warnings.
