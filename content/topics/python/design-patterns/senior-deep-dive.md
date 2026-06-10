---
title: "Python Design Patterns - Senior Deep Dive"
topic: python
subtopic: design-patterns
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, design-patterns, advanced, architecture, dependency-injection]
---

# Python Design Patterns — Senior Deep Dive

## Architectural Patterns for Production Pipelines

Senior-level pattern usage is about knowing which patterns compose well, when to deviate, and recognizing anti-patterns in legacy code.

---

## Dependency Injection — Testable Pipelines

DI passes dependencies from outside rather than creating them internally. Makes pipelines testable without real databases.

```python
from abc import ABC, abstractmethod

class DataRepository(ABC):
    @abstractmethod
    def read(self, query: str) -> list[dict]: pass
    @abstractmethod
    def write(self, table: str, records: list[dict]) -> int: pass

class PostgresRepository(DataRepository):
    def __init__(self, conn_str: str):
        self.conn_str = conn_str
    def read(self, query: str) -> list[dict]:
        return []  # Real DB call
    def write(self, table: str, records: list[dict]) -> int:
        return len(records)

class InMemoryRepository(DataRepository):
    """Test double — no external dependencies."""
    def __init__(self):
        self.stored: dict[str, list[dict]] = {}
    def read(self, query: str) -> list[dict]:
        return self.stored.get("default", [])
    def write(self, table: str, records: list[dict]) -> int:
        self.stored[table] = records
        return len(records)

class ETLPipeline:
    def __init__(self, source: DataRepository, sink: DataRepository):
        self._source = source
        self._sink = sink

    def run(self, query: str, table: str) -> int:
        raw = self._source.read(query)
        transformed = [{k: v for k, v in r.items() if v is not None} for r in raw]
        return self._sink.write(table, transformed)

# Test without real DB
def test_pipeline():
    source = InMemoryRepository()
    source.stored["default"] = [{"name": "Alice", "age": None, "score": 95}]
    sink = InMemoryRepository()
    ETLPipeline(source=source, sink=sink).run("SELECT *", "out")
    assert sink.stored["out"] == [{"name": "Alice", "score": 95}]
```

---

## Circuit Breaker — Resilient External Service Calls

Prevents cascading failures. After repeated failures, it "opens" and fails fast.

```python
import time
from enum import Enum
from dataclasses import dataclass, field

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failures: int = field(default=0, init=False)
    _last_failure: float = field(default=0.0, init=False)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def call(self, func, *args, **kwargs):
        if self.state == CircuitState.OPEN:
            raise RuntimeError("Circuit OPEN — failing fast")
        try:
            result = func(*args, **kwargs)
            self._failures = 0
            self._state = CircuitState.CLOSED
            return result
        except Exception:
            self._failures += 1
            self._last_failure = time.time()
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
            raise

breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
# Usage: breaker.call(requests.get, "https://api.example.com/data")
```

---

## Event Sourcing — Immutable Pipeline State

Store the sequence of events, not current state. Gives full auditability and replay capability.

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

@dataclass
class Event:
    event_type: str
    payload: dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)

class PipelineEventStore:
    def __init__(self):
        self._events: list[Event] = []

    def append(self, event: Event) -> None:
        self._events.append(event)

    def replay(self) -> dict:
        state = {"status": "idle", "rows_processed": 0, "errors": []}
        for event in self._events:
            if event.event_type == "started":
                state["status"] = "running"
            elif event.event_type == "batch_processed":
                state["rows_processed"] += event.payload["count"]
            elif event.event_type == "completed":
                state["status"] = "completed"
        return state

store = PipelineEventStore()
store.append(Event("started", {"pipeline": "user_events"}))
store.append(Event("batch_processed", {"count": 1000}))
store.append(Event("batch_processed", {"count": 850}))
store.append(Event("completed", {}))
print(store.replay())  # {"status": "completed", "rows_processed": 1850, ...}
```

---

## Command Pattern — Idempotent Operations

Encapsulates a request as an object, enabling deduplication and safe retries.

```python
from abc import ABC, abstractmethod
from hashlib import sha256

class PipelineCommand(ABC):
    @abstractmethod
    def execute(self) -> dict: pass
    @abstractmethod
    def idempotency_key(self) -> str: pass

class LoadPartitionCommand(PipelineCommand):
    def __init__(self, table: str, partition_date: str, data_path: str):
        self.table = table
        self.partition_date = partition_date
        self.data_path = data_path

    def execute(self) -> dict:
        return {"action": "overwrite_partition", "table": self.table, "partition": self.partition_date}

    def idempotency_key(self) -> str:
        raw = f"{self.table}:{self.partition_date}:{self.data_path}"
        return sha256(raw.encode()).hexdigest()[:16]

class CommandExecutor:
    def __init__(self):
        self._executed: set[str] = set()

    def execute(self, cmd: PipelineCommand) -> dict | None:
        key = cmd.idempotency_key()
        if key in self._executed:
            return None  # Skip duplicate
        result = cmd.execute()
        self._executed.add(key)
        return result
```

---

## Anti-Patterns in Data Engineering Code

| Anti-Pattern | Problem | Better Approach |
|-------------|---------|-----------------|
| God Class | One class does E, T, L, alert | Single responsibility |
| Hardcoded Config | Conn strings in source | Inject config / env vars |
| Silent Failures | `except: pass` | Explicit errors, dead-letter queues |
| Circular Dependencies | Module A ↔ B | Dependency inversion |
| Premature Abstraction | Abstract before 2nd use | Wait for duplication |

---

## Interview Tips

> **Tip 1:** DI is the senior answer to "how do you test pipelines?" Show you can test transformation logic without real databases by injecting mock repositories. Mention this enables fast local development too.

> **Tip 2:** Circuit Breaker shows production maturity. Explain the three states (closed/open/half-open) and how it prevents one failing API from crashing your entire pipeline. Reference `tenacity` or `pybreaker` for real implementations.

> **Tip 3:** Know when NOT to use patterns. If asked "would you use Event Sourcing here?", it's valid to say "only if we need audit history and replay — otherwise it's over-engineering." Senior engineers know the cost of abstraction.

## ⚡ Cheat Sheet

**Pattern Selection Guide**
| Pattern | When to Use | DE Example |
|---------|-------------|------------|
| Dependency Injection | Need testability without real infra | Inject `InMemoryRepository` in tests |
| Circuit Breaker | External service calls that can fail | DB writes, enrichment API |
| Event Sourcing | Need full audit trail + replay | Pipeline state history |
| Command + Idempotency | Safe retries, deduplication | `LoadPartitionCommand` with SHA hash key |
| Strategy | Swappable algorithms | `HashPartition` vs `RangePartition` |
| Observer/Event Bus | Decoupled notifications | Slack alert, dashboard update on pipeline end |

**Dependency Injection Rules**
- Define abstract base class (`DataRepository(ABC)`) — interface contract
- Real implementation (`PostgresRepository`) and test double (`InMemoryRepository`)
- Inject via constructor: `ETLPipeline(source=..., sink=...)` — never create deps internally
- Test double in tests: no DB required, tests run in milliseconds

**Circuit Breaker Numbers**
- `failure_threshold=5, recovery_timeout=30` for APIs; `failure_threshold=3, recovery_timeout=60` for DB
- Three states: CLOSED → (N failures) → OPEN → (timeout) → HALF_OPEN → (1 success) → CLOSED
- Use `threading.Lock` on state updates for thread safety

**Anti-Patterns to Name in Interviews**
- **God Class**: one class does E+T+L+alerts → single responsibility instead
- **Hardcoded Config**: conn strings in code → inject via env vars / Pydantic BaseSettings
- **Silent Failures**: `except: pass` → explicit error + DLQ
- **Premature Abstraction**: abstract before 2nd concrete use → wait for duplication

**Event Sourcing Trade-offs**
- Pros: full audit trail, replay from any point, time-travel debugging
- Cons: complex queries (must replay events), storage grows unboundedly, learning curve
- Use only if: audit is a hard requirement OR replay/undo is a core feature
- For most pipelines: structured logging + DQ gate is sufficient — don't over-engineer

**Command Pattern Key Properties**
- `idempotency_key()` = hash of (table + partition + source path) → same command = same key
- `CommandExecutor._executed: set[str]` prevents duplicate execution
- Safe to retry: running the same command twice is a no-op
