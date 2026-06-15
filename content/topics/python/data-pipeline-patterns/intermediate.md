---
title: "Data Pipeline Patterns - Intermediate"
topic: python
subtopic: data-pipeline-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, pipeline, circuit-breaker, parallel, producer-consumer, metrics, dead-letter-queue]
---

# Data Pipeline Patterns — Intermediate

## Circuit Breaker Pattern for External API Calls

A **circuit breaker** stops hammering a failing service — after N consecutive failures it "opens" (fast-fails all calls) and periodically tries again. Without it, a broken API causes your pipeline to queue thousands of retried connections.

```python
import time
from enum import Enum
from dataclasses import dataclass, field
from threading import Lock


class CircuitState(Enum):
    CLOSED = "closed"       # Normal: calls pass through
    OPEN = "open"           # Failing: all calls fast-fail
    HALF_OPEN = "half_open" # Testing: one probe call allowed


@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5       # Open after this many consecutive failures
    recovery_timeout: float = 60.0   # Seconds before trying HALF_OPEN
    success_threshold: int = 2       # Successes needed to close from HALF_OPEN

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _lock: Lock = field(default_factory=Lock, init=False)

    def call(self, func, *args, **kwargs):
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                else:
                    raise RuntimeError(f"Circuit {self.name} is OPEN — fast-failing")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if (self._state == CircuitState.HALF_OPEN or
                    self._failure_count >= self.failure_threshold):
                self._state = CircuitState.OPEN


# Usage
breaker = CircuitBreaker("payments-api", failure_threshold=5, recovery_timeout=30)

def fetch_payment(payment_id: str) -> dict:
    import requests
    return requests.get(f"https://payments-api/v1/{payment_id}").json()

for payment_id in payment_ids:
    try:
        data = breaker.call(fetch_payment, payment_id)
    except RuntimeError:
        # Circuit open — send to dead letter queue instead of crashing pipeline
        dlq.put({"id": payment_id, "reason": "circuit_open"})
```

---

## Pipeline Orchestration Without Airflow: Topological Sort

For internal pipelines where Airflow is overkill, you can implement dependency ordering yourself using a topological sort.

```python
from collections import defaultdict, deque


class PipelineGraph:
    """Simple DAG orchestrator using Kahn's topological sort."""

    def __init__(self):
        self.tasks: dict[str, callable] = {}
        self.dependencies: dict[str, set[str]] = defaultdict(set)

    def task(self, name: str, depends_on: list[str] = None):
        """Decorator to register a task with its dependencies."""
        def decorator(func):
            self.tasks[name] = func
            if depends_on:
                self.dependencies[name] = set(depends_on)
            return func
        return decorator

    def execution_order(self) -> list[str]:
        """Return tasks in topological order (Kahn's algorithm)."""
        in_degree = {name: 0 for name in self.tasks}
        for name, deps in self.dependencies.items():
            in_degree[name] = len(deps)

        queue = deque(name for name, deg in in_degree.items() if deg == 0)
        order = []

        while queue:
            task = queue.popleft()
            order.append(task)
            for dependent, deps in self.dependencies.items():
                if task in deps:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        if len(order) != len(self.tasks):
            raise ValueError("Cycle detected in pipeline DAG")
        return order

    def run(self) -> dict:
        results = {}
        for task_name in self.execution_order():
            print(f"Running task: {task_name}")
            results[task_name] = self.tasks[task_name]()
        return results


# Define a pipeline as a DAG
pipeline = PipelineGraph()

@pipeline.task("extract_orders")
def extract_orders():
    return fetch_orders_from_api()

@pipeline.task("extract_customers")
def extract_customers():
    return fetch_customers_from_db()

@pipeline.task("join_and_transform", depends_on=["extract_orders", "extract_customers"])
def join_and_transform():
    return transform_data()

@pipeline.task("load_to_warehouse", depends_on=["join_and_transform"])
def load():
    return write_to_bigquery()

pipeline.run()
# Execution order: extract_orders, extract_customers, join_and_transform, load_to_warehouse
```

---

## Parallel Extraction: ThreadPoolExecutor vs ProcessPoolExecutor

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import requests


# I/O-BOUND: Use ThreadPoolExecutor (threads release GIL during I/O)
# Example: fetching data from 10 REST APIs in parallel
def fetch_endpoint(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

api_endpoints = [
    "https://api.example.com/orders/2024-01-01",
    "https://api.example.com/orders/2024-01-02",
    # ... 8 more dates
]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_endpoint, url): url for url in api_endpoints}
    results = []
    for future in as_completed(futures):
        url = futures[future]
        try:
            results.append(future.result())
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")

# CPU-BOUND: Use ProcessPoolExecutor (bypasses GIL with separate processes)
# Example: parsing and transforming large JSON payloads (CPU-intensive)
import json
import multiprocessing

def parse_and_transform(raw_json: str) -> list[dict]:
    """CPU-bound: JSON parsing + complex business logic."""
    data = json.loads(raw_json)
    return [{"id": r["id"], "total": sum(r["line_items"])} for r in data]

raw_payloads = [...]  # Large JSON strings
num_workers = multiprocessing.cpu_count() - 1  # Leave one CPU for the OS

with ProcessPoolExecutor(max_workers=num_workers) as executor:
    transformed = list(executor.map(parse_and_transform, raw_payloads, chunksize=10))

# GIL RULE:
# - Threads: fine for network I/O, DB queries, file reads (GIL is released during I/O)
# - Processes: required for CPU work like parsing, encryption, regex on large data
```

---

## Producer-Consumer Pattern with queue.Queue

For streaming-style pipelines where extraction and loading run at different speeds:

```python
import queue
import threading
from typing import Iterator


SENTINEL = object()  # Signal to consumers that production is complete


def run_producer_consumer(
    producer_fn: callable,
    consumer_fn: callable,
    num_consumers: int = 4,
    queue_size: int = 1000,  # Bounded queue provides back-pressure
) -> list:
    """
    Producer fills the queue; N consumers drain it concurrently.
    Bounded queue prevents memory explosion if producer >> consumer speed.
    """
    q: queue.Queue = queue.Queue(maxsize=queue_size)
    results = []
    errors = []
    results_lock = threading.Lock()

    def producer():
        try:
            for item in producer_fn():
                q.put(item)  # Blocks if queue is full (back-pressure)
        finally:
            # Put one SENTINEL per consumer thread
            for _ in range(num_consumers):
                q.put(SENTINEL)

    def consumer():
        while True:
            item = q.get()
            if item is SENTINEL:
                q.task_done()
                break
            try:
                result = consumer_fn(item)
                with results_lock:
                    results.append(result)
            except Exception as e:
                with results_lock:
                    errors.append({"item": item, "error": str(e)})
            finally:
                q.task_done()

    threads = [threading.Thread(target=producer)]
    threads += [threading.Thread(target=consumer) for _ in range(num_consumers)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        print(f"Warning: {len(errors)} items failed processing")
    return results
```

---

## Pipeline Metrics Collection

```python
import time
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class PipelineMetrics:
    pipeline_name: str
    run_id: str
    _start_time: float = field(default_factory=time.time, init=False)
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    rows_dropped: int = 0
    rows_failed: int = 0
    _stage_times: dict = field(default_factory=lambda: defaultdict(float), init=False)

    def record_stage_duration(self, stage: str, duration_seconds: float):
        self._stage_times[stage] += duration_seconds

    @property
    def error_rate(self) -> float:
        total = self.rows_extracted
        return self.rows_failed / total if total else 0.0

    @property
    def total_duration(self) -> float:
        return time.time() - self._start_time

    def summary(self) -> dict:
        return {
            "pipeline": self.pipeline_name,
            "run_id": self.run_id,
            "duration_seconds": round(self.total_duration, 2),
            "rows_extracted": self.rows_extracted,
            "rows_loaded": self.rows_loaded,
            "rows_dropped": self.rows_dropped,
            "error_rate": round(self.error_rate, 4),
            "stage_times": dict(self._stage_times),
        }
```

---

## Dead Letter Queue for Failed Records

```python
import json
from pathlib import Path
from datetime import datetime


class DeadLetterQueue:
    """Stores records that failed processing for later inspection/replay."""

    def __init__(self, dlq_path: str, pipeline_name: str):
        self.path = Path(dlq_path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.pipeline_name = pipeline_name

    def send(self, record: dict, error: str, run_id: str):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "pipeline": self.pipeline_name,
            "run_id": run_id,
            "error": error,
            "record": record,
        }
        dlq_file = self.path / f"{self.pipeline_name}_dlq.jsonl"
        with open(dlq_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def replay(self, processor: callable) -> int:
        """Re-process all DLQ records. Returns count of successful replays."""
        dlq_file = self.path / f"{self.pipeline_name}_dlq.jsonl"
        if not dlq_file.exists():
            return 0
        success_count = 0
        remaining = []
        with open(dlq_file) as f:
            for line in f:
                entry = json.loads(line)
                try:
                    processor(entry["record"])
                    success_count += 1
                except Exception:
                    remaining.append(line)  # Still failing — keep in DLQ

        dlq_file.write_text("".join(remaining))
        return success_count
```

---

## Interview Tips

- Explain the **GIL tradeoff** clearly: "For I/O-bound extraction I use `ThreadPoolExecutor`; for CPU-bound transformation I need `ProcessPoolExecutor` because the GIL prevents true parallelism in threads."
- The **circuit breaker** is a common system design question component — knowing its three states (CLOSED/OPEN/HALF_OPEN) signals production experience.
- **Bounded queues** are the key to preventing OOM in producer-consumer: if the producer is faster than the consumer, an unbounded queue will grow until the process crashes.
- Always mention the dead letter queue when discussing error handling — "what do you do with records that can't be processed?" is a standard follow-up.
