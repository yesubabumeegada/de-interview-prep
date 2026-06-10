---
title: "Python Multithreading & Multiprocessing - Senior Deep Dive"
topic: python
subtopic: multithreading-multiprocessing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, multithreading, multiprocessing, performance, producer-consumer]
---

# Python Multithreading & Multiprocessing — Senior Deep Dive

## Production-Grade Concurrent Systems

Senior-level concurrency means handling backpressure, preventing deadlocks, and designing systems that degrade gracefully.

---

## Producer-Consumer with Bounded Queues

The classic architecture for decoupling extraction speed from transformation speed.

```python
import threading
import time
from queue import Queue, Empty

SENTINEL = object()

class ProducerConsumerPipeline:
    """Multi-stage pipeline with backpressure via bounded queue."""

    def __init__(self, queue_size: int = 1000, num_consumers: int = 4):
        self._queue: Queue = Queue(maxsize=queue_size)
        self._num_consumers = num_consumers
        self._produced = 0
        self._consumed = 0

    def produce(self, source_fn, **kwargs) -> None:
        for record in source_fn(**kwargs):
            self._queue.put(record)  # Blocks when full (backpressure)
            self._produced += 1
        for _ in range(self._num_consumers):
            self._queue.put(SENTINEL)

    def consume(self, transform_fn) -> list[dict]:
        results = []
        while True:
            item = self._queue.get(timeout=10.0)
            if item is SENTINEL:
                break
            results.append(transform_fn(item))
            self._consumed += 1
        return results

    def run(self, source_fn, transform_fn, **kwargs) -> dict:
        all_results = []
        lock = threading.Lock()

        def consumer():
            res = self.consume(transform_fn)
            with lock:
                all_results.extend(res)

        consumers = [threading.Thread(target=consumer) for _ in range(self._num_consumers)]
        for c in consumers: c.start()
        self.produce(source_fn, **kwargs)
        for c in consumers: c.join()
        return {"produced": self._produced, "consumed": self._consumed}
```

---

## Backpressure Handling

Without backpressure, fast producers overflow memory. Bounded queues with monitoring solve this.

```python
from queue import Queue, Full
import logging

logger = logging.getLogger(__name__)

class BackpressureQueue:
    def __init__(self, maxsize: int = 1000, high_watermark: float = 0.8):
        self._queue = Queue(maxsize=maxsize)
        self._maxsize = maxsize
        self._high_watermark = int(maxsize * high_watermark)
        self._pressure_events = 0

    def put(self, item, timeout: float = 30.0) -> bool:
        if self._queue.qsize() >= self._high_watermark:
            self._pressure_events += 1
            logger.warning(f"Backpressure: queue at {self._queue.qsize()}/{self._maxsize}")
        try:
            self._queue.put(item, timeout=timeout)
            return True
        except Full:
            return False

    def get(self, timeout: float = 5.0):
        return self._queue.get(timeout=timeout)
```

---

## Memory-Mapped Files for IPC

When processes share large datasets, mmap avoids expensive serialization.

```python
import mmap
import struct

def write_shared_data(filepath: str, data: list[float]):
    """Write floats to memory-mapped file — no pickle overhead."""
    size = 4 + 8 * len(data)  # 4-byte header + 8 bytes per float64
    with open(filepath, "wb") as f:
        f.write(b"\x00" * size)
    with open(filepath, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)
        struct.pack_into("I", mm, 0, len(data))
        for i, val in enumerate(data):
            struct.pack_into("d", mm, 4 + i * 8, val)
        mm.close()

def read_shared_data(filepath: str) -> list[float]:
    with open(filepath, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        count = struct.unpack_from("I", mm, 0)[0]
        data = [struct.unpack_from("d", mm, 4 + i * 8)[0] for i in range(count)]
        mm.close()
    return data
```

---

## Deadlock Prevention

```python
import threading
from contextlib import contextmanager

class DeadlockFreeManager:
    """Prevents deadlocks via consistent lock ordering + timeouts."""

    def __init__(self):
        self._locks: dict[str, threading.Lock] = {}
        self._order: dict[str, int] = {}

    def register(self, name: str, priority: int):
        self._locks[name] = threading.Lock()
        self._order[name] = priority

    @contextmanager
    def acquire_all(self, *names: str, timeout: float = 10.0):
        """Acquire locks in priority order — prevents circular waits."""
        ordered = sorted(names, key=lambda n: self._order[n])
        acquired = []
        try:
            for name in ordered:
                if not self._locks[name].acquire(timeout=timeout):
                    raise TimeoutError(f"Deadlock timeout: '{name}'")
                acquired.append(name)
            yield
        finally:
            for name in reversed(acquired):
                self._locks[name].release()
```

**Deadlock prevention rules:**
1. Acquire locks in consistent global order
2. Always use timeouts on acquisitions
3. Prefer lock-free designs (queues, immutable data)
4. Minimize lock hold duration

---

## Decision Tree: asyncio vs threading vs multiprocessing

| Criterion | asyncio | threading | multiprocessing |
|-----------|---------|-----------|-----------------|
| Work type | I/O-bound | I/O-bound | CPU-bound |
| Concurrency level | 1000s+ | 10s-100s | CPU cores |
| Memory per task | ~1 KB | ~8 MB | ~50+ MB |
| Shared state | Easy (single thread) | Needs locks | Needs IPC |
| CPU utilization | Single core | Single core | Multi-core |
| Best DE use | API extraction at scale | Moderate I/O | Heavy transforms |

---

## Performance Profiling

```python
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def benchmark(work_fn, data, workers: int = 4) -> dict:
    """Compare sequential vs threaded vs multiprocess."""
    start = time.perf_counter()
    [work_fn(x) for x in data]
    seq = time.perf_counter() - start

    start = time.perf_counter()
    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(work_fn, data))
    threaded = time.perf_counter() - start

    start = time.perf_counter()
    with ProcessPoolExecutor(workers) as ex:
        list(ex.map(work_fn, data))
    process = time.perf_counter() - start

    return {"sequential": seq, "threaded": threaded, "multiprocess": process,
            "thread_speedup": seq/threaded, "process_speedup": seq/process}
```

---

## Interview Tips

> **Tip 1:** Producer-consumer is the senior answer to "how would you build streaming ETL in Python?" Explain bounded queues, poison pills for shutdown, and backpressure. Then note in production you'd use Kafka for cross-service communication.

> **Tip 2:** For deadlock questions, explain the four conditions (mutual exclusion, hold-and-wait, no preemption, circular wait) and your prevention: "I enforce global lock ordering and always use timeouts. Better yet, I design lock-free systems using queues."

> **Tip 3:** Don't just say "use cProfile." Explain that cProfile doesn't capture thread wait time well. Instead measure wall-clock time, calculate speedup ratio, and verify you're getting actual parallelism, not just concurrency.

## ⚡ Cheat Sheet

**Concurrency Model Decision Table**
| Criterion | asyncio | threading | multiprocessing |
|-----------|---------|-----------|-----------------|
| Work type | I/O-bound | I/O-bound | CPU-bound |
| Tasks | 1000s | 10s–100s | CPU core count |
| Memory/task | ~1 KB | ~8 MB | ~50+ MB |
| Shared state | Easy (single thread) | Needs locks | Needs IPC/queues |
| CPU cores used | 1 | 1 (GIL) | All |

**Producer-Consumer Rules**
- `Queue(maxsize=N)` — producer blocks on `put()` when full = automatic backpressure
- Poison pill: put `SENTINEL` once per consumer thread to signal shutdown
- `queue.get(timeout=10.0)` — always use timeout to detect stalled producers
- Result aggregation: use `threading.Lock()` when appending to shared list

**Deadlock Prevention**
- Four conditions: mutual exclusion + hold-and-wait + no preemption + circular wait
- Prevention: acquire locks in consistent global priority order — breaks circular wait
- Always use `acquire(timeout=T)` — raises `TimeoutError` instead of hanging forever
- Prefer lock-free design: queues + immutable data; minimize lock scope duration

**Memory-Mapped IPC**
- `mmap` shares data between processes without pickle overhead
- `struct.pack_into` / `unpack_from` — binary layout for numeric arrays
- Use for: large float arrays, sorted lookup tables, shared configuration
- `ACCESS_READ` for read-only consumers; `ACCESS_WRITE` for shared write (add lock)

**BackpressureQueue Pattern**
- `high_watermark = 0.8 * maxsize` — log warning when queue reaches 80% full
- Track `_pressure_events` count — metric for consumer-is-slow diagnosis
- `put(item, timeout=30.0)` — return `False` instead of raising on timeout in non-critical paths

**Benchmarking Concurrency**
- If `process_time < single_time / 2`: CPU-bound → use multiprocessing
- If `thread_time < single_time / 2`: I/O-bound → use threads or async
- Neither improves: profile further with `py-spy` — likely a bottleneck inside a C extension
