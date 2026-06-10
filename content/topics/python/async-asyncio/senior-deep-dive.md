---
title: "Async / asyncio — Senior Deep Dive"
topic: python
subtopic: async-asyncio
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, asyncio, aiokafka, asyncpg, backpressure, pipeline-orchestration, when-not-to-async]
---

# Async / asyncio — Senior Deep Dive

At the senior level, async programming means building resilient async pipelines: async Kafka consumers, async database writers with connection pooling, backpressure patterns to prevent memory overload, and knowing exactly when async is the wrong tool.

---

## Async Kafka Consumer with aiokafka

Async Kafka consumers allow you to process messages concurrently while maintaining offset commits correctly.

```python
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
import json
import logging

logger = logging.getLogger(__name__)


class AsyncKafkaIngestionPipeline:
    """
    Async Kafka consumer pipeline with:
    - Concurrent message processing
    - Backpressure via bounded queue
    - Graceful shutdown
    - Manual offset commit after successful processing
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        target_table: str,
        max_concurrent: int = 50,
        batch_size: int = 100,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.target_table = target_table
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        self._consumer = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_concurrent * 2)  # Backpressure
        self._shutdown = asyncio.Event()

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,  # Manual commit for exactly-once semantics
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            max_poll_records=self.batch_size,
        )
        await self._consumer.start()
        logger.info(f"Consumer started for topic: {self.topic}")

    async def stop(self):
        self._shutdown.set()
        if self._consumer:
            await self._consumer.stop()

    async def process_message(self, message: dict, db_pool) -> bool:
        """Process a single message. Returns True on success."""
        async with db_pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO events (user_id, event_type, payload, created_at) "
                    "VALUES ($1, $2, $3, NOW()) ON CONFLICT (event_id) DO NOTHING",
                    message.get("user_id"),
                    message.get("event_type"),
                    json.dumps(message),
                )
                return True
            except Exception as e:
                logger.error(f"DB write failed for message: {e}")
                return False

    async def run(self, db_pool):
        """Main consumer loop with concurrent processing."""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        pending_tasks = set()

        async def process_with_semaphore(msg):
            async with semaphore:
                return await self.process_message(msg.value, db_pool)

        try:
            async for msg in self._consumer:
                if self._shutdown.is_set():
                    break

                # Create task for concurrent processing
                task = asyncio.create_task(process_with_semaphore(msg))
                pending_tasks.add(task)
                task.add_done_callback(pending_tasks.discard)

                # Batch commit when we have enough completed tasks
                if len(pending_tasks) >= self.batch_size:
                    done, _ = await asyncio.wait(
                        pending_tasks, return_when=asyncio.ALL_COMPLETED
                    )
                    success = all(not t.exception() for t in done)
                    if success:
                        await self._consumer.commit()  # Commit only after successful writes
                    pending_tasks = set()

        finally:
            # Wait for all in-flight tasks
            if pending_tasks:
                await asyncio.wait(pending_tasks, return_when=asyncio.ALL_COMPLETED)
            await self.stop()
```

---

## Async Database Writes with asyncpg

```python
import asyncio
import asyncpg
from typing import Any
import time


class AsyncBatchWriter:
    """
    Async batch writer with connection pooling, retry logic,
    and write-ahead-log (WAL) pattern for reliability.
    """

    def __init__(self, dsn: str, pool_size: int = 20, batch_size: int = 500):
        self.dsn = dsn
        self.pool_size = pool_size
        self.batch_size = batch_size
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self):
        self._pool = await asyncpg.create_pool(
            self.dsn,
            min_size=5,
            max_size=self.pool_size,
            command_timeout=60,
        )
        return self

    async def __aexit__(self, *args):
        if self._pool:
            await self._pool.close()

    async def write_batch(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple],
        conflict_action: str = "DO NOTHING"
    ) -> int:
        """Write a batch of rows with upsert semantics."""
        cols = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        conflict_cols = columns[:1]  # Assume first column is the PK

        sql = f"""
            INSERT INTO {table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols[0]}) {conflict_action}
        """

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.executemany(sql, rows)
                return len(rows)

    async def parallel_write_all(
        self,
        table: str,
        columns: list[str],
        all_rows: list[tuple],
    ) -> int:
        """Split into batches and write all concurrently."""
        batches = [
            all_rows[i:i + self.batch_size]
            for i in range(0, len(all_rows), self.batch_size)
        ]

        tasks = [self.write_batch(table, columns, batch) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total = sum(r for r in results if isinstance(r, int))
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logger.error(f"{len(errors)}/{len(batches)} batches failed: {errors[0]}")

        return total


# Usage:
async def ingest_to_postgres(records: list[dict]):
    async with AsyncBatchWriter("postgresql://user:pass@host/db", pool_size=20) as writer:
        rows = [(r["event_id"], r["user_id"], r["amount"]) for r in records]
        written = await writer.parallel_write_all(
            "events",
            ["event_id", "user_id", "amount"],
            rows
        )
        print(f"Wrote {written} records")
```

---

## Backpressure Patterns

Without backpressure, a fast producer + slow consumer = unbounded memory growth and eventual OOM.

```python
import asyncio
from collections import deque


class BoundedAsyncPipeline:
    """
    Producer-consumer pipeline with bounded queue for backpressure.
    If the consumer can't keep up, the producer blocks until the queue drains.
    """

    def __init__(self, queue_size: int = 1000):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        # maxsize > 0 means put() will BLOCK when queue is full → backpressure

    async def producer(self, source_api: str, pages: int):
        """Fetch pages from API and put into queue."""
        async with aiohttp.ClientSession() as session:
            for page in range(pages):
                data = await fetch_page(session, source_api, page)
                await self.queue.put(data)  # BLOCKS if queue is full → controls producer speed
                print(f"Produced page {page}, queue size: {self.queue.qsize()}")

        # Signal completion
        await self.queue.put(None)

    async def consumer(self, db_writer):
        """Read from queue and write to database."""
        while True:
            item = await self.queue.get()
            if item is None:  # Sentinel value → producer is done
                self.queue.task_done()
                break

            try:
                await db_writer.write_batch("events", item)
            except Exception as e:
                logger.error(f"Consumer write failed: {e}")
            finally:
                self.queue.task_done()

    async def run(self, source_api: str, pages: int, db_writer):
        """Run producer and consumer concurrently."""
        await asyncio.gather(
            self.producer(source_api, pages),
            self.consumer(db_writer),
        )
        await self.queue.join()  # Wait until all items are processed


# With multiple consumers for higher throughput:
async def run_with_multiple_consumers(source_api: str, pages: int, db_writer):
    pipeline = BoundedAsyncPipeline(queue_size=500)
    n_consumers = 5

    await asyncio.gather(
        pipeline.producer(source_api, pages),
        *[pipeline.consumer(db_writer) for _ in range(n_consumers)]
    )
```

---

## Async Pipeline Orchestration

```python
import asyncio
from dataclasses import dataclass
from typing import Callable, Awaitable


@dataclass
class PipelineStage:
    name: str
    coroutine: Callable
    concurrency: int = 10


async def run_pipeline_stages(
    records: list[dict],
    stages: list[PipelineStage],
) -> list[dict]:
    """
    Run a multi-stage async pipeline.
    Each stage processes all records concurrently before the next stage begins.
    """
    current_batch = records

    for stage in stages:
        print(f"Running stage: {stage.name} on {len(current_batch)} records")
        semaphore = asyncio.Semaphore(stage.concurrency)

        async def run_with_sem(record, sem=semaphore, coro=stage.coroutine):
            async with sem:
                return await coro(record)

        results = await asyncio.gather(
            *[run_with_sem(record) for record in current_batch],
            return_exceptions=True
        )

        # Filter out failures before next stage
        successes = [r for r in results if not isinstance(r, Exception)]
        failures  = [r for r in results if isinstance(r, Exception)]

        if failures:
            print(f"  Stage {stage.name}: {len(failures)} failures")

        current_batch = successes
        print(f"  Stage {stage.name}: {len(successes)}/{len(records)} succeeded")

    return current_batch


# Example pipeline:
async def validate(record: dict) -> dict:
    await asyncio.sleep(0.01)  # Async validation call
    if not record.get("user_id"):
        raise ValueError(f"Missing user_id: {record}")
    return record


async def enrich(record: dict) -> dict:
    await asyncio.sleep(0.02)  # Async API call
    record["enriched"] = True
    return record


async def write(record: dict) -> dict:
    await asyncio.sleep(0.015)  # Async DB write
    record["written"] = True
    return record


pipeline = [
    PipelineStage("validate", validate, concurrency=100),
    PipelineStage("enrich",   enrich,   concurrency=20),   # Limited by external API
    PipelineStage("write",    write,    concurrency=50),
]

records = [{"user_id": i, "amount": i * 10} for i in range(1000)]
result = asyncio.run(run_pipeline_stages(records, pipeline))
```

---

## When NOT to Use Async (CPU-Bound Work)

This is a senior-level trap. Many engineers over-apply async to CPU-bound work expecting speedups.

```python
import asyncio
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor


# ── CPU-bound task: JSON parsing, data transformation ─────────────────────

def parse_and_transform(raw_data: str) -> dict:
    """CPU-bound: JSON parsing + computation."""
    import json
    data = json.loads(raw_data)  # CPU work
    # Heavy transformation
    data["computed"] = sum(range(100_000))
    return data


# WRONG: Async doesn't help for CPU work
async def wrong_approach(raw_data_list: list[str]) -> list[dict]:
    """asyncio.gather on CPU-bound tasks — no speedup, still sequential."""
    async def async_parse(raw):
        return parse_and_transform(raw)  # No I/O — no yield point

    # These all run sequentially because there are no await points
    # that yield to the event loop
    return await asyncio.gather(*[async_parse(r) for r in raw_data_list])


# CORRECT: Use ProcessPoolExecutor for CPU-bound parallelism
async def correct_approach(raw_data_list: list[str]) -> list[dict]:
    """Run CPU-bound work in a process pool from an async context."""
    loop = asyncio.get_event_loop()

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        # run_in_executor bridges the sync-to-async gap
        tasks = [
            loop.run_in_executor(executor, parse_and_transform, raw)
            for raw in raw_data_list
        ]
        return await asyncio.gather(*tasks)
    # Process pool uses multiple CPU cores — actual parallel execution


# Timing comparison (1000 items, ~10ms CPU work each):
import time

start = time.time()
asyncio.run(wrong_approach(["...json..."] * 1000))
print(f"Wrong (async gather, CPU-bound): {time.time() - start:.1f}s")
# Wrong (async gather, CPU-bound): 10.3s  (sequential)

start = time.time()
asyncio.run(correct_approach(["...json..."] * 1000))
print(f"Correct (process pool):          {time.time() - start:.1f}s")
# Correct (process pool):          2.6s   (4 cores × parallel)
```

### Decision Tree: Async vs Threads vs Processes

```
Is your bottleneck I/O (network, disk, external API)?
    YES → asyncio (async/await, highest concurrency per memory)
    NO  → Is it pure Python computation (pandas, JSON parsing)?
        YES → ProcessPoolExecutor (multiprocessing, bypasses GIL)
        NO  → Are you calling C extensions (numpy, compiled code)?
            YES → ThreadPoolExecutor (GIL is released for C code)
            NO  → You probably have a logic bug, not a concurrency problem
```

---

## Key Takeaways for Senior DEs

1. **Async Kafka consumers** need manual commit after successful writes — never use auto-commit for exactly-once semantics.
2. **Bounded queues** (`asyncio.Queue(maxsize=N)`) implement backpressure — the producer naturally slows down when the consumer can't keep up.
3. **Multi-stage async pipelines** process each stage at its own concurrency limit — a slow enrichment API doesn't block the fast validation stage.
4. **CPU-bound work kills async** — use `loop.run_in_executor(ProcessPoolExecutor(...))` to run CPU work in parallel from an async context.
5. **asyncpg + connection pool** is the right stack for async PostgreSQL writes; use `max_size` matching your semaphore limit.

## ⚡ Cheat Sheet

**When to Use What**
- I/O-bound (network, disk, DB) → `asyncio` (single thread, thousands of tasks)
- I/O-bound + shared state → `threading.ThreadPoolExecutor`
- CPU-bound (pandas, JSON parsing) → `ProcessPoolExecutor`
- CPU in async context → `loop.run_in_executor(ProcessPoolExecutor(), fn, arg)`

**asyncio Primitives**
- `asyncio.gather(*tasks, return_exceptions=True)` — run concurrently, collect results
- `asyncio.Semaphore(N)` — cap concurrency without blocking the event loop
- `asyncio.Queue(maxsize=N)` — `put()` blocks when full = automatic backpressure
- `asyncio.Event()` — lightweight signal for graceful shutdown (`_shutdown.set()`)
- `asyncio.create_task()` — schedules coroutine; keep reference or it may be GC'd

**Kafka / Exactly-Once**
- `enable_auto_commit=False` — commit only after successful writes, not on receive
- `max_poll_records=100` — controls batch size per poll cycle
- Commit after `asyncio.wait(pending_tasks, return_when=ALL_COMPLETED)` — not before

**asyncpg Pool**
- `asyncpg.create_pool(dsn, min_size=5, max_size=20, command_timeout=60)`
- Use `async with pool.acquire() as conn` — returns connection to pool on exit
- `async with conn.transaction()` — rollback on exception automatically
- `conn.executemany(sql, rows)` — batch insert in one round-trip

**Backpressure Pattern**
- `asyncio.Queue(maxsize=N)`: producer blocks on `await queue.put()` when full
- Sentinel `None` value signals consumer to stop (producer puts it last)
- `await queue.join()` — wait until all `task_done()` calls match puts

**Gotchas**
- CPU work inside `async def` blocks the event loop — no other tasks run
- `asyncio.gather` on CPU functions provides zero speedup (still sequential)
- Missing `await` on a coroutine creates a coroutine object, never runs it
- Never `time.sleep()` in async code — use `await asyncio.sleep()`
