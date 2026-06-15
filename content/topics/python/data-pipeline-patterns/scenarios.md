---
title: "Data Pipeline Patterns - Scenario Questions"
topic: python
subtopic: data-pipeline-patterns
content_type: scenario_question
tags: [python, pipeline, etl, retry, idempotency, circuit-breaker, dead-letter-queue, metrics]
---

# Scenario Questions — Data Pipeline Patterns

<article data-difficulty="junior">

## 🟢 Junior: Implement a Retry Decorator with Exponential Backoff

**Scenario:** You have a function `fetch_exchange_rates(date: str) -> dict` that calls an external API. The API is flaky and occasionally returns HTTP 503. Write a retry decorator that retries up to 3 times with exponential backoff (1s, 2s, 4s) and logs each failed attempt. The decorator should be reusable on any function.

<details>
<summary>✅ Solution</summary>

```python
import time
import logging
import functools
from typing import Callable, Type

logger = logging.getLogger(__name__)


def retry_exponential(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator: retry with exponential backoff.
    Delays: base_delay, base_delay*2, base_delay*4, ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed permanently after "
                            f"{max_attempts} attempts. Last error: {e}"
                        )
                        raise

                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
        return wrapper
    return decorator


# Apply to any function that can fail transiently
import requests

@retry_exponential(max_attempts=3, base_delay=1.0,
                   exceptions=(requests.exceptions.RequestException,))
def fetch_exchange_rates(date: str) -> dict:
    url = f"https://api.exchangeratesapi.io/{date}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# Test the behavior
# Attempt 1 fails → wait 1s
# Attempt 2 fails → wait 2s
# Attempt 3 succeeds → returns result
# OR: all 3 fail → raises the last exception

rates = fetch_exchange_rates("2024-01-15")
print(rates)
```

**Why `functools.wraps`?** Without it, `fetch_exchange_rates.__name__` would be `"wrapper"` — the decorator would hide the original function's identity, breaking logging, debugging, and introspection.

**When to NOT retry:**
- HTTP 4xx errors (400 Bad Request, 401 Unauthorized, 404 Not Found) — these are not transient; retrying won't fix them
- Only retry HTTP 5xx (server errors) and network exceptions

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design an Idempotent ETL Pipeline Class

**Scenario:** You're building a daily pipeline that loads orders from a REST API into a Postgres database. The pipeline runs at midnight but sometimes fails halfway through. When Airflow retries it at 1 AM, it must not create duplicate records. Design a pipeline class with: idempotent run semantics (safe to run multiple times), checkpoint tracking to skip completed dates, and clean separation of extract/transform/load steps.

<details>
<summary>✅ Solution</summary>

```python
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator

import psycopg2
import requests

logger = logging.getLogger(__name__)


def make_run_id(pipeline: str, logical_date: str) -> str:
    """Deterministic: same pipeline + date always → same run_id."""
    return hashlib.md5(f"{pipeline}:{logical_date}".encode()).hexdigest()[:12]


@dataclass
class OrdersETLPipeline:
    """
    Idempotent daily pipeline: API → Postgres.
    Safe to run multiple times for the same date.
    """
    db_url: str
    api_base_url: str
    checkpoint_dir: str = "/tmp/pipeline_checkpoints"
    batch_size: int = 500

    def __post_init__(self):
        Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)

    # ── Checkpoint management ──────────────────────────────────────────────

    def _checkpoint_path(self, run_id: str) -> Path:
        return Path(self.checkpoint_dir) / f"orders_{run_id}.done"

    def _is_complete(self, run_id: str) -> bool:
        return self._checkpoint_path(run_id).exists()

    def _mark_complete(self, run_id: str, metadata: dict):
        self._checkpoint_path(run_id).write_text(json.dumps(metadata))

    # ── Extract ────────────────────────────────────────────────────────────

    def extract(self, date: str) -> Iterator[dict]:
        """Fetch all orders for a date from the API (paginates automatically)."""
        page, page_size = 1, 200
        while True:
            resp = requests.get(
                f"{self.api_base_url}/orders",
                params={"date": date, "page": page, "per_page": page_size},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            records = data.get("orders", [])
            if not records:
                break
            yield from records
            if len(records) < page_size:
                break
            page += 1

    # ── Transform ──────────────────────────────────────────────────────────

    def transform(self, raw: dict) -> dict | None:
        """Return cleaned record, or None to drop it."""
        amount = raw.get("amount")
        if amount is None or float(amount) < 0:
            return None
        return {
            "order_id": str(raw["id"]),
            "customer_id": str(raw["customer_id"]),
            "amount_cents": int(float(amount) * 100),
            "status": raw.get("status", "unknown").lower(),
            "order_date": raw["date"],
            "loaded_at": datetime.utcnow().isoformat(),
        }

    # ── Load (idempotent upsert) ───────────────────────────────────────────

    def load(self, records: list[dict]) -> int:
        """
        Upsert into Postgres using ON CONFLICT.
        Re-running with the same records updates them, never creates duplicates.
        """
        if not records:
            return 0
        cols = list(records[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c != "order_id"
        )
        sql = f"""
            INSERT INTO fact_orders ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (order_id)
            DO UPDATE SET {update_set}
        """
        values = [tuple(r[c] for c in cols) for r in records]
        conn = psycopg2.connect(self.db_url)
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return len(records)

    # ── Orchestrate ────────────────────────────────────────────────────────

    def run(self, logical_date: str) -> dict:
        run_id = make_run_id("orders", logical_date)

        # IDEMPOTENCY CHECK: skip if this date already completed
        if self._is_complete(run_id):
            logger.info(f"Run {run_id} already complete for {logical_date} — skipping")
            return {"status": "skipped", "run_id": run_id}

        logger.info(f"Starting run {run_id} for {logical_date}")
        batch, total_loaded, total_dropped = [], 0, 0

        for raw in self.extract(logical_date):
            transformed = self.transform(raw)
            if transformed is None:
                total_dropped += 1
                continue
            batch.append(transformed)
            if len(batch) >= self.batch_size:
                total_loaded += self.load(batch)
                batch = []

        if batch:
            total_loaded += self.load(batch)

        result = {
            "status": "success",
            "run_id": run_id,
            "logical_date": logical_date,
            "rows_loaded": total_loaded,
            "rows_dropped": total_dropped,
        }
        self._mark_complete(run_id, result)
        logger.info(f"Run {run_id} complete: {result}")
        return result


# Usage — safe to call multiple times for the same date
pipeline = OrdersETLPipeline(
    db_url=os.environ["DB_URL"],
    api_base_url="https://api.orders.example.com",
)
result = pipeline.run("2024-01-15")
result = pipeline.run("2024-01-15")  # Second call: instantly skipped
```

**Key idempotency guarantees:**
1. `ON CONFLICT DO UPDATE` — same `order_id` written twice → one row, updated
2. Checkpoint file — same `run_id` run twice → second run skips entirely
3. Deterministic `run_id` — same date always generates the same ID, so checkpoint is found on retry

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Pipeline Framework with Parallel Extraction, Circuit Breaker, Dead Letter Queue, and Metrics

**Scenario:** You're building a reusable pipeline framework for your team. It must: (1) extract from multiple sources in parallel using threads, (2) protect downstream services with a circuit breaker, (3) route failed records to a dead letter queue instead of crashing, and (4) emit structured metrics (rows processed, error rate, duration per stage). Design the architecture and implement the key classes. The framework must be testable and extensible.

<details>
<summary>✅ Solution</summary>

```python
import json
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator, Callable

logger = logging.getLogger(__name__)


# ── Metrics ───────────────────────────────────────────────────────────────

@dataclass
class StageMetrics:
    stage: str
    rows_in: int = 0
    rows_out: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0.0

    @property
    def error_rate(self) -> float:
        return self.rows_failed / self.rows_in if self.rows_in else 0.0

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_failed": self.rows_failed,
            "error_rate": round(self.error_rate, 4),
            "duration_seconds": round(self.duration_seconds, 2),
            "throughput_rps": round(self.rows_out / self.duration_seconds, 1)
            if self.duration_seconds else 0,
        }


# ── Circuit Breaker ────────────────────────────────────────────────────────

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._last_failure = 0.0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure >= self.recovery_timeout:
                with self._lock:
                    self._state = CircuitState.HALF_OPEN
                return False
            return True
        return False

    def call(self, func: Callable, *args, **kwargs):
        if self.is_open:
            raise RuntimeError(f"Circuit '{self.name}' is OPEN")
        try:
            result = func(*args, **kwargs)
            with self._lock:
                self._failures = 0
                self._state = CircuitState.CLOSED
            return result
        except Exception:
            with self._lock:
                self._failures += 1
                self._last_failure = time.time()
                if self._failures >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.error(f"Circuit '{self.name}' OPENED after "
                                 f"{self._failures} failures")
            raise


# ── Dead Letter Queue ──────────────────────────────────────────────────────

class DeadLetterQueue:
    def __init__(self, pipeline_name: str, dlq_dir: str = "/tmp/dlq"):
        self.pipeline_name = pipeline_name
        self.path = Path(dlq_dir) / f"{pipeline_name}_dlq.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.count = 0

    def send(self, record: dict, error: str, stage: str):
        entry = {
            "timestamp": time.time(),
            "pipeline": self.pipeline_name,
            "stage": stage,
            "error": error,
            "record": record,
        }
        with self._lock:
            with open(self.path, "a") as f:
                f.write(json.dumps(entry) + "\n")
            self.count += 1
        logger.warning(f"DLQ: record sent from stage '{stage}': {error}")


# ── Plugin Interfaces ──────────────────────────────────────────────────────

class BaseExtractor(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def extract(self) -> Iterator[dict]: ...


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, record: dict) -> dict | None: ...


class BaseLoader(ABC):
    @abstractmethod
    def load(self, records: list[dict]) -> int: ...


# ── Framework Orchestrator ─────────────────────────────────────────────────

class PipelineFramework:
    """
    Reusable framework: parallel extraction → transform → load.
    Built-in: circuit breaker on load, DLQ for failed records, metrics per stage.
    """

    def __init__(
        self,
        pipeline_name: str,
        extractors: list[BaseExtractor],
        transformer: BaseTransformer,
        loader: BaseLoader,
        batch_size: int = 1000,
        max_extract_workers: int = 4,
        circuit_breaker: CircuitBreaker | None = None,
    ):
        self.name = pipeline_name
        self.extractors = extractors
        self.transformer = transformer
        self.loader = loader
        self.batch_size = batch_size
        self.max_extract_workers = max_extract_workers
        self.circuit_breaker = circuit_breaker or CircuitBreaker(pipeline_name)
        self.dlq = DeadLetterQueue(pipeline_name)
        self._metrics: dict[str, StageMetrics] = {}

    def _parallel_extract(self) -> list[dict]:
        """Run all extractors in parallel, merge results."""
        all_records = []
        lock = threading.Lock()
        extract_metrics = StageMetrics("extract")
        start = time.time()

        def run_extractor(extractor: BaseExtractor) -> list[dict]:
            records = list(extractor.extract())
            logger.info(f"Extractor '{extractor.name()}': {len(records)} records")
            return records

        with ThreadPoolExecutor(max_workers=self.max_extract_workers) as executor:
            futures = {executor.submit(run_extractor, e): e for e in self.extractors}
            for future in as_completed(futures):
                extractor = futures[future]
                try:
                    records = future.result()
                    with lock:
                        all_records.extend(records)
                        extract_metrics.rows_out += len(records)
                except Exception as e:
                    logger.error(f"Extractor '{extractor.name()}' failed: {e}")
                    extract_metrics.rows_failed += 1

        extract_metrics.rows_in = len(self.extractors)
        extract_metrics.duration_seconds = time.time() - start
        self._metrics["extract"] = extract_metrics
        return all_records

    def _transform_all(self, raw_records: list[dict]) -> list[dict]:
        """Transform records, routing failures to DLQ."""
        stage = StageMetrics("transform")
        stage.rows_in = len(raw_records)
        start = time.time()
        results = []

        for record in raw_records:
            try:
                transformed = self.transformer.transform(record)
                if transformed is None:
                    stage.rows_failed += 1  # Intentionally dropped
                else:
                    results.append(transformed)
                    stage.rows_out += 1
            except Exception as e:
                stage.rows_failed += 1
                self.dlq.send(record, str(e), stage="transform")

        stage.duration_seconds = time.time() - start
        self._metrics["transform"] = stage
        return results

    def _load_batched(self, records: list[dict]) -> int:
        """Load in batches with circuit breaker protection."""
        stage = StageMetrics("load")
        stage.rows_in = len(records)
        start = time.time()
        total_loaded = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i : i + self.batch_size]
            try:
                n = self.circuit_breaker.call(self.loader.load, batch)
                total_loaded += n
                stage.rows_out += n
            except RuntimeError:
                # Circuit open: send entire batch to DLQ
                for record in batch:
                    self.dlq.send(record, "circuit_open", stage="load")
                stage.rows_failed += len(batch)
            except Exception as e:
                # Load failed: send batch to DLQ
                for record in batch:
                    self.dlq.send(record, str(e), stage="load")
                stage.rows_failed += len(batch)

        stage.duration_seconds = time.time() - start
        self._metrics["load"] = stage
        return total_loaded

    def run(self) -> dict:
        logger.info(f"Pipeline '{self.name}' starting")
        overall_start = time.time()

        raw = self._parallel_extract()
        transformed = self._transform_all(raw)
        loaded = self._load_batched(transformed)

        summary = {
            "pipeline": self.name,
            "total_duration_seconds": round(time.time() - overall_start, 2),
            "rows_extracted": len(raw),
            "rows_loaded": loaded,
            "rows_in_dlq": self.dlq.count,
            "stages": {k: v.to_dict() for k, v in self._metrics.items()},
        }
        logger.info(f"Pipeline complete: {json.dumps(summary)}")
        return summary


# ── Example usage ──────────────────────────────────────────────────────────

class ApiExtractor(BaseExtractor):
    def __init__(self, name_: str, endpoint: str):
        self._name = name_
        self.endpoint = endpoint

    def name(self) -> str:
        return self._name

    def extract(self) -> Iterator[dict]:
        import requests
        resp = requests.get(self.endpoint, timeout=30)
        resp.raise_for_status()
        yield from resp.json()["records"]


class OrderTransformer(BaseTransformer):
    def transform(self, record: dict) -> dict | None:
        if not record.get("order_id"):
            return None
        return {
            "order_id": record["order_id"],
            "amount_cents": int(float(record.get("amount", 0)) * 100),
        }


class PostgresLoader(BaseLoader):
    def load(self, records: list[dict]) -> int:
        # ... upsert via psycopg2 ...
        return len(records)


framework = PipelineFramework(
    pipeline_name="daily_orders",
    extractors=[
        ApiExtractor("us_orders", "https://api.example.com/orders/us"),
        ApiExtractor("eu_orders", "https://api.example.com/orders/eu"),
        ApiExtractor("apac_orders", "https://api.example.com/orders/apac"),
    ],
    transformer=OrderTransformer(),
    loader=PostgresLoader(),
    batch_size=1000,
    max_extract_workers=3,
    circuit_breaker=CircuitBreaker("postgres-loader", failure_threshold=3),
)

result = framework.run()
print(json.dumps(result, indent=2))
```

**Architecture decisions to highlight in the interview:**

1. **Parallel extraction with `ThreadPoolExecutor`:** Extractors are I/O-bound (network calls), so threads are appropriate. CPU-bound transforms use a single thread to avoid serialization overhead.
2. **Circuit breaker on the loader (not extractor):** The loader hits the destination database — the most likely bottleneck in production. Protecting it prevents cascading failures when the DB is slow.
3. **DLQ per stage:** Knowing whether a record failed in transform (data quality issue) vs load (infrastructure issue) tells you whether to fix the data or fix the infra.
4. **Metrics per stage:** `throughput_rps` in the metrics lets you identify which stage is the bottleneck in profiling — often the transform stage for complex business rules.
5. **Plugin interfaces as ABCs:** Teams can add new extractors/loaders without modifying the framework. The `BaseExtractor` contract is the API boundary.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a retry and a dead letter queue?**
A: A retry is for transient failures (network timeout, rate limit) where the same record might succeed if you try again. A DLQ is for records that consistently fail — after N retries, instead of crashing the pipeline you park the record in a DLQ for later investigation. The pipeline continues processing the other 9,999,999 records.

**Q: What makes a pipeline idempotent?**
A: The pipeline produces the same outcome regardless of how many times it runs for the same inputs. This requires two things: idempotent writes (upserts with `ON CONFLICT` or deduplication keys so the same record written twice produces one row), and idempotent run tracking (checkpoint that skips the entire run if it already completed).

**Q: When would you use ProcessPoolExecutor over ThreadPoolExecutor?**
A: Use `ProcessPoolExecutor` for CPU-bound work (parsing JSON, applying complex transformations, encryption, regex matching on large strings) because the GIL prevents true parallelism in threads for CPU work. Use `ThreadPoolExecutor` for I/O-bound work (HTTP requests, database queries, file reads) because threads release the GIL during I/O system calls and share memory cheaply.

**Q: What is backpressure and why does it matter in pipelines?**
A: Backpressure is a mechanism by which a slow consumer signals a fast producer to slow down. Without it, the producer can generate records faster than the consumer processes them, causing unbounded queue growth and OOM. In Python: `queue.Queue(maxsize=N)` applies backpressure by blocking `put()` when the queue is full. In async code: bounded `asyncio.Queue` has the same effect.
