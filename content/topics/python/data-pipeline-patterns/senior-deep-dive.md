---
title: "Data Pipeline Patterns - Senior Deep Dive"
topic: python
subtopic: data-pipeline-patterns
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, pipeline, plugin-architecture, exactly-once, backpressure, memory-profiling, testing]
---

# Data Pipeline Patterns — Senior Deep Dive

## Plugin Architecture for Extensible Pipelines

A plugin architecture lets teams add new extractors/loaders without modifying core pipeline code. The registry pattern is the standard implementation.

```python
from abc import ABC, abstractmethod
from typing import Iterator, Type


# Abstract base classes define the contract
class BaseExtractor(ABC):
    """Every extractor plugin must implement this interface."""

    @abstractmethod
    def extract(self, config: dict) -> Iterator[dict]:
        """Yield records from the data source."""
        ...

    @abstractmethod
    def validate_config(self, config: dict) -> None:
        """Raise ValueError if config is invalid."""
        ...


class BaseLoader(ABC):
    @abstractmethod
    def load(self, records: list[dict], config: dict) -> int:
        """Load records to destination. Return count loaded."""
        ...


# Registry: maps string names to plugin classes
_EXTRACTOR_REGISTRY: dict[str, Type[BaseExtractor]] = {}
_LOADER_REGISTRY: dict[str, Type[BaseLoader]] = {}


def register_extractor(name: str):
    """Decorator to register an extractor plugin."""
    def decorator(cls: Type[BaseExtractor]):
        if not issubclass(cls, BaseExtractor):
            raise TypeError(f"{cls} must subclass BaseExtractor")
        _EXTRACTOR_REGISTRY[name] = cls
        return cls
    return decorator


def register_loader(name: str):
    def decorator(cls: Type[BaseLoader]):
        _LOADER_REGISTRY[name] = cls
        return cls
    return decorator


def get_extractor(name: str) -> BaseExtractor:
    if name not in _EXTRACTOR_REGISTRY:
        available = list(_EXTRACTOR_REGISTRY.keys())
        raise KeyError(f"Unknown extractor '{name}'. Available: {available}")
    return _EXTRACTOR_REGISTRY[name]()


# Plugin implementations — these live in separate modules/packages
@register_extractor("postgres")
class PostgresExtractor(BaseExtractor):
    def extract(self, config: dict) -> Iterator[dict]:
        import psycopg2
        conn = psycopg2.connect(config["connection_string"])
        with conn.cursor() as cur:
            cur.execute(config["query"])
            cols = [d[0] for d in cur.description]
            for row in cur:
                yield dict(zip(cols, row))

    def validate_config(self, config: dict):
        for key in ("connection_string", "query"):
            if key not in config:
                raise ValueError(f"PostgresExtractor requires '{key}' in config")


@register_extractor("s3_csv")
class S3CsvExtractor(BaseExtractor):
    def extract(self, config: dict) -> Iterator[dict]:
        import boto3, csv, io
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=config["bucket"], Key=config["key"])
        reader = csv.DictReader(io.TextIOWrapper(obj["Body"]))
        yield from reader

    def validate_config(self, config: dict):
        for key in ("bucket", "key"):
            if key not in config:
                raise ValueError(f"S3CsvExtractor requires '{key}'")


# Config-driven pipeline: define entire pipeline in YAML/JSON, no code changes
pipeline_config = {
    "extractor": "postgres",
    "extractor_config": {
        "connection_string": "postgresql://prod-db/warehouse",
        "query": "SELECT * FROM orders WHERE date = '2024-01-15'",
    },
    "loader": "bigquery",
    "loader_config": {
        "project": "my-project",
        "dataset": "analytics",
        "table": "orders",
    },
}

extractor = get_extractor(pipeline_config["extractor"])
extractor.validate_config(pipeline_config["extractor_config"])
```

**Pipeline-as-code vs config-driven tradeoffs:**
- **Config-driven:** Non-engineers can modify pipelines; easy to template; works well when pipelines are structurally similar (extract-transform-load with different sources/targets)
- **Pipeline-as-code:** Full expressiveness of Python; easier to debug; better for complex branching, conditional logic, and custom state management

---

## Exactly-Once Processing: Idempotent Upserts and Deduplication

True exactly-once delivery is impossible in distributed systems. The practical answer is **at-least-once delivery + idempotent writes**.

```python
import hashlib
import json
from datetime import datetime


def generate_dedup_key(record: dict, key_fields: list[str]) -> str:
    """Stable hash of key fields — same record always gets same key."""
    key_data = {f: record[f] for f in sorted(key_fields)}
    return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()


# Postgres: ON CONFLICT DO UPDATE (upsert) for exactly-once semantics
def upsert_records(conn, records: list[dict], table: str, key_cols: list[str]):
    """
    Load records idempotently — re-running with same data produces same result.
    Uses INSERT ... ON CONFLICT DO UPDATE (Postgres 9.5+).
    """
    if not records:
        return 0

    cols = list(records[0].keys())
    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    conflict_cols = ", ".join(key_cols)

    # Update all non-key columns on conflict
    update_cols = [c for c in cols if c not in key_cols]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_clause}, updated_at = NOW()
    """

    values = [tuple(r[c] for c in cols) for r in records]
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()
    return len(records)


# Deduplication in-memory before loading
def deduplicate(records: list[dict], key_fields: list[str]) -> list[dict]:
    """Keep the last record for each unique key (most recent wins)."""
    seen: dict[str, dict] = {}
    for record in records:
        key = generate_dedup_key(record, key_fields)
        seen[key] = record  # Later record overwrites earlier
    return list(seen.values())
```

---

## Backpressure Handling: Bounded Queues and Semaphores

Without backpressure, a fast producer will exhaust memory.

```python
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor


# Pattern 1: Bounded queue (blocks producer when consumer is slow)
import queue

def pipeline_with_backpressure(source_iter, process_fn, max_queue_size=500):
    """
    Bounded queue limits memory: producer blocks when queue is full.
    max_queue_size = 500 means at most 500 records are in-flight at once.
    """
    q = queue.Queue(maxsize=max_queue_size)
    results = []

    def producer():
        for item in source_iter:
            q.put(item)  # BLOCKS if queue has 500 items — natural backpressure
        q.put(None)  # Sentinel

    def consumer():
        while True:
            item = q.get()
            if item is None:
                break
            results.append(process_fn(item))

    t_prod = threading.Thread(target=producer)
    t_cons = threading.Thread(target=consumer)
    t_prod.start(); t_cons.start()
    t_prod.join(); t_cons.join()
    return results


# Pattern 2: Semaphore for concurrent API call throttling
import asyncio
import aiohttp

async def fetch_all_with_backpressure(urls: list[str], max_concurrent: int = 10):
    """Semaphore limits concurrent requests regardless of list size."""
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []

    async def fetch_one(session, url):
        async with semaphore:  # Only max_concurrent coroutines run at once
            async with session.get(url) as response:
                return await response.json()

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    return [r for r in results if not isinstance(r, Exception)]
```

---

## Memory-Efficient Processing: Generators, Chunking, and tracemalloc

```python
import tracemalloc
import gc
from typing import Iterator


def chunked_reader(filepath: str, chunksize: int = 10_000) -> Iterator[list[dict]]:
    """Read a large CSV in chunks — never loads the whole file."""
    import csv
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunksize:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def process_large_file(filepath: str):
    """Process 10GB CSV with bounded memory using chunked generator."""
    total = 0
    for chunk in chunked_reader(filepath, chunksize=50_000):
        processed = transform_chunk(chunk)
        load_to_db(processed)
        total += len(processed)
        del chunk, processed   # Explicit delete helps GC in tight loops
        gc.collect()           # Force GC for very large chunks
    return total


# Memory profiling with tracemalloc
def profile_memory(func, *args, **kwargs):
    """Wrap any function to measure peak memory usage."""
    tracemalloc.start()
    result = func(*args, **kwargs)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Current memory: {current / 1024**2:.1f} MB")
    print(f"Peak memory:    {peak / 1024**2:.1f} MB")

    # Get the top 5 lines by memory allocation
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")
    for stat in top_stats[:5]:
        print(stat)

    return result
```

---

## Pipeline Versioning and Schema Evolution

```python
from dataclasses import dataclass
from typing import Callable


@dataclass
class SchemaVersion:
    version: int
    migrator: Callable[[dict], dict]


class SchemaMigrator:
    """Migrate records from any older version to current version."""

    def __init__(self):
        self._migrations: dict[int, SchemaVersion] = {}
        self.current_version = 0

    def register(self, from_version: int):
        """Decorator: register a migration from version N to N+1."""
        def decorator(func: Callable):
            self._migrations[from_version] = SchemaVersion(
                version=from_version + 1,
                migrator=func,
            )
            self.current_version = max(self.current_version, from_version + 1)
            return func
        return decorator

    def migrate(self, record: dict) -> dict:
        """Bring record up to the latest schema version."""
        version = record.get("_schema_version", 1)
        while version < self.current_version:
            migration = self._migrations.get(version)
            if not migration:
                raise ValueError(f"No migration from version {version}")
            record = migration.migrator(record)
            record["_schema_version"] = migration.version
            version = migration.version
        return record


migrator = SchemaMigrator()

@migrator.register(from_version=1)
def v1_to_v2(record: dict) -> dict:
    """v1 had 'amount' as string; v2 has 'amount_cents' as int."""
    record["amount_cents"] = int(float(record.pop("amount")) * 100)
    return record

@migrator.register(from_version=2)
def v2_to_v3(record: dict) -> dict:
    """v2 had 'user' as a flat string; v3 has 'user_id' and 'user_name'."""
    user_parts = record.pop("user", "0:unknown").split(":")
    record["user_id"] = int(user_parts[0])
    record["user_name"] = user_parts[1] if len(user_parts) > 1 else "unknown"
    return record
```

---

## Testing Strategies for Pipeline Code

```python
import pytest
from unittest.mock import MagicMock, patch


# 1. Unit test the transformer in isolation
def test_transform_normalizes_amount():
    transformer = OrderTransformer()
    raw = {"order_id": "123", "amount": "45.67", "currency": "USD"}
    result = transformer.transform(raw)
    assert result["amount_cents"] == 4567
    assert result["currency"] == "USD"

# 2. Test the full pipeline with mock extractor/loader
def test_pipeline_skips_null_records():
    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = iter([
        {"id": 1, "amount": "10.00"},
        {"id": 2, "amount": None},   # Should be dropped
        {"id": 3, "amount": "30.00"},
    ])
    mock_loader = MagicMock()
    mock_loader.load.return_value = 2

    pipeline = ETLPipeline(mock_extractor, OrderTransformer(), mock_loader)
    result = pipeline.run()

    assert result["loaded"] == 2
    assert result["dropped"] == 1
    mock_loader.load.assert_called_once()

# 3. Test idempotency: running twice should not double-count
def test_pipeline_idempotent(tmp_path):
    store = CheckpointStore(str(tmp_path))
    run_id = "test-run-001"

    run_count = 0
    def run_if_needed():
        nonlocal run_count
        if not store.is_complete(run_id):
            run_count += 1
            store.mark_complete(run_id, {"rows": 100})

    run_if_needed()
    run_if_needed()  # Second call should be skipped

    assert run_count == 1  # Pipeline ran exactly once
```

---

## Interview Tips

- **Plugin architecture** is a strong signal at senior level — it shows you design for extensibility. Mention `abstract base classes` + `registry pattern` as the canonical Python approach.
- For **exactly-once**, the correct framing is: "exactly-once is a property of the write, not the delivery. I implement at-least-once delivery with idempotent upserts so re-runs are safe."
- **tracemalloc** is the built-in Python memory profiler — knowing it (vs just `memory_profiler`) signals comfort with stdlib tools.
- Testing pipelines: always cover idempotency (can re-run?), the "skip if already done" path, and error handling for bad records.
