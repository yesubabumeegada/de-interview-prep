---
title: "Python Multithreading & Multiprocessing - Real World"
topic: python
subtopic: multithreading-multiprocessing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, multithreading, multiprocessing, production]
---

# Python Multithreading & Multiprocessing — Real World Production Patterns

## Production Concurrency for Data Pipelines

These patterns solve real extraction and transformation bottlenecks with proper error handling and monitoring.

---

## Pattern 1: Parallel API Extraction (ThreadPool with Retries)

```python
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import threading

@dataclass
class ExtractionResult:
    source: str
    records: list[dict]
    success: bool
    error: str | None = None

class ParallelExtractor:
    def __init__(self, max_workers: int = 10, max_retries: int = 3):
        self.max_workers = max_workers
        self.max_retries = max_retries

    def _fetch_with_retry(self, name: str, fetch_fn, **kwargs) -> ExtractionResult:
        for attempt in range(self.max_retries):
            try:
                records = fetch_fn(**kwargs)
                return ExtractionResult(name, records, True)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    return ExtractionResult(name, [], False, str(e))
                time.sleep(2 ** attempt)

    def extract_all(self, sources: list[dict]) -> dict:
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_with_retry, s["name"], s["fn"], **s.get("kwargs", {})): s["name"]
                for s in sources
            }
            for future in as_completed(futures):
                results.append(future.result())

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        return {
            "successful": len(successes),
            "failed": len(failures),
            "total_records": sum(len(r.records) for r in successes),
            "errors": [{"source": r.source, "error": r.error} for r in failures],
        }

# Usage
import requests
def fetch_api(url: str) -> list[dict]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("results", [])

extractor = ParallelExtractor(max_workers=10)
sources = [
    {"name": "users", "fn": fetch_api, "kwargs": {"url": "https://api.co/users"}},
    {"name": "orders", "fn": fetch_api, "kwargs": {"url": "https://api.co/orders"}},
]
result = extractor.extract_all(sources)
```

---

## Pattern 2: CPU-Bound Transformation with ProcessPool

```python
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import math, json, re

def transform_chunk(chunk: list[dict]) -> list[dict]:
    """CPU-intensive — benefits from separate process with own GIL."""
    results = []
    for record in chunk:
        metadata = json.loads(record.get("meta_json", "{}"))
        email = record.get("contact", "")
        domain_match = re.search(r"@([\w.]+)", email)
        txns = record.get("transactions", [])
        mean_val = sum(txns) / len(txns) if txns else 0

        results.append({
            "id": record["id"],
            "domain": domain_match.group(1) if domain_match else None,
            "total": sum(txns),
            "mean": mean_val,
            "std": math.sqrt(sum((x - mean_val)**2 for x in txns) / max(len(txns), 1)),
            "meta_keys": list(metadata.keys()),
        })
    return results

def parallel_transform(records: list[dict]) -> list[dict]:
    workers = mp.cpu_count()
    chunk_size = math.ceil(len(records) / workers)
    chunks = [records[i:i+chunk_size] for i in range(0, len(records), chunk_size)]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        result_chunks = list(executor.map(transform_chunk, chunks))
    return [r for chunk in result_chunks for r in chunk]

# 100K records across all CPU cores
records = [{"id": i, "contact": f"u{i}@co{i%10}.com", "meta_json": "{}", "transactions": [float(j) for j in range(20)]} for i in range(100_000)]
results = parallel_transform(records)
```

---

## Pattern 3: Async Web Scraper for Data Collection

```python
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ScrapedPage:
    url: str
    status: int
    data: dict | None = None
    error: str | None = None

class AsyncCollector:
    def __init__(self, max_concurrent: int = 20, timeout: int = 15):
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def _fetch(self, session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> ScrapedPage:
        async with sem:
            try:
                async with session.get(url) as resp:
                    text = await resp.text()
                    return ScrapedPage(url, resp.status, {"length": len(text)})
            except asyncio.TimeoutError:
                return ScrapedPage(url, 0, error="timeout")
            except Exception as e:
                return ScrapedPage(url, 0, error=str(e))

    async def collect(self, urls: list[str]) -> list[ScrapedPage]:
        sem = asyncio.Semaphore(self.max_concurrent)
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = [self._fetch(session, url, sem) for url in urls]
            return await asyncio.gather(*tasks)

# collector = AsyncCollector(max_concurrent=20)
# results = asyncio.run(collector.collect(urls))
```

---

## Pattern 4: Producer-Consumer ETL with Bounded Queues

Threads extract (I/O) → bounded queue → processes transform (CPU) → queue → threads load.

```python
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time

STOP = None

class MultiStagePipeline:
    def __init__(self, producers: int = 5, consumers: int = 4, queue_size: int = 50):
        self.num_producers = producers
        self.num_consumers = consumers
        self.extract_q = Queue(maxsize=queue_size)
        self.load_q = Queue(maxsize=queue_size)

    def _extract(self, source_id: int, fetch_fn):
        for batch in fetch_fn(source_id):
            self.extract_q.put(batch)

    def _transform_loop(self, transform_fn):
        with ProcessPoolExecutor(max_workers=2) as pool:
            while True:
                try:
                    batch = self.extract_q.get(timeout=10)
                    if batch is STOP: break
                    result = pool.submit(transform_fn, batch).result(timeout=60)
                    self.load_q.put(result)
                except Empty:
                    break

    def _load_loop(self, write_fn):
        while True:
            try:
                batch = self.load_q.get(timeout=10)
                if batch is STOP: break
                write_fn(batch)
            except Empty:
                break

    def run(self, fetch_fn, transform_fn, write_fn) -> dict:
        # Start producers
        with ThreadPoolExecutor(self.num_producers) as tp:
            futures = [tp.submit(self._extract, i, fetch_fn) for i in range(self.num_producers)]
            for f in futures: f.result()

        # Signal transform to stop
        self.extract_q.put(STOP)

        # Run transform
        t = threading.Thread(target=self._transform_loop, args=(transform_fn,))
        t.start()
        t.join()

        # Signal load to stop
        self.load_q.put(STOP)

        # Run load
        lt = threading.Thread(target=self._load_loop, args=(write_fn,))
        lt.start()
        lt.join()
        return {"status": "complete"}
```

---

## Production Monitoring Checklist

| Metric | Alert Threshold |
|--------|-----------------|
| Queue depth | > 80% capacity |
| Worker utilization | < 30% or > 95% |
| Error rate | > 5% |
| Throughput (rec/s) | < baseline |
| Memory per worker | > 2× initial |

---

## Interview Tips

> **Tip 1:** The ThreadPool → Queue → ProcessPool architecture is the production answer to "design a concurrent Python pipeline." Explain: threads for I/O extraction, bounded queue for backpressure, processes for CPU parallelism.

> **Tip 2:** Always mention graceful shutdown: "Producers flush remaining batches, consumers drain the queue, sentinel values signal termination." This shows operational maturity beyond happy-path thinking.

> **Tip 3:** For async scraping, mention rate limiting: "I use `asyncio.Semaphore` to cap concurrency at 20. Without it, you overwhelm the target and get rate-limited." Shows you consider external system impact.
