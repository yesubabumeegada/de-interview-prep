---
title: "Python Multithreading & Multiprocessing - Scenario Questions"
topic: python
subtopic: multithreading-multiprocessing
content_type: scenario_question
tags: [python, concurrency, interview, scenarios]
---

# Scenario Questions — Python Multithreading & Multiprocessing

<article data-difficulty="junior">

## 🟢 Junior: Explain GIL and Choose Threads vs Processes

**Scenario:** Your team lead asks you to speed up two different Python scripts:

- **Script A:** Downloads 50 CSV files from an SFTP server (each takes 2-5 seconds)
- **Script B:** Reads one 10GB CSV file and applies regex parsing + statistical calculations on every row

For each script, explain: Would you use threading or multiprocessing? Why? What role does the GIL play in your decision?

<details>
<summary>💡 Hint</summary>

Think about what the CPU is doing in each case. Is it waiting (I/O) or computing?

</details>

<details>
<summary>✅ Solution</summary>

**Script A — SFTP Downloads: Use Threading**

```python
from concurrent.futures import ThreadPoolExecutor
import paramiko

def download_file(sftp_config: dict, remote_path: str, local_path: str) -> str:
    """Download a file via SFTP. I/O-bound — GIL is released during network wait."""
    transport = paramiko.Transport((sftp_config["host"], 22))
    transport.connect(username=sftp_config["user"], password=sftp_config["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get(remote_path, local_path)
    sftp.close()
    transport.close()
    return local_path

files_to_download = [(f"/remote/data_{i}.csv", f"/local/data_{i}.csv") for i in range(50)]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(download_file, sftp_config, remote, local)
        for remote, local in files_to_download
    ]
    results = [f.result() for f in futures]

# ~10x speedup: 50 files in 5 seconds instead of 50 seconds
```

**Why threads work here:**
- Network I/O releases the GIL
- While one thread waits for data, others can initiate their downloads
- Low memory overhead (shared memory space)

---

**Script B — Heavy Computation: Use Multiprocessing**

```python
from concurrent.futures import ProcessPoolExecutor
import re
import math

def process_chunk(chunk: list[str]) -> list[dict]:
    """Parse and compute — CPU-bound. Each process gets its own GIL."""
    pattern = re.compile(r'(\d{4}-\d{2}-\d{2}),(.+?),(\d+\.?\d*)')
    results = []
    
    for line in chunk:
        match = pattern.match(line)
        if match:
            date, name, value = match.groups()
            val = float(value)
            results.append({
                "date": date,
                "name": name.strip(),
                "value": val,
                "log_value": math.log1p(val),
                "normalized": val / 1000.0,
            })
    return results

# Read file and split into chunks for parallel processing
with open("big_file.csv") as f:
    lines = f.readlines()

chunk_size = len(lines) // 8  # 8 processes
chunks = [lines[i:i+chunk_size] for i in range(0, len(lines), chunk_size)]

with ProcessPoolExecutor(max_workers=8) as executor:
    result_chunks = list(executor.map(process_chunk, chunks))

all_results = [r for chunk in result_chunks for r in chunk]
```

**Why processes work here:**
- Regex parsing and math are pure CPU work
- GIL prevents thread parallelism for CPU tasks
- Each process has its own GIL = true parallel execution
- 8 cores → ~8x speedup for CPU-bound work

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement Parallel API Fetcher with Error Handling

**Scenario:** You need to extract data from 30 external REST APIs as part of a daily pipeline. Requirements:
1. Fetch all 30 APIs concurrently (max 10 simultaneous connections)
2. Each API call should timeout after 15 seconds
3. Retry failed requests up to 3 times with exponential backoff
4. Collect results for successful calls and error details for failures
5. The pipeline should NOT fail if some APIs are down — collect what you can

Implement this with proper error handling.

<details>
<summary>💡 Hint</summary>

Use `ThreadPoolExecutor` with `as_completed`. Implement retry logic within each worker. Track successes and failures separately.

</details>

<details>
<summary>✅ Solution</summary>

```python
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

@dataclass
class APIResult:
    source: str
    success: bool
    data: list[dict] | None = None
    error: str | None = None
    attempts: int = 0
    duration_sec: float = 0.0

class ResilientAPIExtractor:
    def __init__(self, max_concurrent: int = 10, timeout: int = 15, max_retries: int = 3):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.max_retries = max_retries
    
    def _fetch_with_retry(self, source_name: str, url: str, headers: dict = None) -> APIResult:
        """Fetch a single API with retries and exponential backoff."""
        start = time.perf_counter()
        last_error = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, headers=headers or {}, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                # Normalize to list
                records = data if isinstance(data, list) else data.get("results", [data])
                
                return APIResult(
                    source=source_name, success=True, data=records,
                    attempts=attempt,
                    duration_sec=time.perf_counter() - start,
                )
            except requests.exceptions.Timeout:
                last_error = f"Timeout after {self.timeout}s"
            except requests.exceptions.HTTPError as e:
                last_error = f"HTTP {e.response.status_code}"
                if e.response.status_code < 500:
                    break  # Don't retry client errors (4xx)
            except requests.exceptions.ConnectionError:
                last_error = "Connection refused"
            except Exception as e:
                last_error = str(e)
            
            # Exponential backoff
            if attempt < self.max_retries:
                time.sleep(2 ** (attempt - 1))
        
        return APIResult(
            source=source_name, success=False, error=last_error,
            attempts=attempt, duration_sec=time.perf_counter() - start,
        )
    
    def extract_all(self, api_configs: list[dict]) -> dict[str, Any]:
        """
        Extract from all APIs concurrently.
        
        api_configs: [{"name": str, "url": str, "headers": dict}, ...]
        """
        results: list[APIResult] = []
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {
                executor.submit(
                    self._fetch_with_retry,
                    config["name"], config["url"], config.get("headers")
                ): config["name"]
                for config in api_configs
            }
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result.success:
                    print(f"  ✓ {result.source}: {len(result.data)} records ({result.duration_sec:.1f}s)")
                else:
                    print(f"  ✗ {result.source}: {result.error} (after {result.attempts} attempts)")
        
        # Aggregate
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        all_data = [record for r in successes for record in r.data]
        
        return {
            "total_sources": len(api_configs),
            "successful": len(successes),
            "failed": len(failures),
            "total_records": len(all_data),
            "data": all_data,
            "errors": [{"source": r.source, "error": r.error} for r in failures],
        }

# Usage
extractor = ResilientAPIExtractor(max_concurrent=10, timeout=15, max_retries=3)
api_configs = [
    {"name": "users_api", "url": "https://api.example.com/users"},
    {"name": "orders_api", "url": "https://api.example.com/orders"},
    {"name": "metrics_api", "url": "https://metrics.internal/data", "headers": {"Authorization": "Bearer ..."}},
    # ... 27 more APIs
]

result = extractor.extract_all(api_configs)
print(f"\nExtraction complete: {result['successful']}/{result['total_sources']} sources, "
      f"{result['total_records']} records collected")
# Pipeline continues with partial data — doesn't fail completely
```

**Key design decisions:**
- `max_concurrent=10` prevents overwhelming networks or hitting rate limits
- Don't retry 4xx errors (client issues won't self-resolve)
- Exponential backoff prevents thundering herd on recovery
- Pipeline collects partial results rather than failing entirely

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Concurrent ETL Pipeline That Doesn't Deadlock

**Scenario:** Design a multi-stage concurrent ETL pipeline with these requirements:
1. **Stage 1 (Extract):** 5 producer threads pull from different APIs
2. **Stage 2 (Transform):** 4 worker processes apply CPU-heavy transformations
3. **Stage 3 (Load):** 2 writer threads batch-insert into the database

The stages communicate via bounded queues. Your design must:
- Never deadlock
- Handle backpressure (slow consumers don't cause OOM)
- Gracefully shut down on error in any stage
- Provide throughput metrics

<details>
<summary>💡 Hint</summary>

Use bounded queues between stages (backpressure). Poison pills for shutdown. A coordinator thread monitors health. Lock ordering prevents deadlock.

</details>

<details>
<summary>✅ Solution</summary>

```python
import threading
import time
from queue import Queue, Empty, Full
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any
import logging

logger = logging.getLogger(__name__)
SHUTDOWN = object()

@dataclass
class PipelineMetrics:
    extracted: int = 0
    transformed: int = 0
    loaded: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    
    def inc(self, field_name: str, amount: int = 1):
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + amount)

class ConcurrentETLPipeline:
    """
    Multi-stage pipeline with deadlock-free design.
    
    Extract (threads) → Queue1 → Transform (processes) → Queue2 → Load (threads)
    """
    
    def __init__(self):
        self.extract_queue = Queue(maxsize=100)   # Bounded: backpressure
        self.load_queue = Queue(maxsize=50)       # Bounded: backpressure
        self.metrics = PipelineMetrics()
        self.shutdown_event = threading.Event()   # Global shutdown signal
        self.error_event = threading.Event()      # Error occurred
    
    # --- Stage 1: Extract (I/O-bound threads) ---
    def _extract_worker(self, source_id: int, fetch_fn):
        """Producer thread: extracts and puts on queue."""
        try:
            while not self.shutdown_event.is_set():
                records = fetch_fn(source_id)
                if not records:
                    break
                
                for record in records:
                    if self.shutdown_event.is_set():
                        return
                    try:
                        self.extract_queue.put(record, timeout=5.0)
                        self.metrics.inc("extracted")
                    except Full:
                        if self.shutdown_event.is_set():
                            return
        except Exception as e:
            logger.error(f"Extract worker {source_id} failed: {e}")
            self.metrics.inc("errors")
            self.error_event.set()
    
    # --- Stage 2: Transform (CPU-bound processes) ---
    def _transform_coordinator(self, transform_fn, num_workers: int = 4):
        """Coordinator thread: feeds process pool from extract_queue."""
        with ProcessPoolExecutor(max_workers=num_workers) as pool:
            batch = []
            batch_size = 50
            
            while not self.shutdown_event.is_set():
                try:
                    record = self.extract_queue.get(timeout=2.0)
                    if record is SHUTDOWN:
                        break
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        # Send batch to process pool
                        future = pool.submit(transform_fn, batch)
                        results = future.result(timeout=60)
                        for r in results:
                            self.load_queue.put(r, timeout=5.0)
                            self.metrics.inc("transformed")
                        batch = []
                except Empty:
                    # Flush partial batch
                    if batch:
                        future = pool.submit(transform_fn, batch)
                        results = future.result(timeout=60)
                        for r in results:
                            self.load_queue.put(r, timeout=5.0)
                            self.metrics.inc("transformed")
                        batch = []
                except Exception as e:
                    logger.error(f"Transform failed: {e}")
                    self.metrics.inc("errors")
                    self.error_event.set()
                    break
        
        # Signal load workers to stop
        self.load_queue.put(SHUTDOWN)
    
    # --- Stage 3: Load (I/O-bound threads) ---
    def _load_worker(self, write_fn, batch_size: int = 100):
        """Consumer thread: batches and writes to database."""
        batch = []
        
        while not self.shutdown_event.is_set():
            try:
                record = self.load_queue.get(timeout=2.0)
                if record is SHUTDOWN:
                    # Re-put for other load workers
                    self.load_queue.put(SHUTDOWN)
                    break
                batch.append(record)
                
                if len(batch) >= batch_size:
                    write_fn(batch)
                    self.metrics.inc("loaded", len(batch))
                    batch = []
            except Empty:
                if batch:
                    write_fn(batch)
                    self.metrics.inc("loaded", len(batch))
                    batch = []
        
        # Flush remaining
        if batch:
            write_fn(batch)
            self.metrics.inc("loaded", len(batch))
    
    def run(self, sources, fetch_fn, transform_fn, write_fn) -> dict:
        """Run the full pipeline with graceful shutdown."""
        threads = []
        
        # Start extract workers
        for source_id in sources:
            t = threading.Thread(target=self._extract_worker, args=(source_id, fetch_fn))
            t.start()
            threads.append(("extract", t))
        
        # Start transform coordinator
        t = threading.Thread(target=self._transform_coordinator, args=(transform_fn,))
        t.start()
        threads.append(("transform", t))
        
        # Start load workers
        for _ in range(2):
            t = threading.Thread(target=self._load_worker, args=(write_fn,))
            t.start()
            threads.append(("load", t))
        
        # Monitor thread
        def monitor():
            while not self.shutdown_event.is_set():
                time.sleep(5)
                logger.info(
                    f"Pipeline: extracted={self.metrics.extracted}, "
                    f"transformed={self.metrics.transformed}, "
                    f"loaded={self.metrics.loaded}, errors={self.metrics.errors}"
                )
                if self.error_event.is_set():
                    logger.error("Error detected — initiating shutdown")
                    self.shutdown_event.set()
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        
        # Wait for extract to complete
        for name, t in threads:
            if name == "extract":
                t.join()
        
        # Signal end of extraction
        self.extract_queue.put(SHUTDOWN)
        
        # Wait for transform and load
        for name, t in threads:
            if name != "extract":
                t.join(timeout=120)
        
        self.shutdown_event.set()
        
        return {
            "extracted": self.metrics.extracted,
            "transformed": self.metrics.transformed,
            "loaded": self.metrics.loaded,
            "errors": self.metrics.errors,
        }
```

**Deadlock prevention guarantees:**
1. **Bounded queues + timeout on put/get** — no infinite blocking
2. **Single direction data flow** — no circular waits
3. **Shutdown event** — any thread can signal exit; all threads check periodically
4. **Poison pills** — explicit shutdown signal through queues
5. **No nested locks** — metrics use a single lock, no lock ordering needed

</details>

</article>

---

## Interview Tips

> **Tip 1:** For GIL questions, give the one-liner: "GIL prevents parallel CPU execution in threads, but is released during I/O." Then immediately apply it: "So for API calls I use threads, for transforms I use processes." Show you can apply the concept, not just recite it.

> **Tip 2:** For the mid-level API fetcher, emphasize partial failure tolerance: "A production pipeline should never fail completely because one API is down. I collect what I can and log what failed." This shows production mindset over academic correctness.

> **Tip 3:** For the senior pipeline design, draw the architecture first: "Extract threads → bounded queue → transform processes → bounded queue → load threads." Then explain deadlock prevention: timeouts, poison pills, one-directional flow. Interviewers want to see you think about failure modes before writing code.
