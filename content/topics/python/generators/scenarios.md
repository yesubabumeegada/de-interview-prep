---
title: "Python Generators - Scenario Questions"
topic: python
subtopic: generators
content_type: scenario_question
tags: [python, generators, interview, scenarios, memory-efficient]
---

# Scenario Questions — Python Generators

<article data-difficulty="junior">

## 🟢 Junior: Process a File Too Large for Memory

**Scenario:** You have a 50 GB CSV log file. You need to count how many rows have `status = "ERROR"`. The file is too large to load into memory with `pd.read_csv()`. Write a memory-efficient solution using generators.

<details>
<summary>✅ Solution</summary>

```python
def read_csv_rows(filepath):
    """Generator: yields one dict per row, constant memory."""
    with open(filepath, 'r') as f:
        header = f.readline().strip().split(',')
        for line in f:
            values = line.strip().split(',')
            yield dict(zip(header, values))

# Count errors with O(1) memory regardless of file size
error_count = sum(1 for row in read_csv_rows('server_logs.csv') if row['status'] == 'ERROR')
print(f"Error count: {error_count}")

# Alternative: generator expression directly on file
with open('server_logs.csv') as f:
    header = f.readline()
    error_count = sum(1 for line in f if ',ERROR,' in line)
```

**Why this works:** The file is never fully loaded. Each line is read, checked, and discarded. Peak memory: ~1 KB (one line at a time), regardless of whether the file is 50 GB or 500 GB.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement a Streaming Data Pipeline

**Scenario:** Build a mini-pipeline using generators: read records from a source, filter invalid ones, transform the valid ones, and batch them for database insertion. Each stage should be a generator that chains to the next. Total records: 10 million. Memory limit: 500 MB.

<details>
<summary>✅ Solution</summary>

```python
from typing import Iterator, Generator
import json

# Stage 1: Source (reads from file/API lazily)
def read_source(filepath: str) -> Generator[dict, None, None]:
    """Yields one record at a time from a large JSON-lines file."""
    with open(filepath) as f:
        for line in f:
            yield json.loads(line)

# Stage 2: Filter (removes invalid records)
def filter_valid(records: Iterator[dict]) -> Generator[dict, None, None]:
    """Pass through only records with required fields."""
    for record in records:
        if (record.get('user_id') and 
            record.get('amount') and 
            float(record['amount']) > 0):
            yield record

# Stage 3: Transform (enrich/modify each record)
def transform(records: Iterator[dict]) -> Generator[dict, None, None]:
    """Apply business transformations."""
    for record in records:
        yield {
            'user_id': record['user_id'],
            'amount_cents': int(float(record['amount']) * 100),
            'region': record.get('region', 'UNKNOWN').upper(),
            'processed': True,
        }

# Stage 4: Batch (group into chunks for DB insertion)
def batch(records: Iterator[dict], size: int = 5000) -> Generator[list, None, None]:
    """Yield lists of N records for bulk operations."""
    chunk = []
    for record in records:
        chunk.append(record)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

# Compose the pipeline (lazy — nothing executes until you iterate)
def run_pipeline(input_file: str, batch_size: int = 5000):
    source = read_source(input_file)         # Generator
    valid = filter_valid(source)             # Generator wrapping generator
    transformed = transform(valid)           # Generator wrapping generator
    batches = batch(transformed, batch_size) # Generator wrapping generator
    
    total_loaded = 0
    for chunk in batches:
        db.bulk_insert('fact_events', chunk)  # Insert 5000 at a time
        total_loaded += len(chunk)
        if total_loaded % 100000 == 0:
            print(f"Loaded {total_loaded:,} records...")
    
    print(f"Pipeline complete: {total_loaded:,} records loaded")

run_pipeline('events_10M.jsonl')
# Memory usage: ~50 MB (one batch of 5000 dicts in memory at peak)
# NOT 10M records (~4 GB) in memory!
```

**Key insight:** Each generator stage processes ONE record and passes it to the next stage before requesting the next input. The entire 10M-record pipeline runs in constant memory because records flow through one at a time.

**Pipeline flow for a single record:**
```
File line → json.loads → filter check → transform → accumulate in batch
→ batch full → bulk_insert → batch cleared → next records flow in
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Implement a Parallel Generator Pipeline

**Scenario:** Your single-threaded generator pipeline processes 10M records in 2 hours. The bottleneck is the `transform` stage (CPU-bound: 1ms per record). The `read` and `write` stages are I/O-bound. Design a solution that parallelizes the transform stage while keeping the generator-based memory efficiency.

<details>
<summary>✅ Solution</summary>

```python
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Iterator, Generator
import multiprocessing as mp

def read_source(filepath: str) -> Generator[dict, None, None]:
    """I/O-bound: yields records lazily."""
    with open(filepath) as f:
        for line in f:
            yield json.loads(line)

def transform_record(record: dict) -> dict:
    """CPU-bound: 1ms per record (the bottleneck)."""
    # Complex business logic, regex, calculations
    return {
        'user_id': record['user_id'],
        'score': expensive_calculation(record),
        'category': classify(record),
    }

def _transform_batch(batch: list[dict]) -> list[dict]:
    """Module-level function (required for pickling in multiprocessing)."""
    return [transform_record(r) for r in batch]

def parallel_transform(records: Iterator[dict], workers: int = 8, 
                       chunk_size: int = 1000) -> Generator[dict, None, None]:
    """
    Parallelize CPU-bound transforms using process pool.
    Maintains generator semantics (yields one record at a time).
    """
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit chunks of records in parallel
        buffer = []
        futures = []
        
        for record in records:
            buffer.append(record)
            
            if len(buffer) >= chunk_size:
                # Submit chunk for parallel processing
                # Note: must use a module-level function (lambdas can't be pickled!)
                future = executor.submit(_transform_batch, buffer.copy())
                futures.append(future)
                buffer = []
                
                # Yield completed results (maintain back-pressure)
                while futures and futures[0].done():
                    for result in futures.pop(0).result():
                        yield result
        
        # Submit remaining buffer
        if buffer:
            future = executor.submit(_transform_batch, buffer)
            futures.append(future)
        
        # Drain all remaining futures
        for future in futures:
            for result in future.result():
                yield result

def batch_and_write(records: Iterator[dict], batch_size: int = 5000):
    """Batch results and write to destination."""
    chunk = []
    for record in records:
        chunk.append(record)
        if len(chunk) >= batch_size:
            db.bulk_insert('output_table', chunk)
            chunk = []
    if chunk:
        db.bulk_insert('output_table', chunk)

# Run the parallel pipeline
source = read_source('events_10M.jsonl')
transformed = parallel_transform(source, workers=8, chunk_size=1000)
batch_and_write(transformed, batch_size=5000)

# Performance:
# Before: 10M × 1ms = 10,000 seconds = 2.8 hours (single-threaded)
# After: 10M × 1ms / 8 workers = 1,250 seconds = 21 minutes (8x speedup)
# Memory: still bounded (~8 chunks × 1000 records in flight = ~8K records max)
```

**Design decisions:**
- **ProcessPoolExecutor** (not threads): CPU-bound work needs multiple processes to bypass GIL
- **Chunk-based submission:** Amortizes process communication overhead (don't submit 1 record at a time)
- **Back-pressure via yield:** Only process as fast as the consumer can write (prevents unbounded memory growth)
- **Generator semantics maintained:** Downstream code sees a simple iterator of records

</details>

</article>
