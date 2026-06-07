---
title: "Python Generators - Intermediate"
topic: python
subtopic: generators
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, generators, yield-from, coroutines, itertools, pipelines]
---

# Python Generators — Intermediate Concepts

## yield from — Delegating to Sub-generators

`yield from` delegates iteration to another generator, creating composable pipelines:

```python
def read_file_lines(filepath: str):
    """Base generator — yields lines from one file."""
    with open(filepath, 'r') as f:
        yield from f  # Delegates to file iterator

def read_all_sources(filepaths: list[str]):
    """Composed generator — chains multiple file generators."""
    for path in filepaths:
        yield from read_file_lines(path)

# Single stream from many files — constant memory regardless of file count
for line in read_all_sources(["data_001.csv", "data_002.csv", "data_003.csv"]):
    process(line)
```

**The analogy:** `yield from` is like a manager delegating tasks. Instead of micromanaging each item, the manager says "hand everything from this sub-team directly to the requester."

### yield from with Return Values

```python
def accumulator():
    """Sub-generator that returns a final value."""
    total = 0
    while True:
        value = yield
        if value is None:
            return total  # This becomes the value of yield from
        total += value

def pipeline_counter():
    """Parent generator captures sub-generator return value."""
    total = yield from accumulator()
    print(f"Total processed: {total}")

gen = pipeline_counter()
next(gen)          # Prime the generator
gen.send(100)      # Send values to sub-generator
gen.send(200)
gen.send(None)     # Triggers return → "Total processed: 300"
```

---

## Generator Pipelines — Unix Pipes in Python

Chain generators like Unix pipes for composable data processing:

```python
import csv
from typing import Iterator, Dict

def read_csv_records(filepath: str) -> Iterator[Dict]:
    """Stage 1: Read raw records."""
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        yield from reader

def filter_active(records: Iterator[Dict]) -> Iterator[Dict]:
    """Stage 2: Filter to active records only."""
    for record in records:
        if record.get("status") == "active":
            yield record

def normalize_emails(records: Iterator[Dict]) -> Iterator[Dict]:
    """Stage 3: Normalize email field."""
    for record in records:
        record["email"] = record["email"].lower().strip()
        yield record

def add_metadata(records: Iterator[Dict], source: str) -> Iterator[Dict]:
    """Stage 4: Enrich with metadata."""
    for record in records:
        record["_source"] = source
        record["_ingested_at"] = "2024-01-15T10:00:00Z"
        yield record

# Compose the pipeline — nothing executes until iteration
pipeline = add_metadata(
    normalize_emails(
        filter_active(
            read_csv_records("users.csv")
        )
    ),
    source="user_system"
)

# Only NOW does data flow — one record at a time through all stages
for record in pipeline:
    load_to_warehouse(record)
```

The diagram below shows the composed pipeline as a chain of stages, where each generator pulls one record from the previous stage on demand rather than materializing intermediate lists.

```mermaid
flowchart LR
    A[CSV File] --> B[read_csv_records]
    B --> C[filter_active]
    C --> D[normalize_emails]
    D --> E[add_metadata]
    E --> F[load_to_warehouse]
    
    style A fill:#f9f9f9
    style F fill:#f9f9f9
```

---

## send() and throw() — Two-Way Communication

Generators can receive values via `send()` and handle exceptions via `throw()`:

```python
def rate_limited_processor(max_per_second: int):
    """
    Generator that accepts records and respects rate limits.
    Uses send() for two-way communication.
    """
    import time
    
    count = 0
    window_start = time.time()
    
    while True:
        # Receive a record via send()
        record = yield f"processed_{count}"
        
        if record is None:
            break
        
        # Rate limiting logic
        count += 1
        if count >= max_per_second:
            elapsed = time.time() - window_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            count = 0
            window_start = time.time()
        
        # Process the record
        transform_and_load(record)

# Usage
processor = rate_limited_processor(max_per_second=100)
next(processor)  # Prime the generator

for record in source_records:
    status = processor.send(record)  # Send data IN, get status OUT
    log_status(status)

processor.close()  # Cleanup
```

### throw() for Error Injection

```python
def resilient_consumer():
    """Generator that handles injected exceptions."""
    while True:
        try:
            value = yield
            process(value)
        except ValueError as e:
            # Handle bad records without stopping
            log_bad_record(value, e)
            continue
        except GeneratorExit:
            # Cleanup when close() is called
            flush_buffers()
            return

consumer = resilient_consumer()
next(consumer)

consumer.send(good_record)
consumer.throw(ValueError, "Bad format")  # Inject error, generator continues
consumer.send(another_good_record)
consumer.close()  # Triggers GeneratorExit
```

---

## Coroutines Basics — Generator-Based Concurrency

Before `async/await`, coroutines were built with generators:

```python
def data_sink(destination: str):
    """
    Coroutine pattern — receives data and batches writes.
    This is the foundation that async/await was built on.
    """
    buffer = []
    buffer_size = 1000
    
    try:
        while True:
            record = yield  # Pause and wait for data
            buffer.append(record)
            
            if len(buffer) >= buffer_size:
                flush_to_destination(buffer, destination)
                buffer = []
    except GeneratorExit:
        # Final flush on close
        if buffer:
            flush_to_destination(buffer, destination)

# Usage — push-based pipeline
sink = data_sink("s3://bucket/output/")
next(sink)  # Prime

for record in extract_records():
    sink.send(record)  # Push records into the sink

sink.close()  # Trigger final flush
```

---

## itertools Integration — Power Combinations

```python
import itertools
from typing import Iterator, TypeVar

T = TypeVar('T')

def chunked(iterable: Iterator[T], size: int) -> Iterator[list[T]]:
    """Batch items into fixed-size chunks. Last chunk may be smaller."""
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

def windowed(iterable: Iterator[T], size: int) -> Iterator[tuple]:
    """Sliding window over a stream."""
    it = iter(iterable)
    window = tuple(itertools.islice(it, size))
    if len(window) == size:
        yield window
    for item in it:
        window = window[1:] + (item,)
        yield window

def roundrobin(*iterables):
    """Interleave multiple sources (useful for fair multi-source ingestion)."""
    iterators = [iter(it) for it in iterables]
    while iterators:
        next_iterators = []
        for it in iterators:
            try:
                yield next(it)
            except StopIteration:
                continue
            else:
                next_iterators.append(it)
        iterators = next_iterators

# Usage: Fair reading from multiple Kafka partitions
partitions = [read_partition(i) for i in range(8)]
for record in roundrobin(*partitions):
    process(record)

# Chunked inserts — batch 10K records per DB transaction
for batch in chunked(extract_all_records(), size=10_000):
    bulk_insert(batch)
```

### itertools.tee — Multiple Consumers from One Generator

```python
import itertools

def fork_stream(source: Iterator, num_consumers: int = 2):
    """
    WARNING: tee buffers records if consumers advance at different rates.
    Only use when consumers process at similar speeds.
    """
    streams = itertools.tee(source, num_consumers)
    return streams

# Split one source into validation stream and load stream
source = extract_records()
validation_stream, load_stream = itertools.tee(source, 2)

# Process both (careful — this buffers in memory!)
bad_records = list(validate(validation_stream))
load_count = load_records(load_stream)
```

---

## Generator Memory Model

The comparison below contrasts the two execution models: a generator pipeline keeps only a single record in memory at each stage, while a list-based pipeline materializes the full dataset at every step, multiplying memory use.

```mermaid
flowchart TD
    subgraph "Generator Pipeline Memory"
        A[Source: 10M records on disk] --> B[Generator 1: 1 record in memory]
        B --> C[Generator 2: 1 record in memory]
        C --> D[Generator 3: 1 record in memory]
        D --> E[Output: 1 record at a time]
    end
    
    subgraph "List Pipeline Memory"
        F[Source: 10M records on disk] --> G[List 1: 10M records in RAM]
        G --> H[List 2: 10M records in RAM]
        H --> I[List 3: 10M records in RAM]
        I --> J[Output: all at once]
    end
```

---

## Interview Tips

> **Tip 1:** When asked about processing large files, lead with generators as your memory management strategy. Say: "I'd use a generator pipeline where each stage holds at most one record in memory, giving O(1) memory complexity regardless of input size." This immediately signals you think about production constraints.

> **Tip 2:** Know when NOT to use generators — if you need random access, multiple passes over data, or the dataset fits comfortably in memory, a list is simpler and faster (no generator frame overhead). The key tradeoff is memory vs. simplicity.

> **Tip 3:** `yield from` is your answer for composability. When the interviewer asks how you'd combine multiple data sources into a single stream, or how you'd refactor nested generator logic, `yield from` shows you know modern Python patterns beyond basic iteration.
