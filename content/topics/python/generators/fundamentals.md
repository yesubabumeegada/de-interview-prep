---
title: "Python Generators - Fundamentals"
topic: python
subtopic: generators
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, generators, yield, lazy-evaluation, memory-efficient, iterators]
---

# Python Generators — Fundamentals

## What Are Generators?

A generator is a function that produces a sequence of values **one at a time** using `yield` instead of `return`. It doesn't hold the entire sequence in memory — it computes each value only when requested.

**The analogy:** A list is like a printed book (all pages exist at once, uses memory for every page). A generator is like a storyteller (produces one sentence at a time, only keeps the current sentence in memory).

> **Why generators matter for DE:** When processing millions of records, loading everything into memory crashes your job. Generators let you process one record (or one chunk) at a time with O(1) memory regardless of data size.

---

## Generator Function vs Normal Function

```python
# Normal function: builds entire list in memory
def get_all_records(n):
    results = []
    for i in range(n):
        results.append(f"record_{i}")
    return results  # Returns the WHOLE list at once

# Generator function: yields one item at a time
def get_records_lazy(n):
    for i in range(n):
        yield f"record_{i}"  # Produces one item, pauses, waits for next request

# Usage comparison:
all_records = get_all_records(10_000_000)     # 10M strings in memory (~800 MB!)
lazy_records = get_records_lazy(10_000_000)   # Almost zero memory (just the generator state)

# Process one at a time
for record in lazy_records:
    process(record)  # Only ONE record in memory at any time
```

---

## How yield Works

```python
def countdown(n):
    print("Starting countdown")
    while n > 0:
        yield n        # Pauses here, returns n to the caller
        n -= 1         # Resumes here on next() call
    print("Done!")

gen = countdown(3)          # Nothing executes yet! Just creates the generator object
print(next(gen))            # "Starting countdown" → yields 3
print(next(gen))            # Resumes → yields 2
print(next(gen))            # Resumes → yields 1
# next(gen) → "Done!" then raises StopIteration
```

**Execution flow:**
1. `countdown(3)` creates a generator but does NOT execute the function body
2. `next(gen)` runs until the first `yield`, returns the yielded value, then pauses
3. Each subsequent `next()` resumes from where it paused
4. When the function ends (or returns), `StopIteration` is raised

---

## Generator Expressions (One-Liners)

Like list comprehensions but with parentheses instead of brackets:

```python
# List comprehension: builds entire list in memory
squares_list = [x**2 for x in range(1_000_000)]  # ~8 MB in memory

# Generator expression: produces values lazily
squares_gen = (x**2 for x in range(1_000_000))   # ~100 bytes in memory!

# Use in functions that accept iterables
total = sum(x**2 for x in range(1_000_000))  # No extra memory for the list
largest = max(x**2 for x in range(1_000_000))
```

---

## DE Use Cases for Generators

### 1. Reading Large Files Line by Line

```python
def read_large_csv(filepath):
    """Process a 50 GB CSV without loading it all into memory."""
    with open(filepath, 'r') as f:
        header = f.readline().strip().split(',')
        for line in f:
            values = line.strip().split(',')
            yield dict(zip(header, values))

# Process 50 GB file with constant memory (~1 KB at a time)
for record in read_large_csv('/data/huge_file.csv'):
    if record['amount'] and float(record['amount']) > 1000:
        write_to_output(record)
```

### 2. Batching Records for Database Inserts

```python
def batch(iterable, size=1000):
    """Split any iterable into fixed-size chunks."""
    batch_items = []
    for item in iterable:
        batch_items.append(item)
        if len(batch_items) == size:
            yield batch_items
            batch_items = []
    if batch_items:  # Don't forget the last partial batch!
        yield batch_items

# Usage: insert 5M records in batches of 10K
records = read_large_csv('orders.csv')  # Generator — lazy
for chunk in batch(records, size=10000):
    db.bulk_insert('orders', chunk)  # Insert 10K at a time
    print(f"Inserted batch of {len(chunk)}")
```

### 3. Chaining Multiple Data Sources

```python
import itertools

def read_source_a():
    """Read from source A (10M records)."""
    for record in query_database_a():
        yield transform_a(record)

def read_source_b():
    """Read from source B (5M records)."""
    for record in query_api_b():
        yield transform_b(record)

# Process both sources as one seamless stream
all_records = itertools.chain(read_source_a(), read_source_b())
# 15M records, but only ONE in memory at any time!

for record in all_records:
    write_to_data_lake(record)
```

### 4. Paginated API Fetching

```python
def fetch_all_pages(api_url, page_size=100):
    """Fetch all pages from a paginated API lazily."""
    page = 1
    while True:
        response = requests.get(api_url, params={'page': page, 'size': page_size})
        data = response.json()
        
        if not data['results']:
            break  # No more pages
        
        for item in data['results']:
            yield item
        
        page += 1

# Seamlessly iterate through all pages
for user in fetch_all_pages('https://api.example.com/users'):
    process_user(user)
# The consumer doesn't know or care about pagination
```

---

## Memory Comparison

```python
import sys

# List: stores ALL items
data_list = [i for i in range(1_000_000)]
print(f"List memory: {sys.getsizeof(data_list):,} bytes")  # ~8,448,728 bytes (~8 MB)

# Generator: stores only the state
data_gen = (i for i in range(1_000_000))
print(f"Generator memory: {sys.getsizeof(data_gen):,} bytes")  # 112 bytes!
```

| Data Size | List Memory | Generator Memory |
|-----------|-------------|-----------------|
| 1K items | 8 KB | 112 bytes |
| 1M items | 8 MB | 112 bytes |
| 100M items | 800 MB | 112 bytes |
| 1B items | 8 GB (OOM!) | 112 bytes |

---

## Generator Pitfalls

| Pitfall | Problem | Fix |
|---------|---------|-----|
| Can only iterate once | Second `for` loop gets nothing | Recreate the generator or use `itertools.tee` |
| No random access | Can't do `gen[5]` | Convert to list if you need indexing |
| No `len()` | Can't check size without consuming | Count separately or use a different structure |
| Debugging is harder | Can't print all values easily | Use `list(gen)` for small samples during debugging |

```python
gen = (x for x in range(5))
list(gen)  # [0, 1, 2, 3, 4]
list(gen)  # [] ← EMPTY! Generator is exhausted after first pass

# Fix: recreate the generator
def make_gen():
    return (x for x in range(5))

list(make_gen())  # [0, 1, 2, 3, 4]
list(make_gen())  # [0, 1, 2, 3, 4] ← Fresh generator each time
```

---

## Interview Tips

> **Tip 1:** "When would you use a generator instead of a list?" — "Whenever the data is large or potentially infinite. If processing 100M records, a list would use 8+ GB of memory and crash. A generator processes one record at a time with constant memory. I use generators for reading large files, paginated API calls, and streaming data between pipeline stages."

> **Tip 2:** "Explain `yield` vs `return`" — "`return` terminates the function and sends back one value. `yield` pauses the function, sends back a value, and remembers where it left off. On the next `next()` call, it resumes from after the `yield`. A function with `yield` becomes a generator — it can produce many values over time."

> **Tip 3:** "Write a batch function using generators" — This is a common interview question. The pattern: accumulate items into a list, yield the list when it reaches the batch size, clear and continue. Don't forget to yield the final partial batch at the end.
