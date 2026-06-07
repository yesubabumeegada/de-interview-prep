---
title: "Python Data Structures - Intermediate"
topic: python
subtopic: data-structures
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, data-structures, collections, deque, heapq, ordered-dict]
---

# Python Data Structures — Intermediate Concepts

## The `collections` Module — Power Tools for DE

### defaultdict — Auto-Initializing Dictionary

```python
from collections import defaultdict

# Group records by partition key
events_by_date = defaultdict(list)
for event in event_stream:
    events_by_date[event["date"]].append(event)

# Count with defaultdict(int)
error_counts = defaultdict(int)
for log_line in log_file:
    if "ERROR" in log_line:
        error_counts[log_line.split(":")[0]] += 1

# Nested grouping (multi-level aggregation)
stats = defaultdict(lambda: defaultdict(int))
for record in sales_data:
    stats[record["region"]][record["product"]] += record["amount"]
# stats["US"]["Widget"] → total US Widget sales
```

### Counter — Frequency Analysis

```python
from collections import Counter

# Word frequency in log analysis
error_types = Counter(line.split(" ")[0] for line in error_logs)
print(error_types.most_common(5))
# [('TimeoutError', 342), ('ConnectionError', 218), ...]

# Counter arithmetic
morning_errors = Counter(morning_logs)
evening_errors = Counter(evening_logs)
total = morning_errors + evening_errors        # Combined counts
difference = evening_errors - morning_errors   # What increased

# Useful for data quality checks
column_nulls = Counter(
    col for row in dataset for col, val in row.items() if val is None
)
```

### deque — Double-Ended Queue

```python
from collections import deque

# Sliding window implementation (O(1) append/pop from both ends)
def sliding_window_max(data, window_size):
    """Efficient sliding window using deque."""
    window = deque(maxlen=window_size)
    results = []
    for value in data:
        window.append(value)
        if len(window) == window_size:
            results.append(max(window))
    return results

# Fixed-size buffer for recent events (auto-evicts oldest)
recent_events = deque(maxlen=1000)
for event in event_stream:
    recent_events.append(event)  # Oldest auto-removed when full

# BFS traversal (common in DAG/dependency resolution)
def topological_order(graph):
    """Process DAG tasks in dependency order."""
    in_degree = {node: 0 for node in graph}
    for node in graph:
        for neighbor in graph[node]:
            in_degree[neighbor] += 1
    
    queue = deque([n for n in in_degree if in_degree[n] == 0])
    order = []
    
    while queue:
        node = queue.popleft()  # O(1)
        order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    return order
```

### OrderedDict — Order-Preserving Dictionary

```python
from collections import OrderedDict

# LRU Cache implementation (common interview question)
class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity
    
    def get(self, key):
        if key not in self.cache:
            return -1
        self.cache.move_to_end(key)  # Mark as recently used
        return self.cache[key]
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)  # Remove least recently used

# Note: Since Python 3.7, regular dicts maintain insertion order.
# OrderedDict is still useful for .move_to_end() and .popitem(last=False)
```

## heapq — Priority Queue / Top-K Problems

```python
import heapq

# Find top-K largest records (O(n log k) — better than sorting for large n)
def top_k_records(records, k, key_func):
    """Efficiently find top K records without sorting entire dataset."""
    return heapq.nlargest(k, records, key=key_func)

# Usage: top 100 customers by revenue from millions of records
top_customers = heapq.nlargest(100, customers, key=lambda c: c["revenue"])

# Merge sorted iterators (perfect for merging sorted file chunks)
sorted_chunks = [
    iter([1, 4, 7, 10]),
    iter([2, 5, 8, 11]),
    iter([3, 6, 9, 12]),
]
merged = list(heapq.merge(*sorted_chunks))  # [1, 2, 3, 4, 5, ...]

# Priority-based task scheduling
task_queue = []
heapq.heappush(task_queue, (1, "critical_etl"))    # Priority 1 (highest)
heapq.heappush(task_queue, (3, "reporting"))        # Priority 3
heapq.heappush(task_queue, (2, "data_quality"))     # Priority 2

while task_queue:
    priority, task = heapq.heappop(task_queue)
    print(f"Running: {task} (priority={priority})")
```

## dataclasses — Structured Data (Python 3.7+)

```python
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

@dataclass
class PipelineConfig:
    """Typed, self-documenting configuration object."""
    source_table: str
    target_table: str
    batch_size: int = 10000
    retry_count: int = 3
    columns: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    
    @property
    def is_full_load(self) -> bool:
        return self.batch_size == 0

@dataclass(frozen=True)  # Immutable — can be used as dict key or in sets
class PartitionKey:
    """Immutable partition identifier."""
    date: str
    region: str
    
# Usage
config = PipelineConfig(
    source_table="raw.events",
    target_table="curated.events",
    columns=["user_id", "event_type", "timestamp"]
)

# Immutable keys for tracking processed partitions
processed = set()
key = PartitionKey(date="2024-01-15", region="us-east-1")
processed.add(key)  # Works because frozen=True makes it hashable
```

## Itertools — Memory-Efficient Iteration

```python
import itertools

# Chain multiple iterables (process multiple files as one stream)
def process_all_files(file_paths):
    """Stream records from multiple files without loading all into memory."""
    all_records = itertools.chain.from_iterable(
        read_records(path) for path in file_paths
    )
    for record in all_records:
        yield transform(record)

# Batching (chunking) for API calls or DB inserts
def batch(iterable, size):
    """Split iterable into fixed-size chunks."""
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

# Usage: Insert 1M records in batches of 10K
for batch_records in batch(all_records, 10000):
    db.bulk_insert(batch_records)

# Group consecutive items (useful for sessionization)
data = [("A", 1), ("A", 2), ("B", 3), ("B", 4), ("A", 5)]
for key, group in itertools.groupby(data, key=lambda x: x[0]):
    print(f"{key}: {list(group)}")
# A: [('A', 1), ('A', 2)]
# B: [('B', 3), ('B', 4)]
# A: [('A', 5)]

# Generate all column pair combinations (for correlation analysis)
columns = ["revenue", "users", "sessions", "bounce_rate"]
pairs = list(itertools.combinations(columns, 2))
# [('revenue', 'users'), ('revenue', 'sessions'), ...]
```

## Performance Comparison Table

| Operation | list | dict | set | deque |
|-----------|------|------|-----|-------|
| Append/Add | O(1)* | O(1)* | O(1)* | O(1) |
| Pop end | O(1) | - | - | O(1) |
| Pop front | O(n) | - | - | O(1) |
| Lookup by index | O(1) | - | - | O(n) |
| Lookup by value | O(n) | O(1) | O(1) | O(n) |
| Insert middle | O(n) | - | - | O(n) |
| Memory overhead | Low | High | High | Medium |

\* Amortized — occasional resizing is O(n)

## Interview Tip 💡

> For DE roles, the most impressive answer pattern is: "I'd use X because of its O(1) [operation] vs O(n) for [alternative], and since we're processing millions of records, that difference matters." Then follow up with the memory trade-off. For example: "A set gives O(1) membership testing but uses more memory than a sorted list with binary search. At our scale (100M records), the memory cost is worth the speed gain."
