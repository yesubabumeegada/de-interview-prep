---
title: "Python Data Structures - Scenario Questions"
topic: python
subtopic: data-structures
content_type: scenario_question
tags: [python, data-structures, interview, scenarios, collections]
---

# Scenario Questions — Python Data Structures

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Deduplicate While Preserving Order

**Scenario:** You receive a stream of event records (list of dicts) with duplicates due to at-least-once delivery. Deduplicate based on a composite key (`user_id` + `event_id`) while preserving the original order.

<details>
<summary>💡 Hint</summary>
Sets give O(1) lookup but don't preserve order. Combine a set (tracking seen keys) with a list (building the ordered result).
</details>

<details>
<summary>✅ Solution</summary>

```python
def deduplicate_events(events: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for event in events:
        key = (event["user_id"], event["event_id"])
        if key not in seen:
            seen.add(key)
            unique.append(event)
    return unique
```

**Explanation:**
- `set` lookup is O(1) → total dedup is O(n)
- Tuples are hashable (required for set membership)
- Order preserved by iterating original list and appending first-seen only
- This is the standard in-memory dedup pattern for streaming micro-batches

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Fixed-Size Buffer with Auto-Eviction

**Scenario:** Your pipeline receives streaming metrics. Keep only the last N records for computing a moving average. When the buffer is full, the oldest entry should auto-evict.

<details>
<summary>✅ Solution</summary>

```python
from collections import deque

class MetricsBuffer:
    def __init__(self, max_size: int):
        self._buffer = deque(maxlen=max_size)
    
    def add(self, value: float) -> None:
        self._buffer.append(value)  # Oldest auto-evicted when full
    
    def moving_average(self) -> float:
        return sum(self._buffer) / len(self._buffer) if self._buffer else 0.0

buffer = MetricsBuffer(max_size=5)
for v in [10, 20, 30, 40, 50, 60, 70]:
    buffer.add(v)
print(buffer.moving_average())  # 50.0 (average of [30,40,50,60,70])
```

**Explanation:**
- `deque(maxlen=N)` provides O(1) append with automatic eviction of oldest element
- Much better than `list` + manual slicing (which is O(n) per eviction)
- Perfect for sliding window calculations in streaming pipelines

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Choose the Right Structure — 1M ID Lookups

**Scenario:** You need to check if each of 10M incoming records has a `user_id` that exists in a set of 1M known users. Which data structure should store the 1M known IDs, and why?

<details>
<summary>✅ Solution</summary>

```python
# BAD: list — O(n) per lookup × 10M records = O(10M × 1M) = 10 TRILLION operations!
known_users = [1, 2, 3, ...]  # list
if user_id in known_users:    # O(1M) per check — painfully slow

# GOOD: set — O(1) per lookup × 10M records = O(10M) operations
known_users = {1, 2, 3, ...}  # set
if user_id in known_users:    # O(1) per check — instant
```

**Explanation:**
- `set` uses a hash table → O(1) average membership test
- `list` must scan linearly → O(n) per check
- For 1M elements × 10M checks: set = 10M ops (seconds), list = 10T ops (hours/days)
- Rule: if you're checking "is X in this collection?" → use a `set`

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Group Records by Key

**Scenario:** Given a list of order records, group them by `region` so you can process each region's orders separately. Do it in one pass without manual key-checking.

<details>
<summary>✅ Solution</summary>

```python
from collections import defaultdict

orders = [
    {"id": 1, "region": "US", "amount": 100},
    {"id": 2, "region": "EU", "amount": 200},
    {"id": 3, "region": "US", "amount": 150},
    {"id": 4, "region": "EU", "amount": 300},
]

by_region = defaultdict(list)
for order in orders:
    by_region[order["region"]].append(order)

# Result: {"US": [{id:1,...}, {id:3,...}], "EU": [{id:2,...}, {id:4,...}]}
for region, region_orders in by_region.items():
    process_region(region, region_orders)
```

**Explanation:**
- `defaultdict(list)` auto-creates empty list for new keys (no `if key not in` boilerplate)
- Single pass: O(n) time, O(n) space
- This is the pure-Python equivalent of SQL's GROUP BY or PySpark's groupBy

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Count Frequencies and Find Top-10

**Scenario:** Given a list of 10M log lines, count how many times each error type appears and return the top 10 most common errors.

<details>
<summary>✅ Solution</summary>

```python
from collections import Counter

error_types = [extract_error_type(line) for line in log_lines]
counts = Counter(error_types)

# Top 10 most common
top_10 = counts.most_common(10)
# [('TimeoutError', 50234), ('ConnectionError', 32100), ...]

# Counter also supports arithmetic:
morning_errors = Counter(morning_logs)
afternoon_errors = Counter(afternoon_logs)
new_in_afternoon = afternoon_errors - morning_errors  # Errors that appeared/increased
```

**Explanation:**
- `Counter` is optimized for counting (implemented in C under the hood)
- `most_common(n)` uses a heap internally → O(n log k) for top-k
- Counter arithmetic (`+`, `-`, `&`, `|`) is powerful for comparing distributions

</details>
</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Merge K Sorted Files with Constant Memory

**Scenario:** You have K sorted CSV files (each too large to fit in memory together). Merge them into a single sorted output. Memory usage must be O(K) regardless of total data size.

<details>
<summary>✅ Solution</summary>

```python
import heapq
import csv

def read_sorted_file(filepath):
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def merge_sorted_files(filepaths, sort_key, output_path):
    iterators = [
        ((row[sort_key], i, row) for row in read_sorted_file(fp))
        for i, fp in enumerate(filepaths)
    ]
    
    with open(output_path, 'w', newline='') as out:
        writer = None
        for sort_val, _, row in heapq.merge(*iterators):
            if writer is None:
                writer = csv.DictWriter(out, fieldnames=row.keys())
                writer.writeheader()
            writer.writerow(row)

merge_sorted_files(['chunk1.csv', 'chunk2.csv', 'chunk3.csv'], 
                   sort_key='timestamp', output_path='merged.csv')
```

**Explanation:**
- `heapq.merge(*iterators)` merges K sorted iterables using a min-heap of size K
- Memory: O(K) — only K elements in the heap at any time (one per file)
- Each file is read lazily (generator) — never loaded fully into memory
- This is how Spark/Hadoop handle the merge phase of external sort

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: LRU Cache with OrderedDict

**Scenario:** Implement an LRU (Least Recently Used) cache with O(1) get and put operations. When the cache reaches capacity, evict the least recently accessed item.

<details>
<summary>✅ Solution</summary>

```python
from collections import OrderedDict

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
            self.cache.popitem(last=False)  # Remove oldest (least recently used)

cache = LRUCache(3)
cache.put("a", 1)
cache.put("b", 2)
cache.put("c", 3)
cache.get("a")      # Returns 1, moves "a" to end
cache.put("d", 4)   # Evicts "b" (least recently used)
```

**Explanation:**
- `OrderedDict` maintains insertion order + provides O(1) `move_to_end` and `popitem`
- `get()`: move accessed key to end (most recent)
- `put()`: add/move key to end, evict from front if over capacity
- This is a classic interview question AND used in real caching systems (Redis LRU)

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Batch Iterator for Database Inserts

**Scenario:** Write a reusable `batch()` function that splits any iterable into fixed-size chunks for bulk database inserts. Must work with generators (lazy, no `len()`).

<details>
<summary>✅ Solution</summary>

```python
from typing import Iterator, TypeVar
import itertools

T = TypeVar('T')

def batch(iterable: Iterator[T], size: int) -> Iterator[list[T]]:
    """Split any iterable into fixed-size chunks."""
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

# Usage: insert 5M records in batches of 10K
records = read_large_csv('orders.csv')  # Generator (lazy)
for chunk in batch(records, size=10000):
    db.bulk_insert('orders', chunk)
    print(f"Inserted {len(chunk)} records")
```

**Explanation:**
- `itertools.islice` takes exactly N items from the iterator without loading all into memory
- Works with generators, files, database cursors — any iterable
- The outer loop yields `list[T]` chunks that can be passed to bulk insert APIs
- This is the standard batching pattern for ETL pipelines

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Priority Task Queue

**Scenario:** Design a task scheduler where tasks have priorities. Higher priority tasks execute first. Tasks with the same priority execute in FIFO order.

<details>
<summary>✅ Solution</summary>

```python
import heapq
from dataclasses import dataclass, field

@dataclass(order=True)
class PriorityTask:
    priority: int                           # Lower number = higher priority
    sequence: int = field(compare=True)     # FIFO tiebreaker
    name: str = field(compare=False)
    payload: dict = field(compare=False, default_factory=dict)

class TaskQueue:
    def __init__(self):
        self._heap = []
        self._counter = 0
    
    def push(self, name: str, priority: int, payload: dict = None):
        task = PriorityTask(priority, self._counter, name, payload or {})
        heapq.heappush(self._heap, task)
        self._counter += 1
    
    def pop(self) -> PriorityTask:
        return heapq.heappop(self._heap)
    
    def __len__(self):
        return len(self._heap)

queue = TaskQueue()
queue.push("low-priority-report", priority=3)
queue.push("critical-etl", priority=1)
queue.push("medium-alert", priority=2)
queue.push("another-critical", priority=1)  # Same priority → FIFO with "critical-etl"

print(queue.pop().name)  # "critical-etl" (priority 1, first inserted)
print(queue.pop().name)  # "another-critical" (priority 1, second inserted)
```

**Explanation:**
- `heapq` provides O(log n) push/pop for a min-heap
- `sequence` counter ensures FIFO order for same-priority tasks
- `@dataclass(order=True)` auto-generates comparison based on field order (priority first, then sequence)
- This pattern is used in task schedulers, job queues, and Airflow-like systems

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Schema-Aware Type Coercion

**Scenario:** Your pipeline receives JSON records with inconsistent types (numbers as strings, booleans as "true"/"false"). Design a schema-aware coercion system using dataclasses and dicts.

<details>
<summary>✅ Solution</summary>

```python
from dataclasses import dataclass, field
from typing import Any, Callable

COERCERS: dict[str, Callable] = {
    "int": lambda v: int(float(v)) if v not in (None, "", "null") else None,
    "float": lambda v: float(v) if v not in (None, "", "null") else None,
    "bool": lambda v: str(v).lower() in ("true", "1", "yes") if v is not None else None,
    "str": lambda v: str(v).strip() if v is not None else None,
}

@dataclass
class FieldSchema:
    name: str
    target_type: str
    nullable: bool = True

def coerce_record(record: dict, schema: list[FieldSchema]) -> dict:
    result = {}
    for field_def in schema:
        raw = record.get(field_def.name)
        if raw is None and not field_def.nullable:
            raise ValueError(f"Non-nullable field '{field_def.name}' is None")
        coercer = COERCERS.get(field_def.target_type, lambda v: v)
        result[field_def.name] = coercer(raw) if raw is not None else None
    return result

# Usage
schema = [
    FieldSchema("user_id", "int", nullable=False),
    FieldSchema("amount", "float"),
    FieldSchema("is_premium", "bool"),
]
raw = {"user_id": "12345", "amount": "99.5", "is_premium": "true"}
clean = coerce_record(raw, schema)
# {"user_id": 12345, "amount": 99.5, "is_premium": True}
```

**Explanation:**
- Schema-driven: adding a new field is just adding a FieldSchema entry (no code change)
- Handles edge cases: empty strings, "null" strings, numeric strings
- Raises on non-nullable violations (fail fast, don't silently corrupt data)
- This is a lightweight version of what Pydantic/Great Expectations do

</details>
</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Bloom Filter for Pipeline Deduplication

**Scenario:** Your pipeline processes 500M events/day. Implement a Bloom filter from scratch to skip obviously-duplicate records before the expensive DB lookup. Must use <2 GB memory for 500M items at 0.1% false positive rate.

<details>
<summary>✅ Solution</summary>

```python
import hashlib
import math

class BloomFilter:
    def __init__(self, expected_items: int, fp_rate: float = 0.001):
        self.size = int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        self.hash_count = int((self.size / expected_items) * math.log(2))
        self.bit_array = bytearray(math.ceil(self.size / 8))
    
    def _hashes(self, item: str) -> list[int]:
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self.size for i in range(self.hash_count)]
    
    def add(self, item: str):
        for pos in self._hashes(item):
            self.bit_array[pos // 8] |= (1 << (pos % 8))
    
    def might_contain(self, item: str) -> bool:
        return all(self.bit_array[pos // 8] & (1 << (pos % 8)) for pos in self._hashes(item))
    
    @property
    def memory_mb(self) -> float:
        return len(self.bit_array) / (1024 * 1024)

bloom = BloomFilter(expected_items=500_000_000, fp_rate=0.001)
print(f"Memory: {bloom.memory_mb:.0f} MB")  # ~860 MB

def process_event(event, bloom, db):
    key = f"{event['source']}:{event['event_id']}"
    if bloom.might_contain(key):
        if db.exists(key):
            return False  # Confirmed duplicate
    bloom.add(key)
    db.insert(event)
    return True
```

**Explanation:**
- Bloom filter: O(k) add/check, O(1) memory per item (bits, not full objects)
- 500M items at 0.1% FP rate = ~860 MB (vs 40+ GB for a full set of 500M strings)
- "Definitely not seen" = skip DB lookup (saves ~90% of expensive DB calls)
- "Probably seen" = do the DB lookup to confirm (0.1% false positive rate = rare)

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: DAG Dependency Resolver

**Scenario:** Build a mini-orchestrator: given task definitions with dependencies, implement topological sort, cycle detection, and parallel execution groups.

<details>
<summary>✅ Solution</summary>

```python
from collections import defaultdict, deque

class DAGResolver:
    def __init__(self, tasks: dict[str, list[str]]):
        """tasks: {task_name: [dependency_names]}"""
        self.graph = defaultdict(set)
        self.in_degree = defaultdict(int)
        self.all_nodes = set()
        
        for task, deps in tasks.items():
            self.all_nodes.add(task)
            for dep in deps:
                self.all_nodes.add(dep)
                self.graph[dep].add(task)
                self.in_degree[task] += 1
            self.in_degree.setdefault(task, self.in_degree.get(task, 0))
    
    def detect_cycle(self) -> bool:
        order = self.topological_sort()
        return order is None
    
    def topological_sort(self) -> list[str] | None:
        queue = deque(n for n in self.all_nodes if self.in_degree.get(n, 0) == 0)
        order, remaining = [], dict(self.in_degree)
        while queue:
            node = queue.popleft()
            order.append(node)
            for child in self.graph[node]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    queue.append(child)
        return order if len(order) == len(self.all_nodes) else None  # None = cycle
    
    def parallel_groups(self) -> list[list[str]]:
        groups, resolved, remaining = [], set(), dict(self.in_degree)
        while len(resolved) < len(self.all_nodes):
            group = [n for n in self.all_nodes if n not in resolved and remaining.get(n, 0) == 0]
            if not group:
                raise ValueError("Cycle detected")
            groups.append(sorted(group))
            for node in group:
                resolved.add(node)
                for child in self.graph[node]:
                    remaining[child] -= 1
        return groups

tasks = {
    "extract_a": [], "extract_b": [],
    "transform": ["extract_a", "extract_b"],
    "load": ["transform"],
    "notify": ["load"],
}
resolver = DAGResolver(tasks)
print(resolver.parallel_groups())
# [['extract_a', 'extract_b'], ['transform'], ['load'], ['notify']]
```

**Explanation:**
- Kahn's algorithm: BFS from zero-in-degree nodes
- Cycle detection: if sorted order length < total nodes → cycle exists
- Parallel groups: nodes with zero remaining dependencies can run simultaneously
- This is how Airflow resolves DAG execution order

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Sliding Window Aggregator

**Scenario:** Implement a memory-efficient sliding window that computes sum, min, max, and count over the last N elements in a stream. All operations must be O(1) amortized.

<details>
<summary>✅ Solution</summary>

```python
from collections import deque

class SlidingWindowAggregator:
    def __init__(self, window_size: int):
        self.window_size = window_size
        self.buffer = deque(maxlen=window_size)
        self.total = 0.0
        self._min_deque = deque()  # Monotonic min queue
        self._max_deque = deque()  # Monotonic max queue
    
    def add(self, value: float):
        # Handle eviction of oldest element
        if len(self.buffer) == self.window_size:
            evicted = self.buffer[0]
            self.total -= evicted
            if self._min_deque and self._min_deque[0] == evicted:
                self._min_deque.popleft()
            if self._max_deque and self._max_deque[0] == evicted:
                self._max_deque.popleft()
        
        self.buffer.append(value)
        self.total += value
        
        # Maintain monotonic min deque
        while self._min_deque and self._min_deque[-1] > value:
            self._min_deque.pop()
        self._min_deque.append(value)
        
        # Maintain monotonic max deque
        while self._max_deque and self._max_deque[-1] < value:
            self._max_deque.pop()
        self._max_deque.append(value)
    
    @property
    def sum(self) -> float: return self.total
    @property
    def count(self) -> int: return len(self.buffer)
    @property
    def avg(self) -> float: return self.total / len(self.buffer) if self.buffer else 0
    @property
    def min(self) -> float: return self._min_deque[0] if self._min_deque else float('inf')
    @property
    def max(self) -> float: return self._max_deque[0] if self._max_deque else float('-inf')

window = SlidingWindowAggregator(window_size=5)
for v in [10, 20, 30, 40, 50, 60]:
    window.add(v)
print(f"sum={window.sum}, min={window.min}, max={window.max}, avg={window.avg}")
# sum=200, min=20, max=60, avg=40.0 (window: [20,30,40,50,60])
```

**Explanation:**
- `deque(maxlen)` handles O(1) eviction for sum/count
- Monotonic deques maintain min/max in O(1) amortized (classic algorithm)
- All operations: O(1) amortized time, O(n) space (window size only)
- Used in: real-time anomaly detection, streaming metrics, financial tick processing

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Inverted Index for Pipeline Metadata Search

**Scenario:** Build an inverted index that enables fast text search across pipeline metadata (descriptions, tags, column names). Support: exact match, prefix search, and multi-term AND queries.

<details>
<summary>✅ Solution</summary>

```python
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class SearchResult:
    doc_id: str
    score: float
    metadata: dict

class InvertedIndex:
    def __init__(self):
        self._index: dict[str, set[str]] = defaultdict(set)  # term → doc_ids
        self._docs: dict[str, dict] = {}  # doc_id → metadata
    
    def index_document(self, doc_id: str, text: str, metadata: dict):
        self._docs[doc_id] = metadata
        tokens = self._tokenize(text)
        for token in tokens:
            self._index[token].add(doc_id)
            # Index prefixes for prefix search
            for i in range(1, len(token)):
                self._index[f"prefix:{token[:i]}"].add(doc_id)
    
    def search(self, query: str, mode: str = "AND") -> list[SearchResult]:
        terms = self._tokenize(query)
        if not terms:
            return []
        
        # Get matching doc_ids per term
        term_results = [self._index.get(t, set()) for t in terms]
        
        if mode == "AND":
            matching_ids = set.intersection(*term_results) if term_results else set()
        else:  # OR
            matching_ids = set.union(*term_results) if term_results else set()
        
        # Score by number of matching terms
        results = []
        for doc_id in matching_ids:
            score = sum(1 for t_set in term_results if doc_id in t_set) / len(terms)
            results.append(SearchResult(doc_id, score, self._docs[doc_id]))
        
        return sorted(results, key=lambda r: r.score, reverse=True)
    
    def prefix_search(self, prefix: str) -> list[str]:
        return list(self._index.get(f"prefix:{prefix.lower()}", set()))
    
    def _tokenize(self, text: str) -> list[str]:
        return [w.lower().strip('.,;:!?') for w in text.split() if len(w) > 1]

# Usage
idx = InvertedIndex()
idx.index_document("pipe_1", "daily orders ETL pipeline spark", {"owner": "team_a"})
idx.index_document("pipe_2", "hourly events streaming kafka", {"owner": "team_b"})
idx.index_document("pipe_3", "daily customer churn spark ML", {"owner": "team_a"})

results = idx.search("daily spark")  # AND: finds pipe_1 and pipe_3
```

**Explanation:**
- Inverted index: maps terms → document IDs (O(1) lookup per term)
- Prefix indexing enables autocomplete-style search
- AND mode: intersection of all term result sets
- This is a simplified version of what Elasticsearch/Solr do internally

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Thread-Safe Counter for Multi-Threaded ETL

**Scenario:** Your ETL processes files in 8 parallel threads. You need a thread-safe data structure to: count records processed per file, track errors, and compute a final summary — all without race conditions.

<details>
<summary>✅ Solution</summary>

```python
import threading
from collections import defaultdict
from dataclasses import dataclass, field

@dataclass
class ETLMetrics:
    """Thread-safe metrics collector for parallel ETL."""
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _records_processed: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _errors: list[dict] = field(default_factory=list)
    _total_bytes: int = 0
    
    def record_processed(self, file_name: str, count: int = 1):
        with self._lock:
            self._records_processed[file_name] += count
    
    def record_error(self, file_name: str, error: str):
        with self._lock:
            self._errors.append({"file": file_name, "error": error})
    
    def add_bytes(self, n: int):
        with self._lock:
            self._total_bytes += n
    
    @property
    def summary(self) -> dict:
        with self._lock:
            return {
                "total_records": sum(self._records_processed.values()),
                "files_processed": len(self._records_processed),
                "total_errors": len(self._errors),
                "total_bytes_mb": self._total_bytes / (1024 * 1024),
                "error_details": self._errors[:10],  # First 10 errors
            }

# Usage with ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor

metrics = ETLMetrics()

def process_file(filepath):
    try:
        records = read_and_transform(filepath)
        metrics.record_processed(filepath, len(records))
        metrics.add_bytes(os.path.getsize(filepath))
    except Exception as e:
        metrics.record_error(filepath, str(e))

with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(process_file, file_list)

print(metrics.summary)
```

**Explanation:**
- `threading.Lock` ensures only one thread modifies shared state at a time
- `with self._lock:` is a context manager pattern — always releases even on exception
- `defaultdict(int)` auto-initializes counters for new file names
- This pattern is used in production ETL frameworks for aggregating metrics from parallel workers

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the time complexity of lookup in a Python dict vs. a list?**
A: Dict lookup is O(1) average (hash table). List lookup by value (`in` operator) is O(n). In data engineering, converting a list to a set or dict before repeated membership tests is a common optimization for large reference datasets.

**Q: When would you use a `defaultdict` over a plain `dict`?**
A: When building a mapping that accumulates values (e.g., grouping rows by key into a list). `defaultdict(list)` automatically creates an empty list for new keys, eliminating the `if key not in d: d[key] = []` guard. This is cleaner and slightly faster for building group-by structures in Python.

**Q: What is the difference between a list and a deque for queue operations?**
A: `list.pop(0)` (removing from the front) is O(n) because all remaining elements shift. `collections.deque.popleft()` is O(1) because deque is a doubly-linked list. For FIFO queues (BFS, sliding window processing), always use `deque`.

**Q: What is a heap in Python and how do you use it?**
A: Python's `heapq` module implements a min-heap using a regular list. `heapq.heappush(h, item)` and `heapq.heappop(h)` maintain the heap property in O(log n). Use it for top-K problems: push all items and maintain a size-K heap, or use `heapq.nlargest(k, iterable)` directly.

**Q: What is the difference between `frozenset` and `set`?**
A: `frozenset` is an immutable set—it cannot be modified after creation. It is hashable, so it can be used as a dict key or set element. Use `frozenset` when you need to use a set as a dictionary key (e.g., caching results keyed by a variable set of parameters).

**Q: What is `collections.Counter` and how does it help in data processing?**
A: `Counter` is a subclass of `dict` that counts hashable object occurrences. `Counter(iterable)` builds a frequency map in one line. It supports arithmetic (`+`, `-`, `&`, `|`) for combining counts from multiple sources—useful for computing word frequencies, event counts, or histogram merging.

**Q: What is `collections.namedtuple` and when would you use it over a plain tuple or dict?**
A: `namedtuple` creates a lightweight, immutable record type with named fields. It has the memory efficiency of a tuple (no per-instance `__dict__`) but field access by name for readability. Use it for passing structured records through a pipeline when a full class is overkill but positional tuple access is too fragile.

**Q: What is a `bisect` and when is it useful in data engineering?**
A: `bisect` provides binary search on sorted lists in O(log n). Use it for: efficiently inserting into a sorted list (`bisect.insort`), finding which bucket a value falls into (range partitioning without if-elif chains), or time-series lookups where you need the nearest timestamp.

---

## 💼 Interview Tips

- Interviewers frequently give a DE scenario (count distinct values, find top-K, group rows by key) and expect you to choose the right data structure. Practice mapping problem types to structures: grouping → defaultdict; frequency → Counter; top-K → heapq; membership test → set.
- Know time complexities for common operations: dict O(1) get/set, list O(1) append/O(n) insert, set O(1) add/lookup, deque O(1) both ends, heap O(log n) push/pop.
- Senior interviewers often probe memory trade-offs: a list of tuples vs. a list of dicts vs. a Pandas DataFrame for 1M records. Discuss per-object overhead, cache locality, and when to reach for NumPy/Pandas vs. pure Python.
- `collections` module fluency (Counter, defaultdict, deque, OrderedDict, namedtuple) is a strong signal of Python proficiency—mention and use these instead of manual workarounds.
- Connect data structures to correctness: using a `set` for deduplication in a pipeline guarantees O(1) membership tests regardless of accumulated count, whereas a list would silently degrade to O(n)—demonstrate you think about scale from the start.
