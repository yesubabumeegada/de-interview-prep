---
title: "Interview Coding Problems — Senior Deep Dive"
topic: python
subtopic: interview-coding-problems
content_type: study_material
layer: senior-deep-dive
difficulty_level: senior
tags: [python, coding-problems, interview]
---

# Python Interview Coding Problems — Senior Deep Dive

Senior screens test whether you can design data machinery, not just use it: bounded-memory algorithms, correct concurrency-adjacent primitives, dependency ordering, and code somebody else can test. Six problems below come up again and again in senior DE loops.

---

## Problem 1: Memory-Efficient Top-N and Streaming Median

**Interview prompt:** "From a stream of billions of events, keep (a) the 10 largest amounts and (b) a running median. You cannot store the stream."

### Top-N with a bounded min-heap

```python
import heapq
from typing import Iterable

def top_n(amounts: Iterable[float], n: int = 10) -> list[float]:
    heap: list[float] = []                 # min-heap of the N largest so far
    for x in amounts:
        if len(heap) < n:
            heapq.heappush(heap, x)
        elif x > heap[0]:                  # beats the smallest of the top N
            heapq.heapreplace(heap, x)
    return sorted(heap, reverse=True)
```

**Complexity:** O(n log k) time, **O(k) memory** — vs `sorted(stream)[-10:]` which is O(n log n) time and O(n) memory and simply impossible on a stream. (`heapq.nlargest(10, stream)` does the same thing; knowing both earns points.)

### Streaming median with two heaps

```python
import heapq

class StreamingMedian:
    """lo: max-heap (negated) of the lower half; hi: min-heap of the upper half."""
    def __init__(self) -> None:
        self.lo: list[float] = []
        self.hi: list[float] = []

    def add(self, x: float) -> None:
        heapq.heappush(self.lo, -x)
        heapq.heappush(self.hi, -heapq.heappop(self.lo))  # rebalance flow
        if len(self.hi) > len(self.lo):
            heapq.heappush(self.lo, -heapq.heappop(self.hi))

    def median(self) -> float:
        if len(self.lo) > len(self.hi):
            return -self.lo[0]
        return (-self.lo[0] + self.hi[0]) / 2
```

O(log n) per insert, O(1) median. **Senior nuance to volunteer:** exact median fundamentally needs O(n) memory; the two-heap trick keeps it cheap per-event but still stores everything. At true billions-scale you'd use an approximate sketch (t-digest / P²) — saying so is the difference between a coder and an engineer.

---

## Problem 2: Merge Overlapping Intervals

**Interview prompt:** "Given (start, end) session windows, merge all overlapping or touching intervals. Classic prerequisite for sessionization and calendar/SLA logic."

```python
def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    intervals = sorted(intervals)                  # by start, then end
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:                      # overlaps or touches
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged

print(merge_intervals([(1, 4), (9, 12), (3, 6), (11, 15), (20, 21)]))
# [(1, 6), (9, 15), (20, 21)]
```

**Complexity:** O(n log n) for the sort; the merge pass is O(n).

**Traps interviewers probe:**
- `max(last_end, end)` is mandatory — a contained interval like `(2, 3)` inside `(1, 10)` must not shrink the merged end.
- Decide `<` vs `<=`: do `(1, 4)` and `(4, 6)` merge? Ask; don't assume. For half-open intervals `[start, end)` touching usually merges.
- Already-sorted input (e.g., events ordered by time)? Skip the sort and state the pipeline is now O(n) streaming.

---

## Problem 3: Rate Limiter (Sliding Window)

**Interview prompt:** "Implement `allow(timestamp)` permitting at most N calls per rolling 60 seconds — you're wrapping a flaky vendor API in your ingestion job."

```python
from collections import deque

class SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, window_sec: float) -> None:
        self.max_calls = max_calls
        self.window = window_sec
        self.calls: deque[float] = deque()

    def allow(self, now: float) -> bool:
        while self.calls and self.calls[0] <= now - self.window:
            self.calls.popleft()                  # evict expired timestamps
        if len(self.calls) < self.max_calls:
            self.calls.append(now)
            return True
        return False

rl = SlidingWindowRateLimiter(max_calls=3, window_sec=60)
assert [rl.allow(t) for t in (0, 1, 2, 30, 61)] == [True, True, True, False, True]
```

**Complexity:** amortized O(1) per call; memory O(max_calls). `deque.popleft()` is O(1) — a list's `pop(0)` is O(n), and using one is a classic senior-screen red flag.

**Follow-ups to be ready for:** fixed window (cheaper, bursty at boundaries) vs sliding log (this, exact) vs token bucket (allows controlled bursts, O(1) memory); and "what about multiple workers?" → the state must move to Redis/the DB — in-process deques don't coordinate across machines.

---

## Problem 4: LRU Cache from Scratch

**Interview prompt:** "Build an LRU cache with O(1) get and put. Then tell me when you'd just use the stdlib."

```python
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.data: OrderedDict = OrderedDict()

    def get(self, key):
        if key not in self.data:
            return None
        self.data.move_to_end(key)                # mark most-recently-used
        return self.data[key]

    def put(self, key, value) -> None:
        if key in self.data:
            self.data.move_to_end(key)
        self.data[key] = value
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)         # evict least-recently-used

cache = LRUCache(2)
cache.put("a", 1); cache.put("b", 2); cache.get("a"); cache.put("c", 3)
assert cache.get("b") is None and cache.get("a") == 1
```

`OrderedDict` gives O(1) `move_to_end` and `popitem` — under the hood it is exactly the hashmap + doubly-linked-list structure the textbook answer hand-rolls. Offer to build the linked-list version if they insist, but lead with this.

**The senior framing:** in production you write `@functools.lru_cache(maxsize=...)` on the hot lookup (dimension-key resolution, schema fetches). Hand-rolling is for interviews and for when you need TTLs or per-key invalidation — at which point reach for `cachetools.TTLCache`. Warn about caching unbounded key spaces in long-running services: that's a slow memory leak.

---

## Problem 5: Topological Sort for DAG Dependencies

**Interview prompt:** "Given task dependencies (Airflow-style), return a valid execution order, and detect cycles."

### Kahn's algorithm (BFS, detects cycles naturally)

```python
from collections import deque

def topo_sort(deps: dict[str, list[str]]) -> list[str]:
    """deps maps task -> list of tasks it depends on (upstream)."""
    tasks = set(deps) | {u for ups in deps.values() for u in ups}
    indegree = {t: 0 for t in tasks}
    downstream: dict[str, list[str]] = {t: [] for t in tasks}
    for task, ups in deps.items():
        indegree[task] = len(ups)
        for u in ups:
            downstream[u].append(task)

    queue = deque(sorted(t for t in tasks if indegree[t] == 0))
    order: list[str] = []
    while queue:
        t = queue.popleft()
        order.append(t)
        for d in downstream[t]:
            indegree[d] -= 1
            if indegree[d] == 0:
                queue.append(d)

    if len(order) != len(tasks):
        cyclic = sorted(t for t in tasks if indegree[t] > 0)
        raise ValueError(f"cycle detected involving: {cyclic}")
    return order

deps = {"load": ["transform"], "transform": ["extract_a", "extract_b"],
        "report": ["load"]}
print(topo_sort(deps))
# ['extract_a', 'extract_b', 'transform', 'load', 'report']
```

**Complexity:** O(V + E). **Why Kahn's over DFS in a DE interview:** cycle detection falls out for free (leftover tasks with indegree > 0 *are* the cycle), and the zero-indegree frontier at each step is precisely the set of tasks you can run **in parallel** — which is the inevitable follow-up question ("how would you maximize parallelism?" → process the queue level by level).

---

## Problem 6: Writing Testable ETL Functions

**Interview prompt:** "Here's a script that reads a CSV, cleans it, and writes to a database. Refactor it so it can be unit-tested."

### The refactor: separate I/O from logic

```python
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Iterator

@dataclass(frozen=True)
class CleanOrder:
    order_id: int
    amount: float
    order_ts: datetime

def transform(rows: Iterable[dict], *, now: datetime) -> Iterator[CleanOrder]:
    """Pure: dicts in, records out. No files, no DB, no clock, no globals."""
    for row in rows:
        amount = float(row["amount"])
        if amount <= 0:
            continue                                # business rule: drop refunds
        ts = datetime.fromisoformat(row["order_ts"]).astimezone(timezone.utc)
        if ts > now:
            continue                                # business rule: no future orders
        yield CleanOrder(int(row["order_id"]), amount, ts)

def run(reader, writer, clock=lambda: datetime.now(timezone.utc)) -> int:
    """Thin orchestration shell; I/O injected, trivially fake-able."""
    count = 0
    for rec in transform(reader(), now=clock()):
        writer(rec)
        count += 1
    return count
```

```python
# test_transform.py — no files, no DB, no mocks of the clock needed
from datetime import datetime, timezone

FROZEN = datetime(2024, 6, 1, tzinfo=timezone.utc)

def test_drops_refunds_and_future_orders():
    rows = [
        {"order_id": "1", "amount": "10.0",  "order_ts": "2024-05-01T00:00:00+00:00"},
        {"order_id": "2", "amount": "-5.0",  "order_ts": "2024-05-01T00:00:00+00:00"},
        {"order_id": "3", "amount": "8.0",   "order_ts": "2030-01-01T00:00:00+00:00"},
    ]
    out = list(transform(rows, now=FROZEN))
    assert [r.order_id for r in out] == [1]
```

**The principles to narrate:**
1. **Functional core, imperative shell** — `transform` is a pure function over iterables; all I/O lives in injectable `reader`/`writer` callables.
2. **Inject the clock.** `datetime.now()` buried in logic makes tests flaky and time-dependent rules untestable.
3. **Iterables in, iterators out** keeps the testable version streaming-capable — testability did not cost you memory efficiency.
4. Frozen dataclasses make outputs comparable and accidental-mutation-proof.

---

## ⚡ Cheat Sheet

### Idioms

| Need | Reach for |
|---|---|
| Top-N from a stream | `heapq.nlargest(n, it)` / bounded min-heap |
| Smallest-N | `heapq.nsmallest(n, it)` |
| Merge K sorted files | `heapq.merge(*iterables)` |
| O(1) pops from both ends | `collections.deque` |
| O(1) memoization | `@functools.lru_cache(maxsize=...)` |
| Consecutive pairs | `itertools.pairwise(it)` |
| Fixed-size batches | `itertools.batched(it, n)` (3.12+) / `islice` loop |
| Chain streams without copying | `itertools.chain(a, b)` |
| Running totals | `itertools.accumulate(it)` |
| Group sorted stream | `itertools.groupby(sorted_it, key=...)` |
| Multi-key sort | `sorted(rows, key=lambda r: (r.a, -r.b))` |

### Complexity quick table

| Operation | Complexity |
|---|---|
| dict/set lookup, insert | O(1) average |
| `list.append` / `pop()` | O(1) amortized |
| `list.pop(0)` / `insert(0, x)` | O(n) — use deque |
| `x in list` | O(n) — use set |
| `sorted()` / `.sort()` | O(n log n) |
| heap push/pop | O(log n) |
| Top-N via heap | O(n log k), O(k) memory |
| Kahn's toposort | O(V + E) |
| Hash join (index + probe) | O(n + m) |

### Say this in the interview

- "I'll keep a bounded heap so memory is O(k), not O(n) — this has to survive a stream."
- "Exact streaming median still stores everything; at real scale I'd use t-digest and accept approximate."
- "Deque, not list — `pop(0)` is O(n) and this is a hot path."
- "Kahn's gives me cycle detection for free, and each zero-indegree frontier is a parallel execution wave."
- "An in-process rate limiter doesn't coordinate across workers; distributed limiting means shared state in Redis."
- "I separate the pure transform from I/O so the business rules unit-test without a database — functional core, imperative shell."
- "I'd use `functools.lru_cache` in production; here's the `OrderedDict` version to show the mechanics."
- "Intervals: sort by start, sweep once, and `max()` the end so contained intervals can't shrink the merge."
