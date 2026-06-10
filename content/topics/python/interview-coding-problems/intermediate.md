---
title: "Interview Coding Problems — Intermediate"
topic: python
subtopic: interview-coding-problems
content_type: study_material
difficulty_level: mid-level
tags: [python, coding-problems, interview]
---

# Python Interview Coding Problems — Intermediate

Mid-level screens stop asking "can you use a dict" and start asking "can you do what pandas does, in pure Python, on a file bigger than memory." These are the seven patterns that cover ~90% of those questions.

---

## Problem 1: GROUP BY with Multiple Aggregates (no pandas)

**Interview prompt:** "Given order dicts, produce per-customer count, total, and max — like `GROUP BY customer_id` — without pandas."

### Solution

```python
from collections import defaultdict

def group_stats(orders: list[dict]) -> dict[int, dict]:
    acc: dict[int, dict] = defaultdict(lambda: {"count": 0, "total": 0.0, "max": float("-inf")})
    for o in orders:
        s = acc[o["customer_id"]]
        s["count"] += 1
        s["total"] += o["amount"]
        s["max"] = max(s["max"], o["amount"])
    return dict(acc)

orders = [
    {"customer_id": 101, "amount": 250.0},
    {"customer_id": 101, "amount": 180.0},
    {"customer_id": 102, "amount": 420.0},
]
# {101: {'count': 2, 'total': 430.0, 'max': 250.0}, 102: {...}}
```

**Complexity:** O(n) single pass, O(k) memory for k groups — the same shape as a hash aggregate in a database.

### Why not `itertools.groupby`?

```python
# itertools.groupby only groups CONSECUTIVE equal keys — you must sort first:
from itertools import groupby
ordered = sorted(orders, key=lambda o: o["customer_id"])   # O(n log n)!
for cust, grp in groupby(ordered, key=lambda o: o["customer_id"]):
    ...
```

The dict accumulator is O(n) and streams; `groupby` needs an O(n log n) sort (and full materialization). Say this trade-off out loud — it is exactly what the interviewer is fishing for.

---

## Problem 2: Merging Two Datasets (a JOIN in pure Python)

**Interview prompt:** "You have a list of orders and a list of customers. Produce orders enriched with customer name — i.e., a LEFT JOIN on customer_id."

### Solution: build a lookup index first

```python
def left_join(orders: list[dict], customers: list[dict]) -> list[dict]:
    by_id = {c["customer_id"]: c for c in customers}        # O(m) index
    return [
        {**o, "name": by_id.get(o["customer_id"], {}).get("name")}
        for o in orders                                      # O(n) probe
    ]
```

**Complexity:** O(n + m) — this is a hash join. The naive nested loop (`for o in orders: for c in customers:`) is O(n·m) and is the #1 way candidates fail this question.

### Variation: one-to-many join (customer → list of orders)

```python
from collections import defaultdict

orders_by_cust: dict[int, list[dict]] = defaultdict(list)
for o in orders:
    orders_by_cust[o["customer_id"]].append(o)

enriched = [
    {**c, "orders": orders_by_cust.get(c["customer_id"], [])}
    for c in customers
]
```

**Edge cases to name:** duplicate keys on the build side (last one wins in a dict comprehension — is that correct for your data?), unmatched rows (LEFT vs INNER semantics), and key type mismatches (`"101"` from CSV vs `101` from JSON — normalize first).

---

## Problem 3: Datetime Gymnastics — Timezones and Gaps

**Interview prompt:** "Events arrive with ISO timestamps in mixed timezones. Normalize to UTC, then find any gaps longer than 5 minutes between consecutive events."

### Solution

```python
from datetime import datetime, timedelta, timezone

def find_gaps(timestamps: list[str], max_gap_min: int = 5) -> list[tuple[datetime, datetime]]:
    utc = sorted(
        datetime.fromisoformat(ts).astimezone(timezone.utc)
        for ts in timestamps
    )
    threshold = timedelta(minutes=max_gap_min)
    return [
        (a, b)
        for a, b in zip(utc, utc[1:])
        if b - a > threshold
    ]

events = [
    "2024-05-01T10:00:00+02:00",   # 08:00 UTC
    "2024-05-01T08:02:00+00:00",   # 08:02 UTC
    "2024-05-01T03:15:00-05:00",   # 08:15 UTC  → 13-min gap before this
]
for start, end in find_gaps(events):
    print(start.isoformat(), "->", end.isoformat())
# 2024-05-01T08:02:00+00:00 -> 2024-05-01T08:15:00+00:00
```

### Common mistakes

- Comparing a naive datetime to an aware one — `TypeError`. If an input lacks an offset, decide explicitly: reject it, or assume UTC with `.replace(tzinfo=timezone.utc)`.
- Sorting timestamps as **strings** — works only if every string has the identical format and offset. `"2024-05-01T09:00:00+02:00"` sorts after `"2024-05-01T08:30:00+00:00"` lexically but is earlier in real time.
- For named zones use `zoneinfo`: `datetime.now(tz=ZoneInfo("America/New_York"))` — never fixed offsets for anything DST-affected.
- `zip(utc, utc[1:])` is the idiomatic consecutive-pairs pattern (Python 3.10+ also has `itertools.pairwise`).

---

## Problem 4: Generators for Large-File Streaming

**Interview prompt:** "Sum the `amount` column of a 50 GB CSV on a machine with 8 GB of RAM."

### Solution: a generator pipeline

```python
import csv
from typing import Iterator

def read_rows(path: str) -> Iterator[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)      # one row in memory at a time

def completed_amounts(rows: Iterator[dict]) -> Iterator[float]:
    for row in rows:
        if row["status"] == "completed":
            yield float(row["amount"])

total = sum(completed_amounts(read_rows("orders_50gb.csv")))
```

**Memory:** O(1) per row regardless of file size. Each stage pulls one item through the pipeline — nothing is materialized.

### Key talking points

- A generator function returns a lazy iterator; nothing runs until consumed. `yield from` delegates cleanly.
- The `with` block stays open while the generator is being consumed and closes when it's exhausted (or garbage-collected) — that's why the `open` lives *inside* the generator.
- Generators are single-use: once exhausted, iterating again yields nothing. If asked "what if I sum it twice?" — the second `sum` is 0; you must recreate the generator.
- Square brackets `[f(x) for x in ...]` build the whole list; parentheses `(f(x) for x in ...)` stream. One character is the difference between 50 GB and 50 bytes.

---

## Problem 5: Chunked Processing

**Interview prompt:** "Insert 10 million rows into a database API that accepts at most 500 records per call. Batch the stream."

### Solution

```python
from itertools import islice
from typing import Iterable, Iterator

def chunked(iterable: Iterable, size: int) -> Iterator[list]:
    it = iter(iterable)
    while batch := list(islice(it, size)):
        yield batch

for batch in chunked(read_rows("orders_50gb.csv"), 500):
    db.insert_many(batch)     # 500 rows per round trip
```

**Why this version:** it works on *any* iterable, including generators with no `len()` — slicing tricks like `rows[i:i+500]` only work on lists already in memory. The walrus operator (`:=`) makes the loop terminate cleanly when `islice` returns an empty batch. (Python 3.12+ ships `itertools.batched` doing exactly this.)

### Common mistakes

- Accumulating all batches into a list before inserting — defeats the purpose.
- Off-by-one logic in hand-rolled `if len(buf) == size: flush()` loops: forgetting to flush the final partial batch. The `islice` pattern can't make that mistake.

---

## Problem 6: Error-Tolerant Parsing (dead-letter pattern)

**Interview prompt:** "Parse a feed of JSON lines. Some lines are corrupt. Don't crash, don't silently drop — keep good rows and quarantine bad ones."

### Solution

```python
import json
from typing import Iterable

def parse_feed(lines: Iterable[str]) -> tuple[list[dict], list[dict]]:
    good: list[dict] = []
    bad: list[dict] = []
    for lineno, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            if "id" not in rec:                       # semantic validation
                raise ValueError("missing required field: id")
            good.append(rec)
        except (json.JSONDecodeError, ValueError) as exc:
            bad.append({"lineno": lineno, "raw": line, "error": str(exc)})
    return good, bad

good, bad = parse_feed(open("events.jsonl", encoding="utf-8"))
print(f"parsed={len(good)} quarantined={len(bad)}")
```

### Talking points that win the round

- Catch **specific** exceptions. A bare `except:` swallows `KeyboardInterrupt` and real bugs; `except Exception` is the widest acceptable net, and only with logging.
- The bad-rows list is a **dead-letter queue**: it preserves the raw line, position, and reason so the failure is debuggable and replayable.
- Mention a failure budget: "if more than 1% of rows are bad, I'd fail the job — that's a source problem, not noise."

---

## Problem 7: Flattening Nested JSON

**Interview prompt:** "Flatten arbitrarily nested JSON into a single-level dict with dotted keys, so it can be loaded into a flat table."

### Solution: recursive flatten

```python
def flatten(obj: dict, prefix: str = "") -> dict:
    flat: dict = {}
    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(flatten(value, full_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flat.update(flatten(item, f"{full_key}[{i}]"))
                else:
                    flat[f"{full_key}[{i}]"] = item
        else:
            flat[full_key] = value
    return flat

record = {"id": 7, "user": {"name": "Ada", "address": {"city": "Austin"}},
          "tags": ["vip", "beta"]}
print(flatten(record))
# {'id': 7, 'user.name': 'Ada', 'user.address.city': 'Austin',
#  'tags[0]': 'vip', 'tags[1]': 'beta'}
```

**Complexity:** O(total keys); recursion depth equals nesting depth (Python's default limit is 1000 — fine for real payloads; mention an iterative stack version if asked about hostile inputs).

### Design questions interviewers follow up with

1. "What about lists?" — three honest options: index into keys (shown), explode into multiple rows (one per list element — the warehouse-friendly answer), or JSON-encode the list into a single column. State the trade-off rather than picking silently.
2. "Key collisions?" — `{"a.b": 1, "a": {"b": 2}}` both flatten to `a.b`. Pick a rarer separator or detect collisions and raise.

---

## Key Patterns Summary

| Problem | Core technique | Complexity | The trap |
|---|---|---|---|
| Group-by aggregates | `defaultdict` accumulator | O(n) | `itertools.groupby` without sorting |
| Joining datasets | dict index + probe (hash join) | O(n + m) | O(n·m) nested loop |
| Timezone gaps | `fromisoformat` → `astimezone(UTC)` → sort | O(n log n) | naive vs aware comparison |
| Large-file streaming | generator pipeline | O(1) memory | `[...]` vs `(...)`; single-use iterators |
| Chunking | `islice` + walrus | O(batch) memory | dropping the final partial batch |
| Tolerant parsing | try/except + dead-letter list | O(n) | bare `except`, silent drops |
| Flatten JSON | recursion with key prefix | O(keys) | unhandled lists, key collisions |

---

## What Interviewers Are Testing

> **Mid-level:** Can you reproduce GROUP BY and JOIN semantics from scratch and name their complexity? Do you stream by default instead of loading files into lists? Does one corrupt row kill your pipeline or land in a dead-letter queue?

They want evidence you've operated real pipelines: hash-join instincts, O(1)-memory habits, timezone paranoia, and a plan for bad data. The code is the easy part — narrating the trade-offs is what separates mid-level from junior.
