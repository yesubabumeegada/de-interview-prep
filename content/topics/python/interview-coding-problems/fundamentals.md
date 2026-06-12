---
title: "Interview Coding Problems — Fundamentals"
topic: python
subtopic: interview-coding-problems
content_type: study_material
layer: fundamentals
difficulty_level: junior
tags: [python, coding-problems, interview]
---

# Python Interview Coding Problems — Fundamentals

These are the Python problems Data Engineers actually get in phone screens. Not LeetCode tree inversions — string cleanup, dict aggregations, file parsing, and dedup logic. Interviewers use them to check whether you can manipulate data fluently without reaching for pandas.

---


## 🎯 Analogy

Think of Python interview problems like algorithmic crosswords: each puzzle tests a specific skill (sliding window, two pointers, hash map frequency counts). The trick is recognizing the pattern, not memorizing solutions.

---
## Problem 1: Word Frequency from a Messy String

**Interview prompt:** "Given a string of text, return the top 3 most common words, case-insensitive, ignoring punctuation."

### Solution

```python
import re
from collections import Counter

def top_words(text: str, n: int = 3) -> list[tuple[str, int]]:
    words = re.findall(r"[a-z']+", text.lower())
    return Counter(words).most_common(n)

text = "The pipeline failed. The pipeline restarted, and the pipeline succeeded!"
print(top_words(text))
# [('the', 3), ('pipeline', 3), ('failed', 1)]
```

**Complexity:** O(n) over the characters of the input; `most_common(k)` is O(u log k) for u unique words.

### Common mistakes

- Splitting on `" "` only — `"failed."` and `"failed"` count as different words. Use `re.findall` or strip punctuation per token.
- Forgetting `.lower()` — `"The"` and `"the"` split the count.
- Hand-rolling the counter with `if word in d: d[word] += 1 else: ...` — works, but interviewers expect you to know `Counter` (or at least `dict.get(word, 0) + 1` / `defaultdict(int)`).

---

## Problem 2: Aggregate Sales per Category (dict aggregation)

**Interview prompt:** "Given a list of `(category, amount)` tuples, return total sales per category."

This is the Python equivalent of `GROUP BY category, SUM(amount)` — the single most common screen question for DE roles.

### Solution

```python
from collections import defaultdict

def totals_by_category(rows: list[tuple[str, float]]) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    for category, amount in rows:
        totals[category] += amount
    return dict(totals)

rows = [("books", 12.5), ("toys", 8.0), ("books", 4.5), ("food", 20.0)]
print(totals_by_category(rows))
# {'books': 17.0, 'toys': 8.0, 'food': 20.0}
```

**Complexity:** O(n) time, O(k) space for k distinct categories.

### Variation interviewers love: count AND sum together

```python
def stats_by_category(rows: list[tuple[str, float]]) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    for category, amount in rows:
        s = stats.setdefault(category, {"count": 0, "total": 0.0})
        s["count"] += 1
        s["total"] += amount
    for s in stats.values():
        s["avg"] = s["total"] / s["count"]
    return stats
```

### Common mistakes

- `KeyError` from `totals[category] += amount` on a plain dict — use `defaultdict`, `setdefault`, or `dict.get`.
- Returning the `defaultdict` itself — fine usually, but later lookups silently create keys. Convert with `dict(...)`.

---

## Problem 3: List Comprehensions and Filtering

**Interview prompt:** "Given a list of user dicts, return the emails of active users, lowercased, skipping records with a missing email."

### Solution

```python
users = [
    {"id": 1, "email": "Alice@X.com", "active": True},
    {"id": 2, "email": None,          "active": True},
    {"id": 3, "email": "bob@x.com",   "active": False},
    {"id": 4, "email": "Carol@X.com", "active": True},
]

active_emails = [
    u["email"].lower()
    for u in users
    if u["active"] and u.get("email")
]
print(active_emails)  # ['alice@x.com', 'carol@x.com']
```

**Complexity:** O(n).

### Common mistakes

- Order matters: `u["email"].lower()` runs only for rows that pass the filter — but writing `if u["email"].lower() and u["active"]` crashes on `None` before the truthiness check. Put the None-guard first: `u.get("email")` is falsy for both missing keys and `None`.
- Nesting three conditions and a transform into one comprehension. If it doesn't fit on ~2 lines, use a loop — interviewers grade readability.

---

## Problem 4: Read and Aggregate a CSV (no pandas)

**Interview prompt:** "Read `orders.csv` and print revenue per customer. You may not use pandas."

```text
order_id,customer_id,amount,status
1,101,250.00,completed
2,101,180.00,completed
3,102,420.00,cancelled
4,103,500.00,completed
```

### Solution

```python
import csv
from collections import defaultdict

def revenue_per_customer(path: str) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] == "completed":
                totals[row["customer_id"]] += float(row["amount"])
    return dict(totals)

# {'101': 430.0, '103': 500.0}
```

**Complexity:** O(n) rows, O(k) memory — streams the file, never loads it all.

### Common mistakes

- `f.read().split(",")` instead of the `csv` module — breaks on quoted fields containing commas (`"Smith, John"`). Always say "I'll use the csv module because of quoting/escaping."
- Forgetting every CSV value is a **string**: `row["amount"]` must be cast with `float()` before summing.
- `open(path)` without `with` — the file handle leaks. Interviewers notice.

---

## Problem 5: Parse JSON and Handle Missing Keys

**Interview prompt:** "Given a JSON API response, extract `(id, city)` for each user. `city` is nested under `address` and may be absent."

### Solution

```python
import json

payload = '''
{"users": [
    {"id": 1, "address": {"city": "Austin", "zip": "78701"}},
    {"id": 2, "address": {}},
    {"id": 3}
]}
'''

def extract_cities(raw: str) -> list[tuple[int, str | None]]:
    data = json.loads(raw)
    return [
        (u["id"], u.get("address", {}).get("city"))
        for u in data.get("users", [])
    ]

print(extract_cities(payload))
# [(1, 'Austin'), (2, None), (3, None)]
```

**Complexity:** O(n) users.

### Common mistakes

- `u["address"]["city"]` — `KeyError` on user 3. Chain `.get()` with a `{}` default for safe nested access.
- Confusing `json.load` (file object) with `json.loads` (string). It comes up constantly.
- Assuming `data["users"]` exists — defensive `.get("users", [])` returns an empty result instead of crashing on a malformed payload.

---

## Problem 6: Deduplicate While Preserving Order

**Interview prompt:** "Remove duplicate records from a list, keeping the FIRST occurrence and preserving order."

### Solution

```python
def dedupe(items: list) -> list:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

print(dedupe([3, 1, 3, 2, 1, 5]))  # [3, 1, 2, 5]
```

**Complexity:** O(n) time, O(n) space. Set lookup is O(1) average.

### Variation: dedupe dicts by a key field (keep latest)

Dicts aren't hashable, so `set()` fails. Key on the business field instead — overwriting keeps the **last** record per id:

```python
def dedupe_by_key(records: list[dict], key: str) -> list[dict]:
    latest: dict = {}
    for rec in records:        # later records overwrite earlier ones
        latest[rec[key]] = rec
    return list(latest.values())
```

### Common mistakes

- `list(set(items))` — removes dupes but **destroys order** (and fails on dicts). Mention this trade-off out loud.
- Quadratic version: `if item not in result` does an O(n) list scan per element → O(n²). Use a set for membership.
- `dict.fromkeys(items)` is a neat O(n) order-preserving dedupe for hashables — knowing it scores points.

---

## Problem 7: Simple Log Parsing

**Interview prompt:** "Given web server log lines, count requests per status code and list the top 3 most-requested paths."

```text
2024-05-01T10:00:01 GET /api/users 200
2024-05-01T10:00:02 GET /api/orders 500
2024-05-01T10:00:03 POST /api/users 201
2024-05-01T10:00:04 GET /api/users 200
```

### Solution

```python
from collections import Counter

def parse_logs(lines: list[str]) -> tuple[Counter, list[tuple[str, int]]]:
    status_counts: Counter = Counter()
    path_counts: Counter = Counter()
    for line in lines:
        parts = line.split()
        if len(parts) != 4:          # skip malformed lines, don't crash
            continue
        _ts, _method, path, status = parts
        status_counts[status] += 1
        path_counts[path] += 1
    return status_counts, path_counts.most_common(3)

with open("access.log", encoding="utf-8") as f:
    statuses, top_paths = parse_logs(f.read().splitlines())
```

**Complexity:** O(n) lines.

### Common mistakes

- Unpacking `ts, method, path, status = line.split()` with no length check — one malformed line kills the whole job with `ValueError`. Skipping (or counting) bad lines is the DE instinct interviewers screen for.
- Reading the whole file when you could iterate `for line in f:` — fine at this size, but say you'd stream for large files.

---

## Key Patterns Summary

| Problem | Core tool | Complexity | Classic mistake |
|---|---|---|---|
| Word frequency | `Counter` + `re.findall` | O(n) | splitting on spaces only |
| Group-and-sum | `defaultdict(float)` | O(n) | KeyError on plain dict |
| Filter + transform | list comprehension | O(n) | None-guard after the crash point |
| CSV aggregation | `csv.DictReader` | O(n), streams | manual `split(",")`, no float cast |
| Nested JSON | chained `.get()` | O(n) | `KeyError` on missing nesting |
| Ordered dedup | set + list (or `dict.fromkeys`) | O(n) | `list(set(...))` loses order |
| Log parsing | `str.split` + length check | O(n) | crashing on malformed lines |

---

## What Interviewers Are Testing

> **Junior level:** Can you group-and-aggregate with a dict without googling? Do you reach for `csv.DictReader` and `Counter` instinctively? Does your code survive one bad row?

At the junior level, interviewers want to see:

1. Fluency with dicts, sets, and comprehensions — no hesitation
2. Standard library awareness: `collections`, `csv`, `json`, `re`
3. Defensive habits: `.get()` for missing keys, skipping malformed lines, casting CSV strings
4. Stating complexity unprompted ("this is O(n), one pass, constant memory per group")

They are NOT testing algorithmic cleverness. They are testing whether you can be trusted to write the glue code that moves data every day.

## ▶️ Try It Yourself

```python
# Classic: find pairs that sum to target (two-pointer / hash map)
def two_sum(nums: list[int], target: int) -> list[int]:
    seen = {}
    for i, n in enumerate(nums):
        complement = target - n
        if complement in seen:
            return [seen[complement], i]
        seen[n] = i
    return []

print(two_sum([2, 7, 11, 15], 9))   # [0, 1]

# Sliding window: max sum subarray of size k
def max_sum_subarray(nums: list[int], k: int) -> int:
    window = sum(nums[:k])
    best = window
    for i in range(k, len(nums)):
        window += nums[i] - nums[i - k]
        best = max(best, window)
    return best

print(max_sum_subarray([1, 4, 2, 9, 7, 3], 3))  # 18 (9+7+2? -> 4+2+9=15, 2+9+7=18)

# Frequency count: top-k elements
from collections import Counter
def top_k(nums: list[int], k: int) -> list[int]:
    return [x for x, _ in Counter(nums).most_common(k)]

print(top_k([1, 1, 1, 2, 2, 3], 2))  # [1, 2]
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
