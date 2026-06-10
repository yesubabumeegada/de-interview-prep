---
title: "Interview Coding Problems — Scenarios"
topic: python
subtopic: interview-coding-problems
content_type: scenario_question
tags: [python, coding-problems, interview, scenarios]
---

# Python Interview Coding Problems — Scenarios

Work these three as live-screen simulations. Read the scenario, write your solution before opening the hint, and time yourself: junior under 10 minutes, mid-level under 20, senior under 30.

---

<article data-difficulty="junior">

## 🟢 Junior: Top Talkers in a Web Log

**Scenario:** "Here's a web server log file. Each line looks like `203.0.113.5 - - [01/May/2024:10:02:11] \"GET /api/users HTTP/1.1\" 200`. Write a function that returns the 3 IP addresses making the most requests, with their counts. Some lines may be blank or malformed — your function shouldn't crash on them."

```text
203.0.113.5 - - [01/May/2024:10:02:11] "GET /api/users HTTP/1.1" 200
198.51.100.7 - - [01/May/2024:10:02:12] "GET /api/orders HTTP/1.1" 200
203.0.113.5 - - [01/May/2024:10:02:13] "POST /api/users HTTP/1.1" 201
<corrupted line>
203.0.113.5 - - [01/May/2024:10:02:15] "GET /health HTTP/1.1" 200
```

<details>
<summary>💡 Hint</summary>

The IP is the first whitespace-separated token, so you don't need a full log-format regex — `line.split()` gets you there. `collections.Counter` has a method that returns the N most common items directly. For malformed lines, validate the token looks like an IP before counting it.

</details>

<details>
<summary>✅ Solution</summary>

```python
import re
from collections import Counter
from typing import Iterable

IP_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")

def top_ips(lines: Iterable[str], n: int = 3) -> list[tuple[str, int]]:
    counts: Counter = Counter()
    for line in lines:
        parts = line.split()
        if not parts or not IP_RE.match(parts[0]):
            continue                      # blank or malformed -> skip, don't crash
        counts[parts[0]] += 1
    return counts.most_common(n)

with open("access.log", encoding="utf-8") as f:
    for ip, hits in top_ips(f):          # pass the file object: streams line by line
        print(f"{ip}\t{hits}")
```

**Why this earns full marks:**

- **Streams.** It takes any iterable of lines and accepts the file object itself — never `f.read()`. Memory is O(unique IPs), not O(file size).
- **Tolerates garbage.** The split-and-validate guard means `<corrupted line>` and blank lines are skipped; one bad row can't kill the job.
- **Uses the right tool.** `Counter.most_common(n)` is O(u log n) via an internal heap — and saying "I could also sort the items, but that's O(u log u)" shows you know why.

**Follow-ups to expect:**

1. "Top 3 IPs *per status code*?" — `Counter` per status in a `defaultdict(Counter)`, then `most_common(3)` on each.
2. "The log is 100 GB?" — nothing changes; the solution already streams. That's the point of writing it this way the first time.

</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Merge K Sorted Log Files

**Scenario:** "Each of our 12 app servers writes its own log file, sorted by timestamp. Write a function that merges them into one globally time-ordered stream. The combined logs are far bigger than memory, so you cannot concatenate and sort."

<details>
<summary>💡 Hint</summary>

You only ever need to compare the *current head line* of each file — K lines in memory total, not K files. A min-heap keyed on each head's timestamp tells you in O(log K) which file to pull from next. The standard library has a function that implements exactly this pattern; knowing it (and being able to sketch the heap version by hand) is the whole question.

</details>

<details>
<summary>✅ Solution</summary>

```python
import heapq
from contextlib import ExitStack
from typing import Iterator

def parse_ts(line: str) -> str:
    return line.split(" ", 1)[0]          # ISO-8601 prefix sorts lexically

def merge_logs(paths: list[str]) -> Iterator[str]:
    with ExitStack() as stack:
        files = [stack.enter_context(open(p, encoding="utf-8")) for p in paths]
        yield from heapq.merge(*files, key=parse_ts)

for line in merge_logs(["app1.log", "app2.log", "app3.log"]):
    process(line)
```

**Manual heap version** (interviewers often ask you to open the black box):

```python
def merge_logs_manual(paths: list[str]) -> Iterator[str]:
    with ExitStack() as stack:
        files = [stack.enter_context(open(p, encoding="utf-8")) for p in paths]
        heap: list[tuple[str, int, str]] = []
        for i, f in enumerate(files):
            line = f.readline()
            if line:
                heap.append((parse_ts(line), i, line))
        heapq.heapify(heap)
        while heap:
            _ts, i, line = heapq.heappop(heap)
            yield line
            nxt = files[i].readline()
            if nxt:
                heapq.heappush(heap, (parse_ts(nxt), i, nxt))
```

**Complexity:** O(N log K) time for N total lines across K files; **O(K) memory** — one buffered line per file. Concatenate-and-sort is O(N log N) time and O(N) memory, which the prompt explicitly forbids.

**Details that separate candidates:**

- The heap tuple includes the file index `i` as a tiebreaker — without it, two identical timestamps make Python compare the line strings, which is wasteful (and would crash if the payloads weren't comparable).
- `ExitStack` closes K files reliably; a loop of bare `open()` calls leaks handles on any exception.
- ISO-8601 timestamps sort lexically, so no datetime parsing is needed — but say that assumption out loud, because `05/01/2024` formats do not.
- This is exactly the merge phase of **external merge sort** — the canonical answer to "sort a file bigger than RAM," a near-certain follow-up.

</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Streaming Deduplication with a Time Window

**Scenario:** "Our Kafka consumer occasionally receives duplicate events because the producer retries. Duplicates share an `event_id` and arrive within minutes of each other. Write a deduplicator: `seen_before(event_id, timestamp)` returns True for any `event_id` already seen in the last 10 minutes. It runs for weeks, processing millions of events per hour — memory must stay bounded. Then tell me what your design can and cannot guarantee."

<details>
<summary>💡 Hint</summary>

A plain `set` of seen ids grows forever — the question is really about *eviction*. Pair a dict (id → last-seen time) with a queue ordered by arrival time, and evict expired entries from the front on each call. Think about: re-seeing an id refreshing its window, slightly out-of-order timestamps, and what happens to your guarantees when the process restarts.

</details>

<details>
<summary>✅ Solution</summary>

```python
from collections import deque
from datetime import datetime, timedelta

class StreamingDeduper:
    """Sliding-window dedupe: True if event_id was seen within `window`."""

    def __init__(self, window: timedelta = timedelta(minutes=10)) -> None:
        self.window = window
        self.last_seen: dict[str, datetime] = {}
        self.queue: deque[tuple[datetime, str]] = deque()   # arrival order

    def _evict(self, now: datetime) -> None:
        cutoff = now - self.window
        while self.queue and self.queue[0][0] < cutoff:
            ts, event_id = self.queue.popleft()
            # only evict if no fresher sighting refreshed this id
            if self.last_seen.get(event_id) == ts:
                del self.last_seen[event_id]

    def seen_before(self, event_id: str, ts: datetime) -> bool:
        self._evict(ts)
        prev = self.last_seen.get(event_id)
        duplicate = prev is not None and ts - prev <= self.window
        self.last_seen[event_id] = ts             # refresh sighting
        self.queue.append((ts, event_id))
        return duplicate
```

```python
# tests
from datetime import datetime, timedelta

T0 = datetime(2024, 5, 1, 12, 0)
m = lambda k: T0 + timedelta(minutes=k)

def test_duplicate_inside_window():
    d = StreamingDeduper()
    assert d.seen_before("e1", m(0)) is False
    assert d.seen_before("e1", m(5)) is True

def test_same_id_after_window_is_new():
    d = StreamingDeduper()
    d.seen_before("e1", m(0))
    assert d.seen_before("e1", m(11)) is False

def test_refresh_extends_the_window():
    d = StreamingDeduper()
    d.seen_before("e1", m(0))
    d.seen_before("e1", m(9))                 # duplicate, but refreshes
    assert d.seen_before("e1", m(18)) is True # 9 min since refresh

def test_memory_stays_bounded():
    d = StreamingDeduper()
    for i in range(100_000):
        d.seen_before(f"e{i}", m(i))          # 1 event/min, ids never repeat
    assert len(d.last_seen) <= 11             # only the 10-min window remains
```

**Complexity:** amortized O(1) per event (each entry is pushed and popped once); memory is O(events per window), not O(total events) — that's the bounded-memory requirement.

**The guarantees discussion (this is where senior is decided):**

1. **It's in-process state.** A restart wipes the window → duplicates leak through after every deploy. The downstream must therefore be idempotent (e.g., `MERGE` on `event_id`); this deduper reduces *volume*, it cannot create exactly-once on its own.
2. **Multiple consumers don't share the dict.** Either partition the stream by `event_id` (Kafka key) so each id always lands on the same consumer, or move state to Redis (`SET key NX EX 600` is this whole class in one command).
3. **Out-of-order events:** eviction uses event time from the front of an arrival-ordered queue; an event arriving with a slightly older timestamp can't corrupt the structure, but a wildly skewed clock could evict early — state the assumption of roughly monotonic timestamps, or evict on processing time.
4. **Scale lever:** if even per-window exactness is too expensive, a Bloom filter (per rotating window bucket) gives fixed memory with a tunable false-positive rate — you'd drop a tiny fraction of *non*-duplicates and must say whether the business tolerates that.

</details>

</article>

---

## Interview Tips

> **Narrate memory, not just logic.** Every one of these problems is secretly a memory question. The phrases "this streams, so it's O(1) per row" and "this dict is bounded by the window, not the stream" earn more than a perfect-but-silent solution.

> **Make malformed input a first-class case.** Before coding, ask: "Can lines be corrupt? Can timestamps go backwards? Can ids repeat legitimately?" DE interviewers plant dirty data on purpose; candidates who guard for it before being told are the ones who get the offer.

> **Reach for the stdlib first, then prove you could build it.** Lead with `Counter.most_common`, `heapq.merge`, `functools.lru_cache` — then offer the hand-rolled version. That ordering signals you write maintainable production code *and* understand the machinery underneath.

---

## ⚡ Quick-fire Q&A

**Q: Why is `for line in f:` better than `f.readlines()` for big files?**
A: It streams one line at a time in O(1) memory; `readlines()` materializes the entire file as a list.

**Q: What's the complexity of `Counter(items).most_common(k)`?**
A: O(n) to count plus O(u log k) to select via heap, for u unique keys — better than fully sorting at O(u log u).

**Q: When does `heapq.merge` beat `sorted(chain(...))`?**
A: When the inputs are already sorted and too large to materialize — merge streams in O(N log K) with O(K) memory.

**Q: Why prefer `deque` over `list` for a sliding window?**
A: `popleft()` is O(1); `list.pop(0)` shifts every element and is O(n) per eviction.

**Q: `dict.get(k, default)` vs `defaultdict` — when each?**
A: `get` for occasional safe reads; `defaultdict` for hot accumulation loops. Beware: `defaultdict` inserts the default on every miss, even from a read.

**Q: Why is `float` wrong for money reconciliation?**
A: Binary floats can't represent most decimal fractions (`0.1 + 0.2 != 0.3`), so equality checks fail unpredictably. Use `Decimal` built from strings.

**Q: How do you dedupe a list while preserving first-seen order in one line?**
A: `list(dict.fromkeys(items))` — dicts are insertion-ordered and keys are unique, all O(n).

**Q: What makes an ETL function testable?**
A: Pure logic over iterables with I/O and the clock injected at the edges — tests pass in lists and frozen datetimes, no files, DBs, or sleeps.
