---
title: "Interview Coding Problems — Real World"
topic: python
subtopic: interview-coding-problems
content_type: study_material
layer: real-world
difficulty_level: mid-level
tags: [python, coding-problems, interview]
---

# Python Interview Coding Problems — Real World

Three problems that show up nearly verbatim as DE take-homes and 45-minute live screens, solved end-to-end: working code, tests, and the design commentary reviewers grade you on.

---

## Problem 1: Sessionize Clickstream Logs

**The brief:** "You receive click events as `(user_id, timestamp)`. Group each user's events into sessions: a session ends when more than 30 minutes pass between consecutive events. Output one row per session with user, start, end, and event count."

### Solution

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Iterable

SESSION_GAP = timedelta(minutes=30)

@dataclass(frozen=True)
class Session:
    user_id: str
    start: datetime
    end: datetime
    events: int

def sessionize(events: Iterable[tuple[str, datetime]],
               gap: timedelta = SESSION_GAP) -> list[Session]:
    by_user: dict[str, list[datetime]] = defaultdict(list)
    for user_id, ts in events:
        by_user[user_id].append(ts)

    sessions: list[Session] = []
    for user_id, stamps in by_user.items():
        stamps.sort()                       # never trust event ordering
        start = prev = stamps[0]
        count = 1
        for ts in stamps[1:]:
            if ts - prev > gap:             # gap exceeded -> close session
                sessions.append(Session(user_id, start, prev, count))
                start, count = ts, 0
            prev = ts
            count += 1
        sessions.append(Session(user_id, start, prev, count))  # flush last
    return sorted(sessions, key=lambda s: (s.user_id, s.start))
```

### Tests

```python
from datetime import datetime

def t(minute: int) -> datetime:
    return datetime(2024, 5, 1, 10, minute)

def test_splits_on_gap_over_30_minutes():
    events = [("u1", t(0)), ("u1", t(10)), ("u1", t(50))]   # 40-min gap
    s = sessionize(events)
    assert [(x.events, x.start, x.end) for x in s] == [
        (2, t(0), t(10)), (1, t(50), t(50))]

def test_exactly_30_minutes_stays_in_session():
    s = sessionize([("u1", t(0)), ("u1", t(30))])           # boundary: > not >=
    assert len(s) == 1 and s[0].events == 2

def test_out_of_order_events_are_sorted_first():
    s = sessionize([("u1", t(10)), ("u1", t(0))])
    assert len(s) == 1 and s[0].start == t(0)

def test_users_do_not_share_sessions():
    s = sessionize([("u1", t(0)), ("u2", t(1))])
    assert {x.user_id for x in s} == {"u1", "u2"}
```

**What reviewers look for:** sorting before sessionizing (real clickstreams arrive out of order), the explicit `>` vs `>=` boundary decision, flushing the final session (the classic bug), and the gap as a parameter, not a buried constant. Bonus follow-up: "what if one user's events don't fit in memory?" → events are usually delivered partitioned by user and time; sessionize per partition, and handle sessions spanning partition boundaries by carrying the open session forward.

---

## Problem 2: Reconcile Two CSV Exports

**The brief:** "Finance exports `bank.csv`; our billing system exports `internal.csv`. Both have `txn_id, amount`. Produce a reconciliation report: matched, missing from bank, missing from internal, and amount mismatches (tolerance: 1 cent)."

### Solution

```python
import csv
from dataclasses import dataclass, field
from decimal import Decimal

TOLERANCE = Decimal("0.01")

@dataclass
class ReconReport:
    matched: list[str] = field(default_factory=list)
    missing_in_bank: list[str] = field(default_factory=list)
    missing_in_internal: list[str] = field(default_factory=list)
    mismatched: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)

def load(path: str) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            txn_id = row["txn_id"].strip()
            if txn_id in out:
                raise ValueError(f"duplicate txn_id {txn_id!r} in {path}")
            out[txn_id] = Decimal(row["amount"].strip())
    return out

def reconcile(internal: dict[str, Decimal],
              bank: dict[str, Decimal]) -> ReconReport:
    report = ReconReport()
    for txn_id in sorted(internal.keys() | bank.keys()):
        in_amt, bk_amt = internal.get(txn_id), bank.get(txn_id)
        if in_amt is None:
            report.missing_in_internal.append(txn_id)
        elif bk_amt is None:
            report.missing_in_bank.append(txn_id)
        elif abs(in_amt - bk_amt) <= TOLERANCE:
            report.matched.append(txn_id)
        else:
            report.mismatched.append((txn_id, in_amt, bk_amt))
    return report
```

### Tests

```python
from decimal import Decimal as D

def test_full_outer_semantics():
    internal = {"t1": D("10.00"), "t2": D("5.00"), "t4": D("9.99")}
    bank     = {"t1": D("10.00"), "t3": D("7.00"), "t4": D("8.00")}
    r = reconcile(internal, bank)
    assert r.matched == ["t1"]
    assert r.missing_in_bank == ["t2"]
    assert r.missing_in_internal == ["t3"]
    assert r.mismatched == [("t4", D("9.99"), D("8.00"))]

def test_one_cent_tolerance_inclusive():
    r = reconcile({"t1": D("10.00")}, {"t1": D("10.01")})
    assert r.matched == ["t1"]

def test_float_trap_would_fail_here():
    # 0.1 + 0.2 != 0.3 in float; Decimal makes this exact
    r = reconcile({"t1": D("0.1") + D("0.2")}, {"t1": D("0.3")})
    assert r.matched == ["t1"] and r.mismatched == []
```

**What reviewers look for:** `Decimal` for money — `float("10.10")` comparisons fail intermittently and reviewers screen for exactly this; the `keys() | keys()` union as a full outer join; duplicate `txn_id` treated as a hard error, not silently overwritten (whichever export has dupes is broken upstream); deterministic sorted output so reruns diff cleanly. Follow-up: "bank exports settle a day late" → reconcile on a date-windowed key and hold unmatched rows in a carry-forward bucket for N days before alerting.

---

## Problem 3: Dedupe Customer Records with Fuzzy Keys

**The brief:** "Three source systems each export customers. The same person appears with formatting differences — `'Jane O'Brien'` vs `'jane obrien'`, `'+1 (512) 555-0001'` vs `'5125550001'`. Merge to one record per customer, preferring the most recently updated values."

### Solution: normalize → block on a match key → keep latest

```python
import re
import unicodedata
from datetime import datetime

def normalize_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode()       # José -> Jose
    s = re.sub(r"[^a-z ]", "", s.lower())          # drop punctuation
    return " ".join(s.split())                     # collapse whitespace

def normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]                        # strip US country code
    return digits or None

def normalize_email(email: str | None) -> str | None:
    return email.strip().lower() if email and email.strip() else None

def match_key(rec: dict) -> tuple:
    """Email is the strongest identifier; else phone; else exact normalized name."""
    if email := normalize_email(rec.get("email")):
        return ("email", email)
    if phone := normalize_phone(rec.get("phone")):
        return ("phone", phone)
    return ("name", normalize_name(rec["name"]))

def dedupe_customers(records: list[dict]) -> list[dict]:
    best: dict[tuple, dict] = {}
    for rec in records:
        key = match_key(rec)
        current = best.get(key)
        if current is None or rec["updated_at"] > current["updated_at"]:
            merged = dict(current or {})
            merged.update({k: v for k, v in rec.items() if v not in (None, "")})
            best[key] = merged                     # newest non-null values win
    return list(best.values())
```

### Tests

```python
from datetime import datetime as dt

def test_merges_on_normalized_phone():
    recs = [
        {"name": "Jane O'Brien", "email": None, "phone": "+1 (512) 555-0001",
         "city": "Austin", "updated_at": dt(2024, 1, 1)},
        {"name": "jane obrien", "email": None, "phone": "5125550001",
         "city": None, "updated_at": dt(2024, 3, 1)},
    ]
    out = dedupe_customers(recs)
    assert len(out) == 1
    assert out[0]["city"] == "Austin"              # null didn't clobber a value
    assert out[0]["name"] == "jane obrien"         # newest record's value won

def test_email_beats_phone_as_identifier():
    recs = [
        {"name": "A", "email": "X@Y.com", "phone": "111", "updated_at": dt(2024, 1, 1)},
        {"name": "B", "email": "x@y.com", "phone": "222", "updated_at": dt(2024, 2, 1)},
    ]
    assert len(dedupe_customers(recs)) == 1

def test_distinct_people_stay_distinct():
    recs = [
        {"name": "Ann Lee", "email": "ann@a.com", "phone": None, "updated_at": dt(2024, 1, 1)},
        {"name": "Ann Lee", "email": "ann@b.com", "phone": None, "updated_at": dt(2024, 1, 2)},
    ]
    assert len(dedupe_customers(recs)) == 2        # same name, different emails

def test_accents_and_punctuation_normalize():
    assert normalize_name("José  Núñez-García") == "jose nunezgarcia"
```

**What reviewers look for:** a deliberate identifier hierarchy (email > phone > name) stated as a business decision; normalization handling unicode, punctuation, and country codes; merge semantics where newer records win **per field** but nulls never erase known values; and the honesty that exact-match-on-normalized-key is *blocking*, not true fuzzy matching. Strong candidates name the next step — similarity scoring (`difflib.SequenceMatcher`, Levenshtein, or a tool like Splink) within blocks — and the risk both ways: false merges (two Ann Lees become one) vs missed merges. Say which error is worse for *this* business before tuning.

---

## How to Present a Take-Home

| Deliverable | What it signals |
|---|---|
| `README.md` with assumptions and how to run | You communicate; you noticed the brief was ambiguous |
| Pure functions + thin I/O layer | You've heard of testing in production codebases |
| Tests covering boundaries (gap == 30 min, 1-cent tolerance) | You think in edge cases, not happy paths |
| Explicit decisions in comments ("> not >=, per brief") | You make trade-offs visible instead of silent |
| A "what I'd do with more time" section | You can scope — the most senior signal of all |

> **The meta-rule:** every one of these problems hides 2–3 deliberate ambiguities (boundary inclusivity, dupes within one source, late data). The grade is mostly about whether you *found* them and wrote down your choice — not which choice you made.
