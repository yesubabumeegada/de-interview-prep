---
title: "Python Data Structures - Real-World Production Examples"
topic: python
subtopic: data-structures
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, data-structures, production, etl, pipeline, optimization]
---

# Python Data Structures — Real-World Production Examples

## Example 1: High-Performance Record Deduplication Pipeline

A production pipeline that deduplicates 50M daily events using a combination of sets, generators, and bloom filters:

```python
"""
Production deduplication pipeline for event ingestion.
Handles 50M events/day with <4GB memory footprint.
"""
import hashlib
from collections import defaultdict
from typing import Iterator, Dict, Set
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class DeduplicationStats:
    """Track dedup metrics for monitoring."""
    total_processed: int = 0
    duplicates_found: int = 0
    unique_forwarded: int = 0
    bloom_false_positives: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    
    @property
    def dedup_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return self.duplicates_found / self.total_processed * 100


class TwoLevelDeduplicator:
    """
    Two-level deduplication strategy:
    Level 1: Bloom filter (fast, O(1), small memory) — catches ~99% of duplicates
    Level 2: Exact set check (for bloom filter positives) — confirms duplicates
    
    Memory budget: 
    - Bloom filter: ~125MB for 100M capacity at 1% FP rate
    - Exact set: Only stores IDs that pass bloom filter (~1% of total)
    """
    
    def __init__(self, expected_items: int = 100_000_000):
        self.bloom_size = expected_items * 10  # 10 bits per item ≈ 1% FP
        self.bloom = bytearray(self.bloom_size // 8 + 1)
        self.num_hashes = 7
        self.exact_set: Set[str] = set()
        self.stats = DeduplicationStats()
    
    def _bloom_positions(self, item_id: str) -> list:
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.md5(f"{item_id}:{i}".encode()).hexdigest()
            pos = int(h, 16) % self.bloom_size
            positions.append(pos)
        return positions
    
    def _bloom_add(self, item_id: str):
        for pos in self._bloom_positions(item_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bloom[byte_idx] |= (1 << bit_idx)
    
    def _bloom_check(self, item_id: str) -> bool:
        for pos in self._bloom_positions(item_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bloom[byte_idx] & (1 << bit_idx)):
                return False
        return True
    
    def is_duplicate(self, record_id: str) -> bool:
        """Check if record is duplicate. O(1) amortized."""
        self.stats.total_processed += 1
        
        # Level 1: Bloom filter (fast negative check)
        if not self._bloom_check(record_id):
            # Definitely new — add to bloom and forward
            self._bloom_add(record_id)
            self.stats.unique_forwarded += 1
            return False
        
        # Level 2: Exact check (bloom said "maybe")
        if record_id in self.exact_set:
            self.stats.duplicates_found += 1
            return True
        
        # Bloom false positive — actually new
        self.exact_set.add(record_id)
        self._bloom_add(record_id)
        self.stats.bloom_false_positives += 1
        self.stats.unique_forwarded += 1
        return False


def deduplicated_stream(events: Iterator[Dict]) -> Iterator[Dict]:
    """Generator that yields only unique events."""
    deduper = TwoLevelDeduplicator()
    
    for event in events:
        event_id = event.get("event_id", "")
        if not deduper.is_duplicate(event_id):
            yield event
    
    # Log stats at end
    print(f"Dedup stats: {deduper.stats.dedup_rate:.1f}% duplicates, "
          f"{deduper.stats.bloom_false_positives} bloom FPs")
```

## Example 2: Schema-Aware Data Validation Pipeline

```python
"""
Production data validation using typed data structures.
Validates incoming records against expected schemas with detailed error reporting.
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum
from collections import defaultdict
import re

class Severity(Enum):
    ERROR = "error"      # Record rejected
    WARNING = "warning"  # Record accepted with flag
    INFO = "info"        # Logged only

@dataclass(frozen=True)
class ValidationRule:
    """Immutable validation rule (hashable, can be used in sets)."""
    field_name: str
    rule_name: str
    severity: Severity
    validator: Callable[[Any], bool]
    message: str

@dataclass
class ValidationResult:
    """Mutable result accumulator."""
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

class SchemaValidator:
    """
    Validates records against a set of rules.
    Uses defaultdict for efficient per-field rule grouping.
    """
    
    def __init__(self):
        # Group rules by field for efficient lookup
        self._rules: Dict[str, List[ValidationRule]] = defaultdict(list)
        # Track error frequencies using Counter pattern
        self._error_counts: Dict[str, int] = defaultdict(int)
    
    def add_rule(self, rule: ValidationRule):
        self._rules[rule.field_name].append(rule)
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        for field_name, rules in self._rules.items():
            value = record.get(field_name)
            
            for rule in rules:
                if not rule.validator(value):
                    self._error_counts[f"{field_name}.{rule.rule_name}"] += 1
                    
                    if rule.severity == Severity.ERROR:
                        result.is_valid = False
                        result.errors.append(
                            f"[{field_name}] {rule.message} (got: {repr(value)[:50]})"
                        )
                    elif rule.severity == Severity.WARNING:
                        result.warnings.append(f"[{field_name}] {rule.message}")
        
        return result
    
    def get_error_summary(self) -> List[Tuple[str, int]]:
        """Return errors sorted by frequency (most common first)."""
        return sorted(self._error_counts.items(), key=lambda x: -x[1])

# Build validator for user events pipeline
validator = SchemaValidator()
validator.add_rule(ValidationRule(
    field_name="user_id",
    rule_name="not_null",
    severity=Severity.ERROR,
    validator=lambda v: v is not None and v != "",
    message="user_id is required"
))
validator.add_rule(ValidationRule(
    field_name="email",
    rule_name="valid_format",
    severity=Severity.WARNING,
    validator=lambda v: v is None or re.match(r'^[^@]+@[^@]+\.[^@]+$', str(v)),
    message="email format is invalid"
))
validator.add_rule(ValidationRule(
    field_name="event_type",
    rule_name="allowed_values",
    severity=Severity.ERROR,
    validator=lambda v: v in {"click", "view", "purchase", "signup"},
    message="event_type must be one of: click, view, purchase, signup"
))
```

## Example 3: Efficient Partition State Tracker

```python
"""
Tracks ETL pipeline partition state using memory-efficient data structures.
Handles 100K+ partitions across multiple tables.
"""
from dataclasses import dataclass, field
from typing import Dict, Set, Optional, FrozenSet
from enum import Enum, auto
from datetime import datetime, date
from collections import OrderedDict

class PartitionStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()
    STALE = auto()  # Needs reprocessing

@dataclass(frozen=True)  # Immutable → hashable → can use in sets
class PartitionKey:
    """Lightweight, immutable partition identifier."""
    table: str
    date: date
    region: str = "global"
    
    def __str__(self):
        return f"{self.table}/{self.date}/{self.region}"

@dataclass
class PartitionMetadata:
    """Mutable metadata for a partition."""
    status: PartitionStatus = PartitionStatus.PENDING
    row_count: int = 0
    byte_size: int = 0
    last_updated: Optional[datetime] = None
    retry_count: int = 0
    checksum: str = ""

class PartitionStateTracker:
    """
    Memory-efficient partition state management.
    
    Design decisions:
    - FrozenSet for completed partitions (immutable, fast lookup)
    - OrderedDict for in-progress (preserves processing order, O(1) lookup)
    - defaultdict(set) for per-table grouping
    """
    
    def __init__(self, max_in_progress: int = 100):
        self._metadata: Dict[PartitionKey, PartitionMetadata] = {}
        self._by_status: Dict[PartitionStatus, Set[PartitionKey]] = defaultdict(set)
        self._by_table: Dict[str, Set[PartitionKey]] = defaultdict(set)
        self._processing_order: OrderedDict[PartitionKey, datetime] = OrderedDict()
        self._max_in_progress = max_in_progress
    
    def register(self, key: PartitionKey):
        """Register a new partition for processing."""
        if key not in self._metadata:
            self._metadata[key] = PartitionMetadata()
            self._by_status[PartitionStatus.PENDING].add(key)
            self._by_table[key.table].add(key)
    
    def start_processing(self, key: PartitionKey) -> bool:
        """Attempt to start processing. Returns False if at capacity."""
        if len(self._processing_order) >= self._max_in_progress:
            return False
        
        meta = self._metadata.get(key)
        if not meta:
            return False
        
        self._by_status[meta.status].discard(key)
        meta.status = PartitionStatus.IN_PROGRESS
        meta.last_updated = datetime.now()
        self._by_status[PartitionStatus.IN_PROGRESS].add(key)
        self._processing_order[key] = datetime.now()
        return True
    
    def complete(self, key: PartitionKey, row_count: int, checksum: str):
        """Mark partition as completed."""
        meta = self._metadata.get(key)
        if not meta:
            return
        
        self._by_status[meta.status].discard(key)
        meta.status = PartitionStatus.COMPLETED
        meta.row_count = row_count
        meta.checksum = checksum
        meta.last_updated = datetime.now()
        self._by_status[PartitionStatus.COMPLETED].add(key)
        self._processing_order.pop(key, None)
    
    def get_pending(self, table: Optional[str] = None, limit: int = 50) -> list:
        """Get pending partitions, optionally filtered by table."""
        pending = self._by_status[PartitionStatus.PENDING]
        if table:
            pending = pending & self._by_table[table]
        return sorted(pending, key=lambda k: k.date)[:limit]
    
    def get_stale(self, max_age_minutes: int = 60) -> Set[PartitionKey]:
        """Find partitions stuck in processing too long."""
        cutoff = datetime.now()
        stale = set()
        for key, started_at in self._processing_order.items():
            elapsed = (cutoff - started_at).total_seconds() / 60
            if elapsed > max_age_minutes:
                stale.add(key)
        return stale

# Usage
tracker = PartitionStateTracker(max_in_progress=50)

# Register today's partitions
for region in ["us-east", "us-west", "eu-west"]:
    key = PartitionKey(table="events", date=date.today(), region=region)
    tracker.register(key)
    tracker.start_processing(key)

# Check for stuck partitions
stale = tracker.get_stale(max_age_minutes=30)
for key in stale:
    print(f"ALERT: Partition {key} has been processing for >30 minutes")
```

## Production Design Principles

### 1. Immutability for Safety
Use `frozen=True` dataclasses or tuples for data that flows between pipeline stages. Prevents accidental mutation bugs that are hard to debug at scale.

### 2. Generators for Memory
If you can process records one-at-a-time, use generators. A pipeline processing 100M records should use constant memory, not linear.

### 3. Sets for Reconciliation
Any time you need to compare "what's in source vs target," use set operations. They're the most expressive and efficient way to express data drift.

### 4. defaultdict for Aggregation
Any counting, grouping, or accumulation pattern should use `defaultdict`. It eliminates the "check if key exists" boilerplate and is slightly faster.

## Interview Tip 💡

> Production DE work is about **composing** the right data structures for the problem. Show you can pick complementary structures: "I'd use a Bloom filter for the fast path, backed by a set for confirmation, and track metrics in a defaultdict(int)." This demonstrates systems thinking, not just textbook knowledge.
