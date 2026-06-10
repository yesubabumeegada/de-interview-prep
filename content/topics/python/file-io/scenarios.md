---
title: "Python File I/O - Scenario Questions"
topic: python
subtopic: file-io
content_type: scenario_question
tags: [python, file-io, interview, scenarios, csv, streaming, file-processor]
---

# Scenario Questions — Python File I/O

<article data-difficulty="junior">

## Junior: Read CSV to Dictionary

**Scenario:** Write a function that reads a CSV file and returns a dictionary mapping user_id to their total order amount. Handle missing values and type conversion errors gracefully.

```csv
user_id,order_id,amount,status
u1,o101,99.99,completed
u2,o102,invalid,completed
u1,o103,45.50,completed
u3,o104,,cancelled
u2,o105,200.00,completed
```

<details>
<summary>Solution</summary>

```python
import csv
from collections import defaultdict
from typing import Dict

def aggregate_user_totals(filepath: str) -> Dict[str, float]:
    """
    Read CSV and aggregate total amounts per user.
    Skips invalid/missing amounts. Only counts completed orders.
    """
    totals = defaultdict(float)
    errors = 0
    
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Skip non-completed orders
            if row.get("status") != "completed":
                continue
            
            # Safely parse amount
            try:
                amount = float(row["amount"])
                totals[row["user_id"]] += amount
            except (ValueError, TypeError, KeyError):
                errors += 1
                continue
    
    if errors:
        print(f"Warning: {errors} records had invalid amounts")
    
    return dict(totals)

# Result: {"u1": 145.49, "u2": 200.00}
# u3 excluded (cancelled), one u2 record skipped (invalid amount)
```

</details>

</article>

<article data-difficulty="mid-level">

## Mid-Level: Stream a Large File with Generator

**Scenario:** You have a 20GB CSV log file. Write a generator-based solution that:
1. Streams the file without loading into memory
2. Filters to only ERROR-level log entries
3. Batches results in groups of 1000 for database insertion
4. Reports progress every 1M lines

<details>
<summary>Solution</summary>

```python
import csv
import time
from typing import Iterator, Dict
from pathlib import Path

def stream_log_file(filepath: str) -> Iterator[Dict]:
    """Stream CSV log file line by line — O(1) memory."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        yield from reader

def filter_errors(records: Iterator[Dict]) -> Iterator[Dict]:
    """Keep only ERROR-level entries."""
    for record in records:
        if record.get("level", "").upper() == "ERROR":
            yield record

def with_progress(records: Iterator[Dict], report_every: int = 1_000_000) -> Iterator[Dict]:
    """Add progress reporting to a stream."""
    count = 0
    start = time.time()
    
    for record in records:
        count += 1
        if count % report_every == 0:
            elapsed = time.time() - start
            rate = count / elapsed
            print(f"Processed {count:,} lines ({rate:,.0f} lines/sec)")
        yield record
    
    print(f"Total: {count:,} lines in {time.time()-start:.1f}s")

def batch(records: Iterator[Dict], size: int = 1000) -> Iterator[list[Dict]]:
    """Collect records into fixed-size batches."""
    batch_buffer = []
    for record in records:
        batch_buffer.append(record)
        if len(batch_buffer) >= size:
            yield batch_buffer
            batch_buffer = []
    if batch_buffer:
        yield batch_buffer

# Compose the pipeline
def process_large_log(filepath: str, db_connection):
    """Full pipeline: stream -> filter -> batch -> insert."""
    pipeline = batch(
        filter_errors(
            with_progress(
                stream_log_file(filepath)
            )
        ),
        size=1000
    )
    
    total_inserted = 0
    for error_batch in pipeline:
        db_connection.executemany(
            "INSERT INTO error_logs (timestamp, message, source) VALUES (%s, %s, %s)",
            [(r["timestamp"], r["message"], r["source"]) for r in error_batch]
        )
        db_connection.commit()
        total_inserted += len(error_batch)
    
    print(f"Inserted {total_inserted:,} error records to database")
    return total_inserted
```

**Key points:**
- Memory stays constant regardless of file size
- Each generator holds at most 1 record (or 1 batch)
- Progress reporting doesn't interfere with pipeline logic
- Batching amortizes DB overhead (1 commit per 1000 records)

</details>

</article>

<article data-difficulty="senior">

## Senior: Design a Multi-Format File Processor

**Scenario:** Design a file processing system that:
1. Accepts CSV, JSON, JSONL, Parquet, and gzipped variants
2. Auto-detects format from extension
3. Streams records with constant memory
4. Normalizes output to a common schema
5. Is extensible for new formats without modifying existing code

<details>
<summary>Solution</summary>

```python
"""
Extensible multi-format file processor using Strategy + Factory patterns.
Adding a new format requires only one new class — zero existing code changes.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Dict, Optional, List
import logging

logger = logging.getLogger(__name__)

# Registry for format handlers
_format_registry: Dict[str, type] = {}

def register_format(*extensions):
    """Decorator to register a format handler for given extensions."""
    def decorator(cls):
        for ext in extensions:
            _format_registry[ext.lower()] = cls
        return cls
    return decorator

class FormatHandler(ABC):
    """Base interface for all file format handlers."""
    
    @abstractmethod
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        """Yield records one at a time from the file."""
        ...
    
    @abstractmethod
    def get_schema(self, filepath: str) -> List[str]:
        """Return column names without reading full file."""
        ...

@register_format(".csv")
class CSVHandler(FormatHandler):
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        import csv
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            yield from csv.DictReader(f)
    
    def get_schema(self, filepath: str) -> List[str]:
        import csv
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            return next(reader)

@register_format(".csv.gz", ".csv.gzip")
class GzippedCSVHandler(FormatHandler):
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        import csv, gzip
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            yield from csv.DictReader(f)
    
    def get_schema(self, filepath: str) -> List[str]:
        import csv, gzip
        with gzip.open(filepath, "rt") as f:
            return next(csv.reader(f))

@register_format(".json")
class JSONHandler(FormatHandler):
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        import json
        with open(filepath, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                yield from data
            else:
                yield data
    
    def get_schema(self, filepath: str) -> List[str]:
        record = next(self.stream_records(filepath))
        return list(record.keys())

@register_format(".jsonl", ".ndjson")
class JSONLHandler(FormatHandler):
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        import json
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    
    def get_schema(self, filepath: str) -> List[str]:
        record = next(self.stream_records(filepath))
        return list(record.keys())

@register_format(".parquet")
class ParquetHandler(FormatHandler):
    def stream_records(self, filepath: str) -> Iterator[Dict]:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(filepath)
        for batch in pf.iter_batches(batch_size=10000):
            yield from batch.to_pylist()
    
    def get_schema(self, filepath: str) -> List[str]:
        import pyarrow.parquet as pq
        return pq.read_schema(filepath).names

class MultiFormatProcessor:
    """
    Unified file processing interface.
    Auto-detects format and streams records uniformly.
    """
    
    def __init__(self, schema_mapping: Optional[Dict[str, str]] = None):
        self.schema_mapping = schema_mapping or {}
    
    def process(self, filepath: str) -> Iterator[Dict]:
        """Stream records from any supported format."""
        handler = self._get_handler(filepath)
        logger.info(f"Processing {filepath} with {type(handler).__name__}")
        
        for record in handler.stream_records(filepath):
            yield self._normalize(record)
    
    def get_schema(self, filepath: str) -> List[str]:
        handler = self._get_handler(filepath)
        return handler.get_schema(filepath)
    
    def _get_handler(self, filepath: str) -> FormatHandler:
        """Detect format and return appropriate handler."""
        path = Path(filepath)
        
        # Check compound extensions first (.csv.gz)
        for ext_length in [2, 1]:
            ext = "".join(path.suffixes[-ext_length:]).lower()
            if ext in _format_registry:
                return _format_registry[ext]()
        
        raise ValueError(
            f"Unsupported format: {path.suffix}. "
            f"Supported: {list(_format_registry.keys())}"
        )
    
    def _normalize(self, record: Dict) -> Dict:
        """Apply schema mapping (rename columns)."""
        if not self.schema_mapping:
            return record
        return {
            self.schema_mapping.get(k, k): v
            for k, v in record.items()
        }

# Usage
processor = MultiFormatProcessor(schema_mapping={
    "userId": "user_id",      # Normalize different naming conventions
    "event_ts": "timestamp",
    "amt": "amount",
})

# Same interface regardless of format
for record in processor.process("data/events.csv.gz"):
    load_to_warehouse(record)

for record in processor.process("data/events.parquet"):
    load_to_warehouse(record)
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the preferred way to open a file in Python and why?**
A: Use the `with open(path, mode) as f:` context manager. It guarantees the file is closed when the block exits, even if an exception occurs—preventing file descriptor leaks in long-running processes.

**Q: What is the difference between text mode and binary mode when opening files?**
A: Text mode (`'r'`, `'w'`) decodes/encodes bytes using the platform's default encoding (or the `encoding` parameter) and translates newlines. Binary mode (`'rb'`, `'wb'`) reads/writes raw bytes without any encoding or newline translation—required for images, Parquet, compressed files, or any non-text format.

**Q: How do you efficiently read a large file line by line without loading it all into memory?**
A: Iterate directly over the file object: `for line in f:`. Python buffers reads internally and yields lines lazily—memory usage stays constant regardless of file size. Avoid `f.readlines()` which loads all lines into a list.

**Q: What is `pathlib.Path` and why is it preferred over `os.path`?**
A: `pathlib.Path` is an object-oriented, cross-platform path manipulation API. It provides intuitive operators (`/` for joining paths), attribute access (`.stem`, `.suffix`, `.parent`), and methods (`.glob()`, `.read_text()`, `.write_bytes()`) without the string manipulation boilerplate of `os.path`. It is the modern Python standard.

**Q: How do you handle CSV files efficiently in Python for data engineering tasks?**
A: Use `csv.DictReader` for simple CSV parsing (returns rows as dicts). For large files or data manipulation, use Pandas `pd.read_csv()` with appropriate `dtype`, `usecols`, and `chunksize` parameters. For highest performance, prefer Parquet over CSV—columnar storage with type metadata and compression avoids the parsing and type-inference overhead.

**Q: What is the `tempfile` module used for in data engineering?**
A: `tempfile.NamedTemporaryFile` and `tempfile.TemporaryDirectory` create temporary files/directories that are automatically deleted on context manager exit. In DE pipelines, use them for intermediate processing artifacts (unzipping an archive, staging a download before S3 upload) to avoid manual cleanup and avoid polluting shared directories.

**Q: How do you atomically write a file to avoid partial reads by concurrent processes?**
A: Write to a temporary file in the same filesystem, then `os.replace(tmp_path, final_path)`. `os.replace` is atomic on POSIX systems—readers either see the old complete file or the new complete file, never a partial write. This pattern is critical for pipelines where downstream systems poll for file completion.

**Q: How do you read and write compressed files (gzip, bzip2) in Python?**
A: Use `gzip.open(path, 'rt')` or `bz2.open(path, 'rt')` as drop-in replacements for `open()`. They support context manager protocol and streaming—you can iterate line by line over a gzipped file without decompressing the entire file into memory.

---

## 💼 Interview Tips

- Always use `with open(...)` in interviews—bare `open` without a context manager is an automatic red flag showing you do not handle resource cleanup.
- `pathlib` vs. `os.path` is a litmus test: using `pathlib.Path` signals modern Python fluency. Construct paths with `/` operator, use `.stem`/`.suffix`, and avoid `os.path.join` strings.
- Senior interviewers probe large-file handling: demonstrate that you know to stream line by line and never call `.readlines()` or `.read()` on multi-GB files.
- The atomic write pattern (`write to tmp → os.replace`) is a DE-specific requirement. Knowing it shows you think about data integrity under concurrent access—a signal of production experience.
- Connect file I/O to cloud storage: in modern DE, local file I/O often wraps `boto3.download_file` / `s3fs` / `fsspec`. Showing you understand that the same patterns apply (context managers, streaming, atomic writes) across local and cloud storage demonstrates transferable thinking.
