---
title: "Python File I/O - Senior Deep Dive"
topic: python
subtopic: file-io
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, file-io, s3, boto3, memory-mapped, async-io, file-formats, performance]
---

# Python File I/O — Senior Deep Dive

## S3 I/O Patterns with boto3

### Streaming S3 Reads (No Full Download)

```python
import boto3
import json
from typing import Iterator

def stream_s3_jsonl(bucket: str, key: str) -> Iterator[dict]:
    """
    Stream JSONL from S3 without downloading the full file.
    Memory usage: O(1) per record regardless of file size.
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    
    # StreamingBody supports iteration
    for line in response["Body"].iter_lines():
        if line:
            yield json.loads(line.decode("utf-8"))

def stream_s3_csv(bucket: str, key: str) -> Iterator[dict]:
    """Stream CSV from S3 line by line."""
    import csv
    import io
    
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    
    # Wrap streaming body in text wrapper for csv module
    lines = (line.decode("utf-8") for line in response["Body"].iter_lines())
    reader = csv.DictReader(lines)
    yield from reader
```

### Multipart Upload for Large Files

```python
import boto3
from boto3.s3.transfer import TransferConfig

def upload_large_file(local_path: str, bucket: str, key: str):
    """
    Multipart upload — splits file into parts for parallel upload.
    Essential for files > 100MB.
    """
    s3 = boto3.client("s3")
    
    config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,  # 100MB before multipart
        max_concurrency=10,
        multipart_chunksize=50 * 1024 * 1024,   # 50MB chunks
    )
    
    s3.upload_file(local_path, bucket, key, Config=config)

def streaming_upload(data_iterator: Iterator[bytes], bucket: str, key: str):
    """Upload streaming data without writing to disk first."""
    s3 = boto3.client("s3")
    
    # Initiate multipart upload
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1
    buffer = b""
    min_part_size = 5 * 1024 * 1024  # S3 minimum: 5MB per part
    
    try:
        for chunk in data_iterator:
            buffer += chunk
            
            if len(buffer) >= min_part_size:
                response = s3.upload_part(
                    Bucket=bucket, Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=buffer
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                part_number += 1
                buffer = b""
        
        # Upload remaining buffer
        if buffer:
            response = s3.upload_part(
                Bucket=bucket, Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=buffer
            )
            parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
        
        # Complete the upload
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
```

---

## Memory-Mapped Files

Memory mapping lets the OS manage file-to-memory mapping efficiently. Useful for random access patterns on large files:

```python
import mmap
import struct

def search_binary_log(filepath: str, target_timestamp: int) -> bytes:
    """
    Binary search through a sorted log file using memory mapping.
    The OS pages data in/out as needed — never loads full file.
    """
    with open(filepath, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        file_size = mm.size()
        record_size = 128  # Fixed-size records
        num_records = file_size // record_size
        
        # Binary search for timestamp
        lo, hi = 0, num_records - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            offset = mid * record_size
            ts = struct.unpack_from("Q", mm, offset)[0]  # 8-byte unsigned int
            
            if ts == target_timestamp:
                return mm[offset:offset + record_size]
            elif ts < target_timestamp:
                lo = mid + 1
            else:
                hi = mid - 1
        
        mm.close()
        return None

def count_pattern_mmap(filepath: str, pattern: bytes) -> int:
    """Count occurrences of a pattern in a file using mmap."""
    with open(filepath, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        count = 0
        start = 0
        while True:
            pos = mm.find(pattern, start)
            if pos == -1:
                break
            count += 1
            start = pos + 1
        mm.close()
        return count
```

---

## Async File I/O with aiofiles

For I/O-bound workloads where you process many files concurrently:

```python
import asyncio
import aiofiles
import json
from pathlib import Path
from typing import List, Dict

async def read_json_async(filepath: str) -> dict:
    """Non-blocking file read."""
    async with aiofiles.open(filepath, "r") as f:
        content = await f.read()
        return json.loads(content)

async def process_files_concurrently(filepaths: List[str]) -> List[Dict]:
    """Process multiple files in parallel using async I/O."""
    tasks = [read_json_async(fp) for fp in filepaths]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful = []
    for filepath, result in zip(filepaths, results):
        if isinstance(result, Exception):
            print(f"Failed to read {filepath}: {result}")
        else:
            successful.append(result)
    
    return successful

async def stream_write_async(records: List[dict], filepath: str):
    """Async streaming write for JSONL."""
    async with aiofiles.open(filepath, "w") as f:
        for record in records:
            await f.write(json.dumps(record) + "\n")

# Run concurrent file processing
async def main():
    files = list(Path("data/daily/").glob("*.json"))
    results = await process_files_concurrently([str(f) for f in files])
    print(f"Processed {len(results)} files")

asyncio.run(main())
```

---

## File Format Performance Comparison

```python
"""
Benchmark comparison of file formats for a typical DE workload.
Dataset: 1M records, 20 columns (mixed types).
"""

# Results (approximate, typical hardware):
# Format        | Write Time | Read Time | File Size | Column Read |
# --------------|-----------|-----------|-----------|-------------|
# CSV           | 4.2s      | 3.8s     | 450 MB    | 3.8s (full) |
# CSV + gzip    | 8.1s      | 5.2s     | 85 MB     | 5.2s (full) |
# JSON Lines    | 6.5s      | 5.1s     | 620 MB    | 5.1s (full) |
# Parquet snappy| 1.8s      | 0.9s     | 95 MB     | 0.1s (1 col)|
# Parquet zstd  | 2.5s      | 1.0s     | 72 MB     | 0.1s (1 col)|

# Key insight: Parquet's columnar format means reading 1 column
# from a 20-column table is 10-40x faster than row-based formats.

import time

def benchmark_formats(records: list[dict]):
    """Compare write/read performance across formats."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import csv
    import json
    
    results = {}
    
    # CSV
    start = time.time()
    with open("/tmp/bench.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    results["csv_write"] = time.time() - start
    
    # Parquet
    start = time.time()
    table = pa.Table.from_pylist(records)
    pq.write_table(table, "/tmp/bench.parquet", compression="snappy")
    results["parquet_write"] = time.time() - start
    
    # Read single column — Parquet
    start = time.time()
    pq.read_table("/tmp/bench.parquet", columns=["user_id"])
    results["parquet_single_col"] = time.time() - start
    
    return results
```

---

## S3 Select — Query in Place

```python
import boto3
import json

def query_s3_csv_in_place(bucket: str, key: str, sql: str) -> list[dict]:
    """
    Use S3 Select to filter data server-side.
    Only matching rows are transferred — saves bandwidth and time.
    """
    s3 = boto3.client("s3")
    
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression=sql,
        InputSerialization={"CSV": {"FileHeaderInfo": "USE", "RecordDelimiter": "\n"}},
        OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
    )
    
    records = []
    for event in response["Payload"]:
        if "Records" in event:
            payload = event["Records"]["Payload"].decode("utf-8")
            for line in payload.strip().split("\n"):
                if line:
                    records.append(json.loads(line))
    
    return records

# Usage — filter 10GB CSV, only transfer matching rows
results = query_s3_csv_in_place(
    bucket="data-lake",
    key="raw/events/2024-01-15.csv",
    sql="SELECT * FROM s3object WHERE amount > 100 AND event_type = 'purchase'"
)
```

---

## File Locking for Concurrent Access

```python
import fcntl
from contextlib import contextmanager

@contextmanager
def file_lock(filepath: str, timeout: float = 30.0):
    """
    Advisory file lock for preventing concurrent writes.
    Use case: Multiple pipeline workers writing to shared files.
    """
    import time
    lock_path = f"{filepath}.lock"
    lock_file = open(lock_path, "w")
    start = time.time()
    
    while True:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except IOError:
            if time.time() - start >= timeout:
                lock_file.close()
                raise TimeoutError(f"Could not acquire lock on {filepath}")
            time.sleep(0.1)
    
    try:
        yield filepath
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()

# Usage
with file_lock("output/metrics.json"):
    data = json.loads(Path("output/metrics.json").read_text())
    data["last_updated"] = datetime.now().isoformat()
    Path("output/metrics.json").write_text(json.dumps(data))
```

---

## Interview Tips

> **Tip 1:** For S3 I/O questions, demonstrate streaming awareness: "I'd use `iter_lines()` on the StreamingBody to process S3 files line-by-line without downloading to disk. For uploads > 100MB, I'd use multipart upload with 50MB chunks and 10 concurrent parts. This handles network interruptions gracefully since individual parts can be retried."

> **Tip 2:** Memory-mapped files are a power answer for "random access on huge files." Explain: "mmap lets the OS manage paging — only the accessed pages are loaded into memory. This enables binary search on a 100GB sorted file with O(log n) I/O operations, using only a few KB of actual memory."

> **Tip 3:** Know the file format decision tree: "For analytics queries — Parquet (columnar, compressed, fast column reads). For streaming/append workloads — JSONL (append-friendly, human-readable). For interchange — CSV with explicit schema documentation. For archival — Parquet with zstd compression for best size."

## ⚡ Cheat Sheet

**File Format Decision Tree**
| Use Case | Format | Why |
|----------|--------|-----|
| Analytics / column reads | Parquet (snappy/zstd) | Columnar, compressed, 10–40× faster column scan |
| Streaming / append logs | JSONL | Append-friendly, human-readable, one record per line |
| Interchange / external | CSV | Universal, human-readable, needs explicit schema |
| Archival / cold storage | Parquet zstd | Best compression ratio |
| Machine-to-machine binary | Protocol Buffers / Avro | Schema-enforced, compact |

**Parquet Key Numbers**
- 1 column read from 20-column table: 0.1 s vs 3.8 s for CSV (38× faster)
- snappy: faster compress/decompress; zstd: better ratio (preferred for archival)
- S3 Select works on Parquet and CSV — filter server-side before transfer

**S3 I/O Patterns**
- Stream read: `response["Body"].iter_lines()` — O(1) memory, no temp file
- Multipart upload threshold: `100 MB`; chunk size: `50 MB`; concurrency: `10`
- Min part size: `5 MB` (S3 hard limit per part)
- `s3.abort_multipart_upload()` in `except` block — prevents orphaned uploads billing

**Memory-Mapped Files**
- `mmap.mmap(f.fileno(), 0, access=ACCESS_READ)` — OS pages data in/out on demand
- Binary search on 100 GB sorted file: O(log n) I/O, constant Python memory
- `mm.find(pattern, start)` — fast byte-level search without loading file
- Not useful for write-heavy workloads on large files (copy-on-write overhead)

**S3 Select**
- Filter 10 GB CSV: only matching rows transferred — saves bandwidth + Athena cost
- `ExpressionType="SQL"`, `InputSerialization={"CSV": {"FileHeaderInfo": "USE"}}`
- Output as JSON lines for easy parsing; limited SQL (no JOINs, subqueries)

**Async File I/O**
- `aiofiles.open()` — non-blocking; other coroutines run during disk I/O wait
- Use when processing many files concurrently (100+ files in parallel)
- `asyncio.gather(*[read_json_async(fp) for fp in files], return_exceptions=True)` — collect all

**File Locking**
- `fcntl.flock(LOCK_EX | LOCK_NB)` — advisory exclusive lock, non-blocking attempt
- Retry loop with timeout; release in `finally` block
- Only works on same host (NFS locks are unreliable — use DB-based locks for distributed)
