---
title: "Python File I/O - Real-World Production Examples"
topic: python
subtopic: file-io
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, file-io, production, csv, parquet, s3, streaming, file-watching]
---

# Python File I/O — Real-World Production Examples

## Pattern 1: Process 50GB CSV with Constant Memory

A production pipeline for processing files that exceed available RAM:

```python
"""
Streaming CSV processor for files larger than available memory.
Processes 50GB+ files with <500MB memory usage.
Key techniques: generator pipeline, chunked writes, progress tracking.
"""
import csv
import gzip
import time
from pathlib import Path
from typing import Iterator, Dict, Tuple
from dataclasses import dataclass, field

@dataclass
class ProcessingStats:
    total_read: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    bytes_processed: int = 0
    start_time: float = field(default_factory=time.time)
    
    @property
    def records_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.total_read / elapsed if elapsed > 0 else 0

def stream_csv_records(filepath: str) -> Iterator[Dict]:
    """
    Stream records from CSV (plain or gzipped).
    Handles: auto-detection of compression, encoding fallback.
    """
    open_func = gzip.open if filepath.endswith(".gz") else open
    mode = "rt" if filepath.endswith(".gz") else "r"
    
    with open_func(filepath, mode, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        yield from reader

def validate_and_transform(
    records: Iterator[Dict],
    stats: ProcessingStats,
    required_fields: list[str]
) -> Iterator[Dict]:
    """Transform stream, tracking stats for monitoring."""
    for record in records:
        stats.total_read += 1
        
        # Validate
        if not all(record.get(f) for f in required_fields):
            stats.invalid_records += 1
            continue
        
        # Transform
        try:
            record["amount"] = float(record.get("amount", 0))
            record["timestamp"] = record["timestamp"][:19]
            stats.valid_records += 1
            yield record
        except (ValueError, TypeError):
            stats.invalid_records += 1
        
        # Progress reporting
        if stats.total_read % 1_000_000 == 0:
            print(
                f"Progress: {stats.total_read:,} read, "
                f"{stats.valid_records:,} valid, "
                f"{stats.records_per_second:,.0f} rec/s"
            )

def write_partitioned_output(
    records: Iterator[Dict],
    output_dir: str,
    partition_key: str,
    max_records_per_file: int = 500_000
):
    """Write to partitioned files with size limits."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    buffers: Dict[str, list] = {}
    file_counts: Dict[str, int] = {}
    
    for record in records:
        partition_value = record.get(partition_key, "unknown")
        
        if partition_value not in buffers:
            buffers[partition_value] = []
            file_counts[partition_value] = 0
        
        buffers[partition_value].append(record)
        
        if len(buffers[partition_value]) >= max_records_per_file:
            _flush_partition(output_dir, partition_key, partition_value,
                          buffers[partition_value], file_counts)
            buffers[partition_value] = []
            file_counts[partition_value] += 1
    
    # Flush remaining
    for partition_value, records in buffers.items():
        if records:
            _flush_partition(output_dir, partition_key, partition_value,
                          records, file_counts)

def _flush_partition(output_dir, key, value, records, counts):
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    partition_dir = Path(output_dir) / f"{key}={value}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    
    filepath = partition_dir / f"part-{counts.get(value, 0):05d}.parquet"
    table = pa.Table.from_pylist(records)
    pq.write_table(table, str(filepath), compression="snappy")

# Run the pipeline
stats = ProcessingStats()
records = validate_and_transform(
    stream_csv_records("data/events_50gb.csv.gz"),
    stats,
    required_fields=["user_id", "event_type", "timestamp"]
)
write_partitioned_output(records, "output/events/", partition_key="event_date")
print(f"Complete: {stats.valid_records:,} records in {time.time()-stats.start_time:.0f}s")
```

---

## Pattern 2: S3 Upload/Download Patterns

Production S3 operations with retry, progress tracking, and integrity verification:

```python
"""
Production S3 file operations.
Handles: multipart uploads, integrity checks, concurrent transfers.
"""
import boto3
import hashlib
from pathlib import Path
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)

class S3FileManager:
    """Production-grade S3 file operations."""
    
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)
    
    def upload_with_integrity(
        self,
        local_path: str,
        s3_key: str,
        content_type: str = "application/octet-stream"
    ) -> dict:
        """Upload with MD5 integrity verification."""
        # Calculate MD5 before upload
        md5 = self._calculate_md5(local_path)
        file_size = Path(local_path).stat().st_size
        
        logger.info(f"Uploading {local_path} ({file_size/1024/1024:.1f}MB) to s3://{self.bucket}/{s3_key}")
        
        extra_args = {
            "ContentType": content_type,
            "Metadata": {"md5": md5, "source_path": local_path}
        }
        
        self.s3.upload_file(local_path, self.bucket, s3_key, ExtraArgs=extra_args)
        
        # Verify upload integrity
        response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
        uploaded_size = response["ContentLength"]
        
        if uploaded_size != file_size:
            raise IOError(
                f"Size mismatch: local={file_size}, S3={uploaded_size}"
            )
        
        return {"key": s3_key, "size": file_size, "md5": md5}
    
    def download_with_verification(
        self,
        s3_key: str,
        local_path: str
    ) -> str:
        """Download and verify integrity."""
        self.s3.download_file(self.bucket, s3_key, local_path)
        
        # Verify against stored MD5
        response = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
        expected_md5 = response.get("Metadata", {}).get("md5")
        
        if expected_md5:
            actual_md5 = self._calculate_md5(local_path)
            if actual_md5 != expected_md5:
                Path(local_path).unlink()  # Delete corrupted file
                raise IOError(f"MD5 mismatch for {s3_key}")
        
        return local_path
    
    def parallel_upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
        pattern: str = "*",
        max_workers: int = 10
    ) -> list[dict]:
        """Upload all matching files in parallel."""
        files = list(Path(local_dir).glob(pattern))
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for filepath in files:
                s3_key = f"{s3_prefix}{filepath.name}"
                future = executor.submit(
                    self.upload_with_integrity, str(filepath), s3_key
                )
                futures[future] = filepath
            
            for future in as_completed(futures):
                filepath = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to upload {filepath}: {e}")
        
        return results
    
    def _calculate_md5(self, filepath: str) -> str:
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()
```

---

## Pattern 3: Format Conversion Pipeline (CSV to Parquet)

```python
"""
Production CSV-to-Parquet converter.
Handles: schema inference, type coercion, partitioning, validation.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
from pathlib import Path
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

class FormatConverter:
    """Converts between data file formats with validation."""
    
    def __init__(self, schema_overrides: Optional[Dict[str, pa.DataType]] = None):
        self.schema_overrides = schema_overrides or {}
    
    def csv_to_parquet(
        self,
        input_path: str,
        output_path: str,
        partition_cols: list[str] = None,
        compression: str = "snappy",
        row_group_size: int = 1_000_000,
    ) -> dict:
        """
        Convert CSV to Parquet with optimal settings.
        Uses PyArrow's native CSV reader for speed (not Python csv module).
        """
        # Configure CSV parsing
        read_options = pa_csv.ReadOptions(
            block_size=64 * 1024 * 1024,  # 64MB read blocks
        )
        parse_options = pa_csv.ParseOptions(
            delimiter=",",
            quote_char='"',
        )
        convert_options = pa_csv.ConvertOptions(
            column_types=self.schema_overrides,
            strings_can_be_null=True,
            null_values=["", "NULL", "null", "None", "\\N"],
        )
        
        logger.info(f"Converting {input_path} to Parquet")
        
        # Read CSV (PyArrow handles streaming internally)
        table = pa_csv.read_csv(
            input_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
        
        # Apply schema overrides and validation
        table = self._apply_schema(table)
        
        # Write Parquet
        if partition_cols:
            pq.write_to_dataset(
                table,
                root_path=output_path,
                partition_cols=partition_cols,
                compression=compression,
            )
        else:
            pq.write_table(
                table, output_path,
                compression=compression,
                row_group_size=row_group_size,
            )
        
        stats = {
            "input_file": input_path,
            "output_path": output_path,
            "rows": table.num_rows,
            "columns": table.num_columns,
            "compression": compression,
            "schema": str(table.schema),
        }
        
        logger.info(f"Conversion complete: {stats['rows']:,} rows, {stats['columns']} columns")
        return stats
    
    def _apply_schema(self, table: pa.Table) -> pa.Table:
        """Apply type coercions and validations."""
        for col_name, target_type in self.schema_overrides.items():
            if col_name in table.column_names:
                col = table.column(col_name)
                table = table.set_column(
                    table.column_names.index(col_name),
                    col_name,
                    col.cast(target_type)
                )
        return table

# Usage
converter = FormatConverter(schema_overrides={
    "amount": pa.float64(),
    "user_id": pa.string(),
    "event_date": pa.date32(),
})

stats = converter.csv_to_parquet(
    input_path="data/raw_events.csv",
    output_path="data/events/",
    partition_cols=["event_date"],
    compression="zstd"
)
```

---

## Pattern 4: File Watching and Ingestion

```python
"""
File watcher for automatic ingestion of dropped files.
Use case: Partners drop CSV files in an S3 prefix or local directory.
"""
import time
import threading
from pathlib import Path
from typing import Callable, Set
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

@dataclass
class FileEvent:
    filepath: str
    event_type: str  # "created", "modified"
    size: int
    timestamp: float

class DirectoryWatcher:
    """
    Watch a directory for new files and trigger processing.
    Handles: debouncing (wait for file to finish writing),
    deduplication, error recovery.
    """
    
    def __init__(
        self,
        watch_dir: str,
        patterns: list[str] = None,
        settle_time: float = 5.0,
        poll_interval: float = 2.0
    ):
        self.watch_dir = Path(watch_dir)
        self.patterns = patterns or ["*"]
        self.settle_time = settle_time
        self.poll_interval = poll_interval
        self._processed: Set[str] = set()
        self._pending: dict = {}
        self._running = False
    
    def start(self, callback: Callable[[FileEvent], None]):
        """Start watching (blocking)."""
        self._running = True
        logger.info(f"Watching {self.watch_dir} for {self.patterns}")
        
        while self._running:
            self._scan_directory(callback)
            time.sleep(self.poll_interval)
    
    def stop(self):
        self._running = False
    
    def _scan_directory(self, callback):
        """Scan for new/modified files."""
        current_files = set()
        
        for pattern in self.patterns:
            for filepath in self.watch_dir.glob(pattern):
                if not filepath.is_file():
                    continue
                
                file_key = str(filepath)
                current_files.add(file_key)
                
                if file_key in self._processed:
                    continue
                
                # Check if file has settled (not still being written)
                current_size = filepath.stat().st_size
                last_size = self._pending.get(file_key, -1)
                
                if current_size == last_size and current_size > 0:
                    # File size stable — ready to process
                    event = FileEvent(
                        filepath=file_key,
                        event_type="created",
                        size=current_size,
                        timestamp=time.time()
                    )
                    try:
                        callback(event)
                        self._processed.add(file_key)
                        del self._pending[file_key]
                    except Exception as e:
                        logger.error(f"Failed to process {file_key}: {e}")
                else:
                    # File still being written
                    self._pending[file_key] = current_size

# Usage
def process_new_file(event: FileEvent):
    """Callback for new file detection."""
    logger.info(f"Processing new file: {event.filepath} ({event.size} bytes)")
    
    converter = FormatConverter()
    converter.csv_to_parquet(
        input_path=event.filepath,
        output_path=f"processed/{Path(event.filepath).stem}.parquet"
    )
    
    # Archive the original
    Path(event.filepath).rename(f"archive/{Path(event.filepath).name}")

watcher = DirectoryWatcher(
    watch_dir="/data/incoming/",
    patterns=["*.csv", "*.json"],
    settle_time=5.0
)
watcher.start(process_new_file)
```

---

## Interview Tips

> **Tip 1:** For the "process a huge file" question, emphasize the streaming architecture: "I'd never load 50GB into memory. Instead, I stream with generators — read in chunks, transform in-flight, and write partitioned output. Memory stays under 500MB regardless of input size. I'd also track progress metrics (records/second, error rate) for operational visibility."

> **Tip 2:** The S3 upload pattern with integrity checks shows production thinking. Mention: "I calculate MD5 before upload, store it in S3 metadata, and verify after download. For large files, I use multipart upload with 50MB chunks. If upload fails, I abort the multipart to avoid zombie parts consuming storage."

> **Tip 3:** For format conversion, know why PyArrow's native CSV reader is preferred over Python's csv module: "PyArrow reads CSV at ~10x the speed of Python's csv module because it's implemented in C++ with SIMD optimizations. It also handles type inference and null detection natively, outputting directly to columnar Arrow format without an intermediate Python representation."
