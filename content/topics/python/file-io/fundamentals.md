---
title: "Python File I/O - Fundamentals"
topic: python
subtopic: file-io
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, file-io, csv, json, pathlib, encoding, reading, writing]
---

# Python File I/O — Fundamentals


## 🎯 Analogy

Think of file I/O patterns like reading a newspaper: you can read the whole thing at once (small files), page by page (chunked reading for large files), or use a delivery service (streaming).

---
## Why File I/O Matters in Data Engineering

Every data pipeline starts with reading data and ends with writing it. Whether it's CSV exports, JSON API responses, or log files — understanding file I/O is the foundation of data processing.

**The analogy:** File I/O is like mail handling — you open the envelope (open file), read the contents (read data), process it, write a response (write data), and seal it (close file). Forgetting to close is like leaving the mailbox open in the rain.

---

## Reading Files — The Basics

```python
# ALWAYS use context managers (with) for file operations
# This guarantees the file is closed even if an exception occurs

# Read entire file as string
with open("data/config.txt", "r") as f:
    content = f.read()

# Read line by line (memory efficient for large files)
with open("data/events.log", "r") as f:
    for line in f:
        process_line(line.strip())

# Read all lines into a list
with open("data/small_file.txt", "r") as f:
    lines = f.readlines()  # Each line includes \n
```

### Writing Files

```python
# Write (overwrites existing content)
with open("output/results.txt", "w") as f:
    f.write("Pipeline completed successfully\n")
    f.write(f"Records processed: {count}\n")

# Append (adds to existing file)
with open("output/pipeline.log", "a") as f:
    f.write(f"[{timestamp}] Step completed\n")

# Write multiple lines
with open("output/ids.txt", "w") as f:
    f.writelines(f"{id}\n" for id in user_ids)
```

---

## CSV Files — The Data Engineer's Bread and Butter

```python
import csv

# Reading CSV as dictionaries (most common in DE)
def read_events_csv(filepath: str) -> list[dict]:
    """Read CSV file into list of dicts."""
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)

# All values are strings! You must cast.
records = read_events_csv("data/events.csv")
for record in records:
    record["amount"] = float(record["amount"])
    record["count"] = int(record["count"])

# Writing CSV from dicts
def write_events_csv(records: list[dict], filepath: str):
    """Write list of dicts to CSV."""
    if not records:
        return
    
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

# Handling edge cases
with open("messy_data.csv", "r", newline="") as f:
    reader = csv.DictReader(f, delimiter="|", quotechar='"')
    for row in reader:
        print(row)
```

---

## JSON Files

```python
import json

# Read JSON file
def load_config(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)

# Write JSON file (pretty-printed for readability)
def save_metrics(metrics: dict, filepath: str):
    with open(filepath, "w") as f:
        json.dump(metrics, f, indent=2, default=str)  # default=str handles datetimes

# JSON Lines (JSONL) — one JSON object per line
# Preferred format for streaming/large datasets
def read_jsonl(filepath: str) -> list[dict]:
    records = []
    with open(filepath, "r") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records

def write_jsonl(records: list[dict], filepath: str):
    with open(filepath, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
```

---

## pathlib — Modern Path Handling

```python
from pathlib import Path

# Create paths (works cross-platform)
data_dir = Path("data") / "raw" / "events"
output_file = Path("output") / "results.csv"

# Path operations
print(output_file.name)       # "results.csv"
print(output_file.stem)       # "results"
print(output_file.suffix)     # ".csv"
print(output_file.parent)     # Path("output")

# Check existence
if not data_dir.exists():
    data_dir.mkdir(parents=True)  # Create all intermediate directories

# List files
csv_files = list(data_dir.glob("*.csv"))          # Current directory only
all_csvs = list(data_dir.glob("**/*.csv"))        # Recursive
parquet_files = list(data_dir.glob("*.parquet"))

# Read/write shortcuts
content = Path("config.json").read_text()
Path("output.txt").write_text("done")

# Iterate over files in a directory
for filepath in sorted(Path("data/daily/").iterdir()):
    if filepath.suffix == ".csv":
        process_file(filepath)
```

---

## Encoding — Handling Text Properly

```python
# UTF-8 is the standard, but you'll encounter others
# Common in DE: latin-1 from legacy systems, utf-8-sig from Excel

# Detect and handle encoding issues
def safe_read(filepath: str) -> str:
    """Try UTF-8 first, fall back to latin-1."""
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    
    for encoding in encodings:
        try:
            with open(filepath, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Cannot decode {filepath} with any supported encoding")

# Specify encoding explicitly
with open("data/legacy_export.csv", "r", encoding="latin-1") as f:
    reader = csv.DictReader(f)
    records = list(reader)

# Write with BOM for Excel compatibility
with open("report.csv", "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Name", "Amount"])
    writer.writerows(data)
```

---

## Binary Mode — When Text Won't Work

```python
# Binary mode for non-text files (images, Parquet, Avro, compressed)
with open("data/events.parquet", "rb") as f:  # 'rb' = read binary
    content = f.read()

with open("output/archive.gz", "wb") as f:  # 'wb' = write binary
    f.write(compressed_data)

# Combining with compression
import gzip

# Read gzipped CSV
with gzip.open("data/events.csv.gz", "rt") as f:  # 'rt' = read text
    reader = csv.DictReader(f)
    for row in reader:
        process(row)

# Write gzipped output
with gzip.open("output/results.json.gz", "wt") as f:  # 'wt' = write text
    json.dump(results, f)
```

---

## Common Patterns in Data Engineering

### Reading Multiple Files

```python
from pathlib import Path

def read_all_csvs(directory: str) -> list[dict]:
    """Read all CSV files in a directory into a single list."""
    all_records = []
    for filepath in sorted(Path(directory).glob("*.csv")):
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            all_records.extend(reader)
    return all_records
```

### File Existence Checks

```python
from pathlib import Path

def safe_load(filepath: str) -> dict:
    """Load JSON config with existence check."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    if path.stat().st_size == 0:
        raise ValueError(f"Config file is empty: {filepath}")
    return json.loads(path.read_text())
```

---

## File I/O Flow

The diagram below traces the typical read-process-write lifecycle: a source file is opened with an explicit encoding, parsed into records, transformed, then serialized and written to a destination, with the `with` block guaranteeing the handle is closed.

```mermaid
flowchart LR
    A[Source File] --> B[Open with encoding]
    B --> C[Read: text or binary]
    C --> D[Parse: CSV/JSON/lines]
    D --> E[Process records]
    E --> F[Serialize output]
    F --> G[Write to destination]
    G --> H[Close automatically via with]
```

---


## ▶️ Try It Yourself

```python
import csv
import json
from pathlib import Path

# Write a CSV
Path("/tmp/orders.csv").write_text("id,amount,region
1,100,US
2,200,EU
")

# Read CSV efficiently (generator — memory-constant for large files)
def read_csv_chunks(path: str, chunk_size: int = 1000):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk.clear()
        if chunk:
            yield chunk

for chunk in read_csv_chunks("/tmp/orders.csv"):
    print(f"Processing chunk: {len(chunk)} rows")

# JSON lines (JSONL) — one JSON object per line, streamable
with open("/tmp/events.jsonl", "w") as f:
    for i in range(3):
        f.write(json.dumps({"id": i, "event": "click"}) + "
")

with open("/tmp/events.jsonl") as f:
    for line in f:
        event = json.loads(line)
        print(event)
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** Always use `with open(...)` context managers — never manual `f.close()`. If an exception occurs between open and close, the file leaks. This is the most basic but critical Python I/O pattern, and getting it wrong in an interview signals lack of production experience.

> **Tip 2:** Know the difference between `csv.DictReader` (returns dicts, self-documenting) and `csv.reader` (returns lists, slightly faster). For DE work, DictReader is almost always preferred because column names are preserved. Mention that all CSV values are strings — you must explicitly cast numeric fields.

> **Tip 3:** Mention pathlib over os.path — it's the modern approach and works cross-platform. Using `Path("data") / "file.csv"` instead of `os.path.join("data", "file.csv")` shows you write contemporary Python and think about code readability.
