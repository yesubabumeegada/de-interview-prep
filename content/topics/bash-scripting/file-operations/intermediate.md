---
title: "File Operations - Intermediate"
topic: bash-scripting
subtopic: file-operations
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, file-operations, scripting, data-pipelines, automation]
---

# Bash File Operations — Intermediate

## File Processing Patterns for Data Engineering

### Landing Zone File Router

```bash
#!/bin/bash
# Route incoming files to appropriate processing directories based on naming convention
# Expected: orders_2024-03-15.csv, events_2024-03-15.json, customers_full.csv

LANDING="/data/landing"
PROCESSING="/data/processing"

for file in "$LANDING"/*; do
    [ -f "$file" ] || continue  # Skip if not a regular file
    
    filename=$(basename "$file")
    
    case "$filename" in
        orders_*.csv)
            mv "$file" "$PROCESSING/orders/"
            echo "Routed: $filename → orders/"
            ;;
        events_*.json)
            mv "$file" "$PROCESSING/events/"
            echo "Routed: $filename → events/"
            ;;
        customers_*.csv)
            mv "$file" "$PROCESSING/customers/"
            echo "Routed: $filename → customers/"
            ;;
        *)
            mv "$file" "$PROCESSING/unknown/"
            echo "WARNING: Unknown file pattern: $filename"
            ;;
    esac
done
```

### File Validation Before Processing

```bash
#!/bin/bash
# Validate a CSV file before loading into database

validate_csv() {
    local file="$1"
    local expected_cols="$2"
    local min_rows="$3"
    
    # Check file exists and is readable
    if [ ! -r "$file" ]; then
        echo "ERROR: Cannot read $file"
        return 1
    fi
    
    # Check file is not empty
    if [ ! -s "$file" ]; then
        echo "ERROR: $file is empty"
        return 1
    fi
    
    # Check column count (header)
    actual_cols=$(head -1 "$file" | awk -F',' '{print NF}')
    if [ "$actual_cols" -ne "$expected_cols" ]; then
        echo "ERROR: Expected $expected_cols columns, got $actual_cols"
        return 1
    fi
    
    # Check minimum row count (excluding header)
    actual_rows=$(wc -l < "$file")
    actual_rows=$((actual_rows - 1))  # Subtract header
    if [ "$actual_rows" -lt "$min_rows" ]; then
        echo "ERROR: Only $actual_rows rows (minimum: $min_rows)"
        return 1
    fi
    
    # Check no null bytes (binary corruption)
    if grep -qP '\x00' "$file"; then
        echo "ERROR: File contains null bytes (possibly corrupted)"
        return 1
    fi
    
    echo "VALID: $file ($actual_rows rows, $actual_cols columns)"
    return 0
}

# Usage:
validate_csv "/data/landing/orders.csv" 8 1000
if [ $? -eq 0 ]; then
    echo "Loading into database..."
    # psql -c "\COPY orders FROM '/data/landing/orders.csv' CSV HEADER"
fi
```

---

## Atomic File Operations

```bash
# PROBLEM: If script crashes mid-write, output file is corrupted/partial
# SOLUTION: Write to temp file, then atomic rename (mv)

process_data() {
    local input="$1"
    local output="$2"
    local tmp_output="${output}.tmp.$$"  # Temp file with PID for uniqueness
    
    # Process to temp file
    python transform.py "$input" > "$tmp_output"
    
    if [ $? -eq 0 ]; then
        # SUCCESS: atomic rename (instant, can't leave partial file)
        mv "$tmp_output" "$output"
        echo "Output written: $output"
    else
        # FAILURE: remove temp, don't corrupt output
        rm -f "$tmp_output"
        echo "ERROR: Processing failed, output not updated"
        return 1
    fi
}

# The key: mv (rename) is ATOMIC on the same filesystem
# Readers of $output always see either the old complete file or the new complete file
# Never a partial/corrupted state!
```

---

## Parallel File Processing

```bash
#!/bin/bash
# Process multiple files in parallel (speed up batch operations)

MAX_PARALLEL=4  # Limit concurrent processes
LANDING="/data/landing"

# Using xargs for parallel execution:
find "$LANDING" -name "*.csv" -print0 | \
    xargs -0 -P $MAX_PARALLEL -I {} bash -c '
        file="{}"
        echo "Processing: $file"
        python transform.py "$file" && mv "$file" /data/archive/
    '

# Alternative: using GNU parallel
find "$LANDING" -name "*.csv" | \
    parallel -j $MAX_PARALLEL 'python transform.py {} && mv {} /data/archive/'

# Alternative: background processes with wait
pids=()
for file in "$LANDING"/*.csv; do
    python transform.py "$file" &
    pids+=($!)
    
    # Limit parallelism
    if [ ${#pids[@]} -ge $MAX_PARALLEL ]; then
        wait "${pids[0]}"  # Wait for oldest process
        pids=("${pids[@]:1}")  # Remove from array
    fi
done
wait  # Wait for remaining processes
```

---

## File Locking (Prevent Concurrent Access)

```bash
#!/bin/bash
# Prevent two instances of a script from processing the same file

LOCKFILE="/tmp/etl_pipeline.lock"

# Acquire lock (or exit if another instance is running)
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "Another instance is already running. Exiting."
    exit 0
fi

# Critical section (only one instance runs this at a time)
echo "Pipeline started at $(date)"
# ... your ETL logic here ...
echo "Pipeline completed at $(date)"

# Lock is automatically released when script exits (fd 200 closed)
```

---

## Working with Large Files

```bash
# Split large file into smaller chunks (for parallel processing):
split -l 1000000 huge_file.csv chunk_     # 1M lines per chunk
# Creates: chunk_aa, chunk_ab, chunk_ac, ...

# Process each chunk, then concatenate results:
for chunk in chunk_*; do
    python process.py "$chunk" > "${chunk}.out" &
done
wait
cat chunk_*.out > final_output.csv
rm chunk_* chunk_*.out

# Stream processing (never loads full file into memory):
# Count orders per region from a 50 GB CSV:
tail -n +2 huge_orders.csv | \       # Skip header
    cut -d',' -f5 | \                 # Extract region column (field 5)
    sort | \                          # Sort for uniq
    uniq -c | \                       # Count occurrences
    sort -rn                          # Sort by count descending
# Processes 50 GB with ~0 memory! (streaming through pipes)
```

---

## Directory Monitoring (inotifywait)

```bash
#!/bin/bash
# Watch for new files and process them immediately (like a sensor)

WATCH_DIR="/data/landing"

inotifywait -m -e create -e moved_to "$WATCH_DIR" | while read dir event file; do
    echo "New file detected: $file (event: $event)"
    
    # Wait a moment for file to finish writing
    sleep 2
    
    # Process the file
    if [[ "$file" == *.csv ]]; then
        echo "Processing CSV: $file"
        python ingest.py "$WATCH_DIR/$file"
        mv "$WATCH_DIR/$file" "/data/archive/$file"
    fi
done

# This is a "file sensor" — similar to Airflow's FileSensor
# Useful for: triggering pipelines on file arrival without polling
```

---

## Interview Tips

> **Tip 1:** "How do you ensure file writes are atomic?" — Write to a temp file (same filesystem), then `mv` (rename) to the final path. `mv` is atomic on the same filesystem — readers always see either the old complete file or the new complete file. Never write directly to the output path (risk of partial/corrupted file if script crashes mid-write).

> **Tip 2:** "How do you process 1000 files quickly?" — Parallel execution: `xargs -P 4` or GNU `parallel -j 4` runs up to 4 files simultaneously. For I/O-bound tasks (S3 upload): higher parallelism (8-16). For CPU-bound (compression): match to CPU cores. Always limit parallelism to avoid overwhelming the system.

> **Tip 3:** "How do you prevent two cron jobs from processing the same file?" — File locking with `flock`: acquire a lock file at script start (non-blocking). If lock is already held → another instance is running → exit gracefully. Lock released automatically on script exit. Simple, reliable, no external dependencies.
