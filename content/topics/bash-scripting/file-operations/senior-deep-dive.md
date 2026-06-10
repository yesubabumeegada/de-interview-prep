---
title: "File Operations - Senior Deep Dive"
topic: bash-scripting
subtopic: file-operations
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, file-operations, production, performance, reliability]
---

# Bash File Operations — Senior-Level Deep Dive

## Production File Pipeline Architecture

```bash
#!/bin/bash
# Production-grade file processing pipeline with:
# - Locking (prevent concurrent runs)
# - Validation (reject bad files)
# - Atomic moves (no partial state)
# - Error handling (retry + alert)
# - Logging (audit trail)
# - Cleanup (prevent disk fill)

set -euo pipefail  # Strict mode: exit on error, undefined vars, pipe failures

# Configuration
LANDING="/data/landing"
PROCESSING="/data/processing"
ARCHIVE="/data/archive/$(date +%Y/%m/%d)"
ERROR_DIR="/data/errors/$(date +%Y%m%d)"
LOG_FILE="/var/log/etl/file_pipeline_$(date +%Y%m%d).log"
LOCK_FILE="/tmp/file_pipeline.lock"
MAX_RETRIES=3

# Logging
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

# Locking
exec 200>"$LOCK_FILE"
flock -n 200 || { log "ERROR: Another instance running. Exiting."; exit 0; }

# Ensure directories exist
mkdir -p "$PROCESSING" "$ARCHIVE" "$ERROR_DIR"

log "Pipeline started. Scanning $LANDING for new files..."

# Process each file
file_count=0
error_count=0

for file in "$LANDING"/*.{csv,json,parquet} 2>/dev/null; do
    [ -f "$file" ] || continue
    filename=$(basename "$file")
    file_count=$((file_count + 1))
    
    log "Processing: $filename ($(stat -c%s "$file") bytes)"
    
    # Validate
    if [ ! -s "$file" ]; then
        log "SKIP: $filename is empty"
        mv "$file" "$ERROR_DIR/"
        error_count=$((error_count + 1))
        continue
    fi
    
    # Move to processing (atomic)
    mv "$file" "$PROCESSING/$filename"
    
    # Process with retry
    success=false
    for attempt in $(seq 1 $MAX_RETRIES); do
        if python /opt/etl/process_file.py "$PROCESSING/$filename"; then
            success=true
            break
        fi
        log "WARN: Attempt $attempt failed for $filename. Retrying..."
        sleep $((attempt * 5))
    done
    
    # Route based on success/failure
    if $success; then
        mv "$PROCESSING/$filename" "$ARCHIVE/$filename"
        log "SUCCESS: $filename → archive"
    else
        mv "$PROCESSING/$filename" "$ERROR_DIR/$filename"
        log "ERROR: $filename failed after $MAX_RETRIES attempts → error dir"
        error_count=$((error_count + 1))
    fi
done

# Summary
log "Pipeline complete: $file_count files processed, $error_count errors"

# Alert on errors
if [ $error_count -gt 0 ]; then
    curl -X POST "$SLACK_WEBHOOK" \
        -d "{\"text\":\"⚠️ File pipeline: $error_count/$file_count files failed. Check $ERROR_DIR\"}"
fi

# Cleanup old archives (>30 days)
find /data/archive -type f -mtime +30 -delete
log "Cleanup: removed files older than 30 days from archive"
```

---

## High-Performance File Operations

### Efficient Large File Handling

```bash
# Process 100 GB file without loading into memory:

# Stream processing with awk (constant memory, any file size):
awk -F',' '
    NR == 1 { next }  # Skip header
    $5 == "US" { sum += $3; count++ }  # Sum amount (field 3) for US region
    END { printf "US Revenue: %.2f (%d orders)\n", sum, count }
' /data/huge_orders.csv
# Processes 100 GB in ~5 minutes with <1 MB memory!

# Parallel decompression + processing:
pigz -dc huge_file.csv.gz | \  # Parallel gzip decompress (uses all cores)
    awk -F',' 'NR > 1 { print $1","$3","$5 }' | \  # Extract columns
    sort -t',' -k3 | \  # Sort by column 3
    gzip > output.csv.gz  # Re-compress output
# Streaming: decompress → transform → compress (never fully in memory!)

# Named pipes for complex streaming:
mkfifo /tmp/pipe1 /tmp/pipe2
# Process 1: split stream into two paths
tee /tmp/pipe1 < input.csv | process_a.py > output_a.csv &
process_b.py < /tmp/pipe1 > output_b.csv &
wait
rm /tmp/pipe1
```

### S3 File Operations (AWS CLI)

```bash
# Sync landing zone from S3:
aws s3 sync s3://bucket/landing/ /data/landing/ --exclude "*.tmp"

# Upload with parallel threads:
aws s3 cp large_file.parquet s3://bucket/archive/ \
    --storage-class STANDARD_IA \
    --metadata "pipeline=daily_orders,date=$(date +%Y-%m-%d)"

# List files by date pattern:
aws s3 ls s3://bucket/landing/orders/ --recursive | \
    awk '$1 >= "2024-03-15" { print $4 }'

# Bulk operations with concurrent transfers:
aws s3 sync /data/output/ s3://bucket/processed/ \
    --exclude "*" --include "*.parquet" \
    --storage-class INTELLIGENT_TIERING \
    --only-show-errors
```

---

## File System Monitoring for Pipelines

```bash
#!/bin/bash
# Monitor disk usage and alert before pipelines fail

check_disk_space() {
    local path="$1"
    local threshold="$2"  # Percentage (e.g., 85)
    
    usage=$(df "$path" | awk 'NR==2 {gsub(/%/,""); print $5}')
    
    if [ "$usage" -ge "$threshold" ]; then
        echo "ALERT: $path is ${usage}% full (threshold: ${threshold}%)"
        # Emergency: clean old files
        find "$path" -name "*.tmp" -mtime +1 -delete
        find "$path" -name "*.log" -mtime +7 -delete
        return 1
    fi
    return 0
}

# Run before each pipeline:
check_disk_space "/data" 85 || {
    echo "Disk space critical! Attempting cleanup..."
    # Additional cleanup strategies...
}
```

---

## Interview Tips

> **Tip 1:** "Design a production file processing pipeline" — Components: landing zone scan → validation (size, format, schema) → atomic move to processing → transform with retry → archive on success / error dir on failure → alert on failures → cleanup old archives. Always: set -euo pipefail, flock for concurrency, logging for audit.

> **Tip 2:** "How do you handle a 100 GB file in bash?" — Stream it (never load fully): `awk`, `sed`, `cut` process line-by-line with constant memory. Use `pigz` for parallel compression. Use `split` to chunk for parallel processing. Use named pipes (`mkfifo`) for complex streaming topologies. Key principle: data flows through pipes, never fully materializes.

> **Tip 3:** "How do you prevent disk full from crashing pipelines?" — Pre-flight check: `df` before processing, abort if >85% full. Cleanup cron: delete files older than N days from temp/archive. Alert at 80%: notification before critical. Write to temp filesystem first, move to target only if complete. Monitor: track disk growth rate, predict full date.

## ⚡ Cheat Sheet

**Safe file operations**
```bash
# Atomic write (never leaves partial file)
tmpfile=$(mktemp /tmp/output.XXXXXX)
generate_data > "$tmpfile"
mv "$tmpfile" /final/output.csv

# Safe delete (to trash or with confirmation)
[ -f "$file" ] && rm -f "$file" || echo "File not found: $file"

# Check before overwrite
[ -f output.csv ] && { cp output.csv output.csv.bak; }
```

**Finding files**
```bash
find /data -name "*.csv" -newer /tmp/last_run -type f  # newer than marker
find /data -name "*.log" -mtime +30 -delete             # delete >30 days old
find /data -size +100M -name "*.parquet"                # large files
find /data -empty -delete                                # remove empty files/dirs
```

**Bulk operations**
```bash
# Process all CSVs (handles spaces in names)
while IFS= read -r -d '' file; do
    process "$file"
done < <(find /data -name "*.csv" -print0)

# GNU parallel
find /data -name "*.csv" | parallel -j8 python process.py {}
```

**File locking**
```bash
exec 200>/tmp/mylock
flock -x 200 || exit 1  # exclusive lock; wait or use -n for non-blocking
# Lock auto-released when fd 200 closed (on script exit)
```

**Checksums and integrity**
```bash
md5sum file.csv > file.csv.md5
md5sum -c file.csv.md5  # verify
sha256sum -c checksums.sha256
# Compare before/after
md5sum before.csv > before.md5; process; md5sum -c before.md5
```

**Key patterns for DE**
```bash
# Wait for file to appear (polling)
until [ -f /data/ready.flag ]; do sleep 5; done

# Archive processed files
mkdir -p /data/archive/$(date +%Y/%m/%d)
mv /data/input/*.csv /data/archive/$(date +%Y/%m/%d)/

# Disk usage check before writing
avail=$(df -BG /data | awk 'NR==2{print $4}' | tr -d G)
[ "$avail" -lt 10 ] && die "Less than 10 GB available"
```
