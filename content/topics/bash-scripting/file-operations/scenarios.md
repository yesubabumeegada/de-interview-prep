---
title: "File Operations - Scenario Questions"
topic: bash-scripting
subtopic: file-operations
content_type: scenario_question
tags: [bash, file-operations, interview, scenarios]
---

# Scenario Questions — Bash File Operations

<article data-difficulty="junior">

## 🟢 Junior: File Existence Check

**Scenario:** Write a bash script that checks if `/data/landing/orders.csv` exists, is not empty, and has more than 1 line (not just a header). If all checks pass, move it to `/data/processing/`.

<details>
<summary>💡 Hint</summary>
Use -f (exists), -s (not empty), and `wc -l` for line count. Use `mv` for atomic move.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
FILE="/data/landing/orders.csv"
DEST="/data/processing/"

# Check exists
if [ ! -f "$FILE" ]; then
    echo "ERROR: File not found: $FILE"
    exit 1
fi

# Check not empty
if [ ! -s "$FILE" ]; then
    echo "ERROR: File is empty: $FILE"
    exit 1
fi

# Check has more than just header (>1 line)
line_count=$(wc -l < "$FILE")
if [ "$line_count" -le 1 ]; then
    echo "ERROR: File has only $line_count line(s) — no data rows!"
    exit 1
fi

# All checks pass — move to processing
mkdir -p "$DEST"
mv "$FILE" "$DEST/"
echo "SUCCESS: Moved $FILE to $DEST/ ($((line_count - 1)) data rows)"
```

**Key Points:**
- `-f` checks file exists (not directory)
- `-s` checks file has size > 0 bytes (not empty)
- `wc -l < "$FILE"` counts lines (using redirect avoids filename in output)
- `mv` is atomic (file is either in landing OR processing, never both/neither)
- Always `mkdir -p` before moving (ensures destination exists)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Batch File Processing with Error Handling

**Scenario:** Process all `.csv` files in `/data/landing/`. For each: validate (non-empty, >100 rows), transform with a Python script, move to `/data/archive/` on success or `/data/errors/` on failure. Log everything.

<details>
<summary>💡 Hint</summary>
Loop through files, validate each, call Python (check exit code), route based on success/failure, log with timestamps.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -uo pipefail

LANDING="/data/landing"
ARCHIVE="/data/archive/$(date +%Y%m%d)"
ERRORS="/data/errors/$(date +%Y%m%d)"
LOG="/var/log/batch_process_$(date +%Y%m%d).log"

mkdir -p "$ARCHIVE" "$ERRORS"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

success=0; failed=0; skipped=0

for file in "$LANDING"/*.csv; do
    [ -f "$file" ] || continue
    fname=$(basename "$file")
    
    # Validate: non-empty and >100 rows
    if [ ! -s "$file" ]; then
        log "SKIP (empty): $fname"
        mv "$file" "$ERRORS/"
        skipped=$((skipped + 1))
        continue
    fi
    
    rows=$(wc -l < "$file")
    if [ "$rows" -le 100 ]; then
        log "SKIP (only $rows rows): $fname"
        mv "$file" "$ERRORS/"
        skipped=$((skipped + 1))
        continue
    fi
    
    # Process
    if python /opt/etl/transform.py "$file"; then
        mv "$file" "$ARCHIVE/"
        log "SUCCESS: $fname ($rows rows)"
        success=$((success + 1))
    else
        mv "$file" "$ERRORS/"
        log "FAILED: $fname (transform error)"
        failed=$((failed + 1))
    fi
done

log "SUMMARY: success=$success, failed=$failed, skipped=$skipped"
[ $failed -gt 0 ] && exit 1 || exit 0
```

**Key Points:**
- `set -uo pipefail`: strict mode (undefined vars error, pipe failures propagate)
- Validation BEFORE processing (don't waste compute on bad files)
- Exit code from Python (`$?`) determines success/failure routing
- Log with timestamps for debugging (when did each file process?)
- Summary at end: quick health check for monitoring
- Exit 1 if ANY file failed (lets calling system know there were issues)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production File Pipeline with Locking and Retry

**Scenario:** Design a production-grade file processor: prevents concurrent runs (flock), retries failed files 3 times with backoff, sends Slack alert on failures, and cleans up files older than 30 days.

<details>
<summary>💡 Hint</summary>
flock for locking, retry loop with sleep, curl for Slack webhook, find -mtime for cleanup. Combine all patterns into one robust script.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

# --- Configuration ---
LANDING="/data/landing"
ARCHIVE="/data/archive"
ERROR_DIR="/data/errors"
LOCK="/tmp/file_processor.lock"
MAX_RETRIES=3
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL:-}"
LOG="/var/log/etl/processor_$(date +%Y%m%d_%H%M).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"; }
alert() {
    [ -n "$SLACK_WEBHOOK" ] && curl -sS -X POST "$SLACK_WEBHOOK" \
        -H 'Content-type: application/json' -d "{\"text\":\"$1\"}" > /dev/null
}

# --- Locking (prevent concurrent runs) ---
exec 200>"$LOCK"
if ! flock -n 200; then
    log "WARN: Another instance running. Exiting."
    exit 0
fi

log "=== Pipeline started ==="
mkdir -p "$ARCHIVE/$(date +%Y/%m/%d)" "$ERROR_DIR"

# --- Process files with retry ---
total=0; success=0; failed=0

for file in "$LANDING"/*.{csv,json,parquet} 2>/dev/null; do
    [ -f "$file" ] || continue
    total=$((total + 1))
    fname=$(basename "$file")
    processed=false
    
    for attempt in $(seq 1 $MAX_RETRIES); do
        if python /opt/etl/process.py "$file" 2>>"$LOG"; then
            mv "$file" "$ARCHIVE/$(date +%Y/%m/%d)/"
            log "OK: $fname (attempt $attempt)"
            success=$((success + 1))
            processed=true
            break
        fi
        log "RETRY: $fname attempt $attempt/$MAX_RETRIES failed"
        sleep $((attempt * 10))  # Backoff: 10s, 20s, 30s
    done
    
    if ! $processed; then
        mv "$file" "$ERROR_DIR/"
        log "FAILED: $fname after $MAX_RETRIES attempts"
        failed=$((failed + 1))
    fi
done

# --- Cleanup (files >30 days) ---
deleted=$(find "$ARCHIVE" -type f -mtime +30 -delete -print | wc -l)
log "Cleanup: $deleted old files removed from archive"

# --- Summary and alerting ---
log "=== Complete: total=$total success=$success failed=$failed ==="

if [ $failed -gt 0 ]; then
    alert "⚠️ File processor: $failed/$total files failed! Check: $ERROR_DIR"
    exit 1
fi
```

**Key Points:**
- `flock`: prevents two cron runs from overlapping (single-instance guarantee)
- Retry with exponential backoff: handles transient failures (network, temp file locks)
- Slack alert: immediate notification to team on failures
- Cleanup with `find -mtime +30 -delete`: prevents disk fill from accumulating archives
- `set -euo pipefail`: strict mode catches bugs early
- Log everything: full audit trail for debugging
- Exit code: 0 (all good) or 1 (failures occurred) — integrates with cron/monitoring

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: How do you check if a file exists before operating on it in bash?**
A: Use a conditional test: `[ -f /path/to/file ] && echo "exists"` or `if [ -f /path/to/file ]; then ...`. Use `-d` for directories, `-e` for any filesystem entry, and `-s` for a file that exists and is non-empty.

**Q: What is the difference between `>` and `>>` when redirecting output to a file?**
A: `>` truncates the file and writes from the beginning, overwriting any existing content. `>>` appends to the end of the file, preserving existing content. Use `>>` for logs and audit trails.

**Q: How do you safely create a temporary file in a script?**
A: Use `mktemp` to generate a uniquely named temporary file: `tmpfile=$(mktemp)`. Register cleanup with `trap "rm -f $tmpfile" EXIT` to ensure the file is deleted even if the script fails.

**Q: How do you recursively find all `.csv` files modified in the last 24 hours?**
A: Use `find`: `find /data -name "*.csv" -mtime -1`. The `-mtime -1` flag matches files modified less than 1 day ago. Add `-type f` to exclude directories named with `.csv`.

**Q: What is the difference between `cp`, `mv`, and `rsync` for file operations?**
A: `cp` copies files (source remains). `mv` moves or renames files (source is removed). `rsync` is more powerful — it supports incremental transfers, remote hosts, checksums, and bandwidth limiting, making it preferred for large or repeated data transfers.

**Q: How do you read a file line by line in bash?**
A: Use a `while read` loop: `while IFS= read -r line; do echo "$line"; done < file.txt`. `IFS=` prevents trimming of leading/trailing whitespace and `-r` prevents backslash interpretation.

**Q: How do you count the number of lines, words, and bytes in a file?**
A: Use `wc`: `wc -l file` for lines, `wc -w file` for words, `wc -c file` for bytes. Run `wc file` without flags to get all three counts at once.

**Q: How do you atomically replace a file to avoid readers seeing a partial write?**
A: Write to a temporary file in the same filesystem, then rename it: `cp newdata /tmp/file.tmp && mv /tmp/file.tmp /data/file`. The `mv` (rename syscall) is atomic on the same filesystem, so readers never see a partial state.

---

## 💼 Interview Tips

- Atomic file replacement (write-then-rename) is a go-to answer for "how do you safely update a file that other processes read" — it shows systems-level thinking beyond basic bash.
- Always mention `trap` for temp file cleanup; leaving temp files behind in `/tmp` is a classic mistake interviewers probe for in production-readiness discussions.
- For data engineering file ingestion scenarios, discuss `rsync` versus `cp` and when checksums matter — it shows awareness of data integrity beyond just moving bytes.
- Demonstrate that you know `find` deeply: `-newer`, `-mtime`, `-size`, and `-exec` flags come up constantly in DE scripts for partition management and data discovery.
- Senior interviewers appreciate mention of filesystem permissions and ownership (`chown`, `chmod`) when discussing file operations in shared data platform environments.
- Avoid using `ls` output in scripts for iteration — use `find` or globs instead; parsing `ls` is a well-known pitfall with filenames containing spaces or special characters.
