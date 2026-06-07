---
title: "Bash Pipelines - Scenario Questions"
topic: bash-scripting
subtopic: pipelines
content_type: scenario_question
tags: [bash, pipelines, interview, scenarios]
---

# Scenario Questions — Bash Pipelines

<article data-difficulty="junior">

## 🟢 Junior: Composing a Basic Pipeline

**Scenario:** Given a CSV file `orders.csv` with columns (order_id, customer_id, amount, date, region), write a one-liner pipeline that: skips the header, filters to region="US", extracts only order_id and amount columns, and sorts by amount (descending).

<details>
<summary>💡 Hint</summary>
Chain: tail (skip header) | grep/awk (filter US) | cut/awk (extract columns) | sort (numeric, reverse).
</details>

<details>
<summary>✅ Solution</summary>

```bash
tail -n +2 orders.csv | awk -F',' '$5=="US" {print $1","$3}' | sort -t',' -k2 -rn

# Breakdown:
# tail -n +2: skip header (line 1)
# awk -F',' '$5=="US"': filter rows where column 5 = "US"
# {print $1","$3}: output only columns 1 (order_id) and 3 (amount)
# sort -t',' -k2 -rn: sort by column 2 (amount), reverse numeric

# Alternative using multiple commands:
tail -n +2 orders.csv | grep ",US$" | cut -d',' -f1,3 | sort -t',' -k2 -rn

# Output example:
# 5001,9999.99
# 3042,5500.00
# 1234,2100.50
# ...
```

**Key Points:**
- `tail -n +2`: skip header line (start from line 2)
- awk can filter AND project in one command (most efficient)
- `sort -t',' -k2 -rn`: comma delimiter, sort by field 2, reverse numeric
- Entire pipeline: streaming, constant memory, handles any file size
- No temporary files needed — data flows through pipes directly

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Streaming S3 Processing

**Scenario:** Process a 20 GB gzipped CSV from S3 without downloading it to disk. Filter rows where amount > 100, add a "processed_at" timestamp column, and upload the result (gzipped) back to S3. Zero local disk usage.

<details>
<summary>💡 Hint</summary>
`aws s3 cp source -` streams to stdout. Pipe through: gunzip → awk (filter + add column) → gzip → `aws s3 cp - target`. All streaming!
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

SOURCE="s3://data-lake/raw/orders_20240315.csv.gz"
TARGET="s3://data-lake/processed/orders_filtered_20240315.csv.gz"
TIMESTAMP=$(date -Iseconds)

aws s3 cp "$SOURCE" - | \
    gunzip | \
    awk -F',' -v OFS=',' -v ts="$TIMESTAMP" '
        NR == 1 { print $0",processed_at"; next }   # Add column to header
        $3+0 > 100 { print $0","ts }                # Filter + add timestamp
    ' | \
    gzip | \
    aws s3 cp - "$TARGET"

echo "Done: $SOURCE → (filter amount>100, add timestamp) → $TARGET"

# HOW IT WORKS:
# aws s3 cp source - : downloads to stdout (streaming, not to disk!)
# gunzip : decompresses stream
# awk : filters rows (amount > 100) + adds processed_at column
# gzip : re-compresses the result stream
# aws s3 cp - target : uploads from stdin (streaming!)
#
# TOTAL DISK USAGE: 0 bytes!
# MEMORY USAGE: ~10 MB (pipe buffers only)
# Works on a t3.nano (0.5 GB RAM) for a 20 GB file!
```

**Key Points:**
- `aws s3 cp file -`: the `-` means "stdout" (stream to pipe, not to disk)
- `aws s3 cp - file`: the `-` means "stdin" (upload from pipe)
- gunzip/gzip work as streaming filters (no temp files)
- awk does filter + transform in one pass (single-threaded but I/O-bound anyway)
- Zero disk, ~10 MB RAM, handles any file size
- `set -euo pipefail`: if ANY stage fails → pipeline stops (no corrupted partial upload)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Parallel Pipeline with Metrics

**Scenario:** Process 100 CSV files in /data/landing/. For each: validate (non-empty, correct columns), transform (filter + clean), and archive. Run 4 files in parallel. Track: files processed, total rows, failures. Report metrics at end.

<details>
<summary>💡 Hint</summary>
Use xargs -P 4 for parallel execution. Write a process_file function that validates, transforms, and archives. Collect metrics to a shared file (thread-safe with flock). Report aggregate at end.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -uo pipefail

LANDING="/data/landing"
OUTPUT="/data/output"
ARCHIVE="/data/archive/$(date +%Y%m%d)"
METRICS="/tmp/pipeline_metrics_$$"
MAX_PARALLEL=4

mkdir -p "$OUTPUT" "$ARCHIVE"
> "$METRICS"  # Initialize metrics file

# Process a single file (called in parallel):
process_file() {
    local file="$1"
    local fname=$(basename "$file")
    local status="success"
    local rows=0
    
    # Validate:
    if [ ! -s "$file" ]; then
        echo "skip:$fname:empty" >> "$METRICS"
        return 0
    fi
    
    local cols=$(head -1 "$file" | awk -F',' '{print NF}')
    if [ "$cols" -ne 8 ]; then
        echo "fail:$fname:wrong_columns_$cols" >> "$METRICS"
        return 1
    fi
    
    # Transform (streaming pipeline!):
    rows=$(tail -n +2 "$file" | \
        awk -F',' '$3+0 > 0 && $1 != ""' | \
        sort -t',' -k4 | \
        tee "$OUTPUT/$fname" | wc -l)
    
    if [ $rows -gt 0 ]; then
        mv "$file" "$ARCHIVE/"
        echo "ok:$fname:$rows" >> "$METRICS"
    else
        echo "fail:$fname:zero_rows_after_filter" >> "$METRICS"
        return 1
    fi
}
export -f process_file
export OUTPUT ARCHIVE METRICS

# Run in parallel (4 at a time):
find "$LANDING" -name "*.csv" -print0 | \
    xargs -0 -P $MAX_PARALLEL -I {} bash -c 'process_file "$@"' _ {}

# Report metrics:
echo ""
echo "═══════════════════════════════════════"
echo "  Pipeline Metrics"
echo "═══════════════════════════════════════"
total=$(wc -l < "$METRICS")
success=$(grep -c "^ok:" "$METRICS" || echo 0)
failed=$(grep -c "^fail:" "$METRICS" || echo 0)
skipped=$(grep -c "^skip:" "$METRICS" || echo 0)
total_rows=$(grep "^ok:" "$METRICS" | awk -F: '{sum+=$3} END {print sum+0}')

echo "  Files: $total (success: $success, failed: $failed, skipped: $skipped)"
echo "  Total rows processed: $total_rows"
echo ""

if [ $failed -gt 0 ]; then
    echo "  Failures:"
    grep "^fail:" "$METRICS" | awk -F: '{printf "    ❌ %s — %s\n", $2, $3}'
fi

rm -f "$METRICS"
[ $failed -eq 0 ]  # Exit 0 if no failures, 1 if any
```

**Key Points:**
- `xargs -P 4`: runs up to 4 files in parallel (true parallelism!)
- `export -f process_file`: makes function available to xargs subshells
- Metrics file: shared between parallel processes (append-only = safe)
- Each file: validate → transform (streaming pipe) → archive (atomic mv)
- Report: aggregates from metrics file (total, success, failures, row counts)
- Failure isolation: one file failing doesn't stop others (parallel continues)
- Exit code: 0 if all succeeded, 1 if any failed (caller knows overall status)
- 100 files × 4 parallel = finishes 4x faster than sequential!

</details>

</article>
