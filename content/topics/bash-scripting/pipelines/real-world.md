---
title: "Bash Pipelines - Real-World Production Examples"
topic: bash-scripting
subtopic: pipelines
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, pipelines, production, streaming, data-engineering]
---

# Bash Pipelines — Real-World Production Examples

## Pattern 1: Log Analysis Pipeline

```bash
#!/bin/bash
# Analyze 24 hours of pipeline logs (10 GB compressed)
# Produce: error summary, slow queries, throughput stats

LOG_DIR="/var/log/pipelines"
REPORT="/data/reports/daily_log_analysis_$(date +%Y%m%d).txt"
YESTERDAY=$(date -d yesterday +%Y-%m-%d)

{
echo "=== Daily Pipeline Log Analysis: $YESTERDAY ==="
echo ""

echo "## Error Summary (top 10 error types):"
zcat "$LOG_DIR"/*${YESTERDAY}*.gz | \
    grep "ERROR" | \
    grep -oP 'error_type=\K\w+' | \
    sort | uniq -c | sort -rn | head -10
echo ""

echo "## Slowest Operations (>60 seconds):"
zcat "$LOG_DIR"/*${YESTERDAY}*.gz | \
    grep "duration_ms=" | \
    awk -F'[ =]' '{for(i=1;i<=NF;i++) if($i=="duration_ms") print $(i+1), $0}' | \
    sort -rn | head -10 | \
    awk '{printf "  %6.1fs | %s\n", $1/1000, substr($0, index($0,$2))}'
echo ""

echo "## Throughput by Hour:"
zcat "$LOG_DIR"/*${YESTERDAY}*.gz | \
    grep "rows_processed=" | \
    grep -oP '\d{4}-\d{2}-\d{2}T(\d{2}).*rows_processed=(\d+)' | \
    awk -F'[T=]' '{hour=substr($2,1,2); rows+=$NF; count++} END {for(h in hour_rows) printf "  %s:00 — %d rows\n", h, hour_rows[h]}' | \
    sort
echo ""

echo "## Pipeline Success Rates:"
for pipeline in daily_orders hourly_events customer_sync; do
    total=$(zcat "$LOG_DIR"/*${YESTERDAY}*.gz | grep "pipeline=$pipeline" | grep "step=complete\|step=failed" | wc -l)
    failed=$(zcat "$LOG_DIR"/*${YESTERDAY}*.gz | grep "pipeline=$pipeline" | grep "step=failed" | wc -l)
    [ $total -gt 0 ] && rate=$(echo "scale=1; ($total-$failed)*100/$total" | bc) || rate="N/A"
    printf "  %-20s %s%% (%d/%d runs)\n" "$pipeline" "$rate" "$((total-failed))" "$total"
done

} > "$REPORT"

cat "$REPORT"
echo "Report saved: $REPORT"
```

---

## Pattern 2: S3 → Transform → Load (Zero Disk)

```bash
#!/bin/bash
# Stream processing: S3 source → transform → database (no local storage!)
set -euo pipefail

DB_CONN="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"
SOURCE="s3://data-lake/landing/orders/$(date -d yesterday +%Y/%m/%d)/"

echo "Streaming from $SOURCE → silver.orders_staging"

# Create temp table (drop if exists)
psql "$DB_CONN" -c "DROP TABLE IF EXISTS silver.orders_staging; CREATE TABLE silver.orders_staging (LIKE silver.orders)"

# Stream: S3 → decompress → filter → transform → load to DB (one pipeline!)
aws s3 cp "${SOURCE}orders.csv.gz" - | \
    gunzip | \
    tail -n +2 | \                          # Skip header
    awk -F',' -v OFS=',' '
        $1 != "" && $3+0 > 0 {             # Validate: non-null ID, positive amount
            $7 = strftime("%Y-%m-%d %H:%M:%S", systime())  # Add load timestamp
            print
        }
    ' | \
    psql "$DB_CONN" -c "\COPY silver.orders_staging FROM STDIN WITH (FORMAT CSV)"

# Verify and swap
ROW_COUNT=$(psql "$DB_CONN" -t -c "SELECT COUNT(*) FROM silver.orders_staging")
echo "Loaded $ROW_COUNT rows to staging"

if [ "$ROW_COUNT" -gt 0 ]; then
    psql "$DB_CONN" -c "BEGIN; DROP TABLE silver.orders; ALTER TABLE silver.orders_staging RENAME TO orders; COMMIT;"
    echo "Swap complete!"
else
    echo "ERROR: Zero rows loaded! Keeping existing table."
    psql "$DB_CONN" -c "DROP TABLE silver.orders_staging"
    exit 1
fi
```

---

## Pattern 3: Data Quality Pipeline

```bash
#!/bin/bash
# Streaming DQ checks on a CSV (validates without loading full file into memory)

FILE="$1"
[ -f "$FILE" ] || { echo "Usage: $0 <csv_file>"; exit 1; }

echo "=== Data Quality Pipeline: $(basename $FILE) ==="
echo "File size: $(ls -lh "$FILE" | awk '{print $5}')"
echo ""

# Check 1: Column count consistency (streaming — constant memory!)
echo -n "Column consistency: "
inconsistent=$(awk -F',' '{print NF}' "$FILE" | sort -u | wc -l)
if [ "$inconsistent" -eq 1 ]; then
    echo "✓ (all rows have $(head -1 "$FILE" | awk -F',' '{print NF}') columns)"
else
    echo "✗ INCONSISTENT! Found $inconsistent different column counts"
fi

# Check 2: Null rate per column (streaming)
echo "Null rates:"
cols=$(head -1 "$FILE" | awk -F',' '{print NF}')
total=$(tail -n +2 "$FILE" | wc -l)
for i in $(seq 1 $cols); do
    col_name=$(head -1 "$FILE" | cut -d',' -f$i)
    nulls=$(tail -n +2 "$FILE" | cut -d',' -f$i | grep -cE '^$|^NULL$|^null$' || echo 0)
    pct=$(echo "scale=1; $nulls * 100 / $total" | bc)
    flag=$( [ $(echo "$pct > 5" | bc) -eq 1 ] && echo "⚠️" || echo "" )
    printf "  %-20s %5d nulls (%5.1f%%) %s\n" "$col_name" "$nulls" "$pct" "$flag"
done

# Check 3: Duplicate check on primary key (column 1)
echo -n "Duplicates (col 1): "
dupes=$(tail -n +2 "$FILE" | cut -d',' -f1 | sort | uniq -d | wc -l)
[ "$dupes" -eq 0 ] && echo "✓ None" || echo "✗ $dupes duplicate keys found!"

echo ""
echo "Total rows: $total"
```

---

## Pattern 4: Multi-Source Data Merge Pipeline

```bash
#!/bin/bash
# Merge data from 3 sources into one unified file (streaming, sorted)
set -euo pipefail

OUTPUT="/data/output/unified_orders_$(date +%Y%m%d).csv"

echo "Merging 3 sources into unified output..."

# Header (from first source):
head -1 /data/source_a/orders.csv | sed 's/$/,source/' > "$OUTPUT"

# Merge all sources (add source column, sort by date):
{
    tail -n +2 /data/source_a/orders.csv | awk -F',' -v OFS=',' '{print $0,"source_a"}'
    tail -n +2 /data/source_b/orders.csv | awk -F',' -v OFS=',' '{print $0,"source_b"}'
    tail -n +2 /data/source_c/orders.csv | awk -F',' -v OFS=',' '{print $0,"source_c"}'
} | sort -t',' -k4 >> "$OUTPUT"
# Sorts ALL 3 sources by date column (column 4) during merge!

echo "Merged output: $(wc -l < "$OUTPUT") rows → $OUTPUT"
echo "Breakdown:"
tail -n +2 "$OUTPUT" | awk -F',' '{print $NF}' | sort | uniq -c
```

---

## Interview Tips

> **Tip 1:** "Design a zero-disk streaming pipeline" — `aws s3 cp source - | gunzip | transform | psql \COPY FROM STDIN`. Download streams directly to transform, transform streams to database load. No intermediate files. Works for 100+ GB on a machine with 1 GB RAM. The pipe buffers (~10 MB) are the only memory used.

> **Tip 2:** "How do you merge multiple data sources in bash?" — Concatenate (curly braces `{}`), add a source column (awk), pipe to sort (sort -t',' -k N). All streaming: no intermediate combined file needed until the final output. Handles GB-scale merges with constant memory.

> **Tip 3:** "Data quality checking with pipelines?" — Stream the file through validation commands: awk for column count check, cut + grep for null detection, sort + uniq -d for duplicate detection. Each check reads the file once (or use tee for single-pass). Produces immediate DQ report without loading the file into memory.
