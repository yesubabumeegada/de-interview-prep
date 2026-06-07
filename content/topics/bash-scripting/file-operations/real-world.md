---
title: "File Operations - Real-World Production Examples"
topic: bash-scripting
subtopic: file-operations
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, file-operations, production, patterns, data-engineering]
---

# Bash File Operations — Real-World Production Examples

## Pattern 1: S3 Landing Zone Processor

```bash
#!/bin/bash
# Download files from S3, validate, process, upload results, archive originals
set -euo pipefail

DATE=$(date +%Y-%m-%d)
LANDING="/data/landing/$DATE"
OUTPUT="/data/output/$DATE"
LOG="/var/log/etl/s3_processor_$DATE.log"

mkdir -p "$LANDING" "$OUTPUT"
exec > >(tee -a "$LOG") 2>&1  # Log all output

echo "=== S3 Landing Zone Processor: $DATE ==="

# Step 1: Sync from S3
echo "Syncing from S3..."
aws s3 sync "s3://company-lake/landing/$DATE/" "$LANDING/" --quiet
file_count=$(find "$LANDING" -type f | wc -l)
echo "Downloaded: $file_count files"

# Step 2: Validate each file
valid_files=()
for file in "$LANDING"/*; do
    [ -f "$file" ] || continue
    if [ -s "$file" ] && file "$file" | grep -qE "text|data|gzip"; then
        valid_files+=("$file")
    else
        echo "WARN: Skipping invalid file: $(basename $file)"
        mv "$file" "/data/errors/$DATE/"
    fi
done
echo "Valid files: ${#valid_files[@]}"

# Step 3: Process (parallel, 4 at a time)
printf '%s\n' "${valid_files[@]}" | \
    xargs -P 4 -I {} bash -c 'python /opt/etl/transform.py "{}" "/data/output/'$DATE'/"'

# Step 4: Upload results to S3
aws s3 sync "$OUTPUT/" "s3://company-lake/processed/$DATE/" --quiet
echo "Uploaded results to S3"

# Step 5: Archive originals
aws s3 mv "s3://company-lake/landing/$DATE/" "s3://company-lake/archive/$DATE/" --recursive --quiet
echo "Archived original files"

# Step 6: Cleanup local
rm -rf "$LANDING" "$OUTPUT"
echo "=== Complete: $file_count files processed ==="
```

---

## Pattern 2: Log Rotation and Archiving

```bash
#!/bin/bash
# Rotate and archive application logs (prevent disk fill)

LOG_DIR="/var/log/pipeline"
ARCHIVE_DIR="/data/log-archive"
MAX_AGE_DAYS=7
COMPRESS_AGE_DAYS=1

# Compress logs older than 1 day
find "$LOG_DIR" -name "*.log" -mtime +$COMPRESS_AGE_DAYS -not -name "*.gz" | while read logfile; do
    gzip "$logfile"
    echo "Compressed: $logfile"
done

# Move compressed logs to archive after 7 days
find "$LOG_DIR" -name "*.gz" -mtime +$MAX_AGE_DAYS | while read gzfile; do
    mv "$gzfile" "$ARCHIVE_DIR/"
done

# Upload old archives to S3 (long-term storage)
find "$ARCHIVE_DIR" -name "*.gz" -mtime +30 | while read old_archive; do
    aws s3 cp "$old_archive" "s3://company-logs/archive/" --quiet && rm "$old_archive"
done

# Report disk usage
echo "Current log usage: $(du -sh $LOG_DIR | cut -f1)"
echo "Archive usage: $(du -sh $ARCHIVE_DIR | cut -f1)"
```

---

## Pattern 3: Multi-Source File Ingestion

```bash
#!/bin/bash
# Ingest files from multiple sources (SFTP, S3, local) into unified landing zone

LANDING="/data/landing/$(date +%Y%m%d)"
mkdir -p "$LANDING"/{sftp,s3,local}

# Source 1: SFTP download
echo "Fetching from SFTP..."
sftp -b - partner@sftp.partner.com << EOF
lcd $LANDING/sftp
cd /outbound/daily
mget *.csv
bye
EOF

# Source 2: S3 sync
echo "Syncing from S3..."
aws s3 sync s3://partner-bucket/exports/ "$LANDING/s3/" --exclude "*" --include "$(date +%Y%m%d)*"

# Source 3: Local directory (another team drops files here)
echo "Checking local drops..."
find /shared/data-drops/ -newer /tmp/.last_ingest_marker -type f -exec mv {} "$LANDING/local/" \;
touch /tmp/.last_ingest_marker

# Unified processing (all sources → same pipeline)
total_files=$(find "$LANDING" -type f | wc -l)
echo "Total files to process: $total_files"

find "$LANDING" -type f | while read file; do
    python /opt/etl/ingest.py "$file" && mv "$file" "/data/archive/"
done

echo "Ingestion complete: $total_files files processed"
```

---

## Pattern 4: Data Quality Report Generator

```bash
#!/bin/bash
# Generate daily data quality report from CSV files

REPORT="/data/reports/dq_report_$(date +%Y%m%d).txt"

echo "=== Data Quality Report: $(date) ===" > "$REPORT"
echo "" >> "$REPORT"

for table_dir in /data/silver/*/; do
    table=$(basename "$table_dir")
    latest_file=$(ls -t "$table_dir"*.csv 2>/dev/null | head -1)
    [ -z "$latest_file" ] && continue
    
    row_count=$(wc -l < "$latest_file")
    col_count=$(head -1 "$latest_file" | awk -F',' '{print NF}')
    file_size=$(stat -c%s "$latest_file" | numfmt --to=iec)
    null_count=$(grep -c ',,' "$latest_file" || true)
    
    echo "Table: $table" >> "$REPORT"
    echo "  Rows: $((row_count - 1)) | Columns: $col_count | Size: $file_size" >> "$REPORT"
    echo "  Empty fields: $null_count | Last modified: $(date -r "$latest_file" '+%Y-%m-%d %H:%M')" >> "$REPORT"
    
    # Flag issues
    if [ $row_count -lt 100 ]; then
        echo "  ⚠️ WARNING: Low row count!" >> "$REPORT"
    fi
    echo "" >> "$REPORT"
done

cat "$REPORT"
# Optionally: email the report or post to Slack
```

---

## Interview Tips

> **Tip 1:** "Design a multi-source file ingestion pipeline" — Multiple sources (SFTP, S3, local) → unified landing zone → validation → processing → archive. Each source has its own fetch method but all feed the same downstream pipeline. Use markers/timestamps to track "new" files. Parallel processing for throughput.

> **Tip 2:** "How do you handle log management on a data pipeline server?" — Tiered: compress after 1 day (gzip), archive after 7 days (move to archive dir), upload to S3 after 30 days (long-term), delete local after upload. Cron job runs daily. Monitor disk usage. Alert before disk fills. Never let logs crash a pipeline!

> **Tip 3:** "How do you generate a quick data quality report in bash?" — `wc -l` for row counts, `head -1 | awk` for column counts, `grep -c ',,'` for empty fields, `stat` for file sizes/dates. Loop through all data directories, output to a report file. Fast alternative to building a full DQ tool — perfect for initial monitoring before investing in Great Expectations.
