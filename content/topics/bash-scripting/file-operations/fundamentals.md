---
title: "File Operations - Fundamentals"
topic: bash-scripting
subtopic: file-operations
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, file-operations, linux, scripting, data-engineering]
---

# Bash File Operations — Fundamentals

## Why File Operations Matter for DE

Data engineers constantly work with files: landing zone data, log files, config management, pipeline scripts, and data transfers. Knowing bash file operations makes you efficient on servers, in Docker containers, and in CI/CD pipelines.

```bash
# Common DE file tasks:
# - Move files between directories (landing → processing → archive)
# - Check if a file exists before processing
# - Get file sizes for monitoring
# - Compress/decompress data files
# - Parse file paths for metadata (date extraction from filename)
```

---

## Basic File Commands

### Reading Files

```bash
# Display file content
cat data.csv                    # Print entire file
head -n 20 data.csv             # First 20 lines
tail -n 50 data.csv             # Last 50 lines
tail -f /var/log/pipeline.log   # Follow log in real-time (streaming)

# Count lines/words/bytes
wc -l data.csv                  # Line count (row count!)
wc -w data.csv                  # Word count
wc -c data.csv                  # Byte count (file size)

# View specific columns (CSV)
cut -d',' -f1,3 data.csv        # Fields 1 and 3 (comma delimiter)
cut -d'|' -f2-5 data.tsv        # Fields 2 through 5 (pipe delimiter)
```

### Creating and Writing Files

```bash
# Create empty file
touch new_file.txt

# Write to file (overwrite)
echo "Pipeline started at $(date)" > pipeline.log

# Append to file
echo "Step 1 complete" >> pipeline.log
echo "Step 2 complete" >> pipeline.log

# Write multi-line content (heredoc)
cat > config.yaml << EOF
database:
  host: prod-db.internal
  port: 5432
  name: analytics
EOF

# Redirect command output to file
ls -la /data/landing/ > file_listing.txt
psql -c "SELECT COUNT(*) FROM orders" > row_count.txt
```

### Copying, Moving, Deleting

```bash
# Copy
cp source.csv /backup/source.csv           # Copy file
cp -r /data/landing/ /data/backup/         # Copy directory recursively
cp -p source.csv dest.csv                   # Preserve permissions/timestamps

# Move/Rename
mv landing/file.csv processing/file.csv    # Move file
mv old_name.csv new_name.csv               # Rename file
mv /data/landing/*.csv /data/archive/      # Move all CSVs

# Delete
rm file.csv                                # Delete file
rm -f file.csv                             # Force delete (no confirmation)
rm -rf /tmp/scratch/                       # Delete directory recursively (CAREFUL!)

# Safe delete pattern for DE pipelines:
# Move to trash first, delete later:
mv processed_file.csv /data/trash/
# Cron job cleans trash after 7 days
```

---

## File Testing (Conditionals)

```bash
# Check if file exists before processing:
if [ -f "/data/landing/orders.csv" ]; then
    echo "File found — processing..."
    python process_orders.py
else
    echo "ERROR: orders.csv not found!"
    exit 1
fi

# Common file tests:
# -f FILE    : file exists and is regular file
# -d DIR     : directory exists
# -s FILE    : file exists and is NOT empty (size > 0)
# -r FILE    : file is readable
# -w FILE    : file is writable
# -x FILE    : file is executable
# -e PATH    : path exists (file or directory)

# DE pattern: wait for file to appear (sensor-like behavior)
MAX_WAIT=3600  # 1 hour
WAITED=0
while [ ! -f "/data/landing/daily_export.csv" ]; do
    sleep 60
    WAITED=$((WAITED + 60))
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "TIMEOUT: File not received after 1 hour!"
        exit 1
    fi
    echo "Waiting for file... ($WAITED seconds elapsed)"
done
echo "File received! Processing..."
```

---

## File Permissions

```bash
# View permissions
ls -la data.csv
# -rw-r--r-- 1 dataeng dataeng 1048576 Mar 15 10:00 data.csv
# │││ │││ │││
# │││ │││ └── Others: read only
# │││ └──── Group: read only  
# └────── Owner: read + write

# Change permissions
chmod 755 pipeline.sh      # rwx r-x r-x (owner:all, group:read+exec, others:read+exec)
chmod 644 data.csv         # rw- r-- r-- (owner:read+write, group:read, others:read)
chmod +x run_etl.sh        # Add execute permission (make script runnable)

# Change ownership
chown dataeng:dataeng pipeline.sh   # Set owner and group
chown -R dataeng /data/pipeline/    # Recursively change ownership
```

---

## Finding Files

```bash
# Find files by name
find /data/landing -name "*.csv"                    # All CSVs in landing
find /data -name "orders_2024*" -type f             # Files matching pattern
find /logs -name "*.log" -mtime -1                  # Logs modified in last 24 hours
find /data -name "*.parquet" -size +100M            # Parquet files > 100 MB

# Find and act on results
find /data/archive -name "*.csv" -mtime +90 -delete  # Delete CSVs older than 90 days
find /data/landing -name "*.json" -exec wc -l {} \;  # Count lines in each JSON file

# Find with xargs (more efficient for many files):
find /data -name "*.gz" | xargs ls -lh              # List all .gz files with sizes
find /data -name "*.csv" | xargs wc -l | tail -1    # Total line count across all CSVs
```

---

## Compression

```bash
# Compress (for archiving processed files)
gzip data.csv                        # Creates data.csv.gz (removes original!)
gzip -k data.csv                     # Keep original (creates data.csv.gz alongside)
gzip -9 data.csv                     # Maximum compression (slower, smaller)

# Decompress
gunzip data.csv.gz                   # Decompress (removes .gz)
gzip -d data.csv.gz                  # Same as gunzip
zcat data.csv.gz | head -5           # View compressed file without decompressing!

# Tar (archive multiple files)
tar -czf archive.tar.gz /data/2024/03/   # Create compressed archive
tar -xzf archive.tar.gz                   # Extract archive
tar -tzf archive.tar.gz                   # List contents without extracting

# DE pattern: compress after processing, decompress before processing
gzip -d /data/landing/orders.csv.gz       # Decompress incoming file
python process_orders.py                   # Process
gzip /data/processed/orders.csv           # Compress processed output
aws s3 cp orders.csv.gz s3://archive/     # Upload compressed to S3
```

---

## File Metadata

```bash
# File size
stat -c %s data.csv                  # Size in bytes (Linux)
ls -lh data.csv                      # Human-readable size (1.2G, 450M, etc.)
du -sh /data/landing/                # Total size of directory

# File timestamps
stat data.csv                        # Full metadata (access, modify, change times)
date -r data.csv                     # Last modification time

# Extract metadata from filename (common DE pattern):
FILENAME="orders_2024-03-15_us-east.csv"
DATE=$(echo $FILENAME | grep -oP '\d{4}-\d{2}-\d{2}')    # 2024-03-15
REGION=$(echo $FILENAME | grep -oP '(?<=_)[a-z-]+(?=\.)')  # us-east
echo "Date: $DATE, Region: $REGION"
```

---

## Interview Tips

> **Tip 1:** "How do you process files in a landing zone?" — Check file exists (-f test), validate non-empty (-s test), move to processing directory, run transformation, move to archive on success or error directory on failure. Always: atomic moves (mv), not copies followed by deletes (risk of partial state).

> **Tip 2:** "How do you find large files consuming disk space?" — `find /data -type f -size +1G | xargs ls -lhS` (find files > 1 GB, list sorted by size). For directories: `du -sh /data/* | sort -rh | head -10` (top 10 largest subdirectories). Essential for: troubleshooting full disks on data pipeline servers.

> **Tip 3:** "How do you safely delete old files?" — `find /data/archive -name "*.csv" -mtime +90 -delete` (delete files older than 90 days). Add `-print` first to verify what would be deleted. In production: move to trash first, delete from trash after N days (safety net against accidental deletion of needed files).
