---
title: "Bash Pipelines - Intermediate"
topic: bash-scripting
subtopic: pipelines
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, pipelines, xargs, parallel, named-pipes, streaming]
---

# Bash Pipelines — Intermediate

## xargs (Convert stdin to Arguments)

```bash
# xargs reads lines from stdin and passes them as ARGUMENTS to a command:

# Find CSVs and count lines in each:
find /data -name "*.csv" | xargs wc -l

# Parallel execution (process 4 files simultaneously):
find /data -name "*.csv" | xargs -P 4 -I {} python transform.py {}
# -P 4: max 4 parallel processes
# -I {}: replace {} with each input line

# Safe handling of filenames with spaces:
find /data -name "*.csv" -print0 | xargs -0 -P 4 wc -l
# -print0 + -0: use null delimiters (handles spaces in filenames!)

# Batch operations (pass N items per invocation):
cat file_list.txt | xargs -n 10 aws s3 rm
# Passes 10 files per aws command (fewer API calls)

# Delete files from a list:
cat files_to_delete.txt | xargs rm -f

# Run SQL for each table:
echo "orders events customers" | tr ' ' '\n' | \
    xargs -I {} psql -c "ANALYZE silver.{}"
```

---

## Named Pipes (FIFOs)

```bash
# Named pipes: persistent pipes that exist as files (for complex topologies)

# Create:
mkfifo /tmp/data_pipe

# Writer (in one terminal/process):
cat huge_data.csv > /tmp/data_pipe &

# Reader (in another):
python process.py < /tmp/data_pipe

# Use case: split a stream into multiple consumers
mkfifo /tmp/pipe_a /tmp/pipe_b

# Fork: one source, two consumers (run in parallel)
tee /tmp/pipe_a < input.csv | consumer_b.py &   # pipe_b gets stdin
consumer_a.py < /tmp/pipe_a &                    # pipe_a gets tee output
wait

# Cleanup:
rm /tmp/pipe_a /tmp/pipe_b
```

---

## Multi-Command Pipelines

```bash
# Complex DE pipeline: download → decompress → transform → compress → upload
aws s3 cp s3://source/data.csv.gz - | \   # Download to stdout
    gunzip | \                              # Decompress (streaming)
    awk -F',' 'NR==1 || $5=="US"' | \       # Filter US rows (keep header)
    gzip | \                                # Re-compress
    aws s3 cp - s3://target/us_data.csv.gz  # Upload from stdin

# Total disk usage: ZERO! (all streaming through pipes)
# Total memory: ~10 MB (pipe buffers only)
# Works for 100 GB files on a machine with 1 GB RAM!

# Pipeline with error handling:
set -o pipefail  # Fail if ANY command in pipe fails

cat data.csv | \
    grep -v "^#" | \           # Remove comments
    awk -F',' 'NF == 8' | \    # Keep only rows with 8 columns
    sort -t',' -k1 -u | \      # Sort + dedup by key (column 1)
    tee /data/clean.csv | \     # Save intermediate result
    wc -l                       # Count final rows

echo "Pipeline exit code: $? (0=all commands succeeded)"
```

---

## Pipeline Performance Optimization

```bash
# TIP 1: Reduce data EARLY in the pipeline (filter first!)
# BAD (sorts 50M rows, then filters to 1M):
cat huge.csv | sort | grep "US"

# GOOD (filters to 1M rows, then sorts only 1M):
cat huge.csv | grep "US" | sort
# 50x faster! (sort is O(n log n) — reducing n early is huge)

# TIP 2: Use LC_ALL=C for faster sort/grep (byte-level, not locale-aware):
export LC_ALL=C
cat data.csv | sort -t',' -k1    # 3-5x faster than locale-aware sort!

# TIP 3: Use parallel sort for multi-core:
sort --parallel=8 -t',' -k1 huge.csv   # Uses 8 CPU cores

# TIP 4: Avoid unnecessary cats (useless use of cat!):
# BAD: cat file | grep pattern
# GOOD: grep pattern file (one fewer process!)

# TIP 5: Use mawk instead of gawk for speed:
# mawk: simpler, 2-5x faster for standard operations
cat huge.csv | mawk -F',' '{sum += $3} END {print sum}'
```

---

## Pipeline Debugging

```bash
# Technique 1: Inspect intermediate stages with tee
cat data.csv | \
    tee /tmp/stage1.txt | grep "US" | \
    tee /tmp/stage2.txt | sort | \
    tee /tmp/stage3.txt | uniq -c
# Check /tmp/stage1.txt, stage2.txt, stage3.txt to see data at each stage

# Technique 2: Use head to limit during development
cat huge.csv | head -100 | grep "US" | sort | uniq -c
# Process only first 100 lines (fast iteration!)
# Remove 'head -100' when ready for production

# Technique 3: Count rows at each stage
cat data.csv | \
    tee >(wc -l >&2) | grep "US" | \
    tee >(wc -l >&2) | sort -u | \
    wc -l
# Prints row count at each stage to stderr:
# 1000000 (total rows)
# 350000 (after grep US)
# 280000 (after sort -u / dedup)
# Shows where data is being filtered/lost

# Technique 4: PIPESTATUS array (exit codes of all commands)
cat data.csv | grep "pattern" | wc -l
echo "${PIPESTATUS[@]}"
# 0 1 0 → cat succeeded, grep found nothing (exit 1), wc succeeded
# With pipefail: overall exit = 1 (grep's failure propagates)
```

---

## Interview Tips

> **Tip 1:** "How do you process a 100 GB file without disk space?" — Stream it through pipes: `aws s3 cp file - | gunzip | awk '...' | gzip | aws s3 cp - output`. Zero disk usage! Data streams from S3 → through transforms → back to S3. Works for ANY size file because pipes use constant memory (~10 MB buffers).

> **Tip 2:** "How do you debug a long pipeline?" — Insert `tee /tmp/stageN.txt` between stages to save intermediate results. Or add `tee >(wc -l >&2)` to print row counts at each stage (to stderr, so it doesn't interfere with the pipe). Check `${PIPESTATUS[@]}` to see which command failed.

> **Tip 3:** "Pipeline performance tips?" — (1) Filter early (reduce data volume before expensive operations like sort), (2) `LC_ALL=C` for byte-level sort/grep (3-5x faster), (3) `sort --parallel=N` for multi-core sorting, (4) Avoid useless `cat` (use `grep pattern file` not `cat file | grep pattern`), (5) Use `mawk` over `gawk` for speed.
