---
title: "Bash Pipelines - Fundamentals"
topic: bash-scripting
subtopic: pipelines
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, pipelines, pipes, stdin, stdout, streaming, composition]
---

# Bash Pipelines — Fundamentals


## 🎯 Analogy

Think of bash pipelines like an assembly line: each worker (command) takes parts from the conveyor (stdin), does one job, and passes results along. No warehouse (temp files) needed — data flows continuously.

---
## What Are Bash Pipelines?

A pipeline connects the **output of one command to the input of the next** using the pipe operator (`|`). Data flows left-to-right through a chain of commands — each transforming the data in some way.

```bash
# Single command:
cat data.csv              # Outputs entire file

# Pipeline (3 commands chained):
cat data.csv | grep "US" | wc -l
# cat → outputs all lines → grep → keeps only "US" lines → wc → counts them

# This is equivalent to:
# 1. cat outputs all 1M rows
# 2. grep filters to ~300K US rows
# 3. wc counts: 300000
# Total: zero intermediate files, constant memory, instant composition!
```

> **Key Insight for DE:** Pipelines process data in a STREAM — each command processes one line at a time without loading the entire dataset into memory. A pipeline processing 50 GB uses only a few MB of RAM!

---

## Pipe Operator (|)

```bash
# The | connects stdout of left command to stdin of right command:
command1 | command2 | command3

# Data flows: command1 → command2 → command3
# Each command starts SIMULTANEOUSLY (parallel execution!)
# Data moves through as soon as it's produced (streaming, not batch)

# Common DE pipelines:
# Count rows in a CSV (excluding header):
tail -n +2 orders.csv | wc -l

# Extract and count unique values:
cut -d',' -f5 orders.csv | sort | uniq -c | sort -rn

# Filter + transform + save:
cat raw_data.csv | grep -v "^#" | awk -F',' '{print $1","$3","$5}' > clean_data.csv

# Compress on-the-fly:
cat huge_file.csv | gzip > huge_file.csv.gz

# Download + process + upload (streaming through!):
aws s3 cp s3://bucket/data.csv - | python transform.py | aws s3 cp - s3://bucket/output.csv
```

---

## stdin, stdout, stderr

```bash
# Every command has 3 channels:
# stdin (fd 0): input (keyboard or pipe)
# stdout (fd 1): normal output (displayed or piped)
# stderr (fd 2): error messages (displayed, NOT piped)

# stdin examples:
echo "hello" | cat              # cat reads from stdin (pipe)
cat < file.txt                  # cat reads from file via redirect
python script.py <<< "input"    # Here-string: pass string as stdin

# stdout: flows through pipes
echo "data" | wc -c             # stdout of echo → stdin of wc

# stderr: does NOT flow through pipes (goes to terminal)
ls nonexistent 2>/dev/null      # Suppress errors
ls nonexistent 2>&1 | grep "No" # Merge stderr into stdout THEN pipe

# Redirect patterns:
command > output.txt             # stdout to file (overwrite)
command >> output.txt            # stdout to file (append)
command 2> errors.txt            # stderr to file
command > out.txt 2>&1           # Both stdout and stderr to file
command &> combined.txt          # Shorthand: both to file (bash 4+)
command > /dev/null 2>&1         # Suppress ALL output (silent)
```

---

## Pipeline Building Blocks

### Common Commands in Pipelines

| Command | What It Does | Pipeline Role |
|---------|-------------|---------------|
| `cat` | Output file contents | SOURCE (start of pipeline) |
| `grep` | Filter lines by pattern | FILTER |
| `awk` | Process columns, compute | TRANSFORM |
| `sed` | Find/replace text | TRANSFORM |
| `sort` | Sort lines | SORT |
| `uniq` | Remove duplicates (on sorted input) | DEDUP |
| `cut` | Extract specific columns | PROJECT |
| `head` / `tail` | First/last N lines | LIMIT |
| `wc` | Count lines/words/bytes | AGGREGATE |
| `tee` | Copy stream to file AND continue pipe | FORK |
| `xargs` | Convert stdin to arguments | EXECUTE |

---

## Pipeline Examples for Data Engineering

### Quick Data Profiling

```bash
# How many rows?
wc -l < data.csv

# Column count:
head -1 data.csv | awk -F',' '{print NF}'

# Top 10 values in column 5:
tail -n +2 data.csv | cut -d',' -f5 | sort | uniq -c | sort -rn | head -10

# Find empty/null values in column 3:
awk -F',' 'NR>1 && ($3=="" || $3=="NULL")' data.csv | wc -l

# Sample N random rows:
tail -n +2 data.csv | shuf | head -20
```

### Data Transformation Pipeline

```bash
# CSV → filtered → transformed → new CSV
tail -n +2 raw_orders.csv |         # Skip header
    awk -F',' '$3 > 0' |             # Filter: positive amounts only
    awk -F',' -v OFS=',' '{
        $6 = $3 * 1.1;              # Add 10% markup column
        print
    }' |
    sort -t',' -k4 |                  # Sort by date column
    (echo "order_id,customer_id,amount,date,region,marked_up"; cat) \
    > processed_orders.csv            # Add header + save

echo "Processed: $(wc -l < processed_orders.csv) rows"
```

### Log Analysis Pipeline

```bash
# Find top error types in the last hour:
grep "$(date -d '1 hour ago' +%Y-%m-%dT%H)" pipeline.log | \
    grep "ERROR" | \
    grep -oP 'error_type=\K\w+' | \
    sort | uniq -c | sort -rn | head -5
# Output:
#   45 connection_timeout
#   12 disk_full
#    8 permission_denied
#    3 invalid_data
#    1 unknown
```

---

## tee (Fork the Stream)

```bash
# tee copies the stream to a file AND passes it through (T-split):
command | tee output.txt | next_command
# Data goes to BOTH output.txt AND next_command!

# Log + continue processing:
python transform.py | tee /var/log/transform_output.txt | python load.py
# transform output is: logged to file AND piped to load.py

# Multiple tee (fork into many destinations):
cat data.csv | tee copy1.csv | tee copy2.csv | wc -l
# Data goes to: copy1.csv, copy2.csv, AND wc (all get the same stream)

# Append mode:
command | tee -a logfile.txt | next_command
# Appends to logfile (doesn't overwrite)
```

---

## Process Substitution (Advanced Piping)

```bash
# Compare two pipeline results:
diff <(sort file1.csv) <(sort file2.csv)
# <(...) creates a temporary pipe: each sort runs in parallel, diff compares outputs!

# Feed multiple inputs to a command:
paste <(cut -d',' -f1 orders.csv) <(cut -d',' -f3 orders.csv)
# Combines column 1 and column 3 side by side

# Use a pipeline result as a file argument:
psql -f <(echo "SELECT COUNT(*) FROM orders WHERE date = '$(date +%Y-%m-%d)';")
# Generates SQL dynamically and feeds it as a "file" to psql
```

---

## Performance

```bash
# Pipelines are EFFICIENT:
# 1. Streaming: data flows one line at a time (constant memory!)
# 2. Parallel: all commands in the pipe run SIMULTANEOUSLY
# 3. No temp files: data stays in memory (kernel buffers)

# 50 GB file processed with ~5 MB RAM:
tail -n +2 huge.csv | grep "US" | cut -d',' -f1,3 | sort -u | wc -l
# All 5 commands run in parallel, each processing its input as it arrives
# Total memory: ~5 MB (kernel pipe buffers) regardless of file size!

# Compare with Python loading the same file:
# pandas: 50 GB × 2-3x overhead = 100-150 GB RAM needed!
# Pipeline: 5 MB regardless of size!
```

---


## ▶️ Try It Yourself

```bash
# Pipeline: count rows excluding header
tail -n +2 data.csv | wc -l

# Pipeline: top 10 values in column 3
cut -d',' -f3 data.csv | sort | uniq -c | sort -rn | head -10

# Pipeline: filter + transform + save (no temp files)
cat orders.csv | grep "US" | awk -F',' '{print $1","$3}' > us_orders.csv

# tee: split pipeline — send to file AND continue
cat data.csv | tee /tmp/backup.csv | grep "EU" | wc -l

# Process substitution: compare two streams without temp files
diff <(sort file1.csv) <(sort file2.csv)

# Parallel pipeline: download + process + upload without touching disk
aws s3 cp s3://bucket/huge.csv - | python transform.py | aws s3 cp - s3://bucket/out.csv
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How do bash pipelines handle large files?" — Streaming: data flows line-by-line through the pipe (not loaded into memory). All commands run in parallel. A pipeline processing 50 GB uses ~5 MB RAM (kernel pipe buffers only). This is why bash can process files that would crash Python/pandas with OOM.

> **Tip 2:** "How does the pipe operator work?" — `|` connects stdout of left command to stdin of right command. Both commands run simultaneously (parallel). Data flows as soon as it's produced (streaming). stderr is NOT piped (goes to terminal unless explicitly redirected with `2>&1`).

> **Tip 3:** "tee — what is it and when do you use it?" — `tee` copies the stream to a file AND passes it through the pipe (T-shaped split). Use for: logging intermediate results while continuing processing (`transform | tee log.txt | load`), creating backup copies in-stream, or debugging pipelines (inspect what data looks like at each stage).
