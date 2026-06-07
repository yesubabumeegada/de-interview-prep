---
title: "Text Processing - Senior Deep Dive"
topic: bash-scripting
subtopic: text-processing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, text-processing, production, performance, streaming]
---

# Bash Text Processing — Senior-Level Deep Dive

## High-Performance Text Processing

### Processing Multi-GB Files Efficiently

```bash
# PRINCIPLE: Stream data through pipes — never load full file into memory!

# Process 50 GB CSV: count orders by region (constant memory, ~5 min):
tail -n +2 huge_file.csv | \          # Skip header (stream)
    cut -d',' -f5 | \                  # Extract region column (stream)
    sort | \                           # Sort (uses temp files for large data)
    uniq -c | \                        # Count (streaming)
    sort -rn                           # Final sort by count

# Parallel processing with GNU sort (uses multiple cores):
export LC_ALL=C  # Byte-level sort (faster than locale-aware)
sort --parallel=8 -t',' -k3 -n huge_file.csv > sorted.csv
# 8 threads, numeric sort on column 3

# mawk vs gawk: mawk is 2-5x faster for simple operations
mawk -F',' '{sum += $3} END {print sum}' huge_file.csv
# For 50 GB: mawk ~3 min vs gawk ~8 min

# Avoid unnecessary operations:
# BAD (sorts TWICE):
cat file.csv | sort | uniq -c | sort -rn
# GOOD (sort only once, awk handles counting):
awk -F',' '{count[$5]++} END {for(k in count) print count[k], k}' file.csv | sort -rn
# awk does counting in memory (hash map) — one pass, no sort needed for the counting phase!
```

---

## Production Log Analysis

```bash
# Real-time log monitoring with pattern detection:

# Count errors per minute (rolling window):
tail -f /var/log/pipeline.log | \
    grep --line-buffered "ERROR" | \
    awk '{
        # Extract minute from timestamp
        minute = substr($1, 1, 16)
        count[minute]++
        if (count[minute] > 10) {
            printf "ALERT: %d errors in minute %s\n", count[minute], minute
            system("curl -s -X POST $SLACK_WEBHOOK -d '{\"text\":\"Error spike!\"}'")
        }
    }'

# Parse structured logs into metrics:
# Input: 2024-03-15T10:30:45 [INFO] pipeline=orders step=transform rows=50000 duration_ms=3200

awk '/\[INFO\]/ && /pipeline=/ {
    for (i=1; i<=NF; i++) {
        if ($i ~ /^pipeline=/) pipeline = substr($i, 10)
        if ($i ~ /^step=/) step = substr($i, 6)
        if ($i ~ /^duration_ms=/) duration = substr($i, 13) + 0
    }
    if (duration > 0) {
        total[pipeline"-"step] += duration
        count[pipeline"-"step]++
        if (duration > max[pipeline"-"step]) max[pipeline"-"step] = duration
    }
}
END {
    printf "%-30s %8s %8s %8s\n", "Pipeline-Step", "Count", "Avg(ms)", "Max(ms)"
    for (key in total) {
        printf "%-30s %8d %8.0f %8d\n", key, count[key], total[key]/count[key], max[key]
    }
}' pipeline.log
```

---

## Complex Data Transformations

```bash
# Denormalize: join two CSVs on a key (without csvkit)
# orders.csv: order_id,customer_id,amount
# customers.csv: customer_id,name,region

# Load customers into awk associative array, then join:
awk -F',' '
    NR == FNR { name[$1] = $2; region[$1] = $3; next }  # First file: build lookup
    FNR > 1 { print $0","name[$2]","region[$2] }          # Second file: join
' customers.csv orders.csv
# Output: order_id,customer_id,amount,customer_name,region
# This is a hash join — O(n+m) time, O(m) memory for the lookup table!

# Sessionization in awk (group events into sessions by time gap):
awk -F',' '
    NR > 1 {
        user = $1; ts = $2 + 0  # Convert timestamp to number
        if (user != prev_user || ts - prev_ts > 1800) {  # 30-min gap = new session
            session_id++
        }
        print $0","session_id
        prev_user = user; prev_ts = ts
    }
' <(sort -t',' -k1,1 -k2,2n events.csv)
# Groups events into sessions (same user, <30 min between events)

# SCD Type 1 in bash (keep latest record per key):
sort -t',' -k1,1 -k5,5r data.csv | \   # Sort by ID then timestamp (desc)
    awk -F',' '!seen[$1]++ { print }'     # Keep first occurrence per ID (latest!)
```

---

## Building Data Quality Checks

```bash
#!/bin/bash
# Comprehensive DQ validation suite for a CSV file

validate_data() {
    local file="$1"
    local errors=0
    
    echo "=== Data Quality Report: $(basename $file) ==="
    echo "Rows: $(tail -n +2 "$file" | wc -l)"
    echo "Columns: $(head -1 "$file" | awk -F',' '{print NF}')"
    
    # Check 1: Consistent column count
    bad_cols=$(awk -F',' 'NF != cols { if(NR==1){cols=NF}else{print NR} }' "$file" | wc -l)
    if [ "$bad_cols" -gt 0 ]; then
        echo "❌ Column count inconsistency: $bad_cols rows have wrong column count"
        errors=$((errors + 1))
    else
        echo "✓ Column count consistent"
    fi
    
    # Check 2: Null/empty primary key (column 1)
    null_pks=$(awk -F',' 'NR>1 && ($1=="" || $1=="NULL" || $1=="null")' "$file" | wc -l)
    if [ "$null_pks" -gt 0 ]; then
        echo "❌ Null primary keys: $null_pks rows"
        errors=$((errors + 1))
    else
        echo "✓ No null primary keys"
    fi
    
    # Check 3: Duplicate primary keys
    dupes=$(tail -n +2 "$file" | cut -d',' -f1 | sort | uniq -d | wc -l)
    if [ "$dupes" -gt 0 ]; then
        echo "❌ Duplicate primary keys: $dupes"
        errors=$((errors + 1))
    else
        echo "✓ No duplicate primary keys"
    fi
    
    # Check 4: Numeric column validation (column 3 = amount)
    non_numeric=$(tail -n +2 "$file" | cut -d',' -f3 | grep -cvP '^-?[0-9]+\.?[0-9]*$' || true)
    if [ "$non_numeric" -gt 0 ]; then
        echo "❌ Non-numeric amounts: $non_numeric rows"
        errors=$((errors + 1))
    else
        echo "✓ All amounts are numeric"
    fi
    
    # Check 5: Date format validation (column 4)
    bad_dates=$(tail -n +2 "$file" | cut -d',' -f4 | grep -cvP '^\d{4}-\d{2}-\d{2}$' || true)
    if [ "$bad_dates" -gt 0 ]; then
        echo "❌ Invalid date format: $bad_dates rows (expected YYYY-MM-DD)"
        errors=$((errors + 1))
    else
        echo "✓ All dates valid"
    fi
    
    echo ""
    echo "Total issues: $errors"
    return $errors
}

validate_data "$1"
```

---

## Interview Tips

> **Tip 1:** "How do you join two CSV files in bash?" — Load the smaller file into an awk associative array (hash), then stream through the larger file and look up values. This is a hash join: O(n+m) time. Works for: enriching a fact file with a dimension lookup. For complex joins: use `csvjoin` or Python/SQL instead.

> **Tip 2:** "How do you handle 50 GB files in bash?" — Stream processing: pipe commands together (`tail | cut | sort | uniq`). Each command processes one line at a time (constant memory). Use `LC_ALL=C` for faster sorting. Use `sort --parallel=N` for multi-core sorting. Use `mawk` instead of `gawk` (2-5x faster). Never use `cat file | command` — use `command < file` or `command file` instead.

> **Tip 3:** "How do you build a data quality check in bash?" — Shell function that validates: column count consistency (awk NF), null primary keys (grep empty fields), duplicates (sort | uniq -d), numeric validation (grep regex), date format validation. Returns exit code based on pass/fail. Run before loading data into the warehouse — cheap gate that catches 80% of issues.
