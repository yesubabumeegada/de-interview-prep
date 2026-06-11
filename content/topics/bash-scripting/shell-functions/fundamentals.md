---
title: "Shell Functions - Fundamentals"
topic: bash-scripting
subtopic: shell-functions
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, functions, reusable-code, scripting, modularity]
---

# Shell Functions — Fundamentals


## 🎯 Analogy

Think of shell functions like reusable macros: instead of copy-pasting the same 5-line S3 upload + log pattern in every script, you define it once as a function and call it everywhere.

---
## What Are Shell Functions?

Functions are **reusable blocks of code** within a bash script. They promote: DRY (Don't Repeat Yourself), readability, and testability. Think of them as mini-scripts within your script.

```bash
# Define a function:
greet() {
    echo "Hello, $1!"
}

# Call it:
greet "Data Engineer"   # Output: Hello, Data Engineer!
greet "World"           # Output: Hello, World!

# More practical — logging function used throughout a pipeline:
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2"
}

log "INFO" "Pipeline started"
log "INFO" "Processing 5000 rows"
log "ERROR" "Connection timeout!"
# [2024-03-15 10:30:00] [INFO] Pipeline started
# [2024-03-15 10:30:05] [INFO] Processing 5000 rows
# [2024-03-15 10:30:10] [ERROR] Connection timeout!
```

---

## Function Syntax

```bash
# Syntax Option 1 (preferred):
function_name() {
    # commands
    echo "I'm a function"
}

# Syntax Option 2 (alternative):
function function_name {
    # commands
    echo "I'm also a function"
}

# Parameters: accessed via $1, $2, $3... (positional)
process_file() {
    local file="$1"           # First argument
    local output_dir="$2"     # Second argument
    local format="${3:-csv}"  # Third argument with default "csv"
    
    echo "Processing $file → $output_dir (format: $format)"
}

process_file "orders.json" "/data/output" "parquet"
process_file "events.csv" "/data/output"  # format defaults to "csv"
```

---

## Return Values

```bash
# Functions return an EXIT CODE (0-255), not a string!
# 0 = success, non-zero = failure

is_file_valid() {
    local file="$1"
    [ -f "$file" ] && [ -s "$file" ]  # Returns 0 (true) or 1 (false)
}

if is_file_valid "/data/orders.csv"; then
    echo "File is valid!"
else
    echo "File is missing or empty!"
fi

# To "return" a string: use echo + command substitution
get_row_count() {
    local file="$1"
    local count=$(wc -l < "$file")
    echo $((count - 1))  # Subtract header
}

ROW_COUNT=$(get_row_count "/data/orders.csv")
echo "Row count: $ROW_COUNT"

# Return vs Echo:
# return N → sets exit status (for if/then checking)
# echo "value" → outputs text (capture with $(...))
```

---

## Local Variables

```bash
# WITHOUT local: variable leaks to the caller (bug-prone!)
bad_function() {
    result="I leaked!"  # Changes caller's 'result' variable!
}

# WITH local: variable is scoped to the function (safe!)
good_function() {
    local result="I stay inside"  # Only exists within this function
    local temp_file=$(mktemp)
    # ...
    rm -f "$temp_file"
}

# RULE: Always use 'local' for variables inside functions!
# Exception: when you intentionally want to set a caller variable (rare)

process_batch() {
    local input="$1"
    local output="$2"
    local count=0
    local status="success"
    
    # All variables above are local — can't accidentally affect the caller
    while IFS= read -r line; do
        count=$((count + 1))
    done < "$input"
    
    echo "$count"  # Return value via stdout
}
```

---

## Common Utility Functions for Data Engineering

```bash
#!/bin/bash
# /opt/etl/lib/utils.sh — shared utility functions

# Logging with levels
log() {
    local level="$1"; shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a "$LOG_FILE"
}
log_info() { log "INFO" "$@"; }
log_warn() { log "WARN" "$@"; }
log_error() { log "ERROR" "$@"; }

# Retry a command N times with backoff
retry() {
    local max_attempts="$1"; shift
    local delay="$1"; shift
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if "$@"; then
            return 0
        fi
        log_warn "Attempt $attempt/$max_attempts failed. Retrying in ${delay}s..."
        sleep $delay
        delay=$((delay * 2))  # Exponential backoff
        attempt=$((attempt + 1))
    done
    log_error "All $max_attempts attempts failed!"
    return 1
}

# Check if a command/tool is available
require_command() {
    local cmd="$1"
    if ! command -v "$cmd" &>/dev/null; then
        log_error "Required command not found: $cmd"
        exit 1
    fi
}

# Measure execution time
timed() {
    local start=$(date +%s)
    "$@"
    local status=$?
    local duration=$(( $(date +%s) - start ))
    log_info "Command took ${duration}s: $*"
    return $status
}

# Alert via Slack
alert() {
    local message="$1"
    local severity="${2:-warning}"  # warning or critical
    local emoji=$( [ "$severity" = "critical" ] && echo "🚨" || echo "⚠️" )
    
    if [ -n "${SLACK_WEBHOOK:-}" ]; then
        curl -sS -X POST "$SLACK_WEBHOOK" \
            -d "{\"text\":\"$emoji $message\"}" > /dev/null 2>&1
    fi
    log_warn "ALERT ($severity): $message"
}

# Usage in pipeline scripts:
# source /opt/etl/lib/utils.sh
# LOG_FILE="/var/log/etl/pipeline.log"
# 
# require_command "psql"
# require_command "aws"
# log_info "Starting pipeline"
# retry 3 5 psql -h "$DB_HOST" -c "SELECT 1"
# timed python /opt/etl/transform.py
# alert "Pipeline completed" "info"
```

---

## Sourcing Function Libraries

```bash
# Keep functions in separate files (like Python modules):

# /opt/etl/lib/utils.sh → logging, retry, alert
# /opt/etl/lib/db.sh → database operations
# /opt/etl/lib/s3.sh → S3 operations
# /opt/etl/lib/validation.sh → data quality checks

# In your pipeline script:
#!/bin/bash
source /opt/etl/lib/utils.sh
source /opt/etl/lib/db.sh
source /opt/etl/lib/validation.sh

# Now all functions from those files are available:
log_info "Starting pipeline"
db_connect "$DB_HOST" "$DB_PORT"
validate_csv "/data/landing/orders.csv" 8 1000
```

---


## ▶️ Try It Yourself

```bash
#!/bin/bash

# Function with arguments and return value (via exit code)
log() {
    local level="$1"; shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a /tmp/pipeline.log
}

check_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        log ERROR "File not found: $path"
        return 1
    fi
    log INFO "File found: $path ($(wc -l < "$path") lines)"
    return 0
}

upload_to_s3() {
    local src="$1" dst="$2"
    log INFO "Uploading $src -> $dst"
    if aws s3 cp "$src" "$dst"; then
        log INFO "Upload succeeded"
    else
        log ERROR "Upload failed"
        return 1
    fi
}

# Usage
log INFO "Pipeline starting"
check_file "/tmp/orders.csv" || exit 1
upload_to_s3 "/tmp/orders.csv" "s3://my-bucket/raw/orders.csv" || exit 1
log INFO "Pipeline complete" 
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How do you structure reusable bash code?" — Function libraries: put common functions (logging, retry, alerting, validation) in /opt/etl/lib/*.sh files. Source them at the top of each pipeline script. Same pattern as Python imports. Benefits: DRY, testable, consistent behavior across all scripts.

> **Tip 2:** "local keyword — why is it important?" — Without `local`, variables in functions pollute the caller's namespace (bugs!). Always use `local` for function-internal variables. This prevents: accidental overwrites, hard-to-debug state leaks, and name collisions between functions.

> **Tip 3:** "How do you return values from functions?" — Two methods: (1) Exit code for success/failure (`return 0` or `return 1` — use with if/then), (2) Echo for data (`echo "$result"` — capture with `$(function_name args)`). Never use both for the same purpose (confusing). Convention: return for status, echo for data.
