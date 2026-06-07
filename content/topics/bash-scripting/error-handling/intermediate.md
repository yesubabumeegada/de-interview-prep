---
title: "Error Handling - Intermediate"
topic: bash-scripting
subtopic: error-handling
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, error-handling, try-catch, logging, retry, production]
---

# Bash Error Handling — Intermediate

## Try-Catch Pattern in Bash

```bash
#!/bin/bash
set -uo pipefail  # Note: NOT -e (we handle errors manually in try-catch)

# Bash doesn't have try/catch, but we can simulate it:
try() {
    set +e  # Disable exit-on-error inside "try"
    "$@"
    __TRY_EXIT_CODE=$?
    set -e  # Re-enable
}

catch() {
    if [ $__TRY_EXIT_CODE -ne 0 ]; then
        "$@"  # Execute the catch handler
    fi
}

# Usage:
try python transform.py
catch echo "Transform failed with exit code: $__TRY_EXIT_CODE"

# More practical pattern (inline):
{
    python step1.py
    python step2.py
    python step3.py
} || {
    echo "Pipeline failed at one of the steps!"
    alert "ETL pipeline error" "critical"
    exit 1
}
```

---

## Structured Error Logging

```bash
#!/bin/bash
# Structured error logging for production troubleshooting

LOG_FILE="/var/log/etl/pipeline_$(date +%Y%m%d).log"

log_error() {
    local message="$1"
    local context="${2:-}"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local caller="${FUNCNAME[1]:-main}"
    local line="${BASH_LINENO[0]}"
    
    # Structured log entry (parseable by log aggregators):
    echo "{\"ts\":\"$timestamp\",\"level\":\"ERROR\",\"msg\":\"$message\",\"func\":\"$caller\",\"line\":$line,\"context\":\"$context\"}" >> "$LOG_FILE"
    
    # Also print to stderr (visible in terminal):
    echo "[$timestamp] ERROR in $caller (line $line): $message" >&2
}

# Error trap that captures context:
error_handler() {
    local exit_code=$?
    local line_no=$1
    log_error "Command failed" "line=$line_no, exit_code=$exit_code, command=${BASH_COMMAND}"
}
trap 'error_handler $LINENO' ERR

# Now ANY error automatically logs with context:
set -e
cp nonexistent.csv dest.csv  # Triggers error_handler with full context!
# Log: {"ts":"2024-03-15 10:30:00","level":"ERROR","msg":"Command failed","func":"main","line":42,"context":"line=42, exit_code=1, command=cp nonexistent.csv dest.csv"}
```

---

## Comprehensive Error Recovery

```bash
#!/bin/bash
set -uo pipefail

# Multi-level error recovery strategy:
run_with_recovery() {
    local step_name="$1"; shift
    local primary_cmd="$1"; shift
    local fallback_cmd="${1:-}"
    
    echo "Running: $step_name"
    
    # Try primary command
    if eval "$primary_cmd"; then
        echo "  ✓ $step_name succeeded"
        return 0
    fi
    
    echo "  ⚠ $step_name failed — attempting recovery..."
    
    # Try fallback if provided
    if [ -n "$fallback_cmd" ]; then
        if eval "$fallback_cmd"; then
            echo "  ✓ $step_name recovered via fallback"
            return 0
        fi
    fi
    
    echo "  ✗ $step_name FAILED (no recovery possible)"
    return 1
}

# Usage:
run_with_recovery "Database Query" \
    "psql -h primary-db -c 'SELECT 1'" \
    "psql -h replica-db -c 'SELECT 1'"  # Fallback to replica!

run_with_recovery "S3 Download" \
    "aws s3 cp s3://primary-bucket/data.csv /tmp/" \
    "aws s3 cp s3://backup-bucket/data.csv /tmp/"  # Fallback to backup!

run_with_recovery "Transform" \
    "python transform_v2.py" \
    "python transform_v1.py"  # Fallback to stable version!
```

---

## Error Aggregation and Reporting

```bash
#!/bin/bash
# Process multiple items, collect all errors, report at end (don't stop on first)

declare -a ERRORS=()
TOTAL=0
SUCCEEDED=0

process_with_error_collection() {
    local item="$1"
    TOTAL=$((TOTAL + 1))
    
    if python process_item.py "$item" 2>/tmp/err_$$; then
        SUCCEEDED=$((SUCCEEDED + 1))
    else
        local error=$(cat /tmp/err_$$ | head -1)
        ERRORS+=("$item: $error")
    fi
    rm -f /tmp/err_$$
}

# Process all items (don't stop on individual failures):
for file in /data/landing/*.csv; do
    process_with_error_collection "$file"
done

# Report:
FAILED=${#ERRORS[@]}
echo ""
echo "=== Processing Summary ==="
echo "Total: $TOTAL | Succeeded: $SUCCEEDED | Failed: $FAILED"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "Errors:"
    for err in "${ERRORS[@]}"; do
        echo "  ❌ $err"
    done
    
    # Alert with summary
    alert "$FAILED/$TOTAL files failed processing"
    exit 1
fi
```

---

## Timeout with Graceful Handling

```bash
#!/bin/bash
# Timeout that gives the process time to clean up

run_with_timeout() {
    local timeout="$1"; shift
    local grace_period=10  # Seconds to wait after SIGTERM before SIGKILL
    
    # Run command in background
    "$@" &
    local pid=$!
    
    # Wait with timeout
    local waited=0
    while kill -0 $pid 2>/dev/null && [ $waited -lt $timeout ]; do
        sleep 1
        waited=$((waited + 1))
    done
    
    # Check if still running
    if kill -0 $pid 2>/dev/null; then
        echo "TIMEOUT: Process exceeded ${timeout}s — sending SIGTERM..."
        kill -TERM $pid
        
        # Give grace period for cleanup
        sleep $grace_period
        
        if kill -0 $pid 2>/dev/null; then
            echo "Process didn't stop — sending SIGKILL..."
            kill -9 $pid
        fi
        
        return 124  # Standard timeout exit code
    fi
    
    # Process finished normally — return its exit code
    wait $pid
}

# Usage:
run_with_timeout 600 python heavy_transform.py
case $? in
    0) echo "Success!" ;;
    124) echo "TIMEOUT — exceeded 10 minutes"; alert "Transform timeout!" ;;
    *) echo "Failed with code $?" ;;
esac
```

---

## Interview Tips

> **Tip 1:** "How do you implement error recovery in bash?" — Multi-level: (1) Retry (transient errors: network, DB connection), (2) Fallback (primary fails → try backup: replica DB, backup S3 bucket, stable code version), (3) Degrade gracefully (skip non-critical step, alert, continue). Pattern: primary command || fallback || alert + exit.

> **Tip 2:** "How do you handle errors in batch processing (50 files)?" — Don't stop on first error! Collect errors in an array, process all items, then report ALL failures at once. Exit code: 0 if all succeeded, 1 if any failed. This gives operators a complete picture (not "first error, fix, rerun, second error, fix, rerun..." loop).

> **Tip 3:** "BASH_COMMAND and LINENO in error traps?" — `trap 'handler $LINENO' ERR` fires on any error. Inside handler: `$LINENO` = line number that failed, `$BASH_COMMAND` = the exact command that failed, `$?` = exit code. Log all three → pinpoint errors instantly (no more "something failed somewhere in this 200-line script").
