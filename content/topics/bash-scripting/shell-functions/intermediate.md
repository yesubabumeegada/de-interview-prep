---
title: "Shell Functions - Intermediate"
topic: bash-scripting
subtopic: shell-functions
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, functions, libraries, patterns, error-handling]
---

# Shell Functions — Intermediate

## Advanced Function Patterns

### Function Libraries with Namespacing

```bash
# Prevent naming collisions between libraries:
# /opt/etl/lib/db.sh
db_connect() { psql "postgresql://$1:$2@$3/$4" -c "SELECT 1" > /dev/null 2>&1; }
db_query() { psql "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST/$DB_NAME" -t -A -c "$1"; }
db_row_count() { db_query "SELECT COUNT(*) FROM $1"; }

# /opt/etl/lib/s3.sh
s3_list() { aws s3 ls "$1" --recursive | awk '{print $4}'; }
s3_download() { aws s3 cp "$1" "$2" --quiet; }
s3_upload() { aws s3 cp "$1" "$2" --quiet; }
s3_file_count() { aws s3 ls "$1" | wc -l; }

# Usage:
source /opt/etl/lib/db.sh
source /opt/etl/lib/s3.sh
db_row_count "production.orders"  # 5000000
s3_file_count "s3://bucket/landing/"  # 42
```

### Higher-Order Functions (Functions Calling Functions)

```bash
# Process multiple tables with the same logic:
process_table() {
    local table="$1"
    local transform_fn="$2"  # Function to apply!
    
    log_info "Processing: $table"
    local data=$(db_query "SELECT * FROM raw.$table LIMIT 10000")
    
    # Call the passed function:
    echo "$data" | $transform_fn
}

# Different transform functions:
clean_orders() { awk -F',' '$3 > 0 {print}'; }
clean_events() { grep -v "^test_"; }

# Apply:
process_table "orders" "clean_orders"
process_table "events" "clean_events"
```

### Error-Aware Functions

```bash
# Functions that handle and report their own errors:
safe_db_query() {
    local query="$1"
    local description="${2:-query}"
    local result
    
    result=$(db_query "$query" 2>/tmp/db_error_$$)
    local status=$?
    
    if [ $status -ne 0 ]; then
        local error=$(cat /tmp/db_error_$$)
        log_error "DB $description failed: $error"
        rm -f /tmp/db_error_$$
        return 1
    fi
    
    rm -f /tmp/db_error_$$
    echo "$result"
}

# Usage:
count=$(safe_db_query "SELECT COUNT(*) FROM orders" "order count") || {
    alert "Cannot query orders table!"
    exit 1
}
echo "Orders: $count"
```

---

## Function Testing

```bash
#!/bin/bash
# Test your functions (like unit tests!)

source /opt/etl/lib/utils.sh

# Test framework (minimal):
TESTS_PASSED=0; TESTS_FAILED=0

assert_equals() {
    local expected="$1" actual="$2" test_name="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  ✓ $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo "  ✗ $test_name (expected: '$expected', got: '$actual')"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

assert_exit_code() {
    local expected="$1"; shift
    "$@" > /dev/null 2>&1
    local actual=$?
    assert_equals "$expected" "$actual" "$*"
}

# Tests:
echo "=== Testing utils.sh ==="

# Test: retry succeeds on first attempt
assert_exit_code 0 retry 3 1 true

# Test: retry fails after max attempts
assert_exit_code 1 retry 2 0 false

# Test: require_command for existing command
assert_exit_code 0 bash -c 'source /opt/etl/lib/utils.sh; require_command "bash"'

# Summary:
echo ""
echo "Results: $TESTS_PASSED passed, $TESTS_FAILED failed"
[ $TESTS_FAILED -eq 0 ] && exit 0 || exit 1
```

---

## Pipeline Composition with Functions

```bash
#!/bin/bash
# Build a pipeline by composing functions (like building blocks)

source /opt/etl/lib/utils.sh
source /opt/etl/lib/db.sh
source /opt/etl/lib/validation.sh

# Define pipeline steps as functions:
step_ingest() {
    log_info "Step 1: Ingesting from S3"
    aws s3 sync s3://bucket/landing/ /data/landing/ --quiet
    local file_count=$(ls /data/landing/*.csv 2>/dev/null | wc -l)
    log_info "Downloaded $file_count files"
    [ $file_count -gt 0 ] || { log_error "No files to process!"; return 1; }
}

step_validate() {
    log_info "Step 2: Validating files"
    for file in /data/landing/*.csv; do
        validate_csv "$file" 8 100 || { log_error "Validation failed: $file"; return 1; }
    done
    log_info "All files valid"
}

step_transform() {
    log_info "Step 3: Transforming"
    timed python /opt/etl/transform.py
}

step_load() {
    log_info "Step 4: Loading to warehouse"
    retry 3 10 psql -c "\COPY silver.orders FROM '/data/output/orders.csv' CSV HEADER"
}

# Execute pipeline (composition):
run_pipeline() {
    step_ingest && step_validate && step_transform && step_load
}

# Main:
LOG_FILE="/var/log/etl/pipeline_$(date +%Y%m%d).log"
log_info "=== Pipeline started ==="

if run_pipeline; then
    log_info "=== Pipeline SUCCESS ==="
else
    log_error "=== Pipeline FAILED ==="
    alert "Daily pipeline failed!" "critical"
    exit 1
fi
```

---

## Interview Tips

> **Tip 1:** "How do you make bash scripts modular?" — Function libraries (lib/*.sh files) sourced by pipeline scripts. Each library has a namespace (db_*, s3_*, validate_*). Functions handle their own errors and logging. Pipeline scripts compose functions into steps. Adding new functionality: add a function to the appropriate library.

> **Tip 2:** "How do you test bash functions?" — Create test scripts that: source the library, call functions with known inputs, assert outputs (compare expected vs actual). Run in CI: `bash tests/test_utils.sh`. Not as rich as pytest, but catches regressions in shared library functions.

> **Tip 3:** "How do you compose a pipeline from functions?" — Each step is a function (step_ingest, step_validate, step_transform, step_load). Chain with `&&` (stop on first failure). Wrap in run_pipeline function. Call it: `if run_pipeline; then success; else failure; fi`. Clean, readable, each step testable independently.
