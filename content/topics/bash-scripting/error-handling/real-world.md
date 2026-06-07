---
title: "Error Handling - Real-World Production Examples"
topic: bash-scripting
subtopic: error-handling
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, error-handling, production, reliability, patterns]
---

# Bash Error Handling — Real-World Production Examples

## Pattern 1: Production Pipeline with Full Error Handling

```bash
#!/bin/bash
set -euo pipefail

source /opt/etl/lib/utils.sh
source /opt/etl/lib/error_handling.sh

PIPELINE="daily_orders"
LOG="/var/log/etl/${PIPELINE}_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

set_context "$PIPELINE" "init"
trap '_global_error_handler $LINENO' ERR
trap 'cleanup' EXIT

TEMP_DIR=$(mktemp -d)
cleanup() { rm -rf "$TEMP_DIR"; log_info "Cleanup done"; }

# Pre-flight (fail fast if dependencies are down):
set_context "$PIPELINE" "preflight"
require_commands psql python aws
retry 3 5 pg_isready -h "$DB_HOST" -t 5

# Ingest
set_context "$PIPELINE" "ingest"
log_info "Downloading files..."
aws s3 sync "s3://$S3_BUCKET/landing/$(date +%Y/%m/%d)/" "$TEMP_DIR/" --quiet
file_count=$(find "$TEMP_DIR" -name "*.csv" | wc -l)
log_info "Downloaded $file_count files"
[ $file_count -gt 0 ] || { log_warn "No files today — exiting normally"; exit 0; }

# Transform
set_context "$PIPELINE" "transform"
log_info "Transforming..."
timed python /opt/etl/transform_orders.py --input="$TEMP_DIR" --output="$TEMP_DIR/output"

# Load
set_context "$PIPELINE" "load"
log_info "Loading to database..."
retry 3 10 python /opt/etl/load_orders.py --input="$TEMP_DIR/output"
row_count=$(db_query "SELECT COUNT(*) FROM silver.orders WHERE load_date = CURRENT_DATE")
log_info "Loaded $row_count rows"

# Validate
set_context "$PIPELINE" "validate"
[ "$row_count" -gt 0 ] || { log_error "Zero rows loaded!"; alert "Zero rows loaded!" "critical"; exit 1; }

log_info "Pipeline complete: $row_count rows loaded"
```

---

## Pattern 2: Batch Processing with Per-Item Error Isolation

```bash
#!/bin/bash
set -uo pipefail

# Process 50 files — don't let one bad file stop the other 49!
LANDING="/data/landing"
SUCCESS_DIR="/data/archive/$(date +%Y%m%d)"
ERROR_DIR="/data/errors/$(date +%Y%m%d)"
REPORT="/tmp/batch_report_$$.txt"

mkdir -p "$SUCCESS_DIR" "$ERROR_DIR"
> "$REPORT"

total=0; ok=0; failed=0

for file in "$LANDING"/*.csv; do
    [ -f "$file" ] || continue
    total=$((total + 1))
    fname=$(basename "$file")
    
    # Process in subshell (isolates errors!)
    if (
        set -e  # Strict inside subshell
        python /opt/etl/validate.py "$file"
        python /opt/etl/transform.py "$file" > "$SUCCESS_DIR/${fname%.csv}_transformed.csv"
    ); then
        mv "$file" "$SUCCESS_DIR/"
        echo "✓ $fname" >> "$REPORT"
        ok=$((ok + 1))
    else
        mv "$file" "$ERROR_DIR/"
        echo "✗ $fname (exit: $?)" >> "$REPORT"
        failed=$((failed + 1))
    fi
done

# Summary
echo ""
echo "=== Batch Summary ==="
echo "Total: $total | OK: $ok | Failed: $failed"
echo ""
cat "$REPORT"

# Alert if any failures
if [ $failed -gt 0 ]; then
    alert "$failed/$total files failed! Check: $ERROR_DIR"
fi

# Exit code reflects overall status
[ $failed -eq 0 ]
```

---

## Pattern 3: Defensive Database Operations

```bash
#!/bin/bash
set -euo pipefail

# Safe database operations with verification at each step

safe_db_load() {
    local source_file="$1"
    local target_table="$2"
    
    # Pre-check: source file valid?
    [ -s "$source_file" ] || { echo "ERROR: Source file empty!"; return 1; }
    local source_rows=$(( $(wc -l < "$source_file") - 1 ))
    
    # Pre-check: target table accessible?
    db_query "SELECT 1 FROM $target_table LIMIT 1" > /dev/null || { echo "ERROR: Can't access $target_table"; return 1; }
    local before_count=$(db_row_count "$target_table")
    
    # Load
    psql "$DB_CONN" -c "\COPY $target_table FROM '$source_file' CSV HEADER"
    
    # Post-check: verify load was successful
    local after_count=$(db_row_count "$target_table")
    local loaded=$((after_count - before_count))
    
    if [ $loaded -eq $source_rows ]; then
        echo "✓ Loaded $loaded rows into $target_table (matches source)"
    elif [ $loaded -gt 0 ]; then
        echo "⚠️ Loaded $loaded rows but source has $source_rows (partial load?)"
    else
        echo "✗ Zero rows loaded! Before=$before_count, After=$after_count"
        return 1
    fi
}

# Safe truncate with backup:
safe_truncate() {
    local table="$1"
    local backup_table="${table}_backup_$(date +%Y%m%d)"
    
    echo "Creating backup: $backup_table"
    db_query "CREATE TABLE $backup_table AS SELECT * FROM $table"
    
    echo "Truncating: $table"
    db_query "TRUNCATE TABLE $table"
    
    echo "Backup: $backup_table ($(db_row_count $backup_table) rows)"
}

# Usage:
safe_truncate "silver.orders"
safe_db_load "/data/output/orders.csv" "silver.orders" || {
    echo "Load failed! Restoring from backup..."
    db_query "INSERT INTO silver.orders SELECT * FROM silver.orders_backup_$(date +%Y%m%d)"
    exit 1
}
```

---

## Interview Tips

> **Tip 1:** "How do you prevent one bad file from stopping a batch?" — Process each item in a subshell (`if ( set -e; ... ); then success; else failure; fi`). The subshell isolates errors — one failure doesn't propagate to the main script. Collect all errors, process all items, then report aggregate status.

> **Tip 2:** "How do you make database operations safe?" — Pattern: check source → count before → load → count after → verify (expected = actual). For destructive ops (TRUNCATE): create backup table first → truncate → load → verify → keep backup for 24h. On failure: restore from backup immediately.

> **Tip 3:** "set_context + ERR trap — why is this important?" — When a script has 10+ operations, the ERR trap alone says "line 47 failed" — but not WHICH LOGICAL STEP. `set_context "pipeline_name" "step_name"` + custom ERR handler → error message says "Transform step failed at line 47: cp command returned 1". Operators know: which pipeline, which step, which command, which line.
