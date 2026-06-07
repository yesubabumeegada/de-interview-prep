---
title: "Shell Functions - Real-World Production Examples"
topic: bash-scripting
subtopic: shell-functions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, functions, production, libraries, patterns]
---

# Shell Functions — Real-World Production Examples

## Pattern 1: Production Utility Library

```bash
#!/bin/bash
# /opt/etl/lib/utils.sh — battle-tested utility functions

# === LOGGING ===
LOG_FILE="${LOG_FILE:-/dev/stdout}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${@:2}" | tee -a "$LOG_FILE"; }
log_info() { log "INFO" "$@"; }
log_warn() { log "WARN" "$@"; }
log_error() { log "ERROR" "$@" >&2; }
log_debug() { [ "${DEBUG:-false}" = "true" ] && log "DEBUG" "$@"; }

# === RETRY ===
retry() {
    local max="$1" delay="$2"; shift 2
    local attempt=1
    while [ $attempt -le $max ]; do
        "$@" && return 0
        log_warn "Retry $attempt/$max failed: $*"
        sleep $delay
        delay=$((delay * 2))
        attempt=$((attempt + 1))
    done
    log_error "All $max retries exhausted: $*"
    return 1
}

# === ALERTING ===
alert() {
    local msg="$1" severity="${2:-warning}"
    log_warn "ALERT [$severity]: $msg"
    [ -n "${SLACK_WEBHOOK:-}" ] && \
        curl -sS -X POST "$SLACK_WEBHOOK" -d "{\"text\":\"$([[ $severity == critical ]] && echo '🚨' || echo '⚠️') $msg\"}" > /dev/null 2>&1 || true
}

# === TIMING ===
timer_start() { eval "TIMER_$1=$(date +%s)"; }
timer_stop() { 
    local start_var="TIMER_$1"
    local duration=$(( $(date +%s) - ${!start_var} ))
    echo $duration
}

# === VALIDATION ===
require_vars() {
    local missing=()
    for var in "$@"; do
        [ -z "${!var:-}" ] && missing+=("$var")
    done
    [ ${#missing[@]} -gt 0 ] && { log_error "Missing: ${missing[*]}"; return 1; }
}

require_commands() {
    for cmd in "$@"; do
        command -v "$cmd" &>/dev/null || { log_error "Command not found: $cmd"; return 1; }
    done
}

# === FILE OPERATIONS ===
atomic_write() {
    local target="$1"; shift
    local tmp="${target}.tmp.$$"
    "$@" > "$tmp" && mv "$tmp" "$target" || { rm -f "$tmp"; return 1; }
}

wait_for_file() {
    local file="$1" timeout="${2:-600}" waited=0
    while [ ! -f "$file" ]; do
        sleep 10; waited=$((waited + 10))
        [ $waited -ge $timeout ] && { log_error "Timeout waiting for: $file"; return 1; }
    done
}
```

---

## Pattern 2: Database Operations Library

```bash
#!/bin/bash
# /opt/etl/lib/db.sh — database helper functions

DB_CONN_STRING="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

db_query() {
    psql "$DB_CONN_STRING" -t -A -c "$1"
}

db_query_csv() {
    psql "$DB_CONN_STRING" -t -A -F',' -c "$1"
}

db_execute() {
    psql "$DB_CONN_STRING" -c "$1" > /dev/null 2>&1
}

db_table_exists() {
    local table="$1"
    local exists=$(db_query "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='$table')")
    [ "$exists" = "t" ]
}

db_row_count() {
    db_query "SELECT COUNT(*) FROM $1"
}

db_is_reachable() {
    pg_isready -h "$DB_HOST" -p "$DB_PORT" -t 5 -q
}

db_load_csv() {
    local file="$1" table="$2"
    psql "$DB_CONN_STRING" -c "\COPY $table FROM '$file' CSV HEADER"
}

db_truncate_and_load() {
    local file="$1" table="$2"
    db_execute "TRUNCATE TABLE $table"
    db_load_csv "$file" "$table"
    log_info "Loaded $(db_row_count $table) rows into $table"
}
```

---

## Pattern 3: Complete Pipeline Using Libraries

```bash
#!/bin/bash
# /opt/etl/pipelines/daily_orders.sh
set -euo pipefail

# Load libraries
source /opt/etl/lib/utils.sh
source /opt/etl/lib/db.sh
source /opt/etl/config/load_env.sh

# Configuration
LOG_FILE="/var/log/etl/daily_orders_$(date +%Y%m%d).log"
LANDING="/data/landing/orders"
OUTPUT="/data/output/orders"

# Pre-flight
log_info "=== Daily Orders Pipeline ==="
require_commands psql aws python
require_vars DB_HOST DB_PASSWORD S3_BUCKET
retry 3 10 db_is_reachable || { alert "DB unreachable!" "critical"; exit 1; }

# Step 1: Download from S3
timer_start "ingest"
log_info "Downloading from S3..."
aws s3 sync "$S3_BUCKET/landing/orders/$(date +%Y/%m/%d)/" "$LANDING/" --quiet
file_count=$(ls "$LANDING"/*.csv 2>/dev/null | wc -l)
log_info "Downloaded $file_count files ($(timer_stop ingest)s)"
[ $file_count -eq 0 ] && { log_warn "No files today — skipping"; exit 0; }

# Step 2: Transform
timer_start "transform"
log_info "Transforming..."
python /opt/etl/scripts/transform_orders.py --input="$LANDING" --output="$OUTPUT"
log_info "Transform complete ($(timer_stop transform)s)"

# Step 3: Load to DB
timer_start "load"
log_info "Loading to database..."
for file in "$OUTPUT"/*.csv; do
    retry 3 5 db_load_csv "$file" "silver.orders"
done
log_info "Loaded $(db_row_count silver.orders) total rows ($(timer_stop load)s)"

# Step 4: Validate
expected_min=1000
actual=$(db_row_count "silver.orders WHERE order_date = CURRENT_DATE")
if [ $actual -lt $expected_min ]; then
    alert "Low row count: $actual (expected >$expected_min)" "warning"
fi

log_info "=== Pipeline Complete ==="
```

---

## Interview Tips

> **Tip 1:** "How do you organize a bash ETL project?" — Separate libraries (lib/utils.sh, lib/db.sh) from pipeline scripts (pipelines/daily_orders.sh). Libraries provide reusable functions (log, retry, db_query). Pipeline scripts source libraries and compose steps. Pattern: source libs → validate → ingest → transform → load → validate → done.

> **Tip 2:** "What functions should every DE bash toolkit have?" — (1) log/log_info/log_error (structured logging), (2) retry (exponential backoff), (3) alert (Slack/PagerDuty notification), (4) require_vars/require_commands (pre-flight validation), (5) atomic_write (safe file writes), (6) timer_start/timer_stop (performance tracking). These 6 make any script production-ready.

> **Tip 3:** "How do you make bash scripts testable?" — Extract logic into pure functions (input → output, no side effects). Test functions with known inputs and assert outputs. Keep the "main" script thin (just composes function calls). Function libraries are unit-testable; pipeline scripts are integration-testable.
