---
title: "Automation Patterns - Intermediate"
topic: bash-scripting
subtopic: automation-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, automation, patterns, orchestration, deployment]
---

# Bash Automation Patterns — Intermediate

## Pattern: ETL Wrapper with Full Lifecycle

```bash
#!/bin/bash
# Universal ETL wrapper — makes any script production-ready
set -euo pipefail

# Wrapper accepts the actual ETL command as arguments:
# Usage: ./etl_wrapper.sh python /opt/etl/daily_orders.py --date=2024-03-15

COMMAND="$@"
JOB_NAME=$(echo "$1" | xargs basename | sed 's/\..*//')
LOG="/var/log/etl/${JOB_NAME}_$(date +%Y%m%d_%H%M%S).log"
LOCK="/tmp/etl_${JOB_NAME}.lock"
MAX_RUNTIME=7200
SLACK="${SLACK_WEBHOOK_URL:-}"

exec > >(tee -a "$LOG") 2>&1
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }

# Locking
exec 200>"$LOCK"
flock -n 200 || { log "SKIP: $JOB_NAME already running"; exit 0; }
trap 'rm -f "$LOCK"' EXIT

# Execute with full monitoring:
log "START: $JOB_NAME"
log "Command: $COMMAND"
log "Timeout: ${MAX_RUNTIME}s"

start=$(date +%s)
if timeout $MAX_RUNTIME $COMMAND; then
    duration=$(( $(date +%s) - start ))
    log "SUCCESS: $JOB_NAME (${duration}s)"
    
    # Heartbeat ping (dead man's switch)
    curl -fsS "https://hc-ping.com/${HC_UUID:-}" > /dev/null 2>&1 || true
else
    code=$?
    duration=$(( $(date +%s) - start ))
    log "FAILED: $JOB_NAME (exit=$code, ${duration}s)"
    
    [ -n "$SLACK" ] && curl -sS -X POST "$SLACK" \
        -d "{\"text\":\"🚨 $JOB_NAME failed (exit=$code, ${duration}s)\"}" > /dev/null 2>&1
    exit $code
fi

# Crontab entry:
# 0 6 * * * /opt/etl/etl_wrapper.sh python /opt/etl/daily_orders.py
# 0 * * * * /opt/etl/etl_wrapper.sh python /opt/etl/hourly_sync.py
# Any script becomes production-ready by wrapping it!
```

---

## Pattern: Configuration-Driven Batch Processor

```bash
#!/bin/bash
# Process multiple data sources defined in a config file (no code changes to add new sources!)

CONFIG_FILE="/opt/etl/config/sources.conf"
# Format: source_name|source_path|target_table|format|min_rows

set -euo pipefail
source /opt/etl/lib/utils.sh

log_info "=== Batch Processor Started ==="

while IFS='|' read -r name path table format min_rows; do
    [[ "$name" =~ ^#.*$ ]] && continue  # Skip comments
    [ -z "$name" ] && continue           # Skip empty lines
    
    log_info "Processing: $name"
    
    # Download
    if [[ "$path" == s3://* ]]; then
        aws s3 cp "$path" "/tmp/${name}.${format}" --quiet || { log_error "Download failed: $name"; continue; }
    else
        cp "$path" "/tmp/${name}.${format}" || { log_error "Copy failed: $name"; continue; }
    fi
    
    # Validate
    local_file="/tmp/${name}.${format}"
    rows=$(wc -l < "$local_file")
    if [ $rows -lt $min_rows ]; then
        log_warn "$name: only $rows rows (min: $min_rows) — skipping"
        continue
    fi
    
    # Load
    if retry 3 5 psql -c "\COPY $table FROM '$local_file' CSV HEADER"; then
        log_info "$name: loaded $rows rows into $table ✓"
    else
        log_error "$name: load FAILED"
    fi
    
    rm -f "$local_file"
done < "$CONFIG_FILE"

log_info "=== Batch Processor Complete ==="

# sources.conf:
# # name|path|target_table|format|min_rows
# orders|s3://lake/landing/orders.csv|raw.orders|csv|1000
# events|s3://lake/landing/events.csv|raw.events|csv|5000
# customers|/data/sftp/customers.csv|raw.customers|csv|100
#
# Adding a new source: just add a line to sources.conf! Zero code changes!
```

---

## Pattern: Deployment Automation

```bash
#!/bin/bash
# Deploy ETL code from git to production servers
set -euo pipefail

REPO="git@github.com:company/etl-pipelines.git"
DEPLOY_DIR="/opt/etl"
BACKUP_DIR="/opt/etl_backup/$(date +%Y%m%d_%H%M%S)"
ENV="${1:-production}"

log() { echo "[$(date '+%H:%M:%S')] $1"; }

log "Deploying to $ENV..."

# Backup current
log "Backing up current deployment..."
cp -r "$DEPLOY_DIR" "$BACKUP_DIR"

# Pull latest code
log "Pulling latest from git..."
cd "$DEPLOY_DIR"
git fetch origin
git checkout main
git pull origin main

# Install dependencies
log "Installing dependencies..."
pip install -r requirements.txt --quiet

# Run tests
log "Running smoke tests..."
if ! python -m pytest tests/smoke/ -q; then
    log "TESTS FAILED! Rolling back..."
    rm -rf "$DEPLOY_DIR"
    cp -r "$BACKUP_DIR" "$DEPLOY_DIR"
    alert "Deployment FAILED: tests didn't pass. Rolled back."
    exit 1
fi

# Restart services
log "Restarting pipeline services..."
systemctl restart etl-watcher.service

log "Deployment complete! ✓"
log "Rollback available at: $BACKUP_DIR"
```

---

## Pattern: Data Reconciliation Automation

```bash
#!/bin/bash
# Automated source→target reconciliation (verify loads are correct)
set -euo pipefail

TABLES=("orders" "events" "customers" "products")
FAILURES=0

echo "=== Data Reconciliation Report ==="
printf "%-20s %12s %12s %8s %s\n" "Table" "Source" "Target" "Diff" "Status"
printf "%-20s %12s %12s %8s %s\n" "-----" "------" "------" "----" "------"

for table in "${TABLES[@]}"; do
    # Source count (from landing files)
    source_count=$(wc -l < "/data/landing/${table}.csv" 2>/dev/null || echo 0)
    source_count=$((source_count - 1))  # Minus header
    
    # Target count (from database)
    target_count=$(psql -t -c "SELECT COUNT(*) FROM silver.$table WHERE load_date = CURRENT_DATE" | tr -d ' ')
    
    # Compare
    diff=$((target_count - source_count))
    if [ $diff -eq 0 ]; then
        status="✓"
    else
        status="✗ MISMATCH"
        FAILURES=$((FAILURES + 1))
    fi
    
    printf "%-20s %12d %12d %8d %s\n" "$table" "$source_count" "$target_count" "$diff" "$status"
done

echo ""
if [ $FAILURES -gt 0 ]; then
    echo "⚠️ $FAILURES reconciliation failures detected!"
    alert "$FAILURES tables have source/target mismatch"
    exit 1
fi
echo "✓ All tables reconciled successfully"
```

---

## Pattern: Self-Service Data Request

```bash
#!/bin/bash
# Script that analysts can run to extract data (self-service, parameterized)
# Usage: ./extract_data.sh --table=orders --date=2024-03-15 --format=csv

# Parse arguments:
for arg in "$@"; do
    case $arg in
        --table=*) TABLE="${arg#*=}" ;;
        --date=*) DATE="${arg#*=}" ;;
        --format=*) FORMAT="${arg#*=}" ;;
        --help) echo "Usage: $0 --table=TABLE --date=DATE [--format=csv|json]"; exit 0 ;;
    esac
done

# Defaults:
FORMAT="${FORMAT:-csv}"
DATE="${DATE:-$(date +%Y-%m-%d)}"
: "${TABLE:?ERROR: --table is required}"

# Validate:
ALLOWED_TABLES="orders events customers products"
if ! echo "$ALLOWED_TABLES" | grep -qw "$TABLE"; then
    echo "ERROR: Table '$TABLE' not allowed. Available: $ALLOWED_TABLES"
    exit 1
fi

# Extract:
OUTPUT="/data/extracts/${USER}/${TABLE}_${DATE}.${FORMAT}"
mkdir -p "$(dirname "$OUTPUT")"

echo "Extracting $TABLE for $DATE..."
if [ "$FORMAT" = "csv" ]; then
    psql -c "\COPY (SELECT * FROM gold.$TABLE WHERE date = '$DATE') TO '$OUTPUT' CSV HEADER"
else
    psql -t -c "SELECT row_to_json(t) FROM (SELECT * FROM gold.$TABLE WHERE date = '$DATE') t" > "$OUTPUT"
fi

echo "Output: $OUTPUT ($(wc -l < "$OUTPUT") rows)"
```

---

## Interview Tips

> **Tip 1:** "How do you make a script config-driven?" — Read source definitions from a config file (pipe-delimited or YAML-simple). Loop through entries, process each. Adding new source = adding a line to config (zero code changes). Benefits: non-engineers can modify config, changes are reviewable in git, no risk of breaking script logic.

> **Tip 2:** "How do you automate deployments for ETL code?" — Pattern: backup current → git pull → install deps → run smoke tests → if tests pass: keep new code. If tests fail: rollback to backup. Always have a rollback path. Alert on failure. This gives you: zero-downtime deploys with instant rollback capability.

> **Tip 3:** "What's a universal ETL wrapper?" — A script that wraps ANY command with: locking (prevent overlap), logging (to file), timeout (prevent infinite runs), alerting (Slack on failure), and heartbeat (dead man's switch on success). Usage: `./wrapper.sh python my_etl.py`. One wrapper makes every script production-ready without modifying the script itself.
