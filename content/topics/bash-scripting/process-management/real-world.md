---
title: "Process Management - Real-World Production Examples"
topic: bash-scripting
subtopic: process-management
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, process-management, production, monitoring, automation]
---

# Bash Process Management — Real-World Production Examples

## Pattern 1: ETL Job Wrapper with Full Lifecycle Management

```bash
#!/bin/bash
# Production wrapper: handles logging, locking, timeout, cleanup, alerting
set -euo pipefail

JOB_NAME="daily_orders_etl"
LOG="/var/log/etl/${JOB_NAME}_$(date +%Y%m%d_%H%M%S).log"
PID_FILE="/var/run/${JOB_NAME}.pid"
TIMEOUT=7200  # 2 hours max
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL:-}"

exec > >(tee -a "$LOG") 2>&1  # Capture ALL output to log

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$JOB_NAME] $1"; }
alert() { [ -n "$SLACK_WEBHOOK" ] && curl -sS -X POST "$SLACK_WEBHOOK" -d "{\"text\":\"$1\"}" > /dev/null 2>&1 || true; }

# Cleanup on exit (any exit)
cleanup() {
    rm -f "$PID_FILE"
    log "Exiting (cleanup complete)"
}
trap cleanup EXIT

# Prevent concurrent execution
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    log "SKIP: Already running (PID $(cat $PID_FILE))"
    exit 0
fi
echo $$ > "$PID_FILE"

# Run with timeout
log "Starting (PID: $$, timeout: ${TIMEOUT}s)"
start_time=$(date +%s)

if timeout $TIMEOUT python /opt/etl/daily_orders.py; then
    duration=$(( $(date +%s) - start_time ))
    log "SUCCESS in ${duration}s"
    
    # Alert if slow (SLA warning)
    if [ $duration -gt 3600 ]; then
        alert "⚠️ $JOB_NAME took ${duration}s (SLA: 3600s)"
    fi
else
    exit_code=$?
    duration=$(( $(date +%s) - start_time ))
    log "FAILED (exit: $exit_code, duration: ${duration}s)"
    
    if [ $exit_code -eq 124 ]; then
        alert "🚨 $JOB_NAME TIMEOUT after ${TIMEOUT}s!"
    else
        alert "🚨 $JOB_NAME FAILED (exit: $exit_code). Check: $LOG"
    fi
    exit $exit_code
fi
```

---

## Pattern 2: Parallel File Downloader with Progress

```bash
#!/bin/bash
# Download 50 files from S3 in parallel with progress tracking

S3_PREFIX="s3://data-lake/exports/$(date +%Y/%m/%d)/"
LOCAL_DIR="/data/landing/$(date +%Y%m%d)"
MAX_PARALLEL=8
PROGRESS_FILE="/tmp/download_progress.txt"

mkdir -p "$LOCAL_DIR"
> "$PROGRESS_FILE"

# Get file list
FILES=$(aws s3 ls "$S3_PREFIX" | awk '{print $4}')
TOTAL=$(echo "$FILES" | wc -l)
echo "Downloading $TOTAL files (parallelism: $MAX_PARALLEL)..."

# Download function
download_file() {
    local filename="$1"
    aws s3 cp "${S3_PREFIX}${filename}" "${LOCAL_DIR}/${filename}" --quiet
    echo "$filename" >> "$PROGRESS_FILE"
    local done=$(wc -l < "$PROGRESS_FILE")
    echo -ne "\r  Progress: $done/$TOTAL files"
}
export -f download_file
export S3_PREFIX LOCAL_DIR PROGRESS_FILE TOTAL

# Parallel download
echo "$FILES" | xargs -P $MAX_PARALLEL -I {} bash -c 'download_file "$@"' _ {}

echo -e "\nDownload complete: $TOTAL files → $LOCAL_DIR"
echo "Total size: $(du -sh "$LOCAL_DIR" | cut -f1)"
```

---

## Pattern 3: Service Health Checker

```bash
#!/bin/bash
# Check all pipeline dependencies before running ETL

check_service() {
    local name="$1" cmd="$2"
    if eval "$cmd" > /dev/null 2>&1; then
        echo "  ✓ $name"
        return 0
    else
        echo "  ✗ $name (FAILED)"
        return 1
    fi
}

echo "=== Pre-flight Health Check ==="
failures=0

check_service "PostgreSQL (source)" "pg_isready -h db-source -p 5432" || failures=$((failures+1))
check_service "PostgreSQL (target)" "pg_isready -h db-target -p 5432" || failures=$((failures+1))
check_service "S3 access" "aws s3 ls s3://data-lake/ --max-items 1" || failures=$((failures+1))
check_service "Disk space (>10GB free)" "[ \$(df /data --output=avail | tail -1) -gt 10485760 ]" || failures=$((failures+1))
check_service "API endpoint" "curl -sf https://api.internal/health" || failures=$((failures+1))
check_service "Kafka broker" "nc -z kafka-broker 9092" || failures=$((failures+1))

echo ""
if [ $failures -gt 0 ]; then
    echo "ABORT: $failures service(s) unavailable!"
    exit 1
fi
echo "All services healthy — proceeding with ETL."
```

---

## Pattern 4: Watchdog with Auto-Recovery

```bash
#!/bin/bash
# Watchdog: monitor Spark streaming job, restart on failure, alert after threshold

APP_NAME="streaming_orders"
START_CMD="/opt/spark/bin/spark-submit --class OrdersStream /opt/app/streaming.jar"
HEALTH_URL="http://localhost:4040/api/v1/applications"
MAX_RESTARTS=5
CHECK_INTERVAL=30

restarts=0
last_restart=0

start_app() {
    log "Starting $APP_NAME..."
    $START_CMD >> "/var/log/$APP_NAME.log" 2>&1 &
    APP_PID=$!
    echo $APP_PID > "/var/run/$APP_NAME.pid"
    log "Started with PID: $APP_PID"
    last_restart=$(date +%s)
}

is_healthy() {
    # Check: process running AND Spark UI responding
    kill -0 $APP_PID 2>/dev/null && curl -sf "$HEALTH_URL" > /dev/null 2>&1
}

log() { echo "[$(date '+%H:%M:%S')] $1"; }

# Initial start
start_app

# Monitoring loop
while true; do
    sleep $CHECK_INTERVAL
    
    if ! is_healthy; then
        log "UNHEALTHY: $APP_NAME (PID: $APP_PID)"
        
        # Reset restart counter if running for >1 hour successfully
        if [ $(($(date +%s) - last_restart)) -gt 3600 ]; then
            restarts=0
        fi
        
        restarts=$((restarts + 1))
        
        if [ $restarts -gt $MAX_RESTARTS ]; then
            log "CRITICAL: $APP_NAME failed $MAX_RESTARTS times! Giving up."
            alert "🚨 $APP_NAME crashed $MAX_RESTARTS times. Manual intervention needed!"
            exit 1
        fi
        
        # Kill zombie process if still lingering
        kill -9 $APP_PID 2>/dev/null
        sleep 5
        
        # Restart
        log "Restarting (attempt $restarts/$MAX_RESTARTS)..."
        start_app
    fi
done
```

---

## Pattern 5: Pipeline Dependency Runner

```bash
#!/bin/bash
# Simple DAG executor: respects dependencies between steps

# Define steps and dependencies
declare -A DEPS
DEPS[ingest_orders]=""
DEPS[ingest_events]=""
DEPS[transform_orders]="ingest_orders"
DEPS[transform_events]="ingest_events"
DEPS[build_gold]="transform_orders transform_events"
DEPS[notify]="build_gold"

declare -A STATUS
STEPS=(ingest_orders ingest_events transform_orders transform_events build_gold notify)

run_step() {
    local step="$1"
    echo "[$(date +%H:%M:%S)] Running: $step"
    if python /opt/etl/${step}.py; then
        STATUS[$step]="done"
    else
        STATUS[$step]="failed"
        return 1
    fi
}

can_run() {
    local step="$1"
    for dep in ${DEPS[$step]}; do
        [ "${STATUS[$dep]:-}" != "done" ] && return 1
    done
    return 0
}

# Execute DAG
remaining=${#STEPS[@]}
while [ $remaining -gt 0 ]; do
    progress=false
    for step in "${STEPS[@]}"; do
        [ "${STATUS[$step]:-}" != "" ] && continue  # Already processed
        
        if can_run "$step"; then
            run_step "$step" || { echo "FAILED: $step"; exit 1; }
            remaining=$((remaining - 1))
            progress=true
        fi
    done
    
    # Detect deadlock (nothing can run but steps remain)
    $progress || { echo "DEADLOCK: remaining steps can't run!"; exit 1; }
done

echo "All steps complete!"
```

---

## Interview Tips

> **Tip 1:** "Design a production ETL wrapper script" — Components: logging (tee to file), locking (PID file + check), timeout (prevent infinite run), cleanup (trap EXIT), exit code handling (success → log, failure → alert), SLA monitoring (duration check + warning). This wrapper makes ANY script production-ready.

> **Tip 2:** "How do you build a simple DAG executor in bash?" — Declare steps + dependencies in arrays. Loop: find steps whose dependencies are all "done" → run them. Repeat until all steps complete or a failure occurs. Simple version: sequential. Advanced: run independent steps in parallel (background + wait). For real production: use Airflow (but bash DAG is great for 5-10 step pipelines).

> **Tip 3:** "How do you implement a watchdog for streaming jobs?" — Loop: check if process is running (kill -0) AND healthy (HTTP health endpoint). If unhealthy: increment restart counter, kill orphan, restart. If restarts exceed threshold: stop trying, alert team (indicates systemic issue, not transient failure). Reset counter after stable period (1+ hours running = recovered).
