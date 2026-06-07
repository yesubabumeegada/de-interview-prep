---
title: "Automation Patterns - Fundamentals"
topic: bash-scripting
subtopic: automation-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, automation, patterns, data-engineering, scripting]
---

# Bash Automation Patterns — Fundamentals

## Why Automation Patterns Matter

Data engineers automate everything: file transfers, data validation, pipeline orchestration, monitoring, alerting, and deployment. Bash is the glue that connects tools (Python, SQL, AWS CLI, Docker) into automated workflows.

---

## Core Automation Patterns

### Pattern 1: Watch and React (File Sensor)

```bash
#!/bin/bash
# Wait for a file to appear, then process it (like Airflow's FileSensor)

WATCH_PATH="/data/landing/daily_export.csv"
TIMEOUT=7200  # 2 hours max wait
INTERVAL=60   # Check every 60 seconds

echo "Watching for: $WATCH_PATH (timeout: ${TIMEOUT}s)"
waited=0

while [ ! -f "$WATCH_PATH" ]; do
    sleep $INTERVAL
    waited=$((waited + INTERVAL))
    
    if [ $waited -ge $TIMEOUT ]; then
        echo "TIMEOUT: File not received after $((TIMEOUT/60)) minutes!"
        alert "File sensor timeout: daily_export.csv not received"
        exit 1
    fi
    
    echo "  Waiting... (${waited}s / ${TIMEOUT}s)"
done

echo "File detected! Processing..."
python /opt/etl/process_daily.py "$WATCH_PATH"
mv "$WATCH_PATH" "/data/archive/$(date +%Y%m%d)_daily_export.csv"
echo "Done."
```

### Pattern 2: Retry Until Success

```bash
#!/bin/bash
# Retry a flaky operation until it succeeds (or give up)

MAX_RETRIES=5
RETRY_DELAY=30

for attempt in $(seq 1 $MAX_RETRIES); do
    echo "Attempt $attempt/$MAX_RETRIES..."
    
    if aws s3 sync s3://partner-bucket/data/ /data/landing/; then
        echo "Success on attempt $attempt!"
        break
    fi
    
    if [ $attempt -eq $MAX_RETRIES ]; then
        echo "FAILED after $MAX_RETRIES attempts!"
        exit 1
    fi
    
    echo "  Retrying in ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
    RETRY_DELAY=$((RETRY_DELAY * 2))  # Exponential backoff
done
```

### Pattern 3: Sequential Steps (Simple Pipeline)

```bash
#!/bin/bash
# Run steps in order — stop if any fails
set -euo pipefail

echo "=== Daily ETL Pipeline ==="
echo "[1/4] Ingesting..."
python /opt/etl/ingest.py

echo "[2/4] Validating..."
python /opt/etl/validate.py

echo "[3/4] Transforming..."
python /opt/etl/transform.py

echo "[4/4] Loading..."
python /opt/etl/load.py

echo "=== Pipeline Complete ==="
```

### Pattern 4: Parallel Execution

```bash
#!/bin/bash
# Run independent tasks in parallel, wait for all

echo "Starting parallel ingestion..."
python /opt/etl/ingest_orders.py &
python /opt/etl/ingest_events.py &
python /opt/etl/ingest_customers.py &

wait  # Block until ALL background tasks finish
echo "All ingestion complete!"

# Continue with dependent step:
python /opt/etl/join_all.py
```

### Pattern 5: Conditional Execution (If-Then Pipeline)

```bash
#!/bin/bash
# Run different logic based on conditions

TODAY=$(date +%u)  # 1=Monday, 7=Sunday

if [ $TODAY -eq 1 ]; then
    echo "Monday: running full refresh"
    python /opt/etl/full_refresh.py
else
    echo "Weekday: running incremental update"
    python /opt/etl/incremental.py
fi

# Only run reports on weekdays:
if [ $TODAY -le 5 ]; then
    python /opt/etl/generate_report.py
fi
```

### Pattern 6: Heartbeat / Health Check

```bash
#!/bin/bash
# Periodic health check with alerting

while true; do
    # Check: is the pipeline service healthy?
    if curl -sf "http://pipeline-service:8080/health" > /dev/null; then
        echo "[$(date)] ✓ Healthy"
    else
        echo "[$(date)] ✗ UNHEALTHY!"
        alert "Pipeline service health check failed!"
    fi
    
    sleep 60  # Check every minute
done
```

---

## Automation Building Blocks

| Pattern | When to Use | Example |
|---------|------------|---------|
| File Sensor | Wait for external file delivery | Partner sends daily CSV |
| Retry | Handle transient failures | API calls, S3 operations |
| Sequential | Steps depend on each other | ingest → transform → load |
| Parallel | Independent tasks | Download 5 files simultaneously |
| Conditional | Different logic per day/condition | Full refresh on Monday |
| Heartbeat | Continuous monitoring | Check service every 60s |
| Scheduled (cron) | Time-based execution | Daily at 6 AM |
| Event-driven (inotifywait) | React to file system events | New file → process |

---

## Script Templates

```bash
#!/bin/bash
# TEMPLATE: Production automation script

set -euo pipefail

# Configuration
SCRIPT_NAME=$(basename "$0")
LOG_FILE="/var/log/etl/${SCRIPT_NAME%.sh}_$(date +%Y%m%d).log"
LOCK_FILE="/tmp/${SCRIPT_NAME%.sh}.lock"

# Logging
exec > >(tee -a "$LOG_FILE") 2>&1
log() { echo "[$(date '+%H:%M:%S')] $1"; }

# Locking (prevent overlap)
exec 200>"$LOCK_FILE"
flock -n 200 || { log "Already running. Exiting."; exit 0; }

# Cleanup
trap 'rm -f "$LOCK_FILE"' EXIT

# Main
log "=== $SCRIPT_NAME started ==="
# ... your automation logic here ...
log "=== $SCRIPT_NAME complete ==="
```

---

## Interview Tips

> **Tip 1:** "What automation patterns do you use in bash for DE?" — Six core patterns: file sensor (wait for data), retry (handle flaky APIs), sequential (pipeline steps), parallel (independent tasks), conditional (weekday vs weekend logic), heartbeat (continuous monitoring). Most real pipelines combine 2-3 of these.

> **Tip 2:** "How do you structure an automation script?" — Template: set -euo pipefail (safety), logging setup (tee to file), locking (flock), cleanup (trap EXIT), then the actual logic. Every production script should have these five elements regardless of what it does.

> **Tip 3:** "Bash automation vs Python vs Airflow?" — Bash: gluing CLI tools together, file operations, simple orchestration (5-10 steps). Python: complex data transformations, API integrations, ML. Airflow: complex DAGs (20+ tasks), scheduling with backfill, retries, UI. Use bash for the "connective tissue" between tools; Python/Airflow for the heavy logic.
