---
title: "Process Management - Scenario Questions"
topic: bash-scripting
subtopic: process-management
content_type: scenario_question
tags: [bash, process-management, interview, scenarios]
---

# Scenario Questions — Bash Process Management

<article data-difficulty="junior">

## 🟢 Junior: Background Job Management

**Scenario:** You need to run 3 data ingestion scripts simultaneously (they're independent) and wait for all to finish before starting the transformation step. Write the bash commands.

<details>
<summary>💡 Hint</summary>
Use & to background each job. Use `wait` to block until all background jobs complete. Then run the next step.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
echo "Starting parallel ingestion..."

# Run 3 jobs in parallel (& = background)
python ingest_orders.py > /tmp/ingest_orders.log 2>&1 &
PID1=$!

python ingest_events.py > /tmp/ingest_events.log 2>&1 &
PID2=$!

python ingest_customers.py > /tmp/ingest_customers.log 2>&1 &
PID3=$!

echo "PIDs: $PID1, $PID2, $PID3"

# Wait for ALL to complete
wait $PID1; status1=$?
wait $PID2; status2=$?
wait $PID3; status3=$?

# Check results
if [ $status1 -eq 0 ] && [ $status2 -eq 0 ] && [ $status3 -eq 0 ]; then
    echo "All ingestion complete! Starting transformation..."
    python transform.py
else
    echo "ERROR: One or more ingestion jobs failed!"
    echo "  orders: exit $status1, events: exit $status2, customers: exit $status3"
    exit 1
fi
```

**Key Points:**
- `&` runs command in background (returns immediately)
- `$!` captures the PID of the last backgrounded process
- `wait $PID` blocks until that specific process finishes
- `$?` after `wait` gives the exit code of the waited process
- All 3 run simultaneously (true parallelism if enough CPU cores)
- Transform only starts after ALL ingestion jobs succeed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Graceful Shutdown with Cleanup

**Scenario:** Your ETL script creates temp files and holds a lock. If killed (Ctrl+C or `kill`), temp files remain and the lock is never released. Implement graceful shutdown that cleans up regardless of how the script exits.

<details>
<summary>💡 Hint</summary>
Use `trap` to register a cleanup function for SIGTERM, SIGINT, and EXIT signals. The cleanup function removes temp files and releases the lock.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -euo pipefail

TEMP_DIR=$(mktemp -d /tmp/etl_XXXXXX)
LOCK_FILE="/var/lock/etl_pipeline.lock"
DB_CONN_FILE="$TEMP_DIR/db_connection"

# Cleanup function: runs on ANY exit (normal, error, signal)
cleanup() {
    local exit_code=$?
    echo "Cleaning up (exit code: $exit_code)..."
    
    # Remove temp files
    rm -rf "$TEMP_DIR"
    echo "  Removed temp dir: $TEMP_DIR"
    
    # Release lock
    rm -f "$LOCK_FILE"
    echo "  Released lock: $LOCK_FILE"
    
    # Close DB connection (if applicable)
    [ -f "$DB_CONN_FILE" ] && echo "  Closed DB connection"
    
    echo "Cleanup complete."
    exit $exit_code  # Preserve original exit code
}

# Register cleanup for ALL exit scenarios:
trap cleanup EXIT        # Normal exit
trap cleanup SIGTERM     # kill command
trap cleanup SIGINT      # Ctrl+C

# Acquire lock
if [ -f "$LOCK_FILE" ]; then
    echo "ERROR: Lock file exists — another instance running?"
    exit 1
fi
echo $$ > "$LOCK_FILE"

# Main work
echo "Pipeline started (PID: $$, temp: $TEMP_DIR)"
echo "connected" > "$DB_CONN_FILE"

python /opt/etl/heavy_transform.py --temp-dir="$TEMP_DIR"
# If this fails, crashes, or is killed: cleanup() STILL runs!

echo "Pipeline complete."
# cleanup() runs automatically via trap EXIT
```

**Key Points:**
- `trap cleanup EXIT`: runs on ANY exit (even `exit 1`, even script error)
- `trap cleanup SIGTERM SIGINT`: also runs on kill/Ctrl+C signals
- Cleanup removes temp files, releases locks, closes connections
- `$?` captured at start of cleanup preserves the original exit reason
- Without trap: killed script leaves lock + temp files → next run blocked!
- This pattern should be in EVERY production bash script

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Process Pool with Controlled Concurrency

**Scenario:** You have 50 files to process. Each takes 2-5 minutes. Running all 50 simultaneously would overwhelm the database. Implement a process pool: max 6 concurrent jobs, track success/failure of each, and report summary.

<details>
<summary>💡 Hint</summary>
Maintain a counter of active jobs. When at max: `wait -n` (wait for ANY one to finish) before starting another. Track PIDs and their associated filenames for reporting.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
set -uo pipefail

MAX_PARALLEL=6
LANDING="/data/landing"
declare -A PID_TO_FILE  # Map: PID → filename
SUCCESS=0
FAILED=0
TOTAL=0

process_file() {
    local file="$1"
    python /opt/etl/transform.py "$file"
}

# Collect all files
FILES=()
for f in "$LANDING"/*.csv; do
    [ -f "$f" ] && FILES+=("$f")
done
TOTAL=${#FILES[@]}
echo "Processing $TOTAL files (max $MAX_PARALLEL parallel)..."

# Process with concurrency control
active=0
for file in "${FILES[@]}"; do
    # Start processing in background
    process_file "$file" &
    PID_TO_FILE[$!]="$(basename "$file")"
    active=$((active + 1))
    
    # If we've hit max, wait for one to finish
    if [ $active -ge $MAX_PARALLEL ]; then
        wait -n  # Wait for ANY one child (bash 4.3+)
        exit_code=$?
        active=$((active - 1))
        
        if [ $exit_code -eq 0 ]; then
            SUCCESS=$((SUCCESS + 1))
        else
            FAILED=$((FAILED + 1))
        fi
    fi
done

# Wait for remaining jobs
while [ $active -gt 0 ]; do
    wait -n
    exit_code=$?
    active=$((active - 1))
    [ $exit_code -eq 0 ] && SUCCESS=$((SUCCESS + 1)) || FAILED=$((FAILED + 1))
done

# Summary
echo ""
echo "═══════════════════════════════"
echo "Results: $SUCCESS succeeded, $FAILED failed (total: $TOTAL)"
echo "═══════════════════════════════"

[ $FAILED -gt 0 ] && exit 1 || exit 0
```

**Key Points:**
- `wait -n`: waits for ANY one child to finish (not all) — key for pool management
- Never exceeds MAX_PARALLEL concurrent processes (database stays healthy)
- Each completion slot is immediately refilled (maximum throughput)
- Tracks success/failure count for reporting
- 50 files × 3.5 min avg with 6 parallel = ~30 min total (vs 175 min sequential!)
- Exit code 1 if ANY file failed (caller knows there were issues)
- `wait -n` requires bash 4.3+ (check with `bash --version`)

</details>

</article>
