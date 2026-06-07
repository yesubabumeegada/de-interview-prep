---
title: "Process Management - Intermediate"
topic: bash-scripting
subtopic: process-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, process-management, signals, parallel, traps, daemon]
---

# Bash Process Management — Intermediate

## Signal Handling (trap)

```bash
#!/bin/bash
# Graceful shutdown: clean up temp files and connections when killed

TEMP_DIR=$(mktemp -d)
PID_FILE="/tmp/etl_pipeline.pid"

# Register cleanup function for signals:
cleanup() {
    echo "Caught signal — cleaning up..."
    rm -rf "$TEMP_DIR"
    rm -f "$PID_FILE"
    # Kill any child processes
    kill $(jobs -p) 2>/dev/null
    echo "Cleanup complete. Exiting."
    exit 0
}

# Trap signals: SIGTERM (kill), SIGINT (Ctrl+C), EXIT (script ends)
trap cleanup SIGTERM SIGINT EXIT

# Record PID (so other scripts can find/kill us):
echo $$ > "$PID_FILE"

# Main work:
echo "Pipeline started (PID: $$). Temp: $TEMP_DIR"
python long_running_etl.py --temp-dir "$TEMP_DIR"
echo "Pipeline complete."

# Cleanup happens automatically via trap EXIT!
```

### Common Trap Patterns

```bash
# Pattern 1: Cleanup temp files on exit (any exit):
trap 'rm -f /tmp/my_lockfile /tmp/my_tempdata' EXIT

# Pattern 2: Ignore SIGHUP (keep running when terminal closes):
trap '' SIGHUP  # Empty handler = ignore signal

# Pattern 3: Log and re-raise (for debugging):
trap 'echo "Error on line $LINENO"; exit 1' ERR

# Pattern 4: Notify on script termination:
trap 'curl -s "$SLACK_WEBHOOK" -d "{\"text\":\"Pipeline $0 stopped\"}"' EXIT

# Pattern 5: Prevent Ctrl+C during critical section:
trap '' SIGINT      # Disable Ctrl+C
# ... critical section (database transaction) ...
trap - SIGINT       # Re-enable Ctrl+C
```

---

## Parallel Execution with Control

```bash
#!/bin/bash
# Process N files in parallel with MAX_JOBS concurrency limit

MAX_JOBS=4
LANDING="/data/landing"

process_file() {
    local file="$1"
    echo "[$(date +%H:%M:%S)] Processing: $(basename $file)"
    python transform.py "$file"
    local status=$?
    if [ $status -eq 0 ]; then
        mv "$file" /data/archive/
        echo "[$(date +%H:%M:%S)] Done: $(basename $file)"
    else
        mv "$file" /data/errors/
        echo "[$(date +%H:%M:%S)] FAILED: $(basename $file)"
    fi
    return $status
}

# Job control: run up to MAX_JOBS in parallel
active_jobs=0
failed=0

for file in "$LANDING"/*.csv; do
    [ -f "$file" ] || continue
    
    process_file "$file" &
    active_jobs=$((active_jobs + 1))
    
    # When we hit the limit, wait for one to finish
    if [ $active_jobs -ge $MAX_JOBS ]; then
        wait -n  # Wait for ANY one child to finish (bash 4.3+)
        [ $? -ne 0 ] && failed=$((failed + 1))
        active_jobs=$((active_jobs - 1))
    fi
done

# Wait for remaining jobs
wait
echo "All done. Failed: $failed"
[ $failed -gt 0 ] && exit 1
```

---

## Process Monitoring Scripts

```bash
#!/bin/bash
# Monitor a critical process and restart if it dies

PROCESS_NAME="spark-submit"
CHECK_INTERVAL=60  # seconds
MAX_RESTARTS=3
RESTART_CMD="/opt/spark/run_pipeline.sh"

restarts=0

while true; do
    if ! pgrep -f "$PROCESS_NAME" > /dev/null; then
        echo "[$(date)] Process '$PROCESS_NAME' not running!"
        
        if [ $restarts -ge $MAX_RESTARTS ]; then
            echo "Max restarts ($MAX_RESTARTS) reached! Alerting..."
            curl -X POST "$SLACK_WEBHOOK" \
                -d "{\"text\":\"CRITICAL: $PROCESS_NAME failed after $MAX_RESTARTS restarts\"}"
            exit 1
        fi
        
        echo "Restarting (attempt $((restarts + 1))/$MAX_RESTARTS)..."
        $RESTART_CMD &
        restarts=$((restarts + 1))
        sleep 10  # Wait for startup
    else
        # Reset counter if process has been running for a while
        restarts=0
    fi
    
    sleep $CHECK_INTERVAL
done
```

---

## Subshells and Process Groups

```bash
# Subshell: runs commands in a child process (isolated environment)
(
    cd /data/processing
    export SPECIAL_VAR="inside_only"
    python transform.py
)
# After subshell: SPECIAL_VAR doesn't exist, we're still in original directory!

# Process group: kill all related processes at once
# Start a pipeline as a process group:
set -m  # Enable job control
(python step1.py | python step2.py | python step3.py) &
GROUP_PID=$!

# Kill the entire pipeline (all 3 processes) at once:
kill -- -$GROUP_PID  # Negative PID = kill process GROUP

# Timeout an entire pipeline:
timeout 600 bash -c 'python step1.py | python step2.py | python step3.py'
# If the pipeline exceeds 10 minutes: ALL processes killed together
```

---

## Process Resource Limits

```bash
# Limit resources for a process (prevent runaway jobs):

# Limit memory (kill if exceeds 4 GB):
ulimit -v 4194304  # Virtual memory limit in KB
python memory_hungry_job.py

# Limit CPU time (kill after 1 hour of CPU time):
ulimit -t 3600
python cpu_intensive_job.py

# Limit file size (prevent writing huge files):
ulimit -f 10485760  # Max file size 10 GB (in 512-byte blocks)

# Nice/Priority (lower priority so other jobs aren't affected):
nice -n 10 python background_analysis.py
# Runs at lower priority — doesn't steal CPU from critical pipelines

# ionice (I/O priority):
ionice -c 3 python heavy_io_job.py
# "Idle" I/O class: only uses disk when nothing else needs it
# Perfect for: backfills that shouldn't slow down production queries

# Combine: low priority + memory limit + timeout
nice -n 15 timeout 7200 bash -c 'ulimit -v 8388608; python big_job.py'
```

---

## Daemon-Style Scripts

```bash
#!/bin/bash
# Run as a background daemon (like a service)

PIDFILE="/var/run/data_watcher.pid"
LOGFILE="/var/log/data_watcher.log"

start() {
    if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") 2>/dev/null; then
        echo "Already running (PID: $(cat $PIDFILE))"
        return 1
    fi
    
    echo "Starting data watcher..."
    nohup bash -c '
        while true; do
            # Watch for new files and process them
            for f in /data/landing/*.csv; do
                [ -f "$f" ] || continue
                python /opt/etl/process.py "$f" && mv "$f" /data/archive/
            done
            sleep 30
        done
    ' >> "$LOGFILE" 2>&1 &
    
    echo $! > "$PIDFILE"
    echo "Started (PID: $!)"
}

stop() {
    if [ -f "$PIDFILE" ]; then
        kill $(cat "$PIDFILE") 2>/dev/null
        rm -f "$PIDFILE"
        echo "Stopped"
    else
        echo "Not running"
    fi
}

status() {
    if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") 2>/dev/null; then
        echo "Running (PID: $(cat $PIDFILE))"
    else
        echo "Not running"
        rm -f "$PIDFILE" 2>/dev/null
    fi
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    restart) stop; sleep 2; start ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac
```

---

## Interview Tips

> **Tip 1:** "How do you handle graceful shutdown in a bash script?" — Use `trap` to register a cleanup function for SIGTERM and SIGINT signals. The function: kills child processes, removes temp files/locks, closes connections, and exits cleanly. This prevents: orphaned processes, stale lock files, and corrupt output files when scripts are killed.

> **Tip 2:** "How do you limit parallel jobs in bash?" — Count active background processes. When count reaches MAX_JOBS: `wait -n` (wait for ANY one to finish) before starting the next. This gives you controlled parallelism without overwhelming the system. Alternative: `xargs -P N` or GNU `parallel -j N` for simpler cases.

> **Tip 3:** "How do you monitor and auto-restart a critical process?" — Loop: check `pgrep -f "process_name"` → if not running: increment counter, restart. If restarts exceed threshold: alert team (Slack webhook) and stop retrying (avoid infinite restart loops on a systemic failure). Use PID files to track the current instance.
