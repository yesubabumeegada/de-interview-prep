---
title: "Process Management - Senior Deep Dive"
topic: bash-scripting
subtopic: process-management
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, process-management, production, orchestration, reliability]
---

# Bash Process Management — Senior-Level Deep Dive

## Production Process Orchestration

```bash
#!/bin/bash
# Mini-orchestrator: run pipeline steps with dependency management,
# parallel execution, timeout, retry, and status reporting
set -euo pipefail

declare -A STEP_STATUS  # Track: step_name → success/failed/skipped
declare -A STEP_PID     # Track: step_name → PID (for parallel)
LOG="/var/log/pipeline/run_$(date +%Y%m%d_%H%M%S).log"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

run_step() {
    local name="$1"; local cmd="$2"; local timeout="${3:-3600}"; local retries="${4:-2}"
    
    log "START: $name"
    for attempt in $(seq 1 $retries); do
        if timeout $timeout bash -c "$cmd" >> "$LOG" 2>&1; then
            STEP_STATUS[$name]="success"
            log "DONE: $name (attempt $attempt)"
            return 0
        fi
        log "RETRY: $name (attempt $attempt/$retries failed)"
        sleep $((attempt * 10))
    done
    
    STEP_STATUS[$name]="failed"
    log "FAILED: $name after $retries attempts"
    return 1
}

run_parallel() {
    local -a steps=("$@")
    local -a pids=()
    
    for step in "${steps[@]}"; do
        eval "$step" &
        pids+=($!)
    done
    
    local failed=0
    for pid in "${pids[@]}"; do
        wait $pid || failed=$((failed + 1))
    done
    return $failed
}

# === PIPELINE EXECUTION ===
trap 'log "INTERRUPTED"; exit 130' SIGINT SIGTERM

log "=== Pipeline started ==="

# Phase 1: Parallel ingestion
run_step "ingest_orders" "python /opt/etl/ingest_orders.py" 600 3 &
run_step "ingest_events" "python /opt/etl/ingest_events.py" 600 3 &
run_step "ingest_customers" "python /opt/etl/ingest_customers.py" 600 3 &
wait

# Check Phase 1 results
for step in ingest_orders ingest_events ingest_customers; do
    if [ "${STEP_STATUS[$step]}" != "success" ]; then
        log "ABORT: $step failed — cannot continue to transformation"
        exit 1
    fi
done

# Phase 2: Sequential transformation (depends on all ingestion)
run_step "transform_silver" "python /opt/etl/transform.py" 1800 2
run_step "build_gold" "python /opt/etl/build_gold.py" 1200 2

# Summary
log "=== Pipeline complete ==="
for step in "${!STEP_STATUS[@]}"; do
    log "  $step: ${STEP_STATUS[$step]}"
done
```

---

## Process Isolation with cgroups/containers

```bash
# Limit ETL job to specific CPU/memory (prevent impacting other services):

# Using systemd-run (modern Linux):
systemd-run --scope -p MemoryMax=4G -p CPUQuota=200% python heavy_etl.py
# MemoryMax=4G: killed if exceeds 4 GB (OOM killer)
# CPUQuota=200%: max 2 cores (on a 4-core machine)

# Using Docker (full isolation):
docker run --rm \
    --memory=4g --cpus=2 \
    --mount type=bind,src=/data,dst=/data \
    -e DB_HOST=prod-db \
    python-etl:latest python /app/transform.py
# Isolated: own filesystem, network, resource limits
# Reproducible: same environment every time

# Resource monitoring within limits:
# /sys/fs/cgroup/memory.current → actual memory usage
# /sys/fs/cgroup/cpu.stat → CPU time consumed
```

---

## Advanced Signal Handling for Data Pipelines

```bash
#!/bin/bash
# Pipeline that handles shutdown gracefully (completes current record, saves checkpoint)

CHECKPOINT_FILE="/data/state/pipeline_checkpoint.txt"
CURRENT_OFFSET=0
SHOULD_STOP=false

# Graceful shutdown: finish current batch, save state, then exit
handle_shutdown() {
    echo "Shutdown signal received. Finishing current batch..."
    SHOULD_STOP=true
    # Don't exit here! Let the loop finish its current iteration
}
trap handle_shutdown SIGTERM SIGINT

# Load checkpoint (resume from where we left off)
if [ -f "$CHECKPOINT_FILE" ]; then
    CURRENT_OFFSET=$(cat "$CHECKPOINT_FILE")
    echo "Resuming from offset: $CURRENT_OFFSET"
fi

# Processing loop (interruptible between batches, not mid-batch)
while ! $SHOULD_STOP; do
    echo "Processing batch starting at offset: $CURRENT_OFFSET"
    
    # Process one batch (this completes fully even if signal arrives)
    python process_batch.py --offset=$CURRENT_OFFSET --batch-size=1000
    
    if [ $? -eq 0 ]; then
        CURRENT_OFFSET=$((CURRENT_OFFSET + 1000))
        echo "$CURRENT_OFFSET" > "$CHECKPOINT_FILE"  # Save progress
    else
        echo "Batch failed! Retrying same offset next time."
        break
    fi
done

echo "Pipeline stopped cleanly at offset: $CURRENT_OFFSET"
# On restart: picks up from saved checkpoint (no data loss, no duplicates!)
```

---

## Zombie Process Prevention

```bash
# ZOMBIE: child process finished but parent hasn't "waited" for it
# Symptoms: defunct processes accumulating, PID table fills up

# PREVENTION: always wait for child processes!

# BAD (creates zombies if parent doesn't wait):
python job.py &
# ... parent continues without ever calling 'wait' ...

# GOOD (properly reaps children):
python job.py &
child_pid=$!
# ... do other work ...
wait $child_pid  # Reaps the child (removes zombie)

# For long-running daemons: use trap to auto-reap
trap 'wait' SIGCHLD  # Automatically wait() when any child exits

# Detect zombies:
ps aux | awk '$8 ~ /Z/ {print}'  # Find zombie processes
# If found: the PARENT process has a bug (not waiting for children)
```

---

## Interview Tips

> **Tip 1:** "Design a bash pipeline with parallel steps and dependencies" — Phase-based: Phase 1 (independent steps in parallel with `&` + `wait`), validate all succeeded, Phase 2 (dependent steps sequentially or parallel). Each step: has timeout (`timeout`), retry logic (loop), and status tracking. Abort pipeline if a critical dependency fails.

> **Tip 2:** "How do you implement checkpointing in a bash pipeline?" — Save progress to a file after each successful batch (offset/timestamp). On restart: read the checkpoint file and resume from that point. Combined with graceful shutdown (trap SIGTERM → finish current batch → save → exit): gives you exactly-once semantics with any signal/restart scenario.

> **Tip 3:** "How do you prevent resource exhaustion from runaway ETL jobs?" — Multiple layers: (1) `timeout` (kill if exceeds time limit), (2) `ulimit` (memory/file-size limits), (3) systemd-run with cgroup limits (CPU/memory hard caps), (4) Docker containers (full isolation). For production: prefer Docker (isolation + reproducibility). For quick scripts: `timeout` + `ulimit` is sufficient.

## ⚡ Cheat Sheet

**Background jobs**
```bash
cmd &           # background; inherits stdin/stdout
cmd &>/dev/null & # background; no output
PID=$!          # capture PID of last background job
wait $PID       # wait for specific PID
wait            # wait for all background jobs
```

**Signal handling**
```bash
# Common signals
kill -TERM $PID  # graceful stop (15)
kill -KILL $PID  # force stop (9)
kill -HUP  $PID  # reload config (1)
kill -USR1 $PID  # custom signal (10)

# Trap signals in script
trap 'echo "SIGTERM received"; cleanup; exit 0' TERM
trap 'echo "SIGINT received"; exit 130' INT
```

**Process groups and job control**
```bash
# Kill entire process group (all children too)
kill -TERM -$PID  # negative PID = process group
# Disown (keep running after terminal closes)
cmd &; disown $!
# nohup (immune to hangup signal)
nohup long_running_cmd > output.log 2>&1 &
```

**Monitoring children**
```bash
# Wait with timeout
timeout 3600 long_cmd || { echo "Timed out after 1hr"; kill $!; }
# Track multiple children
pids=()
for task in "${tasks[@]}"; do
    run_task "$task" & pids+=($!)
done
failed=0
for pid in "${pids[@]}"; do
    wait "$pid" || ((failed++))
done
[ $failed -gt 0 ] && die "$failed tasks failed"
```

**PID files**
```bash
PIDFILE=/var/run/myservice.pid
echo $$ > "$PIDFILE"
trap "rm -f $PIDFILE" EXIT
# Check if already running
[ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null && die "Already running"
```

**Resource limits**
```bash
ulimit -v $((4*1024*1024))  # limit virtual memory (4 GB)
ulimit -t 3600               # CPU time limit (1 hour)
nice -n 10 heavy_cmd         # lower priority (10 = nicer)
ionice -c 3 io_heavy_cmd     # idle IO priority
```
