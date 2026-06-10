---
title: "Automation Patterns - Scenario Questions"
topic: bash-scripting
subtopic: automation-patterns
content_type: scenario_question
tags: [bash, automation, patterns, interview, scenarios]
---

# Scenario Questions — Bash Automation Patterns

<article data-difficulty="junior">

## 🟢 Junior: Automated File Watcher

**Scenario:** Your team lands CSV files into `/data/incoming/` throughout the day. Write a bash script that watches this directory and, whenever a new `.csv` file appears, moves it to `/data/processing/`, logs the filename and timestamp, and sends a simple notification (echo to a log file). The script should run continuously but sleep 30 seconds between checks to avoid CPU waste.

<details>
<summary>💡 Hint</summary>
Use a `while true` loop with `sleep 30`. Inside the loop, use `find` or a glob to detect new `.csv` files. Move each with `mv`, log with `echo "$(date) ..."`, and track processed files to avoid re-processing.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# /opt/etl/file_watcher.sh — Simple directory watcher
set -uo pipefail

INCOMING="/data/incoming"
PROCESSING="/data/processing"
LOG="/var/log/etl/file_watcher.log"
PROCESSED_LIST="/tmp/file_watcher_processed.txt"

mkdir -p "$PROCESSING"
touch "$PROCESSED_LIST"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"; }

log "File watcher started. Monitoring: $INCOMING"

while true; do
    for file in "$INCOMING"/*.csv; do
        # Skip if no matches (glob returned literal)
        [ -f "$file" ] || continue
        
        fname=$(basename "$file")
        
        # Skip if already processed
        grep -qxF "$fname" "$PROCESSED_LIST" && continue
        
        # Move to processing
        mv "$file" "$PROCESSING/$fname"
        echo "$fname" >> "$PROCESSED_LIST"
        
        log "NEW FILE: $fname → moved to processing/"
    done
    
    sleep 30
done

# USAGE:
# nohup /opt/etl/file_watcher.sh &
# OR run as a systemd service for production
```

**Key Points:**
- `while true` + `sleep 30`: continuous monitoring without burning CPU
- `[ -f "$file" ]`: handles case where glob matches nothing (returns literal `*.csv`)
- Processed list prevents re-processing same file if script restarts
- `tee -a "$LOG"`: writes to log AND stdout (visible if running interactively)
- For production: use `inotifywait` (instant detection) or systemd service
- This pattern is the foundation for file-triggered ETL pipelines

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Self-Healing ETL Wrapper

**Scenario:** You have a Python ETL script (`/opt/etl/transform.py`) that occasionally fails due to transient database connection issues. Design a bash automation wrapper that: retries the script up to 3 times with exponential backoff, checks if the process is already running (prevent duplicate execution), logs each attempt with duration, sends a Slack alert only after all retries are exhausted, and writes a health-check file on success.

<details>
<summary>💡 Hint</summary>
Use a PID file (flock or kill -0) for single-instance. Implement retry with `for i in 1 2 3` and `sleep $((2**i))` for backoff. Track start time with `$SECONDS`. On final failure → curl Slack webhook. On success → touch health file.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# /opt/etl/run_transform.sh — Self-healing ETL wrapper
set -uo pipefail

SCRIPT="/opt/etl/transform.py"
LOCK="/tmp/transform.lock"
HEALTH="/tmp/transform.health"
LOG="/var/log/etl/transform_$(date +%Y%m%d).log"
MAX_RETRIES=3
SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"; }

# --- Single instance guard ---
exec 200>"$LOCK"
if ! flock -n 200; then
    log "SKIP: Another instance is already running (lock held)"
    exit 0
fi

# --- Retry with exponential backoff ---
attempt=0
success=false

while [ $attempt -lt $MAX_RETRIES ]; do
    attempt=$((attempt + 1))
    backoff=$((2 ** attempt))  # 2, 4, 8 seconds
    
    log "Attempt $attempt/$MAX_RETRIES starting..."
    start_time=$SECONDS
    
    if python "$SCRIPT" >> "$LOG" 2>&1; then
        duration=$((SECONDS - start_time))
        log "✓ SUCCESS on attempt $attempt (${duration}s)"
        success=true
        break
    else
        duration=$((SECONDS - start_time))
        log "✗ FAILED attempt $attempt (${duration}s, exit code: $?)"
        
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "  Retrying in ${backoff}s (exponential backoff)..."
            sleep $backoff
        fi
    fi
done

# --- Post-execution ---
if $success; then
    # Write health-check file (monitored by external system)
    echo "last_success=$(date -Iseconds)" > "$HEALTH"
    echo "attempts=$attempt" >> "$HEALTH"
    log "Health file updated: $HEALTH"
else
    log "ALL $MAX_RETRIES ATTEMPTS FAILED!"
    
    # Alert only after all retries exhausted
    if [ -n "$SLACK_WEBHOOK" ]; then
        msg="🚨 ETL transform.py FAILED after $MAX_RETRIES attempts on $(hostname)"
        curl -sS -X POST "$SLACK_WEBHOOK" \
            -H 'Content-Type: application/json' \
            -d "{\"text\":\"$msg\"}" > /dev/null 2>&1
        log "Slack alert sent"
    fi
    
    exit 1
fi

# CRON ENTRY:
# */15 * * * * /opt/etl/run_transform.sh
```

**Key Points:**
- `flock -n 200`: non-blocking lock — skips immediately if another instance is running
- Exponential backoff: 2s → 4s → 8s (gives transient issues time to resolve)
- `$SECONDS`: bash built-in, tracks elapsed time automatically
- Alert only after ALL retries fail (avoids false alarms on transient errors)
- Health file: external monitoring (Datadog, Prometheus) can check staleness
- Lock auto-releases when script exits (file descriptor closes)
- Idempotent: safe to run from cron every 15 minutes

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Dynamic Pipeline Orchestrator

**Scenario:** Design a bash-based pipeline orchestrator that reads a DAG definition from a config file, resolves task dependencies, executes tasks in the correct order (parallelizing independent tasks), handles failures with configurable retry policies, and produces a final execution report. The config file format:

```
# task_name | depends_on | command | max_retries
ingest | - | /opt/etl/ingest.sh | 2
validate | ingest | /opt/etl/validate.sh | 1
transform_a | validate | /opt/etl/transform_a.sh | 3
transform_b | validate | /opt/etl/transform_b.sh | 3
aggregate | transform_a,transform_b | /opt/etl/aggregate.sh | 2
publish | aggregate | /opt/etl/publish.sh | 1
```

<details>
<summary>💡 Hint</summary>
Parse the DAG config into associative arrays (task→deps, task→cmd, task→retries). Use a topological execution loop: find tasks whose dependencies are all in "completed" set → run them in parallel (background `&` + `wait`). Track status per task. After all waves complete, generate report from status array.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
# /opt/etl/orchestrator.sh — DAG-based pipeline orchestrator
set -uo pipefail

DAG_FILE="${1:-/opt/etl/config/pipeline.dag}"
LOG="/var/log/etl/orchestrator_$(date +%Y%m%d_%H%M%S).log"
STATUS_DIR="/tmp/orchestrator_$$"

mkdir -p "$STATUS_DIR"
log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

# --- Parse DAG config ---
declare -A DEPS CMDS RETRIES STATUS DURATIONS
TASKS=()

while IFS='|' read -r task deps cmd retries; do
    # Skip comments and empty lines
    [[ "$task" =~ ^[[:space:]]*# ]] && continue
    [[ -z "$task" ]] && continue
    
    task=$(echo "$task" | xargs)    # trim whitespace
    deps=$(echo "$deps" | xargs)
    cmd=$(echo "$cmd" | xargs)
    retries=$(echo "$retries" | xargs)
    
    TASKS+=("$task")
    DEPS["$task"]="$deps"
    CMDS["$task"]="$cmd"
    RETRIES["$task"]="${retries:-1}"
    STATUS["$task"]="pending"
    DURATIONS["$task"]="0"
done < "$DAG_FILE"

log "DAG loaded: ${#TASKS[@]} tasks"

# --- Execute a single task with retry ---
run_task() {
    local task="$1"
    local cmd="${CMDS[$task]}"
    local max_retries="${RETRIES[$task]}"
    local attempt=0
    local start=$SECONDS
    
    while [ $attempt -lt $max_retries ]; do
        attempt=$((attempt + 1))
        log "  [$task] attempt $attempt/$max_retries: $cmd"
        
        if bash -c "$cmd" >> "$STATUS_DIR/${task}.log" 2>&1; then
            echo "success" > "$STATUS_DIR/${task}.status"
            echo "$((SECONDS - start))" > "$STATUS_DIR/${task}.duration"
            return 0
        fi
        
        [ $attempt -lt $max_retries ] && sleep $((2 ** attempt))
    done
    
    echo "failed" > "$STATUS_DIR/${task}.status"
    echo "$((SECONDS - start))" > "$STATUS_DIR/${task}.duration"
    return 1
}
export -f run_task

# --- Check if all deps of a task are satisfied ---
deps_satisfied() {
    local task="$1"
    local deps="${DEPS[$task]}"
    
    [ "$deps" = "-" ] && return 0  # No dependencies
    
    IFS=',' read -ra dep_list <<< "$deps"
    for dep in "${dep_list[@]}"; do
        dep=$(echo "$dep" | xargs)
        [ "${STATUS[$dep]}" = "success" ] || return 1
    done
    return 0
}

# --- Topological execution (wave-based) ---
pipeline_start=$SECONDS
wave=0
completed=0
total=${#TASKS[@]}

log "Starting pipeline execution..."
log ""

while [ $completed -lt $total ]; do
    wave=$((wave + 1))
    wave_tasks=()
    
    # Find ready tasks (deps satisfied, still pending)
    for task in "${TASKS[@]}"; do
        [ "${STATUS[$task]}" = "pending" ] || continue
        if deps_satisfied "$task"; then
            wave_tasks+=("$task")
        fi
    done
    
    # Check for deadlock (nothing ready but not all complete)
    if [ ${#wave_tasks[@]} -eq 0 ]; then
        log "⚠ DEADLOCK: No tasks ready but pipeline not complete!"
        log "  Blocked tasks:"
        for task in "${TASKS[@]}"; do
            [ "${STATUS[$task]}" = "pending" ] && log "    - $task (deps: ${DEPS[$task]})"
        done
        break
    fi
    
    log "Wave $wave: [${wave_tasks[*]}] (${#wave_tasks[@]} tasks in parallel)"
    
    # Execute wave tasks in parallel
    pids=()
    for task in "${wave_tasks[@]}"; do
        STATUS["$task"]="running"
        run_task "$task" &
        pids+=("$!:$task")
    done
    
    # Wait for all tasks in this wave
    for entry in "${pids[@]}"; do
        IFS=':' read -r pid task <<< "$entry"
        if wait "$pid"; then
            STATUS["$task"]="success"
            log "  ✓ $task ($(cat "$STATUS_DIR/${task}.duration")s)"
        else
            STATUS["$task"]="failed"
            log "  ✗ $task FAILED ($(cat "$STATUS_DIR/${task}.duration")s)"
            # Mark downstream tasks as skipped
            for t in "${TASKS[@]}"; do
                if [[ "${DEPS[$t]}" == *"$task"* ]] && [ "${STATUS[$t]}" = "pending" ]; then
                    STATUS["$t"]="skipped"
                    log "  ⊘ $t skipped (depends on failed: $task)"
                fi
            done
        fi
    done
    
    # Count completed
    completed=0
    for task in "${TASKS[@]}"; do
        [[ "${STATUS[$task]}" =~ ^(success|failed|skipped)$ ]] && completed=$((completed + 1))
    done
    
    log ""
done

# --- Final Report ---
total_time=$((SECONDS - pipeline_start))
success_count=0; fail_count=0; skip_count=0

log "═══════════════════════════════════════════════"
log "  Pipeline Execution Report"
log "═══════════════════════════════════════════════"
log ""

for task in "${TASKS[@]}"; do
    dur=$(cat "$STATUS_DIR/${task}.duration" 2>/dev/null || echo "0")
    case "${STATUS[$task]}" in
        success) symbol="✓"; success_count=$((success_count + 1)) ;;
        failed)  symbol="✗"; fail_count=$((fail_count + 1)) ;;
        skipped) symbol="⊘"; skip_count=$((skip_count + 1)); dur="-" ;;
        *)       symbol="?"; dur="-" ;;
    esac
    printf "  %s %-15s %5ss  (retries: %s)\n" "$symbol" "$task" "$dur" "${RETRIES[$task]}" | tee -a "$LOG"
done

log ""
log "  Total: $total tasks | ✓ $success_count | ✗ $fail_count | ⊘ $skip_count"
log "  Duration: ${total_time}s"
log "  Status: $([ $fail_count -eq 0 ] && echo 'SUCCESS ✓' || echo 'FAILED ✗')"
log "═══════════════════════════════════════════════"

# Cleanup
rm -rf "$STATUS_DIR"

[ $fail_count -eq 0 ]  # Exit code: 0=success, 1=failures occurred
```

**Key Points:**
- **DAG parsing:** Reads pipe-delimited config into associative arrays (bash 4+ required)
- **Topological execution:** Wave-based approach — each wave finds all tasks whose dependencies are met, then runs them in parallel
- **Parallelism:** Independent tasks (transform_a, transform_b) execute simultaneously via background processes
- **Retry with backoff:** Per-task configurable retry count + exponential backoff
- **Failure propagation:** When a task fails, all downstream dependents are marked "skipped" (prevents wasted execution)
- **Deadlock detection:** If no tasks are ready but pipeline isn't complete, reports blocked tasks
- **Status tracking:** File-based (safe for parallel writes) — each task writes its own status file
- **Execution report:** Shows per-task status, duration, and overall pipeline health
- **This is essentially a mini-Airflow in bash** — useful when Airflow is overkill or unavailable

</details>

</article>

</content>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a shell script and a cron job?**
A: A shell script is a file containing commands that can be executed manually or programmatically. A cron job is a scheduled task that runs a shell script (or command) automatically at specified time intervals using the cron daemon.

**Q: How do you make a shell script idempotent?**
A: Design the script so running it multiple times produces the same result as running it once. Use checks before creating files/directories (`[ -f file ] || touch file`), use `mkdir -p` instead of `mkdir`, and use upsert patterns for database operations.

**Q: What is a lockfile and why is it important in automation?**
A: A lockfile is a file created at the start of a script to signal that it is running, preventing concurrent executions. It is important to avoid race conditions and data corruption when the same script might be triggered multiple times simultaneously.

**Q: How do you handle failures gracefully in a long-running automation script?**
A: Use `set -e` to exit on errors, `trap` to catch signals and clean up resources, implement retry logic with exponential backoff for transient failures, and send alerts or write to a log when failures occur.

**Q: What is the purpose of `set -euo pipefail` in bash scripts?**
A: `set -e` exits on any error, `set -u` treats unset variables as errors, and `set -o pipefail` causes a pipeline to return the exit code of the first failed command rather than the last. Together they make scripts fail fast and loudly.

**Q: How do you pass configuration to automation scripts without hardcoding values?**
A: Use environment variables (loaded from a `.env` file or set externally), command-line arguments parsed with `getopts`, or a configuration file in a known location that the script reads at startup.

**Q: What is the difference between running a script with `./script.sh` vs `bash script.sh`?**
A: `./script.sh` requires the script to have execute permissions and uses the shebang line to determine the interpreter. `bash script.sh` explicitly runs the script with bash regardless of permissions or shebang, ignoring the shebang line.

**Q: How do you implement logging in a bash automation script?**
A: Redirect output to a log file with `exec >> /var/log/myscript.log 2>&1`, include timestamps using `echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] message"`, and use log rotation tools like `logrotate` to manage log file size.

---

## 💼 Interview Tips

- Demonstrate awareness of idempotency when describing automation — interviewers want to know you think about re-runs and failure recovery, not just the happy path.
- Mention `set -euo pipefail` early when asked about script reliability; it signals you write production-grade scripts, not quick hacks.
- Avoid saying you would just re-run failed scripts manually — talk about alerting, retries, and observability to show operational maturity.
- When discussing scheduling, go beyond cron and mention modern alternatives like Airflow or cloud schedulers, explaining when each is appropriate.
- Senior interviewers want to hear about testing automation scripts — mention unit testing with `bats`, dry-run modes, and staging environment validation.
- Discuss secret management explicitly: never hardcode credentials, use environment variables or a secrets manager like AWS Secrets Manager or Vault.
